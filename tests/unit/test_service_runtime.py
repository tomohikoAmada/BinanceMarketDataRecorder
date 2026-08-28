from __future__ import annotations

import asyncio
import io
import logging
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import binance_market_data_recorder.service.runtime as runtime_module
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import Market
from binance_market_data_recorder.logging import configure_logging
from binance_market_data_recorder.service.deployment_identity import (
    RuntimeDeploymentIdentity,
)
from binance_market_data_recorder.service.lock import (
    ServiceAlreadyRunning,
    ServiceProcessLock,
)
from binance_market_data_recorder.service.power import CaffeinateAssertion
from binance_market_data_recorder.service.runtime import (
    RuntimeCollector,
    ServiceRuntime,
    _collector_factory,
)
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.spool.recovery import RecoveryAction
from binance_market_data_recorder.status import service_status
from binance_market_data_recorder.storage.capacity import HARD_RESERVE_BYTES
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from binance_market_data_recorder.supervisor import ReadinessSnapshot


class FakeCollector:
    def __init__(
        self,
        market: Market,
        instance_id: str,
        *,
        fail: bool = False,
        return_early: bool = False,
    ) -> None:
        self.market = market
        self.instance_id = instance_id
        self.fail = fail
        self.return_early = return_early
        self.started = False
        self.stopped = False

    async def run(self, stop: asyncio.Event) -> None:
        self.started = True
        if self.fail:
            raise RuntimeError("injected collector failure")
        if self.return_early:
            return
        await stop.wait()
        self.stopped = True

    def readiness_snapshot(self) -> ReadinessSnapshot:
        streams = frozenset({"diff_depth", "agg_trade", "book_ticker"})
        return ReadinessSnapshot(
            market=self.market,
            symbol="BTCUSDT",
            collector_instance_id=self.instance_id,
            collector_version="test",
            connected_streams=streams if self.started else frozenset(),
            persisted_streams=streams if self.started else frozenset(),
            snapshot_persisted=self.started,
            orderbook_synchronized=self.started,
            event_count=1 if self.started else 0,
            last_receive_time_utc_ns=1 if self.started else None,
            failure=None,
        )


class FakeSleepObserver:
    def __init__(self, callback: Callable[[str, int], None]) -> None:
        self.callback = callback
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True
        self.callback("will_sleep", 100)
        self.callback("did_wake", 200)

    def stop(self) -> None:
        self.stopped = True


def test_runtime_applies_ingress_capacity_to_both_bounded_queue_levels(
    tmp_path: Path,
) -> None:
    collectors = _collector_factory(
        RecorderConfig(data_root=tmp_path, ingress_queue_capacity=65_536),
        logging.getLogger("test.runtime.capacity"),
        "test",
        "service-instance",
    )
    spot = collectors["spot"]
    usdm = collectors["um_perpetual"]
    assert spot.settings.queue_capacity == 65_536  # type: ignore[attr-defined]
    assert spot.settings.receipt_queue_capacity == 65_536  # type: ignore[attr-defined]
    assert usdm.settings.queue_capacity == 65_536  # type: ignore[attr-defined]
    assert usdm.settings.receipt_queue_capacity == 65_536  # type: ignore[attr-defined]
    spot.catalog.close()  # type: ignore[attr-defined]
    usdm.catalog.close()  # type: ignore[attr-defined]


class FakePowerAssertion(CaffeinateAssertion):
    def __init__(self) -> None:
        super().__init__(enabled=False)
        self.started = False
        self.stopped = False

    @property
    def active(self) -> bool:
        return self.started and not self.stopped

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


class ControlledHeartbeatRuntime(ServiceRuntime):
    heartbeat_ticks: asyncio.Queue[None]
    heartbeat_owner_count: int

    async def _heartbeat(self, stop: asyncio.Event) -> None:
        self.heartbeat_owner_count += 1
        await super()._heartbeat(stop)

    async def _wait_for_heartbeat_interval(self, stop: asyncio.Event) -> None:
        tick = asyncio.create_task(self.heartbeat_ticks.get())
        stopping = asyncio.create_task(stop.wait())
        _done, pending = await asyncio.wait(
            {tick, stopping}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


class AdvancingClock:
    def __init__(self, value: int = 1_000_000_000_000) -> None:
        self.value = value

    def __call__(self) -> int:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += seconds * 1_000_000_000


def _config(tmp_path: Path) -> RecorderConfig:
    return RecorderConfig(
        data_root=tmp_path,
        heartbeat_seconds=1.0,
        sleep_gap_threshold_seconds=5.0,
    )


def _vps_identity() -> RuntimeDeploymentIdentity:
    return RuntimeDeploymentIdentity(
        identity_sha256="a" * 64,
        source_git_sha="b" * 40,
        wheel_sha256="c" * 64,
        config_sha256="d" * 64,
        systemd_unit_sha256="e" * 64,
        capacity_profile_id="vps-production-v1",
    )


def test_process_lock_rejects_a_second_service(tmp_path: Path) -> None:
    first = ServiceProcessLock(tmp_path / "service.lock")
    second = ServiceProcessLock(tmp_path / "service.lock")
    first.acquire()
    try:
        with pytest.raises(ServiceAlreadyRunning):
            second.acquire()
    finally:
        first.release()
    second.acquire()
    second.release()


def test_starting_heartbeat_advances_during_recovery_and_stop_is_cooperative(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        entered = threading.Event()
        pause = threading.Event()
        clock = AdvancingClock()
        factory_called = False

        def blocked_recovery(
            *,
            layout: StorageLayout,
            catalog: Catalog,
            authority_path: Path | None,
            stop_requested: Callable[[], bool] | None,
        ) -> list[RecoveryAction]:
            del layout, catalog, authority_path
            entered.set()
            if stop_requested is None:
                raise AssertionError("service recovery did not receive stop authority")
            while not stop_requested():
                pause.wait(0.001)
            return []

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            nonlocal factory_called
            factory_called = True
            return {}

        monkeypatch.setattr(runtime_module, "recover_storage", blocked_recovery)
        runtime = ControlledHeartbeatRuntime(
            config=_config(tmp_path),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
            utc_clock_ns=clock,
        )
        runtime.heartbeat_ticks = asyncio.Queue()
        runtime.heartbeat_owner_count = 0

        task = asyncio.create_task(runtime.run())
        assert await asyncio.to_thread(entered.wait, 2)
        for _ in range(100):
            state = runtime.state_store.read()
            if state is not None and state["status"] == "STARTING":
                break
            await asyncio.sleep(0)
        else:
            pytest.fail("STARTING state was not published")
        assert state["startup_recovery_complete"] is False
        assert state["capacity"] is None
        assert state["markets"] == {}
        initial_heartbeat = cast(int, state["heartbeat_at_utc_ns"])
        started_at = cast(int, state["started_at_utc_ns"])

        clock.advance(31)
        runtime.heartbeat_ticks.put_nowait(None)
        for _ in range(100):
            await asyncio.sleep(0)
            state = runtime.state_store.read()
            if (
                state is not None
                and cast(int, state["heartbeat_at_utc_ns"]) > initial_heartbeat
            ):
                break
        else:
            pytest.fail("heartbeat did not advance during blocked recovery")
        assert cast(int, state["heartbeat_at_utc_ns"]) == clock.value
        assert clock.value - started_at > 30_000_000_000
        assert state["status"] == "STARTING"
        assert state["startup_recovery_complete"] is False
        assert factory_called is False
        assert runtime.heartbeat_owner_count == 1

        runtime.request_stop("SIGTERM")
        await asyncio.wait_for(task, timeout=2)
        final = runtime.state_store.read()
        assert final is not None
        assert final["status"] == "STOPPED"
        assert final["startup_recovery_complete"] is False
        assert factory_called is False
        assert runtime._catalog is None

    asyncio.run(exercise())
    probe = ServiceProcessLock(tmp_path / "state" / "service.lock")
    probe.acquire()
    probe.release()
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        assert catalog.operational_events(event_type="SERVICE_FAILED") == []
        assert len(catalog.operational_events(event_type="SERVICE_STOPPED")) == 1


def test_recovery_completion_observes_capacity_before_collectors_and_reuses_heartbeat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        entered = threading.Event()
        release = threading.Event()
        order: list[str] = []
        collectors = {
            "spot": FakeCollector("spot", "service-spot"),
            "um_perpetual": FakeCollector("um_perpetual", "service-um"),
        }

        def slow_recovery(
            *,
            layout: StorageLayout,
            catalog: Catalog,
            authority_path: Path | None,
            stop_requested: Callable[[], bool] | None,
        ) -> list[RecoveryAction]:
            del layout, catalog, authority_path
            entered.set()
            if not release.wait(timeout=2):
                raise AssertionError("recovery completion was not released")
            assert stop_requested is not None and not stop_requested()
            order.append("recovery")
            return []

        def disk_usage(_path: Path) -> object:
            order.append("capacity")
            return SimpleNamespace(
                total=40 * 1024**3,
                used=20 * 1024**3,
                free=20 * 1024**3,
            )

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            assert runtime._startup_recovery_complete is True
            assert runtime._capacity_evidence is not None
            order.append("collectors")
            return collectors

        monkeypatch.setattr(runtime_module, "recover_storage", slow_recovery)
        runtime = ControlledHeartbeatRuntime(
            config=RecorderConfig(
                data_root=tmp_path,
                capacity_profile="vps-production-v1",
                heartbeat_seconds=1.0,
            ),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
            deployment_identity=_vps_identity(),
            disk_usage=disk_usage,
            capacity_poll_seconds=60.0,
        )
        runtime.heartbeat_ticks = asyncio.Queue()
        runtime.heartbeat_owner_count = 0

        task = asyncio.create_task(runtime.run())
        assert await asyncio.to_thread(entered.wait, 2)
        state = runtime.state_store.read()
        assert state is not None
        assert state["status"] == "STARTING"
        assert state["startup_recovery_complete"] is False
        release.set()
        for _ in range(200):
            await asyncio.sleep(0.005)
            state = runtime.state_store.read()
            if state is not None and state["status"] == "RUNNING":
                break
        else:
            pytest.fail("runtime did not become RUNNING after recovery")

        assert state["startup_recovery_complete"] is True
        assert isinstance(state["capacity"], dict)
        assert all(collector.started for collector in collectors.values())
        assert order[:3] == ["recovery", "capacity", "collectors"]
        assert runtime.heartbeat_owner_count == 1
        runtime.request_stop("SIGTERM")
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(exercise())


def test_stop_during_startup_capacity_observation_prevents_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        recovery_complete = threading.Event()
        capacity_entered = threading.Event()
        capacity_returned = threading.Event()
        release_capacity = threading.Event()
        factory_called = False
        sleep_observer: FakeSleepObserver | None = None
        collectors = {
            "spot": FakeCollector("spot", "service-spot"),
            "um_perpetual": FakeCollector("um_perpetual", "service-um"),
        }

        def completed_recovery(
            *,
            layout: StorageLayout,
            catalog: Catalog,
            authority_path: Path | None,
            stop_requested: Callable[[], bool] | None,
        ) -> list[RecoveryAction]:
            del layout, catalog, authority_path, stop_requested
            recovery_complete.set()
            return []

        def blocked_capacity() -> dict[str, object]:
            evidence = {
                "observed_at_utc_ns": 1,
                "total_bytes": 40 * 1024**3,
                "free_bytes": 20 * 1024**3,
                "capacity_profile": "vps-production-v1",
                "capacity_state": "NORMAL",
                "hard_reserve_eta": {"status": "NOT_APPROACHING"},
                "actual_hard_reserve_reached": False,
            }
            capacity_entered.set()
            if not release_capacity.wait(timeout=2):
                raise AssertionError("capacity observation was not released")
            capacity_returned.set()
            runtime._capacity_evidence = evidence
            return evidence

        def sleep_factory(callback: Callable[[str, int], None]) -> FakeSleepObserver:
            nonlocal sleep_observer
            sleep_observer = FakeSleepObserver(callback)
            return sleep_observer

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            nonlocal factory_called
            factory_called = True
            return collectors

        monkeypatch.setattr(runtime_module, "recover_storage", completed_recovery)
        runtime = ServiceRuntime(
            config=RecorderConfig(
                data_root=tmp_path,
                capacity_profile="vps-production-v1",
                heartbeat_seconds=1.0,
            ),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=sleep_factory,
            power_assertion=FakePowerAssertion(),
            deployment_identity=_vps_identity(),
        )
        monkeypatch.setattr(runtime, "_observe_vps_capacity", blocked_capacity)

        task = asyncio.create_task(runtime.run())
        assert await asyncio.to_thread(recovery_complete.wait, 2)
        assert await asyncio.to_thread(capacity_entered.wait, 2)
        assert not capacity_returned.is_set()
        assert runtime._startup_recovery_complete is True
        assert not task.done()

        runtime.request_stop("SIGTERM")
        assert runtime._status == "STOPPING"
        assert runtime.shutdown_reason == "SIGTERM"
        assert runtime._stop is not None and runtime._stop.is_set()
        assert runtime._recovery_stop is not None and runtime._recovery_stop.is_set()

        release_capacity.set()
        await asyncio.wait_for(task, timeout=2)

        assert capacity_returned.is_set()
        assert factory_called is False
        assert runtime._supervisor is None
        assert all(not collector.started for collector in collectors.values())
        power = runtime.power_assertion
        assert isinstance(power, FakePowerAssertion)
        assert power.started is False
        assert sleep_observer is not None
        assert sleep_observer.started is False
        final = runtime.state_store.read()
        assert final is not None
        assert final["status"] == "STOPPED"
        assert final["shutdown_reason"] == "SIGTERM"
        assert final["markets"] == {}

        with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
            assert catalog.operational_events(event_type="SERVICE_STARTED") == []
            assert catalog.operational_events(event_type="SERVICE_FAILED") == []
            stopped = catalog.operational_events(event_type="SERVICE_STOPPED")
        assert len(stopped) == 1
        assert cast(dict[str, object], stopped[0]["evidence"])["reason"] == (
            "SIGTERM"
        )

    asyncio.run(exercise())


def test_runtime_writes_live_state_sleep_gap_and_graceful_stop(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        collectors = {
            "spot": FakeCollector("spot", "service-spot"),
            "um_perpetual": FakeCollector("um_perpetual", "service-um"),
        }

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            return collectors

        power = FakePowerAssertion()
        runtime = ServiceRuntime(
            config=_config(tmp_path),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=power,
        )
        task = asyncio.create_task(runtime.run())
        for _ in range(100):
            await asyncio.sleep(0.005)
            state = runtime.state_store.read()
            if state is not None and state["status"] == "RUNNING" and collectors[
                "spot"
            ].started:
                break
        else:
            pytest.fail("runtime did not become RUNNING")
        for _ in range(200):
            live = service_status(tmp_path)
            if live["network_connected"] is True:
                break
            await asyncio.sleep(0.01)
        assert live["status"] == "RUNNING"
        assert live["network_connected"] is True
        assert live["network_status"] == "ALL_MARKETS_READY"
        runtime.request_stop("SIGTERM")
        await task
        assert all(collector.stopped for collector in collectors.values())
        assert power.started is True
        assert power.stopped is True
        final = runtime.state_store.read()
        assert final is not None
        assert final["status"] == "STOPPED"
        assert service_status(tmp_path)["status"] == "NOT_RUNNING"

    asyncio.run(exercise())
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        assert len(catalog.operational_events(event_type="SERVICE_STARTED")) == 1
        assert len(catalog.operational_events(event_type="SERVICE_STOPPED")) == 1
        gaps = catalog.operational_events(event_type="SYSTEM_SLEEP_GAP")
        assert len(gaps) == 1
        evidence = cast(dict[str, object], gaps[0]["evidence"])
        assert evidence["gap_marked"] is True


def test_vps_startup_hard_reserve_recovers_then_stops_cleanly_without_collectors(
    tmp_path: Path,
) -> None:
    usage = SimpleNamespace(
        total=40 * 1024**3,
        used=30 * 1024**3,
        free=HARD_RESERVE_BYTES,
    )
    factory_called = False

    def factory(
        _config: RecorderConfig,
        _logger: logging.Logger,
        _version: str,
        _instance_id: str,
    ) -> Mapping[str, RuntimeCollector]:
        nonlocal factory_called
        factory_called = True
        return {}

    runtime = ServiceRuntime(
        config=RecorderConfig(
            data_root=tmp_path,
            capacity_profile="vps-production-v1",
            heartbeat_seconds=1.0,
        ),
        logger=configure_logging(stream=io.StringIO()),
        collector_factory=factory,
        sleep_observer_factory=FakeSleepObserver,
        power_assertion=FakePowerAssertion(),
        deployment_identity=_vps_identity(),
        disk_usage=lambda _path: usage,
    )

    asyncio.run(runtime.run())

    assert factory_called is False
    final = runtime.state_store.read()
    assert final is not None
    assert final["status"] == "STOPPED"
    assert final["shutdown_reason"] == "HARD_RESERVE_SAFETY_STOP"
    assert final["startup_recovery_complete"] is True
    capacity = cast(dict[str, object], final["capacity"])
    assert capacity["actual_hard_reserve_reached"] is True
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        events = catalog.operational_events(event_type="DISK_EMERGENCY_STOP")
        open_gaps = catalog.unclosed_stream_discontinuities_by_stream()
    assert len(events) == 1
    evidence = cast(dict[str, object], events[0]["evidence"])
    assert evidence["gap_start_at_utc_ns"] == events[0]["occurred_at_utc_ns"]
    assert evidence["unarchived_raw_deleted"] is False
    assert evidence["termination"] == "HARD_RESERVE_SAFETY_STOP"
    assert set(open_gaps) == {
        (market, stream)
        for market in ("spot", "um_perpetual")
        for stream in ("agg_trade", "book_ticker", "diff_depth")
    }
    assert all(
        cast(dict[str, object], rows[0]["evidence"])["reason"]
        == "session_restart"
        for rows in open_gaps.values()
    )

    repeated = ServiceRuntime(
        config=RecorderConfig(
            data_root=tmp_path,
            capacity_profile="vps-production-v1",
            heartbeat_seconds=1.0,
        ),
        logger=configure_logging(stream=io.StringIO()),
        collector_factory=factory,
        sleep_observer_factory=FakeSleepObserver,
        power_assertion=FakePowerAssertion(),
        deployment_identity=_vps_identity(),
        disk_usage=lambda _path: usage,
    )
    asyncio.run(repeated.run())
    assert factory_called is False
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        assert len(catalog.operational_events(event_type="DISK_EMERGENCY_STOP")) == 1
        repeated_gaps = catalog.unclosed_stream_discontinuities_by_stream()
    assert all(len(rows) == 1 for rows in repeated_gaps.values())


def test_vps_runtime_hard_reserve_seals_via_graceful_stop_and_exits_cleanly(
    tmp_path: Path,
) -> None:
    observations = [
        SimpleNamespace(
            total=40 * 1024**3,
            used=29 * 1024**3,
            free=11 * 1024**3,
        ),
        SimpleNamespace(
            total=40 * 1024**3,
            used=30 * 1024**3,
            free=HARD_RESERVE_BYTES,
        ),
    ]

    def disk_usage(_path: Path) -> object:
        return observations.pop(0) if len(observations) > 1 else observations[0]

    async def exercise() -> None:
        collectors = {
            "spot": FakeCollector("spot", "service-spot"),
            "um_perpetual": FakeCollector("um_perpetual", "service-um"),
        }

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            assert runtime._startup_recovery_complete is True
            return collectors

        runtime = ServiceRuntime(
            config=RecorderConfig(
                data_root=tmp_path,
                capacity_profile="vps-production-v1",
                heartbeat_seconds=1.0,
            ),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
            deployment_identity=_vps_identity(),
            disk_usage=disk_usage,
            capacity_poll_seconds=0.01,
        )
        await runtime.run()

        assert all(
            collector.started and collector.stopped
            for collector in collectors.values()
        )
        final = runtime.state_store.read()
        assert final is not None
        assert final["status"] == "STOPPED"
        assert final["shutdown_reason"] == "HARD_RESERVE_SAFETY_STOP"

    asyncio.run(exercise())
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        assert len(catalog.operational_events(event_type="DISK_EMERGENCY_STOP")) == 1
        stopped = catalog.operational_events(event_type="SERVICE_STOPPED")
    assert len(stopped) == 1
    assert cast(dict[str, object], stopped[0]["evidence"])["reason"] == (
        "HARD_RESERVE_SAFETY_STOP"
    )


def test_all_market_failures_make_service_failed_for_launchd_restart(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        collectors = {
            "spot": FakeCollector("spot", "failed-spot", fail=True),
            "um_perpetual": FakeCollector(
                "um_perpetual", "failed-um", fail=True
            ),
        }

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            return collectors

        runtime = ServiceRuntime(
            config=_config(tmp_path),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
        )
        with pytest.raises(RuntimeError, match="core market Collector terminated"):
            await runtime.run()
        state = runtime.state_store.read()
        assert state is not None
        assert state["status"] == "FAILED"

    asyncio.run(exercise())
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        assert len(catalog.operational_events(event_type="SERVICE_FAILED")) == 1
        assert len(
            catalog.operational_events(event_type="CORE_MARKET_TERMINAL_FAILURE")
        ) == 1


def test_normally_returning_core_marks_service_failed_and_stops_peer(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        returning = FakeCollector(
            "spot", "returning-spot", return_early=True
        )
        healthy = FakeCollector("um_perpetual", "healthy-um")

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            return {"spot": returning, "um_perpetual": healthy}

        runtime = ServiceRuntime(
            config=_config(tmp_path),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
        )
        with pytest.raises(RuntimeError, match="core market Collector terminated"):
            await runtime.run()
        assert healthy.stopped
        state = runtime.state_store.read()
        assert state is not None and state["status"] == "FAILED"

    asyncio.run(exercise())
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        failures = catalog.operational_events(
            event_type="CORE_MARKET_TERMINAL_FAILURE"
        )
        assert len(failures) == 1
        evidence = cast(dict[str, object], failures[0]["evidence"])
        assert evidence["market"] == "spot"
        assert evidence["error_type"] == "RuntimeError"


def test_failed_runtime_state_write_order_cannot_be_overwritten_by_heartbeat(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        initial_running_published = threading.Event()
        running_contention_started = threading.Event()
        release_running = threading.Event()
        permit_collector_return = asyncio.Event()
        failed_store_write_started = asyncio.Event()
        published_statuses: list[str] = []
        published_statuses_lock = threading.Lock()
        loop = asyncio.get_running_loop()

        class SignalingCollector(FakeCollector):
            async def run(self, stop: asyncio.Event) -> None:
                self.started = True
                await permit_collector_return.wait()

        class ControlledStateStore(ServiceStateStore):
            def __init__(self, delegate: ServiceStateStore) -> None:
                super().__init__(delegate.path)
                self.delegate = delegate

            def write(self, document: Mapping[str, object]) -> None:
                status = str(document["status"])
                if status == "RUNNING" and not initial_running_published.is_set():
                    self.delegate.write(document)
                    initial_running_published.set()
                else:
                    if status == "RUNNING":
                        running_contention_started.set()
                        if not release_running.wait(timeout=5):
                            raise AssertionError("running write was not released")
                    elif status == "FAILED":
                        loop.call_soon_threadsafe(failed_store_write_started.set)
                    self.delegate.write(document)
                with published_statuses_lock:
                    published_statuses.append(status)

        returning = SignalingCollector("spot", "returning-spot")
        healthy = FakeCollector("um_perpetual", "healthy-um")

        def factory(
            _config: RecorderConfig,
            _logger: logging.Logger,
            _version: str,
            _instance_id: str,
        ) -> Mapping[str, RuntimeCollector]:
            return {"spot": returning, "um_perpetual": healthy}

        runtime = ServiceRuntime(
            config=_config(tmp_path),
            logger=configure_logging(stream=io.StringIO()),
            collector_factory=factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
        )
        runtime.state_store = ControlledStateStore(runtime.state_store)

        class ObservedStateWriteLock:
            def __init__(self) -> None:
                self._lock = asyncio.Lock()
                self.failed_waiter_seen = asyncio.Event()

            async def __aenter__(self) -> ObservedStateWriteLock:
                if self._lock.locked() and runtime._status == "FAILED":
                    self.failed_waiter_seen.set()
                await self._lock.acquire()
                return self

            async def __aexit__(self, *args: object) -> None:
                self._lock.release()

        observed_lock = ObservedStateWriteLock()
        runtime._state_write_lock = cast(Any, observed_lock)
        task = asyncio.create_task(runtime.run())
        assert await asyncio.to_thread(running_contention_started.wait, 5)
        assert initial_running_published.is_set()
        permit_collector_return.set()

        failed_waiter_task = asyncio.create_task(observed_lock.failed_waiter_seen.wait())
        failed_store_task = asyncio.create_task(failed_store_write_started.wait())
        done, pending = await asyncio.wait(
            {failed_waiter_task, failed_store_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for pending_task in pending:
            pending_task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        if failed_store_task in done:
            raise AssertionError(
                "FAILED publication reached the store before serialized contention"
            )
        assert failed_waiter_task in done
        assert failed_waiter_task.result()
        assert not failed_store_write_started.is_set()

        release_running.set()
        with pytest.raises(RuntimeError, match="core market Collector terminated"):
            await task

        assert healthy.stopped
        state = runtime.state_store.read()
        assert state is not None and state["status"] == "FAILED"
        with published_statuses_lock:
            statuses = list(published_statuses)
        assert statuses.count("RUNNING") == 2
        failure_index = statuses.index("FAILED")
        assert all(status != "RUNNING" for status in statuses[failure_index + 1 :])

    asyncio.run(exercise())
