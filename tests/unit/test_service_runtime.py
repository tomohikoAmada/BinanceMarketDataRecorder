from __future__ import annotations

import asyncio
import io
import logging
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import Market
from binance_market_data_recorder.logging import configure_logging
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
from binance_market_data_recorder.status import service_status
from binance_market_data_recorder.storage.catalog import Catalog
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


def _config(tmp_path: Path) -> RecorderConfig:
    return RecorderConfig(
        data_root=tmp_path,
        heartbeat_seconds=1.0,
        sleep_gap_threshold_seconds=5.0,
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
        heartbeat_started = threading.Event()
        release_heartbeat = threading.Event()
        collector_returned = asyncio.Event()
        published_statuses: list[str] = []
        published_statuses_lock = threading.Lock()

        class SignalingCollector(FakeCollector):
            async def run(self, stop: asyncio.Event) -> None:
                self.started = True
                collector_returned.set()

        class ControlledStateStore(ServiceStateStore):
            def __init__(self, delegate: ServiceStateStore) -> None:
                super().__init__(delegate.path)
                self.delegate = delegate
                self.write_count = 0
                self.write_count_lock = threading.Lock()

            def write(self, document: Mapping[str, object]) -> None:
                with self.write_count_lock:
                    self.write_count += 1
                    write_number = self.write_count
                status = str(document["status"])
                if write_number == 2:
                    heartbeat_started.set()
                    if not release_heartbeat.wait(timeout=5):
                        raise AssertionError("heartbeat write was not released")
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
        task = asyncio.create_task(runtime.run())
        assert await asyncio.to_thread(heartbeat_started.wait, 5)
        await collector_returned.wait()
        release_heartbeat.set()
        with pytest.raises(RuntimeError, match="core market Collector terminated"):
            await task

        assert healthy.stopped
        state = runtime.state_store.read()
        assert state is not None and state["status"] == "FAILED"
        with published_statuses_lock:
            statuses = list(published_statuses)
        failure_index = max(
            index for index, status in enumerate(statuses) if status == "FAILED"
        )
        assert all(status != "RUNNING" for status in statuses[failure_index + 1 :])

    asyncio.run(exercise())
