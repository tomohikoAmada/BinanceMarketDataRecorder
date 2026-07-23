from __future__ import annotations

import asyncio
import io
import logging
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
from binance_market_data_recorder.service.runtime import RuntimeCollector, ServiceRuntime
from binance_market_data_recorder.status import service_status
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.supervisor import ReadinessSnapshot


class FakeCollector:
    def __init__(
        self, market: Market, instance_id: str, *, fail: bool = False
    ) -> None:
        self.market = market
        self.instance_id = instance_id
        self.fail = fail
        self.started = False
        self.stopped = False

    async def run(self, stop: asyncio.Event) -> None:
        self.started = True
        if self.fail:
            raise RuntimeError("injected collector failure")
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
        await asyncio.sleep(0)
        live = service_status(tmp_path)
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
        with pytest.raises(RuntimeError, match="all core market"):
            await runtime.run()
        state = runtime.state_store.read()
        assert state is not None
        assert state["status"] == "FAILED"

    asyncio.run(exercise())
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        assert len(catalog.operational_events(event_type="SERVICE_FAILED")) == 1
