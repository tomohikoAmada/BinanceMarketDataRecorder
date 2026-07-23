from __future__ import annotations

import asyncio
import logging
import multiprocessing
import os
import signal
import time
from collections.abc import Callable, Mapping
from pathlib import Path

from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import Market
from binance_market_data_recorder.logging import configure_logging
from binance_market_data_recorder.service.power import CaffeinateAssertion
from binance_market_data_recorder.service.runtime import RuntimeCollector, ServiceRuntime
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.supervisor import ReadinessSnapshot


class ProcessCollector:
    def __init__(self, market: Market, instance_id: str) -> None:
        self.market = market
        self.instance_id = instance_id
        self.started = False

    async def run(self, stop: asyncio.Event) -> None:
        self.started = True
        await stop.wait()

    def readiness_snapshot(self) -> ReadinessSnapshot:
        streams = frozenset({"diff_depth", "agg_trade", "book_ticker"})
        return ReadinessSnapshot(
            market=self.market,
            symbol="BTCUSDT",
            collector_instance_id=self.instance_id,
            collector_version="signal-test",
            connected_streams=streams if self.started else frozenset(),
            persisted_streams=streams if self.started else frozenset(),
            snapshot_persisted=self.started,
            orderbook_synchronized=self.started,
            event_count=int(self.started),
            last_receive_time_utc_ns=time.time_ns() if self.started else None,
            failure=None,
        )


class ProcessSleepObserver:
    def __init__(self, _callback: Callable[[str, int], None]) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


class ProcessPowerAssertion(CaffeinateAssertion):
    def __init__(self) -> None:
        super().__init__(enabled=False)


def _service_process(data_root: str) -> None:
    root = Path(data_root)

    def factory(
        _config: RecorderConfig,
        _logger: logging.Logger,
        _version: str,
        service_instance_id: str,
    ) -> Mapping[str, RuntimeCollector]:
        return {
            "spot": ProcessCollector("spot", f"{service_instance_id}-spot"),
            "um_perpetual": ProcessCollector(
                "um_perpetual", f"{service_instance_id}-um"
            ),
        }

    runtime = ServiceRuntime(
        config=RecorderConfig(
            data_root=root,
            heartbeat_seconds=1.0,
            sleep_gap_threshold_seconds=5.0,
        ),
        logger=configure_logging(),
        collector_factory=factory,
        sleep_observer_factory=ProcessSleepObserver,
        power_assertion=ProcessPowerAssertion(),
    )
    asyncio.run(runtime.run())


def _wait_running(state_path: Path, *, previous_instance: str | None = None) -> str:
    store = ServiceStateStore(state_path)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            state = store.read()
        except Exception:
            state = None
        if (
            state is not None
            and state.get("status") == "RUNNING"
            and isinstance(state.get("service_instance_id"), str)
            and state["service_instance_id"] != previous_instance
        ):
            return str(state["service_instance_id"])
        time.sleep(0.02)
    raise AssertionError("service child did not become RUNNING")


def test_sigterm_seals_service_and_sigkill_lock_is_restartable(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    state_path = tmp_path / "state" / "service_state.json"

    first = context.Process(target=_service_process, args=(str(tmp_path),))
    first.start()
    first_instance = _wait_running(state_path)
    assert first.pid is not None
    os.kill(first.pid, signal.SIGKILL)
    first.join(timeout=10)
    assert first.exitcode == -signal.SIGKILL

    restarted = context.Process(target=_service_process, args=(str(tmp_path),))
    restarted.start()
    restarted_instance = _wait_running(
        state_path, previous_instance=first_instance
    )
    assert restarted_instance != first_instance
    assert restarted.pid is not None
    os.kill(restarted.pid, signal.SIGTERM)
    restarted.join(timeout=10)
    assert restarted.exitcode == 0

    final = ServiceStateStore(state_path).read()
    assert final is not None
    assert final["status"] == "STOPPED"
    assert final["shutdown_reason"] == "SIGTERM"
    with Catalog(tmp_path / "state" / "catalog.sqlite") as catalog:
        assert len(catalog.operational_events(event_type="SERVICE_STARTED")) == 2
        assert len(catalog.operational_events(event_type="SERVICE_STOPPED")) == 1
