from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.service.runtime import RuntimeCollector, ServiceRuntime
from binance_market_data_recorder.storage.catalog import Catalog
from tests.unit.test_service_runtime import (
    FakeCollector,
    FakePowerAssertion,
    FakeSleepObserver,
    _config,
)


@pytest.mark.parametrize("phase", ["startup", "running"])
def test_ordinary_profile_enforces_real_reserve_without_deleting_raw(
    tmp_path: Path,
    phase: str,
) -> None:
    async def scenario() -> None:
        collector = FakeCollector("spot", "p3")
        low = phase == "startup"

        def disk(path: Path) -> Any:
            return SimpleNamespace(total=100 * 1024**3, free=(1 if low else 80) * 1024**3)

        runtime = ServiceRuntime(
            config=_config(tmp_path),
            logger=logging.getLogger("p3.capacity"),
            collector_factory=lambda *args: {
                ProductKey("spot", "BTCUSDT"): cast(RuntimeCollector, collector)
            },
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
            disk_usage=disk,
            capacity_poll_seconds=0.01,
        )
        saved = runtime.layout.sealed / "retained-unarchived-evidence"
        saved.write_bytes(b"do not delete")
        task = asyncio.create_task(runtime.run())
        if phase == "running":

            async def started() -> None:
                while not collector.started:
                    if task.done():
                        task.result()
                    await asyncio.sleep(0.001)

            await asyncio.wait_for(started(), 3)
            low = True
        await asyncio.wait_for(task, 5)
        assert runtime.shutdown_reason == "HARD_RESERVE_SAFETY_STOP"
        assert saved.read_bytes() == b"do not delete"
        if phase == "startup":
            assert not collector.started
        else:
            assert collector.stopped
        with Catalog(runtime.layout.catalog) as catalog:
            events = catalog.operational_events(event_type="DISK_EMERGENCY_STOP")
            assert len(events) == 1
            evidence = events[0]["evidence"]
            assert isinstance(evidence, dict) and evidence["unarchived_raw_deleted"] is False

    asyncio.run(scenario())
