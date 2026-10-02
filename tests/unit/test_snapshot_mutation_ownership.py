"""Real capture coroutines retain storage ownership across sibling cancellation."""

from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.collector.spot import SpotCollector, SpotCollectorSettings
from binance_market_data_recorder.collector.usdm import UsdMCollector, UsdMCollectorSettings
from binance_market_data_recorder.collector.usdm_side_data import UsdMRestCooldown
from binance_market_data_recorder.spool.stream import StreamSpool
from tests.integration.test_spot_collector import RestApi as SpotApi
from tests.integration.test_usdm_collector import RestApi as UsdmApi


@pytest.mark.parametrize("market", ["spot", "usdm"])
@pytest.mark.parametrize("operation", ["drain_all", "sync"])
@pytest.mark.parametrize("io_failure", [False, True])
def test_snapshot_storage_finishes_before_cancellation_and_readiness(
    tmp_path: Path,
    market: str,
    operation: str,
    io_failure: bool,
) -> None:
    entered, release = threading.Event(), threading.Event()
    active = False
    synced = False

    class Spool:
        def enqueue(self, _envelope: Any) -> None:
            pass

        def drain_all(self) -> int:
            if operation == "drain_all":
                self.block()
            return 1

        def sync(self) -> None:
            nonlocal synced
            if operation == "sync":
                self.block()
            synced = True

        def block(self) -> None:
            nonlocal active
            active = True
            entered.set()
            try:
                assert release.wait(3)
                if io_failure:
                    raise OSError("snapshot durable write failed")
            finally:
                active = False

    async def exercise() -> None:
        kwargs: dict[str, Any] = dict(
            data_root=tmp_path,
            symbol="BTCUSDT",
            collector_instance_id="test",
            collector_version="test",
            durability_interval_seconds=1,
        )
        if market == "spot":
            collector: Any = SpotCollector(
                SpotCollectorSettings(**kwargs),
                logger=logging.getLogger("test.snapshot-owned"),
                rest_api=SpotApi(),
            )
        else:
            collector = UsdMCollector(
                UsdMCollectorSettings(**kwargs),
                logger=logging.getLogger("test.snapshot-owned"),
                request_lock=asyncio.Lock(),
                cooldown=UsdMRestCooldown(),
                rest_api=UsdmApi(),
            )
        original = collector.snapshot_spool
        collector.snapshot_spool = cast(StreamSpool, Spool())
        task = asyncio.create_task(collector._capture_snapshot(asyncio.Event()))
        try:
            for _ in range(1000):
                if entered.is_set():
                    break
                await asyncio.sleep(0.001)
            assert entered.is_set()
            for _ in range(3):
                task.cancel()
                await asyncio.sleep(0.005)
                assert active and not task.done()
                assert not collector.readiness_snapshot().snapshot_persisted
            release.set()
            expected = OSError if io_failure else asyncio.CancelledError
            with pytest.raises(expected):
                await asyncio.wait_for(task, 1)
            assert not active
            assert not collector.readiness_snapshot().snapshot_persisted
            assert synced == (operation == "sync" and not io_failure)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            original.close_and_seal()
            collector.catalog.close()

    asyncio.run(exercise())
