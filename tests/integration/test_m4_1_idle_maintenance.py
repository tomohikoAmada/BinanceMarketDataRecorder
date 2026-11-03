from __future__ import annotations

import asyncio
import logging
import threading
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from binance_market_data_recorder.binance.public_sources import Source
from binance_market_data_recorder.binance.public_websocket import (
    PublicWebSocketStream,
    ReceivedFrame,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.unit.test_p3_availability import owner_at


def test_filtered_and_idle_ws_avoid_worker_jobs_but_sync_and_rotate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            spool = StreamSpool(
                layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT",
                stream="diff_depth", collector_instance_id="m41", collector_version="test",
                queue_capacity=16, rotation=RotationPolicy(), durability_interval_seconds=1,
                max_frame_bytes=1024 * 1024,
            )
            calls = 0
            drain = spool.drain_all

            def tracked_drain() -> int:
                nonlocal calls
                calls += 1
                return drain()

            monkeypatch.setattr(spool, "drain_all", tracked_drain)

            collector = PublicWebSocketStream(
                stream="diff_depth", symbol="BTCUSDT", wire_name="unused", spool=spool,
                collector_instance_id="m41", collector_version="test",
                logger=logging.getLogger("test"),
                opener=MagicMock(side_effect=AssertionError("offline writer test")),
                envelope_factory=lambda **kwargs: None,
            )
            receipt = ReceivedFrame(b"forming", "connection", time.time_ns(), time.monotonic_ns())
            for _ in range(100):
                assert await collector._persist_batch([receipt]) == []
            assert calls == 0
            assert not list(layout.active.iterdir())

            # A real tail must still sync at 1s even if all following frames are filtered.
            spool.enqueue(event(1))
            drain()
            assert spool.persistence_progress().synchronized_frames == 0
            assert await collector._persist_batch([receipt]) == []
            assert calls == 0
            await asyncio.sleep(1.05)
            assert await collector._persist_batch([receipt]) == []
            assert calls == 1
            assert spool.persistence_progress().synchronized_frames == 1
            assert not spool.maintenance_due()

            # A clean idle stream creates no thread jobs on its 100ms polls.
            done = asyncio.Event()
            task = asyncio.create_task(collector._write_until_done(done))
            await asyncio.sleep(0.35)
            assert calls == 1
            done.set()
            await task
            assert len(list(layout.sealed.glob("*.zst"))) == 1

            # Rotation must run for an idle stream even after its tail is synced.
            spool.enqueue(event(2))
            drain()
            assert spool._writer is not None
            spool._writer._rotation_deadline_monotonic = time.monotonic() - 1
            assert spool.maintenance_due()
            before_rotation = calls
            assert await collector._persist_batch([receipt]) == []
            assert calls == before_rotation + 1
            assert not spool.maintenance_due()
            assert len(list(layout.sealed.glob("*.zst"))) == 2
            assert not list(layout.active.glob("*.partial"))

    asyncio.run(scenario())


def test_health_monitor_continues_while_catalog_worker_waits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        entered, release = threading.Event(), threading.Event()
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            owner._active_sources.clear()  # No network tasks in this offline run.
            source = Source(
                family="S01", venue_market="spot", market="spot", symbol="BTCUSDT",
                stream="test", transport="ws", endpoint="/ws/test", cadence_seconds=1,
            )
            owner.add_source(source)
            owner.sources[source.key].connected = True
            persist = catalog.persist_source_availability

            def delayed(
                batch: list[dict[str, Any]], *, watermark: tuple[str, int] | None = None,
            ) -> None:
                if not entered.is_set():
                    entered.set()
                    if not release.wait(5):
                        raise TimeoutError("test did not release Catalog worker")
                persist(batch, watermark=watermark)

            monkeypatch.setattr(catalog, "persist_source_availability", delayed)
            stop = asyncio.Event()
            task = asyncio.create_task(owner.run(stop))
            try:
                async def wait_for_worker() -> None:
                    while not entered.is_set():
                        await asyncio.sleep(0.01)

                await asyncio.wait_for(wait_for_worker(), 2)
                before = owner._last_monitor_utc_ns
                assert before is not None
                await asyncio.sleep(1.1)
                assert owner._last_monitor_utc_ns is not None
                assert owner._last_monitor_utc_ns > before
                assert not task.done()
            finally:
                stop.set()
                release.set()
                await asyncio.wait_for(task, 3)
            assert not owner.availability.pending

    asyncio.run(scenario())


def test_health_monitor_failure_is_owned_and_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            owner._active_sources.clear()

            def fail(now: int) -> None:
                raise RuntimeError("injected health monitor failure")

            monkeypatch.setattr(owner, "_observe_availability", fail)
            with pytest.raises(RuntimeError, match="injected health monitor failure"):
                await asyncio.wait_for(owner.run(asyncio.Event()), 3)
            assert not owner.availability.pending

    asyncio.run(scenario())
