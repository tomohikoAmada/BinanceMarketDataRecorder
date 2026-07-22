from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.collector.spot import SpotCollector, SpotCollectorSettings

pytestmark = pytest.mark.online


@pytest.mark.skipif(
    os.environ.get("BINANCE_MARKET_RECORDER_ONLINE") != "1",
    reason="set BINANCE_MARKET_RECORDER_ONLINE=1 for unsigned public Spot smoke",
)
def test_live_spot_capture_for_at_least_fifteen_minutes(tmp_path: Path) -> None:
    duration = int(os.environ.get("BINANCE_MARKET_RECORDER_LIVE_SECONDS", "900"))
    assert duration >= 900, "M4 acceptance requires at least 900 seconds"

    async def exercise() -> None:
        stop = asyncio.Event()
        collector = SpotCollector(
            SpotCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="m4-live-smoke",
                collector_version="0.1.0+m4-live",
            ),
            logger=logging.getLogger("m4.live.spot"),
        )
        task = asyncio.create_task(collector.run(stop))
        timer = asyncio.create_task(asyncio.sleep(duration))
        try:
            done, _pending = await asyncio.wait(
                {task, timer}, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                await task
                raise AssertionError("Spot Collector returned before the live window elapsed")
        finally:
            stop.set()
            timer.cancel()
        await task

    asyncio.run(exercise())
    documents: list[dict[str, Any]] = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "data" / "manifests").glob("*.json")
    ]
    counts: dict[str, int] = {}
    for document in documents:
        stream = str(document["stream"])
        counts[stream] = counts.get(stream, 0) + int(document["record_count"])
    print(json.dumps({"duration_seconds": duration, "records": counts}, sort_keys=True))
    assert counts["diff_depth"] > 0
    assert counts["agg_trade"] > 0
    assert counts["book_ticker"] > 0
    assert counts["depth_snapshot"] == 1
