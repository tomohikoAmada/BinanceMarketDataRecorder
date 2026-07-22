from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.collector.spot import SpotCollector, SpotCollectorSettings
from binance_market_data_recorder.collector.supervisor import MarketCollectorSupervisor
from binance_market_data_recorder.collector.usdm import UsdMCollector, UsdMCollectorSettings

pytestmark = pytest.mark.online


@pytest.mark.skipif(
    os.environ.get("BINANCE_MARKET_RECORDER_ONLINE") != "1",
    reason="set BINANCE_MARKET_RECORDER_ONLINE=1 for unsigned public combined smoke",
)
def test_spot_and_usdm_run_together_for_at_least_thirty_minutes(tmp_path: Path) -> None:
    duration = int(os.environ.get("BINANCE_MARKET_RECORDER_LIVE_SECONDS", "1800"))
    assert duration >= 1800, "M5 acceptance requires at least 1800 seconds"

    async def exercise() -> MarketCollectorSupervisor:
        stop = asyncio.Event()
        supervisor = MarketCollectorSupervisor(
            {
                "spot": SpotCollector(
                    SpotCollectorSettings(
                        data_root=tmp_path,
                        collector_instance_id="m5-live-spot",
                        collector_version="0.1.0+m5-live",
                    ),
                    logger=logging.getLogger("m5.live.spot"),
                ),
                "um_perpetual": UsdMCollector(
                    UsdMCollectorSettings(
                        data_root=tmp_path,
                        collector_instance_id="m5-live-usdm",
                        collector_version="0.1.0+m5-live",
                    ),
                    logger=logging.getLogger("m5.live.usdm"),
                ),
            }
        )
        task = asyncio.create_task(supervisor.run(stop))
        await asyncio.sleep(duration)
        assert supervisor.failures == {}
        stop.set()
        await task
        return supervisor

    asyncio.run(exercise())
    documents: list[dict[str, Any]] = [
        json.loads(path.read_text()) for path in (tmp_path / "data" / "manifests").glob("*.json")
    ]
    counts: dict[str, dict[str, int]] = {}
    for document in documents:
        market = str(document["market"])
        stream = str(document["stream"])
        market_counts = counts.setdefault(market, {})
        market_counts[stream] = market_counts.get(stream, 0) + int(document["record_count"])
    print(json.dumps({"duration_seconds": duration, "records": counts}, sort_keys=True))
    for market in ("spot", "um_perpetual"):
        assert counts[market]["diff_depth"] > 0
        assert counts[market]["agg_trade"] > 0
        assert counts[market]["book_ticker"] > 0
        assert counts[market]["depth_snapshot"] == 1
