from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.collector.spot import (
    SpotCollector,
    SpotCollectorSettings,
)
from binance_market_data_recorder.collector.supervisor import MarketCollectorSupervisor
from binance_market_data_recorder.collector.usdm import (
    UsdMCollector,
    UsdMCollectorSettings,
)
from binance_market_data_recorder.spool.seal import validate_sealed_artifact
from binance_market_data_recorder.status import service_status

pytestmark = pytest.mark.online


@pytest.mark.skipif(
    os.environ.get("BINANCE_MARKET_RECORDER_PREVIEW_SMOKE") != "1",
    reason="set BINANCE_MARKET_RECORDER_PREVIEW_SMOKE=1 for the public preview smoke",
)
def test_developer_preview_spot_and_usdm_smoke(tmp_path: Path) -> None:
    """Exercise only public BTCUSDT endpoints in an isolated data root."""

    duration = int(os.environ.get("BINANCE_MARKET_RECORDER_LIVE_SECONDS", "300"))
    assert 300 <= duration <= 900, "M18 preview smoke must run for 5 to 15 minutes"

    async def exercise() -> tuple[dict[str, object], dict[str, object]]:
        stop = asyncio.Event()
        spot = SpotCollector(
            SpotCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="m18-preview-spot",
                collector_version="0.1.0a1",
            ),
            logger=logging.getLogger("m18.preview.spot"),
        )
        usdm = UsdMCollector(
            UsdMCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="m18-preview-usdm",
                collector_version="0.1.0a1",
                side_data=None,
            ),
            logger=logging.getLogger("m18.preview.usdm"),
        )
        supervisor = MarketCollectorSupervisor(
            {"spot": spot, "um_perpetual": usdm}
        )
        task = asyncio.create_task(supervisor.run(stop))
        timer = asyncio.create_task(asyncio.sleep(duration))
        try:
            done, _pending = await asyncio.wait(
                {task, timer}, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                await task
                raise AssertionError(
                    "core Collectors returned before the preview smoke elapsed"
                )
            spot_ready = spot.readiness_snapshot().public_dict()
            usdm_ready = usdm.readiness_snapshot().public_dict()
            assert spot_ready["ready"] is True
            assert usdm_ready["ready"] is True
            assert supervisor.failures == {}
            return spot_ready, usdm_ready
        finally:
            stop.set()
            timer.cancel()
            await asyncio.gather(timer, return_exceptions=True)
            await task

    spot_readiness, usdm_readiness = asyncio.run(exercise())
    documents: list[dict[str, Any]] = []
    counts: dict[str, dict[str, int]] = {}
    for manifest_path in sorted((tmp_path / "data" / "manifests").glob("*.json")):
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
        documents.append(document)
        sealed = tmp_path / str(document["relative_path"])
        validate_sealed_artifact(sealed, document)
        market = str(document["market"])
        stream = str(document["stream"])
        streams = counts.setdefault(market, {})
        streams[stream] = streams.get(stream, 0) + int(document["record_count"])

    for market in ("spot", "um_perpetual"):
        assert counts[market]["diff_depth"] > 0
        assert counts[market]["agg_trade"] > 0
        assert counts[market]["book_ticker"] > 0
        assert counts[market]["depth_snapshot"] >= 1

    status = service_status(tmp_path)
    assert status["status"] == "NOT_RUNNING"
    assert status["catalog"]["available"] is True
    assert status["catalog"]["sealed_chunks"] == len(documents)
    print(
        json.dumps(
            {
                "duration_seconds": duration,
                "records": counts,
                "sealed_chunks": len(documents),
                "spot_readiness": spot_readiness,
                "status_after_stop": status["status"],
                "usdm_readiness": usdm_readiness,
            },
            sort_keys=True,
        )
    )
