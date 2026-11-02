from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_sources import Source, product_sources
from binance_market_data_recorder.binance.public_websocket import StreamGroup
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_m1_3_kline_and_seed_retirement import DummyResponse, sample_product


def repair_capture(tmp_path: Path) -> tuple[SelectedAssetCapture, Source, MagicMock]:
    layout = ensure_storage_layout(tmp_path)
    capture = SelectedAssetCapture(
        RecorderConfig(data_root=tmp_path, coverage=CoverageConfig(underlying_assets=("BTC",))),
        MagicMock(),
        version="test",
        instance_id="repair",
        layout=layout,
        catalog=Catalog(layout.catalog),
    )
    source = next(
        s
        for s in product_sources(sample_product(), capture.coverage)
        if s.stream == "kline_1m" and s.transport == "ws"
    )
    capture.add_source(source)
    client = MagicMock()
    capture.clients["spot"] = client
    return capture, source, client


def bar(open_ms: int) -> list[Any]:
    return [open_ms, "1", "2", "1", "2", "10", open_ms + 59_999, "20", 1, "5", "10", "0"]


def test_reconnect_boundary(tmp_path: Path) -> None:
    async def scenario() -> None:
        capture, source, client = repair_capture(tmp_path)
        state = capture.sources[source.key]
        state.last_closed_open_time_ms = 0
        group = StreamGroup("", "", "test", (source,))
        with patch("time.time_ns", return_value=119_000_000_000):
            capture._shared_lifecycle(group, "connected")
        assert state.backfill_start_ms is None
        with patch("time.time_ns", return_value=121_000_000_000):
            capture._shared_lifecycle(group, "disconnected")
            capture._shared_lifecycle(group, "connected")
        client.request.return_value = DummyResponse([bar(60_000)])
        result = await capture._run_kline_backfill(state)
        assert state.backfill_start_ms is None
        assert result.valid
        assert "closed_bar_backfill" in result.envelope.capture_flags
        assert result.envelope.receive_time_utc_ns > 121_000_000_000
        saved = json.loads(result.envelope.raw_payload)
        assert saved["request"]["parameters"]["startTime"] == 60_000
        assert saved["request"]["parameters"]["endTime"] == 119_999
        assert saved["response"]["model"] == [bar(60_000)]

    asyncio.run(scenario())


def test_pages_retries_and_gap_merging(tmp_path: Path) -> None:
    async def scenario() -> None:
        capture, source, client = repair_capture(tmp_path)
        state = capture.sources[source.key]
        capture._schedule_kline_backfill(source, 0, 120 * 60_000 - 1, "1m")
        for document, status in (({"code": -1}, 500), ([], 200), ([bar(60_000)], 200)):
            client.request.return_value = DummyResponse(document, status=status)
            await capture._run_kline_backfill(state)
            assert state.backfill_start_ms == 0
            assert state.last_closed_open_time_ms is None
            assert state.backfill_due > time.monotonic()
        client.request.return_value = DummyResponse([bar(i * 60_000) for i in range(100)])
        await capture._run_kline_backfill(state)
        assert state.backfill_start_ms == 100 * 60_000
        capture._schedule_kline_backfill(source, 120 * 60_000, 122 * 60_000 - 1, "1m")
        client.request.return_value = DummyResponse([bar(i * 60_000) for i in range(100, 122)])
        await capture._run_kline_backfill(state)
        assert state.backfill_start_ms is None

    asyncio.run(scenario())


def test_storage_error_propagates(tmp_path: Path) -> None:
    async def scenario() -> None:
        capture, source, client = repair_capture(tmp_path)
        state = capture.sources[source.key]
        capture._schedule_kline_backfill(source, 0, 59_999, "1m")
        client.request.return_value = DummyResponse([bar(0)])
        capture.persist_rest = AsyncMock(side_effect=OSError("disk full"))  # type: ignore[method-assign]
        with pytest.raises(OSError, match="disk full"):
            await capture._run_kline_backfill(state)
        assert state.backfill_start_ms == 0

    asyncio.run(scenario())


def test_monthly_utc8_and_continuous_routing(tmp_path: Path) -> None:
    capture, source, _ = repair_capture(tmp_path)
    opened = int(datetime(2024, 1, 31, 16, tzinfo=UTC).timestamp() * 1000)
    following = int(datetime(2024, 2, 29, 16, tzinfo=UTC).timestamp() * 1000)
    assert capture._next_kline_open_time_ms(opened, "1M", utc8=True) == following
    continuous = replace(
        source,
        venue_market="usdm",
        symbol="BTCUSDT_CURRENT_QUARTER",
        stream="continuous_kline_1m",
        family="D22",
    )
    repaired = capture._build_kline_backfill_source(continuous, "1m", 0, 59_999)
    assert repaired.endpoint == "/fapi/v1/continuousKlines"
    assert dict(repaired.parameters)["pair"] == "BTCUSDT"
    assert dict(repaired.parameters)["contractType"] == "CURRENT_QUARTER"
    assert repaired.weight == 2


def test_seed_ignores_forming_bar(tmp_path: Path) -> None:
    async def scenario() -> None:
        capture, source, client = repair_capture(tmp_path)
        now = time.time_ns() // 1_000_000
        source = replace(source, stream="continuous_kline_1m")
        capture.add_source(source)
        seed = replace(
            source,
            stream="continuous_seed_1m",
            transport="rest",
            endpoint="/api/v3/klines",
            cadence_seconds=0,
        )
        capture.add_source(seed)
        client.request.return_value = DummyResponse([bar(0), bar(now)])
        await capture.poll_once(capture.sources[seed.key])
        assert capture.sources[source.key].last_closed_open_time_ms == 0
        assert seed.key not in capture._active_sources
        assert seed.key in capture.sources

    asyncio.run(scenario())


def test_scheduler_persists_repair_and_drains_on_stop(tmp_path: Path) -> None:
    from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks

    async def scenario() -> None:
        capture, source, client = repair_capture(tmp_path)
        capture.sources.clear()
        capture.add_source(source)
        state = capture.sources[source.key]
        state.connected = True
        state.last_closed_open_time_ms = 0
        capture._schedule_kline_backfill(source, 60_000, 119_999, "1m")
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def request(src: Source, parameters: dict[str, Any]) -> DummyResponse:
            loop.call_soon_threadsafe(stop.set)
            return DummyResponse([bar(60_000)])

        client.request.side_effect = request
        await asyncio.wait_for(capture.run(stop), 5)
        assert state.backfill_start_ms is None
        chunks = load_source_chunks(layout=capture.layout, catalog=capture.catalog)
        records = [r.envelope for chunk in chunks for r in iter_source_records(chunk)]
        assert len(records) == 1
        assert "closed_bar_backfill" in records[0].capture_flags
        assert json.loads(records[0].raw_payload)["response"]["model"] == [bar(60_000)]

    asyncio.run(scenario())
