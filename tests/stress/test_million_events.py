from __future__ import annotations

import tracemalloc
from pathlib import Path

import pytest

from binance_market_data_recorder.spool.format import scan_chunk
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event

pytestmark = pytest.mark.stress


def test_one_million_synthetic_frames_use_bounded_python_memory(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    envelope = event(1, payload=b"{}")
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="million-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(bytes=1024 * 1024 * 1024),
            durability_interval_seconds=1,
        )
        tracemalloc.start()
        for _ in range(1_000_000):
            writer.append(envelope)
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        writer.close()

        assert writer.record_count == 1_000_000
        assert peak < 64 * 1024 * 1024
        result = scan_chunk(writer.path)
        assert result.is_clean
        assert result.statistics.record_count == 1_000_000
