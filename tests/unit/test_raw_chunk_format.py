from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    ScanIssue,
    scan_chunk,
)
from binance_market_data_recorder.spool.writer import (
    RawChunkWriter,
    _rotation_deadline,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event


def _written_chunk(tmp_path: Path, count: int = 3) -> tuple[Path, Catalog]:
    layout = ensure_storage_layout(tmp_path)
    catalog = Catalog(layout.catalog)
    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        collector_instance_id="collector-1",
        collector_version="0.1.0+test",
        durability_interval_seconds=0,
    )
    for ordinal in range(count):
        writer.append(event(ordinal))
    writer.close()
    return writer.path, catalog


def test_scan_valid_chunk_is_bounded_and_reports_ranges(tmp_path: Path) -> None:
    path, catalog = _written_chunk(tmp_path)
    try:
        result = scan_chunk(path)
        assert result.is_clean
        assert result.statistics.record_count == 3
        assert result.statistics.sequence_ranges()["u"] == {"min": 0, "max": 2}
        assert result.valid_end == result.file_size
        assert result.uncompressed_sha256 is not None
    finally:
        catalog.close()


def test_checksum_corruption_is_not_treated_as_truncatable_tail(tmp_path: Path) -> None:
    path, catalog = _written_chunk(tmp_path, count=1)
    try:
        body = bytearray(path.read_bytes())
        body[-1] ^= 0x01
        path.write_bytes(body)
        result = scan_chunk(path)
        assert result.issue is ScanIssue.CHECKSUM_FAILURE
        assert not result.is_tail_truncatable
    finally:
        catalog.close()


def test_incomplete_frame_prefix_is_tail_truncatable(tmp_path: Path) -> None:
    path, catalog = _written_chunk(tmp_path, count=1)
    try:
        with path.open("ab") as target:
            target.write(b"\x00" * (FRAME_PREFIX.size - 1))
        result = scan_chunk(path)
        assert result.issue is ScanIssue.TRUNCATED_TAIL
        assert result.is_tail_truncatable
    finally:
        catalog.close()


def test_time_rotation_and_idle_durability_are_explicit(tmp_path: Path) -> None:
    path, catalog = _written_chunk(tmp_path, count=0)
    try:
        # The helper has closed its writer; create a live writer for clock gates.
        layout = ensure_storage_layout(tmp_path / "second")
        second_catalog = Catalog(layout.catalog)
        writer = RawChunkWriter(
            layout=layout,
            catalog=second_catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-1",
            collector_version="0.1.0+test",
            durability_interval_seconds=1,
        )
        writer.append(event(1))
        assert writer.sync_if_due(now_monotonic=10**20)
        assert writer.should_rotate(now_monotonic=10**20)
        writer.close()
        second_catalog.close()
        assert path.exists()
    finally:
        catalog.close()


def test_writer_rejects_unreadable_frame_bound_before_creating_file(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog, pytest.raises(ValueError, match="max frame"):
        RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-1",
            collector_version="0.1.0+test",
            max_frame_bytes=65 * 1024 * 1024,
        )
    assert not list(layout.active.iterdir())


def test_stream_rotation_deadlines_are_bounded_and_phase_staggered() -> None:
    opened = 1_000.0
    period = 60.0
    deadlines = {
        _rotation_deadline(
            opened_monotonic=opened,
            period_seconds=period,
            market=market,
            stream=stream,
        )
        for market, stream in (
            ("spot", "book_ticker"),
            ("spot", "agg_trade"),
            ("spot", "diff_depth"),
            ("um_perpetual", "book_ticker"),
            ("um_perpetual", "agg_trade"),
            ("um_perpetual", "diff_depth"),
        )
    }
    assert len(deadlines) == 6
    assert all(opened < deadline <= opened + period for deadline in deadlines)
