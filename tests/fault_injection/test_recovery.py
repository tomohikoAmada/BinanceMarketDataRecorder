from __future__ import annotations

import os
from pathlib import Path
from typing import Any, cast

from binance_market_data_recorder.spool.format import FRAME_PREFIX, scan_chunk
from binance_market_data_recorder.spool.recovery import recover_partials
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import StorageLayout, ensure_storage_layout
from tests.factories import event


def _partial(tmp_path: Path) -> tuple[StorageLayout, RawChunkWriter, Catalog]:
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
    writer.append(event(1))
    writer.close()
    return layout, writer, catalog


def test_truncated_tail_recovers_once_and_marks_manifest_incomplete(tmp_path: Path) -> None:
    layout, writer, catalog = _partial(tmp_path)
    try:
        original_size = writer.path.stat().st_size
        with writer.path.open("ab") as target:
            target.write(FRAME_PREFIX.pack(100, 0, 0, 0) + b"abc")
            target.flush()
            os.fsync(target.fileno())

        first = recover_partials(layout=layout, catalog=catalog)
        second = recover_partials(layout=layout, catalog=catalog)
        assert first[0].action == "tail_truncated"
        assert second[0].action == "unchanged"
        assert writer.path.stat().st_size == original_size
        assert scan_chunk(writer.path).is_clean
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.RECOVERED
        transition_count = catalog.transition_count(str(writer.header.chunk_id))

        third = recover_partials(layout=layout, catalog=catalog)
        assert third[0].action == "unchanged"
        assert catalog.transition_count(str(writer.header.chunk_id)) == transition_count

        manifest = seal_partial(writer.path, layout=layout, catalog=catalog)
        assert manifest["recovered"] is True
        assert manifest["complete"] is False
        recovery = cast(dict[str, Any], manifest["recovery"])
        assert recovery["truncated_bytes"] == FRAME_PREFIX.size + 3
    finally:
        catalog.close()


def test_checksum_corruption_is_quarantined_not_truncated(tmp_path: Path) -> None:
    layout, writer, catalog = _partial(tmp_path)
    try:
        body = bytearray(writer.path.read_bytes())
        body[-1] ^= 0x80
        writer.path.write_bytes(body)
        actions = recover_partials(layout=layout, catalog=catalog)

        assert actions[0].action == "quarantined"
        assert not writer.path.exists()
        assert len(list(layout.quarantine.glob("*.quarantine"))) == 1
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.QUARANTINED
        assert recover_partials(layout=layout, catalog=catalog) == []
    finally:
        catalog.close()


def test_invalid_header_is_quarantined_without_claiming_a_chunk(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    bad = layout.active / "unknown.bmdr.partial"
    bad.write_bytes(b"not-a-valid-header")
    with Catalog(layout.catalog) as catalog:
        actions = recover_partials(layout=layout, catalog=catalog)
        assert actions[0].action == "quarantined"
        assert not bad.exists()
