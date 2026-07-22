from __future__ import annotations

import json
from pathlib import Path

import zstandard

from binance_market_data_recorder.spool.seal import seal_partial, validate_sealed_artifact
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event


def test_verified_seal_manifest_and_catalog_are_consistent(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
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
        writer.append(event(10))
        writer.append(event(11, flags=("sequence_gap",)))
        writer.close()

        manifest = seal_partial(writer.path, layout=layout, catalog=catalog)
        sealed = layout.root / str(manifest["relative_path"])
        manifest_path = layout.manifests / f"{writer.header.chunk_id.hex}.manifest.json"

        assert not writer.path.exists()
        assert sealed.is_file()
        assert manifest_path.is_file()
        assert manifest["record_count"] == 2
        assert manifest["complete"] is False
        assert manifest["gap"] is True
        assert manifest["stored_bytes"] == sealed.stat().st_size
        frame_parameters = zstandard.get_frame_parameters(sealed.read_bytes())
        assert frame_parameters.content_size == manifest["uncompressed_bytes"]
        assert frame_parameters.has_checksum
        validate_sealed_artifact(sealed, manifest)
        assert json.loads(manifest_path.read_text()) == manifest

        row = catalog.chunk(str(writer.header.chunk_id))
        assert row is not None
        assert row["state"] == ChunkState.SEALED
        assert row["record_count"] == 2
        assert row["stored_sha256"] == manifest["stored_sha256"]


def test_catalog_contains_metadata_not_market_event_payloads(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        columns = catalog.table_columns("chunks")
        assert not columns & {"raw_payload", "event_payload", "payload", "event_body"}
        assert {"chunk_id", "state", "record_count", "stored_sha256"} <= columns
        checkpoint_columns = catalog.table_columns("orderbook_checkpoints")
        assert not checkpoint_columns & {"bids", "asks", "levels", "event_payload"}
        assert {"checkpoint_id", "market", "update_id", "book_hash"} <= checkpoint_columns
