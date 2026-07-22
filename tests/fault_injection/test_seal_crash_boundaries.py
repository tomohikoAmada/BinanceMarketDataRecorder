from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.spool.recovery import reconcile_sealed, recover_storage
from binance_market_data_recorder.spool.seal import SealError, seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event


def test_catalog_failure_after_manifest_retains_source_and_retry_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
        writer.append(event(1))
        writer.close()
        original_transition = catalog.transition

        def fail_sealed(chunk_id: str, to_state: ChunkState, **kwargs: Any) -> None:
            if to_state is ChunkState.SEALED:
                raise RuntimeError("injected Catalog failure")
            original_transition(chunk_id, to_state, **kwargs)

        monkeypatch.setattr(catalog, "transition", fail_sealed)
        with pytest.raises(RuntimeError, match="injected Catalog failure"):
            seal_partial(writer.path, layout=layout, catalog=catalog)
        assert writer.path.exists()
        assert len(list(layout.sealed.glob("*.zst"))) == 1
        assert len(list(layout.manifests.glob("*.json"))) == 1

        monkeypatch.setattr(catalog, "transition", original_transition)
        actions = recover_storage(layout=layout, catalog=catalog)
        first = next(
            action for action in actions if action.action == "seal_completed_after_crash"
        )
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.SEALED
        assert not writer.path.exists()
        assert first.detail == str(writer.header.chunk_id)


def test_verified_manifest_reconciles_into_new_catalog(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    first_catalog = Catalog(layout.catalog)
    writer = RawChunkWriter(
        layout=layout,
        catalog=first_catalog,
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        collector_instance_id="collector-1",
        collector_version="0.1.0+test",
        durability_interval_seconds=0,
    )
    writer.append(event(1))
    writer.close()
    manifest = seal_partial(writer.path, layout=layout, catalog=first_catalog)
    first_catalog.close()

    alternate = tmp_path / "state" / "reconciled.sqlite"
    with Catalog(alternate) as catalog:
        actions = reconcile_sealed(layout=layout, catalog=catalog)
        assert actions[0].action == "catalog_reconciled"
        assert catalog.state(str(manifest["chunk_id"])) is ChunkState.SEALED
        transition_count = catalog.transition_count(str(manifest["chunk_id"]))
        reconcile_sealed(layout=layout, catalog=catalog)
        assert catalog.transition_count(str(manifest["chunk_id"])) == transition_count


def test_existing_mismatched_sealed_name_never_overwrites_or_deletes_source(
    tmp_path: Path,
) -> None:
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
        writer.append(event(1))
        writer.close()
        destination = layout.sealed / f"{writer.header.chunk_id.hex}.bmdr.zst"
        destination.write_bytes(b"not-zstandard")

        with pytest.raises(SealError):
            seal_partial(writer.path, layout=layout, catalog=catalog)
        assert writer.path.exists()
        assert destination.read_bytes() == b"not-zstandard"
