from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, cast

import pytest

import binance_market_data_recorder.spool.recovery as recovery_module
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    reconcile_sealed,
    recover_storage,
)
from binance_market_data_recorder.spool.seal import (
    SealError,
    seal_partial,
)
from binance_market_data_recorder.spool.seal import (
    validate_sealed_artifact as full_validate_sealed_artifact,
)
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import (
    StorageLayout,
    ensure_storage_layout,
)
from tests.factories import event


def _stable_sealed_chunks(
    tmp_path: Path, *, count: int
) -> tuple[StorageLayout, list[dict[str, object]]]:
    layout = ensure_storage_layout(tmp_path)
    manifests: list[dict[str, object]] = []
    with Catalog(layout.catalog) as catalog:
        for sequence in range(1, count + 1):
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
            writer.append(event(sequence))
            writer.close()
            manifests.append(seal_partial(writer.path, layout=layout, catalog=catalog))
    return layout, manifests


def _durable_chunk_identity(row: dict[str, object]) -> dict[str, object]:
    fields = (
        "chunk_id",
        "state",
        "created_at_utc_ns",
        "manifest_path",
        "record_count",
        "sealed_path",
        "stored_bytes",
        "stored_sha256",
        "uncompressed_bytes",
        "uncompressed_sha256",
    )
    return {field: row.get(field) for field in fields}


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


def test_stable_sealed_corpus_uses_metadata_fast_path_without_payload_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, manifests = _stable_sealed_chunks(tmp_path, count=8)
    validation_count = 0

    def unexpected_validation(
        _sealed: Path, _manifest: dict[str, object]
    ) -> None:
        nonlocal validation_count
        validation_count += 1

    monkeypatch.setattr(
        recovery_module, "validate_sealed_artifact", unexpected_validation
    )
    with Catalog(layout.catalog) as catalog:
        actions = reconcile_sealed(layout=layout, catalog=catalog)

    assert len(actions) == len(manifests)
    assert {action.action for action in actions} == {"catalog_unchanged"}
    assert validation_count == 0


def test_stable_sealed_manifest_catalog_identity_mismatch_still_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, _manifests = _stable_sealed_chunks(tmp_path, count=1)
    manifest_path = next(layout.manifests.glob("*.manifest.json"))
    document: dict[str, object] = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )
    document["stored_sha256"] = "0" * 64
    manifest_path.write_text(
        json.dumps(document, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )

    def unexpected_validation(
        _sealed: Path, _manifest: dict[str, object]
    ) -> None:
        raise AssertionError("identity conflict must precede payload validation")

    monkeypatch.setattr(
        recovery_module, "validate_sealed_artifact", unexpected_validation
    )
    with Catalog(layout.catalog) as catalog, pytest.raises(
        RecoveryConflictError,
        match="RECOVERY_MANIFEST_CATALOG_IDENTITY_CONFLICT",
    ):
        reconcile_sealed(layout=layout, catalog=catalog)


@pytest.mark.parametrize(
    "initial_state",
    [None, ChunkState.ACTIVE, ChunkState.RECOVERED, ChunkState.SEALING],
    ids=["missing", "active", "recovered", "sealing"],
)
def test_crash_unstable_manifest_states_still_require_full_payload_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    initial_state: ChunkState | None,
) -> None:
    layout, manifests = _stable_sealed_chunks(tmp_path, count=1)
    manifest = manifests[0]
    chunk_id = str(manifest["chunk_id"])
    alternate = tmp_path / "state" / f"{initial_state or 'missing'}.sqlite"
    real_validate = full_validate_sealed_artifact
    validation_count = 0

    def counted_validation(sealed: Path, document: dict[str, object]) -> None:
        nonlocal validation_count
        validation_count += 1
        real_validate(sealed, document)

    monkeypatch.setattr(
        recovery_module, "validate_sealed_artifact", counted_validation
    )
    with Catalog(alternate) as catalog:
        if initial_state is not None:
            catalog.register_active(
                chunk_id=chunk_id,
                partial_path="",
                created_at_utc_ns=cast(int, manifest["created_at_utc_ns"]),
            )
            if initial_state is ChunkState.RECOVERED:
                catalog.transition(
                    chunk_id,
                    ChunkState.RECOVERED,
                    idempotency_key=f"test-recovered:{chunk_id}",
                )
            elif initial_state is ChunkState.SEALING:
                catalog.transition(
                    chunk_id,
                    ChunkState.SEALING,
                    idempotency_key=f"test-sealing:{chunk_id}",
                )
        actions = reconcile_sealed(layout=layout, catalog=catalog)
        assert catalog.state(chunk_id) is ChunkState.SEALED

    assert [action.action for action in actions] == ["catalog_reconciled"]
    assert validation_count == 1


def test_cancelled_recovery_resumes_idempotently_at_manifest_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout, manifests = _stable_sealed_chunks(tmp_path, count=3)
    cancelled_catalog_path = tmp_path / "state" / "cancelled.sqlite"
    uninterrupted_catalog_path = tmp_path / "state" / "uninterrupted.sqlite"
    stop = threading.Event()
    cancel_after_first = True
    validation_count = 0
    real_validate = full_validate_sealed_artifact

    def validating_then_cancel(
        sealed: Path, document: dict[str, object]
    ) -> None:
        nonlocal cancel_after_first, validation_count
        validation_count += 1
        real_validate(sealed, document)
        if cancel_after_first:
            cancel_after_first = False
            stop.set()

    monkeypatch.setattr(
        recovery_module,
        "validate_sealed_artifact",
        validating_then_cancel,
    )
    chunk_ids = [str(manifest["chunk_id"]) for manifest in manifests]
    with Catalog(cancelled_catalog_path) as cancelled:
        first = recover_storage(
            layout=layout,
            catalog=cancelled,
            stop_requested=stop.is_set,
        )
        assert len(first) == 1
        assert sum(
            cancelled.state(chunk_id) is ChunkState.SEALED
            for chunk_id in chunk_ids
        ) == 1

        stop.clear()
        second = recover_storage(
            layout=layout,
            catalog=cancelled,
            stop_requested=stop.is_set,
        )
        assert len(second) == 3
        assert all(
            cancelled.state(chunk_id) is ChunkState.SEALED
            for chunk_id in chunk_ids
        )
        assert validation_count == 3
        transition_counts = {
            chunk_id: cancelled.transition_count(chunk_id)
            for chunk_id in chunk_ids
        }
        validations_before_repeat = validation_count
        third = recover_storage(
            layout=layout,
            catalog=cancelled,
            stop_requested=stop.is_set,
        )
        assert len(third) == 3
        assert validation_count == validations_before_repeat
        assert transition_counts == {
            chunk_id: cancelled.transition_count(chunk_id)
            for chunk_id in chunk_ids
        }
        cancelled_rows = {
            chunk_id: _durable_chunk_identity(
                cast(dict[str, object], cancelled.chunk(chunk_id))
            )
            for chunk_id in chunk_ids
        }

    monkeypatch.setattr(
        recovery_module, "validate_sealed_artifact", real_validate
    )
    with Catalog(uninterrupted_catalog_path) as uninterrupted:
        uninterrupted_actions = recover_storage(
            layout=layout,
            catalog=uninterrupted,
        )
        assert len(uninterrupted_actions) == 3
        uninterrupted_rows = {
            chunk_id: _durable_chunk_identity(
                cast(dict[str, object], uninterrupted.chunk(chunk_id))
            )
            for chunk_id in chunk_ids
        }

    assert cancelled_rows == uninterrupted_rows
