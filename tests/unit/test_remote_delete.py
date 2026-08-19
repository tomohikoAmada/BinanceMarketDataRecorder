from __future__ import annotations

import os
from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.archive.remote_authorization import RemoteAuthorizer
from binance_market_data_recorder.archive.remote_delete import (
    RemoteDeleter,
    RemoteDeletionError,
)
from binance_market_data_recorder.archive.remote_source import (
    RemoteSourceError,
    RemoteSourceExporter,
)
from binance_market_data_recorder.storage.catalog import (
    Catalog,
    CatalogStateError,
    ChunkState,
    RemoteArchiveState,
)
from tests.remote_authorization_support import (
    RemoteAuthorizationFixture,
    build_receipt,
    force_valid_terminal_fixture,
    prepare_remote_authorization,
)


def _authorized(root: Path) -> tuple[RemoteAuthorizationFixture, str]:
    fixture = prepare_remote_authorization(root)
    receipt = build_receipt(fixture)
    _assert_test_owned(fixture, root)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(layout=fixture.prepared.layout, catalog=catalog).authorize(
            receipt.canonical_bytes(), fixture.selections[0]
        )
    return fixture, receipt.receipt_id


def _assert_test_owned(fixture: RemoteAuthorizationFixture, root: Path) -> None:
    layout = fixture.prepared.layout
    assert layout.root == root.resolve() / "internal"
    assert fixture.selections[0].sealed_path.parent == layout.sealed
    assert str(layout.root).startswith(str(root.resolve()))


def test_case_a_deletes_only_raw_and_retains_exact_evidence(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    selection = fixture.selections[0]
    manifest_before = selection.manifest_path.read_bytes()
    sealed_siblings_before = set(selection.sealed_path.parent.iterdir())
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        result = RemoteDeleter(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: 1_900_000_000_000_000_000,
        ).delete_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        events = catalog.remote_archive_events(receipt_id)
        chunk = catalog.chunk(result.chunk_id)

    assert result.state is RemoteArchiveState.REMOTE_DELETED
    assert result.source_deleted
    assert not selection.sealed_path.exists()
    assert selection.manifest_path.read_bytes() == manifest_before
    assert set(selection.sealed_path.parent.iterdir()) == (
        sealed_siblings_before - {selection.sealed_path}
    )
    assert row is not None
    assert row["receipt_bytes"] == build_receipt(fixture).canonical_bytes()
    assert row["remote_deleted_at_utc_ns"] == 1_900_000_000_000_000_000
    assert chunk is not None and chunk["state"] == ChunkState.SEALED.value
    assert len(events) == 2
    assert events[-1]["idempotency_key"] == f"remote-deleted:{receipt_id}"
    terminal_evidence = cast(dict[str, object], events[-1]["evidence"])
    assert terminal_evidence["source_parent_fsync"] is True
    assert terminal_evidence["source_absent"] is True


def test_terminal_absent_retry_performs_no_write(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        deleter = RemoteDeleter(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: 10,
        )
        first = deleter.delete_authorized(receipt_id)
        row_before = catalog.remote_archive_transaction(receipt_id)
        events_before = catalog.remote_archive_events(receipt_id)
        second = RemoteDeleter(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: 999,
        ).delete_authorized(receipt_id)
        assert catalog.remote_archive_transaction(receipt_id) == row_before
        assert catalog.remote_archive_events(receipt_id) == events_before
    assert first.source_deleted
    assert not second.source_deleted


def test_k0_fault_occurs_before_destructive_validation(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source_before = fixture.selections[0].sealed_path.read_bytes()

    def stop(point: str) -> None:
        if point == "k0_entry_before_destructive_validation":
            raise RuntimeError("injected K0")

    with Catalog(fixture.prepared.layout.catalog) as catalog, pytest.raises(
        RuntimeError, match="K0"
    ):
        RemoteDeleter(
            layout=fixture.prepared.layout,
            catalog=catalog,
            fault_hook=stop,
        ).delete_authorized(receipt_id)
    assert fixture.selections[0].sealed_path.read_bytes() == source_before


@pytest.mark.parametrize("replacement", [b"mutated", b"y" * 103])
def test_mutated_raw_fails_before_unlink(tmp_path: Path, replacement: bytes) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    original_size = source.stat().st_size
    if len(replacement) != original_size:
        replacement = (replacement * (original_size + 1))[:original_size]
    source.write_bytes(replacement)
    with Catalog(fixture.prepared.layout.catalog) as catalog, pytest.raises(
        RemoteDeletionError
    ):
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
    with Catalog(fixture.prepared.layout.catalog, read_only=True) as catalog:
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert source.read_bytes() == replacement


@pytest.mark.parametrize("mutation", ["changed", "missing"])
def test_mutated_or_missing_manifest_fails_without_unlink(
    tmp_path: Path, mutation: str
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    manifest = fixture.selections[0].manifest_path
    if mutation == "changed":
        manifest.write_bytes(b"{}\n")
    else:
        os.replace(manifest, tmp_path / "held-manifest")
    with Catalog(fixture.prepared.layout.catalog) as catalog, pytest.raises(
        RemoteDeletionError
    ):
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
    assert source.is_file()


@pytest.mark.parametrize("receipt_id", ["", "0" * 63, "G" * 64, "0" * 64])
def test_wrong_or_missing_receipt_never_mutates_source(
    tmp_path: Path, receipt_id: str
) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    _assert_test_owned(fixture, tmp_path)
    source_before = fixture.selections[0].sealed_path.read_bytes()
    with Catalog(fixture.prepared.layout.catalog) as catalog, pytest.raises(
        RemoteDeletionError
    ):
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
        assert catalog.remote_archive_transactions() == []
    assert fixture.selections[0].sealed_path.read_bytes() == source_before


def test_case_c_absence_without_pending_cannot_be_terminalized(tmp_path: Path) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    _assert_test_owned(fixture, tmp_path)
    source = fixture.selections[0].sealed_path
    os.replace(source, tmp_path / "unexplained-held-raw")
    valid_unpersisted_receipt = build_receipt(fixture)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).reconcile_absent_authorized(valid_unpersisted_receipt.receipt_id)
        assert catalog.remote_archive_transactions() == []


@pytest.mark.parametrize("kind", ["symlink", "directory"])
def test_unsafe_raw_object_fails_without_alternate_delete(
    tmp_path: Path, kind: str
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    held = tmp_path / "held-exact-raw"
    os.replace(source, held)
    if kind == "symlink":
        source.symlink_to(held)
    else:
        source.mkdir()
    with Catalog(fixture.prepared.layout.catalog) as catalog, pytest.raises(
        RemoteDeletionError
    ):
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
    assert held.is_file()
    assert source.exists() or source.is_symlink()


def test_symlinked_sealed_parent_fails_without_deleting_held_tree(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    sealed = fixture.prepared.layout.sealed
    held_parent = tmp_path / "held-sealed-parent"
    os.replace(sealed, held_parent)
    sealed.symlink_to(held_parent, target_is_directory=True)
    with Catalog(fixture.prepared.layout.catalog) as catalog, pytest.raises(
        RemoteDeletionError
    ):
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
    assert (held_parent / fixture.selections[0].sealed_path.name).is_file()


def test_final_parent_relative_identity_detects_same_byte_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    original = source.read_bytes()
    held = tmp_path / "held-validated-inode"
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        deleter = RemoteDeleter(layout=fixture.prepared.layout, catalog=catalog)
        actual_validate = deleter._validate_held_raw

        def replace_after_validation(
            raw_fd: int, authority: object
        ) -> os.stat_result:
            result = actual_validate(raw_fd, authority)  # type: ignore[arg-type]
            os.replace(source, held)
            source.write_bytes(original)
            return result

        monkeypatch.setattr(deleter, "_validate_held_raw", replace_after_validation)
        with pytest.raises(RemoteDeletionError, match="held validated file"):
            deleter.delete_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
    assert held.read_bytes() == original
    assert source.read_bytes() == original


def test_terminal_present_is_a_contradiction_not_a_cleanup_request(
    tmp_path: Path,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        force_valid_terminal_fixture(catalog, receipt_id)
        with pytest.raises(RemoteDeletionError, match="present"):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).delete_authorized(receipt_id)
    assert source.is_file()


def test_impossible_same_host_overlap_fails_without_unlink(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    descriptor = fixture.selections[0].descriptor
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        catalog._connection.execute(
            """
            INSERT INTO archive_transactions(
                transaction_id, chunk_id, storage_id, state, market, stream,
                source_relative_path, source_manifest_relative_path,
                source_manifest_sha256, target_relative_path,
                target_temp_relative_path, external_manifest_relative_path,
                stored_bytes, stored_sha256, created_at_utc_ns, updated_at_utc_ns
            ) VALUES ('overlap', ?, 'storage', 'COPYING', ?, ?, ?, ?, ?,
                      'raw/x', 'raw/x.copying', 'manifests/x', ?, ?, 1, 1)
            """,
            (
                descriptor.chunk_id,
                descriptor.market,
                descriptor.stream,
                descriptor.source_relative_path,
                descriptor.source_manifest_relative_path,
                descriptor.source_manifest_sha256,
                descriptor.stored_bytes,
                descriptor.stored_sha256,
            ),
        )
        with pytest.raises(RemoteDeletionError, match="overlap"):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).delete_authorized(receipt_id)
    assert fixture.selections[0].sealed_path.is_file()


def test_startup_recovery_only_entry_refuses_reappeared_raw(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError, match="present"):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).reconcile_absent_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
    assert fixture.selections[0].sealed_path.is_file()


def test_unsupported_platform_fails_before_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    monkeypatch.setattr(
        "binance_market_data_recorder.archive.remote_delete.sys.platform", "win32"
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog, pytest.raises(
        RemoteDeletionError, match="unsupported"
    ):
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
    assert fixture.selections[0].sealed_path.is_file()


def test_authoritative_terminal_validation_rejects_malformed_event_combinations(
    tmp_path: Path,
) -> None:
    pending, pending_id = _authorized(tmp_path / "pending-extra")
    with Catalog(pending.prepared.layout.catalog) as catalog:
        row = catalog.remote_archive_transaction(pending_id)
        assert row is not None
        catalog._connection.execute(
            "INSERT INTO remote_archive_events(receipt_id, from_state, to_state, "
            "occurred_at_utc_ns, evidence_json, idempotency_key) VALUES "
            "(?, 'REMOTE_DELETE_PENDING', 'REMOTE_DELETED', 2, '{}', ?)",
            (pending_id, f"remote-deleted:{pending_id}"),
        )
        with pytest.raises(CatalogStateError):
            catalog.remote_archive_transaction(pending_id)

    deleted, deleted_id = _authorized(tmp_path / "deleted-missing")
    with Catalog(deleted.prepared.layout.catalog) as catalog:
        force_valid_terminal_fixture(catalog, deleted_id)
        catalog._connection.execute(
            "DELETE FROM remote_archive_events WHERE idempotency_key = ?",
            (f"remote-deleted:{deleted_id}",),
        )
        with pytest.raises(CatalogStateError):
            catalog.remote_archive_transaction(deleted_id)


def test_remote_deleted_source_remains_ineligible_for_export(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
        exporter = RemoteSourceExporter(layout=fixture.prepared.layout, catalog=catalog)
        with pytest.raises(RemoteSourceError, match="remote archive ownership"):
            exporter.select_chunk(fixture.prepared.chunk_ids[0])
