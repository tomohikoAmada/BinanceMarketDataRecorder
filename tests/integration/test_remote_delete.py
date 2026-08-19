from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.archive.manager import ArchiveManager
from binance_market_data_recorder.archive.remote_authorization import RemoteAuthorizer
from binance_market_data_recorder.archive.remote_delete import (
    RemoteDeleter,
    RemoteDeletionError,
)
from binance_market_data_recorder.archive.remote_source import (
    RemoteSourceError,
    RemoteSourceExporter,
)
from binance_market_data_recorder.cli import _archive_status
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    reconcile_sealed,
    recover_storage,
)
from binance_market_data_recorder.storage.catalog import (
    Catalog,
    CatalogStateError,
    RemoteArchiveState,
)
from binance_market_data_recorder.storage.forecast import StorageForecaster
from tests.remote_authorization_support import (
    RemoteAuthorizationFixture,
    build_receipt,
    prepare_remote_authorization,
)


def _authorized(
    root: Path, *, authorized_at_utc_ns: int | None = None
) -> tuple[RemoteAuthorizationFixture, str]:
    fixture = prepare_remote_authorization(root)
    receipt = build_receipt(fixture)
    assert fixture.prepared.layout.root == root.resolve() / "internal"
    assert fixture.selections[0].sealed_path.parent == fixture.prepared.layout.sealed
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=(
                (lambda: authorized_at_utc_ns)
                if authorized_at_utc_ns is not None
                else None
            ),
        ).authorize(receipt.canonical_bytes(), fixture.selections[0])
    return fixture, receipt.receipt_id


def test_normal_startup_discovers_missing_manifest_before_remote_recovery(
    tmp_path: Path,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    manifest = fixture.selections[0].manifest_path
    os.replace(source, tmp_path / "held-raw")
    os.replace(manifest, tmp_path / "held-manifest")

    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(
            RecoveryConflictError,
            match="RECOVERY_REMOTE_RETAINED_MANIFEST_MISSING",
        ):
            recover_storage(layout=fixture.prepared.layout, catalog=catalog)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert not source.exists()
    assert not manifest.exists()
    assert (tmp_path / "held-raw").is_file()
    assert (tmp_path / "held-manifest").is_file()


def test_case_a_startup_with_missing_manifest_fails_closed_without_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    manifest = fixture.selections[0].manifest_path
    os.replace(manifest, tmp_path / "held-manifest")
    unlink_called = False
    actual_unlink = os.unlink

    def observe_unlink(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal unlink_called
        unlink_called = True
        actual_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "unlink", observe_unlink)
    monkeypatch.setattr(
        os, "supports_dir_fd", os.supports_dir_fd | {observe_unlink}
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(
            RecoveryConflictError,
            match="RECOVERY_REMOTE_RETAINED_MANIFEST_MISSING",
        ):
            recover_storage(layout=fixture.prepared.layout, catalog=catalog)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert not unlink_called
    assert fixture.selections[0].sealed_path.is_file()


def test_mutated_manifest_startup_still_fails_closed(
    tmp_path: Path,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    manifest = fixture.selections[0].manifest_path
    document = json.loads(manifest.read_bytes())
    document["stored_sha256"] = "0" * 64
    manifest.write_text(json.dumps(document), encoding="utf-8")
    os.replace(fixture.selections[0].sealed_path, tmp_path / "held-raw")

    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RecoveryConflictError):
            recover_storage(layout=fixture.prepared.layout, catalog=catalog)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert (tmp_path / "held-raw").is_file()


def test_valid_case_b_startup_uses_absence_only_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    os.replace(fixture.selections[0].sealed_path, tmp_path / "held-raw")
    actual_unlink = os.unlink
    unlink_called = False

    def observe_unlink(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal unlink_called
        unlink_called = True
        actual_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "unlink", observe_unlink)
    monkeypatch.setattr(
        os, "supports_dir_fd", os.supports_dir_fd | {observe_unlink}
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        actions = recover_storage(layout=fixture.prepared.layout, catalog=catalog)
        assert any(action.action == "remote_absent_reconciled" for action in actions)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETED.value
        assert len(catalog.remote_archive_events(receipt_id)) == 2
    assert not unlink_called
    assert (tmp_path / "held-raw").is_file()


def test_case_a_startup_preserves_pending_without_unlink(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        actions = recover_storage(layout=fixture.prepared.layout, catalog=catalog)
        assert any(
            action.action == "remote_lifecycle_preserved" for action in actions
        )
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert fixture.selections[0].sealed_path.is_file()


def test_terminal_absent_startup_preserves_valid_terminal_authority(
    tmp_path: Path,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    os.replace(fixture.selections[0].sealed_path, tmp_path / "held-raw")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
        before = catalog.remote_archive_events(receipt_id)
        actions = recover_storage(layout=fixture.prepared.layout, catalog=catalog)
        assert any(
            action.action == "remote_lifecycle_preserved"
            and action.detail == "TERMINAL_ABSENT"
            for action in actions
        )
        assert catalog.remote_archive_events(receipt_id) == before


def test_terminal_absent_with_missing_manifest_fails_closed(
    tmp_path: Path,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    os.replace(fixture.selections[0].sealed_path, tmp_path / "held-raw")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).reconcile_absent_authorized(receipt_id)
    os.replace(fixture.selections[0].manifest_path, tmp_path / "held-manifest")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RecoveryConflictError):
            recover_storage(layout=fixture.prepared.layout, catalog=catalog)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETED.value
        assert len(catalog.remote_archive_events(receipt_id)) == 2


def test_startup_without_remote_transactions_remains_valid(tmp_path: Path) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        assert catalog.remote_archive_transactions() == []
        recover_storage(layout=fixture.prepared.layout, catalog=catalog)


def test_actual_unlink_failure_preserves_raw_pending_and_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    manifest_before = fixture.selections[0].manifest_path.read_bytes()
    actual_unlink = os.unlink

    def fail_exact_unlink(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
    ) -> None:
        if dir_fd is not None and os.fsdecode(path) == source.name:
            raise PermissionError("injected exact unlink failure")
        actual_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "unlink", fail_exact_unlink)
    monkeypatch.setattr(
        os, "supports_dir_fd", os.supports_dir_fd | {fail_exact_unlink}
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError, match="unlink failed"):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).delete_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert source.is_file()
    assert fixture.selections[0].manifest_path.read_bytes() == manifest_before


def test_actual_post_unlink_parent_fsync_failure_leaves_case_b_then_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    manifest_before = fixture.selections[0].manifest_path.read_bytes()
    actual_fsync = os.fsync
    armed = False
    failed = False

    def hook(point: str) -> None:
        nonlocal armed
        if point == "k2_after_unlink_before_parent_fsync":
            armed = True

    def fail_post_unlink_fsync(descriptor: int) -> None:
        nonlocal armed, failed
        if armed and not failed:
            armed = False
            failed = True
            assert not source.exists()
            raise OSError("injected real post-unlink parent fsync failure")
        actual_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_post_unlink_fsync)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError, match="post-unlink"):
            RemoteDeleter(
                layout=fixture.prepared.layout,
                catalog=catalog,
                fault_hook=hook,
            ).delete_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert failed
    assert not source.exists()
    assert fixture.selections[0].manifest_path.read_bytes() == manifest_before

    with Catalog(fixture.prepared.layout.catalog) as fresh:
        result = RemoteDeleter(
            layout=fixture.prepared.layout, catalog=fresh
        ).reconcile_absent_authorized(receipt_id)
        assert result.state is RemoteArchiveState.REMOTE_DELETED
        assert len(fresh.remote_archive_events(receipt_id)) == 2


def test_preflight_parent_fsync_failure_occurs_before_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    source_before = source.read_bytes()
    actual_fsync = os.fsync
    failed = False

    def fail_first_deletion_fsync(descriptor: int) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected preflight directory fsync failure")
        actual_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_first_deletion_fsync)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError, match="preflight"):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).delete_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert failed
    assert source.read_bytes() == source_before


@pytest.mark.parametrize("mutation", ["missing", "changed"])
def test_case_b_requires_exact_retained_manifest(
    tmp_path: Path, mutation: str
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    os.replace(source, tmp_path / "held-case-b-raw")
    manifest = fixture.selections[0].manifest_path
    if mutation == "missing":
        os.replace(manifest, tmp_path / "held-case-b-manifest")
    else:
        manifest.write_bytes(b"{}\n")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).reconcile_absent_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1


def test_case_b_absent_retry_fsyncs_and_terminalizes_without_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    held = tmp_path / "held-authorized-raw"
    os.replace(source, held)
    actual_unlink = os.unlink
    unlink_called = False

    def observe_unlink(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal unlink_called
        unlink_called = True
        actual_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(os, "unlink", observe_unlink)
    monkeypatch.setattr(os, "supports_dir_fd", os.supports_dir_fd | {observe_unlink})
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        result = RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).reconcile_absent_authorized(receipt_id)
        assert result.state is RemoteArchiveState.REMOTE_DELETED
        assert not result.source_deleted
    assert not unlink_called
    assert held.is_file()


@pytest.mark.parametrize("timing", ["before", "after"])
def test_case_b_reappearance_around_fsync_refuses_terminal_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timing: str
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    held = tmp_path / f"held-{timing}"
    os.replace(source, held)
    actual_fsync = os.fsync
    restored = False

    def reappear(descriptor: int) -> None:
        nonlocal restored
        if not restored and timing == "before":
            os.replace(held, source)
            restored = True
        actual_fsync(descriptor)
        if not restored and timing == "after":
            os.replace(held, source)
            restored = True

    monkeypatch.setattr(os, "fsync", reappear)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError, match="present"):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).reconcile_absent_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert source.is_file()


def test_case_b_reappearance_during_final_authority_reload_refuses_terminal_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    source = fixture.selections[0].sealed_path
    held = tmp_path / "held-final-authority-reload"
    os.replace(source, held)
    actual_reload = RemoteDeleter._reload_same_authority
    reload_count = 0

    def reappear_after_reload(
        deleter: RemoteDeleter, authority: Any
    ) -> Any:
        nonlocal reload_count
        current = actual_reload(deleter, authority)
        reload_count += 1
        if reload_count == 2:
            os.replace(held, source)
        return current

    monkeypatch.setattr(RemoteDeleter, "_reload_same_authority", reappear_after_reload)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError, match="present"):
            RemoteDeleter(
                layout=fixture.prepared.layout, catalog=catalog
            ).reconcile_absent_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert reload_count == 2
    assert source.is_file()


@pytest.mark.parametrize(
    "fault_point",
    [
        "before_remote_deleted_begin",
        "before_remote_deleted_event",
        "before_remote_deleted_commit",
    ],
)
def test_terminal_transaction_faults_leave_retryable_case_b(
    tmp_path: Path, fault_point: str
) -> None:
    fixture, receipt_id = _authorized(tmp_path)

    def fail(point: str) -> None:
        if point == fault_point:
            raise RuntimeError(f"injected {point}")

    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteDeletionError, match="retryable"):
            RemoteDeleter(
                layout=fixture.prepared.layout,
                catalog=catalog,
                fault_hook=fail,
            ).delete_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
        assert len(catalog.remote_archive_events(receipt_id)) == 1
    assert not fixture.selections[0].sealed_path.exists()

    with Catalog(fixture.prepared.layout.catalog) as fresh:
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=fresh
        ).reconcile_absent_authorized(receipt_id)
        assert len(fresh.remote_archive_events(receipt_id)) == 2


def test_ambiguous_after_commit_exception_uses_fresh_authoritative_readback(
    tmp_path: Path,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)

    def ambiguous(point: str) -> None:
        if point == "after_remote_deleted_commit":
            raise RuntimeError("injected ambiguous COMMIT return")

    with Catalog(fixture.prepared.layout.catalog) as catalog:
        result = RemoteDeleter(
            layout=fixture.prepared.layout,
            catalog=catalog,
            fault_hook=ambiguous,
        ).delete_authorized(receipt_id)
        row = catalog.remote_archive_transaction(receipt_id)
        assert result.state is RemoteArchiveState.REMOTE_DELETED
        assert row is not None
        terminal_timestamp = row["remote_deleted_at_utc_ns"]
        assert len(catalog.remote_archive_events(receipt_id)) == 2
        retry = RemoteDeleter(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: 999,
        ).delete_authorized(receipt_id)
        assert retry.state is RemoteArchiveState.REMOTE_DELETED
        retry_row = catalog.remote_archive_transaction(receipt_id)
        assert retry_row is not None
        assert retry_row["remote_deleted_at_utc_ns"] == terminal_timestamp
        assert len(catalog.remote_archive_events(receipt_id)) == 2


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("receipt_id", "0" * 64),
        ("from_state", None),
        ("to_state", "REMOTE_DELETE_PENDING"),
        ("occurred_at_utc_ns", 0),
        ("evidence_json", "{}"),
        ("idempotency_key", "wrong"),
    ],
)
def test_terminal_event_corruption_fails_authoritative_reads(
    tmp_path: Path, column: str, value: object
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
        if column == "receipt_id":
            catalog._connection.execute("PRAGMA foreign_keys=OFF")
        catalog._connection.execute(
            f"UPDATE remote_archive_events SET {column} = ? "
            "WHERE idempotency_key = ?",
            (value, f"remote-deleted:{receipt_id}"),
        )
        if column == "receipt_id":
            catalog._connection.execute("PRAGMA foreign_keys=ON")
        with pytest.raises(CatalogStateError):
            catalog.remote_archive_transaction(receipt_id)


def test_production_terminal_state_preserves_all_m22_4a_consumers(
    tmp_path: Path,
) -> None:
    authorization_time = int(
        datetime(2026, 8, 19, 12, tzinfo=UTC).timestamp() * 1_000_000_000
    )
    fixture, receipt_id = _authorized(
        tmp_path, authorized_at_utc_ns=authorization_time
    )
    manifest_before = fixture.selections[0].manifest_path.read_bytes()
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteDeleter(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: authorization_time + 100,
        ).delete_authorized(receipt_id)

        exporter = RemoteSourceExporter(layout=fixture.prepared.layout, catalog=catalog)
        with pytest.raises(RemoteSourceError, match="remote archive ownership"):
            exporter.select_chunk(fixture.prepared.chunk_ids[0])

        manager = ArchiveManager(
            layout=fixture.prepared.layout,
            catalog=catalog,
            target=fixture.prepared.target,
        )
        assert manager._next_transaction() is None
        aggregate = catalog.archive_aggregate(fixture.prepared.target.storage_id)
        assert aggregate["backlog_files"] == 0
        assert aggregate["remote_pending_files"] == 0
        assert aggregate["remote_deleted_files"] == 1
        status = manager.status()
        assert status["remote_deleted_files"] == 1
        cli = _archive_status(catalog)
        assert cli["remote_deleted_files"] == 1

        forecaster = StorageForecaster(
            catalog=catalog, data_root=fixture.prepared.layout.root
        )
        forecaster.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=1000,
            free_bytes=500,
            observed_at_utc_ns=authorization_time + 200,
        )
        assert catalog.space_samples("internal")[-1]["archive_backlog_bytes"] == 0

        actions = reconcile_sealed(layout=fixture.prepared.layout, catalog=catalog)
        assert any(
            action.action == "remote_lifecycle_preserved"
            and action.detail == "TERMINAL_ABSENT"
            for action in actions
        )
        report = DailyReporter(
            catalog=catalog,
            daily_directory=fixture.prepared.layout.daily_reports,
        ).build("2026-08-19", generated_at_utc_ns=authorization_time + 300)
        streams = cast(list[dict[str, Any]], report["streams"])
        assert len(streams) == 1
        output = cast(dict[str, object], streams[0]["output"])
        assert output["archived_files"] == 1
        assert output["archived_bytes"] == fixture.selections[0].descriptor.stored_bytes

        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["receipt_bytes"] == build_receipt(fixture).canonical_bytes()
        assert len(catalog.remote_archive_events(receipt_id)) == 2
    assert fixture.selections[0].manifest_path.read_bytes() == manifest_before


def test_terminal_event_evidence_is_exact_canonical_json(tmp_path: Path) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteDeleter(
            layout=fixture.prepared.layout, catalog=catalog
        ).delete_authorized(receipt_id)
        event = catalog.remote_archive_events(receipt_id)[-1]
        raw = catalog._connection.execute(
            "SELECT evidence_json FROM remote_archive_events "
            "WHERE idempotency_key = ?",
            (f"remote-deleted:{receipt_id}",),
        ).fetchone()["evidence_json"]
    assert raw == json.dumps(event["evidence"], sort_keys=True, separators=(",", ":"))
