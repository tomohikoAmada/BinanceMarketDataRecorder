from __future__ import annotations

import os
from pathlib import Path

import pytest

from binance_market_data_recorder.archive.remote_authorization import (
    RemoteAuthorizer,
    RemoteRecoveryCase,
    RemoteSourceObservation,
    classify_remote_recovery,
    observe_remote_source,
)
from binance_market_data_recorder.archive.remote_receive import RemoteArchiveReceipt
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    reconcile_sealed,
)
from binance_market_data_recorder.storage.catalog import Catalog
from tests.remote_authorization_support import (
    RemoteAuthorizationFixture,
    build_receipt,
    force_valid_terminal_fixture,
    prepare_remote_authorization,
    source_snapshot,
)


def _authorize(
    tmp_path: Path,
) -> tuple[RemoteAuthorizationFixture, RemoteArchiveReceipt]:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(layout=fixture.prepared.layout, catalog=catalog).authorize(
            receipt.canonical_bytes(), fixture.selections[0]
        )
    return fixture, receipt


def _make_raw_absent(fixture: RemoteAuthorizationFixture, holding: Path) -> None:
    selection = fixture.selections[0]
    os.replace(selection.sealed_path, holding)


def test_normal_case_a_case_b_and_case_c_are_explicit(tmp_path: Path) -> None:
    normal = prepare_remote_authorization(tmp_path / "normal")
    with Catalog(normal.prepared.layout.catalog) as catalog:
        decision = classify_remote_recovery(
            layout=normal.prepared.layout,
            catalog=catalog,
            chunk_id=normal.prepared.chunk_ids[0],
        )
        assert decision.case is RemoteRecoveryCase.NORMAL
        assert decision.observation is RemoteSourceObservation.PRESENT_MATCHING

    pending, receipt = _authorize(tmp_path / "pending")
    before = source_snapshot(pending)
    with Catalog(pending.prepared.layout.catalog) as catalog:
        case_a = classify_remote_recovery(
            layout=pending.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert case_a.case is RemoteRecoveryCase.CASE_A
    assert source_snapshot(pending) == before

    holding = tmp_path / "pending-raw-held"
    _make_raw_absent(pending, holding)
    with Catalog(pending.prepared.layout.catalog) as catalog:
        case_b = classify_remote_recovery(
            layout=pending.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert case_b.case is RemoteRecoveryCase.CASE_B
        assert case_b.observation is RemoteSourceObservation.ABSENT
    assert holding.is_file()

    unexplained = prepare_remote_authorization(tmp_path / "unexplained")
    unexplained_holding = tmp_path / "unexplained-raw-held"
    _make_raw_absent(unexplained, unexplained_holding)
    with Catalog(unexplained.prepared.layout.catalog) as catalog:
        case_c = classify_remote_recovery(
            layout=unexplained.prepared.layout,
            catalog=catalog,
            chunk_id=unexplained.prepared.chunk_ids[0],
        )
        assert case_c.case is RemoteRecoveryCase.CASE_C
        assert case_c.fail_closed


def test_case_d_receipt_descriptor_hash_and_manifest_mismatches(tmp_path: Path) -> None:
    fixture, receipt = _authorize(tmp_path / "receipt")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        catalog._connection.execute(
            "UPDATE remote_archive_transactions SET receipt_bytes = ?",
            (b"{}\n",),
        )
        decision = classify_remote_recovery(
            layout=fixture.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert decision.case is RemoteRecoveryCase.CASE_D

    fixture, receipt = _authorize(tmp_path / "descriptor")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        catalog._connection.execute(
            "UPDATE remote_archive_transactions SET source_descriptor_sha256 = ?",
            ("0" * 64,),
        )
        decision = classify_remote_recovery(
            layout=fixture.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert decision.case is RemoteRecoveryCase.CASE_D

    fixture, receipt = _authorize(tmp_path / "manifest")
    fixture.selections[0].manifest_path.write_bytes(b"{}\n")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        decision = classify_remote_recovery(
            layout=fixture.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert decision.case is RemoteRecoveryCase.CASE_D


def test_terminal_absence_presence_and_impossible_overlap(tmp_path: Path) -> None:
    absent, receipt = _authorize(tmp_path / "terminal-absent")
    _make_raw_absent(absent, tmp_path / "terminal-raw-held")
    with Catalog(absent.prepared.layout.catalog) as catalog:
        force_valid_terminal_fixture(catalog, receipt.receipt_id)
        decision = classify_remote_recovery(
            layout=absent.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert decision.case is RemoteRecoveryCase.TERMINAL_ABSENT

    present, receipt = _authorize(tmp_path / "terminal-present")
    with Catalog(present.prepared.layout.catalog) as catalog:
        force_valid_terminal_fixture(catalog, receipt.receipt_id)
        decision = classify_remote_recovery(
            layout=present.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert decision.case is RemoteRecoveryCase.TERMINAL_PRESENT_CONTRADICTION

    overlap, receipt = _authorize(tmp_path / "overlap")
    selection = overlap.selections[0]
    descriptor = selection.descriptor
    with Catalog(overlap.prepared.layout.catalog) as catalog:
        catalog._connection.execute(
            """
            INSERT INTO archive_transactions(
                transaction_id, chunk_id, storage_id, state, market, stream,
                source_relative_path, source_manifest_relative_path,
                source_manifest_sha256, target_relative_path,
                target_temp_relative_path, external_manifest_relative_path,
                stored_bytes, stored_sha256, created_at_utc_ns, updated_at_utc_ns
            ) VALUES (?, ?, ?, 'COPYING', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
            """,
            (
                "impossible-overlap",
                receipt.chunk_id,
                "storage",
                descriptor.market,
                descriptor.stream,
                descriptor.source_relative_path,
                descriptor.source_manifest_relative_path,
                descriptor.source_manifest_sha256,
                "raw/target",
                "raw/target.copying",
                "manifests/target.json",
                descriptor.stored_bytes,
                descriptor.stored_sha256,
            ),
        )
        decision = classify_remote_recovery(
            layout=overlap.prepared.layout,
            catalog=catalog,
            chunk_id=receipt.chunk_id,
        )
        assert decision.case is RemoteRecoveryCase.IMPOSSIBLE_SAME_HOST_REMOTE_OVERLAP


def test_source_observation_rejects_unsafe_objects_and_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        chunk = catalog.chunk(fixture.prepared.chunk_ids[0])
        assert chunk is not None
        source = fixture.selections[0].sealed_path
        held = tmp_path / "held-source"
        os.replace(source, held)
        source.symlink_to(held)
        assert observe_remote_source(
            layout=fixture.prepared.layout, chunk=chunk
        ) is RemoteSourceObservation.PRESENT_MISMATCH
        os.replace(held, source)

        actual_lstat = os.lstat

        def missing_parent(
            path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        ) -> os.stat_result:
            if Path(os.fsdecode(path)) == source.parent:
                raise FileNotFoundError("injected missing parent")
            return actual_lstat(path)

        monkeypatch.setattr(os, "lstat", missing_parent)
        assert observe_remote_source(
            layout=fixture.prepared.layout, chunk=chunk
        ) is RemoteSourceObservation.UNKNOWN
        monkeypatch.setattr(os, "lstat", actual_lstat)

        actual_open = os.open

        def denied(
            path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
            flags: int,
            mode: int = 0o777,
            *,
            dir_fd: int | None = None,
        ) -> int:
            if Path(os.fsdecode(path)) == source:
                raise PermissionError("injected EACCES")
            return actual_open(path, flags, mode, dir_fd=dir_fd)

        monkeypatch.setattr(os, "open", denied)
        assert observe_remote_source(
            layout=fixture.prepared.layout, chunk=chunk
        ) is RemoteSourceObservation.UNKNOWN


def test_spool_recovery_routes_remote_projection_without_chunkstate_reuse(
    tmp_path: Path,
) -> None:
    pending, _receipt = _authorize(tmp_path / "pending-recovery")
    with Catalog(pending.prepared.layout.catalog) as catalog:
        actions = reconcile_sealed(layout=pending.prepared.layout, catalog=catalog)
        assert any(
            action.action == "remote_lifecycle_preserved"
            and action.detail == RemoteRecoveryCase.CASE_A.value
            for action in actions
        )

    interrupted, _receipt = _authorize(tmp_path / "case-b-recovery")
    _make_raw_absent(interrupted, tmp_path / "case-b-held")
    with Catalog(interrupted.prepared.layout.catalog) as catalog:
        actions = reconcile_sealed(layout=interrupted.prepared.layout, catalog=catalog)
        assert any(
            action.action == "remote_absent_reconciled"
            and action.detail == "REMOTE_DELETED"
            for action in actions
        )
        row = catalog.remote_archive_transaction(_receipt.receipt_id)
        assert row is not None and row["state"] == "REMOTE_DELETED"

    terminal, _receipt = _authorize(tmp_path / "terminal-recovery")
    with Catalog(terminal.prepared.layout.catalog) as catalog:
        force_valid_terminal_fixture(catalog, _receipt.receipt_id)
        with pytest.raises(RecoveryConflictError, match="TERMINAL_PRESENT"):
            reconcile_sealed(layout=terminal.prepared.layout, catalog=catalog)
