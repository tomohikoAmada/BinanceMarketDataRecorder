from __future__ import annotations

import hashlib
import json
import multiprocessing
import time
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.archive import ArchiveError, ArchiveManager
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    reconcile_sealed,
)
from binance_market_data_recorder.storage.catalog import (
    ArchiveState,
    Catalog,
    ChunkState,
)
from tests.archive_support import PreparedArchive, prepare_archive

_FAULT_POINT = {
    ChunkState.ARCHIVE_COPYING: "after_reserve",
    ChunkState.ARCHIVE_VERIFYING: "after_copy_catalog_transition",
    ChunkState.ARCHIVED_VERIFIED: "after_catalog_commit",
    ChunkState.LOCAL_DELETE_PENDING: "before_local_delete",
}


def _recovery_process(
    layout_root: str,
    catalog_path: str,
    start_barrier: Any,
    result_path: str,
) -> None:
    from binance_market_data_recorder.spool.recovery import reconcile_sealed
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout

    layout = ensure_storage_layout(Path(layout_root))
    failures: list[dict[str, str]] = []
    preserved = 0
    start_barrier.wait(timeout=30)
    with Catalog(Path(catalog_path)) as catalog:
        for _ in range(50):
            try:
                actions = reconcile_sealed(layout=layout, catalog=catalog)
            except Exception as exc:
                failures.append({"type": type(exc).__name__, "message": str(exc)})
            else:
                preserved += sum(
                    action.action == "archive_state_preserved" for action in actions
                )
            time.sleep(0.005)
    Path(result_path).write_text(
        json.dumps({"failures": failures, "archive_states_preserved": preserved}),
        encoding="utf-8",
    )


def _archive_process(
    layout_root: str,
    catalog_path: str,
    storage_id: str,
    volume_uuid: str,
    registered_relative_path: str,
    marker_nonce: str,
    target_root: str,
    start_barrier: Any,
    result_path: str,
) -> None:
    from binance_market_data_recorder.archive import ArchiveManager, ArchiveTarget
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout

    layout = ensure_storage_layout(Path(layout_root))
    target = ArchiveTarget(
        storage_id=storage_id,
        volume_uuid=volume_uuid,
        registered_relative_path=registered_relative_path,
        marker_nonce=marker_nonce,
        root=Path(target_root),
    )
    failures: list[dict[str, str]] = []
    processed = 0
    start_barrier.wait(timeout=30)
    with Catalog(Path(catalog_path)) as catalog:
        manager = ArchiveManager(layout=layout, catalog=catalog, target=target)
        while True:
            try:
                result = manager.run_once()
            except Exception as exc:
                failures.append({"type": type(exc).__name__, "message": str(exc)})
                break
            if result.state == "NO_ELIGIBLE_CHUNKS":
                break
            processed += 1
    Path(result_path).write_text(
        json.dumps({"failures": failures, "processed": processed}),
        encoding="utf-8",
    )


def _advance_to(prepared: PreparedArchive, state: ChunkState) -> None:
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )
        point = _FAULT_POINT.get(state)
        if point is None:
            assert state is ChunkState.LOCAL_DELETED
            assert manager.run_once().state == ArchiveState.LOCAL_DELETED
            return

        def stop_at(selected: str, _path: Path | None) -> None:
            if selected == point:
                raise RuntimeError(f"stop at {point}")

        manager.fault_hook = stop_at
        with pytest.raises((ArchiveError, RuntimeError), match=f"stop at {point}"):
            manager.run_once()
        assert catalog.state(prepared.chunk_ids[0]) is state


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.mark.parametrize(
    "state",
    [
        ChunkState.ARCHIVE_COPYING,
        ChunkState.ARCHIVE_VERIFYING,
        ChunkState.ARCHIVED_VERIFIED,
        ChunkState.LOCAL_DELETE_PENDING,
        ChunkState.LOCAL_DELETED,
    ],
)
def test_reconcile_preserves_archive_lifecycle_without_side_effects(
    tmp_path: Path, state: ChunkState
) -> None:
    prepared = prepare_archive(tmp_path)
    _advance_to(prepared, state)
    external_before = _tree_bytes(prepared.target.root)

    with Catalog(prepared.layout.catalog) as catalog:
        transaction = catalog.archive_transaction_for_chunk(prepared.chunk_ids[0])
        row = catalog.chunk(prepared.chunk_ids[0])
        assert transaction is not None and row is not None
        transaction_count = len(catalog.archive_transactions())
        transition_count = catalog.transition_count(prepared.chunk_ids[0])
        source = prepared.layout.root / str(transaction["source_relative_path"])
        source_existed = source.exists()

        first = reconcile_sealed(layout=prepared.layout, catalog=catalog)
        second = reconcile_sealed(layout=prepared.layout, catalog=catalog)

        assert [action.action for action in first] == ["archive_state_preserved"]
        assert [action.detail for action in first] == [state.value]
        assert [action.action for action in second] == ["archive_state_preserved"]
        assert catalog.state(prepared.chunk_ids[0]) is state
        assert len(catalog.archive_transactions()) == transaction_count
        assert catalog.transition_count(prepared.chunk_ids[0]) == transition_count
        assert source.exists() is source_existed

    assert _tree_bytes(prepared.target.root) == external_before
    if state is ChunkState.LOCAL_DELETED:
        assert not source.exists()


def test_sealed_reconciliation_twice_is_idempotent(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        before = catalog.transition_count(prepared.chunk_ids[0])
        first = reconcile_sealed(layout=prepared.layout, catalog=catalog)
        second = reconcile_sealed(layout=prepared.layout, catalog=catalog)
        assert [action.action for action in first] == ["catalog_unchanged"]
        assert [action.action for action in second] == ["catalog_unchanged"]
        assert catalog.state(prepared.chunk_ids[0]) is ChunkState.SEALED
        assert catalog.transition_count(prepared.chunk_ids[0]) == before
        assert catalog.archive_transactions() == []


def test_manifest_catalog_identity_conflict_fails_closed(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        catalog._connection.execute(
            "UPDATE chunks SET stored_sha256 = ? WHERE chunk_id = ?",
            ("0" * 64, prepared.chunk_ids[0]),
        )
        with pytest.raises(
            RecoveryConflictError,
            match="RECOVERY_MANIFEST_CATALOG_IDENTITY_CONFLICT",
        ):
            reconcile_sealed(layout=prepared.layout, catalog=catalog)
        assert catalog.state(prepared.chunk_ids[0]) is ChunkState.SEALED
        assert catalog.archive_transactions() == []


def test_archive_manifest_identity_conflict_fails_closed_without_rewrite(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    _advance_to(prepared, ChunkState.ARCHIVE_COPYING)
    external_before = _tree_bytes(prepared.target.root)
    manifest_path = next(prepared.layout.manifests.glob("*.manifest.json"))
    document: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
    document["m21_test_only_change"] = True
    manifest_path.write_text(
        json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with Catalog(prepared.layout.catalog) as catalog:
        transaction_count = len(catalog.archive_transactions())
        with pytest.raises(
            RecoveryConflictError,
            match="RECOVERY_ARCHIVE_TRANSACTION_IDENTITY_CONFLICT",
        ):
            reconcile_sealed(layout=prepared.layout, catalog=catalog)
        assert catalog.state(prepared.chunk_ids[0]) is ChunkState.ARCHIVE_COPYING
        assert len(catalog.archive_transactions()) == transaction_count

    assert _tree_bytes(prepared.target.root) == external_before


def test_archive_reservation_winning_after_sealed_commit_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = prepare_archive(tmp_path)
    manifest_path = next(prepared.layout.manifests.glob("*.manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    alternate_catalog_path = tmp_path / "alternate.sqlite"
    with Catalog(alternate_catalog_path) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )
        original_transition = catalog.transition
        reserved = False

        def racing_transition(
            chunk_id: str, to_state: ChunkState, **kwargs: Any
        ) -> None:
            nonlocal reserved
            original_transition(chunk_id, to_state, **kwargs)
            if to_state is ChunkState.SEALED and not reserved:
                reserved = True
                row = catalog.chunk(chunk_id)
                assert row is not None
                manager._reserve(row)

        monkeypatch.setattr(catalog, "transition", racing_transition)
        actions = reconcile_sealed(layout=prepared.layout, catalog=catalog)
        assert reserved is True
        assert [action.action for action in actions] == ["archive_state_preserved"]
        assert catalog.state(str(manifest["chunk_id"])) is ChunkState.ARCHIVE_COPYING
        assert len(catalog.archive_transactions()) == 1
        integrity = catalog._connection.execute("PRAGMA integrity_check").fetchone()
        assert integrity is not None and integrity[0] == "ok"


def test_archive_transaction_manifest_digest_matches_fixture(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    _advance_to(prepared, ChunkState.ARCHIVE_COPYING)
    with Catalog(prepared.layout.catalog) as catalog:
        transaction = catalog.archive_transaction_for_chunk(prepared.chunk_ids[0])
        assert transaction is not None
        manifest_path = prepared.layout.root / str(
            transaction["source_manifest_relative_path"]
        )
        assert hashlib.sha256(manifest_path.read_bytes()).hexdigest() == transaction[
            "source_manifest_sha256"
        ]


def test_multiprocess_recovery_and_archive_progress_without_reverse_transition(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=12, payload_bytes=64)
    context = multiprocessing.get_context("spawn")
    start_barrier = context.Barrier(3)
    recovery_result_path = tmp_path / "recovery-result.json"
    archive_result_path = tmp_path / "archive-result.json"
    recovery = context.Process(
        name="m21-3-recorder-recovery",
        target=_recovery_process,
        args=(
            str(prepared.layout.root),
            str(prepared.layout.catalog),
            start_barrier,
            str(recovery_result_path),
        ),
    )
    archive = context.Process(
        name="m21-3-archive-writer",
        target=_archive_process,
        args=(
            str(prepared.layout.root),
            str(prepared.layout.catalog),
            prepared.target.storage_id,
            prepared.target.volume_uuid,
            prepared.target.registered_relative_path,
            prepared.target.marker_nonce,
            str(prepared.target.root),
            start_barrier,
            str(archive_result_path),
        ),
    )
    recovery.start()
    archive.start()
    start_barrier.wait(timeout=30)
    recovery.join(timeout=60)
    archive.join(timeout=60)
    try:
        assert not recovery.is_alive()
        assert not archive.is_alive()
        assert recovery.exitcode == 0
        assert archive.exitcode == 0
        recovery_result = json.loads(
            recovery_result_path.read_text(encoding="utf-8")
        )
        archive_result = json.loads(archive_result_path.read_text(encoding="utf-8"))
        assert recovery_result["failures"] == []
        assert recovery_result["archive_states_preserved"] > 0
        assert archive_result == {"failures": [], "processed": 12}

        with Catalog(prepared.layout.catalog) as catalog:
            transactions = catalog.archive_transactions()
            assert len(transactions) == 12
            assert len({row["chunk_id"] for row in transactions}) == 12
            assert {row["state"] for row in transactions} == {
                ArchiveState.LOCAL_DELETED
            }
            assert all(
                catalog.state(chunk_id) is ChunkState.LOCAL_DELETED
                for chunk_id in prepared.chunk_ids
            )
            integrity = catalog._connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()
            assert integrity is not None and integrity[0] == "ok"
        assert list(prepared.layout.sealed.glob("*.bmdr.zst")) == []
    finally:
        if recovery.is_alive():
            recovery.terminate()
            recovery.join(timeout=5)
        if archive.is_alive():
            archive.terminate()
            archive.join(timeout=5)
