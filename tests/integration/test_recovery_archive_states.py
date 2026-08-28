from __future__ import annotations

import hashlib
import json
import multiprocessing
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest

import binance_market_data_recorder.spool.recovery as recovery_module
from binance_market_data_recorder.archive import (
    ArchiveError,
    ArchiveManager,
    ArchiveTarget,
)
from binance_market_data_recorder.spool.recovery import (
    RecoveryConflictError,
    reconcile_sealed,
    recover_storage,
)
from binance_market_data_recorder.spool.seal import (
    _seal_clean_writer,
    validate_sealed_artifact,
)
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import (
    ArchiveState,
    Catalog,
    ChunkState,
)
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from binance_market_data_recorder.storage.macos import StorageRegistry, VolumeInfo
from tests.archive_support import FixedVolumes, PreparedArchive, prepare_archive
from tests.factories import event

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


def _prepare_retained_active_source(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[PreparedArchive, Path]:
    layout = ensure_storage_layout(root / "internal")
    mountpoint = root / "external-volume"
    target_root = mountpoint / "QuantData" / "BinanceRecorder"
    target_root.mkdir(parents=True)
    volume = VolumeInfo(
        disk_id="disk9s1",
        volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=mountpoint,
        writable=True,
        internal=False,
        removable=True,
        total_bytes=100 * 1024**3,
        free_bytes=90 * 1024**3,
        observed_at_utc_ns=1,
    )
    with Catalog(layout.catalog) as catalog:
        registration = StorageRegistry(
            catalog=catalog, volumes=FixedVolumes(volume)
        ).register(target_root)
        target_row = catalog.storage_targets()[0]
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="archive-retained-source-fixture",
            collector_version="0.1.0+test",
            durability_interval_seconds=0,
            created_at_utc_ns=1_700_000_000_000_000_000,
        )
        writer.append(event(1, payload=b"retained-active-source"))
        writer.close()
        original_unlink = Path.unlink
        injected = False

        def fail_active_unlink(path: Path, *args: Any, **kwargs: Any) -> None:
            nonlocal injected
            if path == writer.path and not injected:
                injected = True
                raise OSError("injected retained active source")
            original_unlink(path, *args, **kwargs)

        with monkeypatch.context() as fault:
            fault.setattr(Path, "unlink", fail_active_unlink)
            with pytest.raises(OSError, match="injected retained active source"):
                _seal_clean_writer(writer, layout=layout, catalog=catalog)
        chunk_id = str(writer.header.chunk_id)
        assert injected is True
        assert catalog.state(chunk_id) is ChunkState.SEALED
        assert writer.path.exists()

    target = ArchiveTarget(
        storage_id=str(registration["storage_id"]),
        volume_uuid=volume.volume_uuid,
        registered_relative_path=str(target_row["relative_path"]),
        marker_nonce=str(target_row["marker_nonce"]),
        root=target_root,
    )
    return PreparedArchive(layout, target, (chunk_id,)), writer.path


def test_sealed_missing_artifact_preserves_last_retained_raw(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, retained = _prepare_retained_active_source(tmp_path, monkeypatch)
    chunk_id = prepared.chunk_ids[0]
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(chunk_id)
        assert row is not None
        sealed = prepared.layout.root / str(row["sealed_path"])
        sealed.unlink()

        with pytest.raises(
            RecoveryConflictError,
            match="RECOVERY_RETAINED_SOURCE_SEALED_ARTIFACT_MISSING",
        ):
            recover_storage(layout=prepared.layout, catalog=catalog)

        assert retained.exists()
        assert not sealed.exists()
        assert catalog.state(chunk_id) is ChunkState.SEALED
        assert catalog.archive_transaction_for_chunk(chunk_id) is None
        assert catalog.remote_archive_transaction_for_chunk(chunk_id) is None


def test_sealed_artifact_is_fully_validated_before_retained_source_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, retained = _prepare_retained_active_source(tmp_path, monkeypatch)
    chunk_id = prepared.chunk_ids[0]
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(chunk_id)
        assert row is not None
        sealed = prepared.layout.root / str(row["sealed_path"])
        operations: list[str] = []
        real_validate = validate_sealed_artifact
        real_unlink = Path.unlink

        def tracked_validate(
            selected: Path, manifest: dict[str, object]
        ) -> None:
            real_validate(selected, manifest)
            if selected == sealed:
                operations.append("sealed_validated")

        def tracked_unlink(path: Path, *args: Any, **kwargs: Any) -> None:
            if path == retained:
                operations.append("retained_unlinked")
            real_unlink(path, *args, **kwargs)

        monkeypatch.setattr(
            recovery_module, "validate_sealed_artifact", tracked_validate
        )
        monkeypatch.setattr(Path, "unlink", tracked_unlink)

        actions = recover_storage(layout=prepared.layout, catalog=catalog)

        assert operations == ["sealed_validated", "retained_unlinked"]
        assert not retained.exists()
        assert sealed.exists()
        assert catalog.state(chunk_id) is ChunkState.SEALED
        assert any(
            action.action == "seal_completed_after_crash" for action in actions
        )


def test_sealed_corrupt_artifact_preserves_retained_raw(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, retained = _prepare_retained_active_source(tmp_path, monkeypatch)
    chunk_id = prepared.chunk_ids[0]
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(chunk_id)
        assert row is not None
        sealed = prepared.layout.root / str(row["sealed_path"])
        sealed.write_bytes(b"corrupt-sealed-artifact")

        with pytest.raises(
            RecoveryConflictError,
            match="RECOVERY_RETAINED_SOURCE_SEALED_ARTIFACT_INVALID",
        ):
            recover_storage(layout=prepared.layout, catalog=catalog)

        assert retained.exists()
        assert sealed.exists()
        assert catalog.state(chunk_id) is ChunkState.SEALED
        assert catalog.archive_transaction_for_chunk(chunk_id) is None
        assert catalog.remote_archive_transaction_for_chunk(chunk_id) is None


def test_archive_copying_retained_active_source_converges_without_lifecycle_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, retained = _prepare_retained_active_source(tmp_path, monkeypatch)
    chunk_id = prepared.chunk_ids[0]
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )

        def stop_after_reserve(point: str, _path: Path | None) -> None:
            if point == "after_reserve":
                raise RuntimeError("reserved before retained-source recovery")

        manager.fault_hook = stop_after_reserve
        with pytest.raises(RuntimeError, match="reserved before"):
            manager.run_once()
        assert catalog.state(chunk_id) is ChunkState.ARCHIVE_COPYING
        transaction_before = catalog.archive_transaction_for_chunk(chunk_id)
        assert transaction_before is not None
        transaction_count = len(catalog.archive_transactions())
        transition_count = catalog.transition_count(chunk_id)
        sealed_before = _tree_bytes(prepared.layout.sealed)
        manifests_before = _tree_bytes(prepared.layout.manifests)
        external_before = _tree_bytes(prepared.target.root)

        scans = 0
        real_scan = cast(
            Callable[[Path], Any],
            recovery_module.__dict__["scan_chunk"],
        )

        def counted_scan(path: Path) -> Any:
            nonlocal scans
            if path == retained:
                scans += 1
            return real_scan(path)

        monkeypatch.setattr(recovery_module, "scan_chunk", counted_scan)
        first = recover_storage(layout=prepared.layout, catalog=catalog)
        assert scans >= 2
        assert not retained.exists()
        assert any(
            action.action == "archive_retained_source_removed"
            for action in first
        )
        assert catalog.state(chunk_id) is ChunkState.ARCHIVE_COPYING
        assert catalog.archive_transaction_for_chunk(chunk_id) == transaction_before
        assert len(catalog.archive_transactions()) == transaction_count
        assert catalog.transition_count(chunk_id) == transition_count
        assert _tree_bytes(prepared.layout.sealed) == sealed_before
        assert _tree_bytes(prepared.layout.manifests) == manifests_before
        assert _tree_bytes(prepared.target.root) == external_before

        second = recover_storage(layout=prepared.layout, catalog=catalog)
        assert not any(
            action.action == "archive_retained_source_removed"
            for action in second
        )
        assert catalog.state(chunk_id) is ChunkState.ARCHIVE_COPYING
        assert catalog.archive_transaction_for_chunk(chunk_id) == transaction_before
        assert len(catalog.archive_transactions()) == transaction_count


@pytest.mark.parametrize(
    "state",
    [
        ChunkState.ARCHIVE_VERIFYING,
        ChunkState.ARCHIVED_VERIFIED,
        ChunkState.LOCAL_DELETE_PENDING,
        ChunkState.LOCAL_DELETED,
    ],
)
def test_later_archive_states_remove_only_proven_retained_active_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: ChunkState,
) -> None:
    prepared, retained = _prepare_retained_active_source(tmp_path, monkeypatch)
    _advance_to(prepared, state)
    chunk_id = prepared.chunk_ids[0]
    sealed_before = _tree_bytes(prepared.layout.sealed)
    manifests_before = _tree_bytes(prepared.layout.manifests)
    external_before = _tree_bytes(prepared.target.root)

    with Catalog(prepared.layout.catalog) as catalog:
        transaction_before = catalog.archive_transaction_for_chunk(chunk_id)
        assert transaction_before is not None
        transaction_count = len(catalog.archive_transactions())
        transition_count = catalog.transition_count(chunk_id)
        actions = recover_storage(layout=prepared.layout, catalog=catalog)
        assert not retained.exists()
        assert any(
            action.action == "archive_retained_source_removed"
            for action in actions
        )
        assert catalog.state(chunk_id) is state
        assert catalog.archive_transaction_for_chunk(chunk_id) == transaction_before
        assert len(catalog.archive_transactions()) == transaction_count
        assert catalog.transition_count(chunk_id) == transition_count

        recover_storage(layout=prepared.layout, catalog=catalog)
        assert catalog.state(chunk_id) is state
        assert catalog.archive_transaction_for_chunk(chunk_id) == transaction_before
        assert len(catalog.archive_transactions()) == transaction_count

    assert _tree_bytes(prepared.layout.sealed) == sealed_before
    assert _tree_bytes(prepared.layout.manifests) == manifests_before
    assert _tree_bytes(prepared.target.root) == external_before


def test_retained_source_cleanup_rereads_archive_reservation_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, retained = _prepare_retained_active_source(tmp_path, monkeypatch)
    chunk_id = prepared.chunk_ids[0]
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )
        original_snapshot = catalog.source_lifecycle_snapshot
        reserved = False
        reserved_transaction: dict[str, object] | None = None
        transition_count = catalog.transition_count(chunk_id)

        def racing_snapshot(
            selected_chunk_id: str,
        ) -> tuple[
            dict[str, object] | None,
            dict[str, object] | None,
            dict[str, object] | None,
        ]:
            nonlocal reserved, reserved_transaction
            snapshot = original_snapshot(selected_chunk_id)
            row, transaction, remote = snapshot
            if (
                selected_chunk_id == chunk_id
                and not reserved
                and row is not None
                and ChunkState(str(row["state"])) is ChunkState.SEALED
                and transaction is None
                and remote is None
            ):
                reserved = True
                reserved_transaction = manager._reserve(row)
            return snapshot

        monkeypatch.setattr(catalog, "source_lifecycle_snapshot", racing_snapshot)
        monkeypatch.setattr(
            recovery_module,
            "seal_partial",
            lambda *args, **kwargs: pytest.fail(
                "archive reservation must not re-enter the seal protocol"
            ),
        )
        actions = recover_storage(layout=prepared.layout, catalog=catalog)
        assert reserved is True
        assert reserved_transaction is not None
        assert not retained.exists()
        assert any(
            action.action == "archive_retained_source_removed"
            for action in actions
        )
        assert not any(
            action.action == "seal_completed_after_crash" for action in actions
        )
        assert catalog.state(chunk_id) is ChunkState.ARCHIVE_COPYING
        assert (
            catalog.archive_transaction_for_chunk(chunk_id)
            == reserved_transaction
        )
        assert len(catalog.archive_transactions()) == 1
        assert catalog.transition_count(chunk_id) == transition_count + 1


def test_archive_successor_retained_source_requires_exact_raw_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, retained = _prepare_retained_active_source(tmp_path, monkeypatch)
    chunk_id = prepared.chunk_ids[0]
    _advance_to(prepared, ChunkState.ARCHIVE_COPYING)

    alternate_layout = ensure_storage_layout(tmp_path / "alternate")
    with Catalog(alternate_layout.catalog) as alternate_catalog:
        alternate = RawChunkWriter(
            layout=alternate_layout,
            catalog=alternate_catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="archive-retained-source-fixture",
            collector_version="0.1.0+test",
            durability_interval_seconds=0,
            chunk_id=UUID(chunk_id),
            created_at_utc_ns=1_700_000_000_000_000_000,
        )
        alternate.append(event(2, payload=b"different-valid-raw-source"))
        alternate.close()
        retained.write_bytes(alternate.path.read_bytes())

    with Catalog(prepared.layout.catalog) as catalog:
        transaction_before = catalog.archive_transaction_for_chunk(chunk_id)
        assert transaction_before is not None
        with pytest.raises(
            RecoveryConflictError,
            match="RECOVERY_RETAINED_SOURCE_RAW_IDENTITY_CONFLICT",
        ):
            recover_storage(layout=prepared.layout, catalog=catalog)
        assert retained.exists()
        assert catalog.state(chunk_id) is ChunkState.ARCHIVE_COPYING
        assert catalog.archive_transaction_for_chunk(chunk_id) == transaction_before
        assert len(catalog.archive_transactions()) == 1


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
