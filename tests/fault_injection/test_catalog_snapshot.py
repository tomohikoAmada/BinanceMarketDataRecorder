from __future__ import annotations

import io
import json
import multiprocessing
import os
import signal
import stat
import uuid
from pathlib import Path
from typing import Any, BinaryIO

import pytest

import binance_market_data_recorder.archive.catalog_snapshot as snapshot_module
from binance_market_data_recorder.archive import (
    CatalogSnapshotError,
    CatalogSnapshotExporter,
    CatalogSnapshotStore,
    InProcessRemoteTransport,
    RemoteArchiveSession,
    RemoteReceiveTarget,
    generate_archive_set_id,
)
from binance_market_data_recorder.storage.catalog import Catalog, RemoteArchiveState
from tests.archive_support import PreparedArchive, prepare_archive


class _BytesTransport:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def open_catalog_snapshot(
        self, receipt_id: str, required_state: RemoteArchiveState
    ) -> BinaryIO:
        return io.BytesIO(self.body)


def _sigkill_snapshot_child(
    workspace: str, body: bytes, receipt_id: str, kill_point: str
) -> None:
    def fault(point: str) -> None:
        if point == kill_point:
            os.kill(os.getpid(), signal.SIGKILL)

    CatalogSnapshotStore(
        workspace_root=Path(workspace), fault_hook=fault
    ).snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )


def _serialized_snapshot_child(
    workspace: str,
    body: bytes,
    receipt_id: str,
    ready: Any,
    start: Any,
    attempted: Any,
    lock_acquired: Any,
    continue_transaction: Any | None,
    cleanup_reached: Any | None,
    release_lock: Any | None,
    results: Any,
) -> None:
    def fault(point: str) -> None:
        if point == "after_workspace_writer_lock_acquired":
            lock_acquired.set()
            if continue_transaction is not None and not continue_transaction.wait(10):
                raise RuntimeError("timed out before serialized transaction")
        if point == "after_obsolete_cleanup_before_parent_fsync":
            if cleanup_reached is not None:
                cleanup_reached.set()
            if release_lock is not None and not release_lock.wait(10):
                raise RuntimeError("timed out before writer lock release")

    try:
        store = CatalogSnapshotStore(
            workspace_root=Path(workspace), fault_hook=fault
        )
        ready.set()
        if not start.wait(10):
            raise RuntimeError("timed out before snapshot start")
        attempted.set()
        result = store.snapshot_post_session(
            transport=_BytesTransport(body),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
        results.put(("ok", result.snapshot_id, result.retention_generation))
    except BaseException as exc:
        results.put(("error", repr(exc)))


def _first_use_snapshot_child(
    workspace: str,
    body: bytes,
    receipt_id: str,
    start: Any,
    attempted: Any,
    initialization_lock_acquired: Any,
    release_initialization: Any | None,
    results: Any,
) -> None:
    def fault(point: str) -> None:
        if point == "after_workspace_initialization_lock_acquired":
            initialization_lock_acquired.set()
            if release_initialization is not None and not release_initialization.wait(10):
                raise RuntimeError("timed out before initialization release")

    try:
        if not start.wait(10):
            raise RuntimeError("timed out before construction")
        attempted.set()
        result = CatalogSnapshotStore(
            workspace_root=Path(workspace), fault_hook=fault
        ).snapshot_post_session(
            transport=_BytesTransport(body),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
        results.put(("ok", result.snapshot_id, result.retention_generation))
    except BaseException as exc:
        results.put(("error", repr(exc)))


def _pending(root: Path) -> tuple[PreparedArchive, InProcessRemoteTransport, str]:
    prepared = prepare_archive(root / "vps", payload_bytes=4096)
    catalog = Catalog(prepared.layout.catalog)
    transport = InProcessRemoteTransport(layout=prepared.layout, catalog=catalog)
    target = RemoteReceiveTarget(
        archive_set_id=generate_archive_set_id(),
        storage_id=prepared.target.storage_id,
        volume_uuid=prepared.target.volume_uuid,
        registered_relative_path=prepared.target.registered_relative_path,
        marker_nonce=prepared.target.marker_nonce,
        root=prepared.target.root,
    )
    result = RemoteArchiveSession(transport=transport, target=target).run_one(
        delete=False, session_id="12345678-1234-4234-9234-123456789abc"
    )
    assert result.receipt is not None
    return prepared, transport, result.receipt.receipt_id


def _snapshot_bytes(
    transport: InProcessRemoteTransport, receipt_id: str
) -> bytes:
    with transport.open_catalog_snapshot(
        receipt_id, RemoteArchiveState.REMOTE_DELETE_PENDING
    ) as stream:
        return stream.read()


def test_snapshot_file_fsync_failure_leaves_old_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())

    def fail_fsync(_descriptor: int) -> None:
        raise OSError("injected file fsync failure")

    monkeypatch.setattr(os, "fsync", fail_fsync)
    with pytest.raises(CatalogSnapshotError, match="fsync"):
        store.snapshot_post_session(
            transport=_BytesTransport(body),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
    assert store.current_retention().latest is None
    transport.catalog.close()


def test_directory_fsync_failure_leaves_old_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())

    def fail_directory(_path: Path) -> None:
        raise OSError("injected directory fsync failure")

    monkeypatch.setattr(snapshot_module, "fsync_directory", fail_directory)
    with pytest.raises(CatalogSnapshotError, match="directory fsync"):
        store.snapshot_post_session(
            transport=_BytesTransport(body),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
    assert store.current_retention().latest is None
    transport.catalog.close()


def test_manifest_fsync_window_never_publishes(tmp_path: Path) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)

    def fault(point: str) -> None:
        if point == "after_manifest_file_fsync":
            raise OSError("injected manifest durability failure")

    store = CatalogSnapshotStore(
        workspace_root=(tmp_path / "offline").resolve(), fault_hook=fault
    )
    with pytest.raises(CatalogSnapshotError, match="manifest"):
        store.snapshot_post_session(
            transport=_BytesTransport(body),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
    assert store.current_retention().latest is None
    transport.catalog.close()


def test_first_retention_slot_new_second_old_recovers_highest_valid(
    tmp_path: Path,
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    workspace = (tmp_path / "offline").resolve()
    store = CatalogSnapshotStore(workspace_root=workspace)
    first = store.snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )

    def fault(point: str) -> None:
        if point == "after_first_retention_parent_fsync":
            raise RuntimeError("simulated process death window")

    interrupted = CatalogSnapshotStore(workspace_root=workspace, fault_hook=fault)
    with pytest.raises(CatalogSnapshotError, match="process death"):
        interrupted.snapshot_post_session(
            transport=_BytesTransport(body),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
    recovered = CatalogSnapshotStore(workspace_root=workspace).current_retention()
    assert recovered.generation == 2
    assert recovered.latest is not None
    assert recovered.latest.snapshot_id != first.snapshot_id
    transport.catalog.close()


def test_one_corrupt_slot_recovers_and_next_update_heals_mirror(tmp_path: Path) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    workspace = (tmp_path / "offline").resolve()
    store = CatalogSnapshotStore(workspace_root=workspace)
    store.snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    (store.root / "retention-0.json").write_bytes(b"corrupt")
    assert CatalogSnapshotStore(workspace_root=workspace).current_retention().generation == 1
    healed = CatalogSnapshotStore(workspace_root=workspace)
    healed.snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    assert (healed.root / "retention-0.json").read_bytes() == (
        healed.root / "retention-1.json"
    ).read_bytes()
    transport.catalog.close()


def test_both_slots_corrupt_after_initialization_fail_closed(tmp_path: Path) -> None:
    workspace = (tmp_path / "offline").resolve()
    store = CatalogSnapshotStore(workspace_root=workspace)
    for name in ("retention-0.json", "retention-1.json"):
        (store.root / name).write_bytes(b"corrupt")
    with pytest.raises(CatalogSnapshotError, match="both initialized"):
        CatalogSnapshotStore(workspace_root=workspace).current_retention()


def test_cleanup_failure_is_nonfatal_and_leaves_extra_generation(tmp_path: Path) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    workspace = (tmp_path / "offline").resolve()
    store = CatalogSnapshotStore(workspace_root=workspace)
    first = store.snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    store.snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )

    def cleanup_fault(point: str) -> None:
        if point == "before_obsolete_cleanup":
            raise OSError("cleanup unavailable")

    third = CatalogSnapshotStore(
        workspace_root=workspace, fault_hook=cleanup_fault
    ).snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    assert third.retention_generation == 3
    assert first.snapshot_path.parent.exists()
    transport.catalog.close()


def test_same_workspace_real_process_writers_serialize_full_transaction(
    tmp_path: Path,
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    workspace = (tmp_path / "offline").resolve()
    CatalogSnapshotStore(workspace_root=workspace)
    context = multiprocessing.get_context("fork")
    results = context.Queue()

    a_ready, b_ready = context.Event(), context.Event()
    a_start, b_start = context.Event(), context.Event()
    a_attempted, b_attempted = context.Event(), context.Event()
    a_acquired, b_acquired = context.Event(), context.Event()
    continue_a, cleanup_reached, release_a = (
        context.Event(),
        context.Event(),
        context.Event(),
    )
    process_a = context.Process(
        target=_serialized_snapshot_child,
        args=(
            str(workspace),
            body,
            receipt_id,
            a_ready,
            a_start,
            a_attempted,
            a_acquired,
            continue_a,
            cleanup_reached,
            release_a,
            results,
        ),
    )
    process_b = context.Process(
        target=_serialized_snapshot_child,
        args=(
            str(workspace),
            body,
            receipt_id,
            b_ready,
            b_start,
            b_attempted,
            b_acquired,
            None,
            None,
            None,
            results,
        ),
    )
    process_a.start()
    process_b.start()
    assert a_ready.wait(10) and b_ready.wait(10)
    a_start.set()
    assert a_attempted.wait(10) and a_acquired.wait(10)
    b_start.set()
    assert b_attempted.wait(10)
    assert not b_acquired.wait(0.5)
    continue_a.set()
    assert cleanup_reached.wait(10)
    assert not b_acquired.is_set()
    release_a.set()
    assert b_acquired.wait(10)
    process_a.join(10)
    process_b.join(10)
    assert process_a.exitcode == 0
    assert process_b.exitcode == 0

    outcomes = [results.get(timeout=10), results.get(timeout=10)]
    assert all(outcome[0] == "ok" for outcome in outcomes), outcomes
    generations = {outcome[2]: outcome[1] for outcome in outcomes}
    assert set(generations) == {1, 2}
    fresh = CatalogSnapshotStore(workspace_root=workspace)
    retention = fresh.current_retention()
    assert retention.generation == 2
    assert retention.latest is not None
    assert retention.previous is not None
    assert retention.latest.snapshot_id == generations[2]
    assert retention.previous.snapshot_id == generations[1]
    assert retention.latest != retention.previous
    slot_bodies = [
        (fresh.root / name).read_bytes()
        for name in ("retention-0.json", "retention-1.json")
    ]
    assert slot_bodies[0] == slot_bodies[1] == retention.canonical_bytes()
    assert {path.name for path in fresh.snapshots.iterdir()} == {
        retention.latest.snapshot_id,
        retention.previous.snapshot_id,
    }
    transport.catalog.close()


def test_same_uninitialized_workspace_real_process_constructors_serialize(
    tmp_path: Path,
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    workspace = (tmp_path / "offline").resolve()
    context = multiprocessing.get_context("fork")
    results = context.Queue()
    a_start, b_start = context.Event(), context.Event()
    a_attempted, b_attempted = context.Event(), context.Event()
    a_acquired, b_acquired = context.Event(), context.Event()
    release_a = context.Event()
    process_a = context.Process(
        target=_first_use_snapshot_child,
        args=(
            str(workspace),
            body,
            receipt_id,
            a_start,
            a_attempted,
            a_acquired,
            release_a,
            results,
        ),
    )
    process_b = context.Process(
        target=_first_use_snapshot_child,
        args=(
            str(workspace),
            body,
            receipt_id,
            b_start,
            b_attempted,
            b_acquired,
            None,
            results,
        ),
    )
    process_a.start()
    process_b.start()
    a_start.set()
    assert a_attempted.wait(10) and a_acquired.wait(10)
    b_start.set()
    assert b_attempted.wait(10)
    assert not b_acquired.wait(0.5)
    release_a.set()
    assert b_acquired.wait(10)
    process_a.join(10)
    process_b.join(10)
    assert process_a.exitcode == 0
    assert process_b.exitcode == 0
    outcomes = [results.get(timeout=10), results.get(timeout=10)]
    assert all(outcome[0] == "ok" for outcome in outcomes), outcomes

    fresh = CatalogSnapshotStore(workspace_root=workspace)
    retention = fresh.current_retention()
    assert retention.generation == 2
    assert retention.latest is not None and retention.previous is not None
    assert retention.latest != retention.previous
    assert (fresh.root / ".initialized").is_file()
    assert (fresh.root / "retention-0.json").read_bytes() == (
        fresh.root / "retention-1.json"
    ).read_bytes()
    transport.catalog.close()


def test_workspace_writer_lock_is_fixed_safe_regular_and_persistent(
    tmp_path: Path,
) -> None:
    workspace = (tmp_path / "offline").resolve()
    lock_path = workspace / ".catalog-snapshot-writer.lock"
    first = CatalogSnapshotStore(workspace_root=workspace)
    identity = lock_path.stat()
    assert stat.S_ISREG(identity.st_mode)
    assert stat.S_IMODE(identity.st_mode) == 0o600
    CatalogSnapshotStore(workspace_root=workspace)
    assert lock_path.stat().st_ino == identity.st_ino
    assert first.current_retention().generation == 0


def test_workspace_writer_lock_rejects_symlink(tmp_path: Path) -> None:
    workspace = (tmp_path / "offline").resolve()
    workspace.mkdir()
    target = tmp_path / "outside.lock"
    target.write_bytes(b"outside")
    (workspace / ".catalog-snapshot-writer.lock").symlink_to(target)
    with pytest.raises(CatalogSnapshotError, match="writer lock is unsafe"):
        CatalogSnapshotStore(workspace_root=workspace)


def test_remote_cleanup_preserves_active_and_unknown_but_removes_owned_inactive(
    tmp_path: Path,
) -> None:
    prepared, transport, receipt_id = _pending(tmp_path)
    exporter = CatalogSnapshotExporter(layout=prepared.layout)
    active = exporter.open_catalog_snapshot(
        receipt_id, RemoteArchiveState.REMOTE_DELETE_PENDING
    )
    active_stage = next(exporter.staging_root.iterdir())
    unknown = exporter.staging_root / "unknown"
    unknown.mkdir()
    inactive_id = str(uuid.uuid4())
    inactive = exporter.staging_root / inactive_id
    inactive.mkdir(mode=0o700)
    (inactive / ".catalog-snapshot-owner.json").write_bytes(
        (
            json.dumps(
                {"schema": "catalog-snapshot-staging-owner.v1", "stage_id": inactive_id},
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode()
    )
    os.close(os.open(inactive / ".active.lock", os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600))
    second = exporter.open_catalog_snapshot(
        receipt_id, RemoteArchiveState.REMOTE_DELETE_PENDING
    )
    assert active_stage.exists()
    assert unknown.exists()
    assert not inactive.exists()
    second.close()
    assert active_stage.exists()
    active.close()
    assert not active_stage.exists()
    assert unknown.exists()
    transport.catalog.close()


@pytest.mark.parametrize(
    ("kill_point", "expected_generation"),
    [
        ("after_generation_parent_fsync", 1),
        ("after_first_retention_parent_fsync", 2),
        ("before_obsolete_cleanup", 2),
    ],
)
def test_real_sigkill_retention_windows_recover_old_or_new_authority(
    tmp_path: Path, kill_point: str, expected_generation: int
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    workspace = (tmp_path / "offline").resolve()
    store = CatalogSnapshotStore(workspace_root=workspace)
    store.snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    process = multiprocessing.get_context("fork").Process(
        target=_sigkill_snapshot_child,
        args=(str(workspace), body, receipt_id, kill_point),
    )
    process.start()
    process.join(10)
    assert process.exitcode == -signal.SIGKILL
    recovered = CatalogSnapshotStore(workspace_root=workspace).current_retention()
    assert recovered.generation == expected_generation
    assert recovered.latest is not None
    transport.catalog.close()


def test_real_sigkill_releases_workspace_lock_for_next_writer(tmp_path: Path) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    body = _snapshot_bytes(transport, receipt_id)
    workspace = (tmp_path / "offline").resolve()
    CatalogSnapshotStore(workspace_root=workspace)
    process = multiprocessing.get_context("fork").Process(
        target=_sigkill_snapshot_child,
        args=(
            str(workspace),
            body,
            receipt_id,
            "after_workspace_writer_lock_acquired",
        ),
    )
    process.start()
    process.join(10)
    assert process.exitcode == -signal.SIGKILL

    result = CatalogSnapshotStore(workspace_root=workspace).snapshot_post_session(
        transport=_BytesTransport(body),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    retention = CatalogSnapshotStore(workspace_root=workspace).current_retention()
    assert result.retention_generation == 1
    assert retention.generation == 1
    assert retention.latest is not None
    assert retention.latest.snapshot_id == result.snapshot_id
    transport.catalog.close()
