from __future__ import annotations

import hashlib
import io
import shutil
import sqlite3
import threading
from pathlib import Path
from typing import BinaryIO

import pytest

from binance_market_data_recorder.archive import (
    CatalogSnapshotError,
    CatalogSnapshotExporter,
    CatalogSnapshotStore,
    InProcessRemoteTransport,
    OpenSSHRemoteTransport,
    PostSessionArchiveWorkflow,
    PostSessionSnapshotError,
    RemoteArchiveSession,
    RemoteReceiveTarget,
    generate_archive_set_id,
)
from binance_market_data_recorder.storage.catalog import Catalog, RemoteArchiveState
from tests.archive_support import PreparedArchive, prepare_archive


def _target(prepared: PreparedArchive) -> RemoteReceiveTarget:
    return RemoteReceiveTarget(
        archive_set_id=generate_archive_set_id(),
        storage_id=prepared.target.storage_id,
        volume_uuid=prepared.target.volume_uuid,
        registered_relative_path=prepared.target.registered_relative_path,
        marker_nonce=prepared.target.marker_nonce,
        root=prepared.target.root,
    )


def _pending(root: Path) -> tuple[PreparedArchive, InProcessRemoteTransport, str]:
    prepared = prepare_archive(root / "vps", payload_bytes=4096)
    catalog = Catalog(prepared.layout.catalog)
    transport = InProcessRemoteTransport(layout=prepared.layout, catalog=catalog)
    result = RemoteArchiveSession(transport=transport, target=_target(prepared)).run_one(
        delete=False, session_id="12345678-1234-4234-9234-123456789abc"
    )
    assert result.receipt is not None
    return prepared, transport, result.receipt.receipt_id


def _close(transport: InProcessRemoteTransport) -> None:
    transport.catalog.close()


def test_live_read_only_open_without_wal_observes_later_commit_and_backs_up(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path):
        pass
    assert not path.with_name("catalog.sqlite-wal").exists()
    source = Catalog.open_live_read_only(path)
    assert source.live_read_only
    with Catalog(path) as writer:
        writer.record_operational_event(
            event_id="later", event_type="LATER", occurred_at_utc_ns=1, evidence={}
        )
    destination_path = tmp_path / "backup.sqlite"
    destination = sqlite3.connect(destination_path)
    try:
        source.backup_to(destination)
    finally:
        destination.close()
        source.close()
    with sqlite3.connect(destination_path) as snapshot:
        assert snapshot.execute(
            "SELECT event_type FROM operational_events WHERE event_id='later'"
        ).fetchone() == ("LATER",)


def test_real_online_backup_is_standalone_and_self_binds_pending_receipt(
    tmp_path: Path,
) -> None:
    prepared, transport, receipt_id = _pending(tmp_path)
    try:
        stream = CatalogSnapshotExporter(
            layout=prepared.layout
        ).open_catalog_snapshot(receipt_id, RemoteArchiveState.REMOTE_DELETE_PENDING)
        with stream:
            body = stream.read()
        path = tmp_path / "moved" / "catalog.sqlite"
        path.parent.mkdir()
        path.write_bytes(body)
        assert not path.with_name("catalog.sqlite-wal").exists()
        assert not path.with_name("catalog.sqlite-shm").exists()
        with Catalog(path, read_only=True) as snapshot:
            assert snapshot.integrity_check() == ("ok",)
            row = snapshot.remote_archive_transaction(receipt_id)
            assert row is not None
            assert row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
    finally:
        _close(transport)


def test_snapshot_store_streams_validates_hash_and_retains_latest_previous(
    tmp_path: Path,
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    try:
        store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
        first = store.snapshot_post_session(
            transport=transport,
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
        second = store.snapshot_post_session(
            transport=transport,
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
        third = store.snapshot_post_session(
            transport=transport,
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
        retention = store.current_retention()
        assert retention.generation == 3
        assert retention.latest is not None
        assert retention.previous is not None
        assert retention.latest.snapshot_id == third.snapshot_id
        assert retention.previous.snapshot_id == second.snapshot_id
        assert first.snapshot_id != second.snapshot_id != third.snapshot_id
        assert not first.snapshot_path.parent.exists()
        body = third.snapshot_path.read_bytes()
        assert third.manifest.stored_bytes == len(body)
        assert third.manifest.sha256 == hashlib.sha256(body).hexdigest()
        moved = tmp_path / "detached" / "catalog.sqlite"
        moved.parent.mkdir()
        shutil.copyfile(third.snapshot_path, moved)
        with Catalog(moved, read_only=True) as catalog:
            assert catalog.remote_archive_transaction(receipt_id) is not None
    finally:
        _close(transport)


def test_pending_lower_bound_accepts_later_deleted_snapshot(tmp_path: Path) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    try:
        transport.delete_authorized(receipt_id)
        result = CatalogSnapshotStore(
            workspace_root=(tmp_path / "offline").resolve()
        ).snapshot_post_session(
            transport=transport,
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
        assert result.manifest.required_remote_state == "REMOTE_DELETE_PENDING"
        assert result.manifest.observed_remote_state == "REMOTE_DELETED"
    finally:
        _close(transport)


def test_deleted_requirement_rejects_pending_and_does_not_publish(tmp_path: Path) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    try:
        store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
        with pytest.raises(CatalogSnapshotError, match="below required"):
            store.snapshot_post_session(
                transport=transport,
                receipt_id=receipt_id,
                required_state=RemoteArchiveState.REMOTE_DELETED,
            )
        assert store.current_retention().latest is None
        assert list(store.snapshots.iterdir()) == []
    finally:
        _close(transport)


class _BytesTransport:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def open_catalog_snapshot(
        self, receipt_id: str, required_state: RemoteArchiveState
    ) -> BinaryIO:
        return io.BytesIO(self.body)


class _CountingTransport:
    def __init__(self, delegate: InProcessRemoteTransport, *, fail_snapshot: bool) -> None:
        self.delegate = delegate
        self.fail_snapshot = fail_snapshot
        self.calls = {
            "select": 0,
            "receive": 0,
            "authorize": 0,
            "inspect": 0,
            "delete": 0,
            "snapshot": 0,
        }

    def select_oldest_source(self) -> object:
        self.calls["select"] += 1
        return self.delegate.select_oldest_source()

    def open_stored_bytes(self, source: object) -> BinaryIO:
        self.calls["receive"] += 1
        return self.delegate.open_stored_bytes(source)  # type: ignore[arg-type]

    def authorize_receipt(self, source: object, receipt_bytes: bytes) -> object:
        self.calls["authorize"] += 1
        return self.delegate.authorize_receipt(source, receipt_bytes)  # type: ignore[arg-type]

    def inspect_authority(self, receipt_id: str) -> object:
        self.calls["inspect"] += 1
        return self.delegate.inspect_authority(receipt_id)

    def delete_authorized(self, receipt_id: str) -> object:
        self.calls["delete"] += 1
        return self.delegate.delete_authorized(receipt_id)

    def open_catalog_snapshot(
        self, receipt_id: str, required_state: RemoteArchiveState
    ) -> BinaryIO:
        self.calls["snapshot"] += 1
        if self.fail_snapshot:
            raise CatalogSnapshotError("injected post-session snapshot failure")
        return self.delegate.open_catalog_snapshot(receipt_id, required_state)


def test_local_validation_queries_transferred_snapshot_not_live_catalog(
    tmp_path: Path,
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    unrelated = tmp_path / "unrelated.sqlite"
    with Catalog(unrelated):
        pass
    body = unrelated.read_bytes()
    try:
        store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
        with pytest.raises(CatalogSnapshotError, match="triggering receipt"):
            store.snapshot_post_session(
                transport=_BytesTransport(body),
                receipt_id=receipt_id,
                required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
            )
        assert store.current_retention().latest is None
    finally:
        _close(transport)


def test_committed_delete_snapshot_failure_retries_snapshot_only(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path / "vps", payload_bytes=4096)
    catalog = Catalog(prepared.layout.catalog)
    delegate = InProcessRemoteTransport(layout=prepared.layout, catalog=catalog)
    transport = _CountingTransport(delegate, fail_snapshot=True)
    session = RemoteArchiveSession(transport=transport, target=_target(prepared))  # type: ignore[arg-type]
    store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
    workflow = PostSessionArchiveWorkflow(
        session=session, snapshot_store=store, transport=transport
    )
    with pytest.raises(PostSessionSnapshotError) as failure:
        workflow.run_one(
            delete=True, session_id="12345678-1234-4234-9234-123456789abc"
        )
    error = failure.value
    assert error.code == "POST_SESSION_SNAPSHOT_FAILED"
    assert error.committed_remote_state is RemoteArchiveState.REMOTE_DELETED
    assert error.session_result.authority is not None
    assert error.session_result.authority.state is RemoteArchiveState.REMOTE_DELETED
    receipt_id = error.receipt_id
    assert catalog.remote_archive_transaction(receipt_id)["state"] == "REMOTE_DELETED"  # type: ignore[index]
    assert transport.calls == {
        "select": 1,
        "receive": 1,
        "authorize": 1,
        "inspect": 0,
        "delete": 1,
        "snapshot": 1,
    }
    session_counts = dict(transport.calls)
    transport.fail_snapshot = False
    retry = store.snapshot_post_session(
        transport=transport,
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETED,
    )
    assert retry.manifest.observed_remote_state == "REMOTE_DELETED"
    assert transport.calls["snapshot"] == 2
    for operation in ("select", "receive", "authorize", "inspect", "delete"):
        assert transport.calls[operation] == session_counts[operation]
    assert not prepared.layout.sealed.joinpath(
        Path(error.session_result.receipt.source_relative_path).name  # type: ignore[union-attr]
    ).exists()
    catalog.close()


def test_no_source_workflow_does_not_request_snapshot(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path / "vps", chunk_count=0)
    catalog = Catalog(prepared.layout.catalog)
    delegate = InProcessRemoteTransport(layout=prepared.layout, catalog=catalog)
    transport = _CountingTransport(delegate, fail_snapshot=True)
    result = PostSessionArchiveWorkflow(
        session=RemoteArchiveSession(transport=transport, target=_target(prepared)),  # type: ignore[arg-type]
        snapshot_store=CatalogSnapshotStore(
            workspace_root=(tmp_path / "offline").resolve()
        ),
        transport=transport,
    ).run_one(delete=True)
    assert not result.session.worked
    assert result.snapshot is None
    assert transport.calls["select"] == 1
    assert transport.calls["snapshot"] == 0
    catalog.close()


@pytest.mark.parametrize("mutation", ["truncate", "flip"])
def test_corrupt_transferred_sqlite_never_becomes_latest(
    tmp_path: Path, mutation: str
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    try:
        with transport.open_catalog_snapshot(
            receipt_id, RemoteArchiveState.REMOTE_DELETE_PENDING
        ) as stream:
            body = stream.read()
        changed = body[: len(body) // 2] if mutation == "truncate" else (
            body[:100] + bytes([body[100] ^ 1]) + body[101:]
        )
        store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
        with pytest.raises(CatalogSnapshotError):
            store.snapshot_post_session(
                transport=_BytesTransport(changed),
                receipt_id=receipt_id,
                required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
            )
        assert store.current_retention().latest is None
    finally:
        _close(transport)


@pytest.mark.parametrize("tamper", ["initial_event", "terminal_event", "receipt_bytes"])
def test_malformed_authoritative_evidence_never_publishes(
    tmp_path: Path, tamper: str
) -> None:
    _prepared, transport, receipt_id = _pending(tmp_path)
    try:
        transport.delete_authorized(receipt_id)
        with transport.open_catalog_snapshot(
            receipt_id, RemoteArchiveState.REMOTE_DELETED
        ) as stream:
            body = stream.read()
        changed_path = tmp_path / f"tampered-{tamper}.sqlite"
        changed_path.write_bytes(body)
        with sqlite3.connect(changed_path) as database:
            database.execute("PRAGMA journal_mode=DELETE")
            if tamper == "initial_event":
                database.execute(
                    "UPDATE remote_archive_events SET evidence_json='{}' "
                    "WHERE from_state IS NULL"
                )
            elif tamper == "terminal_event":
                database.execute(
                    "UPDATE remote_archive_events SET evidence_json='{}' "
                    "WHERE from_state='REMOTE_DELETE_PENDING'"
                )
            else:
                database.execute(
                    "UPDATE remote_archive_transactions SET receipt_bytes=?",
                    (b"invalid",),
                )
        store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
        with pytest.raises(CatalogSnapshotError):
            store.snapshot_post_session(
                transport=_BytesTransport(changed_path.read_bytes()),
                receipt_id=receipt_id,
                required_state=RemoteArchiveState.REMOTE_DELETED,
            )
        assert store.current_retention().latest is None
    finally:
        _close(transport)


def test_online_backup_remains_consistent_during_concurrent_writer(tmp_path: Path) -> None:
    prepared, transport, receipt_id = _pending(tmp_path)
    stop = threading.Event()

    def write() -> None:
        with Catalog(prepared.layout.catalog) as writer:
            ordinal = 0
            while not stop.is_set():
                writer.record_operational_event(
                    event_id=f"concurrent-{ordinal}",
                    event_type="CONCURRENT",
                    occurred_at_utc_ns=ordinal,
                    evidence={},
                )
                ordinal += 1

    thread = threading.Thread(target=write)
    thread.start()
    try:
        result = CatalogSnapshotStore(
            workspace_root=(tmp_path / "offline").resolve()
        ).snapshot_post_session(
            transport=transport,
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
        with Catalog(result.snapshot_path, read_only=True) as snapshot:
            assert snapshot.integrity_check() == ("ok",)
            assert snapshot.remote_archive_transaction(receipt_id) is not None
    finally:
        stop.set()
        thread.join(timeout=5)
        _close(transport)
    assert not thread.is_alive()


def _ssh(prepared: PreparedArchive, config: Path) -> OpenSSHRemoteTransport:
    return OpenSSHRemoteTransport(
        host_alias="test-vps",
        ssh_executable=str(Path(__file__).parents[1] / "remote_transport_ssh_shim.py"),
        remote_config_path=config.as_posix(),
        cleanup_timeout_seconds=2,
    )


@pytest.mark.parametrize(
    "fault", ["catalog_snapshot_partial", "catalog_snapshot_full_nonzero"]
)
def test_openssh_snapshot_stream_faults_never_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    prepared, inprocess, receipt_id = _pending(tmp_path)
    _close(inprocess)
    config = tmp_path / "remote.toml"
    config.write_text(f'[recorder]\ndata_root = "{prepared.layout.root}"\n')
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", fault)
    store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
    with pytest.raises(CatalogSnapshotError):
        store.snapshot_post_session(
            transport=_ssh(prepared, config),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
    assert store.current_retention().latest is None
    assert list(store.snapshots.iterdir()) == []


def test_openssh_real_hidden_snapshot_cli_success_and_stderr_pressure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, inprocess, receipt_id = _pending(tmp_path)
    _close(inprocess)
    config = tmp_path / "remote.toml"
    config.write_text(f'[recorder]\ndata_root = "{prepared.layout.root}"\n')
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "catalog_snapshot_stderr_pressure")
    result = CatalogSnapshotStore(
        workspace_root=(tmp_path / "offline").resolve()
    ).snapshot_post_session(
        transport=_ssh(prepared, config),
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    assert result.snapshot_path.is_file()


def test_remote_sigkill_residue_is_not_authority_and_retry_cleans_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, inprocess, receipt_id = _pending(tmp_path)
    config = tmp_path / "remote.toml"
    config.write_text(f'[recorder]\ndata_root = "{prepared.layout.root}"\n')
    stage_root = prepared.layout.state / "catalog-snapshot-staging"
    monkeypatch.setenv("BMDR_SSH_SHIM_FAULT", "catalog_snapshot_kill")
    monkeypatch.setenv("BMDR_SSH_SHIM_REMOTE_STAGE_ROOT", str(stage_root))
    store = CatalogSnapshotStore(workspace_root=(tmp_path / "offline").resolve())
    before = inprocess.inspect_authority(receipt_id)
    with pytest.raises(CatalogSnapshotError):
        store.snapshot_post_session(
            transport=_ssh(prepared, config),
            receipt_id=receipt_id,
            required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
        )
    assert store.current_retention().latest is None
    assert before == inprocess.inspect_authority(receipt_id)
    assert stage_root.is_dir() and list(stage_root.iterdir())
    monkeypatch.delenv("BMDR_SSH_SHIM_FAULT")
    result = store.snapshot_post_session(
        transport=inprocess,
        receipt_id=receipt_id,
        required_state=RemoteArchiveState.REMOTE_DELETE_PENDING,
    )
    assert result.snapshot_path.is_file()
    assert list(stage_root.iterdir()) == []
    _close(inprocess)
