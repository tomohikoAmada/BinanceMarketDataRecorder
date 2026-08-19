from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from binance_market_data_recorder.archive.remote_receive import (
    RemoteArchiveReceipt,
    RemoteReceiveTarget,
)
from binance_market_data_recorder.archive.remote_source import (
    RemoteSourceExporter,
    RemoteSourceSelection,
)
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import PreparedArchive, prepare_archive


@dataclass(frozen=True, slots=True)
class RemoteAuthorizationFixture:
    prepared: PreparedArchive
    selections: tuple[RemoteSourceSelection, ...]
    target: RemoteReceiveTarget


def prepare_remote_authorization(
    root: Path, *, chunk_count: int = 1
) -> RemoteAuthorizationFixture:
    prepared = prepare_archive(root.resolve(), chunk_count=chunk_count, payload_bytes=128)
    with Catalog(prepared.layout.catalog) as catalog:
        exporter = RemoteSourceExporter(layout=prepared.layout, catalog=catalog)
        selections = tuple(exporter.select_chunk(chunk) for chunk in prepared.chunk_ids)
    target = RemoteReceiveTarget(
        archive_set_id="archive-set-m22-4a",
        storage_id=prepared.target.storage_id,
        volume_uuid=prepared.target.volume_uuid,
        registered_relative_path=prepared.target.registered_relative_path,
        marker_nonce=prepared.target.marker_nonce,
        root=prepared.target.root,
    )
    return RemoteAuthorizationFixture(prepared, selections, target)


def build_receipt(
    fixture: RemoteAuthorizationFixture,
    *,
    ordinal: int = 0,
    session_id: str = "12345678-1234-4234-9234-123456789abc",
) -> RemoteArchiveReceipt:
    selection = fixture.selections[ordinal]
    return RemoteArchiveReceipt.build(
        selection=selection,
        target=fixture.target,
        session_id=session_id,
        artifact_relative_path=f"raw/{selection.sealed_path.name}",
        archive_set_entry_sha256="e" * 64,
    )


def source_snapshot(
    fixture: RemoteAuthorizationFixture, *, ordinal: int = 0
) -> tuple[bytes, bytes, tuple[str, ...]]:
    selection = fixture.selections[ordinal]
    return (
        selection.sealed_path.read_bytes(),
        selection.manifest_path.read_bytes(),
        tuple(sorted(path.name for path in selection.sealed_path.parent.iterdir())),
    )


def force_valid_terminal_fixture(
    catalog: Catalog,
    receipt_id: str,
    *,
    occurred_at_utc_ns: int = 5,
) -> None:
    """Create exact terminal authority only for contradiction/corruption tests."""

    row = catalog.remote_archive_transaction(receipt_id)
    assert row is not None
    evidence = json.dumps(
        {
            "chunk_id": row["chunk_id"],
            "receipt_id": receipt_id,
            "source_absent": True,
            "source_descriptor_sha256": row["source_descriptor_sha256"],
            "source_manifest_sha256": row["source_manifest_sha256"],
            "source_parent_fsync": True,
            "source_relative_path": row["source_relative_path"],
            "stored_bytes": row["stored_bytes"],
            "stored_sha256": row["stored_sha256"],
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    connection = catalog._connection
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            "UPDATE remote_archive_transactions SET state = 'REMOTE_DELETED', "
            "remote_deleted_at_utc_ns = ?, updated_at_utc_ns = ? "
            "WHERE receipt_id = ?",
            (occurred_at_utc_ns, occurred_at_utc_ns, receipt_id),
        )
        connection.execute(
            "INSERT INTO remote_archive_events(receipt_id, from_state, to_state, "
            "occurred_at_utc_ns, evidence_json, idempotency_key) "
            "VALUES (?, 'REMOTE_DELETE_PENDING', 'REMOTE_DELETED', ?, ?, ?)",
            (
                receipt_id,
                occurred_at_utc_ns,
                evidence,
                f"remote-deleted:{receipt_id}",
            ),
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
