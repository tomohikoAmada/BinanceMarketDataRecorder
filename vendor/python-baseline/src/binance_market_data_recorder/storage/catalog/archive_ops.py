"""Local archival, remote archive authorization/export, and storage target lifecycle."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

from .base import CatalogBase
from .types import (
    ARCHIVE_CHUNK_STATES,
    ARCHIVE_TRANSITIONS,
    ArchiveState,
    CatalogStateError,
    ChunkState,
    RemoteArchiveState,
    StorageControlState,
)


def _validate_relative_path(value: str) -> None:
    path = Path(value)
    if not value or path.is_absolute() or value in {".", ".."} or ".." in path.parts:
        raise ValueError(f"unsafe relative path: {value!r}")


def _is_lower_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _parse_remote_receipt(body: bytes) -> Any:
    """Use the single M22.3 parser without an import cycle at module load."""

    from ...archive.remote_receive import RemoteArchiveReceipt, RemoteReceiveError

    try:
        return RemoteArchiveReceipt.from_bytes(body)
    except RemoteReceiveError as exc:
        raise CatalogStateError("invalid persisted remote archive receipt") from exc


class CatalogArchiveMixin(CatalogBase):
    """Local and remote archival operations, storage targets, and ejection control."""

    def reserve_archive_transaction(
        self,
        *,
        transaction_id: str,
        chunk_id: str,
        storage_id: str,
        market: str,
        stream: str,
        source_relative_path: str,
        source_manifest_relative_path: str,
        source_manifest_sha256: str,
        target_relative_path: str,
        target_temp_relative_path: str,
        external_manifest_relative_path: str,
        stored_bytes: int,
        stored_sha256: str,
    ) -> dict[str, object]:
        identity_values = (
            transaction_id,
            chunk_id,
            storage_id,
            market,
            stream,
            source_relative_path,
            source_manifest_relative_path,
            source_manifest_sha256,
            target_relative_path,
            target_temp_relative_path,
            external_manifest_relative_path,
            stored_sha256,
        )
        if not all(identity_values) or stored_bytes < 0:
            raise ValueError("archive transaction identity is invalid")
        for path in (
            source_relative_path,
            source_manifest_relative_path,
            target_relative_path,
            target_temp_relative_path,
            external_manifest_relative_path,
        ):
            _validate_relative_path(path)
        now = time.time_ns()
        with self._transaction() as connection:
            control = connection.execute(
                "SELECT state FROM storage_control WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
            if (
                control is not None
                and StorageControlState(control["state"]) is not StorageControlState.ACTIVE
            ):
                raise CatalogStateError(
                    f"storage target blocks new archive allocation: {control['state']}"
                )
            remote_owner = connection.execute(
                "SELECT receipt_id FROM remote_archive_transactions WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
            if remote_owner is not None:
                raise CatalogStateError("remote archive ownership already exists")
            existing = connection.execute(
                "SELECT * FROM archive_transactions WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
            if existing is not None:
                expected = {
                    "transaction_id": transaction_id,
                    "storage_id": storage_id,
                    "market": market,
                    "stream": stream,
                    "source_relative_path": source_relative_path,
                    "source_manifest_relative_path": source_manifest_relative_path,
                    "source_manifest_sha256": source_manifest_sha256,
                    "target_relative_path": target_relative_path,
                    "target_temp_relative_path": target_temp_relative_path,
                    "external_manifest_relative_path": external_manifest_relative_path,
                    "stored_bytes": stored_bytes,
                    "stored_sha256": stored_sha256,
                }
                if any(existing[key] != value for key, value in expected.items()):
                    raise CatalogStateError("chunk already has a different archive transaction")
                return dict(existing)
            chunk = connection.execute(
                "SELECT state FROM chunks WHERE chunk_id = ?", (chunk_id,)
            ).fetchone()
            if chunk is None or ChunkState(chunk["state"]) is not ChunkState.SEALED:
                raise CatalogStateError("only a SEALED chunk may reserve archival")
            connection.execute(
                """
                INSERT INTO archive_transactions(
                    transaction_id, chunk_id, storage_id, state, market, stream,
                    source_relative_path, source_manifest_relative_path,
                    source_manifest_sha256, target_relative_path,
                    target_temp_relative_path, external_manifest_relative_path,
                    stored_bytes, stored_sha256, created_at_utc_ns, updated_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    chunk_id,
                    storage_id,
                    ArchiveState.COPYING,
                    market,
                    stream,
                    source_relative_path,
                    source_manifest_relative_path,
                    source_manifest_sha256,
                    target_relative_path,
                    target_temp_relative_path,
                    external_manifest_relative_path,
                    stored_bytes,
                    stored_sha256,
                    now,
                    now,
                ),
            )
            connection.execute(
                "UPDATE chunks SET state = ?, updated_at_utc_ns = ? WHERE chunk_id = ?",
                (ChunkState.ARCHIVE_COPYING, now, chunk_id),
            )
            evidence = json.dumps(
                {"storage_id": storage_id, "transaction_id": transaction_id},
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                """
                INSERT INTO chunk_transitions(
                    chunk_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    chunk_id,
                    ChunkState.SEALED,
                    ChunkState.ARCHIVE_COPYING,
                    now,
                    evidence,
                    f"archive-reserve:{transaction_id}",
                ),
            )
            connection.execute(
                """
                INSERT INTO archive_transaction_events(
                    transaction_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, NULL, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    ArchiveState.COPYING,
                    now,
                    evidence,
                    f"reserve:{transaction_id}",
                ),
            )
            row = connection.execute(
                "SELECT * FROM archive_transactions WHERE transaction_id = ?",
                (transaction_id,),
            ).fetchone()
        if row is None:
            raise CatalogStateError("archive reservation was not persisted")
        return dict(row)

    def transition_archive(
        self,
        transaction_id: str,
        to_state: ArchiveState,
        *,
        idempotency_key: str,
        evidence: Mapping[str, object] | None = None,
        verified_at_utc_ns: int | None = None,
        local_deleted_at_utc_ns: int | None = None,
    ) -> None:
        now = time.time_ns()
        evidence_json = json.dumps(dict(evidence or {}), sort_keys=True, separators=(",", ":"))
        force_sync = to_state in {
            ArchiveState.LOCAL_DELETED,
            ArchiveState.LOCAL_DELETE_PENDING,
        }
        with self._transaction(force_sync=force_sync) as connection:
            if connection.execute(
                "SELECT 1 FROM archive_transaction_events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone():
                return
            row = connection.execute(
                "SELECT * FROM archive_transactions WHERE transaction_id = ?",
                (transaction_id,),
            ).fetchone()
            if row is None:
                raise CatalogStateError(f"unknown archive transaction {transaction_id}")
            from_state = ArchiveState(row["state"])
            if from_state != to_state and to_state not in ARCHIVE_TRANSITIONS[from_state]:
                raise CatalogStateError(f"invalid archive transition {from_state} -> {to_state}")
            chunk_id = str(row["chunk_id"])
            expected_chunk_state = ARCHIVE_CHUNK_STATES[from_state]
            chunk = connection.execute(
                "SELECT state FROM chunks WHERE chunk_id = ?", (chunk_id,)
            ).fetchone()
            if chunk is None or ChunkState(chunk["state"]) is not expected_chunk_state:
                raise CatalogStateError("archive transaction and chunk state disagree")
            assignments = ["state = ?", "updated_at_utc_ns = ?", "last_error = NULL"]
            parameters: list[object] = [to_state, now]
            if verified_at_utc_ns is not None:
                assignments.append("verified_at_utc_ns = ?")
                parameters.append(verified_at_utc_ns)
            if local_deleted_at_utc_ns is not None:
                assignments.append("local_deleted_at_utc_ns = ?")
                parameters.append(local_deleted_at_utc_ns)
            parameters.append(transaction_id)
            connection.execute(
                f"UPDATE archive_transactions SET {', '.join(assignments)} "
                "WHERE transaction_id = ?",
                parameters,
            )
            to_chunk_state = ARCHIVE_CHUNK_STATES[to_state]
            connection.execute(
                "UPDATE chunks SET state = ?, updated_at_utc_ns = ? WHERE chunk_id = ?",
                (to_chunk_state, now, chunk_id),
            )
            connection.execute(
                """
                INSERT INTO archive_transaction_events(
                    transaction_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    transaction_id,
                    from_state,
                    to_state,
                    now,
                    evidence_json,
                    idempotency_key,
                ),
            )
            connection.execute(
                """
                INSERT INTO chunk_transitions(
                    chunk_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    chunk_id,
                    expected_chunk_state,
                    to_chunk_state,
                    now,
                    evidence_json,
                    f"chunk:{idempotency_key}",
                ),
            )

    def begin_archive_attempt(self, transaction_id: str) -> None:
        self._require_writable()
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE archive_transactions
                SET attempt_count = attempt_count + 1, last_error = NULL,
                    updated_at_utc_ns = ?
                WHERE transaction_id = ?
                """,
                (time.time_ns(), transaction_id),
            )
        if cursor.rowcount != 1:
            raise CatalogStateError(f"unknown archive transaction {transaction_id}")

    def record_archive_error(self, transaction_id: str, error: str) -> None:
        self._require_writable()
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE archive_transactions
                SET last_error = ?, updated_at_utc_ns = ?
                WHERE transaction_id = ?
                """,
                (error, time.time_ns(), transaction_id),
            )
        if cursor.rowcount != 1:
            raise CatalogStateError(f"unknown archive transaction {transaction_id}")

    def archive_transaction(self, transaction_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM archive_transactions WHERE transaction_id = ?",
                (transaction_id,),
            ).fetchone()
        return dict(row) if row else None

    def archive_transaction_for_chunk(self, chunk_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM archive_transactions WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
        return dict(row) if row else None

    def archive_transactions(
        self, *, storage_id: str | None = None, limit: int | None = None, offset: int = 0
    ) -> list[dict[str, object]]:
        if offset < 0 or (limit is not None and not 1 <= limit <= 1000):
            raise ValueError("invalid archive transaction page")
        if offset and limit is None:
            raise ValueError("archive transaction offset requires a limit")
        query = "SELECT * FROM archive_transactions"
        parameters: tuple[object, ...] = ()
        if storage_id is not None:
            query += " WHERE storage_id = ?"
            parameters = (storage_id,)
        query += " ORDER BY created_at_utc_ns, transaction_id"
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            parameters += (limit, offset)
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    def archive_transaction_summary(self, *, storage_id: str | None = None) -> dict[str, object]:
        """Aggregate status in SQLite without materializing historical transactions."""
        where = " WHERE storage_id = ?" if storage_id is not None else ""
        parameters = (storage_id,) if storage_id is not None else ()
        with self._lock:
            rows = self._connection.execute(
                "SELECT state, COUNT(*) AS count, "
                "MAX(CASE WHEN instr(last_error, 'DISAPPEARED_DURING_COPY') > 0 "
                "THEN 1 ELSE 0 END) AS disappeared "
                "FROM archive_transactions" + where + " GROUP BY state",
                parameters,
            ).fetchall()
        return {
            "transaction_count": sum(row["count"] for row in rows),
            "transactions_by_state": {row["state"]: row["count"] for row in rows},
            "status": (
                "DISAPPEARED_DURING_COPY" if any(row["disappeared"] for row in rows) else "OK"
            ),
        }

    def remote_archive_transaction(self, receipt_id: str) -> dict[str, object] | None:
        if not self._remote_schema_present:
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM remote_archive_transactions WHERE receipt_id = ?",
                (receipt_id,),
            ).fetchone()
        return self._validated_remote_row(row) if row is not None else None

    def remote_delete_authority_snapshot(
        self, receipt_id: str
    ) -> tuple[
        dict[str, object] | None,
        dict[str, object] | None,
        dict[str, object] | None,
        dict[str, object] | None,
    ]:
        """Read remote-delete authority from one SQLite read snapshot.

        The remote transaction row is selected and fully validated before the
        corresponding physical chunk, same-host ownership, and remote
        lifecycle rows are copied.  The deferred transaction is intentionally
        read-only: it pins one WAL snapshot without serializing a concurrent
        terminal transition.
        """

        if not self._remote_schema_present:
            return None, None, None, None
        with self._lock:
            self._connection.execute("BEGIN")
            try:
                remote_row = self._connection.execute(
                    "SELECT * FROM remote_archive_transactions WHERE receipt_id = ?",
                    (receipt_id,),
                ).fetchone()
                if remote_row is None:
                    result: tuple[
                        dict[str, object] | None,
                        dict[str, object] | None,
                        dict[str, object] | None,
                        dict[str, object] | None,
                    ] = (None, None, None, None)
                else:
                    validated_row = self._validated_remote_row(remote_row)
                    chunk = self._connection.execute(
                        "SELECT * FROM chunks WHERE chunk_id = ?",
                        (remote_row["chunk_id"],),
                    ).fetchone()
                    same_host = self._connection.execute(
                        "SELECT * FROM archive_transactions WHERE chunk_id = ?",
                        (remote_row["chunk_id"],),
                    ).fetchone()
                    lifecycle_row = self._connection.execute(
                        "SELECT * FROM remote_archive_transactions WHERE chunk_id = ?",
                        (remote_row["chunk_id"],),
                    ).fetchone()
                    validated_lifecycle = (
                        self._validated_remote_row(lifecycle_row)
                        if lifecycle_row is not None
                        else None
                    )
                    result = (
                        validated_row,
                        dict(chunk) if chunk is not None else None,
                        dict(same_host) if same_host is not None else None,
                        validated_lifecycle,
                    )
            except BaseException:
                if self._connection.in_transaction:
                    with suppress(sqlite3.Error):
                        self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")
        return result

    def remote_archive_transaction_for_chunk(self, chunk_id: str) -> dict[str, object] | None:
        if not self._remote_schema_present:
            return None
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM remote_archive_transactions WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
        return self._validated_remote_row(row) if row is not None else None

    def remote_archive_transactions(self) -> list[dict[str, object]]:
        if not self._remote_schema_present:
            return []
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM remote_archive_transactions ORDER BY created_at_utc_ns, receipt_id"
            ).fetchall()
        return [self._validated_remote_row(row) for row in rows]

    def remote_authorizations_between(
        self, start_utc_ns: int, end_utc_ns: int
    ) -> list[dict[str, object]]:
        if not self._remote_schema_present:
            return []
        if start_utc_ns < 0 or end_utc_ns <= start_utc_ns:
            raise ValueError("invalid remote authorization time range")
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM remote_archive_transactions "
                "WHERE created_at_utc_ns >= ? AND created_at_utc_ns < ? "
                "ORDER BY created_at_utc_ns, receipt_id",
                (start_utc_ns, end_utc_ns),
            ).fetchall()
        return [self._validated_remote_row(row) for row in rows]

    def iter_remote_authorizations_between(
        self,
        start_utc_ns: int,
        end_utc_ns: int,
    ) -> Iterator[dict[str, object]]:
        """Report-only pages, preserving the existing unique ordering."""
        if not self._remote_schema_present:
            return
        if start_utc_ns < 0 or end_utc_ns <= start_utc_ns:
            raise ValueError("invalid remote authorization time range")
        after: tuple[int, str] = (-1, "")
        while True:
            with self._lock:
                cursor = self._connection.execute(
                    "SELECT * FROM remote_archive_transactions "
                    "WHERE created_at_utc_ns >= ? AND created_at_utc_ns < ? "
                    "AND (created_at_utc_ns, receipt_id) > (?, ?) "
                    "ORDER BY created_at_utc_ns, receipt_id LIMIT 128",
                    (start_utc_ns, end_utc_ns, *after),
                )
                rows = cursor.fetchall()
                cursor.close()
            if not rows:
                return
            for row in rows:
                yield self._validated_remote_row(row)
            after = (int(rows[-1]["created_at_utc_ns"]), str(rows[-1]["receipt_id"]))

    def remote_archive_events(self, receipt_id: str) -> list[dict[str, object]]:
        if not self._remote_schema_present:
            return []
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM remote_archive_events WHERE receipt_id = ? ORDER BY event_id",
                (receipt_id,),
            ).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            try:
                document["to_state"] = RemoteArchiveState(str(document["to_state"])).value
                from_state = document["from_state"]
                if from_state is not None:
                    document["from_state"] = RemoteArchiveState(str(from_state)).value
                evidence = json.loads(str(document.pop("evidence_json")))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise CatalogStateError("malformed remote archive event") from exc
            if not isinstance(evidence, dict):
                raise CatalogStateError("malformed remote archive event evidence")
            document["evidence"] = evidence
            result.append(document)
        return result

    def reserve_remote_archive_transaction(
        self,
        *,
        receipt_bytes: bytes,
        market: str,
        stream: str,
        expected_chunk: Mapping[str, object],
        occurred_at_utc_ns: int | None = None,
        fault_hook: Callable[[str], None] | None = None,
    ) -> dict[str, object]:
        """Persist the sole non-destructive remote pre-delete authority."""

        self._require_writable()
        if not self._remote_schema_present:
            raise CatalogStateError("remote archive schema is unavailable")
        receipt = _parse_remote_receipt(receipt_bytes)
        if not market or not stream:
            raise ValueError("remote source market/stream identity is invalid")
        now = time.time_ns() if occurred_at_utc_ns is None else occurred_at_utc_ns
        if not isinstance(now, int) or isinstance(now, bool) or now < 0:
            raise ValueError("remote authorization timestamp is invalid")
        required_chunk = {
            "chunk_id": receipt.chunk_id,
            "state": ChunkState.SEALED.value,
            "sealed_path": receipt.source_relative_path,
            "manifest_path": receipt.source_manifest_relative_path,
            "stored_bytes": receipt.stored_bytes,
            "stored_sha256": receipt.stored_sha256,
        }
        if any(expected_chunk.get(key) != value for key, value in required_chunk.items()):
            raise CatalogStateError("remote authorization source snapshot mismatch")
        row: sqlite3.Row | None = None
        with self._transaction(force_sync=True) as connection:
            chunk = connection.execute(
                "SELECT * FROM chunks WHERE chunk_id = ?", (receipt.chunk_id,)
            ).fetchone()
            if chunk is None or ChunkState(str(chunk["state"])) is not ChunkState.SEALED:
                raise CatalogStateError("only a SEALED chunk may be remotely authorized")
            if any(chunk[key] != value for key, value in required_chunk.items()):
                raise CatalogStateError("remote authorization source changed")
            same_host = connection.execute(
                "SELECT transaction_id FROM archive_transactions WHERE chunk_id = ?",
                (receipt.chunk_id,),
            ).fetchone()
            if same_host is not None:
                raise CatalogStateError("same-host archive ownership already exists")
            existing_rows = connection.execute(
                "SELECT * FROM remote_archive_transactions WHERE receipt_id = ? OR chunk_id = ?",
                (receipt.receipt_id, receipt.chunk_id),
            ).fetchall()
            if existing_rows:
                if len(existing_rows) != 1:
                    raise CatalogStateError("conflicting remote archive ownership")
                existing = self._validated_remote_row(existing_rows[0])
                exact = {
                    "receipt_id": receipt.receipt_id,
                    "chunk_id": receipt.chunk_id,
                    "state": RemoteArchiveState.REMOTE_DELETE_PENDING.value,
                    "receipt_bytes": receipt_bytes,
                    "market": market,
                    "stream": stream,
                }
                if any(existing.get(key) != value for key, value in exact.items()):
                    raise CatalogStateError("conflicting remote archive ownership")
                row = existing_rows[0]
            else:
                connection.execute(
                    """
                    INSERT INTO remote_archive_transactions(
                        receipt_id, chunk_id, state, receipt_bytes,
                        receipt_schema_version, session_id,
                        verification_version, verification_outcome,
                        source_descriptor_schema_version,
                        source_descriptor_sha256, market, stream,
                        source_relative_path, source_manifest_relative_path,
                        source_manifest_sha256, stored_bytes, stored_sha256,
                        archive_set_id, storage_id, artifact_relative_path,
                        archive_set_entry_sha256, created_at_utc_ns,
                        updated_at_utc_ns, remote_deleted_at_utc_ns
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, NULL
                    )
                    """,
                    (
                        receipt.receipt_id,
                        receipt.chunk_id,
                        RemoteArchiveState.REMOTE_DELETE_PENDING,
                        receipt_bytes,
                        receipt.receipt_schema_version,
                        receipt.session_id,
                        receipt.verification_version,
                        receipt.verification_outcome,
                        receipt.source_descriptor_schema_version,
                        receipt.source_descriptor_sha256,
                        market,
                        stream,
                        receipt.source_relative_path,
                        receipt.source_manifest_relative_path,
                        receipt.source_manifest_sha256,
                        receipt.stored_bytes,
                        receipt.stored_sha256,
                        receipt.archive_set_id,
                        receipt.storage_id,
                        receipt.artifact_relative_path,
                        receipt.archive_set_entry_sha256,
                        now,
                        now,
                    ),
                )
                evidence = json.dumps(
                    {"chunk_id": receipt.chunk_id, "receipt_id": receipt.receipt_id},
                    sort_keys=True,
                    separators=(",", ":"),
                )
                connection.execute(
                    """
                    INSERT INTO remote_archive_events(
                        receipt_id, from_state, to_state, occurred_at_utc_ns,
                        evidence_json, idempotency_key
                    ) VALUES (?, NULL, ?, ?, ?, ?)
                    """,
                    (
                        receipt.receipt_id,
                        RemoteArchiveState.REMOTE_DELETE_PENDING,
                        now,
                        evidence,
                        f"remote-authorize:{receipt.receipt_id}",
                    ),
                )
                row = connection.execute(
                    "SELECT * FROM remote_archive_transactions WHERE receipt_id = ?",
                    (receipt.receipt_id,),
                ).fetchone()
            if fault_hook is not None:
                fault_hook("before_remote_authorization_commit")
        if fault_hook is not None:
            fault_hook("after_remote_authorization_commit")
        if row is None:
            raise CatalogStateError("remote authorization was not persisted")
        persisted = self.remote_archive_transaction(receipt.receipt_id)
        if persisted is None:
            raise CatalogStateError("remote authorization readback failed")
        return persisted

    def commit_remote_deleted(
        self,
        *,
        receipt_id: str,
        expected_chunk_id: str,
        expected_source_descriptor_sha256: str,
        expected_source_relative_path: str,
        expected_source_manifest_sha256: str,
        expected_stored_bytes: int,
        expected_stored_sha256: str,
        occurred_at_utc_ns: int,
        fault_hook: Callable[[str], None] | None = None,
    ) -> dict[str, object]:
        """Atomically commit the exact durable remote-deletion terminal fact.

        Filesystem deletion and parent-directory durability are deliberately
        outside this API. The caller supplies only equality guards for the
        already-persisted authority; this transaction cannot create or rebind
        receipt/source identity.
        """

        self._require_writable()
        if not self._remote_schema_present:
            raise CatalogStateError("remote archive schema is unavailable")
        if not _is_lower_sha256(receipt_id):
            raise ValueError("remote deletion receipt_id is invalid")
        if not expected_chunk_id:
            raise ValueError("remote deletion chunk_id is invalid")
        for value, label in (
            (expected_source_descriptor_sha256, "source descriptor digest"),
            (expected_source_manifest_sha256, "source manifest digest"),
            (expected_stored_sha256, "stored digest"),
        ):
            if not _is_lower_sha256(value):
                raise ValueError(f"remote deletion {label} is invalid")
        _validate_relative_path(expected_source_relative_path)
        if (
            not isinstance(expected_stored_bytes, int)
            or isinstance(expected_stored_bytes, bool)
            or expected_stored_bytes < 0
        ):
            raise ValueError("remote deletion stored byte count is invalid")
        if (
            not isinstance(occurred_at_utc_ns, int)
            or isinstance(occurred_at_utc_ns, bool)
            or occurred_at_utc_ns < 0
        ):
            raise ValueError("remote deletion terminal timestamp is invalid")

        evidence = {
            "chunk_id": expected_chunk_id,
            "receipt_id": receipt_id,
            "source_absent": True,
            "source_descriptor_sha256": expected_source_descriptor_sha256,
            "source_manifest_sha256": expected_source_manifest_sha256,
            "source_parent_fsync": True,
            "source_relative_path": expected_source_relative_path,
            "stored_bytes": expected_stored_bytes,
            "stored_sha256": expected_stored_sha256,
        }
        evidence_json = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
        with self._lock:
            self._connection.execute("PRAGMA synchronous = FULL")
            transaction_started = False
            try:
                if fault_hook is not None:
                    fault_hook("before_remote_deleted_begin")
                self._connection.execute("BEGIN IMMEDIATE")
                transaction_started = True
                row = self._connection.execute(
                    "SELECT * FROM remote_archive_transactions WHERE receipt_id = ?",
                    (receipt_id,),
                ).fetchone()
                if row is None:
                    raise CatalogStateError("remote deletion authority is missing")
                current = self._validated_remote_row(row)
                expected = {
                    "receipt_id": receipt_id,
                    "chunk_id": expected_chunk_id,
                    "state": RemoteArchiveState.REMOTE_DELETE_PENDING.value,
                    "source_descriptor_sha256": expected_source_descriptor_sha256,
                    "source_relative_path": expected_source_relative_path,
                    "source_manifest_sha256": expected_source_manifest_sha256,
                    "stored_bytes": expected_stored_bytes,
                    "stored_sha256": expected_stored_sha256,
                }
                if any(current.get(key) != value for key, value in expected.items()):
                    raise CatalogStateError(
                        "remote deletion pending authority does not match expected source"
                    )
                chunk = self._connection.execute(
                    "SELECT state, sealed_path, manifest_path, stored_bytes, "
                    "stored_sha256 FROM chunks WHERE chunk_id = ?",
                    (expected_chunk_id,),
                ).fetchone()
                required_chunk = {
                    "state": ChunkState.SEALED.value,
                    "sealed_path": expected_source_relative_path,
                    "manifest_path": current["source_manifest_relative_path"],
                    "stored_bytes": expected_stored_bytes,
                    "stored_sha256": expected_stored_sha256,
                }
                if chunk is None or any(
                    chunk[key] != value for key, value in required_chunk.items()
                ):
                    raise CatalogStateError("remote deletion chunk/source authority changed")
                same_host = self._connection.execute(
                    "SELECT transaction_id FROM archive_transactions WHERE chunk_id = ?",
                    (expected_chunk_id,),
                ).fetchone()
                if same_host is not None:
                    raise CatalogStateError("same-host and remote deletion ownership overlap")
                cursor = self._connection.execute(
                    "UPDATE remote_archive_transactions SET state = ?, "
                    "updated_at_utc_ns = ?, remote_deleted_at_utc_ns = ? "
                    "WHERE receipt_id = ? AND state = ?",
                    (
                        RemoteArchiveState.REMOTE_DELETED,
                        occurred_at_utc_ns,
                        occurred_at_utc_ns,
                        receipt_id,
                        RemoteArchiveState.REMOTE_DELETE_PENDING,
                    ),
                )
                if cursor.rowcount != 1:
                    raise CatalogStateError("remote deletion terminal update lost authority")
                if fault_hook is not None:
                    fault_hook("before_remote_deleted_event")
                self._connection.execute(
                    """
                    INSERT INTO remote_archive_events(
                        receipt_id, from_state, to_state, occurred_at_utc_ns,
                        evidence_json, idempotency_key
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        receipt_id,
                        RemoteArchiveState.REMOTE_DELETE_PENDING,
                        RemoteArchiveState.REMOTE_DELETED,
                        occurred_at_utc_ns,
                        evidence_json,
                        f"remote-deleted:{receipt_id}",
                    ),
                )
                if fault_hook is not None:
                    fault_hook("k4_after_remote_deleted_update_event_before_commit")
                    fault_hook("before_remote_deleted_commit")
                self._connection.execute("COMMIT")
                self._connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
                transaction_started = False
                if fault_hook is not None:
                    fault_hook("after_remote_deleted_commit")
            except BaseException:
                if transaction_started and self._connection.in_transaction:
                    with suppress(sqlite3.Error):
                        self._connection.execute("ROLLBACK")
                raise
            finally:
                self._connection.execute("PRAGMA synchronous = NORMAL")
        persisted = self.remote_archive_transaction(receipt_id)
        if persisted is None:
            raise CatalogStateError("remote deletion terminal readback failed")
        return persisted

    def _validated_remote_row(self, row: sqlite3.Row) -> dict[str, object]:
        document = dict(row)
        try:
            state = RemoteArchiveState(str(document["state"]))
            deleted_at = document["remote_deleted_at_utc_ns"]
            if (state is RemoteArchiveState.REMOTE_DELETE_PENDING) != (deleted_at is None):
                raise ValueError("remote state/timestamp invariant")
            raw_bytes = document["receipt_bytes"]
            if not isinstance(raw_bytes, bytes):
                raise ValueError("receipt_bytes is not a BLOB")
            receipt = _parse_remote_receipt(raw_bytes)
            expected = {
                "receipt_id": receipt.receipt_id,
                "chunk_id": receipt.chunk_id,
                "receipt_schema_version": receipt.receipt_schema_version,
                "session_id": receipt.session_id,
                "verification_version": receipt.verification_version,
                "verification_outcome": receipt.verification_outcome,
                "source_descriptor_schema_version": (receipt.source_descriptor_schema_version),
                "source_descriptor_sha256": receipt.source_descriptor_sha256,
                "source_relative_path": receipt.source_relative_path,
                "source_manifest_relative_path": (receipt.source_manifest_relative_path),
                "source_manifest_sha256": receipt.source_manifest_sha256,
                "stored_bytes": receipt.stored_bytes,
                "stored_sha256": receipt.stored_sha256,
                "archive_set_id": receipt.archive_set_id,
                "storage_id": receipt.storage_id,
                "artifact_relative_path": receipt.artifact_relative_path,
                "archive_set_entry_sha256": receipt.archive_set_entry_sha256,
            }
            if any(document.get(key) != value for key, value in expected.items()):
                raise ValueError("receipt/row identity mismatch")
            for key in (
                "market",
                "stream",
                "source_relative_path",
                "source_manifest_relative_path",
                "artifact_relative_path",
            ):
                value = document.get(key)
                if not isinstance(value, str) or not value:
                    raise ValueError(f"invalid remote row {key}")
            for key in (
                "source_relative_path",
                "source_manifest_relative_path",
                "artifact_relative_path",
            ):
                _validate_relative_path(str(document[key]))
            for key in ("created_at_utc_ns", "updated_at_utc_ns"):
                value = document.get(key)
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise ValueError(f"invalid remote row {key}")
            if state is RemoteArchiveState.REMOTE_DELETE_PENDING:
                if document["updated_at_utc_ns"] != document["created_at_utc_ns"]:
                    raise ValueError("pending remote row timestamp changed")
            elif (
                not isinstance(deleted_at, int)
                or isinstance(deleted_at, bool)
                or deleted_at < 0
                or document["updated_at_utc_ns"] != deleted_at
            ):
                raise ValueError("terminal remote row timestamp invariant")
            chunk = self._connection.execute(
                "SELECT state, sealed_path, manifest_path, stored_bytes, stored_sha256 "
                "FROM chunks WHERE chunk_id = ?",
                (receipt.chunk_id,),
            ).fetchone()
            current_source = {
                "state": ChunkState.SEALED.value,
                "sealed_path": receipt.source_relative_path,
                "manifest_path": receipt.source_manifest_relative_path,
                "stored_bytes": receipt.stored_bytes,
                "stored_sha256": receipt.stored_sha256,
            }
            if chunk is None or any(chunk[key] != value for key, value in current_source.items()):
                raise ValueError("remote row/current Catalog source mismatch")
            events = self._connection.execute(
                "SELECT * FROM remote_archive_events WHERE receipt_id = ? ORDER BY event_id",
                (receipt.receipt_id,),
            ).fetchall()
            initial_by_key = self._connection.execute(
                "SELECT * FROM remote_archive_events WHERE idempotency_key = ?",
                (f"remote-authorize:{receipt.receipt_id}",),
            ).fetchone()
            if (
                not events
                or initial_by_key is None
                or events[0]["event_id"] != (initial_by_key["event_id"])
            ):
                raise ValueError("remote authorization event is missing or rebound")
            initial = events[0]
            initial_evidence = {
                "chunk_id": receipt.chunk_id,
                "receipt_id": receipt.receipt_id,
            }
            initial_evidence_json = json.dumps(
                initial_evidence, sort_keys=True, separators=(",", ":")
            )
            if (
                initial["receipt_id"] != receipt.receipt_id
                or initial["from_state"] is not None
                or initial["to_state"] != RemoteArchiveState.REMOTE_DELETE_PENDING.value
                or initial["occurred_at_utc_ns"] != document["created_at_utc_ns"]
                or initial["idempotency_key"] != f"remote-authorize:{receipt.receipt_id}"
                or initial["evidence_json"] != initial_evidence_json
            ):
                raise ValueError("remote authorization event evidence mismatch")
            if state is RemoteArchiveState.REMOTE_DELETE_PENDING:
                if len(events) != 1:
                    raise ValueError("pending remote row has terminal/extra events")
            else:
                if len(events) != 2:
                    raise ValueError("terminal remote row event count mismatch")
                terminal = events[1]
                terminal_by_key = self._connection.execute(
                    "SELECT event_id FROM remote_archive_events WHERE idempotency_key = ?",
                    (f"remote-deleted:{receipt.receipt_id}",),
                ).fetchone()
                terminal_evidence = {
                    "chunk_id": receipt.chunk_id,
                    "receipt_id": receipt.receipt_id,
                    "source_absent": True,
                    "source_descriptor_sha256": receipt.source_descriptor_sha256,
                    "source_manifest_sha256": receipt.source_manifest_sha256,
                    "source_parent_fsync": True,
                    "source_relative_path": receipt.source_relative_path,
                    "stored_bytes": receipt.stored_bytes,
                    "stored_sha256": receipt.stored_sha256,
                }
                terminal_evidence_json = json.dumps(
                    terminal_evidence, sort_keys=True, separators=(",", ":")
                )
                if (
                    terminal_by_key is None
                    or terminal_by_key["event_id"] != terminal["event_id"]
                    or terminal["receipt_id"] != receipt.receipt_id
                    or terminal["from_state"] != RemoteArchiveState.REMOTE_DELETE_PENDING.value
                    or terminal["to_state"] != RemoteArchiveState.REMOTE_DELETED.value
                    or terminal["occurred_at_utc_ns"] != deleted_at
                    or terminal["idempotency_key"] != f"remote-deleted:{receipt.receipt_id}"
                    or terminal["evidence_json"] != terminal_evidence_json
                ):
                    raise ValueError("remote deletion terminal event mismatch")
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CatalogStateError("malformed persisted remote authorization") from exc
        document["state"] = state.value
        return document

    def begin_storage_eject(
        self,
        *,
        storage_id: str,
        request_id: str,
        occurred_at_utc_ns: int,
    ) -> list[dict[str, object]]:
        """Atomically reject active archive work or block every new allocation."""

        if not storage_id or not request_id or occurred_at_utc_ns < 0:
            raise ValueError("invalid storage eject request")
        with self._transaction() as connection:
            target = connection.execute(
                "SELECT 1 FROM storage_targets WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
            if target is None:
                raise CatalogStateError(f"unknown storage_id: {storage_id}")
            rows = connection.execute(
                """
                SELECT transaction_id, chunk_id, state
                FROM archive_transactions
                WHERE storage_id = ? AND state != ?
                ORDER BY created_at_utc_ns, transaction_id
                """,
                (storage_id, ArchiveState.LOCAL_DELETED),
            ).fetchall()
            if rows:
                return [dict(row) for row in rows]
            evidence = json.dumps(
                {"request_id": request_id},
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                """
                INSERT INTO storage_control(
                    storage_id, state, request_id, updated_at_utc_ns, evidence_json
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(storage_id) DO UPDATE SET
                    state = excluded.state,
                    request_id = excluded.request_id,
                    updated_at_utc_ns = excluded.updated_at_utc_ns,
                    evidence_json = excluded.evidence_json
                """,
                (
                    storage_id,
                    StorageControlState.EJECT_PENDING,
                    request_id,
                    occurred_at_utc_ns,
                    evidence,
                ),
            )
        return []

    def finish_storage_eject(
        self,
        *,
        storage_id: str,
        request_id: str,
        succeeded: bool,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
    ) -> None:
        if not storage_id or not request_id or occurred_at_utc_ns < 0:
            raise ValueError("invalid storage eject completion")
        state = StorageControlState.SAFE_TO_REMOVE if succeeded else StorageControlState.ACTIVE
        evidence_json = json.dumps(dict(evidence), sort_keys=True, separators=(",", ":"))
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state, request_id FROM storage_control WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
            if row is None:
                raise CatalogStateError("storage eject request is not pending")
            if row["request_id"] != request_id:
                raise CatalogStateError("storage eject request identity changed")
            connection.execute(
                """
                UPDATE storage_control
                SET state = ?, updated_at_utc_ns = ?, evidence_json = ?
                WHERE storage_id = ?
                """,
                (state, occurred_at_utc_ns, evidence_json, storage_id),
            )

    def retain_storage_eject_pending(
        self,
        *,
        storage_id: str,
        request_id: str,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
    ) -> None:
        """Persist uncertain asynchronous outcome without reopening allocation."""

        evidence_json = json.dumps(dict(evidence), sort_keys=True, separators=(",", ":"))
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state, request_id FROM storage_control WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
            if (
                row is None
                or row["state"] != StorageControlState.EJECT_PENDING
                or row["request_id"] != request_id
            ):
                raise CatalogStateError("storage eject request is not pending")
            connection.execute(
                """
                UPDATE storage_control
                SET updated_at_utc_ns = ?, evidence_json = ?
                WHERE storage_id = ?
                """,
                (occurred_at_utc_ns, evidence_json, storage_id),
            )

    def activate_storage_target(self, storage_id: str, *, occurred_at_utc_ns: int) -> None:
        """Clear a completed eject latch only after UUID/marker readiness is proven."""

        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state FROM storage_control WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
            if row is None or StorageControlState(row["state"]) is StorageControlState.ACTIVE:
                return
            connection.execute(
                """
                UPDATE storage_control
                SET state = ?, request_id = NULL, updated_at_utc_ns = ?,
                    evidence_json = ?
                WHERE storage_id = ?
                """,
                (
                    StorageControlState.ACTIVE,
                    occurred_at_utc_ns,
                    '{"reason":"ready_after_reinsertion"}',
                    storage_id,
                ),
            )

    def storage_control(self, storage_id: str) -> dict[str, object]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM storage_control WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
        if row is None:
            return {
                "storage_id": storage_id,
                "state": StorageControlState.ACTIVE.value,
                "request_id": None,
            }
        document = dict(row)
        document["evidence"] = json.loads(str(document.pop("evidence_json")))
        return document

    def _validate_remote_rows_in_snapshot(self) -> list[dict[str, object]]:
        """Validate remote rows before a read snapshot uses their presence."""

        rows = self._connection.execute(
            "SELECT * FROM remote_archive_transactions ORDER BY created_at_utc_ns, receipt_id"
        ).fetchall()
        return [self._validated_remote_row(row) for row in rows]

    def source_lifecycle_snapshot(
        self, chunk_id: str
    ) -> tuple[
        dict[str, object] | None,
        dict[str, object] | None,
        dict[str, object] | None,
    ]:
        """Read physical, same-host, and remote ownership in one snapshot."""

        with self._lock:
            self._connection.execute("BEGIN")
            try:
                chunk = self._connection.execute(
                    "SELECT * FROM chunks WHERE chunk_id = ?", (chunk_id,)
                ).fetchone()
                local = self._connection.execute(
                    "SELECT * FROM archive_transactions WHERE chunk_id = ?",
                    (chunk_id,),
                ).fetchone()
                remote = (
                    self._connection.execute(
                        "SELECT * FROM remote_archive_transactions WHERE chunk_id = ?",
                        (chunk_id,),
                    ).fetchone()
                    if self._remote_schema_present
                    else None
                )
                validated_remote = (
                    self._validate_remote_rows_in_snapshot() if self._remote_schema_present else []
                )
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")
        return (
            dict(chunk) if chunk is not None else None,
            dict(local) if local is not None else None,
            next(
                (row for row in validated_remote if row["chunk_id"] == chunk_id),
                None,
            )
            if remote is not None
            else None,
        )

    def source_lifecycle_aggregate(self) -> dict[str, object]:
        """Return global archive backlog and retained remote-source categories."""

        remote_pending_files = 0
        remote_pending_bytes = 0
        remote_deleted_files = 0
        if self._remote_schema_present:
            remote_rows = self.remote_archive_transactions()
            remote_pending = [
                row
                for row in remote_rows
                if row["state"] == RemoteArchiveState.REMOTE_DELETE_PENDING.value
            ]
            remote_deleted = [
                row
                for row in remote_rows
                if row["state"] == RemoteArchiveState.REMOTE_DELETED.value
            ]
            remote_pending_files = len(remote_pending)
            remote_pending_bytes = sum(cast(int, row["stored_bytes"]) for row in remote_pending)
            remote_deleted_files = len(remote_deleted)
        remote_exclusion = (
            "AND NOT EXISTS (SELECT 1 FROM remote_archive_transactions AS remote "
            "WHERE remote.chunk_id = chunks.chunk_id)"
            if self._remote_schema_present
            else ""
        )
        with self._lock:
            row = self._connection.execute(
                f"""
                SELECT COUNT(*) AS files,
                       COALESCE(SUM(stored_bytes), 0) AS bytes,
                       MIN(created_at_utc_ns) AS oldest,
                       SUM(CASE WHEN state = ? THEN 1 ELSE 0 END)
                           AS ordinary_sealed_files
                FROM chunks
                WHERE state IN (?, ?, ?, ?, ?)
                  {remote_exclusion}
                """,
                (
                    ChunkState.SEALED,
                    ChunkState.SEALED,
                    ChunkState.ARCHIVE_COPYING,
                    ChunkState.ARCHIVE_VERIFYING,
                    ChunkState.ARCHIVED_VERIFIED,
                    ChunkState.LOCAL_DELETE_PENDING,
                ),
            ).fetchone()
        return {
            "unarchived_backlog_files": int(row["files"]),
            "unarchived_backlog_bytes": int(row["bytes"]),
            "oldest_unarchived_at_utc_ns": row["oldest"],
            "ordinary_sealed_files": int(row["ordinary_sealed_files"] or 0),
            "remote_pending_files": remote_pending_files,
            "remote_pending_source_bytes": remote_pending_bytes,
            "remote_deleted_files": remote_deleted_files,
        }

    def oldest_incomplete_archive_transaction(self, storage_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM archive_transactions
                WHERE storage_id = ?
                  AND state IN (?, ?, ?, ?)
                ORDER BY created_at_utc_ns, transaction_id
                LIMIT 1
                """,
                (
                    storage_id,
                    ArchiveState.COPYING,
                    ArchiveState.VERIFYING,
                    ArchiveState.VERIFIED,
                    ArchiveState.LOCAL_DELETE_PENDING,
                ),
            ).fetchone()
        return dict(row) if row else None

    def archive_aggregate(self, storage_id: str) -> dict[str, object]:
        """Return target archive totals from one bounded SQLite read snapshot.

        ``unassigned_sealed_*`` is the global queue of SEALED chunks that has
        not yet been assigned to any storage target. ``target_inflight_*`` is
        scoped to ``storage_id``. Backlog is the sum of those two disjoint
        populations.
        """

        remote_exclusion = (
            "AND NOT EXISTS (SELECT 1 FROM remote_archive_transactions AS remote "
            "WHERE remote.chunk_id = c.chunk_id)"
            if self._remote_schema_present
            else ""
        )
        with self._lock:
            self._connection.execute("BEGIN")
            try:
                if self._remote_schema_present:
                    self._validate_remote_rows_in_snapshot()
                state_counts = self._connection.execute(
                    """
                    SELECT state, COUNT(*) AS cnt
                    FROM archive_transactions
                    WHERE storage_id = ?
                    GROUP BY state
                    """,
                    (storage_id,),
                ).fetchall()
                aggregate_row = self._connection.execute(
                    f"""
                    WITH
                    unassigned AS (
                        SELECT c.stored_bytes
                        FROM chunks AS c
                        WHERE c.state = ?
                          AND NOT EXISTS (
                              SELECT 1
                              FROM archive_transactions AS assigned
                              WHERE assigned.chunk_id = c.chunk_id
                          )
                          {remote_exclusion}
                    ),
                    target_inflight AS (
                        SELECT stored_bytes
                        FROM archive_transactions
                        WHERE storage_id = ?
                          AND state IN (?, ?, ?, ?)
                    ),
                    target_verified AS (
                        SELECT stored_bytes, state, verified_at_utc_ns
                        FROM archive_transactions
                        WHERE storage_id = ?
                          AND state IN (?, ?, ?)
                    )
                    SELECT
                        (SELECT COUNT(*) FROM unassigned)
                            AS unassigned_sealed_files,
                        (SELECT COALESCE(SUM(stored_bytes), 0) FROM unassigned)
                            AS unassigned_sealed_bytes,
                        (SELECT COUNT(*) FROM target_inflight)
                            AS target_inflight_files,
                        (SELECT COALESCE(SUM(stored_bytes), 0) FROM target_inflight)
                            AS target_inflight_bytes,
                        (SELECT COUNT(*) FROM target_verified)
                            AS external_verified_files,
                        (SELECT COALESCE(SUM(stored_bytes), 0) FROM target_verified)
                            AS external_verified_bytes,
                        (
                            SELECT COUNT(*) FROM target_verified
                            WHERE state = ?
                        ) AS local_deleted_files,
                        (
                            SELECT COALESCE(SUM(stored_bytes), 0)
                            FROM target_verified
                            WHERE state = ?
                        ) AS local_deleted_bytes,
                        (SELECT MAX(verified_at_utc_ns) FROM target_verified)
                            AS last_verified_at
                    """,
                    (
                        ChunkState.SEALED,
                        storage_id,
                        ArchiveState.COPYING,
                        ArchiveState.VERIFYING,
                        ArchiveState.VERIFIED,
                        ArchiveState.LOCAL_DELETE_PENDING,
                        storage_id,
                        ArchiveState.VERIFIED,
                        ArchiveState.LOCAL_DELETE_PENDING,
                        ArchiveState.LOCAL_DELETED,
                        ArchiveState.LOCAL_DELETED,
                        ArchiveState.LOCAL_DELETED,
                    ),
                ).fetchone()
                error_row = self._connection.execute(
                    """
                    SELECT last_error, updated_at_utc_ns
                    FROM archive_transactions
                    WHERE storage_id = ?
                      AND last_error IS NOT NULL
                      AND last_error != ''
                    ORDER BY updated_at_utc_ns DESC
                    LIMIT 1
                    """,
                    (storage_id,),
                ).fetchone()
                remote_row = (
                    self._connection.execute(
                        """
                        SELECT
                            SUM(CASE WHEN state = ? THEN 1 ELSE 0 END)
                                AS pending_files,
                            COALESCE(SUM(CASE WHEN state = ? THEN stored_bytes ELSE 0 END), 0)
                                AS pending_bytes,
                            SUM(CASE WHEN state = ? THEN 1 ELSE 0 END)
                                AS deleted_files
                        FROM remote_archive_transactions
                        """,
                        (
                            RemoteArchiveState.REMOTE_DELETE_PENDING,
                            RemoteArchiveState.REMOTE_DELETE_PENDING,
                            RemoteArchiveState.REMOTE_DELETED,
                        ),
                    ).fetchone()
                    if self._remote_schema_present
                    else None
                )
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

        txn_states = {str(row["state"]): int(row["cnt"]) for row in state_counts}
        unassigned_sealed_files = int(aggregate_row["unassigned_sealed_files"])
        unassigned_sealed_bytes = int(aggregate_row["unassigned_sealed_bytes"])
        target_inflight_files = int(aggregate_row["target_inflight_files"])
        target_inflight_bytes = int(aggregate_row["target_inflight_bytes"])
        external_verified_files = int(aggregate_row["external_verified_files"])
        external_verified_bytes = int(aggregate_row["external_verified_bytes"])
        local_deleted_files = int(aggregate_row["local_deleted_files"])
        local_deleted_bytes = int(aggregate_row["local_deleted_bytes"])
        last_verified_at_utc_ns: object = aggregate_row["last_verified_at"]
        latest_error_type: object = None
        latest_error_at_utc_ns: object = None
        if error_row:
            raw_error = str(error_row["last_error"] or "")
            if raw_error:
                if "DISAPPEARED" in raw_error:
                    latest_error_type = "DISAPPEARED_DURING_COPY"
                elif "SHA" in raw_error or "hash" in raw_error.lower():
                    latest_error_type = "HASH_MISMATCH"
                elif "size" in raw_error.lower() or "missing" in raw_error.lower():
                    latest_error_type = "MISSING"
                elif "ArchiveError" in raw_error:
                    latest_error_type = "ARCHIVE_ERROR"
                else:
                    latest_error_type = "UNKNOWN"
            latest_error_at_utc_ns = error_row["updated_at_utc_ns"]

        backlog_files = unassigned_sealed_files + target_inflight_files
        backlog_bytes = unassigned_sealed_bytes + target_inflight_bytes

        return {
            "transactions_by_state": txn_states,
            "unassigned_sealed_scope": "GLOBAL",
            "unassigned_sealed_files": unassigned_sealed_files,
            "unassigned_sealed_bytes": unassigned_sealed_bytes,
            "target_inflight_files": target_inflight_files,
            "target_inflight_bytes": target_inflight_bytes,
            "external_verified_files": external_verified_files,
            "external_verified_bytes": external_verified_bytes,
            "local_deleted_files": local_deleted_files,
            "local_deleted_bytes": local_deleted_bytes,
            "backlog_files": backlog_files,
            "backlog_bytes": backlog_bytes,
            "last_verified_at_utc_ns": last_verified_at_utc_ns,
            "latest_error_type": latest_error_type,
            "latest_error_at_utc_ns": latest_error_at_utc_ns,
            "remote_pending_files": (
                int(remote_row["pending_files"] or 0) if remote_row is not None else 0
            ),
            "remote_pending_source_bytes": (
                int(remote_row["pending_bytes"] or 0) if remote_row is not None else 0
            ),
            "remote_deleted_files": (
                int(remote_row["deleted_files"] or 0) if remote_row is not None else 0
            ),
        }

    def register_storage_target(
        self,
        *,
        storage_id: str,
        volume_uuid: str,
        volume_name: str | None,
        filesystem_type: str | None,
        relative_path: str,
        marker_nonce: str,
        registered_at_utc_ns: int,
    ) -> None:
        if not all((storage_id, volume_uuid, relative_path, marker_nonce)):
            raise ValueError("storage target identity fields must be non-empty")
        if relative_path in {".", "/"} or relative_path.startswith("../"):
            raise ValueError("storage target must be below the volume root")
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM storage_targets WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
            location = connection.execute(
                """
                SELECT storage_id FROM storage_targets
                WHERE volume_uuid = ? AND relative_path = ?
                """,
                (volume_uuid, relative_path),
            ).fetchone()
            if location is not None and location["storage_id"] != storage_id:
                raise CatalogStateError("volume directory is registered with another storage_id")
            identity = (
                volume_uuid,
                volume_name,
                filesystem_type,
                relative_path,
                marker_nonce,
                registered_at_utc_ns,
            )
            if existing is not None:
                observed = (
                    existing["volume_uuid"],
                    existing["volume_name"],
                    existing["filesystem_type"],
                    existing["relative_path"],
                    existing["marker_nonce"],
                    existing["registered_at_utc_ns"],
                )
                if observed != identity:
                    raise CatalogStateError("storage_id already has different identity")
                return
            connection.execute(
                """
                INSERT INTO storage_targets(
                    storage_id, volume_uuid, volume_name, filesystem_type,
                    relative_path, marker_nonce, registered_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (storage_id, *identity),
            )

    def unregister_storage_target(self, storage_id: str) -> bool:
        with self._transaction() as connection:
            connection.execute("DELETE FROM storage_control WHERE storage_id = ?", (storage_id,))
            cursor = connection.execute(
                "DELETE FROM storage_targets WHERE storage_id = ?", (storage_id,)
            )
        return cursor.rowcount == 1

    def storage_targets(self) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM storage_targets ORDER BY storage_id"
            ).fetchall()
        return [dict(row) for row in rows]

    def storage_target_for_location(
        self, *, volume_uuid: str, relative_path: str
    ) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM storage_targets
                WHERE volume_uuid = ? AND relative_path = ?
                """,
                (volume_uuid, relative_path),
            ).fetchone()
        return dict(row) if row else None

    def chunk_archive_snapshot(
        self, chunk_id: str
    ) -> tuple[dict[str, object] | None, dict[str, object] | None]:
        """Read Chunk and Archive identity from one consistent SQL statement."""

        with self._lock:
            row = self._connection.execute(
                """
                SELECT
                    chunks.*,
                    archive_transactions.transaction_id AS archive_transaction_id,
                    archive_transactions.chunk_id AS archive_chunk_id,
                    archive_transactions.state AS archive_state,
                    archive_transactions.market AS archive_market,
                    archive_transactions.stream AS archive_stream,
                    archive_transactions.source_relative_path
                        AS archive_source_relative_path,
                    archive_transactions.source_manifest_relative_path
                        AS archive_source_manifest_relative_path,
                    archive_transactions.source_manifest_sha256
                        AS archive_source_manifest_sha256,
                    archive_transactions.stored_bytes AS archive_stored_bytes,
                    archive_transactions.stored_sha256 AS archive_stored_sha256
                FROM chunks
                LEFT JOIN archive_transactions
                    ON archive_transactions.chunk_id = chunks.chunk_id
                WHERE chunks.chunk_id = ?
                """,
                (chunk_id,),
            ).fetchone()
        if row is None:
            return None, None
        combined = dict(row)
        archive_aliases = {
            "transaction_id": "archive_transaction_id",
            "chunk_id": "archive_chunk_id",
            "state": "archive_state",
            "market": "archive_market",
            "stream": "archive_stream",
            "source_relative_path": "archive_source_relative_path",
            "source_manifest_relative_path": ("archive_source_manifest_relative_path"),
            "source_manifest_sha256": "archive_source_manifest_sha256",
            "stored_bytes": "archive_stored_bytes",
            "stored_sha256": "archive_stored_sha256",
        }
        archive = {name: combined.pop(alias) for name, alias in archive_aliases.items()}
        if archive["transaction_id"] is None:
            return combined, None
        return combined, archive
