"""Chunk lifecycle transitions, registration, state queries, and verification."""

from __future__ import annotations

import json
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any

from .base import CatalogBase
from .types import ALLOWED_TRANSITIONS, RAW_VERIFICATION_VERSION, CatalogStateError, ChunkState


class CatalogChunksMixin(CatalogBase):
    """Raw chunk lifecycle, verification records, and quarantined artifacts."""

    def register_active(
        self,
        *,
        chunk_id: str,
        partial_path: str,
        created_at_utc_ns: int,
    ) -> None:
        now = time.time_ns()
        with self._transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO chunks(
                    chunk_id, state, partial_path, created_at_utc_ns, updated_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (chunk_id, ChunkState.ACTIVE, partial_path, created_at_utc_ns, now),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO chunk_transitions(
                    chunk_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, NULL, ?, ?, '{}', ?)
                """,
                (chunk_id, ChunkState.ACTIVE, now, f"create:{chunk_id}"),
            )

    def register_boundary_marker_sealing(
        self,
        *,
        chunk_id: str,
        partial_path: str,
        created_at_utc_ns: int,
        seal_intent: Mapping[str, object],
    ) -> None:
        """Atomically make an empty reconnect marker's seal intent durable.

        The transaction publishes the marker identity directly as SEALING and
        records both its logical ACTIVE creation and ACTIVE -> SEALING intent
        transition.  Only after this method commits may StreamSpool create the
        Raw v1 header.  A crash can therefore expose neither a marker partial
        nor an ACTIVE marker row without the exact reconnect intent.
        """

        if not chunk_id or not partial_path or created_at_utc_ns < 0:
            raise ValueError("invalid boundary marker identity")
        now = time.time_ns()
        sealing_evidence = json.dumps(
            {"verified_frames": 0, "seal_intent": dict(seal_intent)},
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._transaction() as connection:
            existing = connection.execute(
                """
                SELECT state, partial_path, created_at_utc_ns
                FROM chunks WHERE chunk_id = ?
                """,
                (chunk_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO chunks(
                        chunk_id, state, partial_path,
                        created_at_utc_ns, updated_at_utc_ns
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        chunk_id,
                        ChunkState.SEALING,
                        partial_path,
                        created_at_utc_ns,
                        now,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO chunk_transitions(
                        chunk_id, from_state, to_state, occurred_at_utc_ns,
                        evidence_json, idempotency_key
                    ) VALUES (?, NULL, ?, ?, '{}', ?)
                    """,
                    (chunk_id, ChunkState.ACTIVE, now, f"create:{chunk_id}"),
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
                        ChunkState.ACTIVE,
                        ChunkState.SEALING,
                        now,
                        sealing_evidence,
                        f"sealing:{chunk_id}",
                    ),
                )
                return
            if (
                ChunkState(str(existing["state"])) is not ChunkState.SEALING
                or str(existing["partial_path"]) != partial_path
                or int(existing["created_at_utc_ns"]) != created_at_utc_ns
            ):
                raise CatalogStateError(f"boundary marker identity conflict: {chunk_id}")
            transition = connection.execute(
                """
                SELECT evidence_json FROM chunk_transitions
                WHERE idempotency_key = ?
                """,
                (f"sealing:{chunk_id}",),
            ).fetchone()
            if transition is None or str(transition["evidence_json"]) != sealing_evidence:
                raise CatalogStateError(f"boundary marker seal intent conflict: {chunk_id}")

    def state(self, chunk_id: str) -> ChunkState | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT state FROM chunks WHERE chunk_id = ?", (chunk_id,)
            ).fetchone()
        return ChunkState(row["state"]) if row else None

    def transition(
        self,
        chunk_id: str,
        to_state: ChunkState,
        *,
        idempotency_key: str,
        evidence: Mapping[str, object] | None = None,
        fields: Mapping[str, object] | None = None,
    ) -> None:
        now = time.time_ns()
        evidence_json = json.dumps(dict(evidence or {}), sort_keys=True, separators=(",", ":"))
        update_fields = dict(fields or {})
        allowed_columns = {
            "partial_path",
            "sealed_path",
            "manifest_path",
            "record_count",
            "uncompressed_bytes",
            "stored_bytes",
            "uncompressed_sha256",
            "stored_sha256",
        }
        if not set(update_fields) <= allowed_columns:
            raise ValueError("unsupported Catalog update field")
        with self._transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM chunk_transitions WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone():
                return
            row = connection.execute(
                "SELECT state FROM chunks WHERE chunk_id = ?", (chunk_id,)
            ).fetchone()
            if row is None:
                raise CatalogStateError(f"unknown chunk {chunk_id}")
            from_state = ChunkState(row["state"])
            if from_state == to_state:
                connection.execute(
                    """
                    INSERT INTO chunk_transitions(
                        chunk_id, from_state, to_state, occurred_at_utc_ns,
                        evidence_json, idempotency_key
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (chunk_id, from_state, to_state, now, evidence_json, idempotency_key),
                )
                return
            if to_state not in ALLOWED_TRANSITIONS[from_state]:
                raise CatalogStateError(f"invalid transition {from_state} -> {to_state}")
            assignments = ["state = ?", "updated_at_utc_ns = ?"]
            parameters: list[Any] = [to_state, now]
            for name, value in sorted(update_fields.items()):
                assignments.append(f"{name} = ?")
                parameters.append(value)
            parameters.append(chunk_id)
            connection.execute(
                f"UPDATE chunks SET {', '.join(assignments)} WHERE chunk_id = ?",
                parameters,
            )
            connection.execute(
                """
                INSERT INTO chunk_transitions(
                    chunk_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (chunk_id, from_state, to_state, now, evidence_json, idempotency_key),
            )

            if to_state is ChunkState.SEALED:
                connection.execute(
                    "INSERT OR IGNORE INTO raw_verification_queue(chunk_id, enrolled_at_utc_ns) "
                    "VALUES (?, ?)",
                    (chunk_id, now),
                )

    def chunk(self, chunk_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM chunks WHERE chunk_id = ?", (chunk_id,)
            ).fetchone()
        return dict(row) if row else None

    def chunks_in_states(self, *states: ChunkState) -> list[dict[str, object]]:
        if not states:
            return []
        placeholders = ",".join("?" for _ in states)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM chunks WHERE state IN ({placeholders}) ORDER BY chunk_id",
                tuple(states),
            ).fetchall()
        return [dict(row) for row in rows]

    def chunks_in_states_snapshot(self, *states: ChunkState) -> list[dict[str, object]]:
        """Read chunk lifecycle rows from one committed SQLite snapshot.

        Acceptance and audit callers use this short-lived helper to freeze the
        Catalog membership boundary before scanning the filesystem.  It is
        deliberately read-only and does not use the writable transaction
        helper, so static read-only Catalogs remain free of WAL/SHM sidecars.
        """

        if not states:
            return []
        placeholders = ",".join("?" for _ in states)
        with self._lock:
            self._connection.execute("BEGIN")
            try:
                rows = self._connection.execute(
                    f"""
                    SELECT
                        chunks.*,
                        archive_transactions.transaction_id
                            AS archive_transaction_id,
                        archive_transactions.chunk_id AS archive_chunk_id,
                        archive_transactions.state AS archive_state,
                        archive_transactions.storage_id AS archive_storage_id,
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
                    WHERE chunks.state IN ({placeholders})
                    ORDER BY chunks.chunk_id
                    """,
                    tuple(states),
                ).fetchall()
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")
        return [dict(row) for row in rows]

    def oldest_chunk_in_states(self, *states: ChunkState) -> dict[str, object] | None:
        if not states:
            return None
        placeholders = ",".join("?" for _ in states)
        with self._lock:
            row = self._connection.execute(
                f"""
                SELECT * FROM chunks WHERE state IN ({placeholders})
                ORDER BY created_at_utc_ns, chunk_id LIMIT 1
                """,
                tuple(states),
            ).fetchone()
        return dict(row) if row else None

    def oldest_unowned_sealed_chunk(self) -> dict[str, object] | None:
        remote_exclusion = (
            "AND NOT EXISTS (SELECT 1 FROM remote_archive_transactions AS remote "
            "WHERE remote.chunk_id = chunks.chunk_id)"
            if self._remote_schema_present
            else ""
        )
        with self._lock:
            self._connection.execute("BEGIN")
            try:
                if self._remote_schema_present:
                    self._validate_remote_rows_in_snapshot()
                row = self._connection.execute(
                    f"""
                    SELECT * FROM chunks
                    WHERE state = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM archive_transactions AS local
                          WHERE local.chunk_id = chunks.chunk_id
                      )
                      {remote_exclusion}
                    ORDER BY created_at_utc_ns, chunk_id LIMIT 1
                    """,
                    (ChunkState.SEALED,),
                ).fetchone()
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")
        return dict(row) if row else None

    def transition_count(self, chunk_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM chunk_transitions WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchone()
        return int(row["count"])

    def latest_transition_evidence(
        self, chunk_id: str, to_state: ChunkState
    ) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT evidence_json FROM chunk_transitions
                WHERE chunk_id = ? AND to_state = ?
                ORDER BY transition_id DESC LIMIT 1
                """,
                (chunk_id, to_state),
            ).fetchone()
        return json.loads(row["evidence_json"]) if row else None

    def has_reconnect_recovery_evidence(self) -> bool:
        """Whether startup needs the frozen reconnect classification engine.

        Include every lifecycle event, even malformed ones, so legacy blockers
        remain checked before recovery mutates any artifacts.
        """
        with self._lock:
            row = self._connection.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM chunk_transitions
                    WHERE to_state = 'SEALING'
                      AND json_type(evidence_json, '$.seal_intent') != 'null'
                ) OR EXISTS (
                    SELECT 1 FROM operational_events
                    WHERE event_type IN (
                        'STREAM_DISCONTINUITY_STARTED',
                        'STREAM_DISCONTINUITY_COMPLETED'
                    )
                )
                """
            ).fetchone()
        return bool(row[0])

    def sealing_transition_evidence(
        self,
    ) -> list[tuple[str, dict[str, object]]]:
        """Return (chunk_id, evidence) for every ChunkState.SEALING transition.

        The SEALING transition is the first durable seal mutation and carries
        the reconnect-boundary seal intent (``seal_intent``) when the seal was
        requested with reconnect semantics (M21.4.11-R2 P1-A). Startup
        recovery scans all of them, not only currently-SEALING chunks, so a
        crash after the seal completed but before the Catalog STARTED event
        committed still reconstructs the pending discontinuity.
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT chunk_id, evidence_json FROM chunk_transitions
                WHERE to_state = ?
                ORDER BY transition_id
                """,
                (ChunkState.SEALING,),
            ).fetchall()
        output: list[tuple[str, dict[str, object]]] = []
        for row in rows:
            evidence = json.loads(row["evidence_json"])
            if isinstance(evidence, dict):
                output.append((str(row["chunk_id"]), evidence))
        return output

    def register_quarantined_artifact(
        self, *, artifact_id: str, relative_path: str, reason: str, sha256: str
    ) -> None:
        self._require_writable()
        with self._lock:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO quarantined_artifacts(
                    artifact_id, relative_path, reason, sha256, quarantined_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (artifact_id, relative_path, reason, sha256, time.time_ns()),
            )

    def register_orderbook_checkpoint(
        self,
        *,
        checkpoint_id: str,
        market: str,
        symbol: str,
        update_id: int,
        book_hash: str,
        relative_path: str,
        created_at_utc_ns: int,
    ) -> None:
        self._require_writable()
        with self._lock:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO orderbook_checkpoints(
                    checkpoint_id, market, symbol, update_id, book_hash,
                    relative_path, created_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    checkpoint_id,
                    market,
                    symbol,
                    update_id,
                    book_hash,
                    relative_path,
                    created_at_utc_ns,
                ),
            )

    def orderbook_checkpoint(self, checkpoint_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM orderbook_checkpoints WHERE checkpoint_id = ?",
                (checkpoint_id,),
            ).fetchone()
        return dict(row) if row else None

    def sealed_manifests_since(self, since_utc_ns: int) -> list[str]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT DISTINCT c.manifest_path FROM chunk_transitions t JOIN chunks c "
                "USING(chunk_id) WHERE t.to_state=? AND t.occurred_at_utc_ns>=? "
                "AND c.manifest_path IS NOT NULL ORDER BY c.manifest_path",
                (ChunkState.SEALED, since_utc_ns),
            ).fetchall()
        return [str(row[0]) for row in rows]

    def enroll_raw_verification(self, chunk_id: str) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT manifest_path FROM chunks WHERE chunk_id=?", (chunk_id,)
            ).fetchone()
            if row is None or row[0] is None:
                raise CatalogStateError("verification requires a sealed chunk")
            connection.execute(
                "INSERT INTO raw_verification_queue(chunk_id, enrolled_at_utc_ns) VALUES (?, ?) "
                "ON CONFLICT(chunk_id) DO UPDATE SET verified_result_id=NULL, "
                "next_attempt_utc_ns=0, last_error=NULL",
                (chunk_id, time.time_ns()),
            )

    def pending_raw_verifications(
        self,
        *,
        now: int,
        limit: int = 32,
        through_utc_ns: int | None = None,
    ) -> list[dict[str, Any]]:
        with self._lock:
            version = self._verification_version_sql()
            rows = self._connection.execute(
                "SELECT c.*, q.enrolled_at_utc_ns FROM raw_verification_queue q "
                "JOIN chunks c USING(chunk_id) "
                "LEFT JOIN raw_verification_results r ON r.result_id=q.verified_result_id "
                f"WHERE COALESCE({version}=?,0)=0 "
                "AND q.next_attempt_utc_ns <= ? AND (? IS NULL OR q.enrolled_at_utc_ns <= ?) "
                "ORDER BY q.enrolled_at_utc_ns, c.chunk_id LIMIT ?",
                (RAW_VERIFICATION_VERSION, now, through_utc_ns, through_utc_ns, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def record_raw_verification(
        self,
        chunk_id: str,
        *,
        evidence: Mapping[str, object] | None = None,
        error: str | None = None,
    ) -> None:
        now = time.time_ns()
        with self._transaction() as connection:
            if evidence is None:
                connection.execute(
                    "UPDATE raw_verification_queue SET attempts=attempts+1, last_error=?, "
                    "next_attempt_utc_ns=?, verified_result_id=NULL WHERE chunk_id=?",
                    (error, now + 60_000_000_000, chunk_id),
                )
                return
            row = connection.execute(
                "SELECT * FROM chunks WHERE chunk_id=?", (chunk_id,)
            ).fetchone()
            if row is None or any(
                row[key] != evidence[key]
                for key in (
                    "stored_sha256",
                    "uncompressed_sha256",
                    "stored_bytes",
                    "uncompressed_bytes",
                    "record_count",
                    "manifest_path",
                )
            ):
                raise CatalogStateError("Raw identity changed during verification")
            result_id = (
                f"{chunk_id}:{evidence['manifest_sha256']}:{evidence['verifier_version']}:"
                f"{evidence['verified_at_utc_ns']}"
            )
            connection.execute(
                "INSERT OR IGNORE INTO raw_verification_results "
                "(result_id, chunk_id, evidence_json, verifier_version) VALUES (?, ?, ?, ?)",
                (
                    result_id,
                    chunk_id,
                    json.dumps(dict(evidence), sort_keys=True),
                    evidence["verifier_version"],
                ),
            )
            connection.execute(
                "UPDATE raw_verification_queue SET verified_result_id=?, attempts=attempts+1, "
                "last_error=NULL WHERE chunk_id=?",
                (result_id, chunk_id),
            )

    def _verification_version_sql(self) -> str:
        # Read-only observers may see the old schema before its writer upgrades it.
        # Never parse every historical proof JSON just to report/choose pending work.
        return (
            "r.verifier_version"
            if "verifier_version" in self.table_columns("raw_verification_results")
            else "NULL"
        )

    def raw_verification_status(self, *, through_utc_ns: int | None = None) -> dict[str, object]:
        with self._lock:
            present = self._connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='raw_verification_queue'"
            ).fetchone()
            if present is None:
                return {"status": "NOT_ENROLLED", "pending_files": 0, "verified_files": 0}
            # Reuse only the current verifier's proof. Prior v1 results remain
            # immutable, while their already-enrolled files re-enter bounded work.
            version = self._verification_version_sql()
            observations = (
                "WITH observations AS (SELECT q.*, c.stored_bytes, "
                f"COALESCE({version}=?,0) "
                "AS current_proof "
                "FROM raw_verification_queue q JOIN chunks c USING(chunk_id) "
                "LEFT JOIN raw_verification_results r ON r.result_id=q.verified_result_id "
                "WHERE (? IS NULL OR q.enrolled_at_utc_ns<=?)) "
            )
            parameters = (RAW_VERIFICATION_VERSION, through_utc_ns, through_utc_ns)
            row = self._connection.execute(
                observations + "SELECT COUNT(*) AS enrolled_files, "
                "COALESCE(SUM(current_proof),0) AS verified_files, "
                "COALESCE(SUM(NOT current_proof),0) AS pending_files, "
                "COALESCE(SUM(NOT current_proof AND last_error IS NOT NULL),0) AS failed_files, "
                "COALESCE(SUM(CASE WHEN NOT current_proof THEN stored_bytes END),0) "
                "AS pending_stored_bytes, "
                "MIN(CASE WHEN NOT current_proof THEN enrolled_at_utc_ns END) "
                "AS oldest_pending_utc_ns FROM observations",
                parameters,
            ).fetchone()
            high_water = self._connection.execute(
                "SELECT MAX(enrolled_at_utc_ns) FROM raw_verification_queue"
            ).fetchone()[0]
            failures = self._connection.execute(
                observations + "SELECT chunk_id, last_error, attempts FROM observations "
                "WHERE NOT current_proof AND last_error IS NOT NULL "
                "ORDER BY enrolled_at_utc_ns LIMIT 16",
                parameters,
            ).fetchall()
        return {
            "status": "PENDING" if row["pending_files"] else "CAUGHT_UP",
            **dict(row),
            "failures": [dict(item) for item in failures],
            "through_utc_ns": through_utc_ns,
            "enrollment_high_water_utc_ns": high_water,
            "verifier_version": RAW_VERIFICATION_VERSION,
            "oldest_pending_age_seconds": (
                max(0, time.time_ns() - row["oldest_pending_utc_ns"]) / 1e9
                if row["oldest_pending_utc_ns"] is not None
                else 0
            ),
        }

    @contextmanager
    def acceptance_delta_snapshot(self) -> Iterator[Any]:
        """Freeze all acceptance cursors and indexed companions in one read snapshot."""
        from ..acceptance_delta import DeltaSnapshot

        with self._lock:
            self._connection.execute("BEGIN")
            try:
                yield DeltaSnapshot(self._connection)
            finally:
                self._connection.execute("ROLLBACK")
