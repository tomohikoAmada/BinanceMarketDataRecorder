"""SQLite lifecycle Catalog; market-event payloads never enter this database."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path
from threading import RLock
from typing import Any


class ChunkState(StrEnum):
    ACTIVE = "ACTIVE"
    RECOVERED = "RECOVERED"
    SEALING = "SEALING"
    SEALED = "SEALED"
    QUARANTINED = "QUARANTINED"


class CatalogStateError(RuntimeError):
    """Raised for an invalid lifecycle transition."""


ALLOWED_TRANSITIONS = {
    ChunkState.ACTIVE: {ChunkState.RECOVERED, ChunkState.SEALING, ChunkState.QUARANTINED},
    ChunkState.RECOVERED: {ChunkState.SEALING, ChunkState.QUARANTINED},
    ChunkState.SEALING: {ChunkState.SEALED, ChunkState.QUARANTINED},
    ChunkState.SEALED: set(),
    ChunkState.QUARANTINED: set(),
}


class Catalog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._lock = RLock()
        self._connection = sqlite3.connect(
            path, timeout=30, isolation_level=None, check_same_thread=False
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._initialize()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def _initialize(self) -> None:
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                partial_path TEXT,
                sealed_path TEXT,
                manifest_path TEXT,
                record_count INTEGER NOT NULL DEFAULT 0,
                uncompressed_bytes INTEGER,
                stored_bytes INTEGER,
                uncompressed_sha256 TEXT,
                stored_sha256 TEXT,
                created_at_utc_ns INTEGER NOT NULL,
                updated_at_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS chunk_transitions (
                transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id),
                from_state TEXT,
                to_state TEXT NOT NULL,
                occurred_at_utc_ns INTEGER NOT NULL,
                evidence_json TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS quarantined_artifacts (
                artifact_id TEXT PRIMARY KEY,
                relative_path TEXT NOT NULL,
                reason TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                quarantined_at_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orderbook_checkpoints (
                checkpoint_id TEXT PRIMARY KEY,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                update_id INTEGER NOT NULL,
                book_hash TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                created_at_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metric_batches (
                batch_id TEXT NOT NULL,
                utc_date TEXT NOT NULL,
                market TEXT NOT NULL,
                stream TEXT NOT NULL,
                aggregate_json TEXT NOT NULL,
                committed_at_utc_ns INTEGER NOT NULL,
                PRIMARY KEY(batch_id, utc_date, market, stream)
            );
            CREATE INDEX IF NOT EXISTS metric_batches_by_day
                ON metric_batches(utc_date, market, stream);
            """
        )

    def record_metric_batch(
        self,
        *,
        batch_id: str,
        rows: Sequence[tuple[str, str, str, Mapping[str, object]]],
        committed_at_utc_ns: int | None = None,
    ) -> bool:
        """Atomically persist aggregate rows; retrying a batch never double-counts."""

        if not batch_id or not rows:
            raise ValueError("metric batch requires an identity and at least one row")
        committed_at = time.time_ns() if committed_at_utc_ns is None else committed_at_utc_ns
        if committed_at < 0:
            raise ValueError("metric batch commit time must be non-negative")
        serialized = [
            (
                batch_id,
                utc_date,
                market,
                stream,
                json.dumps(dict(aggregate), sort_keys=True, separators=(",", ":")),
                committed_at,
            )
            for utc_date, market, stream, aggregate in rows
        ]
        if any(not utc_date or not market or not stream for utc_date, market, stream, _ in rows):
            raise ValueError("metric batch row identity must be non-empty")
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT COUNT(*) AS count FROM metric_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if existing is not None and int(existing["count"]) > 0:
                return False
            connection.executemany(
                """
                INSERT INTO metric_batches(
                    batch_id, utc_date, market, stream, aggregate_json,
                    committed_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                serialized,
            )
        return True

    def metric_batches(self, utc_date: str) -> list[dict[str, object]]:
        return self._metric_batch_rows("utc_date = ?", (utc_date,))

    def metric_batches_through(self, utc_date: str) -> list[dict[str, object]]:
        return self._metric_batch_rows("utc_date <= ?", (utc_date,))

    def _metric_batch_rows(
        self, where_clause: str, parameters: tuple[object, ...]
    ) -> list[dict[str, object]]:
        if where_clause not in {"utc_date = ?", "utc_date <= ?"}:
            raise ValueError("unsupported metric batch query")
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT batch_id, utc_date, market, stream, aggregate_json,
                       committed_at_utc_ns
                FROM metric_batches
                WHERE {where_clause}
                ORDER BY utc_date, market, stream, batch_id
                """,
                parameters,
            ).fetchall()
        return [
            {
                "batch_id": str(row["batch_id"]),
                "utc_date": str(row["utc_date"]),
                "market": str(row["market"]),
                "stream": str(row["stream"]),
                "aggregate": json.loads(row["aggregate_json"]),
                "committed_at_utc_ns": int(row["committed_at_utc_ns"]),
            }
            for row in rows
        ]

    def metric_batch_count(self, batch_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM metric_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
        return int(row["count"])

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
        evidence_json = json.dumps(
            dict(evidence or {}), sort_keys=True, separators=(",", ":")
        )
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

    def register_quarantined_artifact(
        self, *, artifact_id: str, relative_path: str, reason: str, sha256: str
    ) -> None:
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

    def table_columns(self, table: str) -> set[str]:
        if table not in {
            "chunks",
            "chunk_transitions",
            "quarantined_artifacts",
            "orderbook_checkpoints",
            "metric_batches",
        }:
            raise ValueError("unknown Catalog table")
        with self._lock:
            return {
                str(row["name"])
                for row in self._connection.execute(f"PRAGMA table_info({table})")
            }
