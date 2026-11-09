"""Durable, bounded Catalog read authority for ADR-0034.

Migration is explicit and runs only in the stopped first-V5 baseline. Online
snapshots never migrate, count history, or run an integrity check.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator, Mapping
from typing import Any

SQL_PAGE_CAP = 256
CURSORS = {
    "chunk": ("chunk_transitions", "transition_id"),
    "archive": ("archive_transaction_events", "event_id"),
    "operational": ("operational_event_sequence", "event_seq"),
}
SEQUENCE_SQL = """CREATE TABLE operational_event_sequence (
    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE REFERENCES operational_events(event_id)
)"""
TRIGGER_SQL = """CREATE TRIGGER operational_event_sequence_insert
AFTER INSERT ON operational_events BEGIN
    INSERT INTO operational_event_sequence(event_id) VALUES (NEW.event_id);
END"""
OPEN_INDEX_SQL = "CREATE INDEX acceptance_chunks_by_state ON chunks(state, chunk_id)"


class DeltaAuthorityError(RuntimeError):
    """Catalog delta authority is unavailable or inconsistent."""


def _normalized(sql: str) -> str:
    return " ".join(sql.split()).casefold()


def inspect_sequence(connection: sqlite3.Connection, *, required: bool = False) -> bool:
    objects = dict(
        connection.execute(
            "SELECT name, sql FROM sqlite_master WHERE name IN (?, ?, ?)",
            (
                "operational_event_sequence",
                "operational_event_sequence_insert",
                "acceptance_chunks_by_state",
            ),
        )
    )
    if not objects and not required:
        return False
    expected = {
        "operational_event_sequence": SEQUENCE_SQL,
        "operational_event_sequence_insert": TRIGGER_SQL,
        "acceptance_chunks_by_state": OPEN_INDEX_SQL,
    }
    if set(objects) != set(expected) or any(
        _normalized(str(objects[key])) != _normalized(sql) for key, sql in expected.items()
    ):
        raise DeltaAuthorityError("partial or incompatible operational sequence schema")
    return True


def validate_sequence(connection: sqlite3.Connection) -> None:
    """Full one-to-one validation: baseline/terminal only."""
    inspect_sequence(connection, required=True)
    if (
        connection.execute(
            "SELECT 1 FROM operational_events e LEFT JOIN operational_event_sequence s "
            "ON s.event_id=e.event_id WHERE s.event_id IS NULL LIMIT 1"
        ).fetchone()
        or connection.execute(
            "SELECT 1 FROM operational_event_sequence s LEFT JOIN operational_events e "
            "ON s.event_id=e.event_id WHERE e.event_id IS NULL LIMIT 1"
        ).fetchone()
    ):
        raise DeltaAuthorityError("operational sequence is not one-to-one")


def migrate_sequence(
    connection: sqlite3.Connection, *, checkpoint: Callable[[str], None] | None = None
) -> None:
    """Called inside Catalog's atomic writable transaction."""
    if inspect_sequence(connection):
        validate_sequence(connection)
        return
    connection.execute(SEQUENCE_SQL)
    if checkpoint:
        checkpoint("after_table")
    connection.execute(
        "INSERT INTO operational_event_sequence(event_id) SELECT event_id "
        "FROM operational_events ORDER BY occurred_at_utc_ns, event_id"
    )
    if checkpoint:
        checkpoint("after_backfill")
    connection.execute(TRIGGER_SQL)
    connection.execute(OPEN_INDEX_SQL)
    if checkpoint:
        checkpoint("after_trigger")
    validate_sequence(connection)


class DeltaSnapshot:
    """All methods share the caller's one SQLite read transaction."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        inspect_sequence(connection, required=True)
        self.high_water = {
            family: int(
                connection.execute(f"SELECT COALESCE(MAX({key}),0) FROM {table}").fetchone()[0]
            )
            for family, (table, key) in CURSORS.items()
        }

    def page(
        self, family: str, processed: int, *, limit: int = SQL_PAGE_CAP
    ) -> list[dict[str, Any]]:
        if family not in CURSORS or not 1 <= limit <= SQL_PAGE_CAP:
            raise DeltaAuthorityError("invalid cursor family/page cap")
        if isinstance(processed, bool) or not 0 <= processed <= self.high_water[family]:
            raise DeltaAuthorityError("cursor regression")
        table, key = CURSORS[family]
        if family == "operational":
            query = (
                "SELECT s.event_seq, e.* FROM operational_event_sequence s "
                "LEFT JOIN operational_events e ON e.event_id=s.event_id "
                "WHERE s.event_seq>? AND s.event_seq<=? ORDER BY s.event_seq LIMIT ?"
            )
        else:
            query = f"SELECT * FROM {table} WHERE {key}>? AND {key}<=? ORDER BY {key} LIMIT ?"
        rows = [
            dict(row)
            for row in self.connection.execute(
                query, (processed, self.high_water[family], limit)
            ).fetchmany(limit)
        ]
        if any(row.get("event_id") is None for row in rows) and family == "operational":
            raise DeltaAuthorityError("operational sequence has missing event")
        return rows

    def exact(self, table: str, key: str, value: object) -> dict[str, Any] | None:
        allowed = {
            "chunks": {"chunk_id"},
            "archive_transactions": {"transaction_id", "chunk_id"},
            "chunk_transitions": {"transition_id", "idempotency_key"},
            "archive_transaction_events": {"event_id", "idempotency_key"},
            "operational_event_sequence": {"event_id", "event_seq"},
            "operational_events": {"event_id"},
            "storage_targets": {"storage_id"},
            "storage_control": {"storage_id"},
        }
        if key not in allowed.get(table, set()):
            raise DeltaAuthorityError("unbounded or unindexed companion query")
        rows = self.connection.execute(
            f"SELECT * FROM {table} WHERE {key}=? LIMIT 2", (value,)
        ).fetchmany(2)
        if len(rows) > 1:
            raise DeltaAuthorityError("ambiguous exact companion")
        return dict(rows[0]) if rows else None

    def archive_lifecycle(self, transaction_id: str) -> dict[str, Any]:
        transaction = self.exact("archive_transactions", "transaction_id", transaction_id)
        if transaction is None:
            raise DeltaAuthorityError("missing archive transaction")
        chunk = self.exact("chunks", "chunk_id", transaction["chunk_id"])
        prefixes = (
            "reserve",
            "archive-verifying",
            "archive-verified",
            "local-delete-pending",
            "local-deleted",
        )
        events, transitions = [], []
        for prefix in prefixes:
            key = f"{prefix}:{transaction_id}"
            event = self.exact("archive_transaction_events", "idempotency_key", key)
            chunk_key = (
                f"archive-reserve:{transaction_id}" if prefix == "reserve" else f"chunk:{key}"
            )
            transition = self.exact("chunk_transitions", "idempotency_key", chunk_key)
            if event is not None:
                events.append(event)
            if transition is not None:
                transitions.append(transition)
        target = self.exact("storage_targets", "storage_id", transaction["storage_id"])
        return {
            "transaction": transaction,
            "chunk": chunk,
            "events": events,
            "transitions": transitions,
            "target": target,
        }

    def discontinuity_pair(self, row: Mapping[str, Any]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for event_type in ("STREAM_DISCONTINUITY_STARTED", "STREAM_DISCONTINUITY_COMPLETED"):
            rows = self.connection.execute(
                "SELECT s.event_seq, e.* FROM operational_events e "
                "LEFT JOIN operational_event_sequence s ON s.event_id=e.event_id "
                "WHERE e.event_type=? AND e.market=? AND e.symbol=? AND e.stream=? AND e.gap_id=? "
                "LIMIT 3",
                (event_type, row["market"], row["symbol"], row["stream"], row["gap_id"]),
            ).fetchmany(3)
            if len(rows) > 1:
                raise DeltaAuthorityError("ambiguous discontinuity companion")
            result.extend(dict(item) for item in rows)
        return result

    def companions(self, family: str, row: Mapping[str, Any]) -> dict[str, Any]:
        if family == "archive":
            return {"archive": self.archive_lifecycle(str(row["transaction_id"]))}
        if family == "chunk":
            chunk = self.exact("chunks", "chunk_id", row["chunk_id"])
            transaction = self.exact("archive_transactions", "chunk_id", row["chunk_id"])
            return {
                "chunk": chunk,
                "archive": (
                    self.archive_lifecycle(str(transaction["transaction_id"]))
                    if transaction
                    else None
                ),
            }
        if str(row.get("event_type", "")).startswith("STREAM_DISCONTINUITY_"):
            return {"discontinuity": self.discontinuity_pair(row)}
        return {}

    def bounded_open_chunks(self, limit: int) -> list[dict[str, Any]]:
        # An additive state index is verified/created by the baseline migration.
        rows = self.connection.execute(
            "SELECT * FROM chunks WHERE state IN ('ACTIVE','RECOVERED','SEALING') "
            "ORDER BY state, chunk_id LIMIT ?",
            (limit + 1,),
        ).fetchmany(limit + 1)
        if len(rows) > limit:
            raise DeltaAuthorityError("open chunk topology bound exceeded")
        return [dict(row) for row in rows]

    def archive_backlog(self) -> dict[str, int]:
        # Indexed, unfinished states only: retired history is never counted online.
        return {
            state: int(
                self.connection.execute(
                    "SELECT COUNT(*) FROM archive_transactions WHERE state=?", (state,)
                ).fetchone()[0]
            )
            for state in ("COPYING", "VERIFYING", "VERIFIED", "LOCAL_DELETE_PENDING")
        }


def ordered_rows(connection: sqlite3.Connection, table: str, key: str) -> Iterator[dict[str, Any]]:
    """Full audit only; never materialize Catalog history."""
    if (table, key) not in {
        ("chunks", "chunk_id"),
        ("chunk_transitions", "transition_id"),
        ("archive_transactions", "transaction_id"),
        ("archive_transaction_events", "event_id"),
        ("operational_event_sequence", "event_seq"),
        ("storage_targets", "storage_id"),
    }:
        raise DeltaAuthorityError("unsupported full audit family")
    if table == "operational_event_sequence":
        query = (
            "SELECT s.event_seq,e.* FROM operational_event_sequence s "
            "LEFT JOIN operational_events e ON e.event_id=s.event_id ORDER BY s.event_seq"
        )
    else:
        query = f"SELECT * FROM {table} ORDER BY {key}"
    for row in connection.execute(query):
        yield dict(row)
