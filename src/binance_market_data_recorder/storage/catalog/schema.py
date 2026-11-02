"""Catalog SQLite DDL, migrations, and schema validation."""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable, Mapping

from .availability import (
    _DISCONTINUITY_EVENT_TYPES,
    _SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES,
    _discontinuity_identity,
    _optional_identity_text,
)
from .base import CatalogBase
from .types import LEGACY_SINGLE_SYMBOL, CatalogStateError

_REMOTE_TRANSACTION_COLUMNS = {
    "receipt_id": ("TEXT", 0, 1),
    "chunk_id": ("TEXT", 1, 0),
    "state": ("TEXT", 1, 0),
    "receipt_bytes": ("BLOB", 1, 0),
    "receipt_schema_version": ("TEXT", 1, 0),
    "session_id": ("TEXT", 1, 0),
    "verification_version": ("TEXT", 1, 0),
    "verification_outcome": ("TEXT", 1, 0),
    "source_descriptor_schema_version": ("TEXT", 1, 0),
    "source_descriptor_sha256": ("TEXT", 1, 0),
    "market": ("TEXT", 1, 0),
    "stream": ("TEXT", 1, 0),
    "source_relative_path": ("TEXT", 1, 0),
    "source_manifest_relative_path": ("TEXT", 1, 0),
    "source_manifest_sha256": ("TEXT", 1, 0),
    "stored_bytes": ("INTEGER", 1, 0),
    "stored_sha256": ("TEXT", 1, 0),
    "archive_set_id": ("TEXT", 1, 0),
    "storage_id": ("TEXT", 1, 0),
    "artifact_relative_path": ("TEXT", 1, 0),
    "archive_set_entry_sha256": ("TEXT", 1, 0),
    "created_at_utc_ns": ("INTEGER", 1, 0),
    "updated_at_utc_ns": ("INTEGER", 1, 0),
    "remote_deleted_at_utc_ns": ("INTEGER", 0, 0),
}

_REMOTE_EVENT_COLUMNS = {
    "event_id": ("INTEGER", 0, 1),
    "receipt_id": ("TEXT", 1, 0),
    "from_state": ("TEXT", 0, 0),
    "to_state": ("TEXT", 1, 0),
    "occurred_at_utc_ns": ("INTEGER", 1, 0),
    "evidence_json": ("TEXT", 1, 0),
    "idempotency_key": ("TEXT", 1, 0),
}

_LEGACY_OPERATIONAL_EVENT_COLUMNS = {
    "event_id": ("TEXT", 0, 1),
    "event_type": ("TEXT", 1, 0),
    "occurred_at_utc_ns": ("INTEGER", 1, 0),
    "evidence_json": ("TEXT", 1, 0),
}

_CURRENT_OPERATIONAL_EVENT_COLUMNS = {
    **_LEGACY_OPERATIONAL_EVENT_COLUMNS,
    "market": ("TEXT", 0, 0),
    "symbol": ("TEXT", 0, 0),
    "stream": ("TEXT", 0, 0),
    "gap_id": ("TEXT", 0, 0),
}

_LEGACY_SIDE_DATA_CURSOR_COLUMNS = {
    "kind": ("TEXT", 0, 1),
    "last_persisted_period_timestamp": ("INTEGER", 1, 0),
    "updated_at_utc_ns": ("INTEGER", 1, 0),
    "source_retention_window": ("TEXT", 1, 0),
    "retention_window_ms": ("INTEGER", 1, 0),
}

_CURRENT_SIDE_DATA_CURSOR_COLUMNS = {
    "kind": ("TEXT", 1, 1),
    "symbol": ("TEXT", 1, 2),
    "last_persisted_period_timestamp": ("INTEGER", 1, 0),
    "updated_at_utc_ns": ("INTEGER", 1, 0),
    "source_retention_window": ("TEXT", 1, 0),
    "retention_window_ms": ("INTEGER", 1, 0),
}

_DISCONTINUITY_IDENTITY_INDEX = "operational_events_discontinuity_identity"

_CREATE_CURRENT_OPERATIONAL_EVENTS_SQL = """
    CREATE TABLE IF NOT EXISTS operational_events (
        event_id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        occurred_at_utc_ns INTEGER NOT NULL,
        evidence_json TEXT NOT NULL,
        market TEXT,
        symbol TEXT,
        stream TEXT,
        gap_id TEXT,
        CHECK (
            event_type NOT IN (
                'STREAM_DISCONTINUITY_STARTED',
                'STREAM_DISCONTINUITY_COMPLETED'
            )
            OR symbol IS NOT NULL
        )
    )
"""

_CREATE_CURRENT_SIDE_DATA_CURSORS_SQL = """
    CREATE TABLE IF NOT EXISTS side_data_cursors (
        kind TEXT NOT NULL,
        symbol TEXT NOT NULL,
        last_persisted_period_timestamp INTEGER NOT NULL,
        updated_at_utc_ns INTEGER NOT NULL,
        source_retention_window TEXT NOT NULL,
        retention_window_ms INTEGER NOT NULL,
        PRIMARY KEY(kind, symbol)
    )
"""

_CREATE_DISCONTINUITY_IDENTITY_INDEX_SQL = """
    CREATE INDEX IF NOT EXISTS operational_events_discontinuity_identity
    ON operational_events(event_type, market, symbol, stream, gap_id)
"""


class CatalogSchemaMixin(CatalogBase):
    """DDL execution, schema version migrations, and table structure validation."""

    @property
    def legacy_identity_schema(self) -> bool:
        """Whether this Catalog still exposes the pre-MS1 identity schema."""

        return self._legacy_identity_schema

    def migrate_acceptance_sequence(
        self, *, checkpoint: Callable[[str], None] | None = None
    ) -> None:
        """Atomic stopped-baseline migration for ADR-0034; never an observer write."""
        from ..acceptance_delta import migrate_sequence

        with self._transaction() as connection:
            migrate_sequence(connection, checkpoint=checkpoint)

    def _initialize(self) -> None:
        self._require_writable()
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
            CREATE TABLE IF NOT EXISTS source_availability (
                source_id TEXT PRIMARY KEY,
                state_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS source_availability_lifecycle (
                interval_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                started_at_utc_ns INTEGER NOT NULL,
                ended_at_utc_ns INTEGER
            );
            CREATE INDEX IF NOT EXISTS source_availability_lifecycle_source
                ON source_availability_lifecycle(source_id, started_at_utc_ns);
            CREATE TABLE IF NOT EXISTS source_availability_gaps (
                gap_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                started_at_utc_ns INTEGER NOT NULL,
                ended_at_utc_ns INTEGER,
                evidence_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS source_availability_by_source_time
                ON source_availability_gaps(source_id, started_at_utc_ns);
            CREATE TABLE IF NOT EXISTS source_availability_watermark (
                selection_id TEXT PRIMARY KEY,
                evaluated_through_utc_ns INTEGER NOT NULL,
                updated_at_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raw_verification_queue (
                chunk_id TEXT PRIMARY KEY REFERENCES chunks(chunk_id),
                enrolled_at_utc_ns INTEGER NOT NULL,
                next_attempt_utc_ns INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                verified_result_id TEXT
            );
            CREATE INDEX IF NOT EXISTS raw_verification_due
                ON raw_verification_queue(verified_result_id, next_attempt_utc_ns);
            CREATE TABLE IF NOT EXISTS raw_verification_results (
                result_id TEXT PRIMARY KEY,
                chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id),
                evidence_json TEXT NOT NULL,
                verifier_version TEXT
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
            CREATE INDEX IF NOT EXISTS chunks_by_state_created_id
                ON chunks(state, created_at_utc_ns, chunk_id);
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
            CREATE TABLE IF NOT EXISTS storage_targets (
                storage_id TEXT PRIMARY KEY,
                volume_uuid TEXT NOT NULL,
                volume_name TEXT,
                filesystem_type TEXT,
                relative_path TEXT NOT NULL,
                marker_nonce TEXT NOT NULL,
                registered_at_utc_ns INTEGER NOT NULL,
                UNIQUE(volume_uuid, relative_path)
            );
            CREATE TABLE IF NOT EXISTS storage_control (
                storage_id TEXT PRIMARY KEY REFERENCES storage_targets(storage_id),
                state TEXT NOT NULL,
                request_id TEXT,
                updated_at_utc_ns INTEGER NOT NULL,
                evidence_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS archive_transactions (
                transaction_id TEXT PRIMARY KEY,
                chunk_id TEXT NOT NULL UNIQUE REFERENCES chunks(chunk_id),
                storage_id TEXT NOT NULL,
                state TEXT NOT NULL,
                market TEXT NOT NULL,
                stream TEXT NOT NULL,
                source_relative_path TEXT NOT NULL,
                source_manifest_relative_path TEXT NOT NULL,
                source_manifest_sha256 TEXT NOT NULL,
                target_relative_path TEXT NOT NULL,
                target_temp_relative_path TEXT NOT NULL,
                external_manifest_relative_path TEXT NOT NULL,
                stored_bytes INTEGER NOT NULL,
                stored_sha256 TEXT NOT NULL,
                created_at_utc_ns INTEGER NOT NULL,
                updated_at_utc_ns INTEGER NOT NULL,
                verified_at_utc_ns INTEGER,
                local_deleted_at_utc_ns INTEGER,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );
            CREATE INDEX IF NOT EXISTS archive_transactions_by_state
                ON archive_transactions(state, created_at_utc_ns, transaction_id);
            CREATE TABLE IF NOT EXISTS archive_transaction_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT NOT NULL REFERENCES archive_transactions(transaction_id),
                from_state TEXT,
                to_state TEXT NOT NULL,
                occurred_at_utc_ns INTEGER NOT NULL,
                evidence_json TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS storage_space_samples (
                sample_id TEXT PRIMARY KEY,
                scope_id TEXT NOT NULL,
                storage_id TEXT,
                observed_at_utc_ns INTEGER NOT NULL,
                total_bytes INTEGER NOT NULL,
                free_bytes INTEGER NOT NULL,
                archive_backlog_bytes INTEGER NOT NULL,
                oldest_unarchived_at_utc_ns INTEGER
            );
            CREATE INDEX IF NOT EXISTS storage_space_samples_by_scope_time
                ON storage_space_samples(scope_id, observed_at_utc_ns, sample_id);
            CREATE TABLE IF NOT EXISTS storage_alert_state (
                scope_id TEXT PRIMARY KEY,
                severity TEXT NOT NULL,
                updated_at_utc_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS storage_alert_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope_id TEXT NOT NULL,
                from_severity TEXT,
                to_severity TEXT NOT NULL,
                occurred_at_utc_ns INTEGER NOT NULL,
                evidence_json TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS deployment_sessions (
                deployment_id TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                market TEXT NOT NULL,
                symbol TEXT NOT NULL,
                active_instance_id TEXT NOT NULL,
                active_version TEXT NOT NULL,
                candidate_instance_id TEXT NOT NULL,
                candidate_version TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at_utc_ns INTEGER NOT NULL,
                updated_at_utc_ns INTEGER NOT NULL,
                overlap_started_at_utc_ns INTEGER,
                cutover_at_utc_ns INTEGER,
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS deployment_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                deployment_id TEXT NOT NULL
                    REFERENCES deployment_sessions(deployment_id),
                from_state TEXT,
                to_state TEXT NOT NULL,
                occurred_at_utc_ns INTEGER NOT NULL,
                evidence_json TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE
            );
            """
        )
        if "verifier_version" not in self.table_columns("raw_verification_results"):
            # Metadata-only upgrade. Existing proof bodies/IDs are never rewritten;
            # NULL records require bounded revalidation by the corrected verifier.
            self._connection.execute(
                "ALTER TABLE raw_verification_results ADD COLUMN verifier_version TEXT"
            )
        self._create_current_operational_events(self._connection)
        self._create_current_side_data_cursors(self._connection)

    @staticmethod
    def _create_current_operational_events(connection: sqlite3.Connection) -> None:
        connection.execute(_CREATE_CURRENT_OPERATIONAL_EVENTS_SQL)

    @staticmethod
    def _create_current_side_data_cursors(connection: sqlite3.Connection) -> None:
        connection.execute(_CREATE_CURRENT_SIDE_DATA_CURSORS_SQL)

    @staticmethod
    def _create_discontinuity_identity_index(
        connection: sqlite3.Connection,
    ) -> None:
        connection.execute(_CREATE_DISCONTINUITY_IDENTITY_INDEX_SQL)

    def _fresh_initialization_checkpoint(self, phase: str) -> None:
        """Private deterministic fault seam for fresh MS1 identity tests."""

        if self._migration_fault_phase == phase:
            raise RuntimeError(f"injected MS1 fresh initialization failure: {phase}")

    def _initialize_fresh_symbol_identity_schema(self) -> None:
        """Atomically create every identity object for a genuinely fresh Catalog."""

        with self._transaction() as connection:
            if self._identity_schema_decision() != "fresh":
                raise CatalogStateError("symbol identity schema changed before creation")
            self._create_current_operational_events(connection)
            self._fresh_initialization_checkpoint("after_operational_events")
            self._create_current_side_data_cursors(connection)
            self._fresh_initialization_checkpoint("after_cursor_table")
            self._fresh_initialization_checkpoint("before_index")
            self._create_discontinuity_identity_index(connection)
            self._fresh_initialization_checkpoint("after_index")
            self._validate_symbol_identity_schema(allow_legacy=False)
        self._fresh_initialization_checkpoint("after_fresh_identity_commit")

    def _identity_schema_state(self, table: str) -> str:
        table_row = self._connection.execute(
            "SELECT type FROM sqlite_master WHERE name = ?", (table,)
        ).fetchone()
        if table_row is None:
            return "absent"
        if str(table_row["type"]) != "table":
            raise CatalogStateError(f"partial symbol identity schema: {table}")
        columns = {
            str(row["name"]): (
                str(row["type"]).upper(),
                int(row["notnull"]),
                int(row["pk"]),
            )
            for row in self._connection.execute(f"PRAGMA table_info({table})")
        }
        if columns == _LEGACY_OPERATIONAL_EVENT_COLUMNS and table == "operational_events":
            return "legacy"
        if columns == _CURRENT_OPERATIONAL_EVENT_COLUMNS and table == "operational_events":
            return "current"
        if columns == _LEGACY_SIDE_DATA_CURSOR_COLUMNS and table == "side_data_cursors":
            return "legacy"
        if columns == _CURRENT_SIDE_DATA_CURSOR_COLUMNS and table == "side_data_cursors":
            return "current"
        raise CatalogStateError(f"partial symbol identity schema: {table}")

    def _identity_schema_decision(self) -> str:
        """Classify only repository-supported identity-schema histories."""

        self._reject_identity_migration_artifacts()
        operational_state = self._identity_schema_state("operational_events")
        cursor_state = self._identity_schema_state("side_data_cursors")
        states = (operational_state, cursor_state)
        if states == ("absent", "absent"):
            return "fresh"
        if states == ("legacy", "absent"):
            return "legacy_pre_m19"
        if states == ("legacy", "legacy"):
            return "legacy_c421"
        if states == ("current", "current"):
            return "current"
        raise CatalogStateError("partial symbol identity migration")

    def _validate_symbol_identity_schema(self, *, allow_legacy: bool) -> None:
        decision = self._identity_schema_decision()
        if decision in {"legacy_pre_m19", "legacy_c421"}:
            if not allow_legacy:
                raise CatalogStateError("Catalog requires symbol identity migration")
            self._legacy_identity_schema = True
            return
        if decision == "fresh":
            raise CatalogStateError("Catalog identity schema is absent")
        self._validate_discontinuity_identity_index()
        table_row = self._connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'operational_events'"
        ).fetchone()
        compact = re.sub(r"\s+", "", str(table_row["sql"])).upper() if table_row else ""
        if (
            "CHECK(" not in compact
            or "STREAM_DISCONTINUITY_STARTED" not in compact
            or "STREAM_DISCONTINUITY_COMPLETED" not in compact
            or "SYMBOLISNOTNULL" not in compact
        ):
            raise CatalogStateError("malformed operational discontinuity constraint")
        self._validate_symbol_identity_rows()
        self._legacy_identity_schema = False

    def _reject_identity_migration_artifacts(self) -> None:
        artifacts = self._connection.execute(
            "SELECT name FROM sqlite_master WHERE name IN (?, ?)",
            (
                "operational_events__ms1_legacy",
                "side_data_cursors__ms1_legacy",
            ),
        ).fetchall()
        if artifacts:
            raise CatalogStateError("partial symbol identity migration artifacts present")

    def _validate_discontinuity_identity_index(self) -> None:
        metadata = next(
            (
                row
                for row in self._connection.execute(
                    "PRAGMA index_list(operational_events)"
                ).fetchall()
                if row["name"] == _DISCONTINUITY_IDENTITY_INDEX
            ),
            None,
        )
        if metadata is None or int(metadata["unique"]) or int(metadata["partial"]):
            raise CatalogStateError("missing or malformed discontinuity identity index")
        columns = tuple(
            str(row["name"])
            for row in self._connection.execute(
                f"PRAGMA index_info({_DISCONTINUITY_IDENTITY_INDEX})"
            ).fetchall()
        )
        if columns != ("event_type", "market", "symbol", "stream", "gap_id"):
            raise CatalogStateError("malformed discontinuity identity index")

    def _validate_symbol_identity_rows(self) -> None:
        rows = self._connection.execute(
            "SELECT event_type, evidence_json, market, symbol, stream, gap_id "
            "FROM operational_events"
        ).fetchall()
        for row in rows:
            event_type = str(row["event_type"])
            if event_type in _SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES:
                try:
                    side_evidence = json.loads(str(row["evidence_json"]))
                except (TypeError, json.JSONDecodeError) as exc:
                    raise CatalogStateError("malformed symbol-specific side-data evidence") from exc
                if (
                    not isinstance(side_evidence, dict)
                    or not isinstance(side_evidence.get("symbol"), str)
                    or not side_evidence["symbol"]
                ):
                    raise CatalogStateError("malformed symbol-specific side-data identity")
                if any(row[name] is not None for name in ("market", "symbol", "stream", "gap_id")):
                    raise CatalogStateError("side-data event carries stream identity columns")
                continue
            if event_type not in _DISCONTINUITY_EVENT_TYPES:
                if any(row[name] is not None for name in ("market", "symbol", "stream", "gap_id")):
                    raise CatalogStateError("non-discontinuity event carries stream identity")
                continue
            try:
                evidence = json.loads(str(row["evidence_json"]))
            except (TypeError, json.JSONDecodeError) as exc:
                raise CatalogStateError("malformed discontinuity evidence") from exc
            if not isinstance(evidence, dict):
                raise CatalogStateError("malformed discontinuity evidence")
            evidence_symbol = evidence.get("symbol")
            if (
                not isinstance(row["symbol"], str)
                or not row["symbol"]
                or evidence_symbol != row["symbol"]
            ):
                raise CatalogStateError("malformed symbol-aware discontinuity evidence")
            identity = _discontinuity_identity(evidence, allow_legacy_symbol=False)
            expected: tuple[str | None, str | None, str | None, str | None]
            if identity is not None:
                expected = identity
            else:
                expected = (
                    _optional_identity_text(evidence.get("market")),
                    str(row["symbol"]),
                    _optional_identity_text(evidence.get("stream")),
                    _optional_identity_text(evidence.get("gap_id")),
                )
            if expected != (
                row["market"],
                row["symbol"],
                row["stream"],
                row["gap_id"],
            ):
                raise CatalogStateError("discontinuity identity column mismatch")

    def _migration_checkpoint(self, phase: str) -> None:
        """Private deterministic rollback seam for MS1 migration tests."""

        if self._migration_fault_phase == phase:
            raise RuntimeError(f"injected MS1 migration failure: {phase}")

    def _migrate_symbol_identity_schema(self, expected_decision: str) -> None:
        """Atomically migrate the historical single-symbol Catalog schema."""

        if expected_decision not in {"legacy_pre_m19", "legacy_c421"}:
            raise CatalogStateError("unsupported symbol identity migration decision")

        with self._transaction() as connection:
            if self._identity_schema_decision() != expected_decision:
                raise CatalogStateError("symbol identity schema changed before migration")

            event_rows = connection.execute(
                "SELECT event_id, event_type, occurred_at_utc_ns, evidence_json "
                "FROM operational_events ORDER BY event_id"
            ).fetchall()
            event_updates: dict[str, tuple[str, str, str, str, str]] = {}
            event_bodies: dict[str, str] = {}
            for row in event_rows:
                event_type = str(row["event_type"])
                if event_type in _SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES:
                    try:
                        side_evidence = json.loads(str(row["evidence_json"]))
                    except (TypeError, json.JSONDecodeError) as exc:
                        raise CatalogStateError(
                            "malformed symbol-specific side-data evidence during migration"
                        ) from exc
                    if not isinstance(side_evidence, dict):
                        raise CatalogStateError(
                            "symbol-specific side-data evidence must be an object"
                        )
                    side_symbol = side_evidence.get("symbol")
                    if "symbol" not in side_evidence:
                        side_evidence = {
                            **side_evidence,
                            "symbol": LEGACY_SINGLE_SYMBOL,
                        }
                    elif not isinstance(side_symbol, str) or not side_symbol:
                        raise CatalogStateError(
                            "malformed symbol-specific side-data symbol during migration"
                        )
                    event_bodies[str(row["event_id"])] = json.dumps(
                        side_evidence, sort_keys=True, separators=(",", ":")
                    )
                    continue
                if event_type not in _DISCONTINUITY_EVENT_TYPES:
                    continue
                try:
                    evidence = json.loads(str(row["evidence_json"]))
                except (TypeError, json.JSONDecodeError) as exc:
                    raise CatalogStateError(
                        "malformed discontinuity evidence during migration"
                    ) from exc
                event_identity = _discontinuity_identity(evidence, allow_legacy_symbol=True)
                if event_identity is None:
                    raise CatalogStateError("malformed discontinuity identity during migration")
                market, symbol, stream, gap_id = event_identity
                if "symbol" not in evidence:
                    evidence = {**evidence, "symbol": symbol}
                body = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
                event_updates[str(row["event_id"])] = (
                    body,
                    market,
                    symbol,
                    stream,
                    gap_id,
                )
                event_bodies[str(row["event_id"])] = body

            cursor_rows = (
                connection.execute(
                    "SELECT kind, last_persisted_period_timestamp, "
                    "updated_at_utc_ns, source_retention_window, retention_window_ms "
                    "FROM side_data_cursors ORDER BY kind"
                ).fetchall()
                if expected_decision == "legacy_c421"
                else []
            )
            for row in cursor_rows:
                if (
                    not isinstance(row["kind"], str)
                    or not str(row["kind"])
                    or isinstance(row["last_persisted_period_timestamp"], bool)
                    or not isinstance(row["last_persisted_period_timestamp"], int)
                    or int(row["last_persisted_period_timestamp"]) < 0
                    or isinstance(row["updated_at_utc_ns"], bool)
                    or not isinstance(row["updated_at_utc_ns"], int)
                    or int(row["updated_at_utc_ns"]) < 0
                    or not isinstance(row["source_retention_window"], str)
                    or not str(row["source_retention_window"])
                    or isinstance(row["retention_window_ms"], bool)
                    or not isinstance(row["retention_window_ms"], int)
                    or int(row["retention_window_ms"]) <= 0
                ):
                    raise CatalogStateError("malformed side-data cursor during migration")

            connection.execute(
                "ALTER TABLE operational_events RENAME TO operational_events__ms1_legacy"
            )
            self._migration_checkpoint("after_rename")
            self._create_current_operational_events(connection)
            copied_event = False
            for row in event_rows:
                identity = event_updates.get(str(row["event_id"]))
                connection.execute(
                    """
                    INSERT INTO operational_events(
                        event_id, event_type, occurred_at_utc_ns, evidence_json,
                        market, symbol, stream, gap_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row["event_id"]),
                        str(row["event_type"]),
                        int(row["occurred_at_utc_ns"]),
                        event_bodies.get(
                            str(row["event_id"]),
                            str(row["evidence_json"]),
                        ),
                        identity[1] if identity is not None else None,
                        identity[2] if identity is not None else None,
                        identity[3] if identity is not None else None,
                        identity[4] if identity is not None else None,
                    ),
                )
                if not copied_event:
                    copied_event = True
                    self._migration_checkpoint("mid_copy")
            if not copied_event:
                self._migration_checkpoint("mid_copy")

            if expected_decision == "legacy_c421":
                connection.execute(
                    "ALTER TABLE side_data_cursors RENAME TO side_data_cursors__ms1_legacy"
                )
            self._create_current_side_data_cursors(connection)
            for row in cursor_rows:
                connection.execute(
                    """
                    INSERT INTO side_data_cursors(
                        kind, symbol, last_persisted_period_timestamp,
                        updated_at_utc_ns, source_retention_window,
                        retention_window_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row["kind"]),
                        LEGACY_SINGLE_SYMBOL,
                        int(row["last_persisted_period_timestamp"]),
                        int(row["updated_at_utc_ns"]),
                        str(row["source_retention_window"]),
                        int(row["retention_window_ms"]),
                    ),
                )
            connection.execute("DROP TABLE operational_events__ms1_legacy")
            if expected_decision == "legacy_c421":
                connection.execute("DROP TABLE side_data_cursors__ms1_legacy")
            self._migration_checkpoint("after_drop")
            self._migration_checkpoint("before_index")
            self._create_discontinuity_identity_index(connection)
            self._migration_checkpoint("after_index")
            self._validate_symbol_identity_schema(allow_legacy=False)
            self._migration_checkpoint("before_commit")

    def _inspect_remote_schema(self, *, create: bool) -> bool:
        """Create/validate the additive M22.4A projection, or accept legacy absence."""

        names = {
            str(row["name"])
            for row in self._connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name IN ('remote_archive_transactions', 'remote_archive_events')"
            )
        }
        expected = {"remote_archive_transactions", "remote_archive_events"}
        if names and names != expected:
            raise CatalogStateError("partial remote archive schema")
        if not names:
            if not create:
                return False
            with self._transaction() as connection:
                connection.execute(
                    """
                    CREATE TABLE remote_archive_transactions (
                        receipt_id TEXT PRIMARY KEY,
                        chunk_id TEXT NOT NULL UNIQUE REFERENCES chunks(chunk_id),
                        state TEXT NOT NULL CHECK (state IN (
                            'REMOTE_DELETE_PENDING', 'REMOTE_DELETED'
                        )),
                        receipt_bytes BLOB NOT NULL,
                        receipt_schema_version TEXT NOT NULL,
                        session_id TEXT NOT NULL,
                        verification_version TEXT NOT NULL,
                        verification_outcome TEXT NOT NULL,
                        source_descriptor_schema_version TEXT NOT NULL,
                        source_descriptor_sha256 TEXT NOT NULL,
                        market TEXT NOT NULL,
                        stream TEXT NOT NULL,
                        source_relative_path TEXT NOT NULL,
                        source_manifest_relative_path TEXT NOT NULL,
                        source_manifest_sha256 TEXT NOT NULL,
                        stored_bytes INTEGER NOT NULL,
                        stored_sha256 TEXT NOT NULL,
                        archive_set_id TEXT NOT NULL,
                        storage_id TEXT NOT NULL,
                        artifact_relative_path TEXT NOT NULL,
                        archive_set_entry_sha256 TEXT NOT NULL,
                        created_at_utc_ns INTEGER NOT NULL,
                        updated_at_utc_ns INTEGER NOT NULL,
                        remote_deleted_at_utc_ns INTEGER,
                        CHECK (
                            (state = 'REMOTE_DELETE_PENDING'
                                AND remote_deleted_at_utc_ns IS NULL)
                            OR (state = 'REMOTE_DELETED'
                                AND remote_deleted_at_utc_ns IS NOT NULL)
                        )
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX remote_archive_transactions_by_state
                    ON remote_archive_transactions(state, created_at_utc_ns, receipt_id)
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE remote_archive_events (
                        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        receipt_id TEXT NOT NULL
                            REFERENCES remote_archive_transactions(receipt_id),
                        from_state TEXT,
                        to_state TEXT NOT NULL,
                        occurred_at_utc_ns INTEGER NOT NULL,
                        evidence_json TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL UNIQUE
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX remote_archive_events_by_receipt
                    ON remote_archive_events(receipt_id, event_id)
                    """
                )
                self._validate_remote_schema()
            return True
        self._validate_remote_schema()
        return True

    def _validate_remote_schema(self) -> None:
        self._validate_table_columns("remote_archive_transactions", _REMOTE_TRANSACTION_COLUMNS)
        self._validate_table_columns("remote_archive_events", _REMOTE_EVENT_COLUMNS)
        self._validate_foreign_key("remote_archive_transactions", "chunk_id", "chunks", "chunk_id")
        self._validate_foreign_key(
            "remote_archive_events",
            "receipt_id",
            "remote_archive_transactions",
            "receipt_id",
        )
        self._validate_unique_index("remote_archive_transactions", ("chunk_id",))
        self._validate_unique_index("remote_archive_events", ("idempotency_key",))
        self._validate_named_index(
            "remote_archive_transactions_by_state",
            "remote_archive_transactions",
            ("state", "created_at_utc_ns", "receipt_id"),
        )
        self._validate_named_index(
            "remote_archive_events_by_receipt",
            "remote_archive_events",
            ("receipt_id", "event_id"),
        )
        row = self._connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'remote_archive_transactions'"
        ).fetchone()
        compact = re.sub(r"\s+", "", str(row["sql"])).upper() if row else ""
        required_fragments = (
            "CHECK(STATEIN('REMOTE_DELETE_PENDING','REMOTE_DELETED'))",
            "CHECK((STATE='REMOTE_DELETE_PENDING'ANDREMOTE_DELETED_AT_UTC_NSISNULL)OR"
            "(STATE='REMOTE_DELETED'ANDREMOTE_DELETED_AT_UTC_NSISNOTNULL))",
        )
        if any(fragment not in compact for fragment in required_fragments):
            raise CatalogStateError("malformed remote archive state constraints")

    def _validate_table_columns(
        self, table: str, expected: Mapping[str, tuple[str, int, int]]
    ) -> None:
        rows = self._connection.execute(f"PRAGMA table_info({table})").fetchall()
        actual = {
            str(row["name"]): (str(row["type"]).upper(), int(row["notnull"]), int(row["pk"]))
            for row in rows
        }
        if actual != dict(expected):
            raise CatalogStateError(f"malformed remote Catalog table: {table}")

    def _validate_foreign_key(
        self, table: str, source: str, target_table: str, target: str
    ) -> None:
        rows = self._connection.execute(f"PRAGMA foreign_key_list({table})").fetchall()
        matches = [
            row
            for row in rows
            if row["from"] == source and row["table"] == target_table and row["to"] == target
        ]
        if len(rows) != 1 or len(matches) != 1:
            raise CatalogStateError(f"malformed remote Catalog foreign key: {table}")

    def _validate_unique_index(self, table: str, columns: tuple[str, ...]) -> None:
        for row in self._connection.execute(f"PRAGMA index_list({table})").fetchall():
            if not int(row["unique"]) or int(row["partial"]):
                continue
            names = tuple(
                str(item["name"])
                for item in self._connection.execute(f"PRAGMA index_info({row['name']})").fetchall()
            )
            if names == columns:
                return
        raise CatalogStateError(f"missing remote Catalog UNIQUE constraint: {table}")

    def _validate_named_index(self, name: str, table: str, columns: tuple[str, ...]) -> None:
        row = self._connection.execute(
            "SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        metadata = next(
            (
                item
                for item in self._connection.execute(f"PRAGMA index_list({table})").fetchall()
                if item["name"] == name
            ),
            None,
        )
        actual = tuple(
            str(item["name"])
            for item in self._connection.execute(f"PRAGMA index_info({name})").fetchall()
        )
        if (
            row is None
            or row["tbl_name"] != table
            or metadata is None
            or int(metadata["unique"])
            or int(metadata["partial"])
            or actual != columns
        ):
            raise CatalogStateError(f"missing or malformed remote Catalog index: {name}")
