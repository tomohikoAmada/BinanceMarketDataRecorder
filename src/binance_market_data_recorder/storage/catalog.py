"""SQLite 生命周期 Catalog;market-event 负载永不进入此数据库。

Catalog 是 Recorder 中所有状态转换的唯一持久协调点。它存储生命周期元数据,
而非 market-event 语料库。

状态机:
- ChunkState:ACTIVE -> RECOVERED/SEALING->SEALED -> ARCHIVE_COPYING ->
  ARCHIVE_VERIFYING -> ARCHIVED_VERIFIED -> LOCAL_DELETE_PENDING -> LOCAL_DELETED。
  Quarantine 是终态。允许的转换见 ALLOWED_TRANSITIONS。
- ArchiveState:COPYING -> VERIFYING -> VERIFIED -> LOCAL_DELETE_PENDING ->
  LOCAL_DELETED。Chunk 和 archive 状态必须始终一致(ARCHIVE_CHUNK_STATES)。
- DeploymentState:CANDIDATE_STARTING -> CANDIDATE_READY -> OVERLAP_CONFIRMED ->
  CUTOVER_COMPLETE。ROLLED_BACK 可从任何非终态到达。

事务语义:
- 所有写入使用 BEGIN IMMEDIATE 事务,受 RLock 保护。RLock 串行化所有 Catalog
  访问,确保状态转换原子性。
- chunk_transitions、archive_transaction_events 和 deployment_events 中的
  幂等键防止崩溃后重放同一转换。
- WAL journal mode + synchronous=FULL + foreign_keys=ON。
- Catalog 通过 RLock 保证单个实例内的线程安全。经认证的多进程边界是一个
  Recorder 写进程、一个 Archive 写进程和一个只读 Soak observer 共享
  Catalog:SQLite WAL、30 秒 busy timeout 和 BEGIN IMMEDIATE 负责两个写
  进程的跨进程序列化。DrainLock 另行阻止多个 Archive Drain;不宣称支持
  任意数量的写进程。

Catalog 不包含:Raw 负载字节、价格水平、订单簿状态、market-event 行。
Metrics 仅以聚合 JSON 批次存储(以 batch_id 为键实现幂等重试)。
Side-data cursors 是小型记录,包含 kind、last_persisted_period_timestamp
和保留元数据。
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from enum import StrEnum
from pathlib import Path
from threading import RLock
from typing import Any, cast


class ChunkState(StrEnum):
    ACTIVE = "ACTIVE"
    RECOVERED = "RECOVERED"
    SEALING = "SEALING"
    SEALED = "SEALED"
    ARCHIVE_COPYING = "ARCHIVE_COPYING"
    ARCHIVE_VERIFYING = "ARCHIVE_VERIFYING"
    ARCHIVED_VERIFIED = "ARCHIVED_VERIFIED"
    LOCAL_DELETE_PENDING = "LOCAL_DELETE_PENDING"
    LOCAL_DELETED = "LOCAL_DELETED"
    QUARANTINED = "QUARANTINED"


class ArchiveState(StrEnum):
    COPYING = "COPYING"
    VERIFYING = "VERIFYING"
    VERIFIED = "VERIFIED"
    LOCAL_DELETE_PENDING = "LOCAL_DELETE_PENDING"
    LOCAL_DELETED = "LOCAL_DELETED"


class RemoteArchiveState(StrEnum):
    """Separate remote source lifecycle; physical ChunkState remains SEALED."""

    REMOTE_DELETE_PENDING = "REMOTE_DELETE_PENDING"
    REMOTE_DELETED = "REMOTE_DELETED"


class StorageControlState(StrEnum):
    ACTIVE = "ACTIVE"
    EJECT_PENDING = "EJECT_PENDING"
    SAFE_TO_REMOVE = "SAFE_TO_REMOVE"


class DeploymentState(StrEnum):
    CANDIDATE_STARTING = "CANDIDATE_STARTING"
    CANDIDATE_READY = "CANDIDATE_READY"
    OVERLAP_CONFIRMED = "OVERLAP_CONFIRMED"
    CUTOVER_COMPLETE = "CUTOVER_COMPLETE"
    ROLLED_BACK = "ROLLED_BACK"


class CatalogStateError(RuntimeError):
    """Raised for invalid Catalog lifecycle state or access mode."""


ALLOWED_TRANSITIONS = {
    ChunkState.ACTIVE: {ChunkState.RECOVERED, ChunkState.SEALING, ChunkState.QUARANTINED},
    ChunkState.RECOVERED: {ChunkState.SEALING, ChunkState.QUARANTINED},
    ChunkState.SEALING: {ChunkState.SEALED, ChunkState.QUARANTINED},
    ChunkState.SEALED: {ChunkState.ARCHIVE_COPYING},
    ChunkState.ARCHIVE_COPYING: {ChunkState.ARCHIVE_VERIFYING},
    ChunkState.ARCHIVE_VERIFYING: {ChunkState.ARCHIVED_VERIFIED},
    ChunkState.ARCHIVED_VERIFIED: {ChunkState.LOCAL_DELETE_PENDING},
    ChunkState.LOCAL_DELETE_PENDING: {ChunkState.LOCAL_DELETED},
    ChunkState.LOCAL_DELETED: set(),
    ChunkState.QUARANTINED: set(),
}

ARCHIVE_TRANSITIONS = {
    ArchiveState.COPYING: {ArchiveState.VERIFYING},
    ArchiveState.VERIFYING: {ArchiveState.VERIFIED},
    ArchiveState.VERIFIED: {ArchiveState.LOCAL_DELETE_PENDING},
    ArchiveState.LOCAL_DELETE_PENDING: {ArchiveState.LOCAL_DELETED},
    ArchiveState.LOCAL_DELETED: set(),
}

ARCHIVE_CHUNK_STATES = {
    ArchiveState.COPYING: ChunkState.ARCHIVE_COPYING,
    ArchiveState.VERIFYING: ChunkState.ARCHIVE_VERIFYING,
    ArchiveState.VERIFIED: ChunkState.ARCHIVED_VERIFIED,
    ArchiveState.LOCAL_DELETE_PENDING: ChunkState.LOCAL_DELETE_PENDING,
    ArchiveState.LOCAL_DELETED: ChunkState.LOCAL_DELETED,
}

DEPLOYMENT_TRANSITIONS = {
    DeploymentState.CANDIDATE_STARTING: {
        DeploymentState.CANDIDATE_READY,
        DeploymentState.ROLLED_BACK,
    },
    DeploymentState.CANDIDATE_READY: {
        DeploymentState.OVERLAP_CONFIRMED,
        DeploymentState.ROLLED_BACK,
    },
    DeploymentState.OVERLAP_CONFIRMED: {
        DeploymentState.CUTOVER_COMPLETE,
        DeploymentState.ROLLED_BACK,
    },
    DeploymentState.CUTOVER_COMPLETE: set(),
    DeploymentState.ROLLED_BACK: set(),
}


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


class Catalog:
    def __init__(
        self, path: Path, *, read_only: bool = False, _live_read_only: bool = False
    ) -> None:
        if _live_read_only and not read_only:
            raise CatalogStateError("live read-only mode requires read_only=True")
        self.path = path
        self.read_only = read_only
        self.live_read_only = _live_read_only
        self._lock = RLock()
        if read_only:
            if not self.path.is_file():
                raise CatalogStateError("read-only Catalog does not exist")
            uri = f"{self.path.resolve().as_uri()}?mode=ro"
            # A checkpointed WAL database has no ``-wal`` file. Opening that
            # idle database through ordinary WAL read-only locking would create
            # empty ``-wal``/``-shm`` sidecars. Immutable mode is a true
            # filesystem-read-only snapshot in that case. When a writer's WAL
            # exists, retain normal ``mode=ro`` so the observer sees committed
            # WAL content and participates in SQLite's read locking.
            wal_path = self.path.with_name(f"{self.path.name}-wal")
            if not _live_read_only and not wal_path.exists():
                uri += "&immutable=1"
            try:
                self._connection = sqlite3.connect(
                    uri,
                    timeout=30,
                    isolation_level=None,
                    check_same_thread=False,
                    uri=True,
                )
            except sqlite3.Error as exc:
                raise CatalogStateError("cannot open read-only Catalog") from exc
        else:
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._connection = sqlite3.connect(
                path, timeout=30, isolation_level=None, check_same_thread=False
            )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA busy_timeout=30000")
        self._connection.execute("PRAGMA foreign_keys=ON")
        if read_only:
            self._connection.execute("PRAGMA query_only=ON")
            try:
                self._remote_schema_present = self._inspect_remote_schema(create=False)
            except CatalogStateError:
                raise
            except sqlite3.Error as exc:
                raise CatalogStateError("cannot inspect remote Catalog schema") from exc
        else:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._initialize()
            try:
                self._remote_schema_present = self._inspect_remote_schema(create=True)
            except CatalogStateError:
                raise
            except sqlite3.Error as exc:
                raise CatalogStateError("cannot initialize remote Catalog schema") from exc

    @classmethod
    def open_live_read_only(cls, path: Path) -> Catalog:
        """Open a live WAL-capable source for SQLite Online Backup.

        Unlike the existing static read-only observer, this mode never appends
        ``immutable=1``.  Normal SQLite locking remains active so commits made
        after open can be observed when the backup transaction begins.
        """

        return cls(path, read_only=True, _live_read_only=True)

    def backup_to(self, destination: sqlite3.Connection) -> None:
        """Create one SQLite-consistent committed state in *destination*."""

        if not self.live_read_only:
            raise CatalogStateError("Catalog backup source must be live read-only")
        if not isinstance(destination, sqlite3.Connection):
            raise TypeError("backup destination must be sqlite3.Connection")
        with self._lock:
            self._connection.backup(destination)

    def integrity_check(self) -> tuple[str, ...]:
        """Return the complete SQLite integrity-check result."""

        with self._lock:
            rows = self._connection.execute("PRAGMA integrity_check").fetchall()
        return tuple(str(row[0]) for row in rows)

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        self._require_writable()
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")

    def _require_writable(self) -> None:
        if self.read_only:
            raise CatalogStateError("Catalog is read-only")

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
            CREATE TABLE IF NOT EXISTS operational_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                occurred_at_utc_ns INTEGER NOT NULL,
                evidence_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS side_data_cursors (
                kind TEXT PRIMARY KEY,
                last_persisted_period_timestamp INTEGER NOT NULL,
                updated_at_utc_ns INTEGER NOT NULL,
                source_retention_window TEXT NOT NULL,
                retention_window_ms INTEGER NOT NULL
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
        self._validate_table_columns(
            "remote_archive_transactions", _REMOTE_TRANSACTION_COLUMNS
        )
        self._validate_table_columns("remote_archive_events", _REMOTE_EVENT_COLUMNS)
        self._validate_foreign_key(
            "remote_archive_transactions", "chunk_id", "chunks", "chunk_id"
        )
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
            str(row["name"]): (
                str(row["type"]).upper(), int(row["notnull"]), int(row["pk"])
            )
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
                for item in self._connection.execute(
                    f"PRAGMA index_info({row['name']})"
                ).fetchall()
            )
            if names == columns:
                return
        raise CatalogStateError(f"missing remote Catalog UNIQUE constraint: {table}")

    def _validate_named_index(
        self, name: str, table: str, columns: tuple[str, ...]
    ) -> None:
        row = self._connection.execute(
            "SELECT tbl_name FROM sqlite_master WHERE type = 'index' AND name = ?",
            (name,),
        ).fetchone()
        metadata = next(
            (
                item
                for item in self._connection.execute(
                    f"PRAGMA index_list({table})"
                ).fetchall()
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

    def record_space_sample(
        self,
        *,
        sample_id: str,
        scope_id: str,
        storage_id: str | None,
        observed_at_utc_ns: int,
        total_bytes: int,
        free_bytes: int,
        archive_backlog_bytes: int,
        oldest_unarchived_at_utc_ns: int | None,
        severity: str,
    ) -> bool:
        if (
            not sample_id
            or not scope_id
            or not severity
            or observed_at_utc_ns < 0
            or total_bytes <= 0
            or free_bytes < 0
            or free_bytes > total_bytes
            or archive_backlog_bytes < 0
            or (
                oldest_unarchived_at_utc_ns is not None
                and oldest_unarchived_at_utc_ns < 0
            )
        ):
            raise ValueError("invalid storage space sample")
        with self._transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM storage_space_samples WHERE sample_id = ?",
                (sample_id,),
            ).fetchone():
                return False
            connection.execute(
                """
                INSERT INTO storage_space_samples(
                    sample_id, scope_id, storage_id, observed_at_utc_ns,
                    total_bytes, free_bytes, archive_backlog_bytes,
                    oldest_unarchived_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sample_id,
                    scope_id,
                    storage_id,
                    observed_at_utc_ns,
                    total_bytes,
                    free_bytes,
                    archive_backlog_bytes,
                    oldest_unarchived_at_utc_ns,
                ),
            )
            current = connection.execute(
                "SELECT severity FROM storage_alert_state WHERE scope_id = ?",
                (scope_id,),
            ).fetchone()
            previous = str(current["severity"]) if current is not None else None
            if previous != severity:
                evidence = json.dumps(
                    {
                        "free_bytes": free_bytes,
                        "total_bytes": total_bytes,
                        "sample_id": sample_id,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                connection.execute(
                    """
                    INSERT INTO storage_alert_events(
                        scope_id, from_severity, to_severity,
                        occurred_at_utc_ns, evidence_json, idempotency_key
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scope_id,
                        previous,
                        severity,
                        observed_at_utc_ns,
                        evidence,
                        f"space-severity:{sample_id}",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO storage_alert_state(
                        scope_id, severity, updated_at_utc_ns
                    ) VALUES (?, ?, ?)
                    ON CONFLICT(scope_id) DO UPDATE SET
                        severity = excluded.severity,
                        updated_at_utc_ns = excluded.updated_at_utc_ns
                    """,
                    (scope_id, severity, observed_at_utc_ns),
                )
        return True

    def space_samples(self, scope_id: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM storage_space_samples
                WHERE scope_id = ?
                ORDER BY observed_at_utc_ns, sample_id
                """,
                (scope_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def storage_alert_events(
        self, *, scope_id: str | None = None
    ) -> list[dict[str, object]]:
        query = "SELECT * FROM storage_alert_events"
        parameters: tuple[object, ...] = ()
        if scope_id is not None:
            query += " WHERE scope_id = ?"
            parameters = (scope_id,)
        query += " ORDER BY occurred_at_utc_ns, event_id"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        output: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            document["evidence"] = json.loads(str(evidence_json))
            output.append(document)
        return output

    def record_operational_event(
        self,
        *,
        event_id: str,
        event_type: str,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
    ) -> bool:
        self._require_writable()
        if not event_id or not event_type or occurred_at_utc_ns < 0:
            raise ValueError("invalid operational event")
        body = json.dumps(dict(evidence), sort_keys=True, separators=(",", ":"))
        with self._lock:
            cursor = self._connection.execute(
                """
                INSERT OR IGNORE INTO operational_events(
                    event_id, event_type, occurred_at_utc_ns, evidence_json
                ) VALUES (?, ?, ?, ?)
                """,
                (event_id, event_type, occurred_at_utc_ns, body),
            )
            inserted = cursor.rowcount == 1
            if not inserted:
                existing = self._connection.execute(
                    """
                    SELECT event_type, occurred_at_utc_ns, evidence_json
                    FROM operational_events WHERE event_id = ?
                    """,
                    (event_id,),
                ).fetchone()
                if existing is None or (
                    str(existing["event_type"]) != event_type
                    or int(existing["occurred_at_utc_ns"]) != occurred_at_utc_ns
                    or str(existing["evidence_json"]) != body
                ):
                    raise CatalogStateError(
                        f"operational event identity conflict: {event_id}"
                    )
        return inserted

    def ensure_operational_event(
        self,
        *,
        event_id: str,
        event_type: str,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
    ) -> bool:
        """Insert an event or prove an existing event is byte-semantically exact.

        Reconnect lifecycle callers may treat idempotent replay as success only
        after this check.  The explicit readback also protects recovery from a
        false/ignored insert result supplied by a fault-injection wrapper: no
        materialization action is reported unless the requested event is
        actually durable with identical type, timestamp, and evidence.
        """

        inserted = self.record_operational_event(
            event_id=event_id,
            event_type=event_type,
            occurred_at_utc_ns=occurred_at_utc_ns,
            evidence=evidence,
        )
        if inserted:
            return True
        expected_body = json.dumps(
            dict(evidence), sort_keys=True, separators=(",", ":")
        )
        with self._lock:
            existing = self._connection.execute(
                """
                SELECT event_type, occurred_at_utc_ns, evidence_json
                FROM operational_events WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()
        if existing is None or (
            str(existing["event_type"]) != event_type
            or int(existing["occurred_at_utc_ns"]) != occurred_at_utc_ns
            or str(existing["evidence_json"]) != expected_body
        ):
            raise CatalogStateError(
                f"operational event was not durably validated: {event_id}"
            )
        return False

    def operational_events(
        self, *, event_type: str | None = None
    ) -> list[dict[str, object]]:
        query = "SELECT * FROM operational_events"
        parameters: tuple[object, ...] = ()
        if event_type is not None:
            query += " WHERE event_type = ?"
            parameters = (event_type,)
        query += " ORDER BY occurred_at_utc_ns, event_id"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        output: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            document["evidence"] = json.loads(str(evidence_json))
            output.append(document)
        return output

    def unclosed_stream_discontinuities(
        self, *, market: str, stream: str
    ) -> list[dict[str, object]]:
        """Return STARTED evidence without a matching COMPLETED gap identifier."""

        if not market or not stream:
            raise ValueError("market and stream must be non-empty")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY occurred_at_utc_ns, event_id
                """
            ).fetchall()
        started: list[dict[str, object]] = []
        completed_gap_ids: set[str] = set()
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            evidence = json.loads(str(evidence_json))
            if not isinstance(evidence, dict):
                continue
            if evidence.get("market") != market or evidence.get("stream") != stream:
                continue
            document["evidence"] = evidence
            gap_id = evidence.get("gap_id")
            if document["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                if isinstance(gap_id, str) and gap_id:
                    completed_gap_ids.add(gap_id)
            else:
                started.append(document)
        return [
            event
            for event in started
            if not (
                isinstance(event["evidence"], dict)
                and isinstance(event["evidence"].get("gap_id"), str)
                and event["evidence"].get("gap_id") in completed_gap_ids
            )
        ]

    def closed_stream_discontinuity_intervals_by_stream(
        self,
    ) -> dict[tuple[str, str], list[dict[str, object]]]:
        """Return paired (STARTED, COMPLETED) intervals grouped by (market, stream).

        Each interval is exactly one logical reconnect discontinuity whose
        lifecycle is CLOSED, paired strictly by exact ``gap_id``.  The
        interval exposes the full durable identity evidence: the wall-clock
        timestamps (observational only, never causal-ordering authority),
        the STARTED-side original connection/generation, and the
        COMPLETED-side replacement connection/generation
        (M21.4.11-R3.2).  A pair whose COMPLETED wall timestamp is not
        after its STARTED wall timestamp is still returned, flagged
        ``wall_time_order == NON_MONOTONIC``: inverted wall clocks never
        remove exact lifecycle authority from the classification universe.
        Intervals lacking a well-formed identity field are omitted here and
        surfaced by ``degraded_closed_discontinuity_pairs`` instead:
        classification must never guess from partial evidence.
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_type, occurred_at_utc_ns, evidence_json
                FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY occurred_at_utc_ns, event_id
                """
            ).fetchall()
        started_by_gap: dict[
            tuple[str, str, str], dict[str, object]
        ] = {}
        completed_by_gap: dict[
            tuple[str, str, str], dict[str, object]
        ] = {}
        for row in rows:
            evidence = json.loads(str(row["evidence_json"]))
            if not isinstance(evidence, dict):
                continue
            market = evidence.get("market")
            stream = evidence.get("stream")
            gap_id = evidence.get("gap_id")
            if (
                not isinstance(market, str)
                or not market
                or not isinstance(stream, str)
                or not stream
                or not isinstance(gap_id, str)
                or not gap_id
            ):
                continue
            key = (market, stream, gap_id)
            document = {
                "occurred_at_utc_ns": int(row["occurred_at_utc_ns"]),
                "evidence": evidence,
            }
            if str(row["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                completed_by_gap[key] = document
            else:
                started_by_gap[key] = document
        grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
        for (market, stream, gap_id), started in sorted(started_by_gap.items()):
            completed = completed_by_gap.get((market, stream, gap_id))
            if completed is None:
                continue
            started_evidence = started["evidence"]
            completed_evidence = completed["evidence"]
            if not isinstance(started_evidence, dict) or not isinstance(
                completed_evidence, dict
            ):
                continue
            if not _closed_lifecycle_identity_valid(
                started_evidence, completed_evidence
            ):
                continue
            started_at = cast(int, started_evidence["gap_started_at_utc_ns"])
            ended_at = cast(int, completed_evidence["gap_ended_at_utc_ns"])
            original_connection = started_evidence["original_connection_id"]
            original_generation = started_evidence["original_generation"]
            new_connection = completed_evidence["new_connection_id"]
            new_generation = completed_evidence["new_generation"]
            grouped.setdefault((market, stream), []).append(
                {
                    "gap_id": gap_id,
                    "started_at_utc_ns": started_at,
                    "ended_at_utc_ns": ended_at,
                    "wall_time_order": (
                        "NORMAL" if ended_at > started_at else "NON_MONOTONIC"
                    ),
                    "original_connection_id": original_connection,
                    "original_generation": original_generation,
                    "new_connection_id": new_connection,
                    "new_generation": new_generation,
                }
            )
        return grouped

    def degraded_closed_discontinuity_pairs(
        self,
    ) -> list[dict[str, object]]:
        """Return exactly-paired CLOSED lifecycles with malformed identity.

        A STARTED/COMPLETED pair keyed by the same ``gap_id`` whose identity
        fields (connections, generations, timestamps) are not well-formed
        cannot serve as positive-proof authority.  R3.3 surfaces each such
        pair as an explicit degraded-authority predecision blocker: malformed
        evidence must widen uncertainty and can never reduce the possible
        historical universe (M21.4.11-R3.3).
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_type, evidence_json
                FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                """
            ).fetchall()
        started_by_gap: dict[
            tuple[str, str, str], dict[str, object]
        ] = {}
        completed_by_gap: dict[
            tuple[str, str, str], dict[str, object]
        ] = {}
        for row in rows:
            evidence = json.loads(str(row["evidence_json"]))
            if not isinstance(evidence, dict):
                continue
            market = evidence.get("market")
            stream = evidence.get("stream")
            gap_id = evidence.get("gap_id")
            if (
                not isinstance(market, str)
                or not market
                or not isinstance(stream, str)
                or not stream
                or not isinstance(gap_id, str)
                or not gap_id
            ):
                continue
            key = (market, stream, gap_id)
            if str(row["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                completed_by_gap[key] = evidence
            else:
                started_by_gap[key] = evidence
        degraded: list[dict[str, object]] = []
        for key in sorted(set(started_by_gap) & set(completed_by_gap)):
            started_evidence = started_by_gap[key]
            completed_evidence = completed_by_gap[key]
            if _closed_lifecycle_identity_valid(
                started_evidence, completed_evidence
            ):
                continue
            market, stream, gap_id = key
            degraded.append(
                {
                    "market": market,
                    "stream": stream,
                    "gap_id": gap_id,
                    "reason": "malformed_lifecycle_identity",
                }
            )
        return degraded

    def malformed_discontinuity_events(self) -> list[dict[str, object]]:
        """Inventory every STARTED/COMPLETED row that cannot be keyed safely.

        A lifecycle event whose evidence is not a JSON object, or whose
        ``market``/``stream``/``gap_id`` identity is missing or not text,
        cannot participate in exact gap_id pairing.  R3.3 surfaces each such
        row as an explicit degraded-authority predecision blocker instead of
        silently skipping it: malformed evidence must widen uncertainty and
        can never shrink the searched parent universe
        (M21.4.11-R3.3 REV-001).
        """
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_id, event_type, evidence_json
                FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY event_id
                """
            ).fetchall()
        output: list[dict[str, object]] = []
        for row in rows:
            event_id = str(row["event_id"])
            event_type = str(row["event_type"])
            reason: str | None = None
            try:
                evidence = json.loads(str(row["evidence_json"]))
            except (json.JSONDecodeError, TypeError):
                reason = "evidence_not_json"
            if reason is None and not isinstance(evidence, dict):
                reason = "evidence_not_object"
            if reason is None:
                market = evidence.get("market")
                stream = evidence.get("stream")
                gap_id = evidence.get("gap_id")
                if not isinstance(market, str) or not market:
                    reason = "missing_market"
                elif not isinstance(stream, str) or not stream:
                    reason = "missing_stream"
                elif not isinstance(gap_id, str) or not gap_id:
                    reason = "missing_gap_id"
            if reason is None:
                continue
            output.append(
                {
                    "event_id": event_id,
                    "event_type": event_type,
                    "reason": reason,
                }
            )
        return output

    def unclosed_stream_discontinuities_by_stream(
        self,
    ) -> dict[tuple[str, str], list[dict[str, object]]]:
        """Return STARTED events without a matching COMPLETED, grouped by
        (market, stream), in one pass (M21.4.11-R3.2)."""

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                ORDER BY occurred_at_utc_ns, event_id
                """
            ).fetchall()
        started: list[dict[str, object]] = []
        completed_gap_ids: set[tuple[str, str, str]] = set()
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            evidence = json.loads(str(evidence_json))
            if not isinstance(evidence, dict):
                continue
            market = evidence.get("market")
            stream = evidence.get("stream")
            gap_id = evidence.get("gap_id")
            if (
                not isinstance(market, str)
                or not market
                or not isinstance(stream, str)
                or not stream
                or not isinstance(gap_id, str)
                or not gap_id
            ):
                continue
            document["evidence"] = evidence
            if document["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                completed_gap_ids.add((market, stream, gap_id))
            else:
                started.append(document)
        grouped: dict[tuple[str, str], list[dict[str, object]]] = {}
        for event in started:
            evidence = event.get("evidence")
            if not isinstance(evidence, dict):
                continue
            market = str(evidence.get("market"))
            stream = str(evidence.get("stream"))
            gap_id = str(evidence.get("gap_id"))
            if (market, stream, gap_id) in completed_gap_ids:
                continue
            grouped.setdefault((market, stream), []).append(event)
        return grouped

    def stream_discontinuity_lifecycle(
        self, *, market: str, stream: str, gap_id: str
    ) -> str:
        """Return the durable lifecycle of exactly one gap identifier.

        One of:

        - ``ABSENT``: no STARTED and no COMPLETED record for the gap.
        - ``OPEN``: a STARTED record exists without a matching COMPLETED.
        - ``CLOSED``: a COMPLETED record exists for the gap.

        Startup recovery keys every seal-intent decision by this lifecycle
        (M21.4.11-R2.1/REQ-100): a CLOSED gap is historical and never
        conflicts with a later OPEN gap, while an OPEN gap must agree
        exactly with its seal intent.
        """
        if not market or not stream or not gap_id:
            raise ValueError("market, stream and gap_id must be non-empty")
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_type, evidence_json FROM operational_events
                WHERE event_type IN (
                    'STREAM_DISCONTINUITY_STARTED',
                    'STREAM_DISCONTINUITY_COMPLETED'
                )
                """
            ).fetchall()
        started = False
        completed = False
        for row in rows:
            evidence = json.loads(str(row["evidence_json"]))
            if not isinstance(evidence, dict):
                continue
            if evidence.get("market") != market or evidence.get("stream") != stream:
                continue
            if evidence.get("gap_id") != gap_id:
                continue
            if str(row["event_type"]) == "STREAM_DISCONTINUITY_COMPLETED":
                completed = True
            else:
                started = True
        if completed:
            return "CLOSED"
        if started:
            return "OPEN"
        return "ABSENT"

    def side_data_cursor(self, kind: str) -> dict[str, object] | None:
        if not kind:
            raise ValueError("side-data cursor kind must be non-empty")
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM side_data_cursors WHERE kind = ?", (kind,)
            ).fetchone()
        return dict(row) if row is not None else None

    def advance_side_data_cursor(
        self,
        *,
        kind: str,
        last_persisted_period_timestamp: int,
        updated_at_utc_ns: int,
        source_retention_window: str,
        retention_window_ms: int,
    ) -> bool:
        if (
            not kind
            or last_persisted_period_timestamp < 0
            or updated_at_utc_ns < 0
            or not source_retention_window
            or retention_window_ms <= 0
        ):
            raise ValueError("invalid side-data cursor")
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM side_data_cursors WHERE kind = ?", (kind,)
            ).fetchone()
            if existing is not None:
                current = int(existing["last_persisted_period_timestamp"])
                if last_persisted_period_timestamp < current:
                    raise CatalogStateError("side-data cursor cannot move backward")
                if last_persisted_period_timestamp == current:
                    return False
            connection.execute(
                """
                INSERT INTO side_data_cursors(
                    kind, last_persisted_period_timestamp, updated_at_utc_ns,
                    source_retention_window, retention_window_ms
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(kind) DO UPDATE SET
                    last_persisted_period_timestamp =
                        excluded.last_persisted_period_timestamp,
                    updated_at_utc_ns = excluded.updated_at_utc_ns,
                    source_retention_window = excluded.source_retention_window,
                    retention_window_ms = excluded.retention_window_ms
                """,
                (
                    kind,
                    last_persisted_period_timestamp,
                    updated_at_utc_ns,
                    source_retention_window,
                    retention_window_ms,
                ),
            )
        return True

    def create_deployment(
        self,
        *,
        deployment_id: str,
        reason: str,
        market: str,
        symbol: str,
        active_instance_id: str,
        active_version: str,
        candidate_instance_id: str,
        candidate_version: str,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
    ) -> None:
        identity = (
            deployment_id,
            reason,
            market,
            symbol,
            active_instance_id,
            active_version,
            candidate_instance_id,
            candidate_version,
        )
        if not all(identity) or occurred_at_utc_ns < 0:
            raise ValueError("invalid deployment identity")
        evidence_json = json.dumps(
            dict(evidence), sort_keys=True, separators=(",", ":")
        )
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM deployment_sessions WHERE deployment_id = ?",
                (deployment_id,),
            ).fetchone()
            if existing is not None:
                expected = {
                    "reason": reason,
                    "market": market,
                    "symbol": symbol,
                    "active_instance_id": active_instance_id,
                    "active_version": active_version,
                    "candidate_instance_id": candidate_instance_id,
                    "candidate_version": candidate_version,
                }
                if any(existing[key] != value for key, value in expected.items()):
                    raise CatalogStateError("deployment identity changed")
                return
            connection.execute(
                """
                INSERT INTO deployment_sessions(
                    deployment_id, reason, market, symbol,
                    active_instance_id, active_version,
                    candidate_instance_id, candidate_version,
                    state, created_at_utc_ns, updated_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    *identity,
                    DeploymentState.CANDIDATE_STARTING,
                    occurred_at_utc_ns,
                    occurred_at_utc_ns,
                ),
            )
            connection.execute(
                """
                INSERT INTO deployment_events(
                    deployment_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, NULL, ?, ?, ?, ?)
                """,
                (
                    deployment_id,
                    DeploymentState.CANDIDATE_STARTING,
                    occurred_at_utc_ns,
                    evidence_json,
                    f"deployment-start:{deployment_id}",
                ),
            )

    def transition_deployment(
        self,
        deployment_id: str,
        to_state: DeploymentState,
        *,
        idempotency_key: str,
        evidence: Mapping[str, object],
        occurred_at_utc_ns: int,
        error: str | None = None,
    ) -> None:
        if not deployment_id or not idempotency_key or occurred_at_utc_ns < 0:
            raise ValueError("invalid deployment transition")
        evidence_json = json.dumps(
            dict(evidence), sort_keys=True, separators=(",", ":")
        )
        with self._transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM deployment_events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone():
                return
            row = connection.execute(
                "SELECT state FROM deployment_sessions WHERE deployment_id = ?",
                (deployment_id,),
            ).fetchone()
            if row is None:
                raise CatalogStateError(f"unknown deployment {deployment_id}")
            from_state = DeploymentState(row["state"])
            if to_state not in DEPLOYMENT_TRANSITIONS[from_state]:
                raise CatalogStateError(
                    f"invalid deployment transition {from_state} -> {to_state}"
                )
            overlap_started = (
                occurred_at_utc_ns
                if to_state is DeploymentState.OVERLAP_CONFIRMED
                else None
            )
            cutover = (
                occurred_at_utc_ns
                if to_state is DeploymentState.CUTOVER_COMPLETE
                else None
            )
            connection.execute(
                """
                UPDATE deployment_sessions
                SET state = ?, updated_at_utc_ns = ?,
                    overlap_started_at_utc_ns =
                        COALESCE(?, overlap_started_at_utc_ns),
                    cutover_at_utc_ns = COALESCE(?, cutover_at_utc_ns),
                    last_error = ?
                WHERE deployment_id = ?
                """,
                (
                    to_state,
                    occurred_at_utc_ns,
                    overlap_started,
                    cutover,
                    error,
                    deployment_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO deployment_events(
                    deployment_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    deployment_id,
                    from_state,
                    to_state,
                    occurred_at_utc_ns,
                    evidence_json,
                    idempotency_key,
                ),
            )

    def deployment(self, deployment_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM deployment_sessions WHERE deployment_id = ?",
                (deployment_id,),
            ).fetchone()
        return dict(row) if row else None

    def deployments(self) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM deployment_sessions
                ORDER BY created_at_utc_ns, deployment_id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def deployment_events(self, deployment_id: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM deployment_events
                WHERE deployment_id = ?
                ORDER BY event_id
                """,
                (deployment_id,),
            ).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            document["evidence"] = json.loads(
                str(document.pop("evidence_json"))
            )
            result.append(document)
        return result

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
                and StorageControlState(control["state"])
                is not StorageControlState.ACTIVE
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
                    raise CatalogStateError(
                        "chunk already has a different archive transaction"
                    )
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
        evidence_json = json.dumps(
            dict(evidence or {}), sort_keys=True, separators=(",", ":")
        )
        with self._transaction() as connection:
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
                raise CatalogStateError(
                    f"invalid archive transition {from_state} -> {to_state}"
                )
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
        self, *, storage_id: str | None = None
    ) -> list[dict[str, object]]:
        query = "SELECT * FROM archive_transactions"
        parameters: tuple[object, ...] = ()
        if storage_id is not None:
            query += " WHERE storage_id = ?"
            parameters = (storage_id,)
        query += " ORDER BY created_at_utc_ns, transaction_id"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        return [dict(row) for row in rows]

    def remote_archive_transaction(
        self, receipt_id: str
    ) -> dict[str, object] | None:
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
                        "SELECT * FROM remote_archive_transactions "
                        "WHERE chunk_id = ?",
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

    def remote_archive_transaction_for_chunk(
        self, chunk_id: str
    ) -> dict[str, object] | None:
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
                "SELECT * FROM remote_archive_transactions "
                "ORDER BY created_at_utc_ns, receipt_id"
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

    def remote_archive_events(self, receipt_id: str) -> list[dict[str, object]]:
        if not self._remote_schema_present:
            return []
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM remote_archive_events WHERE receipt_id = ? "
                "ORDER BY event_id",
                (receipt_id,),
            ).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            try:
                document["to_state"] = RemoteArchiveState(
                    str(document["to_state"])
                ).value
                from_state = document["from_state"]
                if from_state is not None:
                    document["from_state"] = RemoteArchiveState(
                        str(from_state)
                    ).value
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
        with self._transaction() as connection:
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
                "SELECT * FROM remote_archive_transactions "
                "WHERE receipt_id = ? OR chunk_id = ?",
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
                    raise CatalogStateError(
                        "remote deletion chunk/source authority changed"
                    )
                same_host = self._connection.execute(
                    "SELECT transaction_id FROM archive_transactions WHERE chunk_id = ?",
                    (expected_chunk_id,),
                ).fetchone()
                if same_host is not None:
                    raise CatalogStateError(
                        "same-host and remote deletion ownership overlap"
                    )
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
                transaction_started = False
                if fault_hook is not None:
                    fault_hook("after_remote_deleted_commit")
            except BaseException:
                if transaction_started and self._connection.in_transaction:
                    with suppress(sqlite3.Error):
                        self._connection.execute("ROLLBACK")
                raise
        persisted = self.remote_archive_transaction(receipt_id)
        if persisted is None:
            raise CatalogStateError("remote deletion terminal readback failed")
        return persisted

    def _validated_remote_row(self, row: sqlite3.Row) -> dict[str, object]:
        document = dict(row)
        try:
            state = RemoteArchiveState(str(document["state"]))
            deleted_at = document["remote_deleted_at_utc_ns"]
            if (state is RemoteArchiveState.REMOTE_DELETE_PENDING) != (
                deleted_at is None
            ):
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
                "source_descriptor_schema_version": (
                    receipt.source_descriptor_schema_version
                ),
                "source_descriptor_sha256": receipt.source_descriptor_sha256,
                "source_relative_path": receipt.source_relative_path,
                "source_manifest_relative_path": (
                    receipt.source_manifest_relative_path
                ),
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
            if chunk is None or any(
                chunk[key] != value for key, value in current_source.items()
            ):
                raise ValueError("remote row/current Catalog source mismatch")
            events = self._connection.execute(
                "SELECT * FROM remote_archive_events WHERE receipt_id = ? "
                "ORDER BY event_id",
                (receipt.receipt_id,),
            ).fetchall()
            initial_by_key = self._connection.execute(
                "SELECT * FROM remote_archive_events WHERE idempotency_key = ?",
                (f"remote-authorize:{receipt.receipt_id}",),
            ).fetchone()
            if not events or initial_by_key is None or events[0]["event_id"] != (
                initial_by_key["event_id"]
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
                or initial["to_state"]
                != RemoteArchiveState.REMOTE_DELETE_PENDING.value
                or initial["occurred_at_utc_ns"] != document["created_at_utc_ns"]
                or initial["idempotency_key"]
                != f"remote-authorize:{receipt.receipt_id}"
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
                    "SELECT event_id FROM remote_archive_events "
                    "WHERE idempotency_key = ?",
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
                    or terminal["from_state"]
                    != RemoteArchiveState.REMOTE_DELETE_PENDING.value
                    or terminal["to_state"]
                    != RemoteArchiveState.REMOTE_DELETED.value
                    or terminal["occurred_at_utc_ns"] != deleted_at
                    or terminal["idempotency_key"]
                    != f"remote-deleted:{receipt.receipt_id}"
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
        state = (
            StorageControlState.SAFE_TO_REMOVE
            if succeeded
            else StorageControlState.ACTIVE
        )
        evidence_json = json.dumps(
            dict(evidence), sort_keys=True, separators=(",", ":")
        )
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

        evidence_json = json.dumps(
            dict(evidence), sort_keys=True, separators=(",", ":")
        )
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

    def activate_storage_target(
        self, storage_id: str, *, occurred_at_utc_ns: int
    ) -> None:
        """Clear a completed eject latch only after UUID/marker readiness is proven."""

        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state FROM storage_control WHERE storage_id = ?",
                (storage_id,),
            ).fetchone()
            if (
                row is None
                or StorageControlState(row["state"]) is StorageControlState.ACTIVE
            ):
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

    def checkpoint(self) -> None:
        """Durably checkpoint Catalog WAL before releasing an external volume."""

        self._require_writable()
        try:
            with self._lock:
                self._connection.execute("PRAGMA wal_checkpoint(FULL)").fetchone()
        except sqlite3.Error as exc:
            raise CatalogStateError(f"Catalog checkpoint failed: {exc}") from exc
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

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

    def _validate_remote_rows_in_snapshot(self) -> list[dict[str, object]]:
        """Validate remote rows before a read snapshot uses their presence."""

        rows = self._connection.execute(
            "SELECT * FROM remote_archive_transactions "
            "ORDER BY created_at_utc_ns, receipt_id"
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
                    self._validate_remote_rows_in_snapshot()
                    if self._remote_schema_present
                    else []
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
            remote_pending_bytes = sum(
                cast(int, row["stored_bytes"]) for row in remote_pending
            )
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

    def oldest_incomplete_archive_transaction(
        self, storage_id: str
    ) -> dict[str, object] | None:
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

    def archive_aggregate(
        self, storage_id: str
    ) -> dict[str, object]:
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

        txn_states = {
            str(row["state"]): int(row["cnt"])
            for row in state_counts
        }
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
                raise CatalogStateError(
                    "volume directory is registered with another storage_id"
                )
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
            connection.execute(
                "DELETE FROM storage_control WHERE storage_id = ?", (storage_id,)
            )
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
                raise CatalogStateError(
                    f"boundary marker identity conflict: {chunk_id}"
                )
            transition = connection.execute(
                """
                SELECT evidence_json FROM chunk_transitions
                WHERE idempotency_key = ?
                """,
                (f"sealing:{chunk_id}",),
            ).fetchone()
            if transition is None or str(transition["evidence_json"]) != sealing_evidence:
                raise CatalogStateError(
                    f"boundary marker seal intent conflict: {chunk_id}"
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

    def chunk(self, chunk_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM chunks WHERE chunk_id = ?", (chunk_id,)
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
            "source_manifest_relative_path": (
                "archive_source_manifest_relative_path"
            ),
            "source_manifest_sha256": "archive_source_manifest_sha256",
            "stored_bytes": "archive_stored_bytes",
            "stored_sha256": "archive_stored_sha256",
        }
        archive = {
            name: combined.pop(alias)
            for name, alias in archive_aliases.items()
        }
        if archive["transaction_id"] is None:
            return combined, None
        return combined, archive

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

    def table_columns(self, table: str) -> set[str]:
        if table not in {
            "chunks",
            "chunk_transitions",
            "quarantined_artifacts",
            "orderbook_checkpoints",
            "metric_batches",
            "storage_targets",
            "archive_transactions",
            "archive_transaction_events",
            "storage_space_samples",
            "storage_alert_state",
            "storage_alert_events",
            "operational_events",
            "remote_archive_transactions",
            "remote_archive_events",
        }:
            raise ValueError("unknown Catalog table")
        with self._lock:
            return {
                str(row["name"])
                for row in self._connection.execute(f"PRAGMA table_info({table})")
            }


def _closed_lifecycle_identity_valid(
    started_evidence: dict[str, object],
    completed_evidence: dict[str, object],
) -> bool:
    """True when a paired CLOSED lifecycle has well-formed identity fields.

    Wall timestamps are NOT part of identity validity: an inverted or equal
    wall pair is still exact lifecycle authority (M21.4.11-R3.2).
    """
    started_at = started_evidence.get("gap_started_at_utc_ns")
    ended_at = completed_evidence.get("gap_ended_at_utc_ns")
    original_connection = started_evidence.get("original_connection_id")
    original_generation = started_evidence.get("original_generation")
    new_connection = completed_evidence.get("new_connection_id")
    new_generation = completed_evidence.get("new_generation")
    return (
        isinstance(started_at, int)
        and not isinstance(started_at, bool)
        and isinstance(ended_at, int)
        and not isinstance(ended_at, bool)
        and isinstance(original_connection, str)
        and bool(original_connection)
        and not isinstance(original_generation, bool)
        and isinstance(original_generation, int)
        and original_generation >= 0
        and isinstance(new_connection, str)
        and bool(new_connection)
        and not isinstance(new_generation, bool)
        and isinstance(new_generation, int)
        and new_generation >= 0
    )


def _validate_relative_path(value: str) -> None:
    path = Path(value)
    if (
        not value
        or path.is_absolute()
        or value in {".", ".."}
        or ".." in path.parts
    ):
        raise ValueError(f"unsafe relative path: {value!r}")


def _is_lower_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _parse_remote_receipt(body: bytes) -> Any:
    """Use the single M22.3 parser without an import cycle at module load."""

    from ..archive.remote_receive import RemoteArchiveReceipt, RemoteReceiveError

    try:
        return RemoteArchiveReceipt.from_bytes(body)
    except RemoteReceiveError as exc:
        raise CatalogStateError("invalid persisted remote archive receipt") from exc
