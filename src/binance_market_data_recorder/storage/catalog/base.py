"""Catalog base connection lifecycle, locking, and transaction primitives."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Self

from .types import _MS1_IDENTITY_FAULT_PHASES, CatalogStateError

if TYPE_CHECKING:
    pass


class CatalogBase:
    """Core SQLite connection management, locking, and atomic transactions."""

    path: Path
    read_only: bool
    live_read_only: bool
    _lock: RLock
    _migration_fault_phase: str | None
    _legacy_identity_schema: bool
    _remote_schema_present: bool
    _connection: sqlite3.Connection

    def __init__(
        self,
        path: Path,
        *,
        read_only: bool = False,
        _live_read_only: bool = False,
        _migration_fault_phase: str | None = None,
    ) -> None:
        if _live_read_only and not read_only:
            raise CatalogStateError("live read-only mode requires read_only=True")
        if _migration_fault_phase not in {None, *_MS1_IDENTITY_FAULT_PHASES}:
            raise ValueError("unknown MS1 identity fault phase")
        if read_only and _migration_fault_phase is not None:
            raise ValueError("cannot inject migration failure in read-only mode")
        self.path = path
        self.read_only = read_only
        self.live_read_only = _live_read_only
        self._legacy_identity_schema = False
        self._migration_fault_phase = _migration_fault_phase
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
                self._validate_symbol_identity_schema(allow_legacy=True)
                self._remote_schema_present = self._inspect_remote_schema(create=False)
            except CatalogStateError:
                raise
            except sqlite3.Error as exc:
                raise CatalogStateError("cannot inspect remote Catalog schema") from exc
        else:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=NORMAL")
            identity_decision = self._identity_schema_decision()
            if identity_decision == "fresh":
                self._initialize_fresh_symbol_identity_schema()
            elif identity_decision in {"legacy_pre_m19", "legacy_c421"}:
                self._migrate_symbol_identity_schema(identity_decision)
            elif identity_decision == "current":
                self._validate_symbol_identity_schema(allow_legacy=False)
            self._initialize()
            self._validate_symbol_identity_schema(allow_legacy=False)
            try:
                self._remote_schema_present = self._inspect_remote_schema(create=True)
            except CatalogStateError:
                raise
            except sqlite3.Error as exc:
                raise CatalogStateError("cannot initialize remote Catalog schema") from exc
        from ..acceptance_delta import DeltaAuthorityError, inspect_sequence

        try:
            inspect_sequence(self._connection)
        except (DeltaAuthorityError, sqlite3.Error) as exc:
            self._connection.close()
            raise CatalogStateError("invalid acceptance delta schema") from exc

    @classmethod
    def open_live_read_only(cls: type[Self], path: Path) -> Self:
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

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @contextmanager
    def _transaction(self, *, force_sync: bool = False) -> Iterator[sqlite3.Connection]:
        self._require_writable()
        with self._lock:
            if force_sync:
                self._connection.execute("PRAGMA synchronous = FULL")
            try:
                self._connection.execute("BEGIN IMMEDIATE")
                try:
                    yield self._connection
                except Exception:
                    self._connection.execute("ROLLBACK")
                    raise
                else:
                    self._connection.execute("COMMIT")
                    if force_sync:
                        self._connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
            finally:
                if force_sync:
                    self._connection.execute("PRAGMA synchronous = NORMAL")

    def _require_writable(self) -> None:
        if self.read_only:
            raise CatalogStateError("Catalog is read-only")

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

    def table_columns(self, table: str) -> set[str]:
        if table not in {
            "raw_verification_results",
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
            "side_data_cursors",
            "deployment_sessions",
            "deployment_events",
            "remote_archive_transactions",
            "remote_archive_events",
        }:
            raise ValueError("unknown Catalog table")
        with self._lock:
            return {
                str(row["name"]) for row in self._connection.execute(f"PRAGMA table_info({table})")
            }

    # --- Stubs implemented by mixins ---
    def _initialize(self) -> None:
        raise NotImplementedError

    def _inspect_remote_schema(self, *, create: bool) -> bool:
        raise NotImplementedError

    def _validate_symbol_identity_schema(self, *, allow_legacy: bool) -> None:
        raise NotImplementedError

    def _identity_schema_decision(self) -> str:
        raise NotImplementedError

    def _initialize_fresh_symbol_identity_schema(self) -> None:
        raise NotImplementedError

    def _migrate_symbol_identity_schema(self, expected_decision: str) -> None:
        raise NotImplementedError

    def _validate_remote_rows_in_snapshot(self) -> list[dict[str, object]]:
        raise NotImplementedError
