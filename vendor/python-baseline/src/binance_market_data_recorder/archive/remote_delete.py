"""Exact Raw-leaf remote deletion with anchored parent durability.

The destructive key is only ``receipt_id``. Authority is reloaded from the
existing Catalog, the retained source manifest is kept immutable, and exactly
one direct child of ``layout.sealed`` can be unlinked. The held Raw descriptor
and anchored parent descriptor remain open across namespace mutation and
parent-directory fsync.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import zstandard

from ..spool.seal import MANIFEST_SCHEMA_VERSION
from ..storage.catalog import (
    Catalog,
    CatalogStateError,
    ChunkState,
    RemoteArchiveState,
)
from ..storage.layout import StorageLayout
from .remote_authorization import (
    RemoteAuthorizationError,
    RemoteRecoveryCase,
    _require_remote_descriptor_binding,
)
from .remote_receive import RemoteArchiveReceipt, RemoteReceiveError
from .remote_source import (
    RemoteSourceDescriptor,
    RemoteSourceError,
    descriptor_from_retained_manifest,
)

BUFFER_BYTES = 1024 * 1024
FaultHook = Callable[[str], None]


class RemoteDeletionError(RuntimeError):
    """Exact durable remote deletion cannot be proven safe."""


@dataclass(frozen=True, slots=True)
class RemoteDeleteResult:
    receipt_id: str
    chunk_id: str
    state: RemoteArchiveState
    recovery_case: RemoteRecoveryCase
    source_deleted: bool
    manifest_retained: bool


@dataclass(frozen=True, slots=True)
class _Authority:
    row: dict[str, object]
    chunk: dict[str, object]
    receipt: RemoteArchiveReceipt
    descriptor: RemoteSourceDescriptor
    manifest_bytes: bytes
    manifest_path: Path
    source_path: Path
    source_leaf: str


class RemoteDeleter:
    """Delete or reconcile one exact persisted remote receipt authority."""

    def __init__(
        self,
        *,
        layout: StorageLayout,
        catalog: Catalog,
        fault_hook: FaultHook | None = None,
        utc_clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.layout = layout
        self.catalog = catalog
        self.fault_hook = fault_hook
        self.utc_clock_ns = utc_clock_ns

    def delete_authorized(self, receipt_id: str) -> RemoteDeleteResult:
        """Explicitly execute CASE A, CASE B, or terminal idempotency."""

        self._hit("k0_entry_before_destructive_validation")
        try:
            authority = self._load_authority(receipt_id)
            _require_supported_platform()
            parent_fd = self._open_anchored_sealed_parent()
            try:
                if authority.row["state"] == RemoteArchiveState.REMOTE_DELETED.value:
                    self._require_absent(parent_fd, authority.source_leaf)
                    return self._terminal_result(authority)

                raw_fd = self._open_raw_if_present(parent_fd, authority.source_leaf)
                if raw_fd is None:
                    return self._reconcile_absent(
                        authority=authority,
                        parent_fd=parent_fd,
                    )
                try:
                    # This non-destructive capability probe occurs before the
                    # only unlink. The same anchored parent fd is retained.
                    os.fsync(parent_fd)
                    held_status = self._validate_held_raw(raw_fd, authority)
                    current = self._require_unchanged_pending(authority)
                    self._require_parent_path_identity(parent_fd)
                    leaf_status = self._required_leaf_status(
                        parent_fd, authority.source_leaf
                    )
                    if _stable_identity(leaf_status) != _stable_identity(held_status):
                        raise RemoteDeletionError(
                            "Raw leaf no longer identifies the held validated file"
                        )
                    self._hit("k1_after_validation_before_unlink")
                    try:
                        os.unlink(authority.source_leaf, dir_fd=parent_fd)
                    except FileNotFoundError:
                        # A concurrent exact caller may have removed this same
                        # authorized leaf. Never select or try another path.
                        return self._reconcile_absent(
                            authority=current,
                            parent_fd=parent_fd,
                        )
                    except OSError as exc:
                        observed = self._leaf_status(parent_fd, authority.source_leaf)
                        detail = (
                            "exact Raw remains present"
                            if observed is not None
                            else "Raw outcome is uncertain"
                        )
                        raise RemoteDeletionError(
                            f"exact Raw unlink failed ({detail}): {exc}"
                        ) from exc
                    self._hit("k2_after_unlink_before_parent_fsync")
                    self._require_absent(parent_fd, authority.source_leaf)
                    os.fsync(parent_fd)
                    self._require_absent(parent_fd, authority.source_leaf)
                    self._hit("k3_after_parent_fsync_before_terminal")
                    return self._terminalize(
                        authority=authority,
                        parent_fd=parent_fd,
                        recovery_case=RemoteRecoveryCase.CASE_A,
                        source_deleted=True,
                    )
                finally:
                    os.close(raw_fd)
            finally:
                os.close(parent_fd)
        except RemoteDeletionError:
            raise
        except (
            CatalogStateError,
            OSError,
            RemoteAuthorizationError,
            RemoteReceiveError,
            RemoteSourceError,
            ValueError,
            zstandard.ZstdError,
        ) as exc:
            raise RemoteDeletionError(f"remote deletion failed closed: {exc}") from exc

    def reconcile_absent_authorized(self, receipt_id: str) -> RemoteDeleteResult:
        """Startup-safe CASE-B entry; it can never unlink a present Raw leaf."""

        self._hit("k0_entry_before_destructive_validation")
        try:
            authority = self._load_authority(receipt_id)
            _require_supported_platform()
            parent_fd = self._open_anchored_sealed_parent()
            try:
                # Absence is deliberately the first source observation. A
                # present symlink, directory, mismatched file, or exact Raw all
                # fail without opening a destructive CASE-A path.
                self._require_absent(parent_fd, authority.source_leaf)
                if authority.row["state"] == RemoteArchiveState.REMOTE_DELETED.value:
                    return self._terminal_result(authority)
                return self._reconcile_absent(
                    authority=authority,
                    parent_fd=parent_fd,
                )
            finally:
                os.close(parent_fd)
        except RemoteDeletionError:
            raise
        except (
            CatalogStateError,
            OSError,
            RemoteAuthorizationError,
            RemoteReceiveError,
            RemoteSourceError,
            ValueError,
        ) as exc:
            raise RemoteDeletionError(
                f"remote absent-source reconciliation failed closed: {exc}"
            ) from exc

    def _reconcile_absent(
        self,
        *,
        authority: _Authority,
        parent_fd: int,
    ) -> RemoteDeleteResult:
        self._require_absent(parent_fd, authority.source_leaf)
        current = self._reload_same_authority(authority)
        if current.row["state"] == RemoteArchiveState.REMOTE_DELETED.value:
            self._require_absent(parent_fd, authority.source_leaf)
            return self._terminal_result(current)
        os.fsync(parent_fd)
        self._require_absent(parent_fd, authority.source_leaf)
        self._hit("k3_after_parent_fsync_before_terminal")
        return self._terminalize(
            authority=authority,
            parent_fd=parent_fd,
            recovery_case=RemoteRecoveryCase.CASE_B,
            source_deleted=False,
        )

    def _terminalize(
        self,
        *,
        authority: _Authority,
        parent_fd: int,
        recovery_case: RemoteRecoveryCase,
        source_deleted: bool,
    ) -> RemoteDeleteResult:
        self._require_parent_path_identity(parent_fd)
        self._require_absent(parent_fd, authority.source_leaf)
        current = self._reload_same_authority(authority)
        if current.row["state"] == RemoteArchiveState.REMOTE_DELETED.value:
            self._require_absent(parent_fd, authority.source_leaf)
            return self._terminal_result(current)
        # The authority reload can take long enough for an accidental source
        # reappearance. CASE B must never convert that race into a terminal
        # deletion fact, and CASE A must not terminalize a recreated leaf.
        self._require_parent_path_identity(parent_fd)
        self._require_absent(parent_fd, authority.source_leaf)
        try:
            self.catalog.commit_remote_deleted(
                receipt_id=authority.receipt.receipt_id,
                expected_chunk_id=authority.receipt.chunk_id,
                expected_source_descriptor_sha256=(
                    authority.receipt.source_descriptor_sha256
                ),
                expected_source_relative_path=authority.receipt.source_relative_path,
                expected_source_manifest_sha256=(
                    authority.receipt.source_manifest_sha256
                ),
                expected_stored_bytes=authority.receipt.stored_bytes,
                expected_stored_sha256=authority.receipt.stored_sha256,
                occurred_at_utc_ns=self.utc_clock_ns(),
                fault_hook=self.fault_hook,
            )
        except Exception as exc:
            outcome = self._fresh_catalog_outcome(authority)
            self._require_absent(parent_fd, authority.source_leaf)
            if outcome.row["state"] == RemoteArchiveState.REMOTE_DELETED.value:
                return RemoteDeleteResult(
                    authority.receipt.receipt_id,
                    authority.receipt.chunk_id,
                    RemoteArchiveState.REMOTE_DELETED,
                    recovery_case,
                    source_deleted,
                    True,
                )
            raise RemoteDeletionError(
                "terminal Catalog commit did not become durable; "
                "exact Raw is absent and REMOTE_DELETE_PENDING is retryable"
            ) from exc
        self._hit("k5_after_terminal_commit")
        terminal = self._load_authority(authority.receipt.receipt_id)
        self._require_same_immutable_authority(authority, terminal)
        if terminal.row["state"] != RemoteArchiveState.REMOTE_DELETED.value:
            raise RemoteDeletionError("terminal Catalog readback is not REMOTE_DELETED")
        if terminal.manifest_bytes != authority.manifest_bytes:
            raise RemoteDeletionError("retained source manifest changed after deletion")
        self._require_absent(parent_fd, authority.source_leaf)
        return RemoteDeleteResult(
            authority.receipt.receipt_id,
            authority.receipt.chunk_id,
            RemoteArchiveState.REMOTE_DELETED,
            recovery_case,
            source_deleted,
            True,
        )

    def _fresh_catalog_outcome(self, authority: _Authority) -> _Authority:
        try:
            with Catalog(self.catalog.path, read_only=True) as fresh:
                observed = RemoteDeleter(
                    layout=self.layout,
                    catalog=fresh,
                )._load_authority(authority.receipt.receipt_id)
            self._require_same_immutable_authority(authority, observed)
            return observed
        except Exception as exc:
            raise RemoteDeletionError(
                "ambiguous terminal commit outcome cannot be validated"
            ) from exc

    def _terminal_result(self, authority: _Authority) -> RemoteDeleteResult:
        if authority.row["state"] != RemoteArchiveState.REMOTE_DELETED.value:
            raise RemoteDeletionError("terminal idempotency requires REMOTE_DELETED")
        return RemoteDeleteResult(
            authority.receipt.receipt_id,
            authority.receipt.chunk_id,
            RemoteArchiveState.REMOTE_DELETED,
            RemoteRecoveryCase.TERMINAL_ABSENT,
            False,
            True,
        )

    def _load_authority(self, receipt_id: str) -> _Authority:
        _require_receipt_id(receipt_id)
        self._require_exact_layout()
        row, chunk, same_host, remote = self.catalog.remote_delete_authority_snapshot(
            receipt_id
        )
        if row is None:
            raise RemoteDeletionError(
                "exact durable remote pending/terminal authority is missing"
            )
        receipt_bytes = row.get("receipt_bytes")
        if not isinstance(receipt_bytes, bytes):
            raise RemoteDeletionError("persisted receipt bytes are invalid")
        receipt = RemoteArchiveReceipt.from_bytes(receipt_bytes)
        if receipt.receipt_id != receipt_id:
            raise RemoteDeletionError("receipt_id does not match persisted receipt")
        if chunk is None or remote is None:
            raise RemoteDeletionError("remote lifecycle authority is incomplete")
        if same_host is not None:
            raise RemoteDeletionError("same-host and remote ownership overlap")
        if chunk.get("state") != ChunkState.SEALED.value:
            raise RemoteDeletionError("remote source physical state is not SEALED")
        if row != remote:
            raise RemoteDeletionError("remote authority changed during load")

        source_path = self._exact_direct_path(
            receipt.source_relative_path,
            self.layout.sealed,
            "Raw source",
        )
        manifest_path = self._exact_direct_path(
            receipt.source_manifest_relative_path,
            self.layout.manifests,
            "source manifest",
        )
        manifest_bytes = self._read_exact_regular(manifest_path)
        descriptor = descriptor_from_retained_manifest(
            layout=self.layout,
            catalog=self.catalog,
            row=chunk,
            manifest_bytes=manifest_bytes,
        )
        _require_remote_descriptor_binding(remote, descriptor)
        self._require_manifest_catalog_identity(manifest_bytes, chunk)
        return _Authority(
            row=row,
            chunk=chunk,
            receipt=receipt,
            descriptor=descriptor,
            manifest_bytes=manifest_bytes,
            manifest_path=manifest_path,
            source_path=source_path,
            source_leaf=source_path.name,
        )

    def _require_unchanged_pending(self, authority: _Authority) -> _Authority:
        current = self._reload_same_authority(authority)
        if current.row["state"] != RemoteArchiveState.REMOTE_DELETE_PENDING.value:
            raise RemoteDeletionError(
                "remote lifecycle changed before destructive mutation"
            )
        return current

    def _reload_same_authority(self, authority: _Authority) -> _Authority:
        current = self._load_authority(authority.receipt.receipt_id)
        self._require_same_immutable_authority(authority, current)
        return current

    @staticmethod
    def _require_same_immutable_authority(
        expected: _Authority, observed: _Authority
    ) -> None:
        immutable_row_fields = (
            "receipt_id",
            "chunk_id",
            "receipt_bytes",
            "receipt_schema_version",
            "session_id",
            "verification_version",
            "verification_outcome",
            "source_descriptor_schema_version",
            "source_descriptor_sha256",
            "market",
            "stream",
            "source_relative_path",
            "source_manifest_relative_path",
            "source_manifest_sha256",
            "stored_bytes",
            "stored_sha256",
            "archive_set_id",
            "storage_id",
            "artifact_relative_path",
            "archive_set_entry_sha256",
            "created_at_utc_ns",
        )
        if any(
            expected.row.get(field) != observed.row.get(field)
            for field in immutable_row_fields
        ):
            raise RemoteDeletionError("persisted remote authority was rebound")
        chunk_fields = (
            "chunk_id",
            "state",
            "sealed_path",
            "manifest_path",
            "record_count",
            "uncompressed_bytes",
            "stored_bytes",
            "uncompressed_sha256",
            "stored_sha256",
            "created_at_utc_ns",
        )
        if any(
            expected.chunk.get(field) != observed.chunk.get(field)
            for field in chunk_fields
        ):
            raise RemoteDeletionError("Catalog source identity changed")
        if (
            expected.receipt != observed.receipt
            or expected.descriptor != observed.descriptor
            or expected.manifest_bytes != observed.manifest_bytes
            or expected.source_path != observed.source_path
            or expected.manifest_path != observed.manifest_path
        ):
            raise RemoteDeletionError("retained source/receipt authority changed")

    def _validate_held_raw(
        self, raw_fd: int, authority: _Authority
    ) -> os.stat_result:
        initial = os.fstat(raw_fd)
        if not stat.S_ISREG(initial.st_mode):
            raise RemoteDeletionError("authorized Raw leaf is not a regular file")
        if initial.st_size != authority.receipt.stored_bytes:
            raise RemoteDeletionError("authorized Raw stored size changed")
        os.lseek(raw_fd, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        stored_bytes = 0
        while block := os.read(raw_fd, BUFFER_BYTES):
            digest.update(block)
            stored_bytes += len(block)
        if (
            stored_bytes != authority.receipt.stored_bytes
            or digest.hexdigest() != authority.receipt.stored_sha256
        ):
            raise RemoteDeletionError("authorized held Raw stored bytes/hash mismatch")

        os.lseek(raw_fd, 0, os.SEEK_SET)
        duplicate = os.dup(raw_fd)
        logical_digest = hashlib.sha256()
        logical_bytes = 0
        compressed = os.fdopen(duplicate, "rb", buffering=0)
        try:
            with zstandard.ZstdDecompressor().stream_reader(compressed) as reader:
                while block := reader.read(BUFFER_BYTES):
                    logical_digest.update(block)
                    logical_bytes += len(block)
        finally:
            compressed.close()
        expected_logical_bytes = _required_nonnegative_int(
            authority.chunk, "uncompressed_bytes"
        )
        expected_logical_digest = _required_text(
            authority.chunk, "uncompressed_sha256"
        )
        if (
            logical_bytes != expected_logical_bytes
            or logical_digest.hexdigest() != expected_logical_digest
        ):
            raise RemoteDeletionError(
                "authorized held Raw decompressed bytes/hash mismatch"
            )
        final = os.fstat(raw_fd)
        if _stable_identity(initial) != _stable_identity(final):
            raise RemoteDeletionError("held Raw changed during full validation")
        return final

    def _open_anchored_sealed_parent(self) -> int:
        self._require_exact_layout()
        before = os.lstat(self.layout.sealed)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISDIR(before.st_mode):
            raise RemoteDeletionError("sealed parent is unsafe")
        flags = (
            os.O_RDONLY
            | _platform_flag("O_CLOEXEC")
            | _platform_flag("O_DIRECTORY")
            | _platform_flag("O_NOFOLLOW")
        )
        descriptor = os.open(self.layout.sealed, flags)
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISDIR(opened.st_mode):
                raise RemoteDeletionError("anchored sealed parent is not a directory")
            if (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino):
                raise RemoteDeletionError("sealed parent changed while anchoring")
            return descriptor
        except Exception:
            os.close(descriptor)
            raise

    def _require_parent_path_identity(self, parent_fd: int) -> None:
        path_status = os.lstat(self.layout.sealed)
        held_status = os.fstat(parent_fd)
        if (
            stat.S_ISLNK(path_status.st_mode)
            or not stat.S_ISDIR(path_status.st_mode)
            or (path_status.st_dev, path_status.st_ino)
            != (held_status.st_dev, held_status.st_ino)
        ):
            raise RemoteDeletionError("anchored sealed parent no longer owns its path")

    @staticmethod
    def _open_raw_if_present(parent_fd: int, leaf: str) -> int | None:
        flags = (
            os.O_RDONLY
            | os.O_NONBLOCK
            | _platform_flag("O_CLOEXEC")
            | _platform_flag("O_NOFOLLOW")
        )
        try:
            return os.open(leaf, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            return None

    @staticmethod
    def _leaf_status(parent_fd: int, leaf: str) -> os.stat_result | None:
        try:
            return os.stat(leaf, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return None

    def _required_leaf_status(self, parent_fd: int, leaf: str) -> os.stat_result:
        observed = self._leaf_status(parent_fd, leaf)
        if observed is None:
            raise RemoteDeletionError("authorized Raw leaf disappeared")
        return observed

    def _require_absent(self, parent_fd: int, leaf: str) -> None:
        if self._leaf_status(parent_fd, leaf) is not None:
            raise RemoteDeletionError("authorized Raw leaf is present")

    def _read_exact_regular(self, path: Path) -> bytes:
        parent = path.parent
        before_parent = os.lstat(parent)
        if stat.S_ISLNK(before_parent.st_mode) or not stat.S_ISDIR(
            before_parent.st_mode
        ):
            raise RemoteDeletionError("retained manifest parent is unsafe")
        parent_flags = os.O_RDONLY
        for name in ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW"):
            value = getattr(os, name, None)
            if value is None:
                raise RemoteDeletionError(f"required platform flag {name} is absent")
            parent_flags |= cast(int, value)
        parent_fd = os.open(parent, parent_flags)
        try:
            opened_parent = os.fstat(parent_fd)
            if (before_parent.st_dev, before_parent.st_ino) != (
                opened_parent.st_dev,
                opened_parent.st_ino,
            ):
                raise RemoteDeletionError("retained manifest parent changed")
            descriptor = os.open(
                path.name,
                os.O_RDONLY
                | _platform_flag("O_CLOEXEC")
                | _platform_flag("O_NOFOLLOW"),
                dir_fd=parent_fd,
            )
            try:
                initial = os.fstat(descriptor)
                if not stat.S_ISREG(initial.st_mode):
                    raise RemoteDeletionError(
                        "retained source manifest is not a regular file"
                    )
                body = bytearray()
                while block := os.read(descriptor, BUFFER_BYTES):
                    body.extend(block)
                final = os.fstat(descriptor)
                if _stable_identity(initial) != _stable_identity(final):
                    raise RemoteDeletionError(
                        "retained source manifest changed during read"
                    )
                return bytes(body)
            finally:
                os.close(descriptor)
        finally:
            os.close(parent_fd)

    def _exact_direct_path(
        self, relative: str, expected_parent: Path, label: str
    ) -> Path:
        candidate = Path(relative)
        lexical = self.layout.root / candidate
        if (
            candidate.is_absolute()
            or ".." in candidate.parts
            or not candidate.name
            or candidate.name in {".", ".."}
            or lexical.parent != expected_parent
            or lexical.name != candidate.name
        ):
            raise RemoteDeletionError(
                f"{label} is not an exact direct Recorder-layout child"
            )
        return lexical

    def _require_exact_layout(self) -> None:
        expected = StorageLayout.from_root(self.layout.root)
        if (
            self.layout.root != expected.root
            or self.layout.sealed != expected.sealed
            or self.layout.manifests != expected.manifests
        ):
            raise RemoteDeletionError("StorageLayout does not match its exact root")
        data = self.layout.sealed.parent
        for path, label in ((data, "data"), (self.layout.sealed, "sealed")):
            observed = os.lstat(path)
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                raise RemoteDeletionError(f"Recorder {label} directory is unsafe")

    @staticmethod
    def _require_manifest_catalog_identity(
        manifest_bytes: bytes, chunk: Mapping[str, object]
    ) -> None:
        try:
            decoded = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RemoteDeletionError("retained source manifest is invalid JSON") from exc
        if not isinstance(decoded, dict):
            raise RemoteDeletionError("retained source manifest is not an object")
        manifest = cast(dict[str, object], decoded)
        expected = {
            "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
            "chunk_id": chunk.get("chunk_id"),
            "relative_path": chunk.get("sealed_path"),
            "record_count": chunk.get("record_count"),
            "stored_bytes": chunk.get("stored_bytes"),
            "stored_sha256": chunk.get("stored_sha256"),
            "uncompressed_bytes": chunk.get("uncompressed_bytes"),
            "uncompressed_sha256": chunk.get("uncompressed_sha256"),
        }
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise RemoteDeletionError(
                "retained manifest/current Catalog source identity mismatch"
            )

    def _hit(self, point: str) -> None:
        if self.fault_hook is not None:
            self.fault_hook(point)


def _require_supported_platform() -> None:
    if sys.platform not in {"linux", "darwin"}:
        raise RemoteDeletionError(
            f"M22.4B remote deletion is unsupported on {sys.platform!r}"
        )
    for name in ("O_CLOEXEC", "O_DIRECTORY", "O_NOFOLLOW"):
        if getattr(os, name, None) is None:
            raise RemoteDeletionError(f"M22.4B requires {name}")
    if os.open not in os.supports_dir_fd:
        raise RemoteDeletionError("M22.4B requires parent-relative os.open")
    if os.stat not in os.supports_dir_fd or os.stat not in os.supports_follow_symlinks:
        raise RemoteDeletionError("M22.4B requires parent-relative no-follow stat")
    if os.unlink not in os.supports_dir_fd:
        raise RemoteDeletionError("M22.4B requires parent-relative os.unlink")


def _platform_flag(name: str) -> int:
    value = getattr(os, name, None)
    if not isinstance(value, int):
        raise RemoteDeletionError(f"M22.4B requires {name}")
    return value


def _require_receipt_id(receipt_id: object) -> None:
    if (
        not isinstance(receipt_id, str)
        or len(receipt_id) != 64
        or any(character not in "0123456789abcdef" for character in receipt_id)
    ):
        raise RemoteDeletionError("receipt_id must be a lowercase SHA-256 digest")


def _stable_identity(status: os.stat_result) -> tuple[int, int, int, int]:
    return (status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns)


def _required_nonnegative_int(row: Mapping[str, object], name: str) -> int:
    value = row.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RemoteDeletionError(f"Catalog source lacks valid {name}")
    return value


def _required_text(row: Mapping[str, object], name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value:
        raise RemoteDeletionError(f"Catalog source lacks valid {name}")
    return value
