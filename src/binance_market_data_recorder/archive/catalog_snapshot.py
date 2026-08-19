"""Post-session SQLite Catalog disaster-recovery snapshots for M22.6."""

from __future__ import annotations

import hashlib
import importlib
import io
import json
import os
import re
import sqlite3
import stat
import sys
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import BinaryIO, Protocol, cast

from ..storage.catalog import Catalog, CatalogStateError, RemoteArchiveState
from ..storage.layout import StorageLayout, fsync_directory
from .remote_session import RemoteArchiveSession, RemoteArchiveSessionResult

CATALOG_SNAPSHOT_MANIFEST_SCHEMA = "catalog-snapshot-manifest.v1"
CATALOG_SNAPSHOT_RETENTION_SCHEMA = "catalog-snapshot-retention.v1"
CATALOG_SNAPSHOT_VERIFICATION_VERSION = "catalog-snapshot-verification.v1"
CATALOG_SNAPSHOT_DIRECTORY = "catalog-backups"
CATALOG_SNAPSHOT_STAGING_DIRECTORY = "catalog-snapshot-staging"

_MANIFEST_FILENAME = "catalog-snapshot-manifest.json"
_SNAPSHOT_FILENAME = "catalog.sqlite"
_REMOTE_OWNER_FILENAME = ".catalog-snapshot-owner.json"
_REMOTE_LOCK_FILENAME = ".active.lock"
_LOCAL_OWNER_FILENAME = ".catalog-snapshot-generation.json"
_INITIALIZED_FILENAME = ".initialized"
_WORKSPACE_WRITER_LOCK_FILENAME = ".catalog-snapshot-writer.lock"
_RETENTION_FILENAMES = ("retention-0.json", "retention-1.json")
_REMOTE_OWNER_SCHEMA = "catalog-snapshot-staging-owner.v1"
_LOCAL_OWNER_SCHEMA = "catalog-snapshot-generation-owner.v1"
_STORE_SCHEMA = "catalog-snapshot-store.v1"
_COPY_BUFFER_BYTES = 1024 * 1024
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class CatalogSnapshotError(RuntimeError):
    """Snapshot generation, transfer, validation, or publication failed."""


class PostSessionSnapshotError(CatalogSnapshotError):
    """The archive session committed, but its required DR snapshot failed."""

    code = "POST_SESSION_SNAPSHOT_FAILED"

    def __init__(
        self,
        *,
        session_result: RemoteArchiveSessionResult,
        receipt_id: str,
        committed_remote_state: RemoteArchiveState,
        cause: BaseException,
    ) -> None:
        self.session_result = session_result
        self.receipt_id = receipt_id
        self.committed_remote_state = committed_remote_state
        self.cause = cause
        super().__init__(
            "archive session already committed; post-session Catalog snapshot "
            f"failed ({receipt_id}, {committed_remote_state.value}): {cause}"
        )


class CatalogSnapshotTransport(Protocol):
    def open_catalog_snapshot(
        self, receipt_id: str, required_state: RemoteArchiveState
    ) -> BinaryIO: ...


@dataclass(frozen=True, slots=True)
class CatalogSnapshotManifest:
    schema: str
    snapshot_id: str
    receipt_id: str
    chunk_id: str
    required_remote_state: str
    observed_remote_state: str
    stored_bytes: int
    sha256: str
    verification_version: str
    verified_at_utc_ns: int

    def document(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "snapshot_id": self.snapshot_id,
            "receipt_id": self.receipt_id,
            "chunk_id": self.chunk_id,
            "required_remote_state": self.required_remote_state,
            "observed_remote_state": self.observed_remote_state,
            "stored_bytes": self.stored_bytes,
            "sha256": self.sha256,
            "verification_version": self.verification_version,
            "verified_at_utc_ns": self.verified_at_utc_ns,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.document())

    @classmethod
    def from_bytes(cls, body: bytes) -> CatalogSnapshotManifest:
        document = _exact_json_object(body, set(cls.__dataclass_fields__))
        try:
            manifest = cls(
                schema=cast(str, document["schema"]),
                snapshot_id=cast(str, document["snapshot_id"]),
                receipt_id=cast(str, document["receipt_id"]),
                chunk_id=cast(str, document["chunk_id"]),
                required_remote_state=cast(str, document["required_remote_state"]),
                observed_remote_state=cast(str, document["observed_remote_state"]),
                stored_bytes=cast(int, document["stored_bytes"]),
                sha256=cast(str, document["sha256"]),
                verification_version=cast(str, document["verification_version"]),
                verified_at_utc_ns=cast(int, document["verified_at_utc_ns"]),
            )
        except (KeyError, TypeError) as exc:
            raise CatalogSnapshotError("snapshot manifest fields are invalid") from exc
        manifest.validate()
        if manifest.canonical_bytes() != body:
            raise CatalogSnapshotError("snapshot manifest is not canonical")
        return manifest

    def validate(self) -> None:
        if self.schema != CATALOG_SNAPSHOT_MANIFEST_SCHEMA:
            raise CatalogSnapshotError("snapshot manifest schema is unsupported")
        _require_uuid4(self.snapshot_id, "snapshot_id")
        _require_sha256(self.receipt_id, "receipt_id")
        _require_uuid(self.chunk_id, "chunk_id")
        required = _required_state(self.required_remote_state)
        observed = _required_state(self.observed_remote_state)
        _require_lower_bound(required, observed)
        if (
            not isinstance(self.stored_bytes, int)
            or isinstance(self.stored_bytes, bool)
            or self.stored_bytes <= 0
        ):
            raise CatalogSnapshotError("snapshot stored_bytes is invalid")
        _require_sha256(self.sha256, "sha256")
        if self.verification_version != CATALOG_SNAPSHOT_VERIFICATION_VERSION:
            raise CatalogSnapshotError("snapshot verification version is unsupported")
        if (
            not isinstance(self.verified_at_utc_ns, int)
            or isinstance(self.verified_at_utc_ns, bool)
            or self.verified_at_utc_ns < 0
        ):
            raise CatalogSnapshotError("snapshot verification time is invalid")


@dataclass(frozen=True, slots=True)
class CatalogSnapshotReference:
    snapshot_id: str
    manifest_sha256: str

    def document(self) -> dict[str, str]:
        return {
            "snapshot_id": self.snapshot_id,
            "manifest_sha256": self.manifest_sha256,
        }

    @classmethod
    def from_object(cls, value: object) -> CatalogSnapshotReference | None:
        if value is None:
            return None
        if not isinstance(value, dict) or set(value) != {
            "snapshot_id",
            "manifest_sha256",
        }:
            raise CatalogSnapshotError("retention reference fields are invalid")
        reference = cls(
            snapshot_id=cast(str, value["snapshot_id"]),
            manifest_sha256=cast(str, value["manifest_sha256"]),
        )
        _require_uuid4(reference.snapshot_id, "retention snapshot_id")
        _require_sha256(reference.manifest_sha256, "retention manifest_sha256")
        return reference


@dataclass(frozen=True, slots=True)
class CatalogSnapshotRetention:
    schema: str
    generation: int
    latest: CatalogSnapshotReference | None
    previous: CatalogSnapshotReference | None

    def document(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "generation": self.generation,
            "latest": None if self.latest is None else self.latest.document(),
            "previous": None if self.previous is None else self.previous.document(),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.document())

    @classmethod
    def from_bytes(cls, body: bytes) -> CatalogSnapshotRetention:
        document = _exact_json_object(
            body, {"schema", "generation", "latest", "previous"}
        )
        generation = document.get("generation")
        if (
            not isinstance(generation, int)
            or isinstance(generation, bool)
            or generation < 0
        ):
            raise CatalogSnapshotError("retention generation is invalid")
        retention = cls(
            schema=cast(str, document.get("schema")),
            generation=generation,
            latest=CatalogSnapshotReference.from_object(document.get("latest")),
            previous=CatalogSnapshotReference.from_object(document.get("previous")),
        )
        if retention.schema != CATALOG_SNAPSHOT_RETENTION_SCHEMA:
            raise CatalogSnapshotError("retention schema is unsupported")
        if retention.latest is None and retention.previous is not None:
            raise CatalogSnapshotError("retention previous exists without latest")
        if retention.latest == retention.previous and retention.latest is not None:
            raise CatalogSnapshotError("retention latest and previous are identical")
        if retention.canonical_bytes() != body:
            raise CatalogSnapshotError("retention document is not canonical")
        return retention


@dataclass(frozen=True, slots=True)
class CatalogSnapshotResult:
    snapshot_id: str
    snapshot_path: Path
    manifest_path: Path
    manifest: CatalogSnapshotManifest
    retention_generation: int


@dataclass(frozen=True, slots=True)
class PostSessionArchiveWorkflowResult:
    session: RemoteArchiveSessionResult
    snapshot: CatalogSnapshotResult | None


class CatalogSnapshotExporter:
    """Generate, self-validate, and expose one owned immutable staging snapshot."""

    def __init__(self, *, layout: StorageLayout) -> None:
        self.layout = layout
        self.staging_root = layout.state / CATALOG_SNAPSHOT_STAGING_DIRECTORY

    def open_catalog_snapshot(
        self, receipt_id: str, required_state: RemoteArchiveState
    ) -> BinaryIO:
        locking = _fcntl_module()
        _require_sha256(receipt_id, "receipt_id")
        required_state = _required_state(required_state)
        self._ensure_staging_root()
        self._cleanup_inactive_stages()
        stage_id = str(uuid.uuid4())
        stage = self.staging_root / stage_id
        stage.mkdir(mode=0o700)
        lock_descriptor: int | None = None
        try:
            owner = _canonical_json(
                {"schema": _REMOTE_OWNER_SCHEMA, "stage_id": stage_id}
            )
            _write_new_file(stage / _REMOTE_OWNER_FILENAME, owner)
            lock_descriptor = os.open(
                stage / _REMOTE_LOCK_FILENAME, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600
            )
            locking.flock(lock_descriptor, locking.LOCK_EX)
            destination_path = stage / _SNAPSHOT_FILENAME
            destination = sqlite3.connect(destination_path)
            try:
                os.chmod(destination_path, 0o600)
                with Catalog.open_live_read_only(self.layout.catalog) as source:
                    source.backup_to(destination)
            finally:
                destination.close()
            _validate_snapshot(destination_path, receipt_id, required_state)
            handle = destination_path.open("rb", buffering=0)
            return cast(
                BinaryIO,
                _OwnedSnapshotStream(
                    handle,
                    lambda: self._release_and_remove(stage, lock_descriptor),
                ),
            )
        except BaseException:
            self._release_and_remove(stage, lock_descriptor)
            raise

    def _ensure_staging_root(self) -> None:
        if self.staging_root.exists() and (
            self.staging_root.is_symlink() or not self.staging_root.is_dir()
        ):
            raise CatalogSnapshotError("remote snapshot staging root is unsafe")
        self.staging_root.mkdir(mode=0o700, parents=False, exist_ok=True)
        os.chmod(self.staging_root, 0o700)
        fsync_directory(self.layout.state)

    def _cleanup_inactive_stages(self) -> None:
        locking = _fcntl_module()
        try:
            children = tuple(self.staging_root.iterdir())
        except OSError as exc:
            raise CatalogSnapshotError("cannot inspect remote snapshot staging") from exc
        for stage in children:
            if stage.is_symlink() or not stage.is_dir() or not _is_uuid4(stage.name):
                continue
            if not _valid_owner_marker(
                stage / _REMOTE_OWNER_FILENAME, _REMOTE_OWNER_SCHEMA, stage.name, "stage_id"
            ):
                continue
            lock_path = stage / _REMOTE_LOCK_FILENAME
            try:
                descriptor = os.open(lock_path, os.O_RDWR | os.O_NOFOLLOW)
            except OSError:
                continue
            try:
                try:
                    locking.flock(descriptor, locking.LOCK_EX | locking.LOCK_NB)
                except BlockingIOError:
                    continue
                _remove_exact_stage(stage, remote=True)
            finally:
                os.close(descriptor)

    def _release_and_remove(self, stage: Path, descriptor: int | None) -> None:
        if descriptor is not None:
            with suppress(OSError):
                locking = _fcntl_module()
                locking.flock(descriptor, locking.LOCK_UN)
            with suppress(OSError):
                os.close(descriptor)
        with suppress(OSError):
            _remove_exact_stage(stage, remote=True)
            fsync_directory(self.staging_root)


class _OwnedSnapshotStream(io.RawIOBase):
    def __init__(self, source: BinaryIO, cleanup: Callable[[], None]) -> None:
        super().__init__()
        self._source = source
        self._cleanup = cleanup

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed snapshot stream")
        return self._source.read(size)

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._source.close()
        finally:
            self._cleanup()
            super().close()


class CatalogSnapshotStore:
    """Receive and durably retain verified immutable snapshot generations."""

    def __init__(
        self,
        *,
        workspace_root: Path,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        if os.name == "nt" or sys.platform.startswith("win"):
            raise CatalogSnapshotError("complete Catalog snapshot durability is unsupported")
        if not workspace_root.is_absolute():
            raise CatalogSnapshotError("Offline Workspace root must be absolute")
        self.workspace_root = workspace_root.resolve()
        self._writer_lock_path = (
            self.workspace_root / _WORKSPACE_WRITER_LOCK_FILENAME
        )
        self.root = self.workspace_root / CATALOG_SNAPSHOT_DIRECTORY
        self.snapshots = self.root / "snapshots"
        self.staging = self.root / ".staging"
        self.fault_hook = fault_hook
        self._ensure_workspace_root()
        with self._workspace_writer_lock(
            fault_point="after_workspace_initialization_lock_acquired"
        ):
            self._initialize_or_validate()

    def snapshot_post_session(
        self,
        *,
        transport: CatalogSnapshotTransport,
        receipt_id: str,
        required_state: RemoteArchiveState,
    ) -> CatalogSnapshotResult:
        _require_sha256(receipt_id, "receipt_id")
        required_state = _required_state(required_state)
        with self._workspace_writer_lock(
            fault_point="after_workspace_writer_lock_acquired"
        ):
            return self._snapshot_post_session_locked(
                transport=transport,
                receipt_id=receipt_id,
                required_state=required_state,
            )

    def _snapshot_post_session_locked(
        self,
        *,
        transport: CatalogSnapshotTransport,
        receipt_id: str,
        required_state: RemoteArchiveState,
    ) -> CatalogSnapshotResult:
        retention, slots = self._read_retention()
        snapshot_id = str(uuid.uuid4())
        stage = self.staging / snapshot_id
        stage.mkdir(mode=0o700)
        snapshot_path = stage / _SNAPSHOT_FILENAME
        published = False
        try:
            _write_new_file(
                stage / _LOCAL_OWNER_FILENAME,
                _canonical_json(
                    {"schema": _LOCAL_OWNER_SCHEMA, "snapshot_id": snapshot_id}
                ),
            )
            descriptor = os.open(snapshot_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                stream = transport.open_catalog_snapshot(receipt_id, required_state)
                with stream:
                    while True:
                        block = stream.read(_COPY_BUFFER_BYTES)
                        if not block:
                            break
                        _write_all(descriptor, block)
                os.fsync(descriptor)
                self._fault("after_snapshot_file_fsync")
            finally:
                os.close(descriptor)
            fsync_directory(stage)
            self._fault("after_snapshot_directory_fsync")
            stored_bytes, sha256 = _hash_reopened_file(snapshot_path)
            row, observed = _validate_snapshot(snapshot_path, receipt_id, required_state)
            self._fault("after_snapshot_validation")
            chunk_id = cast(str, row["chunk_id"])
            manifest = CatalogSnapshotManifest(
                schema=CATALOG_SNAPSHOT_MANIFEST_SCHEMA,
                snapshot_id=snapshot_id,
                receipt_id=receipt_id,
                chunk_id=chunk_id,
                required_remote_state=required_state.value,
                observed_remote_state=observed.value,
                stored_bytes=stored_bytes,
                sha256=sha256,
                verification_version=CATALOG_SNAPSHOT_VERIFICATION_VERSION,
                verified_at_utc_ns=time.time_ns(),
            )
            manifest.validate()
            manifest_path = stage / _MANIFEST_FILENAME
            _write_new_file(manifest_path, manifest.canonical_bytes())
            self._fault("after_manifest_file_fsync")
            reparsed = CatalogSnapshotManifest.from_bytes(manifest_path.read_bytes())
            if reparsed != manifest:
                raise CatalogSnapshotError("snapshot manifest readback changed")
            fsync_directory(stage)
            destination = self.snapshots / snapshot_id
            if destination.exists() or destination.is_symlink():
                raise CatalogSnapshotError("snapshot generation destination already exists")
            os.rename(stage, destination)
            published = True
            self._fault("after_generation_publish_before_parent_fsync")
            fsync_directory(self.snapshots)
            self._fault("after_generation_parent_fsync")
            reference = CatalogSnapshotReference(
                snapshot_id=snapshot_id,
                manifest_sha256=hashlib.sha256(manifest.canonical_bytes()).hexdigest(),
            )
            updated = CatalogSnapshotRetention(
                schema=CATALOG_SNAPSHOT_RETENTION_SCHEMA,
                generation=retention.generation + 1,
                latest=reference,
                previous=retention.latest,
            )
            self._write_retention(updated, slots)
            self._cleanup_obsolete(updated)
            final = destination
            return CatalogSnapshotResult(
                snapshot_id=snapshot_id,
                snapshot_path=final / _SNAPSHOT_FILENAME,
                manifest_path=final / _MANIFEST_FILENAME,
                manifest=manifest,
                retention_generation=updated.generation,
            )
        except BaseException as exc:
            if not published:
                with suppress(OSError):
                    _remove_exact_stage(stage, remote=False)
                    fsync_directory(self.staging)
            if isinstance(exc, CatalogSnapshotError):
                raise
            raise CatalogSnapshotError(f"Catalog snapshot failed: {exc}") from exc

    def current_retention(self) -> CatalogSnapshotRetention:
        return self._read_retention()[0]

    def _ensure_workspace_root(self) -> None:
        if self.workspace_root.exists() and (
            self.workspace_root.is_symlink() or not self.workspace_root.is_dir()
        ):
            raise CatalogSnapshotError("Offline Workspace root is unsafe")
        self.workspace_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.workspace_root.is_symlink() or not self.workspace_root.is_dir():
            raise CatalogSnapshotError("Offline Workspace root is unsafe")

    @contextmanager
    def _workspace_writer_lock(self, *, fault_point: str) -> Iterator[None]:
        locking = _fcntl_module()
        flags = os.O_CREAT | os.O_RDWR
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self._writer_lock_path, flags, 0o600)
        except OSError as exc:
            raise CatalogSnapshotError("workspace snapshot writer lock is unsafe") from exc
        try:
            descriptor_stat = os.fstat(descriptor)
            path_stat = os.lstat(self._writer_lock_path)
            if (
                not stat.S_ISREG(descriptor_stat.st_mode)
                or not stat.S_ISREG(path_stat.st_mode)
                or descriptor_stat.st_dev != path_stat.st_dev
                or descriptor_stat.st_ino != path_stat.st_ino
            ):
                raise CatalogSnapshotError("workspace snapshot writer lock is unsafe")
            os.fchmod(descriptor, 0o600)
            locking.flock(descriptor, locking.LOCK_EX)
        except CatalogSnapshotError:
            with suppress(OSError):
                os.close(descriptor)
            raise
        except OSError as exc:
            with suppress(OSError):
                os.close(descriptor)
            raise CatalogSnapshotError("workspace snapshot writer lock failed") from exc
        try:
            self._fault(fault_point)
            yield
        finally:
            with suppress(OSError):
                locking.flock(descriptor, locking.LOCK_UN)
            with suppress(OSError):
                os.close(descriptor)

    def _initialize_or_validate(self) -> None:
        if not self.root.exists():
            self.root.mkdir(mode=0o700)
            self.snapshots.mkdir(mode=0o700)
            self.staging.mkdir(mode=0o700)
            empty = CatalogSnapshotRetention(
                CATALOG_SNAPSHOT_RETENTION_SCHEMA, 0, None, None
            ).canonical_bytes()
            for name in _RETENTION_FILENAMES:
                _write_new_file(self.root / name, empty)
            fsync_directory(self.snapshots)
            fsync_directory(self.staging)
            fsync_directory(self.root)
            _write_new_file(
                self.root / _INITIALIZED_FILENAME,
                _canonical_json({"schema": _STORE_SCHEMA}),
            )
            fsync_directory(self.root)
            fsync_directory(self.workspace_root)
        if self.root.is_symlink() or not self.root.is_dir():
            raise CatalogSnapshotError("Catalog backup root is unsafe")
        marker = self.root / _INITIALIZED_FILENAME
        try:
            body = marker.read_bytes()
        except OSError as exc:
            raise CatalogSnapshotError("Catalog snapshot store is not safely initialized") from exc
        if body != _canonical_json({"schema": _STORE_SCHEMA}):
            raise CatalogSnapshotError("Catalog snapshot initialization marker is invalid")
        for directory in (self.snapshots, self.staging):
            if directory.is_symlink() or not directory.is_dir():
                raise CatalogSnapshotError("Catalog snapshot directory layout is invalid")

    def _read_retention(
        self,
    ) -> tuple[CatalogSnapshotRetention, tuple[CatalogSnapshotRetention | None, ...]]:
        parsed: list[CatalogSnapshotRetention | None] = []
        bodies: list[bytes | None] = []
        for name in _RETENTION_FILENAMES:
            path = self.root / name
            try:
                body = path.read_bytes()
                retention = CatalogSnapshotRetention.from_bytes(body)
                self._validate_retention_references(retention)
            except (OSError, CatalogSnapshotError):
                body = None
                retention = None
            parsed.append(retention)
            bodies.append(body)
        valid = [(index, value) for index, value in enumerate(parsed) if value is not None]
        if not valid:
            raise CatalogSnapshotError("both initialized retention slots are invalid")
        if len(valid) == 2:
            left = cast(CatalogSnapshotRetention, parsed[0])
            right = cast(CatalogSnapshotRetention, parsed[1])
            if left.generation == right.generation and bodies[0] != bodies[1]:
                raise CatalogSnapshotError("equal-generation retention slots disagree")
        chosen = max((value for _, value in valid), key=lambda value: value.generation)
        return chosen, tuple(parsed)

    def _validate_retention_references(self, retention: CatalogSnapshotRetention) -> None:
        for reference in (retention.latest, retention.previous):
            if reference is None:
                continue
            generation = self.snapshots / reference.snapshot_id
            if generation.is_symlink() or not generation.is_dir():
                raise CatalogSnapshotError("retention generation is missing or unsafe")
            manifest_path = generation / _MANIFEST_FILENAME
            snapshot_path = generation / _SNAPSHOT_FILENAME
            manifest_body = manifest_path.read_bytes()
            if hashlib.sha256(manifest_body).hexdigest() != reference.manifest_sha256:
                raise CatalogSnapshotError("retention manifest identity mismatch")
            manifest = CatalogSnapshotManifest.from_bytes(manifest_body)
            if manifest.snapshot_id != reference.snapshot_id:
                raise CatalogSnapshotError("retention snapshot identity mismatch")
            size, digest = _hash_reopened_file(snapshot_path)
            if size != manifest.stored_bytes or digest != manifest.sha256:
                raise CatalogSnapshotError("retained snapshot bytes mismatch manifest")

    def _write_retention(
        self,
        retention: CatalogSnapshotRetention,
        slots: tuple[CatalogSnapshotRetention | None, ...],
    ) -> None:
        body = retention.canonical_bytes()
        if slots[0] is None:
            order = (0, 1)
        elif slots[1] is None:
            order = (1, 0)
        elif slots[0].generation <= slots[1].generation:
            order = (0, 1)
        else:
            order = (1, 0)
        for ordinal, index in enumerate(order):
            slot = self.root / _RETENTION_FILENAMES[index]
            temporary = self.root / f".{slot.name}.{uuid.uuid4().hex}.partial"
            try:
                _write_new_file(temporary, body)
                self._fault(
                    "after_first_retention_temp_fsync"
                    if ordinal == 0
                    else "after_second_retention_temp_fsync"
                )
                os.replace(temporary, slot)
                self._fault(
                    "after_first_retention_replace"
                    if ordinal == 0
                    else "after_second_retention_replace"
                )
                fsync_directory(self.root)
                self._fault(
                    "after_first_retention_parent_fsync"
                    if ordinal == 0
                    else "after_second_retention_parent_fsync"
                )
            finally:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)
        recovered, _ = self._read_retention()
        if recovered != retention:
            raise CatalogSnapshotError("retention publication readback mismatch")

    def _cleanup_obsolete(self, retention: CatalogSnapshotRetention) -> None:
        keep = {
            reference.snapshot_id
            for reference in (retention.latest, retention.previous)
            if reference is not None
        }
        try:
            self._fault("before_obsolete_cleanup")
            for generation in tuple(self.snapshots.iterdir()):
                if (
                    generation.name in keep
                    or generation.is_symlink()
                    or not generation.is_dir()
                    or not _is_uuid4(generation.name)
                    or not _valid_owner_marker(
                        generation / _LOCAL_OWNER_FILENAME,
                        _LOCAL_OWNER_SCHEMA,
                        generation.name,
                        "snapshot_id",
                    )
                ):
                    continue
                manifest = CatalogSnapshotManifest.from_bytes(
                    (generation / _MANIFEST_FILENAME).read_bytes()
                )
                size, digest = _hash_reopened_file(generation / _SNAPSHOT_FILENAME)
                if manifest.snapshot_id != generation.name or (
                    size,
                    digest,
                ) != (manifest.stored_bytes, manifest.sha256):
                    continue
                _remove_exact_stage(generation, remote=False)
            fsync_directory(self.snapshots)
            self._fault("after_obsolete_cleanup_before_parent_fsync")
        except Exception:
            return

    def _fault(self, point: str) -> None:
        if self.fault_hook is not None:
            self.fault_hook(point)


class PostSessionArchiveWorkflow:
    """Run exactly one existing archive session, then one snapshot-only action."""

    def __init__(
        self,
        *,
        session: RemoteArchiveSession,
        snapshot_store: CatalogSnapshotStore,
        transport: CatalogSnapshotTransport,
    ) -> None:
        self.session = session
        self.snapshot_store = snapshot_store
        self.transport = transport

    def run_one(
        self, *, delete: bool, session_id: str | None = None
    ) -> PostSessionArchiveWorkflowResult:
        result = self.session.run_one(delete=delete, session_id=session_id)
        if not result.worked:
            return PostSessionArchiveWorkflowResult(result, None)
        if result.receipt is None or result.authority is None:
            raise CatalogSnapshotError("successful archive session omitted committed authority")
        required_state = result.authority.state
        if required_state not in {
            RemoteArchiveState.REMOTE_DELETE_PENDING,
            RemoteArchiveState.REMOTE_DELETED,
        }:
            raise CatalogSnapshotError("successful archive session state is unsupported")
        try:
            snapshot = self.snapshot_store.snapshot_post_session(
                transport=self.transport,
                receipt_id=result.receipt.receipt_id,
                required_state=required_state,
            )
        except BaseException as exc:
            raise PostSessionSnapshotError(
                session_result=result,
                receipt_id=result.receipt.receipt_id,
                committed_remote_state=required_state,
                cause=exc,
            ) from exc
        return PostSessionArchiveWorkflowResult(result, snapshot)


def _validate_snapshot(
    path: Path, receipt_id: str, required_state: RemoteArchiveState
) -> tuple[dict[str, object], RemoteArchiveState]:
    if not path.is_file() or path.is_symlink():
        raise CatalogSnapshotError("snapshot database is missing or unsafe")
    if any(path.with_name(f"{path.name}{suffix}").exists() for suffix in ("-wal", "-shm")):
        raise CatalogSnapshotError("snapshot database depends on a sidecar")
    try:
        with Catalog(path, read_only=True) as snapshot:
            if snapshot.integrity_check() != ("ok",):
                raise CatalogSnapshotError("snapshot integrity_check did not return exact ok")
            row = snapshot.remote_archive_transaction(receipt_id)
    except (CatalogStateError, sqlite3.Error) as exc:
        raise CatalogSnapshotError("snapshot SQLite/Catalog validation failed") from exc
    if row is None:
        raise CatalogSnapshotError("snapshot lacks the triggering receipt")
    try:
        observed = RemoteArchiveState(str(row["state"]))
    except (KeyError, ValueError) as exc:
        raise CatalogSnapshotError("snapshot target state is invalid") from exc
    _require_lower_bound(required_state, observed)
    return row, observed


def _required_state(value: RemoteArchiveState | str) -> RemoteArchiveState:
    try:
        state = value if isinstance(value, RemoteArchiveState) else RemoteArchiveState(value)
    except ValueError as exc:
        raise CatalogSnapshotError("required remote state is invalid") from exc
    if state not in {
        RemoteArchiveState.REMOTE_DELETE_PENDING,
        RemoteArchiveState.REMOTE_DELETED,
    }:
        raise CatalogSnapshotError("required remote state is invalid")
    return state


def _require_lower_bound(
    required: RemoteArchiveState, observed: RemoteArchiveState
) -> None:
    allowed = {
        RemoteArchiveState.REMOTE_DELETE_PENDING: {
            RemoteArchiveState.REMOTE_DELETE_PENDING,
            RemoteArchiveState.REMOTE_DELETED,
        },
        RemoteArchiveState.REMOTE_DELETED: {RemoteArchiveState.REMOTE_DELETED},
    }
    if observed not in allowed[required]:
        raise CatalogSnapshotError(
            f"snapshot state {observed.value} is below required {required.value}"
        )


def _hash_reopened_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb", buffering=0) as source:
            while block := source.read(_COPY_BUFFER_BYTES):
                size += len(block)
                digest.update(block)
    except OSError as exc:
        raise CatalogSnapshotError("cannot reopen and hash snapshot bytes") from exc
    return size, digest.hexdigest()


def _write_new_file(path: Path, body: bytes) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        _write_all(descriptor, body)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, body: bytes) -> None:
    remaining = memoryview(body)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("snapshot write made no progress")
        remaining = remaining[written:]


def _remove_exact_stage(path: Path, *, remote: bool) -> None:
    if path.is_symlink() or not path.is_dir() or not _is_uuid4(path.name):
        raise OSError("snapshot cleanup target is not an owned generation")
    allowed = {
        _REMOTE_OWNER_FILENAME,
        _REMOTE_LOCK_FILENAME,
        _SNAPSHOT_FILENAME,
        f"{_SNAPSHOT_FILENAME}-journal",
        f"{_SNAPSHOT_FILENAME}-wal",
        f"{_SNAPSHOT_FILENAME}-shm",
    } if remote else {
        _LOCAL_OWNER_FILENAME,
        _SNAPSHOT_FILENAME,
        _MANIFEST_FILENAME,
    }
    entries = tuple(path.iterdir())
    if any(entry.name not in allowed or entry.is_dir() for entry in entries):
        raise OSError("snapshot cleanup target contains unknown entries")
    for entry in entries:
        entry.unlink()
    path.rmdir()


def _valid_owner_marker(
    path: Path, schema: str, identity: str, identity_field: str
) -> bool:
    if path.is_symlink() or not path.is_file():
        return False
    expected = _canonical_json({"schema": schema, identity_field: identity})
    try:
        return path.read_bytes() == expected
    except OSError:
        return False


def _exact_json_object(body: bytes, fields: set[str]) -> dict[str, object]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CatalogSnapshotError("snapshot metadata is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict) or set(value) != fields:
        raise CatalogSnapshotError("snapshot metadata fields are not exact")
    return cast(dict[str, object], value)


def _canonical_json(document: Mapping[str, object]) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _require_sha256(value: object, field: str) -> None:
    if not isinstance(value, str) or _LOWER_SHA256.fullmatch(value) is None:
        raise CatalogSnapshotError(f"{field} must be a lowercase SHA-256 digest")


def _require_uuid(value: object, field: str) -> uuid.UUID:
    if not isinstance(value, str):
        raise CatalogSnapshotError(f"{field} must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise CatalogSnapshotError(f"{field} must be a canonical UUID") from exc
    if str(parsed) != value:
        raise CatalogSnapshotError(f"{field} must be a canonical UUID")
    return parsed


def _require_uuid4(value: object, field: str) -> None:
    if _require_uuid(value, field).version != 4:
        raise CatalogSnapshotError(f"{field} must be a canonical UUID4")


def _is_uuid4(value: str) -> bool:
    try:
        _require_uuid4(value, "identity")
    except CatalogSnapshotError:
        return False
    return True


def _fcntl_module() -> ModuleType:
    try:
        return importlib.import_module("fcntl")
    except ImportError as exc:
        raise CatalogSnapshotError("Catalog snapshot locking is unsupported") from exc


__all__ = [
    "CATALOG_SNAPSHOT_DIRECTORY",
    "CATALOG_SNAPSHOT_MANIFEST_SCHEMA",
    "CATALOG_SNAPSHOT_RETENTION_SCHEMA",
    "CATALOG_SNAPSHOT_STAGING_DIRECTORY",
    "CATALOG_SNAPSHOT_VERIFICATION_VERSION",
    "CatalogSnapshotError",
    "CatalogSnapshotExporter",
    "CatalogSnapshotManifest",
    "CatalogSnapshotResult",
    "CatalogSnapshotRetention",
    "CatalogSnapshotStore",
    "CatalogSnapshotTransport",
    "PostSessionArchiveWorkflow",
    "PostSessionArchiveWorkflowResult",
    "PostSessionSnapshotError",
]
