"""Durable Archive Set identity and rebuildable workspace index.

This module is deliberately separate from the live Recorder Catalog.  The
registered-storage marker remains the physical identity authority; the files
written here add logical Archive Set membership and whole-chunk inventory.
"""

from __future__ import annotations

import ctypes
import errno
import json
import os
import sqlite3
import sys
import uuid
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ..storage.layout import fsync_directory
from ..storage.macos import StorageRegistrationError, validate_registered_root

ARCHIVE_SET_MEDIUM_SCHEMA = "archive-set-medium.v1"
ARCHIVE_SET_ENTRY_SCHEMA = "archive-set-entry.v1"
ARCHIVE_SET_MEDIUM_FILENAME = ".binance-market-data-recorder-archive-set.json"
ARCHIVE_SET_DIRECTORY_NAME = "archive-set"
ARCHIVE_SET_ENTRIES_DIRECTORY_NAME = "entries"
ARCHIVE_SET_DIRECTORY = Path(ARCHIVE_SET_DIRECTORY_NAME)
ARCHIVE_SET_ENTRIES_DIRECTORY = ARCHIVE_SET_DIRECTORY / ARCHIVE_SET_ENTRIES_DIRECTORY_NAME

_MEDIUM_FIELDS = (
    "schema",
    "archive_set_id",
    "storage_id",
    "volume_uuid",
    "registered_relative_path",
    "marker_nonce",
)
_ENTRY_FIELDS = (
    "schema",
    "archive_set_id",
    "storage_id",
    "chunk_id",
    "artifact_relative_path",
    "archive_manifest_relative_path",
    "archive_manifest_sha256",
    "stored_bytes",
    "stored_sha256",
    "source_manifest_sha256",
)
_HEX_SHA256_LENGTH = 64


class ArchiveSetError(RuntimeError):
    """Archive Set metadata is invalid or cannot be committed safely."""


def generate_archive_set_id() -> str:
    """Generate a canonical UUID4 Archive Set identifier."""

    return str(uuid.uuid4())


@dataclass(frozen=True, slots=True)
class ArchiveMediumIdentity:
    archive_set_id: str
    storage_id: str
    volume_uuid: str
    registered_relative_path: str
    marker_nonce: str

    def __post_init__(self) -> None:
        _require_text(self.archive_set_id, "archive_set_id")
        _require_text(self.storage_id, "storage_id")
        if self.archive_set_id == self.storage_id:
            raise ArchiveSetError("archive_set_id and storage_id must differ")
        _require_text(self.volume_uuid, "volume_uuid")
        _require_relative_path(self.registered_relative_path, "registered_relative_path")
        _require_text(self.marker_nonce, "marker_nonce")

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": ARCHIVE_SET_MEDIUM_SCHEMA,
            "archive_set_id": self.archive_set_id,
            "storage_id": self.storage_id,
            "volume_uuid": self.volume_uuid,
            "registered_relative_path": self.registered_relative_path,
            "marker_nonce": self.marker_nonce,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.as_dict())

    @classmethod
    def from_dict(cls, document: Mapping[str, object]) -> ArchiveMediumIdentity:
        _require_exact_fields(document, _MEDIUM_FIELDS, "Archive Set medium metadata")
        if document.get("schema") != ARCHIVE_SET_MEDIUM_SCHEMA:
            raise ArchiveSetError("unsupported Archive Set medium metadata schema")
        return cls(
            archive_set_id=_required_text(document, "archive_set_id"),
            storage_id=_required_text(document, "storage_id"),
            volume_uuid=_required_text(document, "volume_uuid"),
            registered_relative_path=_required_text(
                document, "registered_relative_path"
            ),
            marker_nonce=_required_text(document, "marker_nonce"),
        )


@dataclass(frozen=True, slots=True)
class ArchiveSetEntry:
    archive_set_id: str
    storage_id: str
    chunk_id: str
    artifact_relative_path: str
    archive_manifest_relative_path: str
    archive_manifest_sha256: str
    stored_bytes: int
    stored_sha256: str
    source_manifest_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "schema": ARCHIVE_SET_ENTRY_SCHEMA,
            "archive_set_id": self.archive_set_id,
            "storage_id": self.storage_id,
            "chunk_id": self.chunk_id,
            "artifact_relative_path": self.artifact_relative_path,
            "archive_manifest_relative_path": self.archive_manifest_relative_path,
            "archive_manifest_sha256": self.archive_manifest_sha256,
            "stored_bytes": self.stored_bytes,
            "stored_sha256": self.stored_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
        }

    def canonical_bytes(self) -> bytes:
        self.validate()
        return _canonical_json(self.as_dict())

    def validate(self) -> None:
        _require_text(self.archive_set_id, "archive_set_id")
        _require_text(self.storage_id, "storage_id")
        _require_safe_segment(self.chunk_id, "chunk_id")
        _require_file_relative_path(
            self.artifact_relative_path, "artifact_relative_path"
        )
        _require_file_relative_path(
            self.archive_manifest_relative_path, "archive_manifest_relative_path"
        )
        for field in (
            "archive_manifest_sha256",
            "stored_sha256",
            "source_manifest_sha256",
        ):
            _require_sha256(getattr(self, field), field)
        if not isinstance(self.stored_bytes, int) or isinstance(self.stored_bytes, bool):
            raise ArchiveSetError("stored_bytes must be an integer")
        if self.stored_bytes < 0:
            raise ArchiveSetError("stored_bytes must not be negative")

    @classmethod
    def from_dict(cls, document: Mapping[str, object]) -> ArchiveSetEntry:
        _require_exact_fields(document, _ENTRY_FIELDS, "Archive Set entry")
        if document.get("schema") != ARCHIVE_SET_ENTRY_SCHEMA:
            raise ArchiveSetError("unsupported Archive Set entry schema")
        entry = cls(
            archive_set_id=_required_text(document, "archive_set_id"),
            storage_id=_required_text(document, "storage_id"),
            chunk_id=_required_text(document, "chunk_id"),
            artifact_relative_path=_required_text(document, "artifact_relative_path"),
            archive_manifest_relative_path=_required_text(
                document, "archive_manifest_relative_path"
            ),
            archive_manifest_sha256=_required_text(
                document, "archive_manifest_sha256"
            ),
            stored_bytes=document.get("stored_bytes"),  # type: ignore[arg-type]
            stored_sha256=_required_text(document, "stored_sha256"),
            source_manifest_sha256=_required_text(
                document, "source_manifest_sha256"
            ),
        )
        entry.validate()
        return entry


@dataclass(frozen=True, slots=True)
class ArchiveSetScan:
    identity: ArchiveMediumIdentity
    entries: tuple[ArchiveSetEntry, ...]


class ArchiveSetStore:
    """Read, bind, and commit one registered medium's Archive Set metadata."""

    def __init__(self, root: Path, identity: ArchiveMediumIdentity) -> None:
        self.root = _registered_root(root)
        self.identity = identity

    @classmethod
    def bind(
        cls,
        root: Path,
        *,
        archive_set_id: str,
        storage_id: str,
        volume_uuid: str,
        registered_relative_path: str,
        marker_nonce: str,
    ) -> ArchiveSetStore:
        identity = ArchiveMediumIdentity(
            archive_set_id=archive_set_id,
            storage_id=storage_id,
            volume_uuid=volume_uuid,
            registered_relative_path=registered_relative_path,
            marker_nonce=marker_nonce,
        )
        store = cls(root, identity)
        store._validate_registered_root()
        identity_path = store.identity_path
        if identity_path.is_symlink():
            raise ArchiveSetError("Archive Set identity file is a symbolic link")
        identity_existed = identity_path.exists()
        if identity_existed:
            existing = store._read_identity_file()
        else:
            published = _atomic_publish(identity_path, identity.canonical_bytes())
            existing = identity if published else store._read_identity_file()
        if existing != identity:
            raise ArchiveSetError("existing Archive Set identity conflicts")
        if identity_existed:
            fsync_directory(identity_path.parent)
        store._ensure_inventory_directories()
        return store

    @classmethod
    def open(cls, root: Path) -> ArchiveSetStore:
        identity = read_archive_medium_identity(root)
        return cls(root, identity)

    @property
    def identity_path(self) -> Path:
        return self.root / ARCHIVE_SET_MEDIUM_FILENAME

    @property
    def archive_set_directory(self) -> Path:
        return self.root / ARCHIVE_SET_DIRECTORY_NAME

    @property
    def entries_directory(self) -> Path:
        return self.archive_set_directory / ARCHIVE_SET_ENTRIES_DIRECTORY_NAME

    def read_identity(self) -> ArchiveMediumIdentity:
        self._validate_registered_root()
        identity = self._read_identity_file()
        if identity != self.identity:
            raise ArchiveSetError("Archive Set identity changed")
        return identity

    def commit_entry(self, entry: ArchiveSetEntry) -> ArchiveSetEntry:
        self.read_identity()
        entry.validate()
        if entry.archive_set_id != self.identity.archive_set_id:
            raise ArchiveSetError("entry archive_set_id does not match medium")
        if entry.storage_id != self.identity.storage_id:
            raise ArchiveSetError("entry storage_id does not match medium")
        self._ensure_inventory_directories()
        final = self.entries_directory / f"{entry.chunk_id}.json"
        if final.is_symlink():
            raise ArchiveSetError("Archive Set entry is a symbolic link")
        entry_existed = final.exists()
        if entry_existed:
            existing = _read_entry_file(final)
        else:
            published = _atomic_publish(final, entry.canonical_bytes())
            existing = entry if published else _read_entry_file(final)
        if existing == entry:
            if entry_existed:
                fsync_directory(final.parent)
            return existing
        raise ArchiveSetError("existing Archive Set entry conflicts")

    def read_entry(self, chunk_id: str) -> ArchiveSetEntry:
        _require_safe_segment(chunk_id, "chunk_id")
        self.read_identity()
        self._validate_inventory_directories(must_exist=False)
        path = self.entries_directory / f"{chunk_id}.json"
        if path.is_symlink():
            raise ArchiveSetError("Archive Set entry is a symbolic link")
        if not path.is_file():
            raise ArchiveSetError("Archive Set entry is missing")
        entry = _read_entry_file(path)
        self._validate_entry_binding(entry)
        return entry

    def scan(self) -> ArchiveSetScan:
        identity = self.read_identity()
        self._validate_inventory_directories(must_exist=False)
        if not self.entries_directory.exists():
            return ArchiveSetScan(identity, ())
        paths = sorted(self.entries_directory.iterdir(), key=lambda path: path.name)
        entries: list[ArchiveSetEntry] = []
        for path in paths:
            if path.is_symlink():
                raise ArchiveSetError("Archive Set inventory contains a symbolic link")
            if not path.is_file() or path.suffix != ".json":
                raise ArchiveSetError("Archive Set inventory contains an unsafe file")
            entry = _read_entry_file(path)
            expected_name = f"{entry.chunk_id}.json"
            if path.name != expected_name:
                raise ArchiveSetError("Archive Set entry filename does not match chunk_id")
            self._validate_entry_binding(entry)
            entries.append(entry)
        return ArchiveSetScan(identity, tuple(entries))

    def _read_identity_file(self) -> ArchiveMediumIdentity:
        return _read_identity_file(self.identity_path)

    def _validate_registered_root(self) -> None:
        try:
            validate_registered_root(
                self.root,
                volume_uuid=self.identity.volume_uuid,
                relative_path=self.identity.registered_relative_path,
                storage_id=self.identity.storage_id,
                marker_nonce=self.identity.marker_nonce,
            )
        except StorageRegistrationError as exc:
            raise ArchiveSetError(f"registered physical identity unavailable: {exc}") from exc

    def _ensure_inventory_directories(self) -> None:
        self._validate_registered_root()
        archive_set = self.archive_set_directory
        if archive_set.exists() and archive_set.is_symlink():
            raise ArchiveSetError("archive-set directory is a symbolic link")
        archive_set.mkdir(mode=0o700, exist_ok=True)
        _validate_direct_directory(archive_set, self.root, "archive-set")
        entries = self.entries_directory
        if entries.exists() and entries.is_symlink():
            raise ArchiveSetError("archive-set/entries directory is a symbolic link")
        entries.mkdir(mode=0o700, exist_ok=True)
        _validate_direct_directory(entries, archive_set, "entries")
        fsync_directory(entries)
        fsync_directory(archive_set)
        fsync_directory(self.root)

    def _validate_inventory_directories(self, *, must_exist: bool) -> None:
        archive_set = self.archive_set_directory
        if not archive_set.exists():
            if must_exist:
                raise ArchiveSetError("archive-set directory is missing")
            return
        if archive_set.is_symlink():
            raise ArchiveSetError("archive-set directory is a symbolic link")
        _validate_direct_directory(archive_set, self.root, "archive-set")
        entries = self.entries_directory
        if not entries.exists():
            if must_exist:
                raise ArchiveSetError("archive-set/entries directory is missing")
            return
        if entries.is_symlink():
            raise ArchiveSetError("archive-set/entries directory is a symbolic link")
        _validate_direct_directory(entries, archive_set, "entries")

    def _validate_entry_binding(self, entry: ArchiveSetEntry) -> None:
        if entry.archive_set_id != self.identity.archive_set_id:
            raise ArchiveSetError("entry archive_set_id does not match medium")
        if entry.storage_id != self.identity.storage_id:
            raise ArchiveSetError("entry storage_id does not match medium")


# This name describes the same medium-local store and keeps the public shape
# aligned with the Archive Set contract without introducing another abstraction.
ArchiveSetMedium = ArchiveSetStore


def read_archive_medium_identity(root: Path) -> ArchiveMediumIdentity:
    """Read and validate one medium without changing its filesystem."""

    registered_root = _registered_root(root)
    identity = _read_identity_file(registered_root / ARCHIVE_SET_MEDIUM_FILENAME)
    store = ArchiveSetStore(registered_root, identity)
    store._validate_registered_root()
    return identity


def scan_archive_medium(root: Path) -> ArchiveSetScan:
    """Read one attached medium and all immutable inventory entries."""

    return ArchiveSetStore.open(root).scan()


class ArchiveSetIndex:
    """Explicit-path convenience index rebuildable from media-local evidence."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path).expanduser().resolve()

    def rebuild(self, roots: Iterable[Path]) -> dict[str, int]:
        supplied_roots = tuple(Path(root) for root in roots)
        resolved_roots = tuple(_registered_root(root) for root in supplied_roots)
        self._validate_index_outside_media(resolved_roots)
        scans = [scan_archive_medium(root) for root in resolved_roots]
        media: dict[str, ArchiveMediumIdentity] = {}
        artifacts: dict[tuple[str, str], tuple[ArchiveMediumIdentity, ArchiveSetEntry]] = {}
        for scan in scans:
            storage_id = scan.identity.storage_id
            previous_media = media.get(storage_id)
            if previous_media is not None and previous_media != scan.identity:
                raise ArchiveSetError("storage_id is bound to conflicting media identities")
            if previous_media is not None:
                raise ArchiveSetError("the same physical medium was supplied twice")
            media[storage_id] = scan.identity
            for entry in scan.entries:
                key = (entry.archive_set_id, entry.chunk_id)
                previous = artifacts.get(key)
                if previous is not None:
                    previous_identity, previous_entry = previous
                    if (
                        previous_identity.storage_id != scan.identity.storage_id
                        or previous_entry.artifact_relative_path
                        != entry.artifact_relative_path
                        or previous_entry.archive_manifest_sha256
                        != entry.archive_manifest_sha256
                        or previous_entry.stored_sha256 != entry.stored_sha256
                        or previous_entry.source_manifest_sha256
                        != entry.source_manifest_sha256
                    ):
                        raise ArchiveSetError(
                            "Archive Set chunk collision has conflicting physical identity"
                        )
                    raise ArchiveSetError("Archive Set chunk is claimed by multiple media")
                artifacts[key] = (scan.identity, entry)

        self._validate_index_outside_media(resolved_roots)
        self._initialize()
        with sqlite3.connect(self.path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN")
            try:
                connection.execute("DELETE FROM archive_artifacts")
                connection.execute("DELETE FROM archive_media")
                connection.execute("DELETE FROM archive_sets")
                for identity in sorted(media.values(), key=lambda item: item.storage_id):
                    connection.execute(
                        "INSERT INTO archive_sets(archive_set_id) VALUES (?) "
                        "ON CONFLICT DO NOTHING",
                        (identity.archive_set_id,),
                    )
                    connection.execute(
                        "INSERT INTO archive_media VALUES (?, ?, ?, ?, ?)",
                        (
                            identity.storage_id,
                            identity.archive_set_id,
                            identity.volume_uuid,
                            identity.registered_relative_path,
                            identity.marker_nonce,
                        ),
                    )
                for (archive_set_id, chunk_id), (identity, entry) in sorted(
                    artifacts.items()
                ):
                    connection.execute(
                        "INSERT INTO archive_artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            archive_set_id,
                            chunk_id,
                            identity.storage_id,
                            entry.artifact_relative_path,
                            entry.archive_manifest_relative_path,
                            entry.archive_manifest_sha256,
                            entry.stored_bytes,
                            entry.stored_sha256,
                            entry.source_manifest_sha256,
                        ),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return {
            "archive_sets": len({identity.archive_set_id for identity in media.values()}),
            "media": len(media),
            "artifacts": len(artifacts),
        }

    def archive_sets(self) -> list[dict[str, object]]:
        return self._rows("SELECT archive_set_id FROM archive_sets ORDER BY archive_set_id")

    def media(self, *, archive_set_id: str | None = None) -> list[dict[str, object]]:
        query = "SELECT * FROM archive_media"
        parameters: tuple[object, ...] = ()
        if archive_set_id is not None:
            query += " WHERE archive_set_id = ?"
            parameters = (archive_set_id,)
        return self._rows(query + " ORDER BY storage_id", parameters)

    def artifacts(
        self,
        *,
        archive_set_id: str | None = None,
        storage_id: str | None = None,
        chunk_id: str | None = None,
    ) -> list[dict[str, object]]:
        clauses: list[str] = []
        parameters: list[object] = []
        for name, value in (
            ("archive_set_id", archive_set_id),
            ("storage_id", storage_id),
            ("chunk_id", chunk_id),
        ):
            if value is not None:
                clauses.append(f"{name} = ?")
                parameters.append(value)
        query = "SELECT * FROM archive_artifacts"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        return self._rows(query + " ORDER BY archive_set_id, chunk_id", tuple(parameters))

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as connection:
            connection.executescript(
                """
                PRAGMA foreign_keys = ON;
                CREATE TABLE IF NOT EXISTS archive_sets (
                    archive_set_id TEXT PRIMARY KEY
                );
                CREATE TABLE IF NOT EXISTS archive_media (
                    storage_id TEXT PRIMARY KEY,
                    archive_set_id TEXT NOT NULL REFERENCES archive_sets(archive_set_id),
                    volume_uuid TEXT NOT NULL,
                    registered_relative_path TEXT NOT NULL,
                    marker_nonce TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS archive_artifacts (
                    archive_set_id TEXT NOT NULL REFERENCES archive_sets(archive_set_id),
                    chunk_id TEXT NOT NULL,
                    storage_id TEXT NOT NULL REFERENCES archive_media(storage_id),
                    artifact_relative_path TEXT NOT NULL,
                    archive_manifest_relative_path TEXT NOT NULL,
                    archive_manifest_sha256 TEXT NOT NULL,
                    stored_bytes INTEGER NOT NULL,
                    stored_sha256 TEXT NOT NULL,
                    source_manifest_sha256 TEXT NOT NULL,
                    PRIMARY KEY (archive_set_id, chunk_id),
                    UNIQUE (storage_id, chunk_id)
                );
                """
            )

    def _validate_index_outside_media(self, roots: tuple[Path, ...]) -> None:
        resolved_index = self.path.resolve()
        if any(
            resolved_index == root or resolved_index.is_relative_to(root)
            for root in roots
        ):
            raise ArchiveSetError("workspace index path resolves inside archive media")

    def _rows(
        self, query: str, parameters: tuple[object, ...] = ()
    ) -> list[dict[str, object]]:
        try:
            with sqlite3.connect(f"{self.path.as_uri()}?mode=ro", uri=True) as connection:
                connection.row_factory = sqlite3.Row
                return [dict(row) for row in connection.execute(query, parameters)]
        except sqlite3.OperationalError:
            if not self.path.exists():
                return []
            raise


def rebuild_archive_set_index(path: Path, roots: Iterable[Path]) -> dict[str, int]:
    """Rebuild an explicit workspace index from supplied attached media."""

    return ArchiveSetIndex(path).rebuild(roots)


def _registered_root(root: Path) -> Path:
    selected = Path(root).expanduser()
    if selected.is_symlink():
        raise ArchiveSetError("registered storage directory is a symbolic link")
    resolved = selected.resolve()
    if not resolved.is_dir():
        raise ArchiveSetError("registered storage directory is unavailable")
    return resolved


def _read_identity_file(path: Path) -> ArchiveMediumIdentity:
    if path.is_symlink():
        raise ArchiveSetError("Archive Set identity file is a symbolic link")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ArchiveSetError(f"invalid Archive Set identity: {exc}") from exc
    if not isinstance(document, dict):
        raise ArchiveSetError("invalid Archive Set identity: expected object")
    return ArchiveMediumIdentity.from_dict(document)


def _read_entry_file(path: Path) -> ArchiveSetEntry:
    if path.is_symlink():
        raise ArchiveSetError("Archive Set entry is a symbolic link")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ArchiveSetError(f"invalid Archive Set entry: {exc}") from exc
    if not isinstance(document, dict):
        raise ArchiveSetError("invalid Archive Set entry: expected object")
    return ArchiveSetEntry.from_dict(document)


def _atomic_publish(path: Path, payload: bytes) -> bool:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            remaining = memoryview(payload)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise ArchiveSetError("Archive Set metadata short write")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        published = _publish_no_clobber(temporary, path)
        fsync_directory(path.parent)
        return published
    except OSError as exc:
        raise ArchiveSetError(f"cannot commit Archive Set metadata: {exc}") from exc
    finally:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


def _publish_no_clobber(source: Path, destination: Path) -> bool:
    """Atomically rename *source* without replacing an existing destination."""

    if source.parent != destination.parent:
        raise ArchiveSetError("Archive Set metadata publication must stay in one directory")
    if sys.platform == "linux":
        return _posix_exclusive_rename(
            source, destination, function_name="renameat2", flag=0x00000001
        )
    if sys.platform == "darwin":
        return _posix_exclusive_rename(
            source, destination, function_name="renameatx_np", flag=0x00000004
        )
    if os.name == "nt":
        try:
            os.rename(source, destination)
        except FileExistsError:
            return False
        return True
    raise ArchiveSetError(
        f"atomic no-clobber publication is unsupported on platform {sys.platform!r}"
    )


def _posix_exclusive_rename(
    source: Path,
    destination: Path,
    *,
    function_name: str,
    flag: int,
) -> bool:
    directory_descriptor = os.open(source.parent, os.O_RDONLY)
    try:
        library = ctypes.CDLL(None, use_errno=True)
        try:
            rename = getattr(library, function_name)
        except AttributeError as exc:
            raise ArchiveSetError(
                f"atomic no-clobber publication primitive {function_name} is unavailable"
            ) from exc
        rename.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        rename.restype = ctypes.c_int
        ctypes.set_errno(0)
        result = rename(
            directory_descriptor,
            os.fsencode(source.name),
            directory_descriptor,
            os.fsencode(destination.name),
            flag,
        )
        if result == 0:
            return True
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            return False
        raise OSError(error, os.strerror(error), str(destination))
    finally:
        os.close(directory_descriptor)


def _validate_direct_directory(path: Path, parent: Path, name: str) -> None:
    resolved_parent = parent.resolve()
    resolved = path.resolve()
    if resolved.parent != resolved_parent or resolved.name != name or not resolved.is_dir():
        raise ArchiveSetError(f"{name} directory resolves outside registered namespace")


def _canonical_json(document: Mapping[str, object]) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def _require_exact_fields(
    document: Mapping[str, object], fields: tuple[str, ...], label: str
) -> None:
    if set(document) != set(fields):
        raise ArchiveSetError(f"{label} fields are not exact")


def _required_text(document: Mapping[str, object], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise ArchiveSetError(f"Archive Set metadata missing {field}")
    return value


def _require_text(value: object, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise ArchiveSetError(f"{field} must be non-empty text")


def _require_relative_path(value: str, field: str) -> None:
    _require_text(value, field)
    path = PurePosixPath(value)
    if (
        value != path.as_posix()
        or path.is_absolute()
        or "\\" in value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ArchiveSetError(f"{field} must be a safe relative path")


def _require_file_relative_path(value: str, field: str) -> None:
    _require_relative_path(value, field)
    if value == ".":
        raise ArchiveSetError(f"{field} must be a safe relative file path")


def _require_safe_segment(value: str, field: str) -> None:
    _require_text(value, field)
    if (
        value in {".", ".."}
        or "/" in value
        or "\\" in value
        or Path(value).name != value
    ):
        raise ArchiveSetError(f"{field} must be a safe path segment")


def _require_sha256(value: str, field: str) -> None:
    _require_text(value, field)
    if len(value) != _HEX_SHA256_LENGTH or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ArchiveSetError(f"{field} must be a lowercase SHA-256 hex digest")
