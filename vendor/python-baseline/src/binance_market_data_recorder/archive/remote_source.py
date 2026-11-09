"""Read-only identity selection for immutable sealed Raw sources.

This module deliberately stops at source identity.  It does not reserve an
archive transaction, change Catalog state, write files, or open a transport.
Later archive-client milestones can bind the returned descriptor and exact
manifest bytes to their own integrity and receipt workflows.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from ..spool.seal import MANIFEST_SCHEMA_VERSION, SealError, validate_sealed_artifact
from ..storage.catalog import Catalog, ChunkState
from ..storage.layout import StorageLayout

REMOTE_SOURCE_DESCRIPTOR_SCHEMA = "remote-source-descriptor.v1"
_DESCRIPTOR_FIELDS = (
    "descriptor_schema_version",
    "chunk_id",
    "market",
    "stream",
    "source_relative_path",
    "stored_bytes",
    "stored_sha256",
    "source_manifest_relative_path",
    "source_manifest_sha256",
    "manifest_schema_version",
    "chunk_schema_version",
    "envelope_schema_version",
)
_SOURCE_IDENTITY_FIELDS = (
    "chunk_id",
    "sealed_path",
    "manifest_path",
    "stored_bytes",
    "stored_sha256",
    "record_count",
    "uncompressed_bytes",
    "uncompressed_sha256",
)


class RemoteSourceError(RuntimeError):
    """A sealed Raw source cannot be selected safely for remote export."""


@dataclass(frozen=True, slots=True)
class RemoteSourceDescriptor:
    """Deterministic, transport-neutral identity for one sealed Raw source."""

    descriptor_schema_version: str
    chunk_id: str
    market: str
    stream: str
    source_relative_path: str
    stored_bytes: int
    stored_sha256: str
    source_manifest_relative_path: str
    source_manifest_sha256: str
    manifest_schema_version: str
    chunk_schema_version: str
    envelope_schema_version: str

    def document(self) -> dict[str, object]:
        return {
            "descriptor_schema_version": self.descriptor_schema_version,
            "chunk_id": self.chunk_id,
            "market": self.market,
            "stream": self.stream,
            "source_relative_path": self.source_relative_path,
            "stored_bytes": self.stored_bytes,
            "stored_sha256": self.stored_sha256,
            "source_manifest_relative_path": self.source_manifest_relative_path,
            "source_manifest_sha256": self.source_manifest_sha256,
            "manifest_schema_version": self.manifest_schema_version,
            "chunk_schema_version": self.chunk_schema_version,
            "envelope_schema_version": self.envelope_schema_version,
        }


@dataclass(frozen=True, slots=True)
class RemoteSourceIdentity:
    """Portable immutable identity material with no VPS-local path handles."""

    descriptor: RemoteSourceDescriptor
    descriptor_bytes: bytes
    descriptor_sha256: str
    manifest_bytes: bytes


@dataclass(frozen=True, slots=True)
class RemoteSourceSelection(RemoteSourceIdentity):
    """VPS-local extension of portable source identity with validated paths."""

    manifest_path: Path
    sealed_path: Path

    @property
    def source_manifest_path(self) -> Path:
        return self.manifest_path

    @property
    def source_path(self) -> Path:
        return self.sealed_path


def canonical_descriptor_bytes(
    descriptor: RemoteSourceDescriptor,
) -> bytes:
    """Serialize a descriptor using the M22.1 canonical JSON profile."""

    return (
        json.dumps(
            descriptor.document(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def descriptor_sha256(descriptor_bytes: bytes) -> str:
    """Return the digest of exact canonical descriptor bytes."""

    return hashlib.sha256(descriptor_bytes).hexdigest()


def remote_source_descriptor_from_bytes(body: bytes) -> RemoteSourceDescriptor:
    """Parse only exact canonical ``remote-source-descriptor.v1`` bytes."""

    if not isinstance(body, bytes):
        raise RemoteSourceError("source descriptor must be bytes")
    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RemoteSourceError("source descriptor is not valid UTF-8 JSON") from exc
    if not isinstance(decoded, dict) or set(decoded) != set(_DESCRIPTOR_FIELDS):
        raise RemoteSourceError("source descriptor fields are not exact")
    document = cast(dict[str, object], decoded)
    for field in _DESCRIPTOR_FIELDS:
        value = document[field]
        if field == "stored_bytes":
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise RemoteSourceError("source descriptor stored_bytes is invalid")
        elif not isinstance(value, str) or not value:
            raise RemoteSourceError(f"source descriptor {field} is invalid")
    try:
        descriptor = RemoteSourceDescriptor(**document)  # type: ignore[arg-type]
    except TypeError as exc:
        raise RemoteSourceError("source descriptor fields are invalid") from exc
    if descriptor.descriptor_schema_version != REMOTE_SOURCE_DESCRIPTOR_SCHEMA:
        raise RemoteSourceError("unsupported source descriptor schema")
    if canonical_descriptor_bytes(descriptor) != body:
        raise RemoteSourceError("source descriptor is not canonical")
    _require_canonical_uuid(descriptor.chunk_id, "chunk_id")
    _require_sha256(descriptor.stored_sha256, "stored_sha256")
    _require_sha256(descriptor.source_manifest_sha256, "source_manifest_sha256")
    _require_relative_path(descriptor.source_relative_path, "source_relative_path")
    _require_relative_path(
        descriptor.source_manifest_relative_path,
        "source_manifest_relative_path",
    )
    return descriptor


def validate_remote_source_identity(identity: RemoteSourceIdentity) -> None:
    """Validate portable descriptor, digest, and exact retained manifest bytes."""

    descriptor = remote_source_descriptor_from_bytes(identity.descriptor_bytes)
    if descriptor != identity.descriptor:
        raise RemoteSourceError("source descriptor object/bytes mismatch")
    if descriptor_sha256(identity.descriptor_bytes) != identity.descriptor_sha256:
        raise RemoteSourceError("source descriptor digest mismatch")
    if hashlib.sha256(identity.manifest_bytes).hexdigest() != (
        descriptor.source_manifest_sha256
    ):
        raise RemoteSourceError("source manifest digest mismatch")
    manifest = RemoteSourceExporter._decode_manifest(identity.manifest_bytes)
    expected = {
        "manifest_schema_version": descriptor.manifest_schema_version,
        "chunk_id": descriptor.chunk_id,
        "market": descriptor.market,
        "stream": descriptor.stream,
        "relative_path": descriptor.source_relative_path,
        "stored_bytes": descriptor.stored_bytes,
        "stored_sha256": descriptor.stored_sha256,
        "chunk_schema_version": descriptor.chunk_schema_version,
        "envelope_schema_version": descriptor.envelope_schema_version,
    }
    if any(manifest.get(field) != value for field, value in expected.items()):
        raise RemoteSourceError("source descriptor/manifest identity mismatch")


def portable_source_identity(selection: RemoteSourceSelection) -> RemoteSourceIdentity:
    """Project a validated VPS-local selection to its portable identity."""

    identity = RemoteSourceIdentity(
        descriptor=selection.descriptor,
        descriptor_bytes=selection.descriptor_bytes,
        descriptor_sha256=selection.descriptor_sha256,
        manifest_bytes=selection.manifest_bytes,
    )
    validate_remote_source_identity(identity)
    return identity


class RemoteSourceExporter:
    """Select and fully validate sealed Raw sources without mutating state."""

    def __init__(self, *, layout: StorageLayout, catalog: Catalog) -> None:
        self.layout = layout
        self.catalog = catalog

    def select_chunk(self, chunk_id: str) -> RemoteSourceSelection:
        if not isinstance(chunk_id, str) or not chunk_id:
            raise RemoteSourceError("chunk missing: chunk_id must be non-empty text")
        row, archive = self.catalog.chunk_archive_snapshot(chunk_id)
        if row is None:
            raise RemoteSourceError(f"chunk missing: {chunk_id}")
        remote = self.catalog.remote_archive_transaction_for_chunk(chunk_id)
        return self._select_from_snapshot(chunk_id, row, archive, remote)

    def select_oldest(self) -> RemoteSourceSelection | None:
        row = self.catalog.oldest_unowned_sealed_chunk()
        if row is None:
            return None
        chunk_id = row.get("chunk_id")
        if not isinstance(chunk_id, str) or not chunk_id:
            raise RemoteSourceError("Catalog SEALED row has invalid chunk_id")
        return self.select_chunk(chunk_id)

    def _select_from_snapshot(
        self,
        chunk_id: str,
        row: dict[str, object],
        archive: dict[str, object] | None,
        remote: dict[str, object] | None,
        *,
        permitted_remote_receipt_id: str | None = None,
    ) -> RemoteSourceSelection:
        state = self._chunk_state(row)
        if state is not ChunkState.SEALED:
            raise RemoteSourceError(
                f"source not eligible: chunk {chunk_id} is {state.value}"
            )
        if archive is not None:
            raise RemoteSourceError(
                "Catalog/archive-state contradiction: SEALED chunk has "
                f"archive transaction {archive.get('transaction_id')}"
            )
        if remote is not None and remote.get("receipt_id") != permitted_remote_receipt_id:
            raise RemoteSourceError(
                "source already has remote archive ownership: "
                f"{remote.get('receipt_id')}"
            )

        sealed_relative = self._required_catalog_text(row, "sealed_path")
        manifest_relative = self._required_catalog_text(row, "manifest_path")
        sealed_path = self._resolve_recorder_path(
            sealed_relative, self.layout.sealed, "Catalog sealed_path"
        )
        manifest_path = self._resolve_recorder_path(
            manifest_relative, self.layout.manifests, "Catalog manifest_path"
        )
        source_identity = _require_no_follow_regular(sealed_path, "source artifact")
        _require_no_follow_regular(manifest_path, "source manifest")

        manifest_bytes = self._read_manifest(manifest_path)
        manifest = self._decode_manifest(manifest_bytes)
        self._validate_manifest_identity(
            chunk_id=chunk_id,
            row=row,
            manifest=manifest,
            sealed_path=sealed_path,
        )
        try:
            validate_sealed_artifact(sealed_path, manifest)
        except (OSError, KeyError, TypeError, ValueError, SealError) as exc:
            raise RemoteSourceError(
                f"source artifact validation failure: {exc}"
            ) from exc
        if _require_no_follow_regular(
            sealed_path, "source artifact"
        ) != source_identity:
            raise RemoteSourceError("source artifact changed during validation")

        final_row, final_archive = self.catalog.chunk_archive_snapshot(chunk_id)
        final_remote = self.catalog.remote_archive_transaction_for_chunk(chunk_id)
        if (
            final_row is None
            or final_archive is not None
            or (
                final_remote is not None
                and final_remote.get("receipt_id") != permitted_remote_receipt_id
            )
            or self._chunk_state(final_row) is not ChunkState.SEALED
            or self._source_identity(final_row) != self._source_identity(row)
        ):
            raise RemoteSourceError(
                f"source state changed during selection: {chunk_id}"
            )

        descriptor = RemoteSourceDescriptor(
            descriptor_schema_version=REMOTE_SOURCE_DESCRIPTOR_SCHEMA,
            chunk_id=chunk_id,
            market=self._required_manifest_text(manifest, "market"),
            stream=self._required_manifest_text(manifest, "stream"),
            source_relative_path=sealed_relative,
            stored_bytes=self._required_manifest_int(manifest, "stored_bytes"),
            stored_sha256=self._required_manifest_text(manifest, "stored_sha256"),
            source_manifest_relative_path=manifest_relative,
            source_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            manifest_schema_version=self._required_manifest_text(
                manifest, "manifest_schema_version"
            ),
            chunk_schema_version=self._required_manifest_text(
                manifest, "chunk_schema_version"
            ),
            envelope_schema_version=self._required_manifest_text(
                manifest, "envelope_schema_version"
            ),
        )
        canonical_bytes = canonical_descriptor_bytes(descriptor)
        return RemoteSourceSelection(
            descriptor=descriptor,
            descriptor_bytes=canonical_bytes,
            descriptor_sha256=descriptor_sha256(canonical_bytes),
            manifest_bytes=manifest_bytes,
            manifest_path=manifest_path,
            sealed_path=sealed_path,
        )

    @staticmethod
    def _chunk_state(row: Mapping[str, object]) -> ChunkState:
        value = row.get("state")
        try:
            return ChunkState(str(value))
        except ValueError as exc:
            raise RemoteSourceError(f"source has unknown Catalog state: {value}") from exc

    @staticmethod
    def _source_identity(row: Mapping[str, object]) -> tuple[object, ...]:
        return tuple(row.get(field) for field in _SOURCE_IDENTITY_FIELDS)

    @staticmethod
    def _required_catalog_text(row: Mapping[str, object], name: str) -> str:
        value = row.get(name)
        if not isinstance(value, str) or not value:
            raise RemoteSourceError(f"invalid Catalog {name}")
        return value

    @staticmethod
    def _required_manifest_text(
        manifest: Mapping[str, object], name: str
    ) -> str:
        value = manifest.get(name)
        if not isinstance(value, str) or not value:
            raise RemoteSourceError(f"manifest field {name} must be non-empty text")
        return value

    @staticmethod
    def _required_manifest_int(
        manifest: Mapping[str, object], name: str
    ) -> int:
        value = manifest.get(name)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RemoteSourceError(f"manifest field {name} must be a non-negative integer")
        return value

    def _resolve_recorder_path(
        self, relative_path: str, directory: Path, label: str
    ) -> Path:
        candidate = Path(relative_path)
        if candidate.is_absolute():
            raise RemoteSourceError(f"invalid {label}: path must be Recorder-relative")
        try:
            lexical = self.layout.root / candidate
            resolved = lexical.resolve()
            directory_resolved = directory.resolve()
            if directory.is_symlink() or resolved.parent != directory_resolved:
                raise RemoteSourceError(
                    f"invalid {label}: path escapes its exact Recorder directory"
                )
        except OSError as exc:
            raise RemoteSourceError(f"invalid {label}: cannot resolve path") from exc
        return lexical

    @staticmethod
    def _read_manifest(path: Path) -> bytes:
        try:
            return _read_no_follow_regular(path)
        except OSError as exc:
            raise RemoteSourceError(f"manifest unreadable: {path}") from exc

    @staticmethod
    def _decode_manifest(manifest_bytes: bytes) -> dict[str, object]:
        try:
            decoded = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RemoteSourceError("manifest unreadable or invalid JSON") from exc
        if not isinstance(decoded, dict):
            raise RemoteSourceError("manifest unreadable or invalid JSON object")
        manifest = cast(dict[str, object], decoded)
        schema = manifest.get("manifest_schema_version")
        if schema != MANIFEST_SCHEMA_VERSION:
            raise RemoteSourceError(
                f"unsupported manifest schema: {schema!r}"
            )
        return manifest

    def _validate_manifest_identity(
        self,
        *,
        chunk_id: str,
        row: Mapping[str, object],
        manifest: Mapping[str, object],
        sealed_path: Path,
    ) -> None:
        manifest_chunk_id = self._required_manifest_text(manifest, "chunk_id")
        if manifest_chunk_id != chunk_id or row.get("chunk_id") != chunk_id:
            raise RemoteSourceError("manifest/Catalog chunk_id mismatch")

        manifest_stored_bytes = self._required_manifest_int(manifest, "stored_bytes")
        catalog_stored_bytes = row.get("stored_bytes")
        if (
            not isinstance(catalog_stored_bytes, int)
            or isinstance(catalog_stored_bytes, bool)
            or manifest_stored_bytes != catalog_stored_bytes
        ):
            raise RemoteSourceError("manifest/Catalog stored_bytes mismatch")

        manifest_stored_sha256 = self._required_manifest_text(
            manifest, "stored_sha256"
        )
        catalog_stored_sha256 = row.get("stored_sha256")
        if manifest_stored_sha256 != catalog_stored_sha256:
            raise RemoteSourceError("manifest/Catalog stored_sha256 mismatch")

        manifest_relative_path = self._required_manifest_text(
            manifest, "relative_path"
        )
        if manifest_relative_path != str(row.get("sealed_path")):
            raise RemoteSourceError(
                "manifest relative_path does not match Catalog sealed_path"
            )
        if Path(manifest_relative_path).is_absolute():
            raise RemoteSourceError("manifest relative_path must be Recorder-relative")
        try:
            manifest_path = (self.layout.root / manifest_relative_path).resolve()
        except OSError as exc:
            raise RemoteSourceError("manifest relative_path cannot be resolved") from exc
        if manifest_path != sealed_path.resolve():
            raise RemoteSourceError(
                "manifest relative_path does not identify Catalog sealed_path"
            )

        for field in (
            "market",
            "stream",
            "manifest_schema_version",
            "chunk_schema_version",
            "envelope_schema_version",
        ):
            self._required_manifest_text(manifest, field)


def revalidate_remote_source_selection(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    selection: RemoteSourceSelection,
    permitted_remote_receipt_id: str | None = None,
) -> RemoteSourceSelection:
    """Re-run M22.1-strength validation and require the exact caller selection."""

    chunk_id = selection.descriptor.chunk_id
    row, archive, remote = catalog.source_lifecycle_snapshot(chunk_id)
    if row is None:
        raise RemoteSourceError(f"chunk missing: {chunk_id}")
    current = RemoteSourceExporter(layout=layout, catalog=catalog)._select_from_snapshot(
        chunk_id,
        row,
        archive,
        remote,
        permitted_remote_receipt_id=permitted_remote_receipt_id,
    )
    if current != selection:
        raise RemoteSourceError("source selection no longer matches current exact source")
    return current


def descriptor_from_retained_manifest(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    row: Mapping[str, object],
    manifest_bytes: bytes,
) -> RemoteSourceDescriptor:
    """Reconstruct the exact M22.1 descriptor without requiring Raw presence."""

    chunk_id = RemoteSourceExporter._required_catalog_text(row, "chunk_id")
    exporter = RemoteSourceExporter(layout=layout, catalog=catalog)
    manifest = exporter._decode_manifest(manifest_bytes)
    sealed_relative = exporter._required_catalog_text(row, "sealed_path")
    manifest_relative = exporter._required_catalog_text(row, "manifest_path")
    sealed_path = exporter._resolve_recorder_path(
        sealed_relative, layout.sealed, "Catalog sealed_path"
    )
    exporter._validate_manifest_identity(
        chunk_id=chunk_id, row=row, manifest=manifest, sealed_path=sealed_path
    )
    return RemoteSourceDescriptor(
        descriptor_schema_version=REMOTE_SOURCE_DESCRIPTOR_SCHEMA,
        chunk_id=chunk_id,
        market=exporter._required_manifest_text(manifest, "market"),
        stream=exporter._required_manifest_text(manifest, "stream"),
        source_relative_path=sealed_relative,
        stored_bytes=exporter._required_manifest_int(manifest, "stored_bytes"),
        stored_sha256=exporter._required_manifest_text(manifest, "stored_sha256"),
        source_manifest_relative_path=manifest_relative,
        source_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        manifest_schema_version=exporter._required_manifest_text(
            manifest, "manifest_schema_version"
        ),
        chunk_schema_version=exporter._required_manifest_text(
            manifest, "chunk_schema_version"
        ),
        envelope_schema_version=exporter._required_manifest_text(
            manifest, "envelope_schema_version"
        ),
    )


def _read_no_follow_regular(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise OSError("safe no-follow file open is unavailable")
    descriptor = os.open(path, flags | no_follow)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError("filesystem object is not a regular file")
        blocks: list[bytes] = []
        while block := os.read(descriptor, 1024 * 1024):
            blocks.append(block)
        return b"".join(blocks)
    finally:
        os.close(descriptor)


def _require_no_follow_regular(path: Path, label: str) -> tuple[int, int, int, int]:
    try:
        no_follow = getattr(os, "O_NOFOLLOW", None)
        if no_follow is None:
            raise OSError("safe no-follow file open is unavailable")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | no_follow,
        )
        try:
            status = os.fstat(descriptor)
            if not stat.S_ISREG(status.st_mode):
                raise RemoteSourceError(f"{label} is not a regular file")
            return (
                status.st_dev,
                status.st_ino,
                status.st_size,
                status.st_mtime_ns,
            )
        finally:
            os.close(descriptor)
    except (OSError, TypeError) as exc:
        raise RemoteSourceError(f"{label} missing or unsafe: {path}") from exc


def _require_canonical_uuid(value: str, field: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise RemoteSourceError(f"{field} must be a canonical UUID") from exc
    if str(parsed) != value:
        raise RemoteSourceError(f"{field} must be a canonical UUID")


def _require_sha256(value: str, field: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise RemoteSourceError(f"{field} must be a lowercase SHA-256 digest")


def _require_relative_path(value: str, field: str) -> None:
    candidate = Path(value)
    if (
        candidate.is_absolute()
        or "\\" in value
        or "//" in value
        or any(part in {"", ".", ".."} for part in candidate.parts)
        or candidate.as_posix() != value
    ):
        raise RemoteSourceError(f"{field} must be a canonical relative path")
