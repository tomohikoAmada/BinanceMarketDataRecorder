"""Read-only identity selection for immutable sealed Raw sources.

This module deliberately stops at source identity.  It does not reserve an
archive transaction, change Catalog state, write files, or open a transport.
Later archive-client milestones can bind the returned descriptor and exact
manifest bytes to their own integrity and receipt workflows.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from ..spool.seal import MANIFEST_SCHEMA_VERSION, SealError, validate_sealed_artifact
from ..storage.catalog import Catalog, ChunkState
from ..storage.layout import StorageLayout

REMOTE_SOURCE_DESCRIPTOR_SCHEMA = "remote-source-descriptor.v1"
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
class RemoteSourceSelection:
    """Validated source handles and immutable identity material."""

    descriptor: RemoteSourceDescriptor
    descriptor_bytes: bytes
    descriptor_sha256: str
    manifest_bytes: bytes
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
        return self._select_from_snapshot(chunk_id, row, archive)

    def select_oldest(self) -> RemoteSourceSelection | None:
        row = self.catalog.oldest_chunk_in_states(ChunkState.SEALED)
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

        sealed_relative = self._required_catalog_text(row, "sealed_path")
        manifest_relative = self._required_catalog_text(row, "manifest_path")
        sealed_path = self._resolve_recorder_path(
            sealed_relative, self.layout.sealed, "Catalog sealed_path"
        )
        manifest_path = self._resolve_recorder_path(
            manifest_relative, self.layout.manifests, "Catalog manifest_path"
        )
        if not sealed_path.is_file():
            raise RemoteSourceError(
                f"source artifact missing: {sealed_relative}"
            )
        if not manifest_path.is_file():
            raise RemoteSourceError(
                f"manifest unreadable or missing: {manifest_relative}"
            )

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

        final_row, final_archive = self.catalog.chunk_archive_snapshot(chunk_id)
        if (
            final_row is None
            or final_archive is not None
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
            resolved = (self.layout.root / candidate).resolve()
            directory_resolved = directory.resolve()
            if resolved.parent != directory_resolved:
                raise RemoteSourceError(
                    f"invalid {label}: path escapes its exact Recorder directory"
                )
        except OSError as exc:
            raise RemoteSourceError(f"invalid {label}: cannot resolve path") from exc
        return resolved

    @staticmethod
    def _read_manifest(path: Path) -> bytes:
        try:
            return path.read_bytes()
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
        if manifest_path != sealed_path:
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
