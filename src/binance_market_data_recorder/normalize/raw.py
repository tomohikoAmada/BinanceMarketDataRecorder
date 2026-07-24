"""Verified location-independent reading of immutable sealed Raw chunks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import google_crc32c
import zstandard

from ..domain.event import EventEnvelope
from ..spool.format import (
    FRAME_PREFIX,
    FRAME_PREFIX_WITHOUT_CRC,
    ChunkFormatError,
    decode_chunk_header,
    decode_envelope,
)
from ..spool.seal import MANIFEST_SCHEMA_VERSION, validate_sealed_artifact
from ..storage.catalog import ArchiveState, Catalog
from ..storage.layout import StorageLayout


class RawSourceError(RuntimeError):
    """A selected immutable Raw source cannot be verified or decoded."""


@dataclass(frozen=True, slots=True)
class SourceChunk:
    chunk_id: str
    artifact_path: Path
    manifest_path: Path
    manifest_sha256: str
    manifest: dict[str, object]

    @property
    def uncompressed_sha256(self) -> str:
        return str(self.manifest["uncompressed_sha256"])


@dataclass(frozen=True, slots=True)
class SourceRecord:
    chunk: SourceChunk
    ordinal: int
    envelope: EventEnvelope


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _safe_relative(base: Path, value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise RawSourceError("manifest contains an invalid relative path")
    candidate = (base / value).resolve()
    try:
        candidate.relative_to(base.resolve())
    except ValueError as exc:
        raise RawSourceError("manifest path escapes its Recorder root") from exc
    return candidate


def _external_artifact(
    *,
    chunk_id: str,
    catalog: Catalog,
    external_roots: Mapping[str, Path],
) -> Path | None:
    transaction = catalog.archive_transaction_for_chunk(chunk_id)
    if transaction is None:
        return None
    state = ArchiveState(str(transaction["state"]))
    if state not in {
        ArchiveState.VERIFIED,
        ArchiveState.LOCAL_DELETE_PENDING,
        ArchiveState.LOCAL_DELETED,
    }:
        return None
    storage_id = str(transaction["storage_id"])
    root = external_roots.get(storage_id)
    if root is None:
        return None
    return _safe_relative(root.resolve(), transaction["target_relative_path"])


def load_source_chunks(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    external_roots: Mapping[str, Path] | None = None,
) -> list[SourceChunk]:
    """Load every Raw manifest and fail if any corresponding content is unavailable."""

    roots = {} if external_roots is None else external_roots
    chunks: list[SourceChunk] = []
    for manifest_path in sorted(layout.manifests.glob("*.manifest.json")):
        try:
            manifest_bytes = manifest_path.read_bytes()
            decoded = json.loads(manifest_bytes)
        except (OSError, json.JSONDecodeError) as exc:
            raise RawSourceError(
                f"cannot read Raw manifest {manifest_path.name}: {type(exc).__name__}"
            ) from exc
        if (
            not isinstance(decoded, dict)
            or decoded.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION
        ):
            raise RawSourceError(f"unsupported Raw manifest {manifest_path.name}")
        chunk_id = decoded.get("chunk_id")
        uncompressed_hash = decoded.get("uncompressed_sha256")
        stored_hash = decoded.get("stored_sha256")
        if not all(
            isinstance(value, str) and len(value) == 64
            for value in (uncompressed_hash, stored_hash)
        ) or not isinstance(chunk_id, str):
            raise RawSourceError(f"invalid Raw identity in {manifest_path.name}")
        artifact = _safe_relative(layout.root, decoded.get("relative_path"))
        if not artifact.is_file():
            external = _external_artifact(
                chunk_id=chunk_id,
                catalog=catalog,
                external_roots=roots,
            )
            if external is None or not external.is_file():
                raise RawSourceError(
                    f"Raw chunk {chunk_id} is unavailable internally and on READY archives"
                )
            artifact = external
        try:
            validate_sealed_artifact(artifact, decoded)
        except Exception as exc:
            raise RawSourceError(
                f"Raw chunk {chunk_id} failed manifest verification: {exc}"
            ) from exc
        chunks.append(
            SourceChunk(
                chunk_id=chunk_id,
                artifact_path=artifact,
                manifest_path=manifest_path,
                manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                manifest=decoded,
            )
        )
    return sorted(
        chunks,
        key=lambda item: (
            item.uncompressed_sha256,
            item.chunk_id,
        ),
    )


def _read_up_to(source: BinaryIO, size: int) -> bytes:
    parts: list[bytes] = []
    remaining = size
    while remaining:
        block = source.read(remaining)
        if not block:
            break
        parts.append(block)
        remaining -= len(block)
    return b"".join(parts)


def iter_source_records(chunk: SourceChunk) -> Iterator[SourceRecord]:
    """Decode a previously verified Zstandard Raw artifact one bounded frame at a time."""

    decompressor = zstandard.ZstdDecompressor()
    try:
        with (
            chunk.artifact_path.open("rb", buffering=0) as compressed,
            decompressor.stream_reader(compressed) as reader,
        ):
            header, _header_bytes = decode_chunk_header(reader)
            if str(header.chunk_id) != chunk.chunk_id:
                raise RawSourceError("Raw header chunk ID differs from manifest")
            if (
                header.market != chunk.manifest.get("market")
                or header.symbol != chunk.manifest.get("symbol")
                or header.stream != chunk.manifest.get("stream")
            ):
                raise RawSourceError("Raw header identity differs from manifest")
            ordinal = 0
            while True:
                prefix = _read_up_to(reader, FRAME_PREFIX.size)
                if not prefix:
                    break
                if len(prefix) != FRAME_PREFIX.size:
                    raise RawSourceError("sealed Raw has a truncated frame prefix")
                body_length, flags, reserved, expected_crc = FRAME_PREFIX.unpack(prefix)
                if body_length > header.max_frame_bytes:
                    raise RawSourceError("sealed Raw frame exceeds declared maximum")
                if flags != 0 or reserved != 0:
                    raise RawSourceError("sealed Raw frame uses unsupported flags")
                body = _read_up_to(reader, body_length)
                if len(body) != body_length:
                    raise RawSourceError("sealed Raw has a truncated frame body")
                covered = prefix[: FRAME_PREFIX_WITHOUT_CRC.size]
                if google_crc32c.value(covered + body) != expected_crc:
                    raise RawSourceError("sealed Raw frame checksum mismatch")
                envelope = decode_envelope(body)
                if (
                    envelope.market != header.market
                    or envelope.symbol != header.symbol
                    or envelope.stream != header.stream
                ):
                    raise RawSourceError("Raw envelope differs from chunk identity")
                yield SourceRecord(chunk, ordinal, envelope)
                ordinal += 1
    except (OSError, zstandard.ZstdError, ChunkFormatError) as exc:
        raise RawSourceError(
            f"cannot decode Raw chunk {chunk.chunk_id}: {type(exc).__name__}: {exc}"
        ) from exc


def source_file_hashes(chunks: list[SourceChunk]) -> dict[Path, str]:
    """Return current hashes for acceptance assertions without changing sources."""

    return {chunk.artifact_path: _sha256_file(chunk.artifact_path) for chunk in chunks}
