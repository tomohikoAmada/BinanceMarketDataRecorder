"""已验证、位置无关的不可变 sealed Raw chunk 读取。

本模块加载 sealed Raw chunk 用于规范化,从内部 sealed/ 目录或
archive-verified 外部副本解析来源。它在解压帧流式输出之前,
按 Raw manifest 验证 sealed artifact(大小 + 存储 SHA-256 + 解压 SHA-256)。
不可用或未验证的来源中止规范化构建,而非静默产生不完整输出。

源 chunk 身份包括 uncompressed_sha256、manifest_sha256 和全部 manifest 元数据。
规范化在每一输出行中以此进行来源追踪。
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
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
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from ..storage.source_location import RawSourceError as RawSourceError
from ..storage.source_location import archived_artifact as _external_artifact
from ..storage.source_location import safe_relative as _safe_relative


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
    _payload_digest: str | None = field(default=None, init=False, compare=False, repr=False)

    @property
    def payload_sha256(self) -> str:
        if self._payload_digest is None:
            object.__setattr__(
                self, "_payload_digest", hashlib.sha256(self.envelope.raw_payload).hexdigest()
            )
        assert self._payload_digest is not None
        return self._payload_digest


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_source_chunks(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    external_roots: Mapping[str, Path] | None = None,
    products: frozenset[tuple[str, str]] | None = None,
    start_utc_ns: int | None = None,
    end_utc_ns: int | None = None,
    manifest_paths: list[Path] | None = None,
) -> list[SourceChunk]:
    """Load every Raw manifest and fail if any corresponding content is unavailable."""

    roots = {} if external_roots is None else external_roots
    chunks: list[SourceChunk] = []
    paths = layout.manifests.glob("*.manifest.json") if manifest_paths is None else manifest_paths
    for manifest_path in sorted(paths):
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
        if products is not None and (decoded.get("market"), decoded.get("symbol")) not in products:
            continue
        times = decoded.get("receive_time_utc_range_ns", {})
        first, last = times.get("min"), times.get("max")
        if start_utc_ns is not None and isinstance(last, int) and last < start_utc_ns:
            continue
        if end_utc_ns is not None and isinstance(first, int) and first >= end_utc_ns:
            continue
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
