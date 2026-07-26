"""Raw chunk 的经验证不可变密封与 manifest 提交。

seal_partial() 实现 ACTIVE/RECOVERED -> SEALING -> SEALED 转换,
按以下顺序执行:

1. scan:验证所有帧,计算统计信息和解压 SHA-256。
2. SEALING 转换:在 Catalog 中记录意图(幂等键防重放)。
3. compress:Zstd level 3 带 content-size 和 checksum,写入 sealed 目录中的
   .partial 文件,然后 fsync。
4. 解压回读:解压并哈希以验证压缩 artifact 与原始解压数据匹配。
5. 原子重命名到 sealed/ 并 fsync 目录。
6. manifest:JSON 文档,绑定 chunk 身份、统计信息、压缩参数和双重哈希
   (存储 + 解压)。
7. SEALED 转换:提交到 Catalog,然后删除原始 .partial 并 fsync active 目录。

仅在 Catalog SEALED 提交后删除 partial 源。此前的所有步骤是幂等的:
若任何步骤失败且进程重启,恢复重新扫描 partial 并重试。已存在且解压哈希
匹配的 sealed 文件被接受,无需重新压缩。

当 capture_flags 包含 checksum_failure、mixed_sequence_type、orderbook_resync、
recovered_tail、sequence_gap 中任一项时,manifest 中的 'complete' 标志为 False。
不完整区间不能携带 complete=true。
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import zstandard

from ..storage.catalog import Catalog, ChunkState
from ..storage.layout import StorageLayout, fsync_directory
from .format import ScanResult, scan_chunk

COMPRESSION_SCHEMA_VERSION = "zstd-frame.v1"
MANIFEST_SCHEMA_VERSION = "raw-chunk-manifest.v1"
ZSTD_LEVEL = 3
READ_BUFFER_BYTES = 1024 * 1024


class SealError(RuntimeError):
    """Raised when a partial cannot be proven safe to seal."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while block := source.read(READ_BUFFER_BYTES):
            digest.update(block)
    return digest.hexdigest()


def _atomic_json(path: Path, document: dict[str, object]) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    body = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("manifest write returned no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(partial, path)
    fsync_directory(path.parent)


def _compress(source_path: Path, target_partial: Path, source_size: int) -> None:
    compressor = zstandard.ZstdCompressor(
        level=ZSTD_LEVEL,
        write_checksum=True,
        write_content_size=True,
        write_dict_id=False,
        threads=0,
    )
    descriptor = os.open(target_partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with source_path.open("rb", buffering=0) as source, os.fdopen(
            descriptor, "wb", buffering=0, closefd=False
        ) as target:
            compressor.copy_stream(source, target, size=source_size)
            target.flush()
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _decompressed_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    decompressor = zstandard.ZstdDecompressor()
    try:
        with path.open("rb", buffering=0) as compressed, decompressor.stream_reader(
            compressed
        ) as reader:
            while block := reader.read(READ_BUFFER_BYTES):
                digest.update(block)
    except (OSError, zstandard.ZstdError) as exc:
        raise SealError(f"cannot validate compressed artifact {path}: {exc}") from exc
    return digest.hexdigest()


def _manifest(
    scan: ScanResult,
    layout: StorageLayout,
    sealed: Path,
    *,
    recovery: dict[str, object] | None,
) -> dict[str, object]:
    if scan.header is None or scan.uncompressed_sha256 is None:
        raise SealError("clean header and hash required")
    statistics = scan.statistics
    stored_sha256 = _sha256_file(sealed)
    stored_bytes = sealed.stat().st_size
    capture_flags = set(statistics.capture_flags)
    if recovery is not None:
        capture_flags.add("recovered_tail")
    incomplete_flags = {
        "checksum_failure",
        "mixed_sequence_type",
        "orderbook_resync",
        "recovered_tail",
        "sequence_gap",
    }
    complete = not bool(capture_flags & incomplete_flags)
    sealed_at = time.time_ns()
    return {
        "capture_flags": sorted(capture_flags),
        "chunk_id": str(scan.header.chunk_id),
        "chunk_schema_version": scan.header.chunk_schema_version,
        "collector_instance_ids": sorted(statistics.collector_instance_ids),
        "collector_version": scan.header.collector_version,
        "complete": complete,
        "compression": {
            "checksum": True,
            "content_size": True,
            "dictionary_id": False,
            "level": ZSTD_LEVEL,
            "schema_version": COMPRESSION_SCHEMA_VERSION,
            "threads": 0,
        },
        "connection_ids": sorted(statistics.connection_ids),
        "created_at_utc_ns": scan.header.created_at_utc_ns,
        "envelope_schema_version": scan.header.envelope_schema_version,
        "exchange_time_ranges": {
            name: {"min": values[0], "max": values[1]}
            for name, values in sorted(statistics.exchange_time_ranges.items())
        },
        "fsync_completed_at_utc_ns": sealed_at,
        "gap": "sequence_gap" in capture_flags,
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "market": scan.header.market,
        "overlap": "overlap" in capture_flags,
        "receive_monotonic_range_ns": {
            "min": statistics.receive_monotonic_min_ns,
            "max": statistics.receive_monotonic_max_ns,
        },
        "receive_time_utc_range_ns": {
            "min": statistics.receive_time_utc_min_ns,
            "max": statistics.receive_time_utc_max_ns,
        },
        "record_count": statistics.record_count,
        "recovered": recovery is not None,
        "recovery": recovery,
        "relative_path": layout.relative(sealed),
        "resync": "orderbook_resync" in capture_flags,
        "sealed_at_utc_ns": sealed_at,
        "sequence_ranges": statistics.sequence_ranges(),
        "stored_bytes": stored_bytes,
        "stored_sha256": stored_sha256,
        "stream": scan.header.stream,
        "symbol": scan.header.symbol,
        "uncompressed_bytes": scan.file_size,
        "uncompressed_sha256": scan.uncompressed_sha256,
    }


def _validate_existing_manifest(path: Path, expected: dict[str, object]) -> None:
    try:
        existing: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SealError(f"cannot validate existing manifest {path}: {exc}") from exc
    stable_fields = (
        "chunk_id",
        "record_count",
        "stored_bytes",
        "stored_sha256",
        "uncompressed_bytes",
        "uncompressed_sha256",
    )
    if any(existing.get(name) != expected[name] for name in stable_fields):
        raise SealError("existing manifest does not identify the same verified chunk")


def seal_partial(path: Path, *, layout: StorageLayout, catalog: Catalog) -> dict[str, object]:
    """Seal a closed partial idempotently; never delete it before Catalog commit."""

    scan = scan_chunk(path)
    if not scan.is_clean or scan.header is None or scan.uncompressed_sha256 is None:
        raise SealError(f"partial is not clean: {scan.issue}: {scan.detail}")
    chunk_id = str(scan.header.chunk_id)
    current = catalog.state(chunk_id)
    if current is None:
        catalog.register_active(
            chunk_id=chunk_id,
            partial_path=layout.relative(path),
            created_at_utc_ns=scan.header.created_at_utc_ns,
        )
        current = ChunkState.ACTIVE
    if current in {ChunkState.ACTIVE, ChunkState.RECOVERED}:
        catalog.transition(
            chunk_id,
            ChunkState.SEALING,
            idempotency_key=f"sealing:{chunk_id}",
            evidence={"verified_frames": scan.statistics.record_count},
        )

    sealed = layout.sealed / f"{scan.header.chunk_id.hex}.bmdr.zst"
    target_partial = sealed.with_suffix(sealed.suffix + ".partial")
    if sealed.exists():
        if _decompressed_sha256(sealed) != scan.uncompressed_sha256:
            raise SealError("existing sealed artifact does not match partial")
    else:
        if target_partial.exists():
            target_partial.unlink()
        _compress(path, target_partial, scan.file_size)
        if _decompressed_sha256(target_partial) != scan.uncompressed_sha256:
            raise SealError("compressed readback hash mismatch")
        os.replace(target_partial, sealed)
        fsync_directory(layout.sealed)

    recovery_evidence = catalog.latest_transition_evidence(
        chunk_id, ChunkState.RECOVERED
    )
    manifest_document = _manifest(
        scan,
        layout,
        sealed,
        recovery=recovery_evidence,
    )
    manifest_path = layout.manifests / f"{scan.header.chunk_id.hex}.manifest.json"
    if manifest_path.exists():
        _validate_existing_manifest(manifest_path, manifest_document)
        manifest_document = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest_partial = manifest_path.with_suffix(manifest_path.suffix + ".partial")
        if manifest_partial.exists():
            manifest_partial.unlink()
        _atomic_json(manifest_path, manifest_document)

    catalog.transition(
        chunk_id,
        ChunkState.SEALED,
        idempotency_key=f"sealed:{chunk_id}",
        evidence={"manifest_schema_version": MANIFEST_SCHEMA_VERSION},
        fields={
            "manifest_path": layout.relative(manifest_path),
            "partial_path": None,
            "record_count": scan.statistics.record_count,
            "sealed_path": layout.relative(sealed),
            "stored_bytes": manifest_document["stored_bytes"],
            "stored_sha256": manifest_document["stored_sha256"],
            "uncompressed_bytes": scan.file_size,
            "uncompressed_sha256": scan.uncompressed_sha256,
        },
    )
    if path.exists():
        path.unlink()
        fsync_directory(layout.active)
    return manifest_document


def validate_sealed_artifact(sealed: Path, manifest: dict[str, object]) -> None:
    """Validate stored size/hash and decompressed logical Raw hash."""

    if sealed.stat().st_size != manifest["stored_bytes"]:
        raise SealError("sealed size mismatch")
    if _sha256_file(sealed) != manifest["stored_sha256"]:
        raise SealError("sealed SHA-256 mismatch")
    if _decompressed_sha256(sealed) != manifest["uncompressed_sha256"]:
        raise SealError("sealed decompressed SHA-256 mismatch")
