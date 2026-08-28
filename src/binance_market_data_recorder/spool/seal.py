"""Raw chunk 的经验证不可变密封与 manifest 提交。

seal_partial() 与 live clean-writer 入口共享 ACTIVE/RECOVERED -> SEALING -> SEALED 转换,
按以下顺序执行:

1. authority:任意/恢复 partial 由 scan 验证所有帧;正常 live-owned writer 使用
   同一批已写 Raw 字节增量派生的一次性内存证据。
2. SEALING 转换:在 Catalog 中记录意图(幂等键防重放)。
3. compress:Zstd level 3 带 content-size 和 checksum,写入 sealed 目录中的
   .partial 文件,然后 fsync。
4. 解压回读:解压并哈希以验证压缩 artifact 与原始解压数据匹配。
5. 原子重命名到 sealed/ 并 fsync 目录。
6. manifest:JSON 文档,绑定 chunk 身份、统计信息、压缩参数和双重哈希
   (存储 + 解压)。
7. SEALED 转换:提交到 Catalog,然后删除原始 .partial 并 fsync active 目录。

仅在 Catalog SEALED 提交后删除 partial 源。此前的所有步骤是幂等的:
若任何步骤失败且进程重启,恢复重新扫描 partial 并重试。已存在且解压大小/哈希
匹配的 sealed 文件被接受,无需重新压缩。

当 capture_flags 包含 checksum_failure、mixed_sequence_type、orderbook_resync、
recovered_tail、sequence_gap、reconnect_gap 中任一项时,manifest 中的 'complete'
标志为 False。不完整区间不能携带 complete=true。

reconnect_gap 是 manifest 级强制不完整标志:它只能通过 forced_flags 或
多连接无蓝绿重叠来源的防线产生,永远不写入 Raw 帧。一个 chunk 一旦包含多个
connection_id 且无 sequence_gap/reconnect_gap/blue_green_overlap 证据,
密封时 fail closed 为 gap=true/complete=false,绝不宣称跨连接完整。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

import zstandard

from ..storage.catalog import Catalog, ChunkState
from ..storage.layout import StorageLayout, fsync_directory
from .evidence import _VerifiedChunkEvidence
from .format import scan_chunk

if TYPE_CHECKING:
    from .writer import RawChunkWriter

COMPRESSION_SCHEMA_VERSION = "zstd-frame.v1"
MANIFEST_SCHEMA_VERSION = "raw-chunk-manifest.v1"
ZSTD_LEVEL = 3
READ_BUFFER_BYTES = 1024 * 1024

#: Manifest-level evidence that a chunk ends at a transport reconnect boundary
#: whose exchange-side completeness cannot be proven. The flag is written only
#: to the manifest (and seal evidence), never to Raw frames: an already
#: persisted last-old frame must not be mutated, and no exchange payload is
#: fabricated. It forces gap=true and complete=false.
RECONNECT_GAP_FLAG = "reconnect_gap"

#: Explicit provenance that multiple connections in one chunk are an intended
#: blue/green deployment overlap rather than an unmarked reconnect boundary.
OVERLAP_FLAG = "blue_green_overlap"

#: Flags that alone make a chunk interval unprovably incomplete.
INCOMPLETE_FLAGS = frozenset(
    {
        "checksum_failure",
        "mixed_sequence_type",
        "orderbook_resync",
        "recovered_tail",
        "sequence_gap",
        RECONNECT_GAP_FLAG,
    }
)

#: Key of the durable seal-intent document inside the ChunkState.SEALING
#: transition evidence (M21.4.11-R2). The seal intent is the crash fallback
#: authority for reconnect-boundary semantics: it is persisted BEFORE any
#: artifact/manifest mutation, so a restart can reconstruct the required
#: forced flags and the pending discontinuity even when the Catalog
#: STREAM_DISCONTINUITY_STARTED event itself failed to commit (P1-A).
SEAL_INTENT_EVIDENCE_KEY = "seal_intent"

#: Current durable reconnect-intent contract version (M21.4.11-R3.3).
#:
#: Every seal intent emitted by the R3.3+ runtime carries this exact string
#: under ``intent_schema``.  The version is durable provenance for legacy
#: recovery classification: under the versioned runtime prevention contract a
#: pure extension can never mint an independent orphan gap identity (it
#: reuses the pending gap's canonical identity) and decision-point-2 uses a
#: fresh genuine logical gap, so a versioned ABSENT intent safely represents
#: the REQ-103 intent-only crash shape.  Intents without ``intent_schema``
#: are pre-R3 legacy intents and follow the conservative legacy policy.  An
#: unknown future schema fails closed.  The field is persisted inside the
#: immutable SEALING transition evidence; it is NOT a SQLite schema migration
#: (SCHEMA_MIGRATION_REQUIRED=false).
RECONNECT_INTENT_SCHEMA_V2 = "reconnect-seal-intent.v2"


class SealError(RuntimeError):
    """Raised when a partial cannot be proven safe to seal."""


MANIFEST_REQUIRED_FIELDS = frozenset(
    {
        "capture_flags",
        "chunk_id",
        "chunk_schema_version",
        "collector_instance_ids",
        "collector_version",
        "complete",
        "compression",
        "connection_ids",
        "created_at_utc_ns",
        "envelope_schema_version",
        "exchange_time_ranges",
        "fsync_completed_at_utc_ns",
        "gap",
        "manifest_schema_version",
        "market",
        "overlap",
        "receive_monotonic_range_ns",
        "receive_time_utc_range_ns",
        "record_count",
        "recovered",
        "recovery",
        "relative_path",
        "resync",
        "sealed_at_utc_ns",
        "sequence_ranges",
        "stored_bytes",
        "stored_sha256",
        "stream",
        "symbol",
        "uncompressed_bytes",
        "uncompressed_sha256",
    }
)
_HEX_SHA256 = re.compile(r"^[0-9a-f]{64}$")


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


def _compress(source_path: Path, target_partial: Path, source_size: int) -> tuple[int, int]:
    compressor = zstandard.ZstdCompressor(
        level=ZSTD_LEVEL,
        write_checksum=True,
        write_content_size=True,
        write_dict_id=False,
        threads=0,
    )
    descriptor = os.open(target_partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with (
            source_path.open("rb", buffering=0) as source,
            os.fdopen(descriptor, "wb", buffering=0, closefd=False) as target,
        ):
            try:
                copied = compressor.copy_stream(source, target, size=source_size)
            except zstandard.ZstdError as exc:
                raise SealError(
                    f"compressor source byte count/identity mismatch: {exc}"
                ) from exc
            target.flush()
        os.fsync(descriptor)
        stored_size = os.fstat(descriptor).st_size
    finally:
        os.close(descriptor)
    if (
        not isinstance(copied, tuple)
        or len(copied) != 2
        or not all(isinstance(value, int) for value in copied)
    ):
        raise SealError("compressor did not report byte counts")
    source_bytes, stored_bytes = copied
    if stored_bytes != stored_size:
        raise SealError("compressor stored byte count mismatch")
    return source_bytes, stored_bytes


def _decompressed_identity(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    byte_count = 0
    decompressor = zstandard.ZstdDecompressor()
    try:
        with (
            path.open("rb", buffering=0) as compressed,
            decompressor.stream_reader(compressed) as reader,
        ):
            while block := reader.read(READ_BUFFER_BYTES):
                digest.update(block)
                byte_count += len(block)
    except (OSError, zstandard.ZstdError) as exc:
        raise SealError(f"cannot validate compressed artifact {path}: {exc}") from exc
    return byte_count, digest.hexdigest()


def _deployment_ids(flags: frozenset[str]) -> set[str]:
    return {flag for flag in flags if flag.startswith("deployment_id=")}


def _overlap_covers_transition(old_flags: frozenset[str], new_flags: frozenset[str]) -> bool:
    """True when a blue/green overlap provably covers this exact transition.

    Both boundary frames must carry the overlap flag, and when a
    ``deployment_id`` is present on either side the two sides must share at
    least one deployment identity. A lone overlap flag elsewhere in the chunk
    never exempts an unrelated connection transition.
    """
    if OVERLAP_FLAG not in old_flags or OVERLAP_FLAG not in new_flags:
        return False
    old_deployments = _deployment_ids(old_flags)
    new_deployments = _deployment_ids(new_flags)
    if not old_deployments and not new_deployments:
        return True
    return bool(old_deployments & new_deployments)


def _boundary_local_evidence_safe(
    transitions: tuple[tuple[str, str, frozenset[str], frozenset[str]], ...],
) -> bool:
    """Every connection transition inside a chunk must carry boundary-local proof.

    A transition is safe only when the exact boundary pair carries
    ``sequence_gap`` on either side or a blue/green overlap that covers that
    exact transition. Evidence from one transition never exempts another.
    """
    for _old, _new, old_flags, new_flags in transitions:
        if "sequence_gap" in old_flags or "sequence_gap" in new_flags:
            continue
        if _overlap_covers_transition(old_flags, new_flags):
            continue
        return False
    return True


def _manifest(
    evidence: _VerifiedChunkEvidence,
    layout: StorageLayout,
    sealed: Path,
    *,
    recovery: dict[str, object] | None,
    forced_flags: frozenset[str] = frozenset(),
) -> dict[str, object]:
    statistics = evidence.statistics.mutable_copy()
    stored_sha256 = _sha256_file(sealed)
    stored_bytes = sealed.stat().st_size
    capture_flags = set(statistics.capture_flags)
    if recovery is not None:
        capture_flags.add("recovered_tail")
    capture_flags.update(forced_flags)
    if (
        len(statistics.connection_ids) > 1
        and not capture_flags & INCOMPLETE_FLAGS
        and not _boundary_local_evidence_safe(evidence.connection_transitions)
    ):
        # Defense in depth: a sealed chunk must never claim
        # gap=false/complete=true across a connection transition that lacks
        # boundary-local evidence. Blue/green overlap is safe only when it
        # covers the exact transition; anything else fails closed to an
        # incomplete interval.
        capture_flags.add(RECONNECT_GAP_FLAG)
    complete = not bool(capture_flags & INCOMPLETE_FLAGS)
    sealed_at = time.time_ns()
    return {
        "capture_flags": sorted(capture_flags),
        "chunk_id": str(evidence.header.chunk_id),
        "chunk_schema_version": evidence.header.chunk_schema_version,
        # Zero-record boundary markers have no frame statistics, but their Raw
        # v1 header still carries authentic collector provenance.  Preserve
        # that header identity without fabricating connection/timestamp data.
        "collector_instance_ids": sorted(statistics.collector_instance_ids)
        or [evidence.header.collector_instance_id],
        "collector_version": evidence.header.collector_version,
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
        "created_at_utc_ns": evidence.header.created_at_utc_ns,
        "envelope_schema_version": evidence.header.envelope_schema_version,
        "exchange_time_ranges": {
            name: {"min": values[0], "max": values[1]}
            for name, values in sorted(statistics.exchange_time_ranges.items())
        },
        "fsync_completed_at_utc_ns": sealed_at,
        "gap": bool(capture_flags & {"sequence_gap", RECONNECT_GAP_FLAG}),
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "market": evidence.header.market,
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
        "stream": evidence.header.stream,
        "symbol": evidence.header.symbol,
        "uncompressed_bytes": evidence.file_size,
        "uncompressed_sha256": evidence.uncompressed_sha256,
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
    for name in ("gap", "complete"):
        if existing.get(name) != expected[name]:
            raise SealError(
                "existing manifest contradicts freshly derived completeness "
                f"semantics: expected {name}={expected[name]}"
            )


def seal_partial(
    path: Path,
    *,
    layout: StorageLayout,
    catalog: Catalog,
    forced_flags: frozenset[str] = frozenset(),
    seal_intent: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Seal a closed partial idempotently; never delete it before Catalog commit.

    ``forced_flags`` are manifest-level only (see ``_manifest``); they never
    mutate Raw frames and therefore cannot fabricate exchange payloads.

    ``seal_intent`` is the durable reconnect-boundary fallback (P1-A): when a
    seal is requested with reconnect semantics, the intent document is
    persisted into the ChunkState.SEALING transition evidence BEFORE any
    artifact, manifest, or SEALED mutation. If the process then crashes and
    the Catalog STREAM_DISCONTINUITY_STARTED event never committed, startup
    recovery reconstructs the required forced flags and the pending
    discontinuity from this evidence instead of sealing the partial
    complete=true.

    This general/recovery entry always scans. Live-owned clean writers use the
    private writer-bound entry below; callers cannot inject optional evidence
    into this API.
    """

    scan = scan_chunk(path)
    if not scan.is_clean or scan.header is None or scan.uncompressed_sha256 is None:
        raise SealError(f"partial is not clean: {scan.issue}: {scan.detail}")
    return _seal_verified(
        _VerifiedChunkEvidence.from_scan(scan),
        layout=layout,
        catalog=catalog,
        forced_flags=forced_flags,
        seal_intent=seal_intent,
    )


def _seal_clean_writer(
    writer: RawChunkWriter,
    *,
    layout: StorageLayout,
    catalog: Catalog,
    forced_flags: frozenset[str] = frozenset(),
    seal_intent: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Consume one live writer's clean evidence and use the shared seal protocol."""

    if writer.layout != layout or writer.catalog is not catalog:
        raise ValueError("clean writer is not bound to this layout and Catalog")
    verified = writer._take_clean_seal_evidence()
    return _seal_verified(
        verified,
        layout=layout,
        catalog=catalog,
        forced_flags=forced_flags,
        seal_intent=seal_intent,
    )


def _seal_verified(
    verified: _VerifiedChunkEvidence,
    *,
    layout: StorageLayout,
    catalog: Catalog,
    forced_flags: frozenset[str],
    seal_intent: Mapping[str, object] | None,
) -> dict[str, object]:
    """The one durable ACTIVE/RECOVERED -> SEALING -> SEALED protocol."""

    path = verified.path
    chunk_id = str(verified.header.chunk_id)
    current = catalog.state(chunk_id)
    if current is None:
        catalog.register_active(
            chunk_id=chunk_id,
            partial_path=layout.relative(path),
            created_at_utc_ns=verified.header.created_at_utc_ns,
        )
        current = ChunkState.ACTIVE
    if current in {ChunkState.ACTIVE, ChunkState.RECOVERED}:
        transition_evidence: dict[str, object] = {
            "verified_frames": verified.statistics.record_count,
        }
        if seal_intent is not None:
            transition_evidence[SEAL_INTENT_EVIDENCE_KEY] = dict(seal_intent)
        catalog.transition(
            chunk_id,
            ChunkState.SEALING,
            idempotency_key=f"sealing:{chunk_id}",
            evidence=transition_evidence,
        )
    elif current is ChunkState.SEALING and seal_intent is not None:
        # A previous seal attempt already made this chunk durable SEALING
        # evidence (possibly with a different intent). A conflicting intent
        # on re-seal is a double-fault: fail closed rather than silently
        # adopting a second boundary identity.
        existing = catalog.latest_transition_evidence(chunk_id, ChunkState.SEALING) or {}
        prior = existing.get(SEAL_INTENT_EVIDENCE_KEY)
        if prior is not None and (not isinstance(prior, dict) or dict(prior) != dict(seal_intent)):
            raise SealError("durable SEALING evidence conflicts with the requested seal intent")

    sealed = layout.sealed / f"{verified.header.chunk_id.hex}.bmdr.zst"
    target_partial = sealed.with_suffix(sealed.suffix + ".partial")
    if sealed.exists():
        decompressed_bytes, decompressed_sha256 = _decompressed_identity(sealed)
        if (
            decompressed_bytes != verified.file_size
            or decompressed_sha256 != verified.uncompressed_sha256
        ):
            raise SealError("existing sealed artifact does not match partial")
    else:
        if target_partial.exists():
            target_partial.unlink()
        consumed_bytes, _stored_bytes = _compress(
            path, target_partial, verified.file_size
        )
        if consumed_bytes != verified.file_size:
            raise SealError("compressor source byte count mismatch")
        decompressed_bytes, decompressed_sha256 = _decompressed_identity(target_partial)
        if decompressed_bytes != verified.file_size:
            raise SealError("compressed readback size mismatch")
        if decompressed_sha256 != verified.uncompressed_sha256:
            raise SealError("compressed readback hash mismatch")
        os.replace(target_partial, sealed)
        fsync_directory(layout.sealed)

    recovery_evidence = catalog.latest_transition_evidence(chunk_id, ChunkState.RECOVERED)
    manifest_document = _manifest(
        verified,
        layout,
        sealed,
        recovery=recovery_evidence,
        forced_flags=forced_flags,
    )
    manifest_path = layout.manifests / f"{verified.header.chunk_id.hex}.manifest.json"
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
            "record_count": verified.statistics.record_count,
            "sealed_path": layout.relative(sealed),
            "stored_bytes": manifest_document["stored_bytes"],
            "stored_sha256": manifest_document["stored_sha256"],
            "uncompressed_bytes": verified.file_size,
            "uncompressed_sha256": verified.uncompressed_sha256,
        },
    )
    if path.exists():
        path.unlink()
        fsync_directory(layout.active)
    return manifest_document


def read_strict_manifest(path: Path, *, recorder_root: Path | None = None) -> dict[str, object]:
    """Read one Raw manifest without accepting partial or approximate evidence.

    Sealing and archive validation already define the manifest fields.  This
    read-only helper exposes that same interpretation to acceptance tooling;
    it intentionally raises on every malformed, unsupported, or contradictory
    document so an inventory can never shrink by skipping a bad file.
    """

    try:
        raw = path.read_bytes()
        document: Any = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SealError(f"cannot read Raw manifest {path}: {type(exc).__name__}") from exc
    if not isinstance(document, dict):
        raise SealError(f"Raw manifest {path} is not an object")
    if document.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
        raise SealError(f"unsupported Raw manifest schema in {path}")
    missing = MANIFEST_REQUIRED_FIELDS - set(document)
    if missing:
        raise SealError(f"Raw manifest {path} is missing fields: {sorted(missing)}")
    if set(document) != MANIFEST_REQUIRED_FIELDS:
        raise SealError(f"Raw manifest {path} has malformed fields")
    text_fields = (
        "chunk_id",
        "chunk_schema_version",
        "collector_version",
        "market",
        "stream",
        "symbol",
        "relative_path",
    )
    for field in text_fields:
        if not isinstance(document[field], str) or not document[field]:
            raise SealError(f"Raw manifest {path} has invalid {field}")
    for field in ("stored_sha256", "uncompressed_sha256"):
        value = document[field]
        if not isinstance(value, str) or _HEX_SHA256.fullmatch(value) is None:
            raise SealError(f"Raw manifest {path} has invalid {field}")
    integer_fields = (
        "created_at_utc_ns",
        "fsync_completed_at_utc_ns",
        "record_count",
        "sealed_at_utc_ns",
        "stored_bytes",
        "uncompressed_bytes",
    )
    for field in integer_fields:
        value = document[field]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise SealError(f"Raw manifest {path} has invalid {field}")
    for field in ("complete", "gap", "overlap", "recovered", "resync"):
        if not isinstance(document[field], bool):
            raise SealError(f"Raw manifest {path} has invalid {field}")
    flags = document["capture_flags"]
    if not isinstance(flags, list) or any(not isinstance(value, str) for value in flags):
        raise SealError(f"Raw manifest {path} has invalid capture_flags")
    if document["gap"] != bool(set(flags) & {"sequence_gap", RECONNECT_GAP_FLAG}):
        raise SealError(f"Raw manifest {path} contradicts gap flags")
    if document["complete"] and set(flags) & INCOMPLETE_FLAGS:
        raise SealError(f"Raw manifest {path} contradicts completeness flags")
    relative = Path(str(document["relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise SealError(f"Raw manifest {path} has unsafe relative_path")
    if recorder_root is not None:
        root = recorder_root.resolve()
        try:
            (root / relative).resolve().relative_to(root)
        except ValueError as exc:
            raise SealError(f"Raw manifest {path} escapes recorder root") from exc
    return {str(key): value for key, value in document.items()}


def validate_sealed_artifact(sealed: Path, manifest: dict[str, object]) -> None:
    """Validate stored and decompressed Raw byte identities."""

    if sealed.stat().st_size != manifest["stored_bytes"]:
        raise SealError("sealed size mismatch")
    if _sha256_file(sealed) != manifest["stored_sha256"]:
        raise SealError("sealed SHA-256 mismatch")
    decompressed_bytes, decompressed_sha256 = _decompressed_identity(sealed)
    if decompressed_bytes != manifest["uncompressed_bytes"]:
        raise SealError("sealed decompressed size mismatch")
    if decompressed_sha256 != manifest["uncompressed_sha256"]:
        raise SealError("sealed decompressed SHA-256 mismatch")
