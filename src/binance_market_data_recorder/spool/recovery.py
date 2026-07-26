"""Raw artifact 的幂等启动恢复与隔离。

recover_storage() 是 M3 启动协调,在任何 Collector 任务开始之前调用。
它按固定顺序运行:

1. recover_partials():扫描 active/ 中的每个 .bmdr.partial。干净的 partial
   在 Catalog 中注册。可截尾 partial 被 ftruncate 到最后一个有效帧,
   重新扫描并标记为 RECOVERED。损坏的 partial(无效头、校验和失败、不支持的
   flags)被隔离并以 SHA-256 哈希保留用于取证。
2. SEALING 协调:任何处于 SEALING 状态且存在未删除 partial 的 chunk 被重新
   提交给 seal_partial()。这覆盖了压缩/重命名与 Catalog SEALED 提交之间的
   崩溃窗口。
3. reconcile_sealed():manifests/ 中的每个 manifest.json 与 sealed artifact
   (大小、存储哈希、解压哈希)进行交叉验证。若 Catalog 仍显示 ACTIVE 或
   RECOVERED,chunk 被幂等推进到 SEALED。这覆盖了 manifest 写入后但 Catalog
   提交前的崩溃窗口。

恢复顺序很重要:partial 必须在 sealed manifest 之前协调,因为 SEALING chunk
可能需要先完成压缩,其 manifest 才能被协调。恢复可安全重复运行;所有 Catalog
转换使用幂等键。
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path

from ..storage.catalog import Catalog, ChunkState
from ..storage.layout import StorageLayout, fsync_directory
from .format import ScanIssue, scan_chunk
from .seal import SealError, seal_partial, validate_sealed_artifact


@dataclass(frozen=True)
class RecoveryAction:
    source: str
    action: str
    detail: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _quarantine(
    path: Path, *, layout: StorageLayout, catalog: Catalog, reason: str
) -> RecoveryAction:
    digest = _sha256_file(path)
    destination = layout.quarantine / f"{path.name}.{digest[:12]}.quarantine"
    if destination.exists():
        if _sha256_file(destination) != digest:
            raise RuntimeError("quarantine name collision with different content")
        path.unlink()
    else:
        os.replace(path, destination)
    fsync_directory(layout.quarantine)
    fsync_directory(path.parent)
    catalog.register_quarantined_artifact(
        artifact_id=digest,
        relative_path=layout.relative(destination),
        reason=reason,
        sha256=digest,
    )
    return RecoveryAction(layout.relative(path), "quarantined", reason)


def recover_partials(*, layout: StorageLayout, catalog: Catalog) -> list[RecoveryAction]:
    """Recover truncated tails and quarantine corruption; safe on repeated startup."""

    actions: list[RecoveryAction] = []
    for path in sorted(layout.active.glob("*.bmdr.partial")):
        scan = scan_chunk(path)
        if scan.header is None:
            actions.append(
                _quarantine(
                    path,
                    layout=layout,
                    catalog=catalog,
                    reason=f"{scan.issue}:{scan.detail}",
                )
            )
            continue
        chunk_id = str(scan.header.chunk_id)
        catalog.register_active(
            chunk_id=chunk_id,
            partial_path=layout.relative(path),
            created_at_utc_ns=scan.header.created_at_utc_ns,
        )
        if scan.issue is ScanIssue.NONE:
            actions.append(RecoveryAction(layout.relative(path), "unchanged", "clean"))
            continue
        if scan.is_tail_truncatable:
            truncated_bytes = scan.file_size - scan.valid_end
            descriptor = os.open(path, os.O_RDWR)
            try:
                os.ftruncate(descriptor, scan.valid_end)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            fsync_directory(layout.active)
            verified = scan_chunk(path)
            if not verified.is_clean:
                raise RuntimeError("tail truncation did not produce a clean partial")
            catalog.transition(
                chunk_id,
                ChunkState.RECOVERED,
                idempotency_key=f"recover:{chunk_id}:{scan.file_size}:{scan.valid_end}",
                evidence={
                    "issue": scan.issue,
                    "truncated_bytes": truncated_bytes,
                    "valid_end": scan.valid_end,
                },
            )
            actions.append(
                RecoveryAction(
                    layout.relative(path),
                    "tail_truncated",
                    f"removed {truncated_bytes} bytes",
                )
            )
            continue
        catalog.transition(
            chunk_id,
            ChunkState.QUARANTINED,
            idempotency_key=f"quarantine:{chunk_id}:{scan.file_size}:{scan.issue}",
            evidence={"detail": scan.detail, "issue": scan.issue},
        )
        actions.append(
            _quarantine(
                path,
                layout=layout,
                catalog=catalog,
                reason=f"{scan.issue}:{scan.detail}",
            )
        )
    return actions


def reconcile_sealed(*, layout: StorageLayout, catalog: Catalog) -> list[RecoveryAction]:
    """Reconcile verified manifests/artifacts after a crash before Catalog commit."""

    actions: list[RecoveryAction] = []
    for manifest_path in sorted(layout.manifests.glob("*.manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            sealed = layout.root / manifest["relative_path"]
            validate_sealed_artifact(sealed, manifest)
            chunk_id = str(manifest["chunk_id"])
            if catalog.state(chunk_id) is None:
                catalog.register_active(
                    chunk_id=chunk_id,
                    partial_path="",
                    created_at_utc_ns=int(manifest["created_at_utc_ns"]),
                )
            current = catalog.state(chunk_id)
            if current in {ChunkState.ACTIVE, ChunkState.RECOVERED}:
                catalog.transition(
                    chunk_id,
                    ChunkState.SEALING,
                    idempotency_key=f"reconcile-sealing:{chunk_id}",
                    evidence={"source": "manifest"},
                )
            catalog.transition(
                chunk_id,
                ChunkState.SEALED,
                idempotency_key=f"reconcile-sealed:{chunk_id}",
                evidence={"source": "manifest"},
                fields={
                    "manifest_path": layout.relative(manifest_path),
                    "partial_path": None,
                    "record_count": manifest["record_count"],
                    "sealed_path": manifest["relative_path"],
                    "stored_bytes": manifest["stored_bytes"],
                    "stored_sha256": manifest["stored_sha256"],
                    "uncompressed_bytes": manifest["uncompressed_bytes"],
                    "uncompressed_sha256": manifest["uncompressed_sha256"],
                },
            )
            actions.append(
                RecoveryAction(layout.relative(manifest_path), "catalog_reconciled", "verified")
            )
        except (KeyError, OSError, TypeError, ValueError, SealError) as exc:
            actions.append(
                RecoveryAction(layout.relative(manifest_path), "reconcile_failed", str(exc))
            )
    return actions


def recover_storage(*, layout: StorageLayout, catalog: Catalog) -> list[RecoveryAction]:
    """Run the complete M3 startup reconciliation in a stable order."""

    actions = recover_partials(layout=layout, catalog=catalog)
    for row in catalog.chunks_in_states(ChunkState.SEALING):
        partial_value = row.get("partial_path")
        if not isinstance(partial_value, str) or not partial_value:
            continue
        partial_path = layout.root / partial_value
        if not partial_path.exists():
            continue
        manifest = seal_partial(partial_path, layout=layout, catalog=catalog)
        actions.append(
            RecoveryAction(
                partial_value,
                "seal_completed_after_crash",
                str(manifest["chunk_id"]),
            )
        )
    actions.extend(reconcile_sealed(layout=layout, catalog=catalog))
    return actions
