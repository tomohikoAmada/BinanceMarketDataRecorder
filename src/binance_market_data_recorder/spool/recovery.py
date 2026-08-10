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

from ..storage.catalog import (
    ARCHIVE_CHUNK_STATES,
    ArchiveState,
    Catalog,
    CatalogStateError,
    ChunkState,
)
from ..storage.layout import StorageLayout, fsync_directory
from .format import ScanIssue, decode_chunk_header, scan_chunk
from .seal import RECONNECT_GAP_FLAG, SealError, seal_partial, validate_sealed_artifact


@dataclass(frozen=True)
class RecoveryAction:
    source: str
    action: str
    detail: str


class RecoveryConflictError(CatalogStateError):
    """Raised when a manifest contradicts immutable Catalog identity."""


_ARCHIVE_STATES = frozenset(ARCHIVE_CHUNK_STATES.values())
_CHUNK_TO_ARCHIVE_STATE = {
    chunk_state: archive_state
    for archive_state, chunk_state in ARCHIVE_CHUNK_STATES.items()
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _required_int(document: dict[str, object], name: str) -> int:
    value = document[name]
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Raw manifest field {name} must be an integer")
    return value


def _required_text(document: dict[str, object], name: str) -> str:
    value = document[name]
    if not isinstance(value, str) or not value:
        raise TypeError(f"Raw manifest field {name} must be non-empty text")
    return value


def _manifest_catalog_fields(
    *,
    layout: StorageLayout,
    manifest_path: Path,
    manifest: dict[str, object],
) -> dict[str, object]:
    return {
        "created_at_utc_ns": _required_int(manifest, "created_at_utc_ns"),
        "manifest_path": layout.relative(manifest_path),
        "record_count": _required_int(manifest, "record_count"),
        "sealed_path": _required_text(manifest, "relative_path"),
        "stored_bytes": _required_int(manifest, "stored_bytes"),
        "stored_sha256": _required_text(manifest, "stored_sha256"),
        "uncompressed_bytes": _required_int(manifest, "uncompressed_bytes"),
        "uncompressed_sha256": _required_text(manifest, "uncompressed_sha256"),
    }


def _validate_catalog_identity(
    row: dict[str, object], expected: dict[str, object]
) -> None:
    if any(row.get(name) != value for name, value in expected.items()):
        raise RecoveryConflictError(
            "RECOVERY_MANIFEST_CATALOG_IDENTITY_CONFLICT"
        )


def _validate_archive_identity(
    *,
    row: dict[str, object],
    transaction: dict[str, object] | None,
    manifest: dict[str, object],
    manifest_bytes: bytes,
    expected_fields: dict[str, object],
) -> None:
    _validate_catalog_identity(row, expected_fields)
    chunk_id = _required_text(manifest, "chunk_id")
    if transaction is None:
        raise RecoveryConflictError("RECOVERY_ARCHIVE_TRANSACTION_MISSING")
    chunk_state = ChunkState(str(row["state"]))
    expected_archive_state = _CHUNK_TO_ARCHIVE_STATE[chunk_state]
    try:
        archive_state = ArchiveState(str(transaction["state"]))
    except (KeyError, ValueError) as exc:
        raise RecoveryConflictError(
            "RECOVERY_ARCHIVE_TRANSACTION_STATE_CONFLICT"
        ) from exc
    if archive_state is not expected_archive_state:
        raise RecoveryConflictError(
            "RECOVERY_ARCHIVE_TRANSACTION_STATE_CONFLICT"
        )
    expected_transaction = {
        "chunk_id": chunk_id,
        "market": _required_text(manifest, "market"),
        "source_manifest_relative_path": expected_fields["manifest_path"],
        "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "source_relative_path": expected_fields["sealed_path"],
        "stored_bytes": expected_fields["stored_bytes"],
        "stored_sha256": expected_fields["stored_sha256"],
        "stream": _required_text(manifest, "stream"),
    }
    if any(
        transaction.get(name) != value
        for name, value in expected_transaction.items()
    ):
        raise RecoveryConflictError(
            "RECOVERY_ARCHIVE_TRANSACTION_IDENTITY_CONFLICT"
        )


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
            manifest_bytes = manifest_path.read_bytes()
            manifest = json.loads(manifest_bytes)
            if not isinstance(manifest, dict):
                raise TypeError("Raw manifest must be a JSON object")
            sealed = layout.root / _required_text(manifest, "relative_path")
            chunk_id = _required_text(manifest, "chunk_id")
            expected_fields = _manifest_catalog_fields(
                layout=layout,
                manifest_path=manifest_path,
                manifest=manifest,
            )
            artifact_validated = False
            catalog_changed = False
            while True:
                row, transaction = catalog.chunk_archive_snapshot(chunk_id)
                if row is None:
                    if not artifact_validated:
                        validate_sealed_artifact(sealed, manifest)
                        artifact_validated = True
                    catalog.register_active(
                        chunk_id=chunk_id,
                        partial_path="",
                        created_at_utc_ns=_required_int(
                            manifest, "created_at_utc_ns"
                        ),
                    )
                    catalog_changed = True
                    continue

                current = ChunkState(str(row["state"]))
                if current in _ARCHIVE_STATES:
                    _validate_archive_identity(
                        row=row,
                        transaction=transaction,
                        manifest=manifest,
                        manifest_bytes=manifest_bytes,
                        expected_fields=expected_fields,
                    )
                    actions.append(
                        RecoveryAction(
                            layout.relative(manifest_path),
                            "archive_state_preserved",
                            current.value,
                        )
                    )
                    break
                if current is ChunkState.QUARANTINED:
                    raise RecoveryConflictError(
                        "RECOVERY_QUARANTINED_CHUNK_CONFLICT"
                    )
                if not artifact_validated:
                    validate_sealed_artifact(sealed, manifest)
                    artifact_validated = True
                if current is ChunkState.SEALED:
                    _validate_catalog_identity(row, expected_fields)
                    if transaction is not None:
                        raise RecoveryConflictError(
                            "RECOVERY_SEALED_ARCHIVE_TRANSACTION_CONFLICT"
                        )
                    actions.append(
                        RecoveryAction(
                            layout.relative(manifest_path),
                            (
                                "catalog_reconciled"
                                if catalog_changed
                                else "catalog_unchanged"
                            ),
                            "SEALED",
                        )
                    )
                    break
                if current in {ChunkState.ACTIVE, ChunkState.RECOVERED}:
                    target = ChunkState.SEALING
                    idempotency_key = f"reconcile-sealing:{chunk_id}"
                    fields: dict[str, object] | None = None
                elif current is ChunkState.SEALING:
                    target = ChunkState.SEALED
                    idempotency_key = f"reconcile-sealed:{chunk_id}"
                    fields = {
                        "manifest_path": expected_fields["manifest_path"],
                        "partial_path": None,
                        "record_count": expected_fields["record_count"],
                        "sealed_path": expected_fields["sealed_path"],
                        "stored_bytes": expected_fields["stored_bytes"],
                        "stored_sha256": expected_fields["stored_sha256"],
                        "uncompressed_bytes": expected_fields["uncompressed_bytes"],
                        "uncompressed_sha256": expected_fields[
                            "uncompressed_sha256"
                        ],
                    }
                else:  # pragma: no cover - exhaustive guard for future states
                    raise RecoveryConflictError(
                        "RECOVERY_UNSUPPORTED_CHUNK_STATE"
                    )
                try:
                    catalog.transition(
                        chunk_id,
                        target,
                        idempotency_key=idempotency_key,
                        evidence={"source": "manifest"},
                        fields=fields,
                    )
                    catalog_changed = True
                except CatalogStateError:
                    # Archive may atomically reserve the chunk immediately after
                    # another recovery writer commits SEALED. Re-read and
                    # classify the winning state rather than issuing a reverse
                    # transition. A genuine contradiction still fails below.
                    latest = catalog.state(chunk_id)
                    if latest not in _ARCHIVE_STATES:
                        raise
                continue
        except (KeyError, OSError, TypeError, ValueError, SealError) as exc:
            actions.append(
                RecoveryAction(layout.relative(manifest_path), "reconcile_failed", str(exc))
            )
    return actions


def _derived_seal_flags(
    partial_path: Path, catalog: Catalog
) -> frozenset[str]:
    """Derive fail-closed seal flags from durable reconnect-boundary intent.

    A clean partial scanned during startup has no in-memory forced flags: the
    collector that crashed was the only holder of that memory. If the Catalog
    still carries an unclosed STREAM_DISCONTINUITY_STARTED for the same
    market/stream, the durable intent proves this partial was cut at a
    reconnect boundary, and sealing it complete=true would fabricate
    exchange-side completeness. Startup therefore forces the manifest-level
    reconnect_gap marker. When no open discontinuity exists the partial is
    sealed with ordinary semantics.
    """
    with partial_path.open("rb", buffering=0) as source:
        header, _body = decode_chunk_header(source)
    open_gaps = catalog.unclosed_stream_discontinuities(
        market=header.market, stream=header.stream
    )
    if open_gaps:
        return frozenset({RECONNECT_GAP_FLAG})
    return frozenset()


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
        manifest = seal_partial(
            partial_path,
            layout=layout,
            catalog=catalog,
            forced_flags=_derived_seal_flags(partial_path, catalog),
        )
        actions.append(
            RecoveryAction(
                partial_value,
                "seal_completed_after_crash",
                str(manifest["chunk_id"]),
            )
        )
    actions.extend(reconcile_sealed(layout=layout, catalog=catalog))
    return actions
