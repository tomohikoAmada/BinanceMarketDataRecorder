"""Raw artifact 的幂等启动恢复与隔离。

recover_storage() 是 M3 启动协调,在任何 Collector 任务开始之前调用。
M21.4.11-R3.2/R3.3 将其分为两个阶段:

- Phase A(只读预决策):使用与只读 preflight 命令共享的决策引擎
  (spool/legacy_reconnect.py)对全部历史 SEALING reconnect intent 做
  穷尽三向划分(PROVEN_LEGITIMATE / PROVEN_EXTENSION / AMBIGUOUS),
  并校验 operator 分类 authority 的强绑定。任何未解决的 ambiguous、
  stale、unmatched、contradictory 或 conflict 状态都会在 BEFORE 任何
  legacy 生命周期 mutation 之前失败关闭,避免部分物化后中断。
- Phase B(执行):仅在全局决策集安全时执行:

1. recover_partials():扫描 active/ 中的每个 .bmdr.partial。干净的 partial
   在 Catalog 中注册。可截尾 partial 被 ftruncate 到最后一个有效帧,
   重新扫描并标记为 RECOVERED。损坏的 partial(无效头、校验和失败、不支持的
   flags)被隔离并以 SHA-256 哈希保留用于取证。
2. 断开连续性物化:按 Phase A 的决策,PROVEN_LEGITIMATE 与
   classified_legitimate_req103 的 intent 以相同 gap_id 物化一个
   STREAM_DISCONTINUITY_STARTED(P1-A 双故障回退);PROVEN_EXTENSION 与
   classified_extension_orphan 被报告为 extension_orphan_ignored,不物化。
3. SEALING 协调:任何处于 SEALING 状态且存在未删除 partial 的 chunk 被重新
   提交给 seal_partial()。这覆盖了压缩/重命名与 Catalog SEALED 提交之间的
   崩溃窗口,并从 durable authority(SEALING intent + 未关闭 STARTED)派生
   fail-closed forced flags。
4. retained source cleanup:若 Catalog SEALED 已提交但 source unlink 前崩溃,
   retained active partial 仍完整扫描并与 Raw manifest、Catalog、sealed artifact
   及任何已推进的 same-host archive transaction 做精确身份校验,然后只删除冗余
   active source。Archive 生命周期不回退。
5. reconcile_sealed():manifests/ 中的每个 manifest.json 与 Catalog
   生命周期元数据交叉验证。已稳定提交的本地 SEALED chunk 只校验
   manifest/Catalog 不可变身份和 artifact 存在/大小,避免每次启动将
   全历史 Raw 隐式重做 bit-rot audit。若 Catalog 行缺失或仍显示
   ACTIVE、RECOVERED、SEALING,则仍完整校验 artifact 大小、存储哈希和
   解压哈希,再幂等推进到 SEALED。这覆盖了 manifest 写入后但
   Catalog 提交前的崩溃窗口。

恢复顺序很重要:partial 必须在 sealed manifest 之前协调,因为 SEALING chunk
可能需要先完成压缩,其 manifest 才能被协调。恢复可安全重复运行;所有 Catalog
转换使用幂等键。干净的孤儿 ACTIVE partial(在 SEALING 提交前崩溃)被刻意保留
为 ACTIVE 且永不自动密封为 complete=true:没有 durable 证据能证明它被
reconnect 边界截断,而未密封的 Raw 证据仍然可恢复(REQ-108/P2-A)。
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from ..storage.catalog import (
    ARCHIVE_CHUNK_STATES,
    ArchiveState,
    Catalog,
    CatalogStateError,
    ChunkState,
    RemoteArchiveState,
    stream_discontinuity_event_id,
)
from ..storage.layout import StorageLayout, fsync_directory
from .format import ScanIssue, ScanResult, decode_chunk_header, scan_chunk
from .legacy_reconnect import (
    LEGACY_CLASSIFICATION_FILENAME,
    LegacyClassificationAuthority,
    LegacyDecisionReport,
    LegacyReconnectConflictError,
    evaluate_legacy_reconnect_decisions,
    validate_seal_intent,
)
from .seal import (
    RECONNECT_GAP_FLAG,
    SEAL_INTENT_EVIDENCE_KEY,
    SealError,
    seal_partial,
    validate_sealed_artifact,
)


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
_ARCHIVE_STATES_REQUIRING_SEALED_SOURCE = frozenset(
    {
        ChunkState.ARCHIVE_COPYING,
        ChunkState.ARCHIVE_VERIFYING,
        ChunkState.ARCHIVED_VERIFIED,
    }
)

RecoveryStopPredicate = Callable[[], bool]


def _stop_requested(stop_requested: RecoveryStopPredicate | None) -> bool:
    return stop_requested is not None and stop_requested()


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


def _retained_source_sealed_path(
    *, layout: StorageLayout, manifest: dict[str, object]
) -> Path:
    relative = Path(_required_text(manifest, "relative_path"))
    if relative.is_absolute() or ".." in relative.parts:
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_SEALED_PATH_INVALID")
    sealed = layout.root / relative
    if sealed.parent.resolve() != layout.sealed.resolve():
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_SEALED_PATH_INVALID")
    return sealed


def _validate_retained_source_file(
    path: Path, scan: ScanResult
) -> os.stat_result:
    try:
        status = os.lstat(path)
    except OSError as exc:
        raise RecoveryConflictError(
            "RECOVERY_RETAINED_SOURCE_UNAVAILABLE"
        ) from exc
    if not stat.S_ISREG(status.st_mode):
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_NOT_REGULAR")
    if status.st_size != scan.file_size:
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_SIZE_CHANGED")
    if scan.uncompressed_sha256 is None or _sha256_file(path) != scan.uncompressed_sha256:
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_SHA256_CHANGED")
    verified = os.lstat(path)
    if (
        verified.st_dev,
        verified.st_ino,
        verified.st_size,
        verified.st_mtime_ns,
    ) != (
        status.st_dev,
        status.st_ino,
        status.st_size,
        status.st_mtime_ns,
    ):
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_CHANGED_DURING_PROOF")
    return verified


def _archive_cleanup_authority(
    transaction: dict[str, object] | None,
) -> tuple[object, ...] | None:
    if transaction is None:
        return None
    return tuple(
        transaction.get(name)
        for name in (
            "transaction_id",
            "chunk_id",
            "state",
            "market",
            "stream",
            "source_relative_path",
            "source_manifest_relative_path",
            "source_manifest_sha256",
            "stored_bytes",
            "stored_sha256",
        )
    )


def _cleanup_retained_source(
    *,
    path: Path,
    layout: StorageLayout,
    catalog: Catalog,
) -> ChunkState | None:
    """Remove only an exact Raw duplicate already represented by durable authority.

    The active source is freshly full-scanned. Raw size/SHA, header identity,
    manifest, Catalog, sealed artifact (while required), and same-host archive
    transaction must all agree. Catalog/archive state is read-only here; a
    concurrent archive transition may advance but is never reversed.

    ``None`` leaves the existing remote-owned SEALED path to its separate
    recovery authority.
    """

    scan = scan_chunk(path)
    if not scan.is_clean or scan.header is None or scan.uncompressed_sha256 is None:
        raise RecoveryConflictError(
            f"RECOVERY_RETAINED_SOURCE_NOT_CLEAN: {scan.issue}: {scan.detail}"
        )
    verified_status = _validate_retained_source_file(path, scan)
    chunk_id = str(scan.header.chunk_id)
    manifest_path = layout.manifests / f"{scan.header.chunk_id.hex}.manifest.json"
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest = json.loads(manifest_bytes)
    except (OSError, json.JSONDecodeError) as exc:
        raise RecoveryConflictError(
            "RECOVERY_RETAINED_SOURCE_MANIFEST_UNAVAILABLE"
        ) from exc
    if not isinstance(manifest, dict):
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_MANIFEST_INVALID")
    if _required_text(manifest, "chunk_id") != chunk_id:
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_CHUNK_ID_CONFLICT")
    expected_fields = _manifest_catalog_fields(
        layout=layout,
        manifest_path=manifest_path,
        manifest=manifest,
    )
    if (
        scan.file_size != expected_fields["uncompressed_bytes"]
        or scan.uncompressed_sha256 != expected_fields["uncompressed_sha256"]
    ):
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_RAW_IDENTITY_CONFLICT")
    header_identity = {
        "chunk_schema_version": scan.header.chunk_schema_version,
        "collector_version": scan.header.collector_version,
        "created_at_utc_ns": scan.header.created_at_utc_ns,
        "envelope_schema_version": scan.header.envelope_schema_version,
        "market": scan.header.market,
        "stream": scan.header.stream,
        "symbol": scan.header.symbol,
    }
    if any(manifest.get(name) != value for name, value in header_identity.items()):
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_HEADER_IDENTITY_CONFLICT")
    sealed = _retained_source_sealed_path(layout=layout, manifest=manifest)

    while True:
        row, transaction, remote = catalog.source_lifecycle_snapshot(chunk_id)
        if row is None:
            return None
        current = ChunkState(str(row["state"]))
        if current is ChunkState.SEALED:
            if transaction is not None:
                raise RecoveryConflictError(
                    "RECOVERY_SEALED_ARCHIVE_TRANSACTION_CONFLICT"
                )
            if remote is not None:
                return None
            _validate_catalog_identity(row, expected_fields)
        elif current in _ARCHIVE_STATES:
            if remote is not None:
                raise RecoveryConflictError("RECOVERY_SAME_HOST_REMOTE_OVERLAP")
            _validate_archive_identity(
                row=row,
                transaction=transaction,
                manifest=manifest,
                manifest_bytes=manifest_bytes,
                expected_fields=expected_fields,
            )
        else:
            return None

        if current is ChunkState.SEALED:
            # Catalog SEALED metadata does not replace the durable Raw artifact.
            # Do not delete the retained source unless the ordinary local sealed
            # artifact still exists and passes the complete stored/decompressed
            # identity validation. Re-sealing here would race ArchiveManager's
            # independent SEALED reservation and could attempt lifecycle reversal.
            try:
                validate_sealed_artifact(sealed, manifest)
            except FileNotFoundError as exc:
                raise RecoveryConflictError(
                    "RECOVERY_RETAINED_SOURCE_SEALED_ARTIFACT_MISSING"
                ) from exc
            except (OSError, SealError) as exc:
                raise RecoveryConflictError(
                    "RECOVERY_RETAINED_SOURCE_SEALED_ARTIFACT_INVALID"
                ) from exc
        elif current in _ARCHIVE_STATES_REQUIRING_SEALED_SOURCE or sealed.exists():
            validate_sealed_artifact(sealed, manifest)

        latest_row, latest_transaction, latest_remote = (
            catalog.source_lifecycle_snapshot(chunk_id)
        )
        if latest_row is None:
            raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_CATALOG_MISSING")
        latest = ChunkState(str(latest_row["state"]))
        if (
            latest is current
            and _archive_cleanup_authority(latest_transaction)
            == _archive_cleanup_authority(transaction)
            and latest_remote == remote
        ):
            break

    final_status = _validate_retained_source_file(path, scan)
    if (
        final_status.st_dev,
        final_status.st_ino,
        final_status.st_size,
        final_status.st_mtime_ns,
    ) != (
        verified_status.st_dev,
        verified_status.st_ino,
        verified_status.st_size,
        verified_status.st_mtime_ns,
    ):
        raise RecoveryConflictError("RECOVERY_RETAINED_SOURCE_CHANGED_DURING_PROOF")
    path.unlink()
    fsync_directory(layout.active)
    return current


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


def recover_partials(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    stop_requested: RecoveryStopPredicate | None = None,
) -> list[RecoveryAction]:
    """Recover truncated tails and quarantine corruption; safe on repeated startup."""

    actions: list[RecoveryAction] = []
    for path in sorted(layout.active.glob("*.bmdr.partial")):
        if _stop_requested(stop_requested):
            break
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


def reconcile_sealed(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    stop_requested: RecoveryStopPredicate | None = None,
) -> list[RecoveryAction]:
    """Reconcile manifests with Catalog, fully validating crash-unstable artifacts."""

    actions: list[RecoveryAction] = []
    for manifest_path in sorted(layout.manifests.glob("*.manifest.json")):
        if _stop_requested(stop_requested):
            break
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
                row, transaction, remote = catalog.source_lifecycle_snapshot(chunk_id)
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
                    if remote is not None:
                        raise RecoveryConflictError(
                            "RECOVERY_SAME_HOST_REMOTE_OVERLAP"
                        )
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
                if current is ChunkState.SEALED:
                    _validate_catalog_identity(row, expected_fields)
                    if transaction is not None:
                        raise RecoveryConflictError(
                            "RECOVERY_SEALED_ARCHIVE_TRANSACTION_CONFLICT"
                        )
                    if remote is not None:
                        from ..archive.remote_authorization import (
                            RemoteRecoveryCase,
                            classify_remote_recovery,
                        )
                        from ..archive.remote_delete import (
                            RemoteDeleter,
                            RemoteDeletionError,
                        )

                        decision = classify_remote_recovery(
                            layout=layout, catalog=catalog, chunk_id=chunk_id
                        )
                        if decision.case not in {
                            RemoteRecoveryCase.CASE_A,
                            RemoteRecoveryCase.CASE_B,
                            RemoteRecoveryCase.TERMINAL_ABSENT,
                        }:
                            raise RecoveryConflictError(
                                f"RECOVERY_REMOTE_{decision.case.value}: "
                                f"{decision.detail}"
                            )
                        if decision.case is RemoteRecoveryCase.CASE_B:
                            try:
                                result = RemoteDeleter(
                                    layout=layout,
                                    catalog=catalog,
                                ).reconcile_absent_authorized(
                                    str(remote["receipt_id"])
                                )
                            except RemoteDeletionError as exc:
                                raise RecoveryConflictError(
                                    "RECOVERY_REMOTE_CASE_B_RECONCILE_FAILED: "
                                    f"{exc}"
                                ) from exc
                            actions.append(
                                RecoveryAction(
                                    layout.relative(manifest_path),
                                    "remote_absent_reconciled",
                                    result.state.value,
                                )
                            )
                            break
                        actions.append(
                            RecoveryAction(
                                layout.relative(manifest_path),
                                "remote_lifecycle_preserved",
                                decision.case.value,
                            )
                        )
                        break
                    if sealed.stat().st_size != expected_fields["stored_bytes"]:
                        raise SealError("sealed size mismatch")
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
                if not artifact_validated:
                    validate_sealed_artifact(sealed, manifest)
                    artifact_validated = True
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


def _remote_retained_manifest_path(
    *, layout: StorageLayout, relative: object
) -> Path:
    """Resolve one persisted remote manifest only inside ``data/manifests``."""

    if not isinstance(relative, str) or not relative:
        raise RecoveryConflictError(
            "RECOVERY_REMOTE_RETAINED_MANIFEST_PATH_INVALID"
        )
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise RecoveryConflictError(
            "RECOVERY_REMOTE_RETAINED_MANIFEST_PATH_INVALID"
        )
    path = layout.root / candidate
    try:
        if (
            layout.manifests.is_symlink()
            or path.parent.resolve() != layout.manifests.resolve()
        ):
            raise RecoveryConflictError(
                "RECOVERY_REMOTE_RETAINED_MANIFEST_PATH_INVALID"
            )
        status = os.lstat(path)
    except FileNotFoundError as exc:
        raise RecoveryConflictError(
            "RECOVERY_REMOTE_RETAINED_MANIFEST_MISSING"
        ) from exc
    except OSError as exc:
        raise RecoveryConflictError(
            "RECOVERY_REMOTE_RETAINED_MANIFEST_UNAVAILABLE"
        ) from exc
    if stat.S_ISLNK(status.st_mode) or not stat.S_ISREG(status.st_mode):
        raise RecoveryConflictError(
            "RECOVERY_REMOTE_RETAINED_MANIFEST_NOT_REGULAR"
        )
    return path


def _validate_remote_recovery_coverage(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    stop_requested: RecoveryStopPredicate | None = None,
) -> None:
    """Discover every durable remote row before manifest-driven recovery.

    The retained manifest is required authority for both pending and terminal
    remote rows. This preflight deliberately checks coverage only; semantic
    manifest/descriptor validation and CASE-A/CASE-B decisions remain in
    ``reconcile_sealed`` and ``RemoteDeleter``.
    """

    try:
        transactions = catalog.remote_archive_transactions()
    except CatalogStateError as exc:
        raise RecoveryConflictError(
            "RECOVERY_REMOTE_AUTHORITY_INVALID"
        ) from exc
    for transaction in transactions:
        if _stop_requested(stop_requested):
            return
        try:
            receipt_id = transaction["receipt_id"]
            chunk_id = transaction["chunk_id"]
            state = RemoteArchiveState(str(transaction["state"]))
            relative = transaction["source_manifest_relative_path"]
        except (KeyError, TypeError, ValueError) as exc:
            raise RecoveryConflictError(
                "RECOVERY_REMOTE_AUTHORITY_INVALID"
            ) from exc
        if not isinstance(receipt_id, str) or not receipt_id:
            raise RecoveryConflictError("RECOVERY_REMOTE_AUTHORITY_INVALID")
        if not isinstance(chunk_id, str) or not chunk_id:
            raise RecoveryConflictError("RECOVERY_REMOTE_AUTHORITY_INVALID")
        if state not in {
            RemoteArchiveState.REMOTE_DELETE_PENDING,
            RemoteArchiveState.REMOTE_DELETED,
        }:
            raise RecoveryConflictError("RECOVERY_REMOTE_AUTHORITY_INVALID")
        try:
            _remote_retained_manifest_path(layout=layout, relative=relative)
        except RecoveryConflictError as exc:
            if exc.args and exc.args[0] == "RECOVERY_REMOTE_RETAINED_MANIFEST_MISSING":
                raise RecoveryConflictError(
                    "RECOVERY_REMOTE_RETAINED_MANIFEST_MISSING "
                    f"receipt_id={receipt_id} chunk_id={chunk_id}"
                ) from exc
            raise


def _derived_seal_flags(
    partial_path: Path,
    catalog: Catalog,
    chunk_id: str,
) -> frozenset[str]:
    """Derive fail-closed seal flags from durable reconnect-boundary authority.

    Phase A of recover_storage() has already reconciled every durable SEALING
    seal intent through the shared legacy decision engine, so the materialize
    decisions are durably present as unclosed STARTED events by the time a
    SEALING partial is re-sealed here.  Derivation therefore reduces to the
    two durable authorities:

    A. The durable SEALING seal intent (required forced flags) recorded in
       the ChunkState.SEALING transition evidence (M21.4.11-R2 P1-A).
    B. An unclosed Catalog STREAM_DISCONTINUITY_STARTED for the same
       market/symbol/stream (the pre-R2 authority).

    A SEALING chunk with no reconnect intent at all is sealed with ordinary
    semantics: blanket "every SEALING chunk is a gap" forcing is prohibited
    (TEST-106).
    """
    with partial_path.open("rb", buffering=0) as source:
        header, _body = decode_chunk_header(source)
    evidence = _sealing_evidence(catalog, chunk_id)
    intent = (
        evidence.get(SEAL_INTENT_EVIDENCE_KEY) if evidence is not None else None
    )
    if intent is not None and not isinstance(intent, dict):
        raise RecoveryConflictError(
            f"RECOVERY_SEAL_INTENT_MALFORMED chunk={chunk_id}"
        )
    required: frozenset[str] = frozenset()
    if intent is not None:
        validate_seal_intent(intent, chunk_id)
        flags = intent["required_forced_flags"]
        required = (
            frozenset(str(flag) for flag in flags)
            if isinstance(flags, list)
            else frozenset()
        )
    open_gaps = catalog.unclosed_stream_discontinuities(
        market=header.market, symbol=header.symbol, stream=header.stream
    )
    if len(open_gaps) > 1:
        # Two genuinely simultaneous unmatched gaps on one market/symbol/stream are
        # a multi-fault state; fail closed (INV-005).
        raise RecoveryConflictError(
            "RECOVERY_SEAL_INTENT_STARTED_CONFLICT "
            f"chunk={chunk_id} multiple simultaneously open gaps"
        )
    if open_gaps:
        return required | frozenset({RECONNECT_GAP_FLAG})
    return required


def _sealing_evidence(
    catalog: Catalog, chunk_id: str
) -> dict[str, object] | None:
    evidence = catalog.latest_transition_evidence(chunk_id, ChunkState.SEALING)
    if not evidence:
        return None
    return evidence


def _record_started_from_intent(
    catalog: Catalog, intent: dict[str, object]
) -> str:
    gap_id = str(intent["gap_id"])
    market = str(intent["market"])
    symbol = str(intent["symbol"])
    stream = str(intent["stream"])
    catalog.ensure_operational_event(
        event_id=stream_discontinuity_event_id(
            event_type="STREAM_DISCONTINUITY_STARTED",
            market=market,
            symbol=symbol,
            stream=stream,
            gap_id=gap_id,
        ),
        event_type="STREAM_DISCONTINUITY_STARTED",
        occurred_at_utc_ns=int(cast(int, intent["gap_started_at_utc_ns"])),
        evidence={
            "gap_id": gap_id,
            "market": market,
            "symbol": symbol,
            "stream": stream,
            "reason": intent["reason"],
            "interval_classification": "UNRELIABLE",
            "gap_started_at_utc_ns": intent["gap_started_at_utc_ns"],
            "original_connection_id": intent["original_connection_id"],
            "original_generation": intent["original_generation"],
            "boundary_kind": intent.get("boundary_kind", "no_last_frame_available"),
            "boundary_frame_persisted": intent.get(
                "boundary_frame_persisted", False
            ),
            "boundary_precision": (
                "reconstructed by startup recovery from durable SEALING "
                "seal intent after the original STARTED write failed; no "
                "exchange payload is fabricated"
            ),
        },
        symbol=symbol,
    )
    return "materialized"


def _apply_legacy_decisions(
    *,
    catalog: Catalog,
    report: LegacyDecisionReport,
    stop_requested: RecoveryStopPredicate | None = None,
) -> list[RecoveryAction]:
    """Execute the Phase A legacy decisions (Phase B of R3.2).

    Startup recovery and the read-only preflight share the same decision
    engine; this function only applies decisions the engine already proved
    safe.  A candidate decided PROVEN_LEGITIMATE or classified
    ``legitimate_req103`` materializes exactly one STARTED with its same
    durable gap_id; a candidate decided PROVEN_EXTENSION or classified
    ``extension_orphan`` materializes nothing and is reported idempotently.
    """
    actions: list[RecoveryAction] = []
    for decision in report.decisions:
        if _stop_requested(stop_requested):
            break
        if decision.final in {
            "proven_legitimate",
            "classified_legitimate_req103",
        }:
            _record_started_from_intent(
                catalog, decision.candidate.intent
            )
            actions.append(
                RecoveryAction(
                    decision.candidate.chunk_id,
                    "pending_discontinuity_materialized",
                    decision.candidate.gap_id,
                )
            )
        elif decision.final in {
            "proven_extension",
            "classified_extension_orphan",
        }:
            actions.append(
                RecoveryAction(
                    decision.candidate.chunk_id,
                    "extension_orphan_ignored",
                    decision.candidate.gap_id,
                )
            )
    return actions


def recover_storage(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    authority_path: Path | None = None,
    stop_requested: RecoveryStopPredicate | None = None,
) -> list[RecoveryAction]:
    """Run the complete M3 startup reconciliation in a stable order.

    M21.4.11-R3.2/R3.3 Phase A: the shared legacy decision engine
    classifies every historical SEALING reconnect intent and validates
    the operator classification authority BEFORE any mutation.  If any
    unresolved ambiguous, stale, unmatched, contradictory, conflicting,
    or degraded-authority blocker exists, startup fails closed with the
    full blocker list instead of partially mutating lifecycle state.

    ``authority_path`` selects the operator authority location.  The
    system service passes the config-namespace path
    (``config_file.parent / legacy_reconnect_classifications.json``),
    which is root-controlled and NOT writable by the service principal
    (M21.4.11-R3.4); the data-root fallback is only for config-less
    interactive/test operation.
    """
    if _stop_requested(stop_requested):
        return []
    selected_authority_path = (
        Path(authority_path)
        if authority_path is not None
        else layout.root / LEGACY_CLASSIFICATION_FILENAME
    )
    try:
        authority = LegacyClassificationAuthority.load(selected_authority_path)
        report = evaluate_legacy_reconnect_decisions(
            catalog=catalog, authority=authority
        )
    except LegacyReconnectConflictError as exc:
        raise RecoveryConflictError(str(exc)) from exc
    if _stop_requested(stop_requested):
        return []
    if not report.first_corrected_startup_eligible:
        raise RecoveryConflictError(
            "RECOVERY_LEGACY_PREDECISION_INELIGIBLE "
            + report.blocker_summary()
        )
    _validate_remote_recovery_coverage(
        layout=layout,
        catalog=catalog,
        stop_requested=stop_requested,
    )
    if _stop_requested(stop_requested):
        return []
    actions = recover_partials(
        layout=layout,
        catalog=catalog,
        stop_requested=stop_requested,
    )
    if _stop_requested(stop_requested):
        return actions
    actions.extend(
        _apply_legacy_decisions(
            catalog=catalog,
            report=report,
            stop_requested=stop_requested,
        )
    )
    if _stop_requested(stop_requested):
        return actions
    for row in catalog.chunks_in_states(ChunkState.SEALING):
        if _stop_requested(stop_requested):
            return actions
        partial_value = row.get("partial_path")
        if not isinstance(partial_value, str) or not partial_value:
            continue
        partial_path = layout.root / partial_value
        if not partial_path.exists():
            continue
        chunk_id = str(row["chunk_id"])
        manifest = seal_partial(
            partial_path,
            layout=layout,
            catalog=catalog,
            forced_flags=_derived_seal_flags(partial_path, catalog, chunk_id),
        )
        actions.append(
            RecoveryAction(
                partial_value,
                "seal_completed_after_crash",
                str(manifest["chunk_id"]),
            )
        )
    # A retained source after the terminal SEALED commit is a physical duplicate,
    # even when the independently running ArchiveManager has already advanced the
    # chunk. Discover only from the small active set, then freshly full-scan and
    # prove exact Raw/manifest/Catalog/artifact/archive identity before unlink.
    # This cleanup never mutates the chunk or archive lifecycle.
    for partial_path in sorted(layout.active.glob("*.bmdr.partial")):
        if _stop_requested(stop_requested):
            return actions
        with partial_path.open("rb", buffering=0) as source:
            header, _header_bytes = decode_chunk_header(source)
        chunk_id = str(header.chunk_id)
        current = catalog.state(chunk_id)
        if current is not ChunkState.SEALED and current not in _ARCHIVE_STATES:
            continue
        removed_from = _cleanup_retained_source(
            path=partial_path,
            layout=layout,
            catalog=catalog,
        )
        if removed_from is None:
            # Remote ownership keeps physical ChunkState.SEALED and has a
            # separate recovery model. Preserve the pre-existing path through
            # the general full-scan seal authority rather than interpreting it
            # as a same-host archive successor.
            if catalog.state(chunk_id) is not ChunkState.SEALED:
                continue
            manifest = seal_partial(
                partial_path,
                layout=layout,
                catalog=catalog,
                forced_flags=_derived_seal_flags(partial_path, catalog, chunk_id),
            )
            detail = str(manifest["chunk_id"])
            action = "seal_completed_after_crash"
        else:
            detail = chunk_id
            action = (
                "seal_completed_after_crash"
                if removed_from is ChunkState.SEALED
                else "archive_retained_source_removed"
            )
        actions.append(
            RecoveryAction(
                layout.relative(partial_path),
                action,
                detail,
            )
        )
    if _stop_requested(stop_requested):
        return actions
    actions.extend(
        reconcile_sealed(
            layout=layout,
            catalog=catalog,
            stop_requested=stop_requested,
        )
    )
    return actions
