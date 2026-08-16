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
4. reconcile_sealed():manifests/ 中的每个 manifest.json 与 sealed artifact
   (大小、存储哈希、解压哈希)进行交叉验证。若 Catalog 仍显示 ACTIVE 或
   RECOVERED,chunk 被幂等推进到 SEALED。这覆盖了 manifest 写入后但 Catalog
   提交前的崩溃窗口。

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
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from ..storage.catalog import (
    ARCHIVE_CHUNK_STATES,
    ArchiveState,
    Catalog,
    CatalogStateError,
    ChunkState,
)
from ..storage.layout import StorageLayout, fsync_directory
from .format import ScanIssue, decode_chunk_header, scan_chunk
from .legacy_reconnect import (
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
       market/stream (the pre-R2 authority).

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
        market=header.market, stream=header.stream
    )
    if len(open_gaps) > 1:
        # Two genuinely simultaneous unmatched gaps on one market/stream are
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
    catalog.ensure_operational_event(
        event_id=f"stream-discontinuity-started:{gap_id}",
        event_type="STREAM_DISCONTINUITY_STARTED",
        occurred_at_utc_ns=int(cast(int, intent["gap_started_at_utc_ns"])),
        evidence={
            "gap_id": gap_id,
            "market": intent["market"],
            "stream": intent["stream"],
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
    )
    return "materialized"


def _apply_legacy_decisions(
    *, catalog: Catalog, report: LegacyDecisionReport
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


def recover_storage(*, layout: StorageLayout, catalog: Catalog) -> list[RecoveryAction]:
    """Run the complete M3 startup reconciliation in a stable order.

    M21.4.11-R3.2 Phase A: the shared legacy decision engine classifies
    every historical SEALING reconnect intent and validates the operator
    classification authority BEFORE any mutation.  If any unresolved
    ambiguous, stale, unmatched, contradictory, or conflicting candidate
    exists, startup fails closed with the full blocker list instead of
    partially mutating lifecycle state.
    """
    try:
        authority = LegacyClassificationAuthority.load(layout)
        report = evaluate_legacy_reconnect_decisions(
            catalog=catalog, authority=authority
        )
    except LegacyReconnectConflictError as exc:
        raise RecoveryConflictError(str(exc)) from exc
    if not report.first_corrected_startup_eligible:
        raise RecoveryConflictError(
            "RECOVERY_LEGACY_PREDECISION_INELIGIBLE "
            + report.blocker_summary()
        )
    actions = recover_partials(layout=layout, catalog=catalog)
    actions.extend(_apply_legacy_decisions(catalog=catalog, report=report))
    for row in catalog.chunks_in_states(ChunkState.SEALING):
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
    actions.extend(reconcile_sealed(layout=layout, catalog=catalog))
    return actions
