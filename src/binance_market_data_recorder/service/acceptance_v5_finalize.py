"""Repository-owned V5 baseline, terminal finalize, and independent verifier."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any
from uuid import uuid4

from .acceptance import (
    AcceptanceError,
    LinuxClock,
    _safe_evidence_root,
    canonical_json,
    sha256_bytes,
)
from .acceptance_v5_audit import AuditRecords
from .acceptance_v5_corpus import (
    baseline_preflight,
    freeze_corpus,
    quiescence_probe,
    validate_quiescence,
    verify_corpus,
)
from .acceptance_v5_io import (
    AUDIT_DOMAIN,
    MAX_RECORDS_PER_SHARD,
    V5_SCHEMA_VERSION,
    ShardWriter,
    chain_next,
    chain_start,
    read_document,
    shard_records,
)
from .acceptance_v5_io import (
    publish as _publish,
)
from .acceptance_v5_online import OnlineReplay, replay_online
from .acceptance_v5_raw import AUDIT_PROGRESS_INTERVAL_NS, CancellableExecutor, cancellable_unit
from .deployment_identity import DeploymentIdentity, verify_identity_files


def cancellable_freeze(
    *,
    data_root: Path,
    root: Path,
    identity: DeploymentIdentity,
    anchor_sha256: str,
    boot_id: str,
    probe: Callable[[Path], dict[str, Any]],
) -> tuple[Path, str, dict[str, Any]]:
    if probe is not quiescence_probe:
        return freeze_corpus(
            data_root=data_root,
            root=root,
            identity=identity,
            anchor_sha256=anchor_sha256,
            boot_id=boot_id,
            probe=probe,
        )
    ordinal = 0
    while (root / f"freeze-progress-{ordinal:08d}.json").exists():
        ordinal += 1
    last_progress = time.monotonic_ns()
    processed = 0

    def progress(size: int) -> None:
        nonlocal last_progress, ordinal, processed
        processed += size
        if time.monotonic_ns() - last_progress >= AUDIT_PROGRESS_INTERVAL_NS:
            _publish(
                root,
                f"freeze-progress-{ordinal:08d}.json",
                {
                    "schema_version": V5_SCHEMA_VERSION,
                    "evidence_kind": "freeze-progress",
                    "anchor_sha256": anchor_sha256,
                    "phase": "corpus-freeze",
                    "bytes_processed": processed,
                },
            )
            ordinal += 1
            last_progress = time.monotonic_ns()

    try:
        result = cancellable_unit(
            {
                "operation": "freeze-corpus",
                "data_root": str(data_root),
                "root": str(root),
                "identity": identity.document(),
                "anchor": anchor_sha256,
                "boot_id": boot_id,
            },
            remaining_ns=None,
            progress=progress,
        )
        if result is None:
            raise AcceptanceError("corpus freeze did not complete")
        return Path(result["path"]), result["sha256"], result["document"]
    except BaseException as exc:
        _publish(
            root,
            f"freeze-interruption-{ordinal:08d}.json",
            {
                "schema_version": V5_SCHEMA_VERSION,
                "evidence_kind": "freeze-interruption",
                "anchor_sha256": anchor_sha256,
                "phase": "corpus-freeze",
                "bytes_processed": processed,
                "reason": f"{type(exc).__name__}: {exc}",
                "eligible_for_next_stage": False,
            },
        )
        raise


def published_shards(root: Path, anchor: str) -> tuple[dict[str, Any], str | None]:
    """Reconstruct published prefix without a growing filename/hash list."""
    count = ordinal = 0
    digest, previous = chain_start(AUDIT_DOMAIN, anchor), None
    shards = root / "shards"
    while (shards / f"shard-{ordinal:08d}.json").exists():
        document, sha = read_document(shards / f"shard-{ordinal:08d}.json")
        if (
            document.get("evidence_kind") != "terminal-audit-shard"
            or document.get("anchor") != anchor
            or document.get("ordinal") != ordinal
            or document.get("previous_shard_sha256") != previous
        ):
            raise AcceptanceError("published audit prefix hash/order/anchor differs")
        records = document.get("records")
        if not isinstance(records, list) or not 1 <= len(records) <= MAX_RECORDS_PER_SHARD:
            raise AcceptanceError("published audit prefix record cap differs")
        count += len(records)
        digest, previous = chain_next(digest, ordinal, sha), sha
        ordinal += 1
    seen = 0
    if shards.exists():
        with os.scandir(shards) as entries:
            seen = sum(entry.name.startswith("shard-") for entry in entries)
    if seen != ordinal:
        raise AcceptanceError("missing/extra/duplicate audit prefix shard")
    return {"shard_count": ordinal, "record_count": count, "root_digest": digest}, previous


def _records(root: Path, anchor: str, summary: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
    return shard_records(
        root / "shards",
        domain=AUDIT_DOMAIN,
        anchor=anchor,
        summary=summary,
        kind="terminal-audit-shard",
    )


def _check_identity(identity: DeploymentIdentity) -> None:
    verify_identity_files(
        identity,
        expected_config_path=Path(identity.config_path),
        expected_profile_id=identity.capacity_profile_id,
        require_root_controlled=True,
    )


def run_audit(
    *,
    root: Path,
    corpus: dict[str, Any],
    corpus_sha: str,
    identity: DeploymentIdentity,
    anchor_sha: str,
    predecessor: Mapping[str, Any] | None,
    stage_root: Path | None,
    products: list[list[str]],
    archive_roots: Mapping[str, Path],
    resume: bool,
    probe: Callable[[Path], dict[str, Any]] = quiescence_probe,
    raw_unit: Callable[..., dict[str, Any] | None] = cancellable_unit,
    checkpoint: Callable[[int], None] | None = None,
    progress_callback: Callable[[int], None] | None = None,
    _controlled: bool = False,
) -> tuple[Path, str, dict[str, Any]]:
    if (
        probe is quiescence_probe
        and raw_unit is cancellable_unit
        and checkpoint is None
        and not _controlled
    ):
        try:
            controller_output = cancellable_unit(
                {
                    "operation": "run-audit",
                    "root": str(root),
                    "corpus": corpus,
                    "corpus_sha": corpus_sha,
                    "identity": identity.document(),
                    "anchor": anchor_sha,
                    "predecessor": dict(predecessor) if predecessor else None,
                    "stage_root": str(stage_root) if stage_root else None,
                    "products": products,
                    "archive_roots": {key: str(value) for key, value in archive_roots.items()},
                    "resume": resume,
                },
                remaining_ns=None,
                progress=progress_callback,
            )
            if controller_output is None:
                raise AcceptanceError("terminal controller did not complete")
            return (
                Path(controller_output["path"]),
                controller_output["sha256"],
                controller_output["document"],
            )
        except BaseException as exc:
            latest: dict[str, Any] = {}
            ordinal = 0
            while (root / f"audit-progress-{ordinal:08d}.json").exists():
                latest, _digest = read_document(root / f"audit-progress-{ordinal:08d}.json")
                ordinal += 1
            _publish(
                root,
                f"audit-interruption-{uuid4().hex}.json",
                {
                    "schema_version": V5_SCHEMA_VERSION,
                    "evidence_kind": "audit-interruption",
                    "anchor_sha256": anchor_sha,
                    "quiescence_corpus_sha256": corpus_sha,
                    "published_prefix": published_shards(root, anchor_sha)[0],
                    "phase": "terminal-controller",
                    "bytes_processed": latest.get("bytes_processed", 0),
                    "bytes_scope": "last_durable_checkpoint",
                    "reason": f"{type(exc).__name__}: {exc}",
                    "eligible_for_next_stage": False,
                },
            )
            raise
    data_root = Path(corpus["data_root"])
    certificate = probe(data_root)
    validate_quiescence(certificate)
    if certificate != corpus["quiescence_certificate"]:
        raise AcceptanceError("terminal audit cannot re-establish exact frozen quiescence")
    existing, previous_sha = published_shards(root, anchor_sha)
    if existing["shard_count"] and not resume:
        raise AcceptanceError("published audit prefix requires explicit resume")
    progress_ordinal = 0
    while (root / f"audit-progress-{progress_ordinal:08d}.json").exists():
        progress_ordinal += 1
    last_progress = time.monotonic_ns()
    bytes_progress = 0

    def publish_progress(
        summary: Mapping[str, Any], phase: str, last_record: Mapping[str, Any] | None
    ) -> None:
        nonlocal progress_ordinal, last_progress
        observed = probe(data_root)
        validate_quiescence(observed)
        if observed != certificate:
            raise AcceptanceError("sanctioned mutator/quiescence changed during audit")
        _publish(
            root,
            f"audit-progress-{progress_ordinal:08d}.json",
            {
                "schema_version": V5_SCHEMA_VERSION,
                "evidence_kind": "audit-progress",
                "anchor_sha256": anchor_sha,
                "quiescence_corpus_sha256": corpus_sha,
                "phase": phase,
                "published_prefix": dict(summary),
                "bytes_processed": bytes_progress,
                "last_record": {
                    key: last_record[key] for key in ("family", "key", "authority_sha256")
                }
                if last_record
                else None,
            },
        )
        progress_ordinal += 1
        last_progress = time.monotonic_ns()

    def after_shard(summary: dict[str, Any], last_record: dict[str, Any]) -> None:
        publish_progress(summary, "shard-published", last_record)
        if progress_callback:
            progress_callback(1)
        if checkpoint:
            checkpoint(summary["shard_count"])

    writer = ShardWriter(
        root / "shards",
        domain=AUDIT_DOMAIN,
        anchor=anchor_sha,
        kind="terminal-audit-shard",
        on_publish=after_shard,
    )
    writer.ordinal, writer.count = existing["shard_count"], existing["record_count"]
    writer.digest, writer.previous = existing["root_digest"], previous_sha

    def raw_progress(size: int) -> None:
        nonlocal bytes_progress
        bytes_progress += size
        if progress_callback:
            progress_callback(size)
        if time.monotonic_ns() - last_progress >= AUDIT_PROGRESS_INTERVAL_NS:
            publish_progress(writer.published_summary(), "raw-stream", None)

    executor = CancellableExecutor()
    producer = AuditRecords(
        root=root,
        corpus=corpus,
        identity=identity,
        predecessor=predecessor,
        stage_root=stage_root,
        archive_roots=archive_roots,
        products=products,
        raw_unit=executor if raw_unit is cancellable_unit else raw_unit,
        progress=raw_progress,
        cached_records=(lambda: _records(root, anchor_sha, existing)) if resume else None,
    )
    prior_records = (
        iter(_records(root, anchor_sha, existing)) if existing["record_count"] else iter(())
    )
    try:
        for ordinal, item in enumerate(producer):
            if ordinal < existing["record_count"]:
                prior = next(prior_records, None)
                if prior != item:
                    raise AcceptanceError(
                        "resume prefix is not the exact deterministic corpus record"
                    )
            else:
                writer.add(item)
        if next(prior_records, None) is not None:
            raise AcceptanceError("resume prefix exceeds deterministic terminal corpus")
        writer.flush()
        publish_progress(writer.summary(), "audit-complete", None)
    except BaseException as exc:
        _publish(
            root,
            f"audit-interruption-{progress_ordinal:08d}.json",
            {
                "schema_version": V5_SCHEMA_VERSION,
                "evidence_kind": "audit-interruption",
                "anchor_sha256": anchor_sha,
                "quiescence_corpus_sha256": corpus_sha,
                "published_prefix": writer.published_summary(),
                "bytes_processed": bytes_progress,
                "phase": "record-stream",
                "reason": f"{type(exc).__name__}: {exc}",
                "eligible_for_next_stage": False,
            },
        )
        raise
    finally:
        executor.close()
    findings = sorted(producer.findings)
    result = "FAIL" if findings else "PASS_CANDIDATE"
    anchor_document, _anchor_digest = read_document(
        stage_root / "stage-target.json" if stage_root else root / "baseline-preflight.json"
    )
    document = {
        "schema_version": V5_SCHEMA_VERSION,
        "evidence_kind": "baseline-audit-root" if stage_root is None else "terminal-audit-root",
        "stage": anchor_document.get("stage"),
        "run_id": anchor_document["run_id"],
        "deployment_identity": identity.document(),
        "anchor_sha256": anchor_sha,
        "predecessor": dict(predecessor) if predecessor else None,
        "quiescence_corpus_sha256": corpus_sha,
        "catalog_sha256": corpus["catalog_sha256"],
        "catalog_authority": corpus["catalog_authority"],
        "manifest_freeze": corpus["manifest_freeze"],
        "counts_by_family": producer.counts,
        "shards": writer.summary(),
        "aggregate_sha256": producer.aggregate.hexdigest(),
        "blocking_findings": findings,
        "completion_state": "COMPLETE",
        "result": result,
        "continuation_seed": producer.continuation_seed,
        "configured_products": products,
        "raw_bytes_verified": producer.bytes_verified,
        "archive_bytes_verified": producer.archive_bytes_verified,
        "formal_duration_credit_ns": 0,
    }
    verify_audit(
        root=root,
        identity=identity,
        archive_roots=archive_roots,
        stage_root=stage_root,
        raw_unit=raw_unit,
        _document=document,
        progress=progress_callback,
    )
    path, digest = _publish(root, "audit-root.json", document)
    return path, digest, document


def verify_audit(
    *,
    root: Path,
    identity: DeploymentIdentity,
    archive_roots: Mapping[str, Path],
    stage_root: Path | None = None,
    raw_unit: Callable[..., dict[str, Any] | None] = cancellable_unit,
    historical_control_only: bool = False,
    _document: dict[str, Any] | None = None,
    progress: Callable[[int], None] | None = None,
) -> tuple[dict[str, Any], str]:
    document, digest = (
        read_document(root / "audit-root.json")
        if _document is None
        else (_document, sha256_bytes(canonical_json(_document)))
    )
    if (
        document.get("deployment_identity") != identity.document()
        or document.get("formal_duration_credit_ns") != 0
    ):
        raise AcceptanceError("terminal audit identity/duration authority differs")
    anchor = document["anchor_sha256"]
    corpus, corpus_sha = verify_corpus(root, identity=identity, anchor_sha256=anchor)
    if document.get("quiescence_corpus_sha256") != corpus_sha or (
        document.get("catalog_sha256") != corpus["catalog_sha256"]
        or document.get("catalog_authority") != corpus["catalog_authority"]
        or document.get("manifest_freeze") != corpus["manifest_freeze"]
    ):
        raise AcceptanceError("terminal audit frozen controls differ")
    if stage_root is not None:
        replay = replay_online(stage_root, identity, require_target=True)
        if (
            anchor != replay.target_sha256
            or document.get("predecessor") != replay.start["predecessor"]
            or document.get("stage") != replay.start["stage"]
            or document.get("run_id") != replay.start["run_id"]
            or document.get("configured_products") != replay.start["configured_products"]
            or document.get("evidence_kind") != "terminal-audit-root"
        ):
            raise AcceptanceError("terminal audit online target/predecessor differs")
    else:
        preflight, preflight_sha = read_document(root / "baseline-preflight.json")
        if preflight_sha != anchor or preflight["deployment_identity"] != identity.document():
            raise AcceptanceError("baseline audit preflight authority differs")
        if (
            document.get("evidence_kind") != "baseline-audit-root"
            or document.get("stage") is not None
            or document.get("run_id") != preflight["run_id"]
            or document.get("configured_products") != preflight["configured_products"]
            or document.get("predecessor") is not None
        ):
            raise AcceptanceError("baseline audit run authority differs")
    executor = CancellableExecutor()
    reconstructed = AuditRecords(
        root=root,
        corpus=corpus,
        identity=identity,
        predecessor=document["predecessor"],
        stage_root=stage_root,
        archive_roots=archive_roots,
        products=document["configured_products"],
        raw_unit=executor if raw_unit is cancellable_unit else raw_unit,
        cached_records=(lambda: _records(root, anchor, document["shards"]))
        if historical_control_only
        else None,
        progress=progress,
    )
    actual = iter(_records(root, anchor, document["shards"]))
    count = 0
    try:
        for expected in reconstructed:
            if next(actual, None) != expected:
                raise AcceptanceError("audit record differs from independent exact corpus replay")
            count += 1
            if progress:
                progress(1)
    finally:
        executor.close()
    if next(actual, None) is not None or count != document["shards"]["record_count"]:
        raise AcceptanceError("terminal audit record inventory differs")
    expected_result = "FAIL" if reconstructed.findings else "PASS_CANDIDATE"
    if (
        document.get("result") != expected_result
        or document.get("blocking_findings") != sorted(reconstructed.findings)
        or document.get("counts_by_family") != reconstructed.counts
        or document.get("aggregate_sha256") != reconstructed.aggregate.hexdigest()
        or document.get("continuation_seed") != reconstructed.continuation_seed
        or document.get("completion_state") != "COMPLETE"
        or document.get("raw_bytes_verified") != reconstructed.bytes_verified
        or document.get("archive_bytes_verified") != reconstructed.archive_bytes_verified
    ):
        raise AcceptanceError("terminal audit producer summary differs from reconstruction")
    return document, digest


def baseline(
    *,
    data_root: Path,
    evidence_root: Path,
    identity: DeploymentIdentity,
    products: list[list[str]],
    archive_roots: Mapping[str, Path],
    resume: bool = False,
    probe: Callable[[Path], dict[str, Any]] = quiescence_probe,
    identity_verifier: Callable[[DeploymentIdentity], None] = _check_identity,
    raw_unit: Callable[..., dict[str, Any] | None] = cancellable_unit,
    boot_id: str | None = None,
    checkpoint: Callable[[int], None] | None = None,
) -> tuple[Path, str, dict[str, Any]]:
    root = _safe_evidence_root(evidence_root, data_root)
    identity_verifier(identity)
    if resume:
        _preflight, anchor = read_document(root / "baseline-preflight.json")
        corpus, corpus_sha = verify_corpus(root, identity=identity, anchor_sha256=anchor)
    else:
        _path, anchor, _preflight = baseline_preflight(
            data_root=data_root,
            root=root,
            identity=identity,
            boot_id=boot_id or LinuxClock().boot_id(),
            products=products,
            probe=probe,
        )
        _path, corpus_sha, corpus = cancellable_freeze(
            data_root=data_root,
            root=root,
            identity=identity,
            anchor_sha256=anchor,
            boot_id=boot_id or LinuxClock().boot_id(),
            probe=probe,
        )
    if resume and (root / "audit-root.json").exists():
        document, digest = verify_audit(
            root=root, identity=identity, archive_roots=archive_roots, raw_unit=raw_unit
        )
        return root / "audit-root.json", digest, document
    published = run_audit(
        root=root,
        corpus=corpus,
        corpus_sha=corpus_sha,
        identity=identity,
        anchor_sha=anchor,
        predecessor=None,
        stage_root=None,
        products=products,
        archive_roots=archive_roots,
        resume=resume,
        probe=probe,
        raw_unit=raw_unit,
        checkpoint=checkpoint,
    )
    return published


def final_document(
    replay: OnlineReplay, audit: Mapping[str, Any], audit_sha: str
) -> dict[str, Any]:
    target = replay.target
    if target is None:
        raise AcceptanceError("terminal target is missing")
    findings = sorted(set(target["blocking_findings"]) | set(audit["blocking_findings"]))
    result = "FAIL" if audit["result"] == "FAIL" else target["result"]
    return {
        "schema_version": V5_SCHEMA_VERSION,
        "evidence_kind": "stage-final",
        "stage": target["stage"],
        "run_id": target["run_id"],
        "deployment_identity": target["deployment_identity"],
        "stage_start_sha256": replay.start_sha256,
        "stage_target_sha256": replay.target_sha256,
        "terminal_audit_sha256": audit_sha,
        "elapsed_boottime_ns": target["elapsed_boottime_ns"],
        "blocking_findings": findings,
        "result": result,
        "eligible_for_next_stage": result == "PASS_CANDIDATE" and not findings,
    }


def finalize(
    *,
    evidence_root: Path,
    identity: DeploymentIdentity,
    archive_roots: Mapping[str, Path],
    resume: bool = False,
    probe: Callable[[Path], dict[str, Any]] = quiescence_probe,
    identity_verifier: Callable[[DeploymentIdentity], None] = _check_identity,
    raw_unit: Callable[..., dict[str, Any] | None] = cancellable_unit,
    checkpoint: Callable[[int], None] | None = None,
) -> tuple[Path, str, dict[str, Any]]:
    identity_verifier(identity)
    replay = replay_online(evidence_root, identity, require_target=True)
    if replay.target is None or replay.target_sha256 is None:
        raise AcceptanceError("terminal target is missing")
    root = evidence_root / "terminal-audit"
    if resume:
        corpus, corpus_sha = verify_corpus(
            root, identity=identity, anchor_sha256=replay.target_sha256
        )
    else:
        _path, corpus_sha, corpus = cancellable_freeze(
            data_root=Path(replay.start["data_root"]),
            root=root,
            identity=identity,
            anchor_sha256=replay.target_sha256,
            boot_id=replay.target["boot_id"],
            probe=probe,
        )
    if (root / "audit-root.json").exists() and resume:
        audit, audit_sha = verify_audit(
            root=root,
            identity=identity,
            archive_roots=archive_roots,
            stage_root=evidence_root,
            raw_unit=raw_unit,
        )
    else:
        _path, audit_sha, audit = run_audit(
            root=root,
            corpus=corpus,
            corpus_sha=corpus_sha,
            identity=identity,
            anchor_sha=replay.target_sha256,
            predecessor=replay.start["predecessor"],
            stage_root=evidence_root,
            products=replay.start["configured_products"],
            archive_roots=archive_roots,
            resume=resume,
            probe=probe,
            raw_unit=raw_unit,
            checkpoint=checkpoint,
        )
    document = final_document(replay, audit, audit_sha)
    if resume and (evidence_root / "stage-final.json").exists():
        existing, digest = read_document(evidence_root / "stage-final.json")
        if existing != document:
            raise AcceptanceError("published stage final differs on resume")
        return evidence_root / "stage-final.json", digest, existing
    path, digest = _publish(evidence_root, "stage-final.json", document)
    return path, digest, document


def verify_completed_v5_stage(
    evidence_root: Path,
    identity: DeploymentIdentity,
    *,
    archive_roots: Mapping[str, Path],
    expected_stage: str | None = None,
    require_eligible: bool = True,
    raw_unit: Callable[..., dict[str, Any] | None] = cancellable_unit,
) -> tuple[dict[str, Any], str]:
    replay = replay_online(evidence_root, identity, require_target=True)
    if expected_stage is not None and replay.start["stage"] != expected_stage:
        raise AcceptanceError("V5 completed stage name differs")
    audit, audit_sha = verify_audit(
        root=evidence_root / "terminal-audit",
        identity=identity,
        archive_roots=archive_roots,
        stage_root=evidence_root,
        raw_unit=raw_unit,
    )
    final, digest = read_document(evidence_root / "stage-final.json")
    if final != final_document(replay, audit, audit_sha):
        raise AcceptanceError("V5 stage-final is not independently reconstructible")
    if require_eligible and not final["eligible_for_next_stage"]:
        raise AcceptanceError("V5 completed stage is not eligible")
    return final, digest
