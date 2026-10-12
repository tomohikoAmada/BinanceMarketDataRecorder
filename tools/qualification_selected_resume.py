"""Continue a frozen owner-selected audit with genuine fresh quiescence.

Only selected immutable controls are copied. This never reselects a Catalog,
reads historical Raw, creates a capture boundary, or grants capture duration.
Callers bind their existing owner start/target and arrange bounded protection.
"""

from __future__ import annotations

import copy
import json
import os
import shutil
from pathlib import Path
from typing import Any

from binance_market_data_recorder.service.acceptance import (
    AcceptanceError,
    canonical_json,
    sha256_bytes,
)
from binance_market_data_recorder.service.acceptance_v5_corpus import (
    quiescence_probe,
    verify_corpus,
)
from binance_market_data_recorder.service.acceptance_v5_finalize import run_audit, verify_audit
from binance_market_data_recorder.service.acceptance_v5_io import (
    frozen_manifests,
    hash_file,
    open_exact,
    publish,
    read_document,
)
from binance_market_data_recorder.service.deployment_identity import DeploymentIdentity
from binance_market_data_recorder.storage.layout import fsync_directory


def prepare_selected_continuation(
    *,
    source: Path,
    root: Path,
    identity: DeploymentIdentity,
    expected: dict[str, str],
    start_sha256: str,
    target_sha256: str,
    run_id: str,
    boot_id: str,
) -> tuple[dict[str, Any], str]:
    """Copy a verified immutable selection; refresh only actual audit protection."""
    if root.exists() or root.resolve() == source.resolve():
        raise AcceptanceError("selected continuation requires a new exclusive root")
    preflight, anchor = read_document(source / "baseline-preflight.json")
    # This private receipt predates V5 schema tagging; retain its exact bytes.
    with open_exact(source, "owner-scope-receipt.json") as inp:
        body = inp.read(65537)
    if len(body) > 65536:
        raise AcceptanceError("selected receipt exceeds its byte bound")
    receipt = json.loads(body)
    if not isinstance(receipt, dict) or canonical_json(receipt) != body:
        raise AcceptanceError("selected receipt is not canonical")
    receipt_sha = sha256_bytes(body)
    corpus, corpus_sha = verify_corpus(source, identity=identity, anchor_sha256=anchor)
    if expected != {"preflight": anchor, "receipt": receipt_sha, "corpus": corpus_sha}:
        raise AcceptanceError("selected source binding changed")
    if (
        preflight.get("run_id") != run_id
        or preflight.get("owner_target_sha256") != target_sha256
        or preflight.get("deployment_identity") != identity.document()
        or preflight.get("owner_selected_scope") != receipt["boundary"]
        or corpus.get("owner_selected_scope") != receipt
        or corpus.get("boot_id") != boot_id
        or receipt["boundary"].get("boot_id") != boot_id
    ):
        raise AcceptanceError("selected owner run/target/boundary identity changed")
    certificate = quiescence_probe(Path(corpus["data_root"]))
    root.mkdir(mode=0o700)

    def exact_copy(relative: str) -> None:
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        with (source / relative).open("rb") as inp, destination.open("xb") as out:
            shutil.copyfileobj(inp, out)
            out.flush()
            os.fsync(out.fileno())
        if hash_file(source / relative) != hash_file(destination):
            raise AcceptanceError("selected control copy changed")
        fsync_directory(destination.parent)

    for name in ("catalog.sqlite", "baseline-preflight.json", "owner-scope-receipt.json"):
        exact_copy(name)
    # The verified freeze is an exact chained shard set; no production inventory.
    shard_names = {
        f"shard-{ordinal:08d}.json" for ordinal in range(corpus["manifest_freeze"]["shard_count"])
    }
    members = list((source / "manifest-freeze").iterdir())
    if {path.name for path in members} != shard_names or any(
        not path.is_file() or path.is_symlink() for path in members
    ):
        raise AcceptanceError("unexpected frozen mapping member")
    for name in sorted(shard_names):
        exact_copy("manifest-freeze/" + name)
    for mapping, _manifest in frozen_manifests(source, corpus):
        relative = mapping["object"]
        if not (root / relative).exists():
            exact_copy(relative)
    if (
        hash_file(root / "baseline-preflight.json")[1] != anchor
        or hash_file(root / "owner-scope-receipt.json")[1] != receipt_sha
        or hash_file(source / "quiescence-corpus.json")[1] != corpus_sha
    ):
        raise AcceptanceError("selected source controls changed during copy")
    continued = copy.deepcopy(corpus)
    continued["quiescence_certificate"] = certificate
    continued["owner_audit_continuation"] = {
        "source_root": str(source.resolve()),
        "source_quiescence_corpus_sha256": corpus_sha,
        "source_preflight_sha256": anchor,
        "source_scope_receipt_sha256": receipt_sha,
        "owner_start_sha256": start_sha256,
        "owner_target_sha256": target_sha256,
        "run_id": run_id,
        "scope_unchanged": True,
        "new_capture": False,
        "formal_duration_credit_ns": 0,
    }
    _path, digest = publish(root, "quiescence-corpus.json", continued)
    if verify_corpus(root, identity=identity, anchor_sha256=anchor) != (continued, digest):
        raise AcceptanceError("selected continuation readback changed")
    if quiescence_probe(Path(corpus["data_root"])) != certificate:
        raise AcceptanceError("selected copy lost actual quiescence")
    return continued, digest


def run_selected_continuation(
    *, root: Path, identity: DeploymentIdentity, archive_roots: dict[str, Path]
) -> dict[str, Any]:
    """Released default spawn executor performs both full LIVE passes."""
    preflight, anchor = read_document(root / "baseline-preflight.json")
    corpus, corpus_sha = verify_corpus(root, identity=identity, anchor_sha256=anchor)
    _path, audit_sha, audit = run_audit(
        root=root,
        corpus=corpus,
        corpus_sha=corpus_sha,
        identity=identity,
        anchor_sha=anchor,
        predecessor=None,
        stage_root=None,
        products=preflight["configured_products"],
        archive_roots=archive_roots,
        resume=True,
    )
    document, verify_sha = verify_audit(
        root=root,
        identity=identity,
        archive_roots=archive_roots,
        historical_control_only=True,
    )
    if (
        document != audit
        or verify_sha != audit_sha
        or audit["result"] != "PASS_CANDIDATE"
        or audit["blocking_findings"]
    ):
        raise AcceptanceError("selected continuation did not completely pass")
    result = {
        "kind": "OWNER_SELECTED_SAVED_DATA_AUDIT_CONTINUATION",
        "completion_state": "COMPLETE",
        "result": "PASS_SAVED_DATA_PENDING_INDEPENDENT_REVIEW",
        "run_id": preflight["run_id"],
        "owner_target_sha256": preflight["owner_target_sha256"],
        "audit_sha256": audit_sha,
        "completed_control_verification_sha256": verify_sha,
        "live_passes": 2,
        "completed_verification_mode": "IMMUTABLE_CONTROL_RECONSTRUCTION_AFTER_DUAL_LIVE",
        "scope_receipt_sha256": hash_file(root / "owner-scope-receipt.json")[1],
        "selected_counts": corpus["owner_selected_scope"]["selected_counts"],
        "observed_saved_frame_contract_conformance": "100%",
        "continuation": corpus["owner_audit_continuation"],
        "canonical_v5_final_eligible": False,
        "limitations": "Selected saved frames only; no every-exchange-event completeness claim.",
    }
    publish(root, "owner-selected-result.json", result)
    return result
