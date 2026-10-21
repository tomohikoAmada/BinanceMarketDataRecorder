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
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from binance_market_data_recorder.service.acceptance import (
    AcceptanceError,
    canonical_json,
    sha256_bytes,
)
from binance_market_data_recorder.service.acceptance_v5_corpus import (
    catalog_authority,
    quiescence_probe,
    read_catalog,
    verify_corpus,
)
from binance_market_data_recorder.service.acceptance_v5_delta import _companions
from binance_market_data_recorder.service.acceptance_v5_finalize import run_audit, verify_audit
from binance_market_data_recorder.service.acceptance_v5_io import (
    FREEZE_DOMAIN,
    ShardWriter,
    frozen_manifests,
    hash_file,
    open_exact,
    publish,
    read_document,
)
from binance_market_data_recorder.service.deployment_identity import DeploymentIdentity
from binance_market_data_recorder.storage.acceptance_delta import (
    TRIGGER_SQL,
    DeltaSnapshot,
    validate_sequence,
)
from binance_market_data_recorder.storage.layout import fsync_directory


def repair_discontinuity_context(
    catalog: Path, source: Path, *, source_sha256: str, boundary: int
) -> dict[str, Any]:
    """Restore only exact causal start rows in a new private Catalog copy."""
    if os.path.samefile(catalog, source):
        raise AcceptanceError("causal repair requires a private Catalog copy")
    if hash_file(source)[1] != source_sha256:
        raise AcceptanceError("operational source Catalog binding changed")
    prior_sha = hash_file(catalog)[1]
    added = []
    with read_catalog(source, immutable=True) as original, closing(sqlite3.connect(catalog)) as db:
        db.row_factory = sqlite3.Row
        validate_sequence(db)
        counter = db.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='operational_event_sequence'"
        ).fetchone()
        db.execute("BEGIN IMMEDIATE")
        # Restore original sequence IDs, rather than firing the online append trigger.
        # DDL and both inserts are atomic; rollback also restores the original trigger.
        db.execute("DROP TRIGGER operational_event_sequence_insert")
        snapshot = DeltaSnapshot(original)
        completed = [
            dict(row)
            for row in db.execute(
                "SELECT s.event_seq,e.* FROM operational_event_sequence s "
                "JOIN operational_events e ON e.event_id=s.event_id "
                "WHERE s.event_seq>? AND e.event_type='STREAM_DISCONTINUITY_COMPLETED' "
                "ORDER BY s.event_seq",
                (boundary,),
            )
        ]
        for row in completed:
            source_row = snapshot.exact("operational_events", "event_id", row["event_id"])
            sequence = snapshot.exact("operational_event_sequence", "event_id", row["event_id"])
            if (
                source_row is None
                or sequence is None
                or {**source_row, "event_seq": sequence["event_seq"]} != row
            ):
                raise AcceptanceError("selected completion differs from frozen source")
            pair = snapshot.discontinuity_pair(row)
            _companions("operational", row, {"discontinuity": pair}, snapshot.high_water)
            for start in pair:
                if start["event_type"] != "STREAM_DISCONTINUITY_STARTED":
                    continue
                present = db.execute(
                    "SELECT s.event_seq,e.* FROM operational_event_sequence s "
                    "JOIN operational_events e ON e.event_id=s.event_id WHERE e.event_id=?",
                    (start["event_id"],),
                ).fetchone()
                if present is not None:
                    if dict(present) != start:
                        raise AcceptanceError("retained start differs from frozen source")
                    continue
                if start["event_seq"] > boundary:
                    raise AcceptanceError("missing post-boundary start is not a causal repair")
                event = {key: value for key, value in start.items() if key != "event_seq"}
                columns = list(event)
                db.execute(
                    "INSERT INTO operational_events ("
                    + ",".join(columns)
                    + ") VALUES ("
                    + ",".join("?" for _ in columns)
                    + ")",
                    tuple(event.values()),
                )
                db.execute(
                    "INSERT INTO operational_event_sequence(event_seq,event_id) VALUES (?,?)",
                    (start["event_seq"], start["event_id"]),
                )
                added.append(start)
        db.execute(TRIGGER_SQL)
        validate_sequence(db)
        after = db.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='operational_event_sequence'"
        ).fetchone()
        if tuple(after or ()) != tuple(counter or ()):
            raise AcceptanceError("causal repair changed operational sequence high water")
        db.commit()
    with catalog.open("rb") as inp:
        os.fsync(inp.fileno())
    if hash_file(source)[1] != source_sha256:
        raise AcceptanceError("operational source changed during causal repair")
    return {
        "source_catalog_sha256": source_sha256,
        "original_selected_catalog_sha256": prior_sha,
        "added_operational_rows": added,
        "new_capture": False,
    }


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
    operational_source: tuple[Path, str] | None = None,
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

    for name in ("catalog.sqlite", "baseline-preflight.json"):
        exact_copy(name)
    repair = None
    if operational_source is not None:
        repair = repair_discontinuity_context(
            root / "catalog.sqlite",
            operational_source[0],
            source_sha256=operational_source[1],
            boundary=receipt["boundary"]["high_water"]["operational"],
        )
        receipt = copy.deepcopy(receipt)
        receipt["selected_catalog_sha256"] = hash_file(root / "catalog.sqlite")[1]
        with read_catalog(root / "catalog.sqlite", immutable=True) as db:
            receipt["catalog_authority"] = catalog_authority(db)
        receipt["operational_context_repair"] = repair
        publish(root, "owner-scope-receipt.json", receipt)
    else:
        exact_copy("owner-scope-receipt.json")
    # The verified freeze is an exact chained shard set; no production inventory.
    shard_names = {
        f"shard-{ordinal:08d}.json" for ordinal in range(corpus["manifest_freeze"]["shard_count"])
    }
    members = list((source / "manifest-freeze").iterdir())
    if {path.name for path in members} != shard_names or any(
        not path.is_file() or path.is_symlink() for path in members
    ):
        raise AcceptanceError("unexpected frozen mapping member")
    if repair is None:
        for name in sorted(shard_names):
            exact_copy("manifest-freeze/" + name)
    writer = (
        ShardWriter(
            root / "manifest-freeze",
            domain=FREEZE_DOMAIN,
            anchor=receipt["selected_catalog_sha256"],
            kind="manifest-freeze-shard",
        )
        if repair is not None
        else None
    )
    for mapping, _manifest in frozen_manifests(source, corpus):
        relative = mapping["object"]
        if not (root / relative).exists():
            exact_copy(relative)
        if writer is not None:
            writer.add(mapping)
    if writer is not None:
        writer.flush()
    if (
        hash_file(root / "baseline-preflight.json")[1] != anchor
        or (repair is None and hash_file(root / "owner-scope-receipt.json")[1] != receipt_sha)
        or hash_file(source / "baseline-preflight.json")[1] != anchor
        or hash_file(source / "owner-scope-receipt.json")[1] != receipt_sha
        or hash_file(source / "quiescence-corpus.json")[1] != corpus_sha
    ):
        raise AcceptanceError("selected source controls changed during copy")
    continued = copy.deepcopy(corpus)
    if repair is not None:
        assert writer is not None
        continued.update(
            catalog_sha256=receipt["selected_catalog_sha256"],
            catalog_authority=receipt["catalog_authority"],
            owner_selected_scope=receipt,
            manifest_freeze=writer.summary(),
        )
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
    if repair is not None:
        continued["owner_audit_continuation"]["operational_context_repair"] = repair
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
