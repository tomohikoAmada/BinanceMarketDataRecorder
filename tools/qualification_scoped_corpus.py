"""Freeze only chunks created by one new owner-policy capture.

The production Catalog and historical Raw are never changed. A private Catalog
copy keeps original row IDs/values and the complete lifecycle of selected chunks.
The released auditor can then audit this explicitly labelled subset in full.
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable, Mapping
from contextlib import closing
from pathlib import Path
from typing import Any

from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_corpus import (
    catalog_authority,
    quiescence_probe,
    read_catalog,
    validate_quiescence,
)
from binance_market_data_recorder.service.acceptance_v5_io import (
    FREEZE_DOMAIN,
    V5_SCHEMA_VERSION,
    ShardWriter,
    hash_file,
    publish,
    snapshot_manifest,
)
from binance_market_data_recorder.service.deployment_identity import DeploymentIdentity
from binance_market_data_recorder.storage.acceptance_delta import DeltaSnapshot
from binance_market_data_recorder.storage.layout import fsync_directory


def capture_boundary(
    data_root: Path, *, inventory_root: Path, utc_ns: int, boot_id: str
) -> dict[str, Any]:
    """A metadata cursor before enabling Recorder; no historical integrity scan."""
    with read_catalog(data_root / "state/catalog.sqlite") as db:
        db.execute("BEGIN")
        high = DeltaSnapshot(db).high_water
        counts = db.execute(
            "SELECT count(*),sum(record_count),sum(stored_bytes),"
            "sum(uncompressed_bytes) FROM chunks"
        ).fetchone()
        active = db.execute("SELECT 1 FROM chunks WHERE state!='LOCAL_DELETED' LIMIT 1").fetchone()
        if active is not None:
            raise AcceptanceError("new capture boundary requires drained previous chunks")
        inventory_root.mkdir(parents=True, exist_ok=True)
        inventory = inventory_root / "start-catalog.sqlite"
        with inventory.open("xb"):
            pass
        with closing(sqlite3.connect(inventory)) as backup:
            db.backup(backup)
            backup.execute("PRAGMA journal_mode=DELETE")
        with inventory.open("rb") as inp:
            os.fsync(inp.fileno())
        fsync_directory(inventory_root)
    return {
        "kind": "OWNER_NEW_CAPTURE_BOUNDARY",
        "utc_ns": utc_ns,
        "boot_id": boot_id,
        "high_water": high,
        "start_counts": [int(value or 0) for value in counts],
        "start_catalog_path": str(inventory.resolve()),
        "start_catalog_sha256": hash_file(inventory)[1],
        "scope": "all new chunks including this attempt's warm-up; no historical Raw",
    }


def select_catalog(source: Path, root: Path, boundary: Mapping[str, Any]) -> dict[str, Any]:
    """Copy metadata, then select new identities without renumbering any ledger."""
    high = boundary["high_water"]
    if set(high) != {"chunk", "archive", "operational"} or any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in high.values()
    ):
        raise AcceptanceError("invalid capture boundary")
    original = root / "source-catalog.sqlite"
    second = root / "source-catalog-check.sqlite"
    selected = root / "catalog.sqlite"
    root.mkdir(parents=True, exist_ok=True)
    for path in (original, second, selected):
        with path.open("xb"):
            pass
    with read_catalog(source) as incoming, closing(sqlite3.connect(original)) as backup:
        incoming.backup(backup)
        backup.execute("PRAGMA journal_mode=DELETE")
    with read_catalog(source) as incoming, closing(sqlite3.connect(second)) as backup:
        incoming.backup(backup)
        backup.execute("PRAGMA journal_mode=DELETE")
    if hash_file(original) != hash_file(second):
        raise AcceptanceError("terminal metadata changed between snapshots")
    with (
        read_catalog(original, immutable=True) as incoming,
        closing(sqlite3.connect(selected)) as db,
    ):
        incoming.backup(db)
        db.execute("PRAGMA journal_mode=DELETE")
        source_high = DeltaSnapshot(incoming).high_water
        if any(source_high[key] < high[key] for key in high):
            raise AcceptanceError("capture cursor exceeds terminal Catalog")
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN")
        db.execute(
            "CREATE TEMP TABLE selected_chunks AS SELECT chunk_id FROM chunk_transitions "
            "WHERE transition_id>? AND from_state IS NULL",
            (high["chunk"],),
        )
        count = db.execute("SELECT count(*) FROM selected_chunks").fetchone()[0]
        start_catalog = Path(boundary["start_catalog_path"])
        if hash_file(start_catalog)[1] != boundary["start_catalog_sha256"]:
            raise AcceptanceError("start chunk inventory changed")
        with read_catalog(start_catalog, immutable=True) as start:
            if DeltaSnapshot(start).high_water != high:
                raise AcceptanceError("start cursor differs from inventory")
            prior = {row[0] for row in start.execute("SELECT chunk_id FROM chunks")}
        all_ids = {row[0] for row in db.execute("SELECT chunk_id FROM chunks")}
        selected_ids = {row[0] for row in db.execute("SELECT chunk_id FROM selected_chunks")}
        if not prior.issubset(all_ids) or all_ids - prior != selected_ids:
            raise AcceptanceError("new chunk membership differs from start/end inventory")
        if count == 0:
            raise AcceptanceError("new capture contains no saved chunks")
        db.execute(
            "DELETE FROM archive_transaction_events WHERE transaction_id NOT IN "
            "(SELECT transaction_id FROM archive_transactions WHERE chunk_id IN "
            "(SELECT chunk_id FROM selected_chunks))"
        )
        if db.execute(
            "SELECT 1 FROM sqlite_master WHERE name='remote_archive_transactions'"
        ).fetchone():
            db.execute(
                "DELETE FROM remote_archive_events WHERE receipt_id NOT IN "
                "(SELECT receipt_id FROM remote_archive_transactions WHERE chunk_id IN "
                "(SELECT chunk_id FROM selected_chunks))"
            )
            db.execute(
                "DELETE FROM remote_archive_transactions WHERE chunk_id NOT IN "
                "(SELECT chunk_id FROM selected_chunks)"
            )
        for table in ("archive_transactions", "chunk_transitions", "chunks"):
            db.execute(
                f"DELETE FROM {table} WHERE chunk_id NOT IN (SELECT chunk_id FROM selected_chunks)"
            )
        # A completion written during this capture can close a gap opened before
        # its boundary. Keep that exact start as metadata authority, without
        # selecting any prior chunk, Raw or duration.
        db.execute("CREATE TEMP TABLE retained_operational(event_id TEXT PRIMARY KEY)")
        db.execute(
            "INSERT INTO retained_operational SELECT event_id FROM operational_event_sequence "
            "WHERE event_seq>?",
            (high["operational"],),
        )
        db.execute(
            "INSERT OR IGNORE INTO retained_operational "
            "SELECT started.event_id FROM operational_events started "
            "JOIN operational_events completed ON started.market=completed.market "
            "AND started.symbol=completed.symbol AND started.stream=completed.stream "
            "AND started.gap_id=completed.gap_id "
            "JOIN operational_event_sequence s ON s.event_id=completed.event_id "
            "WHERE started.event_type='STREAM_DISCONTINUITY_STARTED' "
            "AND completed.event_type='STREAM_DISCONTINUITY_COMPLETED' AND s.event_seq>?",
            (high["operational"],),
        )
        db.execute(
            "DELETE FROM operational_event_sequence WHERE event_id NOT IN "
            "(SELECT event_id FROM retained_operational)"
        )
        db.execute(
            "DELETE FROM operational_events WHERE event_id NOT IN "
            "(SELECT event_id FROM operational_event_sequence)"
        )
        db.commit()
        db.execute("VACUUM")
        db.row_factory = sqlite3.Row
        authority = catalog_authority(db)
        counts = db.execute(
            "SELECT count(*),sum(record_count),sum(stored_bytes),"
            "sum(uncompressed_bytes) FROM chunks"
        ).fetchone()
        if (
            not counts[1]
            or counts[0] != count
            or db.execute(
                "SELECT 1 FROM chunks WHERE state NOT IN ('SEALED','LOCAL_DELETED') LIMIT 1"
            ).fetchone()
        ):
            raise AcceptanceError("selected capture is not completely sealed/drained")
    for path in (original, second, selected):
        with path.open("rb") as inp:
            os.fsync(inp.fileno())
    fsync_directory(root)
    return {
        "kind": "OWNER_SELECTED_CAPTURE_CORPUS",
        "scope": boundary["scope"],
        "boundary": dict(boundary),
        "source_high_water": source_high,
        "source_catalog_sha256": hash_file(original)[1],
        "selected_catalog_sha256": hash_file(selected)[1],
        "selected_counts": dict(
            zip(("chunks", "frames", "stored_bytes", "decoded_bytes"), counts, strict=True)
        ),
        "catalog_authority": authority,
        "canonical_v5_final_eligible": False,
    }


def freeze_selected_corpus(
    *,
    data_root: Path,
    root: Path,
    identity: DeploymentIdentity,
    boundary: Mapping[str, Any],
    anchor_sha256: str,
    boot_id: str,
    probe: Callable[[Path], dict[str, Any]] = quiescence_probe,
) -> tuple[Path, str, dict[str, Any]]:
    if str(data_root.resolve()) != identity.systemd_effective["working_directory"]:
        raise AcceptanceError("selected data root differs from deployment")
    if boundary["boot_id"] != boot_id:
        raise AcceptanceError("capture boundary boot differs")
    certificate = probe(data_root)
    validate_quiescence(certificate)
    receipt = select_catalog(data_root / "state/catalog.sqlite", root, boundary)
    digest = receipt["selected_catalog_sha256"]
    writer = ShardWriter(
        root / "manifest-freeze",
        domain=FREEZE_DOMAIN,
        anchor=digest,
        kind="manifest-freeze-shard",
    )
    with read_catalog(root / "catalog.sqlite", immutable=True) as db:
        for chunk in db.execute("SELECT chunk_id,manifest_path FROM chunks ORDER BY manifest_path"):
            if chunk["manifest_path"] is None:
                raise AcceptanceError("selected chunk has no manifest")
            mapping = snapshot_manifest(data_root, chunk["manifest_path"], root)
            if mapping["chunk_id"] != chunk["chunk_id"]:
                raise AcceptanceError("selected manifest identity differs")
            writer.add(mapping)
    writer.flush()
    postfreeze = root / "source-catalog-postfreeze.sqlite"
    with postfreeze.open("xb"):
        pass
    with (
        read_catalog(data_root / "state/catalog.sqlite") as incoming,
        closing(sqlite3.connect(postfreeze)) as backup,
    ):
        incoming.backup(backup)
        backup.execute("PRAGMA journal_mode=DELETE")
    if hash_file(postfreeze)[1] != receipt["source_catalog_sha256"]:
        raise AcceptanceError("terminal metadata changed during manifest freeze")
    if probe(data_root) != certificate:
        raise AcceptanceError("selected corpus lost quiescence")
    publish(root, "owner-scope-receipt.json", receipt)
    document = {
        "schema_version": V5_SCHEMA_VERSION,
        "evidence_kind": "quiescence-corpus",
        "anchor_sha256": anchor_sha256,
        "deployment_identity": identity.document(),
        "data_root": str(data_root.resolve()),
        "boot_id": boot_id,
        "catalog_sha256": digest,
        "catalog_authority": receipt["catalog_authority"],
        "manifest_freeze": writer.summary(),
        "quiescence_certificate": certificate,
        "owner_selected_scope": receipt,
    }
    path, digest = publish(root, "quiescence-corpus.json", document)
    return path, digest, document


def audit_selected_capture(
    *,
    root: Path,
    data_root: Path,
    identity: DeploymentIdentity,
    boundary: Mapping[str, Any],
    run_id: str,
    target_sha256: str,
    boot_id: str,
    products: list[list[str]],
    archive_roots: Mapping[str, Path],
) -> dict[str, Any]:
    """Reuse released LIVE auditor; publish an explicitly scoped owner result."""
    from binance_market_data_recorder.service.acceptance_v5_finalize import run_audit, verify_audit

    _path, anchor = publish(
        root,
        "baseline-preflight.json",
        {
            "schema_version": V5_SCHEMA_VERSION,
            "evidence_kind": "owner-selected-preflight",
            "run_id": run_id,
            "configured_products": products,
            "deployment_identity": identity.document(),
            "owner_selected_scope": dict(boundary),
            "owner_target_sha256": target_sha256,
            "canonical_v5_final_eligible": False,
        },
    )
    _path, corpus_sha, corpus = freeze_selected_corpus(
        data_root=data_root,
        root=root,
        identity=identity,
        boundary=boundary,
        anchor_sha256=anchor,
        boot_id=boot_id,
    )
    _path, audit_sha, audit = run_audit(
        root=root,
        corpus=corpus,
        corpus_sha=corpus_sha,
        identity=identity,
        anchor_sha=anchor,
        predecessor=None,
        stage_root=None,
        products=products,
        archive_roots=archive_roots,
        resume=False,
    )
    document, verify_sha = verify_audit(
        root=root, identity=identity, archive_roots=archive_roots, historical_control_only=True
    )
    if document != audit or verify_sha != audit_sha or audit["result"] != "PASS_CANDIDATE":
        raise AcceptanceError("selected saved-data audit did not completely pass")
    result = {
        "kind": "OWNER_SELECTED_SAVED_DATA_AUDIT",
        "completion_state": "COMPLETE",
        "result": "PASS_SAVED_DATA_PENDING_INDEPENDENT_REVIEW",
        "run_id": run_id,
        "owner_target_sha256": target_sha256,
        "audit_sha256": audit_sha,
        "completed_control_verification_sha256": verify_sha,
        "live_passes": 2,
        "completed_verification_mode": "IMMUTABLE_CONTROL_RECONSTRUCTION_AFTER_DUAL_LIVE",
        "scope_receipt_sha256": hash_file(root / "owner-scope-receipt.json")[1],
        "selected_counts": corpus["owner_selected_scope"]["selected_counts"],
        "observed_saved_frame_contract_conformance": "100%",
        "scope": boundary["scope"],
        "canonical_v5_final_eligible": False,
        "limitations": (
            "No historical Raw audit, pre-start reconnect predecessor, "
            "canonical online stage, or every-exchange-event completeness claim."
        ),
    }
    publish(root, "owner-selected-result.json", result)
    return result
