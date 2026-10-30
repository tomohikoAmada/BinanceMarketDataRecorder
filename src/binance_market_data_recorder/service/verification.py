"""Durable, bounded verification of newly sealed Raw; never a deletion authority."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping
from pathlib import Path

from ..spool.readback import file_signature, read_artifact
from ..spool.seal import (
    INCOMPLETE_FLAGS,
    _boundary_local_evidence_safe,
    parse_strict_manifest_bytes,
)
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from ..storage.source_location import archived_artifact, safe_relative
from .lock import ServiceAlreadyRunning, ServiceProcessLock

VERIFIER_VERSION = "raw-independent.v1"


def verify_chunk(
    layout: StorageLayout,
    catalog: Catalog,
    row: dict[str, object],
    external_roots: Mapping[str, Path],
) -> dict[str, object]:
    manifest_path = safe_relative(layout.root, row["manifest_path"])
    manifest_bytes = manifest_path.read_bytes()
    manifest = parse_strict_manifest_bytes(
        manifest_bytes, path=manifest_path, recorder_root=layout.root
    )
    for key in (
        "chunk_id",
        "record_count",
        "stored_bytes",
        "stored_sha256",
        "uncompressed_bytes",
        "uncompressed_sha256",
    ):
        if manifest[key] != row[key]:
            raise ValueError(f"Catalog/manifest {key} mismatch")
    artifact = safe_relative(layout.root, row["sealed_path"])
    if not artifact.is_file():
        artifact = (
            archived_artifact(
                chunk_id=str(row["chunk_id"]), catalog=catalog, external_roots=external_roots
            )
            or artifact
        )
    stored_bound, expanded_bound = row["stored_bytes"], row["uncompressed_bytes"]
    assert isinstance(stored_bound, int) and isinstance(expanded_bound, int)
    result = read_artifact(
        artifact,
        scan_frames=True,
        bytes_per_second=16 * 1024 * 1024,
        max_stored_bytes=stored_bound,
        max_uncompressed_bytes=expanded_bound,
    )
    for key in ("stored_bytes", "stored_sha256", "uncompressed_bytes", "uncompressed_sha256"):
        if getattr(result, key) != manifest[key]:
            raise ValueError(f"Raw {key} mismatch")
    scan = result.scan
    assert scan is not None and scan.header is not None
    header, stats = scan.header, scan.statistics
    for key in (
        "chunk_id",
        "market",
        "symbol",
        "stream",
        "collector_version",
        "created_at_utc_ns",
        "chunk_schema_version",
        "envelope_schema_version",
    ):
        if str(getattr(header, key)) != str(manifest[key]):
            raise ValueError(f"Raw header {key} mismatch")
    expected: dict[str, object] = {
        "record_count": stats.record_count,
        "sequence_ranges": stats.sequence_ranges(),
        "exchange_time_ranges": {
            key: {"min": values[0], "max": values[1]}
            for key, values in stats.exchange_time_ranges.items()
        },
        "receive_time_utc_range_ns": {
            "min": stats.receive_time_utc_min_ns,
            "max": stats.receive_time_utc_max_ns,
        },
        "receive_monotonic_range_ns": {
            "min": stats.receive_monotonic_min_ns,
            "max": stats.receive_monotonic_max_ns,
        },
        "connection_ids": sorted(stats.connection_ids),
        "collector_instance_ids": sorted(stats.collector_instance_ids)
        or [header.collector_instance_id],
    }
    for key, value in expected.items():
        if manifest[key] != value:
            raise ValueError(f"Raw {key} statistics mismatch")
    flags = manifest["capture_flags"]
    assert isinstance(flags, list)
    if not stats.capture_flags <= set(flags):
        raise ValueError("Raw capture flags missing from manifest")
    if stats.capture_flags & INCOMPLETE_FLAGS and manifest["complete"]:
        raise ValueError("incomplete Raw marked complete")
    if (
        len(stats.connection_ids) > 1
        and manifest["complete"]
        and not _boundary_local_evidence_safe(scan.connection_transitions)
    ):
        raise ValueError("Raw connection transition lacks completeness evidence")
    if result.file_signature != file_signature(artifact.stat()):
        raise ValueError("artifact changed after readback")
    if manifest_path.read_bytes() != manifest_bytes:
        raise ValueError("manifest changed during verification")
    return {
        "chunk_id": row["chunk_id"],
        "manifest_path": row["manifest_path"],
        "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "verifier_version": VERIFIER_VERSION,
        "verified_at_utc_ns": time.time_ns(),
        "artifact_path": str(artifact),
        "result": "VERIFIED",
        **{
            key: manifest[key]
            for key in (
                "record_count",
                "stored_bytes",
                "stored_sha256",
                "uncompressed_bytes",
                "uncompressed_sha256",
                "market",
                "symbol",
                "stream",
                "sequence_ranges",
                "receive_time_utc_range_ns",
                "gap",
                "complete",
            )
        },
    }


def verify_new(
    layout: StorageLayout,
    catalog: Catalog,
    *,
    max_files: int = 32,
    max_bytes: int = 512 * 1024 * 1024,
    max_seconds: float = 30,
    external_roots: Mapping[str, Path] | None = None,
    through_utc_ns: int | None = None,
) -> dict[str, object]:
    if max_files <= 0 or max_bytes <= 0 or max_seconds <= 0:
        raise ValueError("verification budgets must be positive")
    lock = ServiceProcessLock(layout.state / "raw-verification.lock")
    try:
        lock.acquire()
    except ServiceAlreadyRunning:
        return {"status": "ALREADY_RUNNING"}
    processed = failed = byte_count = 0
    budget_blocked: list[str] = []
    started = time.monotonic()
    try:
        for row in catalog.pending_raw_verifications(
            now=time.time_ns(), limit=max_files, through_utc_ns=through_utc_ns
        ):
            charge = int(row["stored_bytes"]) + int(row["uncompressed_bytes"])
            if time.monotonic() - started >= max_seconds:
                break
            if byte_count + charge > max_bytes:
                budget_blocked.append(str(row["chunk_id"]))
                continue
            try:
                evidence = verify_chunk(layout, catalog, row, external_roots or {})
                catalog.record_raw_verification(str(row["chunk_id"]), evidence=evidence)
            except Exception as exc:
                failed += 1
                failure_id = f"raw-verification-failed:{row['chunk_id']}:{VERIFIER_VERSION}"
                # The verifier lock owns this ID. Preserve its first exact failure;
                # retry timestamps/errors belong to the mutable queue, not that event.
                if catalog.operational_event_by_id(failure_id) is None:
                    catalog.ensure_operational_event(
                        event_id=failure_id,
                        event_type="RAW_VERIFICATION_FAILED",
                        occurred_at_utc_ns=time.time_ns(),
                        evidence={
                            "chunk_id": row["chunk_id"],
                            "error_type": type(exc).__name__,
                            "error": str(exc),
                            "verifier_version": VERIFIER_VERSION,
                        },
                    )
                catalog.record_raw_verification(
                    str(row["chunk_id"]), error=f"{type(exc).__name__}: {exc}"
                )
            processed += 1
            byte_count += charge
        return {
            **catalog.raw_verification_status(through_utc_ns=through_utc_ns),
            "processed_files": processed,
            "failed_this_batch": failed,
            "read_budget_bytes": byte_count,
            "elapsed_seconds": time.monotonic() - started,
            "max_bytes": max_bytes,
            "max_files": max_files,
            "budget_blocked_chunk_ids": budget_blocked,
            "read_rate_bytes_per_second": 16 * 1024 * 1024,
        }
    finally:
        lock.release()
