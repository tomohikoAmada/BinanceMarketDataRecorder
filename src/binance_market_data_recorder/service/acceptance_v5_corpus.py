"""Stopped/quiescent frozen controls, never a Recorder supervisor (ADR-0034)."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import subprocess
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..storage.acceptance_delta import DeltaSnapshot, ordered_rows, validate_sequence
from ..storage.catalog import Catalog
from ..storage.layout import fsync_directory
from .acceptance import AcceptanceError, canonical_json
from .acceptance_v5_io import (
    V5_SCHEMA_VERSION,
    freeze_manifests,
    frozen_manifests,
    hash_file,
    read_document,
)
from .acceptance_v5_io import (
    publish as _publish,
)
from .archive_timer import ARCHIVE_SERVICE_NAME, ARCHIVE_TIMER_NAME
from .deployment_identity import DeploymentIdentity
from .systemd import SYSTEMD_SERVICE_NAME

TABLES = (
    "chunks",
    "chunk_transitions",
    "archive_transactions",
    "archive_transaction_events",
    "operational_events",
    "operational_event_sequence",
    "storage_targets",
)


@contextmanager
def read_catalog(path: Path, *, immutable: bool = False) -> Iterator[sqlite3.Connection]:
    if path.is_symlink() or not path.is_file():
        raise AcceptanceError("Catalog authority is unavailable/unsafe")
    uri = f"{path.resolve().as_uri()}?mode=ro" + ("&immutable=1" if immutable else "")
    connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA cache_size=-1024")
    try:
        yield connection
    finally:
        connection.close()


def catalog_available(path: Path) -> bool:
    """V5 readiness availability only; no Catalog constructor/history scan."""
    try:
        with read_catalog(path) as connection:
            connection.execute("BEGIN")
            DeltaSnapshot(connection)
            connection.execute("SELECT chunk_id FROM chunks LIMIT 1").fetchone()
        return True
    except (OSError, sqlite3.Error, RuntimeError):
        return False


def catalog_authority(connection: sqlite3.Connection) -> dict[str, Any]:
    validate_sequence(connection)
    if [row[0] for row in connection.execute("PRAGMA integrity_check")] != ["ok"]:
        raise AcceptanceError("frozen Catalog integrity check failed")
    if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise AcceptanceError("frozen Catalog foreign-key check failed")
    schema_digest = hashlib.sha256()
    for row in connection.execute(
        "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"
    ):
        schema_digest.update(canonical_json(dict(row)))
    targets_digest = hashlib.sha256()
    target_count = 0
    for row in ordered_rows(connection, "storage_targets", "storage_id"):
        targets_digest.update(canonical_json(row))
        target_count += 1
    return {
        "schema_sha256": schema_digest.hexdigest(),
        "high_water": DeltaSnapshot(connection).high_water,
        "table_counts": {
            table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in TABLES
        },
        "registered_targets": {"count": target_count, "sha256": targets_digest.hexdigest()},
    }


def quiescence_probe(data_root: Path) -> dict[str, Any]:
    """Read-only bounded systemctl queries. No service/timer mutation."""
    units = {}
    for unit in (SYSTEMD_SERVICE_NAME, ARCHIVE_SERVICE_NAME, ARCHIVE_TIMER_NAME):
        result = subprocess.run(
            [
                "/usr/bin/systemctl",
                "show",
                unit,
                "--no-pager",
                "--property=LoadState,ActiveState,SubState,MainPID,UnitFileState,"
                "InvocationID,ActiveEnterTimestampMonotonic,InactiveEnterTimestampMonotonic",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode or len(result.stdout) > 4096:
            raise AcceptanceError("quiescence systemd authority unavailable")
        units[unit] = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)

    def count_partials(descriptor: int, depth: int = 0) -> int:
        if depth > 64:
            raise AcceptanceError("active directory topology exceeded")
        count = 0
        with os.scandir(descriptor) as entries:
            for entry in entries:
                if entry.is_symlink():
                    raise AcceptanceError("active authority symlink rejected")
                if entry.is_dir(follow_symlinks=False):
                    child = os.open(
                        entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor
                    )
                    try:
                        count += count_partials(child, depth + 1)
                    finally:
                        os.close(child)
                elif entry.is_file(follow_symlinks=False):
                    count += entry.name.endswith(".partial")
                else:
                    raise AcceptanceError("unexpected active directory member")
        return count

    descriptor = os.open(
        data_root / "data" / "active", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    )
    try:
        partial_count = count_partials(descriptor)
    finally:
        os.close(descriptor)
    certificate = {"units": units, "active_partial_count": partial_count}
    validate_quiescence(certificate)
    return certificate


def validate_quiescence(certificate: Mapping[str, Any]) -> None:
    units = certificate.get("units")
    if (
        not isinstance(units, dict)
        or set(units)
        != {
            SYSTEMD_SERVICE_NAME,
            ARCHIVE_SERVICE_NAME,
            ARCHIVE_TIMER_NAME,
        }
        or certificate.get("active_partial_count") != 0
    ):
        raise AcceptanceError("quiescence precondition failed")
    for name, state in units.items():
        if state.get("LoadState") != "loaded" or state.get("ActiveState") != "inactive":
            raise AcceptanceError("sanctioned project mutator is not quiescent")
        if name != ARCHIVE_TIMER_NAME and state.get("MainPID") != "0":
            raise AcceptanceError("project worker process remains active")
    if units[SYSTEMD_SERVICE_NAME].get("UnitFileState") != "disabled":
        raise AcceptanceError("Recorder must be stopped and disabled")


def freeze_catalog(
    data_root: Path,
    root: Path,
    progress: Callable[[int], None] | None = None,
) -> tuple[str, dict[str, Any]]:
    if root.resolve(strict=False) != root.absolute():
        raise AcceptanceError("private Catalog directory symlink rejected")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / "catalog.sqlite"
    partial = root / f".{uuid4().hex}.catalog.partial"
    fd = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    try:
        with read_catalog(data_root / "state" / "catalog.sqlite") as source:
            destination = sqlite3.connect(partial)
            last_remaining: int | None = None
            page_size = int(source.execute("PRAGMA page_size").fetchone()[0])
            try:

                def backup_progress(_status: int, remaining: int, total: int) -> None:
                    nonlocal last_remaining
                    previous = total if last_remaining is None else last_remaining
                    if progress and remaining < previous:
                        progress((previous - remaining) * page_size)
                    last_remaining = remaining

                source.backup(destination, pages=256, sleep=0.01, progress=backup_progress)
            finally:
                destination.close()
        with partial.open("rb") as file:
            os.fsync(file.fileno())
        _size, digest = hash_file(partial, progress)
        with read_catalog(partial, immutable=True) as frozen:
            authority = catalog_authority(frozen)
        os.link(partial, path)
        fsync_directory(root)
        if hash_file(path)[1] != digest:
            raise AcceptanceError("Catalog backup readback disagreement")
        return digest, authority
    finally:
        partial.unlink(missing_ok=True)


def freeze_corpus(
    *,
    data_root: Path,
    root: Path,
    identity: DeploymentIdentity,
    anchor_sha256: str,
    boot_id: str,
    probe: Callable[[Path], dict[str, Any]] = quiescence_probe,
    progress: Callable[[int], None] | None = None,
) -> tuple[Path, str, dict[str, Any]]:
    if str(data_root.resolve()) != identity.systemd_effective.get("working_directory"):
        raise AcceptanceError("V5 corpus data root differs from deployment authority")
    before = probe(data_root)
    validate_quiescence(before)
    catalog_sha, authority = freeze_catalog(data_root, root, progress)
    manifests = freeze_manifests(data_root, root, catalog_sha, progress)
    after = probe(data_root)
    validate_quiescence(after)
    if before != after:
        raise AcceptanceError("quiescence certificate changed during corpus freeze")
    # A second consistent backup detects sanctioned changes to Catalog during manifest freeze.
    verification_root = root / f".catalog-verification-{uuid4().hex}"
    check_sha, check_authority = freeze_catalog(data_root, verification_root, progress)
    if check_sha != catalog_sha or check_authority != authority:
        raise AcceptanceError("Catalog changed during quiescence corpus freeze")
    document = {
        "schema_version": V5_SCHEMA_VERSION,
        "evidence_kind": "quiescence-corpus",
        "anchor_sha256": anchor_sha256,
        "deployment_identity": identity.document(),
        "data_root": str(data_root.resolve()),
        "boot_id": boot_id,
        "catalog_sha256": catalog_sha,
        "catalog_authority": authority,
        "manifest_freeze": manifests,
        "quiescence_certificate": before,
    }
    path, digest = _publish(root, "quiescence-corpus.json", document)
    return path, digest, document


def verify_corpus(
    root: Path,
    *,
    identity: DeploymentIdentity,
    anchor_sha256: str,
) -> tuple[dict[str, Any], str]:
    corpus, digest = read_document(root / "quiescence-corpus.json")
    if corpus.get("evidence_kind") != "quiescence-corpus" or (
        corpus.get("deployment_identity") != identity.document()
        or corpus.get("anchor_sha256") != anchor_sha256
    ):
        raise AcceptanceError("quiescence corpus identity/anchor disagreement")
    validate_quiescence(corpus["quiescence_certificate"])
    if corpus.get("data_root") != identity.systemd_effective.get("working_directory"):
        raise AcceptanceError("frozen corpus data root differs from deployment authority")
    if hash_file(root / "catalog.sqlite")[1] != corpus.get("catalog_sha256"):
        raise AcceptanceError("frozen Catalog backup corrupted")
    with read_catalog(root / "catalog.sqlite", immutable=True) as connection:
        if catalog_authority(connection) != corpus.get("catalog_authority"):
            raise AcceptanceError("frozen Catalog metadata disagreement")
    # Streaming hashes/readback; chunk uniqueness is also reconstructed by the full auditor.
    for _record, _manifest in frozen_manifests(root, corpus):
        pass
    return corpus, digest


def baseline_preflight(
    *,
    data_root: Path,
    root: Path,
    identity: DeploymentIdentity,
    boot_id: str,
    products: list[list[str]],
    probe: Callable[[Path], dict[str, Any]] = quiescence_probe,
) -> tuple[Path, str, dict[str, Any]]:
    certificate = probe(data_root)
    validate_quiescence(certificate)
    with Catalog(data_root / "state" / "catalog.sqlite") as catalog:
        catalog.migrate_acceptance_sequence()
        authority = catalog_authority(catalog._connection)
    after = probe(data_root)
    if after != certificate:
        raise AcceptanceError("baseline migration lost quiescence")
    document = {
        "schema_version": V5_SCHEMA_VERSION,
        "evidence_kind": "baseline-preflight",
        "run_id": uuid4().hex,
        "configured_products": products,
        "deployment_identity": identity.document(),
        "boot_id": boot_id,
        "catalog_migration": "VERIFIED",
        "catalog_authority": authority,
        "quiescence_certificate": certificate,
    }
    path, digest = _publish(root, "baseline-preflight.json", document)
    return path, digest, document
