"""Migrate verified closed partition metadata from the hot root to cold storage."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
from pathlib import Path

PROTOTYPE = Path(__file__).resolve().parent.parent / "prototype"
if str(PROTOTYPE) not in sys.path:
    sys.path.insert(0, str(PROTOTYPE))

from partition_roots import (  # noqa: E402
    partition_metadata,
    partition_roots,
    read_current,
    reader_lock,
)

TERMINAL_ARCHIVE_STATES = {"LOCAL_DELETED", "LOCAL_DELETE_PENDING"}
UNARCHIVED_CHUNK_STATES = {
    "ACTIVE",
    "RECOVERED",
    "SEALING",
    "SEALED",
    "ARCHIVE_COPYING",
    "ARCHIVE_VERIFYING",
}


def _fsync_fd(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_tree(root: Path) -> None:
    """Fsync every copied regular file and directory bottom-up."""
    for dirpath, _dirnames, filenames in os.walk(root, topdown=False):
        directory = Path(dirpath)
        for filename in filenames:
            candidate = directory / filename
            if candidate.is_symlink():
                raise ValueError(f"refusing symlink in migrated tree: {candidate}")
            if candidate.is_file():
                _fsync_fd(candidate)
        _fsync_fd(directory)


def _sqlite_integrity(path: Path) -> str:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
        value = db.execute("PRAGMA integrity_check").fetchone()[0]
    if value != "ok":
        raise ValueError(f"SQLite integrity check failed for {path}: {value}")
    return value


def _table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {
        row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()
    }


def _verify_external_targets(
    root: Path, transactions: list[dict[str, object]]
) -> tuple[bool, str]:
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.macos import StorageRegistry
    from binance_market_data_recorder.storage.platform import volume_adapter

    with Catalog(root / "state/catalog.sqlite", read_only=True) as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
        statuses = {
            str(item["storage_id"]): item for item in registry.observe_statuses()
        }
    for tx in transactions:
        storage_id = str(tx.get("storage_id") or "")
        target_rel = str(tx.get("target_relative_path") or "")
        if not storage_id or not target_rel:
            return False, "archive-target-metadata-missing"
        status = statuses.get(storage_id)
        if status is None or not status.get("resolved_path"):
            return False, f"archive-target-unavailable:{storage_id}"
        state_value = str(status.get("state") or "")
        if state_value not in {"READY", "LOW_SPACE"}:
            return False, f"archive-target-not-ready:{storage_id}:{state_value}"
        mount_root = Path(str(status["resolved_path"])).resolve()
        artifact = (mount_root / target_rel).resolve()
        if not artifact.is_relative_to(mount_root) or not artifact.is_file():
            return False, f"archived-artifact-missing:{tx['chunk_id']}"
        stored_bytes = tx.get("stored_bytes")
        if isinstance(stored_bytes, int) and artifact.stat().st_size != stored_bytes:
            return False, f"archived-artifact-size-mismatch:{tx['chunk_id']}"
        ext_manifest = tx.get("external_manifest_relative_path")
        if isinstance(ext_manifest, str) and ext_manifest:
            manifest_path = (mount_root / ext_manifest).resolve()
            if not manifest_path.is_relative_to(mount_root) or not manifest_path.is_file():
                return False, f"external-manifest-missing:{tx['chunk_id']}"
    return True, "archive-complete"


def _archive_complete(root: Path) -> tuple[bool, str]:
    catalog = root / "state/catalog.sqlite"
    if not catalog.is_file():
        return False, "catalog-missing"
    if any((root / "data/sealed").glob("*")):
        return False, "local-sealed-remaining"
    with sqlite3.connect(catalog, timeout=30) as db:
        db.row_factory = sqlite3.Row
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        chunks = (
            db.execute("SELECT chunk_id, state FROM chunks").fetchall()
            if "chunks" in tables
            else []
        )
        if not chunks:
            return True, "archive-complete"
        chunk_states = {row["chunk_id"]: row["state"] for row in chunks}
        if any(state == "QUARANTINED" for state in chunk_states.values()):
            return False, "quarantined-chunks"
        if any(state in UNARCHIVED_CHUNK_STATES for state in chunk_states.values()):
            return False, "raw-archive-pending"
        if "archive_transactions" not in tables:
            return False, "archive-transactions-missing"
        tx_rows = [
            dict(row)
            for row in db.execute("SELECT * FROM archive_transactions").fetchall()
        ]
        if not tx_rows:
            return False, "raw-archive-pending"
        tx_by_chunk = {str(row["chunk_id"]): row for row in tx_rows}
        if set(tx_by_chunk) != set(chunk_states) or len(tx_rows) != len(chunk_states):
            return False, "archive-transaction-count-mismatch"
        tx_cols = _table_columns(db, "archive_transactions")
        for row in tx_rows:
            if row.get("state") not in TERMINAL_ARCHIVE_STATES:
                return False, "raw-archive-pending"
            if "verified_at_utc_ns" in tx_cols and row.get("verified_at_utc_ns") is None:
                return False, "archive-unverified"
            if "storage_id" in tx_cols and not row.get("storage_id"):
                return False, "archive-target-metadata-missing"
            if "target_relative_path" in tx_cols and not row.get("target_relative_path"):
                return False, "archive-target-metadata-missing"
        has_targets = (
            "storage_targets" in tables
            and db.execute("SELECT count(*) FROM storage_targets").fetchone()[0] > 0
        )
    if has_targets:
        return _verify_external_targets(root, tx_rows)
    return True, "archive-complete"


def _directory_size_bytes(path: Path) -> int:
    return sum(
        item.stat().st_size for item in path.rglob("*") if item.is_file()
    )


def _nearest_existing_ancestor(path: Path) -> Path:
    current = path
    while not current.exists():
        parent = current.parent
        if parent == current:
            return current
        current = parent
    return current


def _preflight_cold_root(
    container: Path, cold_root: Path, sources: list[Path]
) -> None:
    if cold_root.is_symlink():
        raise ValueError(f"cold_root must not be a symlink: {cold_root}")
    if cold_root.exists() and not cold_root.is_dir():
        raise ValueError(f"cold_root is not a directory: {cold_root}")
    parts = cold_root.parts
    if len(parts) >= 3 and parts[0] == "/" and parts[1] in {"mnt", "media", "Volumes"}:
        mount_root = Path("/", parts[1], parts[2])
        if not mount_root.is_dir() or not os.path.ismount(mount_root):
            raise ValueError(f"cold_root mount is unavailable: {mount_root}")
    ancestor = _nearest_existing_ancestor(cold_root)
    if ancestor == Path("/") or not ancestor.is_dir():
        raise ValueError(f"cold_root refuses root filesystem /: {cold_root}")
    if not os.access(ancestor, os.W_OK | os.X_OK):
        raise ValueError(f"cold_root is not writable: {cold_root}")
    needed = sum(_directory_size_bytes(source) for source in sources)
    reserve = max(needed + 4096, int(needed * 1.1))
    free = shutil.disk_usage(ancestor).free
    if free < reserve:
        raise ValueError(
            f"insufficient cold_root free space on {ancestor}: {free} < {reserve}"
        )


def _manifest_digests(root: Path) -> dict[str, tuple[str, str]]:
    digests: dict[str, tuple[str, str]] = {}
    for manifest in sorted((root / "data/manifests").glob("*.manifest.json")):
        raw = manifest.read_bytes()
        try:
            decoded = json.loads(raw.decode("utf-8"))
            chunk_id = str(decoded["chunk_id"])
        except (OSError, UnicodeDecodeError, KeyError, json.JSONDecodeError) as error:
            raise ValueError(
                f"invalid manifest in {root}: {manifest.name}"
            ) from error
        digests[manifest.name] = (
            chunk_id,
            hashlib.sha256(raw).hexdigest(),
        )
    return digests


def _catalog_identity(root: Path) -> tuple[list[tuple], list[tuple]]:
    with sqlite3.connect(f"file:{root / 'state/catalog.sqlite'}?mode=ro", uri=True) as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        chunks = (
            db.execute("SELECT * FROM chunks ORDER BY chunk_id").fetchall()
            if "chunks" in tables
            else []
        )
        tx = (
            db.execute(
                "SELECT * FROM archive_transactions ORDER BY chunk_id"
            ).fetchall()
            if "archive_transactions" in tables
            else []
        )
    return chunks, tx


def _verify_copied_root(source: Path, candidate: Path) -> None:
    _sqlite_integrity(candidate / "state/catalog.sqlite")
    source_meta = partition_metadata(source)
    candidate_meta = partition_metadata(candidate)
    if candidate_meta["partition_id"] != source.name:
        raise ValueError("copied partition ID mismatch")
    if candidate_meta.get("state") != "closed":
        raise ValueError("copied partition is not closed")
    if source_meta != candidate_meta:
        raise ValueError("copied partition metadata mismatch")
    source_manifests = _manifest_digests(source)
    candidate_manifests = _manifest_digests(candidate)
    if source_manifests != candidate_manifests:
        raise ValueError("copied manifest set differs from source")
    source_chunks, source_tx = _catalog_identity(source)
    candidate_chunks, candidate_tx = _catalog_identity(candidate)
    if source_chunks != candidate_chunks or source_tx != candidate_tx:
        raise ValueError("copied Catalog rows differ from source")
    catalog_ids = {row[0] for row in candidate_chunks}
    manifest_ids = {chunk_id for chunk_id, _ in candidate_manifests.values()}
    if catalog_ids != manifest_ids:
        raise ValueError("copied Catalog/manifest chunk IDs differ")


def _copy_and_verify(source: Path, destination: Path) -> None:
    temporary = destination.with_name(destination.name + ".partial")
    if temporary.exists():
        shutil.rmtree(temporary)
    with sqlite3.connect(source / "state/catalog.sqlite", timeout=30) as db:
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    shutil.copytree(source, temporary, symlinks=False)
    _verify_copied_root(source, temporary)
    _fsync_tree(temporary)
    os.rename(temporary, destination)
    _fsync_fd(destination.parent)


def _cleanup_partial(destination: Path) -> None:
    temporary = destination.with_name(destination.name + ".partial")
    if temporary.exists():
        shutil.rmtree(temporary)


def migrate(container: Path, cold_root: Path) -> dict:
    with reader_lock(container, exclusive=True, blocking=False) as acquired:
        if not acquired:
            return {
                "status": "BUSY",
                "reason": "reader-active",
                "migrated": [],
                "skipped": [],
            }
        candidates: list[tuple[Path, str]] = []
        skipped: list[dict] = []
        for source in partition_roots(container):
            metadata = partition_metadata(source)
            partition_id = metadata["partition_id"]
            if metadata.get("state") != "closed":
                skipped.append(
                    {"partition_id": partition_id, "reason": "open"}
                )
                continue
            if any((source / "data/active").glob("*")):
                skipped.append(
                    {"partition_id": partition_id, "reason": "active-files"}
                )
                continue
            if any(
                path.suffix == ".partial"
                for directory in ("data/sealed", "data/manifests")
                for path in (source / directory).glob("*")
            ):
                skipped.append(
                    {"partition_id": partition_id, "reason": "partial-files"}
                )
                continue
            complete, reason = _archive_complete(source)
            if not complete:
                skipped.append(
                    {"partition_id": partition_id, "reason": reason}
                )
                continue
            candidates.append((source, partition_id))

        migrated: list[str] = []
        if candidates:
            _preflight_cold_root(
                container, cold_root, [source for source, _ in candidates]
            )
            cold_root.mkdir(parents=True, exist_ok=True)
            for source, partition_id in candidates:
                destination = cold_root / partition_id
                if destination.exists():
                    if destination.is_symlink() or not destination.is_dir():
                        raise ValueError(
                            f"invalid existing cold partition: {destination}"
                        )
                    _verify_copied_root(source, destination)
                    _cleanup_partial(destination)
                    shutil.rmtree(source)
                    _fsync_fd(source.parent)
                    migrated.append(partition_id)
                    continue
                _copy_and_verify(source, destination)
                shutil.rmtree(source)
                _fsync_fd(source.parent)
                migrated.append(partition_id)
        return {
            "status": "OK",
            "current": read_current(container),
            "migrated": migrated,
            "skipped": skipped,
            "cold_root": str(cold_root),
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("container", type=Path)
    parser.add_argument("--cold-root", type=Path, required=True)
    args = parser.parse_args(argv)
    report = migrate(args.container, args.cold_root)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["status"] in {"OK", "BUSY"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
