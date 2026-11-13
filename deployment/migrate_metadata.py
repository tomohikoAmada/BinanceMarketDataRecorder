"""Migrate verified closed partition metadata from the hot root to cold storage."""

from __future__ import annotations

import argparse
import fcntl
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
)

TERMINAL_ARCHIVE_STATES = {"LOCAL_DELETED", "LOCAL_DELETE_PENDING"}


def _sqlite_integrity(path: Path) -> str:
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
        value = db.execute("PRAGMA integrity_check").fetchone()[0]
    if value != "ok":
        raise ValueError(f"SQLite integrity check failed for {path}: {value}")
    return value


def _archive_complete(root: Path) -> tuple[bool, str]:
    catalog = root / "state/catalog.sqlite"
    if not catalog.is_file():
        return False, "catalog-missing"
    with sqlite3.connect(catalog, timeout=30) as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        chunk_count = (
            db.execute("SELECT count(*) FROM chunks").fetchone()[0]
            if "chunks" in tables
            else 0
        )
        if "archive_transactions" not in tables:
            return chunk_count == 0, "archive-transactions-missing"
        states = [
            row[0]
            for row in db.execute(
                "SELECT state FROM archive_transactions"
            )
        ]
        if any(state not in TERMINAL_ARCHIVE_STATES for state in states):
            return False, "raw-archive-pending"
        if len(states) > chunk_count:
            return False, "archive-transaction-count-mismatch"
        return True, "archive-complete"


def _copy_and_verify(source: Path, destination: Path) -> None:
    temporary = destination.with_name(destination.name + ".partial")
    if temporary.exists():
        shutil.rmtree(temporary)
    with sqlite3.connect(source / "state/catalog.sqlite", timeout=30) as db:
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    shutil.copytree(source, temporary)
    _sqlite_integrity(temporary / "state/catalog.sqlite")
    metadata = partition_metadata(temporary)
    if metadata["partition_id"] != source.name:
        raise ValueError("copied partition ID mismatch")
    if metadata.get("state") != "closed":
        raise ValueError("copied partition is not closed")
    catalog_ids: set[str] = set()
    with sqlite3.connect(temporary / "state/catalog.sqlite") as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "chunks" in tables:
            catalog_ids = {
                row[0] for row in db.execute("SELECT chunk_id FROM chunks")
            }
    manifest_ids = set()
    for manifest in (temporary / "data/manifests").glob("*.manifest.json"):
        try:
            manifest_ids.add(
                json.loads(manifest.read_text(encoding="utf-8"))["chunk_id"]
            )
        except (OSError, KeyError, json.JSONDecodeError) as error:
            raise ValueError(
                f"invalid manifest in {temporary}: {manifest.name}"
            ) from error
    if catalog_ids != manifest_ids:
        raise ValueError("copied Catalog/manifest chunk IDs differ")
    os.rename(temporary, destination)
    for parent in (destination.parent, source.parent):
        fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def migrate(container: Path, cold_root: Path) -> dict:
    cold_root.mkdir(parents=True, exist_ok=True)
    lock_path = container / "state/metadata-move.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "BUSY", "migrated": [], "skipped": []}
        migrated: list[str] = []
        skipped: list[dict] = []
        for source in partition_roots(container):
            metadata = partition_metadata(source)
            partition_id = metadata["partition_id"]
            if metadata.get("state") != "closed":
                skipped.append({"partition_id": partition_id, "reason": "open"})
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
                skipped.append({"partition_id": partition_id, "reason": reason})
                continue
            destination = cold_root / partition_id
            if destination.exists():
                skipped.append(
                    {"partition_id": partition_id, "reason": "already-cold"}
                )
                continue
            _copy_and_verify(source, destination)
            shutil.rmtree(source)
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
