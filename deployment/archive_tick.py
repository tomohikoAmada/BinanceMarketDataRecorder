"""One bounded pass through the existing archive transaction engine per market."""

import json
import os
import resource
import sqlite3
import subprocess
import sys
from pathlib import Path
from time import time

RELEASE = Path(__file__).resolve().parent.parent
PROTOTYPE = RELEASE / "prototype"
if str(PROTOTYPE) not in sys.path:
    sys.path.insert(0, str(PROTOTYPE))

from partition_roots import (  # noqa: E402
    partition_metadata,
    partition_roots,
    reader_lock,
)

BLOCKED_EXIT_REASONS = {
    "TARGET_ABSENT",
    "TARGET_NOT_READY",
    "TARGET_LOW_SPACE",
}


def classify(exit_code, parsed):
    exit_reason = parsed.get("exit_reason")
    backlog = parsed.get("backlog_files_after")
    blocked = exit_reason in BLOCKED_EXIT_REASONS
    # A target outage is only a failure when verified data is waiting.
    stalled = blocked and bool(backlog)
    return {
        "exit_reason": exit_reason,
        "target_state": parsed.get("target_state"),
        "backlog_files_after": backlog,
        "blocked": blocked,
        "stalled": stalled,
        "failed_transactions": parsed.get("failed_transactions"),
        "failed": exit_code != 0 or stalled,
    }


def run_archive(root: Path, env: dict) -> dict:
    result = subprocess.run(
        [sys.executable, RELEASE / "prototype/operations.py", "archive", root],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    parsed = {}
    if result.stdout.strip():
        try:
            parsed = json.loads(result.stdout)
        except json.JSONDecodeError:
            parsed = {}
    return {
        "exit": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        **classify(result.returncode, parsed),
    }


def _has_archive_target(root: Path) -> bool:
    catalog = root / "state/catalog.sqlite"
    if not catalog.is_file():
        return False
    with sqlite3.connect(f"file:{catalog}?mode=ro", uri=True) as db:
        table = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='storage_targets'"
        ).fetchone()
        if table is None:
            return False
        row = db.execute("SELECT count(*) FROM storage_targets").fetchone()
        return bool(row and int(row[0]) > 0)


def ensure_archive_target(root: Path, target: Path | None, env: dict) -> dict:
    if _has_archive_target(root):
        return {"status": "registered"}
    if target is None:
        return {"status": "missing-target-mapping"}
    target.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            sys.executable,
            RELEASE / "prototype/operations.py",
            "register",
            root,
            "--target",
            target,
        ],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0 or not _has_archive_target(root):
        return {
            "status": "failed",
            "exit": result.returncode,
            "stderr": result.stderr,
        }
    return {"status": "registered-now"}


def archive_partitions(root: Path, market: str, target: Path | None,
                       env: dict) -> tuple[list[dict], bool]:
    partitions: list[dict] = []
    failed = False
    with reader_lock(root, exclusive=True, blocking=False) as acquired:
        if not acquired:
            return [{"status": "busy-reader"}], False
        for partition in partition_roots(root):
            metadata = partition_metadata(partition)
            if metadata.get("state") != "closed":
                continue
            registration = ensure_archive_target(partition, target, env)
            archived = run_archive(partition, env)
            failed |= archived["failed"] or registration["status"] == "failed"
            partitions.append(
                {
                    "partition_id": metadata["partition_id"],
                    "target_registration": registration,
                    **archived,
                }
            )
    return partitions, failed


def main():
    roots = json.loads(Path("/etc/sbmdr/roots.json").read_text())
    metadata_roots = {}
    metadata_path = Path("/etc/sbmdr/metadata-roots.json")
    if metadata_path.is_file():
        metadata_roots = json.loads(metadata_path.read_text())
    archive_targets = {}
    archive_targets_path = Path("/etc/sbmdr/archive-targets.json")
    if archive_targets_path.is_file():
        archive_targets = json.loads(archive_targets_path.read_text())
    started = time()
    failed = False
    reports = {}
    env = {**os.environ, "PYTHONPATH": str(RELEASE / "python/src")}
    for market, value in roots.items():
        root = Path(value)
        if (root / "partitions").is_dir():
            target = (
                Path(archive_targets[market])
                if archive_targets.get(market)
                else None
            )
            partitions, partition_failed = archive_partitions(
                root, market, target, env
            )
            failed |= partition_failed
            migration = None
            if metadata_roots.get(market):
                migration = subprocess.run(
                    [
                        sys.executable,
                        RELEASE / "deployment/migrate_metadata.py",
                        root,
                        "--cold-root",
                        metadata_roots[market],
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    env=env,
                )
                failed |= migration.returncode != 0
            reports[market] = {
                "mode": "partitioned",
                "partitions": partitions,
                "migration_exit": (
                    None if migration is None else migration.returncode
                ),
                "migration_stdout": (
                    None if migration is None else migration.stdout
                ),
                "migration_stderr": (
                    None if migration is None else migration.stderr
                ),
            }
            continue
        reports[market] = {"mode": "legacy", **run_archive(root, env)}
        failed |= reports[market]["failed"]
    child = resource.getrusage(resource.RUSAGE_CHILDREN)
    own = resource.getrusage(resource.RUSAGE_SELF)
    row = {
        "start_utc_seconds": started,
        "finish_utc_seconds": time(),
        "cpu_seconds": child.ru_utime + child.ru_stime + own.ru_utime + own.ru_stime,
        "peak_rss_upper_bound_bytes": (child.ru_maxrss + own.ru_maxrss) * 1024,
        "failed": failed,
        "markets": reports,
    }
    with Path("/var/log/sbmdr/archive.jsonl").open("a") as out:
        out.write(json.dumps(row) + "\n")
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
