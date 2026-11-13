"""Partition layout discovery shared by readers, archive and verification tools."""

from __future__ import annotations

import fcntl
import json
from contextlib import contextmanager
from pathlib import Path

PARTITION_SCHEMA = "partition.v1"
READER_LOCK_NAME = "state/reader.lock"


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise ValueError(f"not a JSON object: {path}")
    return value


def read_current(container: Path) -> str:
    try:
        return (container / "CURRENT").read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def partition_metadata(root: Path) -> dict:
    value = read_json(root / "partition.json")
    if value.get("schema_version") != PARTITION_SCHEMA:
        raise ValueError(f"unsupported partition schema in {root}")
    if not isinstance(value.get("partition_id"), str):
        raise ValueError(f"missing partition_id in {root}")
    return value


def partition_roots(container: Path) -> list[Path]:
    partitions = container / "partitions"
    if not partitions.is_dir():
        return [container]
    return sorted(path for path in partitions.iterdir() if path.is_dir())


def _intersects(metadata: dict, start_ns: int | None, end_ns: int | None) -> bool:
    if start_ns is None and end_ns is None:
        return True
    value = metadata.get("receive_time_utc_range_ns", {})
    first = value.get("min")
    last = value.get("max")
    if not isinstance(first, int) or not isinstance(last, int):
        return True
    if end_ns is not None and first >= end_ns:
        return False
    if start_ns is not None and last < start_ns:
        return False
    return True


def resolve_roots(
    market_root: Path,
    *,
    cold_root: Path | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> list[Path]:
    """Return the physical roots needed for the requested receive window.

    A legacy single root is returned unchanged.  For a partition container the
    CURRENT/open partition and all closed partitions intersecting the window
    are selected.  A verified cold copy wins over the hot copy, and partitions
    are deduplicated by partition ID.  Missing selected roots are an explicit
    error rather than a silent partial read.
    """

    if not (market_root / "partitions").is_dir():
        return [market_root]
    current = read_current(market_root)
    selected: dict[str, Path] = {}
    for hot in partition_roots(market_root):
        metadata = partition_metadata(hot)
        partition_id = metadata["partition_id"]
        state = metadata.get("state")
        if state != "open" and not _intersects(metadata, start_ns, end_ns):
            continue
        cold = cold_root / partition_id if cold_root is not None else None
        physical = cold if cold is not None and cold.is_dir() else hot
        if not physical.is_dir():
            raise ValueError(f"partition {partition_id} is unavailable")
        selected[partition_id] = physical
    if current and current not in selected:
        hot = market_root / "partitions" / current
        if not hot.is_dir():
            raise ValueError(f"CURRENT partition {current} is unavailable")
        selected[current] = hot
    return [selected[key] for key in sorted(selected)]


@contextmanager
def reader_lock(market_root: Path, *, exclusive: bool = False, blocking: bool = True):
    """Coordinate metadata migration with readers/normalizers.

    Readers take a shared lock before resolving physical roots. Migration takes
    an exclusive non-blocking lock while copying and deleting a closed source;
    if a reader is active the migration reports BUSY and retries next tick.
    Recorder writers do not take this lock.
    """

    if not market_root.is_dir():
        # Legacy read-only callers may probe a nonexistent path; Catalog access
        # will fail explicitly later.
        yield True
        return
    path = market_root / READER_LOCK_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        if not blocking:
            operation |= fcntl.LOCK_NB
        try:
            fcntl.flock(handle.fileno(), operation)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
