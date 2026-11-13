"""Partition layout discovery shared by readers, archive and verification tools."""

from __future__ import annotations

import json
from pathlib import Path

PARTITION_SCHEMA = "partition.v1"


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
