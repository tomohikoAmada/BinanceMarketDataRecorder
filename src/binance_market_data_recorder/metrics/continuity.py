"""Per-source interval accounting from P3 monitoring, with conservative unknown tails."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ..storage.catalog import Catalog


def union(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def continuity_report(
    catalog: Catalog,
    *,
    start: int,
    end: int,
    source_ids: list[str] | None = None,
) -> dict[str, Any]:
    if start >= end:
        raise ValueError("continuity start must precede end")
    states = catalog.source_availability()
    output = []
    for key in sorted(states if source_ids is None else set(source_ids)):
        state = states.get(key)
        if state is None:
            output.append({"source_id": key, "status": "UNKNOWN_SOURCE", "coverage": 0.0})
            continue
        eligible = union(
            (max(start, row["started_at_utc_ns"]), min(end, row["ended_at_utc_ns"] or end))
            for row in catalog.source_eligibility_intervals(key)
        )
        gaps = [
            (max(start, row["started_at_utc_ns"]), min(end, row["ended_at_utc_ns"] or end))
            for row in catalog.source_availability_intervals(key, start, end)
        ]
        # Missing persisted observation is never credited as healthy, including a dead process.
        gaps.append((max(start, int(state["observed_at_utc_ns"])), end))
        unavailable = union((max(a, c), min(b, d)) for a, b in eligible for c, d in gaps)
        duration = sum(b - a for a, b in eligible)
        lost = sum(b - a for a, b in unavailable)
        output.append(
            {
                "source_id": key,
                "status": "OBSERVED" if duration else "NOT_APPLICABLE",
                "eligible_ns": duration,
                "unavailable_or_unknown_ns": lost,
                "coverage": 1 - lost / duration if duration else None,
                "eligible_intervals": eligible,
                "unavailable_intervals": unavailable,
                "latest_state": state,
            }
        )
    return {
        "schema_version": "source-continuity.v1",
        "start_utc_ns": start,
        "end_utc_ns": end,
        "sources": output,
        "threshold": 0.995,
        "production_qualified": False,
        "scope": "observed source lifecycle; freeze source IDs/configuration before qualification",
    }
