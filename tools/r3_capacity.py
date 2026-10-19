"""Offline disk planning from explicit rates; no host or service access.

Run: python -m tools.r3_capacity --input <scenario.json>
All rates must describe the same workload. Archive service rates include full
verification/retirement and are divided by the actual wall-clock duty cycle.
Outputs are scenario estimates, never production admission or qualification.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path

from binance_market_data_recorder.storage.capacity import HARD_RESERVE_BYTES


@dataclass(frozen=True)
class DiskBudget:
    writer_free_bytes: int
    archive_free_bytes: int
    horizon_seconds: float
    archive_unavailable_seconds: float
    sealed_bytes_per_second: float
    chunks_per_second: float
    archive_bytes_per_work_second: float
    archive_chunks_per_work_second: float
    archive_work_seconds: float
    archive_cycle_seconds: float
    archive_max_files_per_cycle: int
    retained_metadata_bytes_per_second: float
    archive_metadata_bytes_per_chunk: int
    other_writer_growth_bytes_per_second: float
    initial_backlog_bytes: int
    initial_backlog_chunks: int
    active_raw_peak_bytes: int
    seal_temporary_peak_bytes: int
    archive_temporary_peak_bytes: int
    wal_peak_bytes: int
    other_temporary_peak_bytes: int

    def __post_init__(self) -> None:
        if any(not math.isfinite(value) or value < 0 for value in asdict(self).values()):
            raise ValueError("budget inputs must be finite nonnegative numbers")
        if self.horizon_seconds <= 0 or self.archive_cycle_seconds <= 0:
            raise ValueError("horizon and archive cycle must be positive")
        if self.archive_work_seconds > self.archive_cycle_seconds:
            raise ValueError("archive work cannot exceed wall-clock cycle")
        if self.archive_unavailable_seconds > self.horizon_seconds:
            raise ValueError("unavailable time cannot exceed horizon")
        if (self.sealed_bytes_per_second > 0) != (self.chunks_per_second > 0):
            raise ValueError("arrival bytes and chunks must describe the same workload")
        if (self.initial_backlog_bytes > 0) != (self.initial_backlog_chunks > 0):
            raise ValueError("initial backlog requires both bytes and chunks")


def evaluate(budget: DiskBudget) -> dict[str, object]:
    duty = budget.archive_work_seconds / budget.archive_cycle_seconds
    byte_service = budget.archive_bytes_per_work_second * duty
    file_service = (
        min(
            budget.archive_chunks_per_work_second * budget.archive_work_seconds,
            budget.archive_max_files_per_cycle,
        )
        / budget.archive_cycle_seconds
    )
    idle = budget.archive_cycle_seconds
    # A worker may exit early on max-files or an empty queue. Treat a whole
    # wall-clock cycle as the service gap bound; the runtime cap is not a
    # guarantee that the worker occupies the rest of that cycle.
    outage = min(budget.horizon_seconds, budget.archive_unavailable_seconds + idle)
    remaining = budget.horizon_seconds - outage
    arrival_bytes_per_chunk = (
        budget.sealed_bytes_per_second / budget.chunks_per_second
        if budget.chunks_per_second
        else 0.0
    )
    # Charge old tiny chunks as equivalent incoming-mix bytes as well as their
    # actual stored size. Otherwise file-limited retirement of old chunks can
    # falsely appear to free bytes from newly arriving large chunks.
    initial_equivalent = max(
        budget.initial_backlog_bytes, budget.initial_backlog_chunks * arrival_bytes_per_chunk
    )
    backlog_bytes = initial_equivalent + budget.sealed_bytes_per_second * outage
    backlog_chunks = budget.initial_backlog_chunks + budget.chunks_per_second * outage
    stable = (
        byte_service > budget.sealed_bytes_per_second and file_service > budget.chunks_per_second
    )
    catchup = (
        max(
            backlog_bytes / (byte_service - budget.sealed_bytes_per_second),
            backlog_chunks / (file_service - budget.chunks_per_second),
        )
        if stable
        else None
    )
    mix_bytes_per_chunk = arrival_bytes_per_chunk or (
        budget.initial_backlog_bytes / budget.initial_backlog_chunks
        if budget.initial_backlog_chunks
        else 0.0
    )
    effective_service = min(byte_service, file_service * mix_bytes_per_chunk)
    end_backlog = max(
        0.0, backlog_bytes + (budget.sealed_bytes_per_second - effective_service) * remaining
    )
    peak_backlog = max(backlog_bytes, end_backlog)
    persistent_growth = (
        budget.retained_metadata_bytes_per_second + budget.other_writer_growth_bytes_per_second
    )
    retained = persistent_growth * budget.horizon_seconds
    transient = (
        budget.active_raw_peak_bytes
        + budget.seal_temporary_peak_bytes
        + budget.archive_temporary_peak_bytes
        + budget.wal_peak_bytes
        + budget.other_temporary_peak_bytes
    )
    # Conservative simultaneous upper envelope: horizon-end persistent growth
    # plus largest backlog and all declared temporary/active peaks.
    writer_min = math.floor(budget.writer_free_bytes - retained - transient - peak_backlog)
    archive_metadata = math.ceil(
        (budget.initial_backlog_chunks + budget.chunks_per_second * budget.horizon_seconds)
        * budget.archive_metadata_bytes_per_chunk
    )
    archive_needed = math.ceil(
        budget.initial_backlog_bytes
        + budget.sealed_bytes_per_second * budget.horizon_seconds
        + archive_metadata
        + budget.archive_temporary_peak_bytes
    )
    archive_min = budget.archive_free_bytes - archive_needed
    reasons = []
    if not stable:
        reasons.append("archive_service_not_above_arrival_in_bytes_and_files")
    if catchup is None or catchup > remaining:
        reasons.append("backlog_not_caught_up_within_horizon")
    if writer_min <= HARD_RESERVE_BYTES:
        reasons.append("writer_peak_at_or_below_10GiB_hard_reserve")
    if archive_min <= 0:
        reasons.append("archive_target_capacity_exhausted")
    no_archive_runway = (
        max(
            0.0,
            (
                budget.writer_free_bytes
                - HARD_RESERVE_BYTES
                - transient
                - budget.initial_backlog_bytes
            )
            / (budget.sealed_bytes_per_second + persistent_growth),
        )
        if budget.sealed_bytes_per_second + persistent_growth > 0
        else None
    )
    return {
        "kind": "R3_OFFLINE_SCENARIO_NOT_PRODUCTION_ADMISSION",
        "inputs": asdict(budget),
        "archive_duty_fraction": duty,
        "normal_archive_gap_bound_seconds": idle,
        "peak_arrival_without_service_seconds": outage,
        "archive_bytes_per_wall_second": byte_service,
        "archive_chunks_per_wall_second": file_service,
        "archive_service_above_arrival": stable,
        "post_outage_catchup_seconds": catchup,
        "initial_backlog_equivalent_bytes": math.ceil(initial_equivalent),
        "peak_sealed_backlog_bytes": math.ceil(peak_backlog),
        "end_sealed_backlog_bytes": math.ceil(end_backlog),
        "retained_writer_growth_bytes": math.ceil(retained),
        "active_and_temporary_peak_bytes": transient,
        "writer_free_lower_bound_bytes": writer_min,
        "writer_hard_reserve_bytes": HARD_RESERVE_BYTES,
        "archive_metadata_growth_bytes": archive_metadata,
        "archive_free_lower_bound_bytes": archive_min,
        "archive_unavailable_runway_seconds": no_archive_runway,
        "scenario_fits": not reasons,
        "blocking_reasons": reasons,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            evaluate(DiskBudget(**json.loads(args.input.read_text()))), indent=2, allow_nan=False
        )
    )


if __name__ == "__main__":
    main()
