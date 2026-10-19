from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest

from tools.r3_capacity import DiskBudget, evaluate

GIB = 1024**3


def scenario() -> DiskBudget:
    return DiskBudget(
        writer_free_bytes=20 * GIB,
        archive_free_bytes=100 * GIB,
        horizon_seconds=86400,
        archive_unavailable_seconds=3600,
        sealed_bytes_per_second=100_000,
        chunks_per_second=1,
        archive_bytes_per_work_second=1_000_000,
        archive_chunks_per_work_second=10,
        archive_work_seconds=50,
        archive_cycle_seconds=120,
        archive_max_files_per_cycle=1000,
        retained_metadata_bytes_per_second=10_000,
        archive_metadata_bytes_per_chunk=4096,
        other_writer_growth_bytes_per_second=1000,
        initial_backlog_bytes=0,
        initial_backlog_chunks=0,
        active_raw_peak_bytes=200_000_000,
        seal_temporary_peak_bytes=100_000_000,
        archive_temporary_peak_bytes=10_000_000,
        wal_peak_bytes=50_000_000,
        other_temporary_peak_bytes=100_000_000,
    )


def test_disk_budget_includes_retained_growth_outage_and_temporary_space() -> None:
    result = evaluate(scenario())
    assert result["scenario_fits"] is True
    assert result["retained_writer_growth_bytes"] == 950_400_000
    assert result["peak_sealed_backlog_bytes"] == 372_000_000
    assert result["writer_free_lower_bound_bytes"] == 20 * GIB - 1_782_400_000
    assert result["post_outage_catchup_seconds"] == pytest.approx(1174.736842)
    assert result["archive_free_lower_bound_bytes"] == 100 * GIB - 9_003_894_400


def test_file_limited_archive_cannot_hide_behind_byte_throughput() -> None:
    result = evaluate(replace(scenario(), archive_chunks_per_work_second=1))
    assert result["scenario_fits"] is False
    assert result["archive_service_above_arrival"] is False
    assert cast(int, result["end_sealed_backlog_bytes"]) >= cast(
        int, result["peak_sealed_backlog_bytes"]
    )
    assert result["post_outage_catchup_seconds"] is None


def test_long_pause_and_large_corpus_block_even_if_archive_is_fast() -> None:
    result = evaluate(replace(scenario(), archive_unavailable_seconds=86400))
    assert result["scenario_fits"] is False
    assert "backlog_not_caught_up_within_horizon" in cast(list[str], result["blocking_reasons"])
    assert evaluate(replace(scenario(), archive_free_bytes=GIB))["scenario_fits"] is False


def test_strict_reserve_boundary_and_existing_backlog_are_accounted() -> None:
    boundary = replace(scenario(), writer_free_bytes=10 * GIB + 1_782_400_000)
    assert evaluate(boundary)["scenario_fits"] is False
    assert (
        evaluate(replace(boundary, writer_free_bytes=boundary.writer_free_bytes + 1))[
            "scenario_fits"
        ]
        is True
    )
    old = evaluate(replace(scenario(), initial_backlog_bytes=GIB, initial_backlog_chunks=1000))
    assert old["peak_sealed_backlog_bytes"] == GIB + 372_000_000


@pytest.mark.parametrize(
    "changes",
    [
        {"horizon_seconds": 0},
        {"chunks_per_second": 0},
        {"archive_work_seconds": 121},
        {"wal_peak_bytes": -1},
        {"sealed_bytes_per_second": float("nan")},
        {"archive_unavailable_seconds": 86401},
    ],
)
def test_invalid_budget_is_not_a_capacity_result(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        replace(scenario(), **changes)


def test_normal_timer_idle_gap_is_budgeted_even_without_failure() -> None:
    budget = replace(scenario(), archive_unavailable_seconds=0)
    result = evaluate(budget)
    assert result["peak_sealed_backlog_bytes"] == 12_000_000
    assert result["peak_arrival_without_service_seconds"] == 120
    # Without the 120s service gap this exact-reserve plan would falsely fit.
    near_reserve = replace(budget, writer_free_bytes=10 * GIB + 1_410_400_000)
    assert evaluate(near_reserve)["scenario_fits"] is False
    short = evaluate(replace(budget, horizon_seconds=30))
    assert short["peak_sealed_backlog_bytes"] == 3_000_000
    assert short["scenario_fits"] is False


def test_old_tiny_chunks_consume_file_service_before_large_new_chunks() -> None:
    result = evaluate(replace(scenario(), initial_backlog_bytes=1000, initial_backlog_chunks=1000))
    assert result["initial_backlog_equivalent_bytes"] == 100_000_000
    assert result["peak_sealed_backlog_bytes"] == 472_000_000


def test_target_manifest_growth_can_exhaust_archive_before_raw_bytes() -> None:
    result = evaluate(replace(scenario(), archive_free_bytes=8_650_000_001))
    assert result["archive_metadata_growth_bytes"] == 353_894_400
    assert result["scenario_fits"] is False
    assert "archive_target_capacity_exhausted" in cast(list[str], result["blocking_reasons"])


def test_worker_file_limit_caps_wall_clock_service_even_with_fast_small_files() -> None:
    result = evaluate(replace(scenario(), archive_max_files_per_cycle=50))
    assert result["archive_chunks_per_wall_second"] == pytest.approx(50 / 120)
    assert result["archive_service_above_arrival"] is False
    assert result["scenario_fits"] is False
