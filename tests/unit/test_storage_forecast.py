from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.forecast import (
    GIB,
    WINDOWS_SECONDS,
    StorageForecaster,
    _eta,
    _sample_int,
    _window_rate,
    hard_reserve_bytes,
    space_severity,
    threshold_bytes,
)
from tests.archive_support import prepare_archive

HOUR_NS = 3_600_000_000_000
DAY_NS = 24 * HOUR_NS


def record_sample(
    catalog: Catalog,
    *,
    sample_id: str,
    observed_at_utc_ns: int,
    free_bytes: int,
    storage_id: str | None = None,
    total_bytes: int = 1_000 * GIB,
    archive_backlog_bytes: int = 0,
    oldest_unarchived_at_utc_ns: int | None = None,
) -> bool:
    return catalog.record_space_sample(
        sample_id=sample_id,
        scope_id="internal",
        storage_id=storage_id,
        observed_at_utc_ns=observed_at_utc_ns,
        total_bytes=total_bytes,
        free_bytes=free_bytes,
        archive_backlog_bytes=archive_backlog_bytes,
        oldest_unarchived_at_utc_ns=oldest_unarchived_at_utc_ns,
        severity=space_severity(total_bytes, free_bytes),
    )


def legacy_full_history_forecast(
    catalog: Catalog, *, scope_id: str, now_utc_ns: int
) -> dict[str, object]:
    """Reference the pre-optimization full-history forecast acquisition."""

    samples = catalog.space_samples(scope_id)
    current = max(
        (
            row
            for row in samples
            if _sample_int(row, "observed_at_utc_ns") <= now_utc_ns
        ),
        key=lambda row: _sample_int(row, "observed_at_utc_ns"),
        default=None,
    )
    if current is None:
        return {
            "scope_id": scope_id,
            "status": "INSUFFICIENT_DATA",
            "sample_count": 0,
        }
    total = _sample_int(current, "total_bytes")
    free = _sample_int(current, "free_bytes")
    windows = {
        name: _window_rate(
            samples, now_utc_ns=now_utc_ns, window_seconds=window_seconds
        )
        for name, window_seconds in WINDOWS_SECONDS.items()
    }
    available_rates = [
        float(value["bytes_per_second"])
        for value in windows.values()
        if value["status"] == "AVAILABLE"
        and isinstance(value["bytes_per_second"], (int, float))
    ]
    selected_rate = max(available_rates) if available_rates else None
    rate_status = (
        "INSUFFICIENT_DATA"
        if selected_rate is None
        else ("NOT_APPROACHING" if selected_rate <= 0 else "AVAILABLE")
    )
    thresholds = threshold_bytes(total)
    oldest_unarchived = current.get("oldest_unarchived_at_utc_ns")
    return {
        "scope_id": scope_id,
        "storage_id": current.get("storage_id"),
        "status": space_severity(total, free),
        "observed_at_utc_ns": _sample_int(current, "observed_at_utc_ns"),
        "total_bytes": total,
        "free_bytes": free,
        "free_percent": round(free / total * 100, 6),
        "threshold_bytes": thresholds,
        "net_growth": {
            "status": rate_status,
            "selected_bytes_per_second": (
                None if selected_rate is None else round(selected_rate, 6)
            ),
            "selection": "MAX_AVAILABLE_WINDOW_MEDIAN",
            "windows": windows,
        },
        "eta": {
            name: _eta(
                now_utc_ns=now_utc_ns,
                free_bytes=free,
                threshold=threshold,
                rate_bytes_per_second=selected_rate,
                rate_status=rate_status,
            )
            for name, threshold in thresholds.items()
        },
        "archive_backlog_bytes": _sample_int(
            current, "archive_backlog_bytes"
        ),
        "oldest_unarchived_at_utc_ns": oldest_unarchived,
        "oldest_unarchived_age_ns": (
            max(0, now_utc_ns - oldest_unarchived)
            if isinstance(oldest_unarchived, int)
            and not isinstance(oldest_unarchived, bool)
            else None
        ),
        "sample_count": len(samples),
    }


def observe_series(
    forecaster: StorageForecaster,
    *,
    total: int,
    free_values: list[int],
    interval_hours: int = 1,
) -> int:
    for ordinal, free in enumerate(free_values):
        forecaster.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=total,
            free_bytes=free,
            observed_at_utc_ns=ordinal * interval_hours * HOUR_NS,
        )
    return (len(free_values) - 1) * interval_hours * HOUR_NS


def test_threshold_boundaries_and_hard_reserve_are_exact() -> None:
    total = 100 * GIB
    assert threshold_bytes(total) == {
        "warning_40_percent": 40 * GIB,
        "critical_15_percent": 15 * GIB,
        "emergency": 10 * GIB,
        "exhausted": 0,
    }
    assert space_severity(total, 40 * GIB) == "WARNING"
    assert space_severity(total, 15 * GIB) == "CRITICAL"
    assert space_severity(total, 10 * GIB) == "EMERGENCY"
    assert space_severity(total, 41 * GIB) == "OK"
    assert hard_reserve_bytes(total, rotation_bytes=128 * 1024**2) == 5 * GIB


def test_insufficient_and_nonpositive_growth_have_exact_sentinels(
    tmp_path: Path,
) -> None:
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        forecaster.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=100 * GIB,
            free_bytes=70 * GIB,
            observed_at_utc_ns=0,
        )
        insufficient = forecaster.forecast("internal", now_utc_ns=0)
        insufficient_net = cast(dict[str, object], insufficient["net_growth"])
        assert insufficient_net["status"] == "INSUFFICIENT_DATA"
        assert all(
            value["status"] == "INSUFFICIENT_DATA"
            for value in cast(dict[str, dict[str, object]], insufficient["eta"]).values()
        )

    with Catalog(tmp_path / "negative.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        now = observe_series(
            forecaster,
            total=100 * GIB,
            free_values=[60 * GIB + ordinal * GIB for ordinal in range(8)],
            interval_hours=24,
        )
        result = forecaster.forecast("internal", now_utc_ns=now)
        net = cast(dict[str, object], result["net_growth"])
        assert net["status"] == "NOT_APPROACHING"
        windows = cast(dict[str, dict[str, object]], net["windows"])
        assert windows["1h"]["status"] == "INSUFFICIENT_DATA"
        assert windows["6h"]["status"] == "INSUFFICIENT_DATA"
        assert windows["24h"]["status"] == "AVAILABLE"
        eta = cast(dict[str, dict[str, object]], result["eta"])
        assert all(value["status"] == "NOT_APPROACHING" for value in eta.values())


def test_all_windows_and_etas_are_deterministic_and_finite(tmp_path: Path) -> None:
    total = 1_000 * GIB
    consumption_per_hour = GIB
    free_values = [900 * GIB - ordinal * consumption_per_hour for ordinal in range(169)]
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        now = observe_series(
            forecaster, total=total, free_values=free_values, interval_hours=1
        )
        result = forecaster.forecast("internal", now_utc_ns=now)
    net = cast(dict[str, Any], result["net_growth"])
    windows = cast(dict[str, dict[str, object]], net["windows"])
    assert set(windows) == {"1h", "6h", "24h", "7d"}
    assert all(value["status"] == "AVAILABLE" for value in windows.values())
    assert net["selected_bytes_per_second"] == round(GIB / 3600, 6)
    eta = cast(dict[str, dict[str, object]], result["eta"])
    assert all(value["status"] == "FORECAST" for value in eta.values())
    assert (
        cast(int, eta["warning_40_percent"]["utc_ns"])
        < cast(int, eta["critical_15_percent"]["utc_ns"])
        < cast(int, eta["emergency"]["utc_ns"])
        < cast(int, eta["exhausted"]["utc_ns"])
    )
    json.dumps(result, allow_nan=False)


def test_archive_release_changes_net_growth_to_not_approaching(
    tmp_path: Path,
) -> None:
    total = 100 * GIB
    # Four hours of +1 GiB/h local growth, then five hours of archive-driven
    # -3 GiB/h net growth. Consecutive-slope median is negative.
    free_values = [
        80 * GIB,
        79 * GIB,
        78 * GIB,
        77 * GIB,
        76 * GIB,
        79 * GIB,
        82 * GIB,
        85 * GIB,
        88 * GIB,
        91 * GIB,
    ]
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        now = observe_series(
            forecaster, total=total, free_values=free_values, interval_hours=1
        )
        result = forecaster.forecast("internal", now_utc_ns=now)
    result_net = cast(dict[str, object], result["net_growth"])
    assert result_net["status"] == "NOT_APPROACHING"


def test_window_median_rejects_single_consumption_outlier(tmp_path: Path) -> None:
    total = 1_000 * GIB
    free_values = [
        900 * GIB,
        899 * GIB,
        898 * GIB,
        848 * GIB,
        847 * GIB,
        846 * GIB,
        845 * GIB,
    ]
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        now = observe_series(
            forecaster, total=total, free_values=free_values, interval_hours=1
        )
        result = forecaster.forecast("internal", now_utc_ns=now)
    net = cast(dict[str, Any], result["net_growth"])
    windows = cast(dict[str, dict[str, object]], net["windows"])
    assert windows["6h"]["bytes_per_second"] == round(GIB / 3600, 6)


def test_alert_transitions_are_persisted_once(tmp_path: Path) -> None:
    total = 100 * GIB
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        for ordinal, free in enumerate((50, 40, 14, 9, 50)):
            forecaster.observe(
                scope_id="internal",
                storage_id=None,
                total_bytes=total,
                free_bytes=free * GIB,
                observed_at_utc_ns=ordinal,
            )
        assert not forecaster.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=total,
            free_bytes=50 * GIB,
            observed_at_utc_ns=4,
        )
        events = catalog.storage_alert_events(scope_id="internal")
    assert [event["to_severity"] for event in events] == [
        "OK",
        "WARNING",
        "CRITICAL",
        "EMERGENCY",
        "OK",
    ]


def test_external_target_has_independent_history_and_capacity_state(
    tmp_path: Path,
) -> None:
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        for ordinal, free in enumerate((80 * GIB, 79 * GIB)):
            forecaster.observe(
                scope_id="external:archive-a",
                storage_id="archive-a",
                total_bytes=100 * GIB,
                free_bytes=free,
                observed_at_utc_ns=ordinal * HOUR_NS,
            )
        result = forecaster.forecast(
            "external:archive-a", now_utc_ns=HOUR_NS
        )
    assert result["storage_id"] == "archive-a"
    assert result["status"] == "OK"
    assert result["archive_backlog_bytes"] == 0
    net = cast(dict[str, object], result["net_growth"])
    assert net["status"] == "AVAILABLE"


def test_backlog_and_oldest_age_follow_verified_archive(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=2)
    with Catalog(prepared.layout.catalog) as catalog:
        forecaster = StorageForecaster(
            catalog=catalog, data_root=prepared.layout.root
        )
        forecaster.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=100 * GIB,
            free_bytes=70 * GIB,
            observed_at_utc_ns=10,
        )
        before = forecaster.forecast("internal", now_utc_ns=10)
        assert cast(int, before["archive_backlog_bytes"]) > 0
        assert before["oldest_unarchived_at_utc_ns"] == 1_700_000_000_000_000_000

        ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        ).run_once()
        forecaster.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=100 * GIB,
            free_bytes=71 * GIB,
            observed_at_utc_ns=20,
        )
        after = forecaster.forecast("internal", now_utc_ns=20)
        assert cast(int, after["archive_backlog_bytes"]) < cast(
            int, before["archive_backlog_bytes"]
        )
        assert after["oldest_unarchived_at_utc_ns"] == 1_700_000_000_000_000_001


def test_bounded_forecast_matches_full_history_reference_at_multiple_times(
    tmp_path: Path,
) -> None:
    now_values = (15 * DAY_NS, 31 * DAY_NS, 40 * DAY_NS)
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        total = 1_000 * GIB
        for ordinal in range(0, 41 * 4 + 1):
            observed = ordinal * 6 * HOUR_NS
            record_sample(
                catalog,
                sample_id=f"history-{ordinal:04d}",
                observed_at_utc_ns=observed,
                free_bytes=900 * GIB - ordinal * GIB,
                total_bytes=total,
            )
        record_sample(
            catalog,
            sample_id="future",
            observed_at_utc_ns=45 * DAY_NS,
            free_bytes=123 * GIB,
            total_bytes=total,
        )
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        for now in now_values:
            assert forecaster.forecast("internal", now_utc_ns=now) == (
                legacy_full_history_forecast(
                    catalog, scope_id="internal", now_utc_ns=now
                )
            )


def test_exact_seven_day_cutoff_is_the_predecessor_side(tmp_path: Path) -> None:
    now = 10 * DAY_NS
    cutoff = now - WINDOWS_SECONDS["7d"] * 1_000_000_000
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        record_sample(
            catalog,
            sample_id="cutoff",
            observed_at_utc_ns=cutoff,
            free_bytes=900 * GIB,
        )
        record_sample(
            catalog,
            sample_id="inside",
            observed_at_utc_ns=cutoff + DAY_NS,
            free_bytes=899 * GIB,
        )
        record_sample(
            catalog,
            sample_id="current",
            observed_at_utc_ns=now,
            free_bytes=898 * GIB,
        )
        assert catalog.space_samples_between(
            "internal",
            start_exclusive_utc_ns=cutoff,
            end_inclusive_utc_ns=now,
        )[0]["sample_id"] == "inside"
        assert catalog.latest_space_sample_at_or_before("internal", cutoff) is not None
        result = StorageForecaster(catalog=catalog, data_root=tmp_path).forecast(
            "internal", now_utc_ns=now
        )
    windows = cast(
        dict[str, dict[str, object]],
        cast(dict[str, object], result["net_growth"])["windows"],
    )
    assert windows["7d"]["status"] == "AVAILABLE"
    assert windows["7d"]["sample_count"] == 3


def test_seven_day_predecessor_tolerance_boundary_is_inclusive(tmp_path: Path) -> None:
    now = 20 * DAY_NS
    window_seconds = WINDOWS_SECONDS["7d"]
    cutoff = now - window_seconds * 1_000_000_000
    tolerance = int(window_seconds * 0.2 * 1_000_000_000)
    counts: list[int] = []
    for ordinal, predecessor_at in enumerate(
        (cutoff - tolerance, cutoff - tolerance - 1)
    ):
        path = tmp_path / f"boundary-{ordinal}.sqlite"
        with Catalog(path) as catalog:
            record_sample(
                catalog,
                sample_id="predecessor",
                observed_at_utc_ns=predecessor_at,
                free_bytes=900 * GIB,
            )
            record_sample(
                catalog,
                sample_id="inside",
                observed_at_utc_ns=cutoff + DAY_NS,
                free_bytes=899 * GIB,
            )
            record_sample(
                catalog,
                sample_id="current",
                observed_at_utc_ns=now,
                free_bytes=898 * GIB,
            )
            result = StorageForecaster(catalog=catalog, data_root=tmp_path).forecast(
                "internal", now_utc_ns=now
            )
            windows = cast(
                dict[str, dict[str, object]],
                cast(dict[str, object], result["net_growth"])["windows"],
            )
            counts.append(cast(int, windows["7d"]["sample_count"]))
    assert counts == [3, 2]


def test_short_window_predecessors_remain_inside_bounded_read_set(
    tmp_path: Path,
) -> None:
    now = 10 * DAY_NS
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        for name, seconds, ordinal in (
            ("1h", WINDOWS_SECONDS["1h"], 1),
            ("6h", WINDOWS_SECONDS["6h"], 6),
            ("24h", WINDOWS_SECONDS["24h"], 24),
        ):
            record_sample(
                catalog,
                sample_id=f"{name}-predecessor",
                observed_at_utc_ns=now - seconds * 1_000_000_000,
                free_bytes=900 * GIB - ordinal * GIB,
            )
        record_sample(
            catalog,
            sample_id="current",
            observed_at_utc_ns=now,
            free_bytes=899 * GIB,
        )
        result = StorageForecaster(catalog=catalog, data_root=tmp_path).forecast(
            "internal", now_utc_ns=now
        )
    windows = cast(
        dict[str, dict[str, object]],
        cast(dict[str, object], result["net_growth"])["windows"],
    )
    assert all(windows[name]["status"] == "AVAILABLE" for name in ("1h", "6h", "24h"))


def test_future_rows_preserve_current_forecast_and_historical_count(
    tmp_path: Path,
) -> None:
    now = 8 * DAY_NS
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        record_sample(
            catalog,
            sample_id="past",
            observed_at_utc_ns=now - DAY_NS,
            free_bytes=900 * GIB,
            storage_id="archive-a",
        )
        record_sample(
            catalog,
            sample_id="current",
            observed_at_utc_ns=now,
            free_bytes=899 * GIB,
            storage_id="archive-a",
        )
        record_sample(
            catalog,
            sample_id="future",
            observed_at_utc_ns=now + DAY_NS,
            free_bytes=100 * GIB,
            storage_id="future-storage",
        )
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        candidate = forecaster.forecast("internal", now_utc_ns=now)
        reference = legacy_full_history_forecast(
            catalog, scope_id="internal", now_utc_ns=now
        )
    assert candidate == reference
    assert candidate["sample_count"] == 3
    assert candidate["storage_id"] == "archive-a"


def test_all_future_rows_remain_insufficient_with_zero_count(tmp_path: Path) -> None:
    now = 8 * DAY_NS
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        record_sample(
            catalog,
            sample_id="future",
            observed_at_utc_ns=now + DAY_NS,
            free_bytes=900 * GIB,
        )
        result = StorageForecaster(catalog=catalog, data_root=tmp_path).forecast(
            "internal", now_utc_ns=now
        )
    assert result == {
        "scope_id": "internal",
        "status": "INSUFFICIENT_DATA",
        "sample_count": 0,
    }


def test_ancient_free_space_perturbation_cannot_change_forecast(
    tmp_path: Path,
) -> None:
    now = 30 * DAY_NS
    results: list[dict[str, object]] = []
    for ordinal, ancient_free in enumerate((100 * GIB, 900 * GIB)):
        with Catalog(tmp_path / f"ancient-{ordinal}.sqlite") as catalog:
            record_sample(
                catalog,
                sample_id="ancient",
                observed_at_utc_ns=0,
                free_bytes=ancient_free,
            )
            record_sample(
                catalog,
                sample_id="recent-predecessor",
                observed_at_utc_ns=now - 6 * DAY_NS,
                free_bytes=800 * GIB,
            )
            record_sample(
                catalog,
                sample_id="current",
                observed_at_utc_ns=now,
                free_bytes=799 * GIB,
            )
            results.append(
                StorageForecaster(catalog=catalog, data_root=tmp_path).forecast(
                    "internal", now_utc_ns=now
                )
            )
    assert results[0] == results[1]


def test_same_timestamp_current_and_predecessor_ties_match_ordered_history(
    tmp_path: Path,
) -> None:
    now = 10 * DAY_NS
    cutoff = now - WINDOWS_SECONDS["7d"] * 1_000_000_000
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        record_sample(
            catalog,
            sample_id="z-predecessor",
            observed_at_utc_ns=cutoff,
            free_bytes=700 * GIB,
            storage_id="z-storage",
        )
        record_sample(
            catalog,
            sample_id="a-predecessor",
            observed_at_utc_ns=cutoff,
            free_bytes=800 * GIB,
            storage_id="a-storage",
        )
        record_sample(
            catalog,
            sample_id="z-current",
            observed_at_utc_ns=now,
            free_bytes=500 * GIB,
            storage_id="z-current-storage",
            archive_backlog_bytes=99,
            oldest_unarchived_at_utc_ns=456,
        )
        record_sample(
            catalog,
            sample_id="a-current",
            observed_at_utc_ns=now,
            free_bytes=600 * GIB,
            storage_id="a-current-storage",
            archive_backlog_bytes=11,
            oldest_unarchived_at_utc_ns=123,
        )
        current_at_cutoff = catalog.latest_space_sample_at_or_before(
            "internal", cutoff
        )
        predecessor_at_cutoff = (
            catalog.latest_predecessor_space_sample_at_or_before(
                "internal", cutoff
            )
        )
        assert current_at_cutoff is not None
        assert predecessor_at_cutoff is not None
        assert current_at_cutoff["sample_id"] == "a-predecessor"
        assert predecessor_at_cutoff["sample_id"] == "z-predecessor"
        result = StorageForecaster(catalog=catalog, data_root=tmp_path).forecast(
            "internal", now_utc_ns=now
        )
        reference = legacy_full_history_forecast(
            catalog, scope_id="internal", now_utc_ns=now
        )
    assert result == reference
    assert result["storage_id"] == "a-current-storage"
    assert result["free_bytes"] == 600 * GIB
    assert result["archive_backlog_bytes"] == 11
    assert result["oldest_unarchived_at_utc_ns"] == 123


class NoFullHistoryCatalog:
    def __init__(self, catalog: Catalog) -> None:
        self.catalog = catalog
        self.range_arguments: tuple[int, int] | None = None
        self.materialized_rows = 0
        self.read_set: tuple[
            list[dict[str, object]],
            dict[str, object] | None,
            dict[str, object] | None,
            int,
        ] | None = None

    def space_samples(self, _scope_id: str) -> list[dict[str, object]]:
        raise AssertionError("unbounded space_samples() was called")

    def space_forecast_read_set(
        self,
        scope_id: str,
        *,
        start_exclusive_utc_ns: int,
        end_inclusive_utc_ns: int,
    ) -> tuple[
        list[dict[str, object]],
        dict[str, object] | None,
        dict[str, object] | None,
        int,
    ]:
        self.range_arguments = (start_exclusive_utc_ns, end_inclusive_utc_ns)
        read_set = self.catalog.space_forecast_read_set(
            scope_id,
            start_exclusive_utc_ns=start_exclusive_utc_ns,
            end_inclusive_utc_ns=end_inclusive_utc_ns,
        )
        self.read_set = read_set
        samples, predecessor, current, _ = read_set
        self.materialized_rows += len(samples)
        self.materialized_rows += int(predecessor is not None)
        self.materialized_rows += int(current is not None)
        return read_set


def test_forecast_rate_input_is_mechanically_bounded(tmp_path: Path) -> None:
    now = 30 * DAY_NS
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        for ordinal in range(200):
            record_sample(
                catalog,
                sample_id=f"ancient-{ordinal:03d}",
                observed_at_utc_ns=ordinal,
                free_bytes=900 * GIB,
            )
        record_sample(
            catalog,
            sample_id="recent",
            observed_at_utc_ns=now,
            free_bytes=899 * GIB,
        )
        probe = NoFullHistoryCatalog(catalog)
        result = StorageForecaster(catalog=probe, data_root=tmp_path).forecast(
            "internal", now_utc_ns=now
        )
        expected_cutoff = now - WINDOWS_SECONDS["7d"] * 1_000_000_000
        total_count = catalog.space_sample_count("internal")
    assert result["sample_count"] == total_count
    assert probe.range_arguments == (expected_cutoff, now)
    assert probe.materialized_rows == 3
    assert probe.materialized_rows < total_count


def test_forecast_read_set_pins_snapshot_and_allows_writer_commit(
    tmp_path: Path,
) -> None:
    now = 10 * DAY_NS
    cutoff = now - WINDOWS_SECONDS["7d"] * 1_000_000_000
    path = tmp_path / "snapshot.sqlite"
    with Catalog(path) as seed:
        record_sample(
            seed,
            sample_id="generation-g-predecessor",
            observed_at_utc_ns=cutoff,
            free_bytes=900 * GIB,
        )
        record_sample(
            seed,
            sample_id="generation-g-current",
            observed_at_utc_ns=now,
            free_bytes=899 * GIB,
        )

    reader = Catalog(path)
    writer = Catalog(path)
    try:
        pre_write = legacy_full_history_forecast(
            reader, scope_id="internal", now_utc_ns=now
        )
        trace: list[str] = []
        reader_paused_before_predecessor = threading.Event()
        writer_commit_completed = threading.Event()
        writer_commit_seen_by_reader = threading.Event()
        writer_inserted: list[bool] = []
        writer_errors: list[BaseException] = []

        def writer_task() -> None:
            if not reader_paused_before_predecessor.wait(timeout=5):
                writer_errors.append(
                    AssertionError("reader did not reach predecessor SELECT")
                )
                writer_commit_completed.set()
                return
            try:
                writer_inserted.append(
                    record_sample(
                        writer,
                        sample_id="generation-g-plus-one",
                        observed_at_utc_ns=now - HOUR_NS,
                        free_bytes=100 * GIB,
                    )
                )
            except BaseException as exc:
                writer_errors.append(exc)
            finally:
                writer_commit_completed.set()

        def trace_callback(statement: str) -> None:
            normalized = " ".join(statement.split()).upper()
            trace.append(normalized)
            if "ORDER BY OBSERVED_AT_UTC_NS DESC, SAMPLE_ID DESC" in normalized:
                reader_paused_before_predecessor.set()
                if writer_commit_completed.wait(timeout=5):
                    writer_commit_seen_by_reader.set()

        reader._connection.set_trace_callback(trace_callback)
        writer_thread = threading.Thread(target=writer_task)
        writer_thread.start()
        try:
            probe = NoFullHistoryCatalog(reader)
            result = StorageForecaster(catalog=probe, data_root=tmp_path).forecast(
                "internal", now_utc_ns=now
            )
        finally:
            reader._connection.set_trace_callback(None)
            writer_thread.join(timeout=5)

        assert not writer_thread.is_alive()
        assert writer_inserted == [True]
        assert not writer_errors
        assert writer_commit_seen_by_reader.is_set()
        assert reader_paused_before_predecessor.is_set()
        assert result == pre_write
        assert probe.materialized_rows == 1 + 1 + 1
        assert probe.range_arguments == (cutoff, now)

        assert probe.read_set is not None
        read_set = probe.read_set
        bounded, predecessor, current, sample_count = read_set
        assert [row["sample_id"] for row in bounded] == [
            "generation-g-current"
        ]
        assert predecessor is not None
        assert predecessor["sample_id"] == "generation-g-predecessor"
        assert current is not None
        assert current["sample_id"] == "generation-g-current"
        assert sample_count == 2

        post_write = legacy_full_history_forecast(
            reader, scope_id="internal", now_utc_ns=now
        )
        assert post_write["sample_count"] == 3
        assert post_write != pre_write
        pre_write_net = cast(dict[str, object], pre_write["net_growth"])
        post_write_net = cast(dict[str, object], post_write["net_growth"])
        assert pre_write_net["selected_bytes_per_second"] != post_write_net[
            "selected_bytes_per_second"
        ]

        assert not reader._connection.in_transaction
        assert trace[0] == "BEGIN"
        assert trace[-1] == "COMMIT"
        selects = [statement for statement in trace if statement.startswith("SELECT")]
        assert len(selects) == 4
        assert "OBSERVED_AT_UTC_NS >" in selects[0]
        assert "OBSERVED_AT_UTC_NS <=" in selects[0]
        assert "ORDER BY OBSERVED_AT_UTC_NS, SAMPLE_ID" in selects[0]
        assert "DESC" not in selects[0]
        assert "ORDER BY OBSERVED_AT_UTC_NS DESC, SAMPLE_ID DESC" in selects[1]
        assert "ORDER BY OBSERVED_AT_UTC_NS DESC, SAMPLE_ID ASC" in selects[2]
        assert selects[3].startswith("SELECT COUNT(*) AS SAMPLE_COUNT")
        assert all("BEGIN IMMEDIATE" not in statement for statement in trace)
    finally:
        reader.close()
        writer.close()


def test_forecast_read_set_rolls_back_after_read_failure(tmp_path: Path) -> None:
    path = tmp_path / "rollback.sqlite"
    now = 10 * DAY_NS
    cutoff = now - WINDOWS_SECONDS["7d"] * 1_000_000_000
    with Catalog(path) as catalog:
        record_sample(
            catalog,
            sample_id="rollback-seed",
            observed_at_utc_ns=now,
            free_bytes=900 * GIB,
        )
        trace: list[str] = []

        def trace_callback(statement: str) -> None:
            trace.append(" ".join(statement.split()).upper())

        def deny_space_sample_reads(
            action: int,
            arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            if action == sqlite3.SQLITE_READ and arg1 == "storage_space_samples":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        catalog._connection.set_trace_callback(trace_callback)
        catalog._connection.set_authorizer(deny_space_sample_reads)
        try:
            with pytest.raises(sqlite3.DatabaseError):
                catalog.space_forecast_read_set(
                    "internal",
                    start_exclusive_utc_ns=cutoff,
                    end_inclusive_utc_ns=now,
                )
        finally:
            catalog._connection.set_authorizer(None)
            catalog._connection.set_trace_callback(None)

        assert trace[0] == "BEGIN"
        assert "ROLLBACK" in trace
        assert not catalog._connection.in_transaction
