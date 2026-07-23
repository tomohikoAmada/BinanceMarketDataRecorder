from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.forecast import (
    GIB,
    StorageForecaster,
    hard_reserve_bytes,
    space_severity,
    threshold_bytes,
)
from tests.archive_support import prepare_archive

HOUR_NS = 3_600_000_000_000


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
