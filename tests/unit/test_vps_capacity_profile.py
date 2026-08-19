from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.storage.capacity import (
    CRITICAL_BYTES,
    EMERGENCY_BYTES,
    ETA_7D_NS,
    ETA_24H_NS,
    ETA_72H_NS,
    HARD_RESERVE_BYTES,
    VPS_PRODUCTION_V1,
    WARNING_BYTES,
    CapacityDecision,
    VpsCapacityState,
    classify_absolute_state,
    classify_eta_state,
    evaluate_capacity,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.forecast import GIB, StorageForecaster

HOUR_NS = 3_600_000_000_000


def decision(
    *,
    free_bytes: int,
    eta: dict[str, object],
    total_bytes: int = 40 * GIB,
    now_utc_ns: int = 0,
) -> CapacityDecision:
    return evaluate_capacity(
        profile=VPS_PRODUCTION_V1,
        scope_id="internal",
        total_bytes=total_bytes,
        free_bytes=free_bytes,
        hard_reserve_eta=eta,
        now_utc_ns=now_utc_ns,
    )


def test_profile_constants_are_absolute_across_filesystem_sizes() -> None:
    assert (WARNING_BYTES, CRITICAL_BYTES, EMERGENCY_BYTES, HARD_RESERVE_BYTES) == (
        18 * 1024**3,
        14 * 1024**3,
        12 * 1024**3,
        10 * 1024**3,
    )
    for total in (20 * GIB, 40 * GIB, 1_000 * GIB):
        assert classify_absolute_state(
            VPS_PRODUCTION_V1, total_bytes=total, free_bytes=18 * GIB
        ) is VpsCapacityState.WARNING
        assert classify_absolute_state(
            VPS_PRODUCTION_V1, total_bytes=total, free_bytes=10 * GIB
        ) is VpsCapacityState.HARD_RESERVE


@pytest.mark.parametrize(
    ("free_gib", "expected"),
    [
        (19, VpsCapacityState.NORMAL),
        (18, VpsCapacityState.WARNING),
        (15, VpsCapacityState.WARNING),
        (14, VpsCapacityState.CRITICAL),
        (13, VpsCapacityState.CRITICAL),
        (12, VpsCapacityState.EMERGENCY),
        (11, VpsCapacityState.EMERGENCY),
        (10, VpsCapacityState.HARD_RESERVE),
        (9, VpsCapacityState.HARD_RESERVE),
    ],
)
def test_absolute_boundaries_are_inclusive(
    free_gib: int, expected: VpsCapacityState
) -> None:
    assert (
        classify_absolute_state(
            VPS_PRODUCTION_V1,
            total_bytes=40 * GIB,
            free_bytes=free_gib * GIB,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("offset_ns", "expected"),
    [
        (ETA_7D_NS - 1, VpsCapacityState.WARNING),
        (ETA_7D_NS, VpsCapacityState.WARNING),
        (ETA_7D_NS + 1, VpsCapacityState.NORMAL),
        (ETA_72H_NS - 1, VpsCapacityState.CRITICAL),
        (ETA_72H_NS, VpsCapacityState.CRITICAL),
        (ETA_72H_NS + 1, VpsCapacityState.WARNING),
        (ETA_24H_NS - 1, VpsCapacityState.EMERGENCY),
        (ETA_24H_NS, VpsCapacityState.EMERGENCY),
        (ETA_24H_NS + 1, VpsCapacityState.CRITICAL),
    ],
)
def test_eta_boundaries_use_integer_nanoseconds(
    offset_ns: int, expected: VpsCapacityState
) -> None:
    assert (
        classify_eta_state(
            {"status": "FORECAST", "utc_ns": offset_ns}, now_utc_ns=0
        )
        is expected
    )


def test_eta_uses_one_fixed_hard_reserve_target_and_sentinels() -> None:
    assert decision(
        free_bytes=13 * GIB,
        eta={"status": "FORECAST", "utc_ns": 12 * 3_600_000_000_000},
    ).state is VpsCapacityState.EMERGENCY
    assert decision(
        free_bytes=20 * GIB,
        eta={"status": "INSUFFICIENT_DATA"},
    ).state is VpsCapacityState.WARNING
    assert decision(
        free_bytes=20 * GIB,
        eta={"status": "NOT_APPROACHING"},
    ).state is VpsCapacityState.NORMAL
    assert decision(
        free_bytes=20 * GIB,
        eta={"status": "BEYOND_SUPPORTED_RANGE"},
    ).state is VpsCapacityState.NORMAL
    assert decision(
        free_bytes=20 * GIB,
        eta={"status": "REACHED", "utc_ns": 0},
    ).state is VpsCapacityState.HARD_RESERVE
    assert decision(free_bytes=10 * GIB, eta={"status": "NOT_APPROACHING"}).state is (
        VpsCapacityState.HARD_RESERVE
    )


def test_most_severe_state_wins_and_small_filesystems_do_not_scale() -> None:
    assert decision(
        free_bytes=17 * GIB,
        eta={"status": "FORECAST", "utc_ns": 48 * 3_600_000_000_000},
    ).state is VpsCapacityState.CRITICAL
    assert decision(
        free_bytes=13 * GIB,
        eta={"status": "FORECAST", "utc_ns": 12 * 3_600_000_000_000},
    ).state is VpsCapacityState.EMERGENCY
    assert decision(
        free_bytes=10 * GIB,
        eta={"status": "FORECAST", "utc_ns": 30 * 24 * 3_600_000_000_000},
    ).state is VpsCapacityState.HARD_RESERVE
    assert classify_absolute_state(
        VPS_PRODUCTION_V1, total_bytes=20 * GIB, free_bytes=15 * GIB
    ) is VpsCapacityState.WARNING


def test_profile_selection_is_explicit_and_internal_only() -> None:
    assert VPS_PRODUCTION_V1.profile_id == "vps-production-v1"
    assert VPS_PRODUCTION_V1.scope == "internal"
    with pytest.raises(ValueError, match="not valid"):
        evaluate_capacity(
            profile=VPS_PRODUCTION_V1,
            scope_id="external:archive-a",
            total_bytes=40 * GIB,
            free_bytes=20 * GIB,
            hard_reserve_eta={"status": "NOT_APPROACHING"},
            now_utc_ns=0,
        )


def test_malformed_eta_fails_explicitly() -> None:
    with pytest.raises(ValueError, match="unknown"):
        classify_eta_state({"status": "UNKNOWN"}, now_utc_ns=0)
    with pytest.raises(ValueError, match="malformed"):
        classify_eta_state({"status": "FORECAST"}, now_utc_ns=0)


def test_forecast_adds_vps_fields_without_redefining_m11_or_alert_authority(
    tmp_path: Path,
) -> None:
    total = 500 * GIB
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        for ordinal in range(169):
            forecaster.observe(
                scope_id="internal",
                storage_id=None,
                total_bytes=total,
                free_bytes=300 * GIB - ordinal * GIB,
                observed_at_utc_ns=ordinal * HOUR_NS,
            )
        generic_before = forecaster.forecast("internal", now_utc_ns=168 * HOUR_NS)
        vps = forecaster.forecast(
            "internal",
            now_utc_ns=168 * HOUR_NS,
            capacity_profile=VPS_PRODUCTION_V1,
        )
        generic_after = forecaster.forecast("internal", now_utc_ns=168 * HOUR_NS)
        events = catalog.storage_alert_events(scope_id="internal")

    assert vps["status"] == generic_before["status"] == generic_after["status"]
    assert vps["threshold_bytes"] == generic_before["threshold_bytes"]
    assert vps["eta"] == generic_before["eta"]
    assert vps["net_growth"] == generic_before["net_growth"]
    assert vps["capacity_profile"] == "vps-production-v1"
    assert vps["capacity_state"] == "WARNING"
    hard_eta = cast(dict[str, object], vps["hard_reserve_eta"])
    assert hard_eta["status"] == "FORECAST"
    assert abs(cast(int, hard_eta["utc_ns"]) - (168 + 122) * HOUR_NS) <= 1_000_000
    assert [event["to_severity"] for event in events] == ["OK", "WARNING"]
    assert all("capacity_profile" not in event for event in events)
    assert json.dumps(vps, allow_nan=False)


def test_shared_host_free_space_decline_is_selected_without_cause_attribution(
    tmp_path: Path,
) -> None:
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        forecaster = StorageForecaster(catalog=catalog, data_root=tmp_path)
        for ordinal in range(8):
            forecaster.observe(
                scope_id="internal",
                storage_id=None,
                total_bytes=100 * GIB,
                free_bytes=80 * GIB - ordinal * 2 * GIB,
                observed_at_utc_ns=ordinal * 24 * HOUR_NS,
            )
        result = forecaster.forecast("internal", now_utc_ns=7 * 24 * HOUR_NS)
    net = cast(dict[str, object], result["net_growth"])
    assert net["selected_bytes_per_second"] == round(2 * GIB / 86_400, 6)
    assert "cause" not in net
