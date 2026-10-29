from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from binance_market_data_recorder.binance.coverage_config import (
    FUTURES_INTERVALS,
    CoverageConfig,
)
from binance_market_data_recorder.binance.public_sources import (
    nominal_derivative_weight_per_minute,
    product_sources,
    source_plan,
    source_selection_report,
)
from binance_market_data_recorder.config import load_config
from tests.unit.test_p2_coverage import products


@pytest.mark.parametrize("intervals", [("1m",), FUTURES_INTERVALS])
def test_initial_selection_removes_only_dated_premium_and_reports_unknown(
    intervals: tuple[str, ...],
) -> None:
    full_config = CoverageConfig(underlying_assets=("BTC", "ETH"), futures_intervals=intervals)
    initial = full_config.model_copy(update={"capture_dated_premium_klines": False})
    full_sources = source_plan(products(), full_config)
    full = {s.key: s.public_dict() for s in full_sources}
    selected = source_plan(products(), initial)
    actual = {s.key: s.public_dict() for s in selected}
    omitted = {
        key: s
        for key, s in full.items()
        if s["market"] in {"um_delivery", "cm_delivery"}
        and str(s["endpoint"]).endswith("/premiumIndexKlines")
    }
    assert len(omitted) == 8 * len(intervals)
    assert actual == {key: s for key, s in full.items() if key not in omitted}
    report = source_selection_report(products(), initial, selected)
    assert report["full_target_source_count"] == len(full)
    assert report["selected_source_count"] == len(actual)
    assert report["uncaptured_source_count"] == len(omitted)
    assert report["selected_ws_streams"] == sum(s["transport"] == "ws" for s in full.values())
    omissions = report["uncaptured_sources"]
    assert isinstance(omissions, list)
    assert {(s["market"], s["symbol"], s["stream"]) for s in omissions} == {
        (s["market"], s["symbol"], s["stream"]) for s in omitted.values()
    }
    assert all(
        s["status"] == "OWNER_APPROVED_UNCAPTURED"
        and s["official_applicability"] == "UNKNOWN"
        and s["enabled"] is False
        and s["continuity_eligible"] is False
        for s in omissions
    )
    expected_weight = (
        sum(
            s.weight * 60 / s.cadence_seconds
            for s in selected
            if s.transport == "rest"
            and s.venue_market != "spot"
            and s.cadence_seconds > 0
            and s.lifecycle != "after_delivery"
        )
        + 120 / initial.directory_interval_seconds
    )
    assert report["nominal_steady_derivative_weight_per_minute"] == pytest.approx(expected_weight)
    assert nominal_derivative_weight_per_minute(selected, initial) == pytest.approx(
        nominal_derivative_weight_per_minute(source_plan(products(), full_config), full_config)
        - sum(60 / s.cadence_seconds for s in full_sources if s.key in omitted)
    )


def test_complete_target_remains_the_default_and_has_no_exception() -> None:
    config = CoverageConfig(underlying_assets=("BTC", "ETH"))
    plan = source_plan(products(), config)
    assert config.capture_dated_premium_klines is True
    assert (
        sum(
            s.endpoint.endswith("/premiumIndexKlines")
            and s.market in {"um_delivery", "cm_delivery"}
            for s in plan
        )
        == 120
    )
    report = source_selection_report(products(), config, plan)
    assert report["uncaptured_sources"] == [] and report["policy"] == "complete_target"
    assert report["full_target_source_count"] == report["selected_source_count"]


@pytest.mark.parametrize("market", ["um_delivery", "cm_delivery"])
def test_new_dated_products_for_ten_assets_use_identity_not_known_symbols(market: str) -> None:
    template = next(p for p in products() if p.key.market == market)
    assets = tuple(f"ASSET{i}" for i in range(10))
    config = CoverageConfig(underlying_assets=assets, capture_dated_premium_klines=False)
    for asset in assets:
        p = replace(
            template,
            key=replace(template.key, symbol=asset + "_281229"),
            base_asset=asset,
            pair=asset + "USD",
        )
        selected = product_sources(p, config)
        assert not any(s.endpoint.endswith("/premiumIndexKlines") for s in selected)
        assert all(
            any(s.stream == price + "_price_bars_" + interval for s in selected)
            for price in ("mark", "index")
            for interval in config.futures_intervals
        )


def test_initial_configuration_explicitly_selects_exception_and_preserves_d06(
    tmp_path: Path,
) -> None:
    root = Path(__file__).parents[2]
    legacy = load_config(
        config_file=root / "docs/milestones/P2/closeout-20261028/recorder.measured.toml",
        environ={},
        home=tmp_path,
    ).config.coverage
    initial = load_config(
        config_file=root / "docs/milestones/P2/initial-production-20261029/recorder.toml",
        environ={},
        home=tmp_path,
    ).config.coverage
    assert initial is not None and legacy is not None
    assert legacy.capture_dated_premium_klines and not initial.capture_dated_premium_klines
    assert initial.model_dump(exclude={"capture_dated_premium_klines"}) == legacy.model_dump(
        exclude={"capture_dated_premium_klines"}
    )
