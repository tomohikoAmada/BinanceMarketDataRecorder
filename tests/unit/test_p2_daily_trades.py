from __future__ import annotations

import io
import zipfile
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from binance_market_data_recorder.backfill import HistoricalImporter
from binance_market_data_recorder.backfill.daily_trades import (
    build_daily_trade_plan,
    iter_daily_trade_facts,
    reconcile_trade_facts,
)
from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.domain.event import Market
from binance_market_data_recorder.domain.product import ProductKey
from tests.unit.test_backfill import FixtureOpener
from tests.unit.test_p2_coverage import SMALL, products


def source(market: Market = "um_delivery") -> tuple[Any, bytes]:
    key = ProductKey(market, "BTCUSDT_250328" if market == "um_delivery" else "BTCUSD_PERP")
    entry = build_daily_trade_plan([key], date(2025, 1, 1), date(2025, 1, 1)).entries[0]
    output = io.BytesIO()
    amount = "base_qty" if market.startswith("cm_") else "quote_qty"
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            entry.zip_url.rsplit("/", 1)[1].replace(".zip", ".csv"),
            f"id,price,qty,{amount},time,is_buyer_maker\n"
            "1,12345.0000000000000001,1.0E-4,1.23450000000000001,1735689600000,true\n"
            "2,12345.0,2.00,24690.0,1735689600001,false\n",
        )
    return entry, output.getvalue()


@pytest.mark.parametrize("market", ["um_delivery", "cm_perpetual"])
def test_daily_reader_keeps_precision_acquisition_and_missing_fields(
    tmp_path: Path,
    market: Market,
) -> None:
    entry, body = source(market)
    digest = sha256(body).hexdigest()
    name = entry.zip_url.rsplit("/", 1)[1]
    opener = FixtureOpener(
        {entry.zip_url: body, entry.checksum_url: f"{digest}  {name}\n".encode()}
    )
    importer = HistoricalImporter(data_root=tmp_path, opener=opener, normalization_batch_rows=1)
    result = importer.import_entry(entry)
    assert result.normalized_path is not None
    facts = list(iter_daily_trade_facts(Path(result.normalized_path)))
    assert facts[0]["price"] == "12345.0000000000000001"
    assert facts[0]["qty"] == "1.0E-4"
    assert facts[0]["live_receive_time_utc_ns"] is None
    assert facts[0]["is_rpi_trade_available"] is False
    assert int(facts[0]["downloaded_at_utc_ns"]) > facts[0]["archive_event_time_utc_ns"]
    assert facts[0]["market"] == market and facts[0]["trade_id"] == 1
    assert facts[0]["quote_qty" if market.startswith("cm_") else "base_qty"] is None
    acquired = facts[0]["downloaded_at_utc_ns"]
    again = importer.import_entry(entry)
    assert again.normalized_path is not None
    assert again.status == "ALREADY_VERIFIED"
    assert (
        next(iter_daily_trade_facts(Path(again.normalized_path)))["downloaded_at_utc_ns"]
        == acquired
    )
    assert importer.verify()["verified_normalized_rows"] == 2
    live = {**facts[0], "price": "12345.00000000000000010", "isRPITrade": True}
    matched = reconcile_trade_facts(live, facts[0])
    assert matched["fact_count"] == 1 and not matched["live_gap_repaired"]
    assert matched["preferred_live_fact"]["isRPITrade"] is True
    conflict = reconcile_trade_facts({**live, "price": "9"}, facts[0])
    assert conflict["status"] == "CONFLICT" and conflict["preferred_live_fact"] is None
    assert len(conflict["conflicting_sources"]) == 2


def test_daily_plan_is_explicit_bounded_and_has_no_default_symbol() -> None:
    with pytest.raises(ValueError):
        build_daily_trade_plan([], date(2025, 1, 1), date(2025, 1, 1))
    keys = [ProductKey("um_perpetual", f"COIN{i}USDT") for i in range(10)]
    p = build_daily_trade_plan(keys, date(2025, 1, 1), date(2025, 1, 2))
    assert len(p.entries) == 20 and all("BTCUSDT" not in e.zip_url for e in p.entries)
    assert all(e.granularity == "daily" and e.timestamp_unit == "milliseconds" for e in p.entries)
    with pytest.raises(ValueError):
        build_daily_trade_plan(keys, date(2025, 1, 1), date(2025, 2, 1))
    with pytest.raises(ValueError):
        build_daily_trade_plan(
            [ProductKey("um_perpetual", "../BTC")], date(2025, 1, 1), date(2025, 1, 1)
        )


def test_unknown_daily_fields_fail_and_keep_verified_source(tmp_path: Path) -> None:
    entry, _ = source()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            entry.zip_url.rsplit("/", 1)[1].replace(".zip", ".csv"),
            "id,price,qty,quote_qty,time,is_buyer_maker,unknown\n1,2,3,6,1735689600000,true,9\n",
        )
    body = output.getvalue()
    digest = sha256(body).hexdigest()
    name = entry.zip_url.rsplit("/", 1)[1]
    importer = HistoricalImporter(
        data_root=tmp_path,
        opener=FixtureOpener(
            {entry.zip_url: body, entry.checksum_url: f"{digest}  {name}\n".encode()}
        ),
    )
    with pytest.raises(ValueError):
        importer.import_entry(entry)
    assert list(importer.sources.glob("*/*.zip"))
    assert not list(importer.normalized.glob("*.parquet"))


def test_missing_daily_file_is_an_explicit_gap(tmp_path: Path) -> None:
    entry, _ = source()
    importer = HistoricalImporter(data_root=tmp_path, opener=FixtureOpener({}))
    gap = importer.import_entry(entry).gap
    assert gap is not None and gap["reason"] == "CHECKSUM_NOT_FOUND"


def test_fixed_cadences_are_product_specific_and_invalid_values_rejected() -> None:
    ps = products()
    p = next(p for p in ps if p.key.market == "um_perpetual")
    c = SMALL.model_copy(
        update={"recent_trades_intervals": {f"{p.key.market}:{p.key.symbol}": 3.0}}
    )
    assert next(s for s in product_sources(p, c) if s.family == "D06").cadence_seconds == 3
    with pytest.raises(ValidationError):
        CoverageConfig(
            underlying_assets=("BTC",),
            recent_trades_intervals={"um_perpetual:BTCUSDT": float("nan")},
        )
    with pytest.raises(ValidationError):
        CoverageConfig(
            underlying_assets=("BTC",), recent_trades_intervals={"cm_shared:BTCUSD": 3.0}
        )


def test_selected_daily_cli_plans_only_explicit_physical_products(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json

    from binance_market_data_recorder.cli import main

    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    args = [
        "backfill",
        "plan",
        "--profile",
        "selected-daily-trades",
        "--start",
        "2025-01-01",
        "--end",
        "2025-01-01",
    ]
    assert main([*args, "--product", "cm_delivery:BTCUSD_250328"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["file_count"] == 1
    assert result["entries"][0]["market"] == "cm_delivery"
    assert "/futures/cm/daily/trades/BTCUSD_250328/" in result["entries"][0]["zip_url"]
    assert main(args) == 2
    assert "distinct explicit products" in capsys.readouterr().err
