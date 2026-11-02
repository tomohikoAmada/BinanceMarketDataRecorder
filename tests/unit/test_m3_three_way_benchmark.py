"""The long-run driver must produce native frames and detect Raw content loss."""

from __future__ import annotations

import json
from collections import Counter

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.products import discover_products
from binance_market_data_recorder.binance.public_websocket import envelope_for_group
from tools.benchmark_m1_m3_pipeline import (
    EPOCH_MS,
    build_fixture_frames,
    directory,
    fixture_groups,
)
from tools.benchmark_m3_three_way import Fingerprint, distribution, frame_at


def test_fingerprint_detects_missing_duplicate_and_modified_frames() -> None:
    first, reordered, missing, duplicated, modified = (Fingerprint() for _ in range(5))
    for value in (b"one", b"two", b"one"):
        first.add(value)
    for value in (b"two", b"one", b"one"):
        reordered.add(value)
    for value in (b"one", b"two"):
        missing.add(value)
    for value in (b"one", b"two", b"one", b"one"):
        duplicated.add(value)
    for value in (b"one", b"two", b"ONE"):
        modified.add(value)
    assert first.public() == reordered.public()
    assert all(first.public() != v.public() for v in (missing, duplicated, modified))
    assert distribution(Counter({1000: 50, 2000: 45, 10000: 5}))["p95"] == 2
    assert distribution(Counter())["count"] == 0


def test_all_native_sources_parse_and_klines_follow_expected_close_boundaries() -> None:
    cfg = CoverageConfig(underlying_assets=("BTC", "ETH"), kline_forming_policy="all")
    products = [
        p
        for venue in ("spot", "usdm", "coinm")
        for p in discover_products(venue, directory(venue), cfg)
        if p.status == "TRADING"
    ]
    feed, _ = build_fixture_frames(products, cfg, 4, 123)
    by_key = {(p.key.market, p.key.symbol): p for p in products}
    minute_bars = 0
    for group in fixture_groups(products, cfg):
        for template in feed[group.name][: len(group.wire_names)]:
            source = group.by_wire[json.loads(template)["stream"]]
            for tick in (0, 58, 59, 60, 2699):
                payload = frame_at(template, tick, 123)
                envelope = envelope_for_group(
                    group,
                    by_key.get((source.market, source.symbol)),
                    raw_payload=payload,
                    connection_id="test",
                    collector_instance_id="test",
                    collector_version="test",
                    receive_time_utc_ns=1,
                    receive_monotonic_ns=1,
                )
                assert "malformed" not in envelope.capture_flags
                data = json.loads(payload)["data"]
                if isinstance(data, dict) and "k" in data and data["k"]["i"] == "1m":
                    minute_bars += 1
                    assert data["k"]["x"] == ((tick + 1) % 60 == 0)
                    if tick == 59:
                        assert data["k"]["t"] == EPOCH_MS
                        assert data["k"]["T"] == EPOCH_MS + 59999
                    elif tick == 60:
                        assert data["k"]["t"] == EPOCH_MS + 60000
    assert minute_bars > 0
