from __future__ import annotations

import base64
import json
from dataclasses import replace
from pathlib import Path
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.products import ProductDirectory, discover_products
from binance_market_data_recorder.binance.public_rest import capture_public_response
from binance_market_data_recorder.binance.public_sources import Source, product_sources, source_plan
from binance_market_data_recorder.binance.public_websocket import (
    DepthSequenceObserver,
    envelope_for_group,
    logical_envelope,
    stream_groups,
)
from binance_market_data_recorder.binance.recent_trades import RecentTradesCompactor
from binance_market_data_recorder.binance.rest_representation import response_document
from binance_market_data_recorder.config import RecorderConfig, load_config
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.domain.product import Product
from binance_market_data_recorder.normalize.parser import parse_envelope
from binance_market_data_recorder.orderbook.parser import depth_update_from_envelope
from binance_market_data_recorder.spool.format import decode_envelope, encode_envelope

FIXTURES = Path(__file__).parents[1] / "fixtures/binance/p2"
SMALL = CoverageConfig(
    underlying_assets=("BTC", "ETH"), spot_intervals=("1m",), futures_intervals=("1m",)
)


def directory(venue: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{venue}-directory.json").read_bytes())  # type: ignore[no-any-return]


def products() -> tuple[Product, ...]:
    return tuple(
        p
        for v in ("spot", "usdm", "coinm")
        for p in discover_products(v, directory(v), SMALL)
        if p.status == "TRADING"
    )


class Response:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "12", "Set-Cookie": "exclude"}

    def __init__(self, model: Any) -> None:
        self.model = model

    def data(self) -> Any:
        return self.model


def test_selected_base_discovers_non_usdt_and_all_contract_identities() -> None:
    ps = products()
    assert {p.key.market for p in ps} == {
        "spot",
        "um_perpetual",
        "um_delivery",
        "cm_perpetual",
        "cm_delivery",
    }
    assert any(p.key.symbol == "ETHBTC" and p.key.market == "spot" for p in ps)
    dated = next(p for p in ps if p.key.symbol == "BTCUSD_261225")
    assert dated.pair == "BTCUSD" and dated.symbol_type == 2
    assert dated.contract_size == "100" and dated.margin_asset == "BTC"
    assert dated.delivery_time_ms is not None and not dated.perpetual
    assert not any(p.base_asset not in {"BTC", "ETH"} for p in ps)


def test_missing_directory_is_unknown_not_silent_delisting() -> None:
    d = ProductDirectory(SMALL)
    d.apply("coinm", directory("coinm"))
    d.apply("coinm", {"symbols": [], "rateLimits": []})
    assert d.missing and all(p.status == "MISSING_FROM_DIRECTORY" for p in d.products.values())
    assert not d.not_offered()  # Previously observed identities still exist.


def test_missing_contract_is_explicit_and_failed_directory_is_not_not_offered() -> None:
    d = ProductDirectory(SMALL)
    assert d.not_offered() == []
    d.apply("coinm", {"symbols": [], "rateLimits": []})
    assert len(d.not_offered()) == 4
    assert all(x["status"] == "EXCHANGE_NOT_OFFERED" for x in d.not_offered())


def test_ten_asset_design_and_shared_sources_do_not_duplicate() -> None:
    template = next(p for p in products() if p.key.market == "cm_perpetual")
    assets = tuple(f"ASSET{i}" for i in range(10))
    c = SMALL.model_copy(update={"underlying_assets": assets})
    ps = tuple(
        replace(
            template, key=replace(template.key, symbol=a + "USD_PERP"), base_asset=a, pair=a + "USD"
        )
        for a in assets
    )
    plan = source_plan(ps, c)
    assert len(plan) == len({s.key for s in plan})
    assert sum(s.family == "D27" for s in plan) == 1
    assert sum(s.family == "D01" for s in plan) == 1
    assert sum(s.family == "D13" for s in plan) == 1
    assert len({s.symbol for s in plan if s.family == "D06"}) == 10
    assert all(
        "usdm_weight" in s.charges
        for s in plan
        if s.transport == "rest" and s.venue_market == "coinm"
    )


def test_shared_pair_scope_and_funding_are_not_mechanically_substituted() -> None:
    plan = source_plan(products(), SMALL)
    assert not any(s.family in {"D18", "D19"} and s.venue_market == "usdm" for s in plan)
    assert any(s.family == "D18" and s.venue_market == "coinm" for s in plan)
    assert not any(s.family == "D12" and s.market.endswith("delivery") for s in plan)
    assert all(
        dict(s.parameters)["pair"] == s.symbol
        for s in plan
        if s.family == "D17" and s.venue_market == "coinm"
    )
    assert not any(s.family == "D03" and s.venue_market == "coinm" for s in plan)


def test_full_intervals_and_timezone_not_silently_reduced() -> None:
    c = CoverageConfig(underlying_assets=("BTC",))
    spot = next(p for p in products() if p.key.market == "spot")
    plan = product_sources(spot, c)
    for interval in c.spot_intervals:
        assert any(s.stream == "kline_" + interval for s in plan)
        assert any(s.stream == "kline_" + interval + "_utc8" for s in plan)
    assert len(stream_groups(tuple(s for s in plan if s.transport == "ws"), 4)) > 1


def test_config_discovery_is_explicit_and_loads_nested_toml(tmp_path: Path) -> None:
    config = tmp_path / "recorder.toml"
    config.write_text('[recorder]\n[recorder.coverage]\nunderlying_assets=["btc","eth"]\n')
    loaded = load_config(config_file=config, environ={}, home=tmp_path)
    assert loaded.config.coverage is not None
    assert loaded.config.spot_symbols == loaded.config.usdm_symbols == ()
    assert loaded.config.coverage.underlying_assets == ("BTC", "ETH")
    with pytest.raises(ValidationError):
        RecorderConfig(data_root=tmp_path, spot_symbols=("BTCUSDT",), coverage=SMALL)
    with pytest.raises(ValidationError):
        CoverageConfig(underlying_assets=("BTC",), futures_intervals=("1s",))


@pytest.mark.parametrize("market", ["um_perpetual", "um_delivery", "cm_perpetual", "cm_delivery"])
def test_combined_exact_bytes_and_dated_pair_sequence_reading(market: str) -> None:
    p = next(p for p in products() if p.key.market == market)
    source = next(s for s in product_sources(p, SMALL) if s.stream == "diff_depth")
    group = stream_groups((source,), 128)[0]
    payload = {
        "e": "depthUpdate",
        "s": p.key.symbol,
        "ps": p.pair,
        "st": p.symbol_type,
        "E": 1000,
        "T": 999,
        "U": 41,
        "u": 42,
        "pu": 40,
        "b": [["123.000000000000000001", "3.00000001"]],
        "a": [],
    }
    raw = json.dumps({"stream": group.wire_names[0], "data": payload}, indent=2).encode()
    envelope = envelope_for_group(
        group,
        p,
        raw_payload=raw,
        connection_id="c",
        collector_instance_id="i",
        collector_version="v",
        receive_time_utc_ns=1001,
        receive_monotonic_ns=2001,
    )
    assert not envelope.capture_flags and envelope.raw_payload == raw
    restored = decode_envelope(encode_envelope(envelope))
    logical = logical_envelope(restored)
    depth = depth_update_from_envelope(logical)
    assert depth.previous_final_update_id == 40 and depth.symbol == p.key.symbol
    assert restored.raw_payload == raw
    normalized = parse_envelope(restored)[0]
    assert normalized.valid and normalized.fields["source_stream"] == "diff_depth"
    payload["st"] = 3
    bad = envelope_for_group(
        group,
        p,
        raw_payload=json.dumps({"stream": group.wire_names[0], "data": payload}).encode(),
        connection_id="c",
        collector_instance_id="i",
        collector_version="v",
        receive_time_utc_ns=1001,
        receive_monotonic_ns=2001,
    )
    assert "malformed" in bad.capture_flags


def test_rest_single_representation_preserves_null_missing_and_precision() -> None:
    source = Source(
        "S07",
        "spot",
        "spot",
        "BTCUSDT",
        "reference_price",
        "rest",
        "/api/v3/referencePrice",
        60,
        required_fields=("symbol", "referencePrice", "timestamp"),
    )
    model = {
        "symbol": "BTCUSDT",
        "referencePrice": None,
        "timestamp": 123,
        "extra": "0.00000000000000000001",
    }
    captured = capture_public_response(
        source, lambda s, p: Response(model), collector_instance_id="i", collector_version="v"
    )
    assert captured.valid
    body = json.loads(captured.envelope.raw_payload)
    assert response_document(body) == model
    assert "raw_body_base64" not in body["response"]
    assert body["response"]["headers"] == {"x-mbx-used-weight-1m": "12"}
    del model["referencePrice"]
    missing = capture_public_response(
        source, lambda s, p: Response(model), collector_instance_id="i", collector_version="v"
    )
    assert not missing.valid and "referencePrice" not in response_document(
        json.loads(missing.envelope.raw_payload)
    )


def test_legacy_snapshot_representations_still_read() -> None:
    model = {"lastUpdateId": 5, "bids": [["1.0000", "2"]], "asks": []}
    body = base64.b64encode(json.dumps(model).encode()).decode()
    for response in (
        {"model": model},
        {"model": model, "raw_body_base64": body},
        {"raw_body_base64": body},
    ):
        assert response_document({"response": response}) == model


def trade(i: int, *, price: str = "1.00000000000000001") -> dict[str, Any]:
    return {
        "id": i,
        "price": price,
        "qty": "2.0000",
        "time": i + 100,
        "quoteQty": "2.00000000000000002",
        "isBuyerMaker": True,
    }


def test_recent_overlap_late_id_changes_empty_gap_and_bounded_restart() -> None:
    c = RecentTradesCompactor(1000)
    assert len(c.observe([trade(10), trade(11)], 1)["changes"]) == 2
    assert c.observe([trade(10), trade(11)], 2)["changes"] == []
    changed = c.observe([trade(3), trade(10, price="9")], 3)["changes"]
    assert len(changed) == 2 and changed[1]["same_id_changed"]
    assert changed[1]["first_observed_utc_ns"] == 1
    assert c.observe([], 4)["poll"]["empty"]
    assert c.observe([trade(20)], 5)["poll"]["window_gap"] == {
        "first_unobserved_id": 12,
        "last_unobserved_id": 19,
        "reason": "recent_window_did_not_overlap",
    }
    c.observe([trade(i) for i in range(1000, 3000)], 6)
    assert len(c.rows) == 1000
    assert RecentTradesCompactor().observe([trade(20)], 7)["poll"]["initial_window"]


def test_real_public_ws_samples_preserve_payload_and_required_information() -> None:
    samples = json.loads((FIXTURES / "ws-public-samples.json").read_bytes())
    seen = set()
    for sample in samples:
        p = next(
            p
            for p in products()
            if p.key.market == sample["market"] and p.key.symbol == sample["symbol"]
        )
        groups = stream_groups(product_sources(p, SMALL), 128)
        group = next(g for g in groups if set(g.wire_names) == set(sample["requested_streams"]))
        for observation in sample["observations"]:
            raw = base64.b64decode(observation["raw_base64"], validate=True)
            envelope = envelope_for_group(
                group,
                p,
                raw_payload=raw,
                connection_id="real-sample",
                collector_instance_id="i",
                collector_version="v",
                receive_time_utc_ns=1000,
                receive_monotonic_ns=2000,
            )
            assert "malformed" not in envelope.capture_flags, (p.key, observation["wire_stream"])
            assert decode_envelope(encode_envelope(envelope)).raw_payload == raw
            seen.add(envelope.source_sequence["sourceFamily"])
    assert seen >= {
        "S02",
        "S03",
        "S04",
        "S05",
        "S07",
        "S10",
        "S11",
        "S12",
        "S13",
        "S14",
        "D02",
        "D03",
        "D05",
        "D08",
        "D09",
        "D21",
        "D22",
        "D23",
        "D24",
    }


def test_real_rest_samples_roundtrip_all_supplied_values_and_errors() -> None:
    count = 0
    for path in FIXTURES.glob("*.json"):
        sample = json.loads(path.read_bytes())
        if not isinstance(sample, dict) or "observation" not in sample:
            continue
        record = sample["observation"]
        assert record["representation"] in {"sdk-parsed-json.v1", "http-body-base64.v1"}
        assert ("model" in record["response"]) != ("raw_body_base64" in record["response"])
        expected = response_document(record)
        assert response_document(json.loads(json.dumps(record))) == expected
        assert record["response"]["receive_time_utc_ns"] >= record["request"]["request_time_utc_ns"]
        if not record["response"]["valid"]:
            assert record["response"]["error_type"] is not None
        count += 1
    assert count >= 40


def test_official_sparse_stream_examples_preserve_venue_payloads() -> None:
    from binance_market_data_recorder.binance.public_websocket import StreamGroup

    for example in json.loads((FIXTURES / "official-sparse-ws.json").read_bytes()):
        source = Source(
            example["family"],
            example["venue"],
            "spot" if example["venue"] == "spot" else "cm_perpetual",
            example["symbol"],
            example["stream"],
            "ws",
            "wss://dstream.binance.com/" + example["wire"],
            1,
        )
        group = StreamGroup("wss://dstream.binance.com", "", "ws_market_fixture", (source,))
        raw = json.dumps({"stream": example["wire"], "data": example["payload"]}).encode()
        envelope = envelope_for_group(
            group,
            None,
            raw_payload=raw,
            connection_id="official",
            collector_instance_id="i",
            collector_version="v",
            receive_time_utc_ns=1000,
            receive_monotonic_ns=2000,
        )
        assert not envelope.capture_flags and envelope.raw_payload == raw
        assert envelope.source_sequence["sourceFamily"] == example["family"]


def test_numeric_price_body_representation_preserves_all_decimal_digits() -> None:
    from binance_market_data_recorder.binance.public_rest import _ObservedResponse

    raw = b'[{"deliveryTime":123,"deliveryPrice":1234567890123.0000000000000123}]'
    source = Source(
        "D31",
        "usdm",
        "um_delivery",
        "BTCUSDT",
        "delivery_price",
        "rest",
        "/futures/data/delivery-price",
        60,
        required_fields=("deliveryTime", "deliveryPrice"),
    )
    response = _ObservedResponse(
        200, {}, [{"deliveryTime": 123, "deliveryPrice": 1.0}], 1000, 2000, raw
    )
    captured = capture_public_response(
        source,
        lambda s, p: response,
        collector_instance_id="i",
        collector_version="v",
        utc_clock_ns=lambda: 1,
        monotonic_clock_ns=lambda: 2,
    )
    record = json.loads(captured.envelope.raw_payload)
    assert "model" not in record["response"]
    assert base64.b64decode(record["response"]["raw_body_base64"]) == raw
    assert response_document(record)[0]["deliveryPrice"] == "1234567890123.0000000000000123"
    assert captured.envelope.receive_time_utc_ns == 1000
    assert "exact_rest_http_body_preserved" in captured.envelope.capture_flags


def test_ten_assets_cover_all_five_product_kinds_without_fixed_symbols() -> None:
    templates = [
        next(p for p in products() if p.key.market == kind)
        for kind in ("spot", "um_perpetual", "um_delivery", "cm_perpetual", "cm_delivery")
    ]
    expanded = tuple(
        replace(
            p,
            key=replace(p.key, symbol=f"COIN{i}_{p.key.market}"),
            base_asset=f"COIN{i}",
            pair=f"COIN{i}_{p.pair}",
        )
        for i in range(10)
        for p in templates
    )
    plan = source_plan(
        expanded,
        SMALL.model_copy(update={"underlying_assets": tuple(f"COIN{i}" for i in range(10))}),
    )
    assert len(expanded) == 50 and len(plan) == len({s.key for s in plan})
    assert len({s.symbol for s in plan if s.family == "D06"}) == 40
    assert all(s.market != "um_perpetual" for s in plan if s.symbol == "__GLOBAL__")


def test_native_price_bar_queries_follow_close_period_instead_of_polling_all_each_minute() -> None:
    p = next(p for p in products() if p.key.market == "um_perpetual")
    c = SMALL.model_copy(update={"futures_intervals": ("1m", "1h", "1M")})
    plan = product_sources(p, c)
    periods = {
        dict(s.parameters)["interval"]: s.cadence_seconds
        for s in plan
        if s.family == "D23" and s.transport == "rest"
    }
    assert periods == {"1m": 60, "1h": 3600, "1M": 31 * 86400}
    assert all(
        s.lifecycle == "closed_bar" for s in plan if s.family == "D23" and s.transport == "rest"
    )


@pytest.mark.parametrize(
    "market,stream",
    [
        ("spot", "diff_depth"),
        ("um_perpetual", "diff_depth"),
        ("um_delivery", "diff_depth"),
        ("cm_perpetual", "diff_depth"),
        ("cm_delivery", "diff_depth"),
        ("um_perpetual", "rpi_depth"),
    ],
)
def test_native_sequence_gaps_are_marked_without_rewriting_bytes_or_rewinding_on_duplicates(
    market: str,
    stream: str,
) -> None:
    p = next(p for p in products() if p.key.market == market)
    source = next(s for s in product_sources(p, SMALL) if s.stream == stream)
    group = stream_groups((source,), 128)[0]
    observer = DepthSequenceObserver()

    def event(first: int, final: int, previous: int, connection: str = "c") -> EventEnvelope:
        raw = json.dumps(
            {
                "stream": group.wire_names[0],
                "data": {
                    "s": p.key.symbol,
                    "ps": p.pair,
                    "st": p.symbol_type,
                    "e": "depthUpdate",
                    "E": 1000,
                    "T": 999,
                    "U": first,
                    "u": final,
                    "pu": previous,
                    "b": [],
                    "a": [],
                },
            }
        ).encode()
        value = envelope_for_group(
            group,
            p,
            raw_payload=raw,
            connection_id=connection,
            collector_instance_id="i",
            collector_version="v",
            receive_time_utc_ns=1001,
            receive_monotonic_ns=2001,
        )
        assert "malformed" not in value.capture_flags
        assert value.exchange_transaction_time == 999
        return value

    assert not observer.observe(event(101, 102, 100)).capture_flags
    assert not observer.observe(event(95, 99, 94)).capture_flags  # stale, cannot rewind
    assert not observer.observe(event(103, 104, 102)).capture_flags
    gap = event(110, 112, 109)
    marked = observer.observe(gap)
    assert marked.capture_flags == ("sequence_gap",)
    assert marked.raw_payload == gap.raw_payload
    assert not observer.observe(event(501, 502, 500, "new-connection")).capture_flags
    malformed = event(503, 504, 502, "new-connection").model_copy(
        update={"capture_flags": ("malformed",)}
    )
    observer.observe(malformed)
    assert not observer.observe(event(600, 601, 599, "new-connection")).capture_flags


def test_compact_poll_window_gap_marks_the_raw_interval_incomplete() -> None:
    p = next(p for p in products() if p.key.market == "cm_perpetual")
    source = next(s for s in product_sources(p, SMALL) if s.family == "D06")
    compactor = RecentTradesCompactor()
    rows = [[trade(1), trade(2)], [trade(8), trade(9)]]
    captured = []
    for row in rows:

        def request(
            source: Source,
            params: dict[str, Any],
            data: list[dict[str, Any]] = row,
        ) -> Response:
            return Response(data)

        captured.append(
            capture_public_response(
                source,
                request,
                collector_instance_id="i",
                collector_version="v",
                recent_trades=compactor,
            )
        )
    assert "sequence_gap" not in captured[0].envelope.capture_flags
    assert "sequence_gap" in captured[1].envelope.capture_flags
    assert captured[1].valid
