from __future__ import annotations

import json

import pytest

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize.model import schema_for, stream_fields
from binance_market_data_recorder.normalize.parser import parse_envelope
from tests.normalization_support import envelope, provenance

FIVE_MINUTE_MODELS: dict[str, list[dict[str, object]]] = {
    "open_interest_statistics_5m": [
        {
            "symbol": "BTCUSDT",
            "sumOpenInterest": "123.45678901",
            "sumOpenInterestValue": "9876543.21000000",
            "timestamp": 300_000,
        },
        {
            "symbol": "BTCUSDT",
            "sumOpenInterest": "124.00000000",
            "sumOpenInterestValue": "9876550.00000000",
            "timestamp": 600_000,
        },
    ],
    "taker_buy_sell_volume_5m": [
        {
            "buySellRatio": "1.23456789",
            "buyVol": "12.34567890",
            "sellVol": "10.00000000",
            "timestamp": 300_000,
        }
    ],
    "global_long_short_ratio_5m": [
        {
            "symbol": "BTCUSDT",
            "longShortRatio": "1.10000000",
            "longAccount": "0.52380952",
            "shortAccount": "0.47619048",
            "timestamp": 300_000,
        }
    ],
    "top_long_short_account_ratio_5m": [
        {
            "symbol": "BTCUSDT",
            "longShortRatio": "1.20000000",
            "longAccount": "0.54545455",
            "shortAccount": "0.45454545",
            "timestamp": 300_000,
        }
    ],
    "top_long_short_position_ratio_5m": [
        {
            "symbol": "BTCUSDT",
            "longShortRatio": "1.30000000",
            "longAccount": "0.56521739",
            "shortAccount": "0.43478261",
            "timestamp": 300_000,
        }
    ],
    "basis_5m": [
        {
            "pair": "BTCUSDT",
            "contractType": "PERPETUAL",
            "indexPrice": "65000.12345678",
            "futuresPrice": "65010.87654321",
            "basis": "10.75308643",
            "basisRate": "0.00016543",
            "annualizedBasisRate": "",
            "timestamp": 300_000,
        }
    ],
}

EXPECTED_FIVE_MINUTE_FIELDS = {
    "open_interest_statistics_5m": (
        "observation_empty",
        "timestamp_ms",
        "sum_open_interest",
        "sum_open_interest_value",
    ),
    "taker_buy_sell_volume_5m": (
        "observation_empty",
        "timestamp_ms",
        "buy_sell_ratio",
        "buy_volume",
        "sell_volume",
    ),
    "global_long_short_ratio_5m": (
        "observation_empty",
        "timestamp_ms",
        "long_short_ratio",
        "long_account",
        "short_account",
    ),
    "top_long_short_account_ratio_5m": (
        "observation_empty",
        "timestamp_ms",
        "long_short_ratio",
        "long_account",
        "short_account",
    ),
    "top_long_short_position_ratio_5m": (
        "observation_empty",
        "timestamp_ms",
        "long_short_ratio",
        "long_account",
        "short_account",
    ),
    "basis_5m": (
        "observation_empty",
        "timestamp_ms",
        "pair",
        "contract_type",
        "index_price",
        "futures_price",
        "basis",
        "basis_rate",
        "annualized_basis_rate",
    ),
}


def _side_envelope(
    stream: str,
    model: object,
    ordinal: int = 1,
    *,
    start: int = 300_000,
    end: int = 899_999,
) -> EventEnvelope:
    return envelope(
        market="um_perpetual",
        stream=stream,
        raw_payload=provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=model,
            path=f"/futures/data/{stream}",
            kind=stream,
            parameters={
                "symbol": "BTCUSDT",
                "period": "5m",
                "startTime": start,
                "endTime": end,
            },
        ),
        ordinal=ordinal,
        module="binance.usdm.side_rest.v1",
    )


def _spot_exchange_info() -> EventEnvelope:
    model = {
        "timezone": "UTC",
        "serverTime": 1_725_000_000_000,
        "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "limit": 6000}],
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "orderTypes": ["LIMIT", "MARKET"],
                "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.01000000"}],
                "permissions": ["SPOT"],
                "permissionSets": [["SPOT"]],
            }
        ],
    }
    return envelope(
        market="spot",
        stream="exchange_info",
        raw_payload=provenance(
            schema_version="binance-spot-exchange-info-provenance.v1",
            model=model,
            path="/api/v3/exchangeInfo",
        ),
        ordinal=1,
        module="binance.spot.rest.exchange_info.v1",
    )


def test_spot_exchange_info_uses_spot_specific_schema_and_parser() -> None:
    parsed = parse_envelope(_spot_exchange_info())
    assert len(parsed) == 1
    assert parsed[0].valid
    assert parsed[0].fields == {
        "symbol_present": True,
        "server_time_ms": 1_725_000_000_000,
        "trading_status": "TRADING",
        "filters_json": '[{"filterType":"PRICE_FILTER","tickSize":"0.01000000"}]',
        "order_types_json": '["LIMIT","MARKET"]',
        "rate_limits_json": '[{"limit":6000,"rateLimitType":"REQUEST_WEIGHT"}]',
        "permissions_json": '["SPOT"]',
        "permission_sets_json": '[["SPOT"]]',
        "response_model_sha256": parsed[0].logical_content[
            "response_model_sha256"
        ],
    }
    spot_names = schema_for("spot", "exchange_info").names
    usdm_names = schema_for("um_perpetual", "exchange_info").names
    assert "order_types_json" in spot_names
    assert "contract_type" not in spot_names
    assert "contract_type" in usdm_names
    assert "order_types_json" not in usdm_names


def test_m19_stream_field_contracts_are_explicit_and_market_compatible() -> None:
    assert tuple(
        name for name, _type, _nullable in stream_fields("spot", "exchange_info")
    ) == (
        "symbol_present",
        "server_time_ms",
        "trading_status",
        "filters_json",
        "order_types_json",
        "rate_limits_json",
        "permissions_json",
        "permission_sets_json",
        "response_model_sha256",
    )
    assert tuple(
        name
        for name, _type, _nullable in stream_fields(
            "um_perpetual", "exchange_info"
        )
    ) == (
        "symbol_present",
        "server_time_ms",
        "contract_type",
        "trading_status",
        "filters_json",
        "rate_limits_json",
    )
    for stream, expected in EXPECTED_FIVE_MINUTE_FIELDS.items():
        assert tuple(
            name
            for name, _type, _nullable in stream_fields("um_perpetual", stream)
        ) == expected


@pytest.mark.parametrize("stream", sorted(FIVE_MINUTE_MODELS))
def test_each_five_minute_model_expands_to_timestamp_identified_rows(
    stream: str,
) -> None:
    parsed = parse_envelope(_side_envelope(stream, FIVE_MINUTE_MODELS[stream]))
    assert len(parsed) == len(FIVE_MINUTE_MODELS[stream])
    assert all(row.valid for row in parsed)
    assert [row.fields["timestamp_ms"] for row in parsed] == [
        item["timestamp"] for item in FIVE_MINUTE_MODELS[stream]
    ]
    assert all(
        row.semantic_identity
        == {
            "kind": stream,
            "market": "um_perpetual",
            "symbol": "BTCUSDT",
            "timestamp_ms": row.fields["timestamp_ms"],
        }
        for row in parsed
    )
    assert all(
        "receive_time_utc_ns" not in row.semantic_identity
        and "connection_id" not in row.semantic_identity
        for row in parsed
    )
    assert all(
        isinstance(value, str)
        for row in parsed
        for name, value in row.fields.items()
        if name not in {"observation_empty", "timestamp_ms", "pair"}
        and value is not None
    )


def test_five_minute_empty_response_has_request_range_identity_without_timestamp() -> None:
    parsed = parse_envelope(
        _side_envelope(
            "open_interest_statistics_5m",
            [],
            start=300_000,
            end=599_999,
        )
    )
    assert len(parsed) == 1
    row = parsed[0]
    assert row.valid
    assert row.event_kind == "empty_observation"
    assert row.fields["observation_empty"] is True
    assert row.fields["timestamp_ms"] is None
    assert row.semantic_identity["requested_start_ms"] == 300_000
    assert row.semantic_identity["requested_end_ms"] == 599_999
    assert row.semantic_identity["model_sha256"] == (
        __import__("hashlib").sha256(b"[]").hexdigest()
    )


def test_same_period_identity_ignores_rest_attempt_but_content_can_conflict() -> None:
    first = parse_envelope(
        _side_envelope(
            "taker_buy_sell_volume_5m",
            FIVE_MINUTE_MODELS["taker_buy_sell_volume_5m"],
            ordinal=1,
        )
    )[0]
    changed = json.loads(
        json.dumps(FIVE_MINUTE_MODELS["taker_buy_sell_volume_5m"])
    )
    changed[0]["buyVol"] = "99.00000000"
    second = parse_envelope(
        _side_envelope("taker_buy_sell_volume_5m", changed, ordinal=2)
    )[0]
    assert first.semantic_identity == second.semantic_identity
    assert first.logical_content != second.logical_content
