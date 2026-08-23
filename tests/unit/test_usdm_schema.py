from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.binance.usdm.schema import (
    USDM_DEPTH_CONTINUITY_CONTRACT,
    USDM_STREAMS,
    UsdMStream,
    envelope_from_websocket_frame,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance" / "usdm"


@pytest.mark.parametrize(
    ("name", "stream", "sequence"),
    [
        ("diff_depth.json", UsdMStream.DIFF_DEPTH, {"U": 157, "u": 160, "pu": 149}),
        ("agg_trade.json", UsdMStream.AGG_TRADE, {"a": 12345, "f": 100, "l": 105}),
        ("book_ticker.json", UsdMStream.BOOK_TICKER, {"u": 400900217}),
    ],
)
def test_official_usdm_fixtures_preserve_bytes_and_metadata(
    name: str, stream: UsdMStream, sequence: dict[str, int]
) -> None:
    raw = (FIXTURES / name).read_bytes().rstrip(b"\n")
    envelope = envelope_from_websocket_frame(
        raw_payload=raw,
        stream=stream,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="0.1.0+test",
        receive_time_utc_ns=123,
        receive_monotonic_ns=456,
    )
    assert envelope.market == "um_perpetual"
    assert envelope.raw_payload == raw
    assert envelope.source_sequence == sequence
    assert envelope.exchange_event_time == 1672515782136
    assert envelope.exchange_trade_time == 1672515782135
    assert envelope.capture_flags == ()


def test_usdm_official_routed_endpoint_mapping() -> None:
    routes = {spec.stream: spec.route for spec in USDM_STREAMS}
    assert routes == {
        UsdMStream.DIFF_DEPTH: "public",
        UsdMStream.AGG_TRADE: "market",
        UsdMStream.BOOK_TICKER: "public",
    }


def test_usdm_depth_requires_and_preserves_previous_final_update_id() -> None:
    assert USDM_DEPTH_CONTINUITY_CONTRACT == "each_event_pu_equals_previous_event_u"
    malformed = b'{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]}'
    envelope = envelope_from_websocket_frame(
        raw_payload=malformed,
        stream=UsdMStream.DIFF_DEPTH,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="test",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    assert envelope.raw_payload == malformed
    assert envelope.capture_flags == ("malformed",)
    assert envelope.source_sequence == {}


@pytest.mark.parametrize(
    ("stream", "payload"),
    [
        (
            UsdMStream.AGG_TRADE,
            b'{"e":"aggTrade","E":1,"T":1,"s":"BTCUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"m":true,"st":2}',
        ),
        (
            UsdMStream.AGG_TRADE,
            b'{"e":"aggTrade","E":1,"T":1,"s":"BTCUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"m":true}',
        ),
        (
            UsdMStream.BOOK_TICKER,
            b'{"e":"bookTicker","E":1,"T":1,"s":"BTCUSDT","u":1,"b":"1","B":"1","a":"2","A":"1","st":true}',
        ),
        (
            UsdMStream.DIFF_DEPTH,
            b'{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[],"st":1}',
        ),
        (
            UsdMStream.DIFF_DEPTH,
            b'{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[],"ps":"BTCUSD","st":1}',
        ),
    ],
)
def test_usdm_identity_fields_fail_closed_without_losing_raw_bytes(
    stream: UsdMStream, payload: bytes
) -> None:
    envelope = envelope_from_websocket_frame(
        raw_payload=payload,
        stream=stream,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="test",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    assert envelope.raw_payload == payload
    assert envelope.capture_flags == ("malformed",)
    assert envelope.source_sequence == {}


def test_usdm_agg_trade_does_not_require_undocumented_pair_field() -> None:
    payload = (
        b'{"e":"aggTrade","E":1,"T":1,"s":"BTCUSDT","a":1,'
        b'"p":"1","q":"1","f":1,"l":1,"m":true,"st":1,'
        b'"ps":"NOT_REQUIRED_BY_AGG_TRADE_SCHEMA"}'
    )
    envelope = envelope_from_websocket_frame(
        raw_payload=payload,
        stream=UsdMStream.AGG_TRADE,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="test",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    assert envelope.capture_flags == ()
    assert envelope.source_sequence == {"a": 1, "f": 1, "l": 1}
