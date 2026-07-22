from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.binance.spot.schema import (
    SpotStream,
    envelope_from_websocket_frame,
)
from binance_market_data_recorder.domain.event import EventEnvelope

FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance" / "spot"


def envelope(name: str, stream: SpotStream) -> tuple[bytes, EventEnvelope]:
    raw = (FIXTURES / name).read_bytes().rstrip(b"\n")
    return raw, envelope_from_websocket_frame(
        raw_payload=raw,
        stream=stream,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="0.1.0+test",
        receive_time_utc_ns=123,
        receive_monotonic_ns=456,
    )


@pytest.mark.parametrize(
    ("name", "stream", "sequence", "event_time", "trade_time"),
    [
        ("diff_depth.json", SpotStream.DIFF_DEPTH, {"U": 157, "u": 160}, 1672515782136, None),
        (
            "agg_trade.json",
            SpotStream.AGG_TRADE,
            {"a": 12345, "f": 100, "l": 105},
            1672515782136,
            1672515782136,
        ),
        ("book_ticker.json", SpotStream.BOOK_TICKER, {"u": 400900217}, None, None),
    ],
)
def test_official_spot_fixtures_preserve_exact_bytes_and_metadata(
    name: str,
    stream: SpotStream,
    sequence: dict[str, int],
    event_time: int | None,
    trade_time: int | None,
) -> None:
    raw, parsed = envelope(name, stream)
    assert parsed.raw_payload == raw
    assert parsed.source_sequence == sequence
    assert parsed.exchange_event_time == event_time
    assert parsed.exchange_trade_time == trade_time
    assert parsed.capture_flags == ()


def test_server_shutdown_is_preserved_and_flagged() -> None:
    raw, parsed = envelope("server_shutdown.json", SpotStream.DIFF_DEPTH)
    assert parsed.raw_payload == raw
    assert parsed.exchange_event_time == 1770123456789
    assert parsed.capture_flags == ("server_shutdown",)


def test_malformed_payload_is_preserved_in_raw_instead_of_dropped() -> None:
    raw = b'{"e":"depthUpdate","s":"BTCUSDT","U":9,"u":8}'
    parsed = envelope_from_websocket_frame(
        raw_payload=raw,
        stream=SpotStream.DIFF_DEPTH,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="0.1.0+test",
        receive_time_utc_ns=123,
        receive_monotonic_ns=456,
    )
    assert parsed.raw_payload == raw
    assert parsed.capture_flags == ("malformed",)
    assert parsed.source_sequence == {}
