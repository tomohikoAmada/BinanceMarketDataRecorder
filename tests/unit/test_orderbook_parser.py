from __future__ import annotations

import json

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.orderbook.parser import (
    book_ticker_from_envelope,
    depth_update_from_envelope,
    snapshot_from_envelope,
)


def envelope(market: str, stream: str, payload: dict[str, object]) -> EventEnvelope:
    return EventEnvelope(
        market=market,  # type: ignore[arg-type]
        symbol="BTCUSDT",
        stream=stream,
        module="test",
        connection_id="test",
        collector_instance_id="test",
        collector_version="test",
        receive_time_utc_ns=123,
        receive_monotonic_ns=456,
        raw_payload=json.dumps(payload, separators=(",", ":")).encode(),
    )


def test_raw_depth_and_book_ticker_envelopes_are_adapted_without_market_mix() -> None:
    depth = depth_update_from_envelope(
        envelope(
            "um_perpetual",
            "diff_depth",
            {
                "e": "depthUpdate",
                "s": "BTCUSDT",
                "U": 10,
                "u": 12,
                "pu": 9,
                "b": [["99", "2"]],
                "a": [],
            },
        )
    )
    assert depth.previous_final_update_id == 9
    assert depth.receive_time_utc_ns == 123
    ticker = book_ticker_from_envelope(
        envelope(
            "um_perpetual",
            "book_ticker",
            {"s": "BTCUSDT", "u": 12, "b": "99", "B": "2", "a": "101", "A": "3"},
        )
    )
    assert ticker.update_id == 12


def test_snapshot_provenance_model_is_adapted() -> None:
    captured = snapshot_from_envelope(
        envelope(
            "spot",
            "depth_snapshot",
            {
                "response": {
                    "model": {
                        "lastUpdateId": 42,
                        "bids": [["99", "2"]],
                        "asks": [["101", "3"]],
                    }
                }
            },
        )
    )
    assert captured.last_update_id == 42
    assert captured.bids == (("99", "2"),)
