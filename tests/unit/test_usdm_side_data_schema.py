from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.binance.usdm.side_data_schema import (
    USDM_SIDE_STREAMS,
    UsdMSideStream,
    envelope_from_side_stream_frame,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance" / "usdm"


@pytest.mark.parametrize(
    ("name", "stream", "event_time", "trade_time", "sequence", "flags"),
    [
        (
            "mark_price.json",
            UsdMSideStream.MARK_PRICE,
            1562305380000,
            None,
            {"nextFundingTime": 1562306400000},
            (),
        ),
        (
            "liquidation.json",
            UsdMSideStream.LIQUIDATION,
            1568014460893,
            1568014460893,
            {"orderTradeTime": 1568014460893},
            ("event_sparse_snapshot_stream",),
        ),
    ],
)
def test_official_side_stream_fixtures_preserve_exact_bytes_and_semantics(
    name: str,
    stream: UsdMSideStream,
    event_time: int,
    trade_time: int | None,
    sequence: dict[str, int],
    flags: tuple[str, ...],
) -> None:
    raw = (FIXTURES / name).read_bytes().rstrip(b"\n")
    envelope = envelope_from_side_stream_frame(
        raw_payload=raw,
        stream=stream,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="test",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    assert envelope.raw_payload == raw
    assert envelope.exchange_event_time == event_time
    assert envelope.exchange_trade_time == trade_time
    assert envelope.exchange_transaction_time is None
    assert envelope.source_sequence == sequence
    assert envelope.capture_flags == flags


def test_side_stream_routes_are_current_market_routes() -> None:
    assert {(spec.stream, spec.route, spec.wire_name) for spec in USDM_SIDE_STREAMS} == {
        (UsdMSideStream.MARK_PRICE, "market", "btcusdt@markPrice@1s"),
        (UsdMSideStream.LIQUIDATION, "market", "btcusdt@forceOrder"),
    }


def test_liquidation_silence_is_sparse_semantics_not_a_synthetic_event() -> None:
    malformed = b'{"e":"forceOrder","E":1,"o":{"s":"ETHUSDT"}}'
    envelope = envelope_from_side_stream_frame(
        raw_payload=malformed,
        stream=UsdMSideStream.LIQUIDATION,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="test",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    assert envelope.raw_payload == malformed
    assert envelope.capture_flags == ("event_sparse_snapshot_stream", "malformed")
    assert envelope.source_sequence == {}


def test_mark_price_requires_current_usdm_symbol_type() -> None:
    raw = (FIXTURES / "mark_price.json").read_bytes().replace(b'"st":1', b'"st":2')
    envelope = envelope_from_side_stream_frame(
        raw_payload=raw,
        stream=UsdMSideStream.MARK_PRICE,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="test",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    assert envelope.capture_flags == ("malformed",)
