from __future__ import annotations

import pytest

from binance_market_data_recorder.replay import (
    EventClock,
    GapPolicy,
    MissingExchangeTimeError,
    MissingExchangeTimePolicy,
    ReplayClock,
    ReplayQuery,
)


def test_receive_and_stream_specific_exchange_clocks_are_explicit() -> None:
    row = {
        "market": "spot",
        "stream": "agg_trade",
        "receive_time_utc_ns": 9_000_000_000,
        "exchange_event_time_ms": 1_000,
        "exchange_trade_time_ms": 2_000,
    }
    receive = EventClock(ReplayClock.RECEIVE_TIME).resolve(row)
    exchange = EventClock(ReplayClock.EXCHANGE_TIME).resolve(row)
    assert receive is not None and receive.event_time_ns == 9_000_000_000
    assert exchange is not None and exchange.event_time_ns == 2_000_000_000
    assert exchange.used_receive_time_fallback is False


def test_missing_exchange_time_never_falls_back_silently() -> None:
    row = {
        "market": "spot",
        "stream": "book_ticker",
        "receive_time_utc_ns": 9_000_000_000,
        "exchange_event_time_ms": None,
        "exchange_transaction_time_ms": None,
    }
    with pytest.raises(MissingExchangeTimeError, match="no documented"):
        EventClock(ReplayClock.EXCHANGE_TIME).resolve(row)
    assert (
        EventClock(
            ReplayClock.EXCHANGE_TIME,
            MissingExchangeTimePolicy.EXCLUDE,
        ).resolve(row)
        is None
    )
    fallback = EventClock(
        ReplayClock.EXCHANGE_TIME,
        MissingExchangeTimePolicy.FALLBACK_RECEIVE,
    ).resolve(row)
    assert fallback is not None
    assert fallback.event_time_ns == 9_000_000_000
    assert fallback.used_receive_time_fallback is True


def test_replay_query_uses_half_open_validated_nanosecond_bounds() -> None:
    query = ReplayQuery(
        clock=ReplayClock.RECEIVE_TIME,
        markets=("spot",),
        streams=("agg_trade",),
        start_time_ns=10,
        end_time_ns=20,
        gap_policy=GapPolicy.INCLUDE,
    )
    assert query.start_time_ns == 10
    assert query.end_time_ns == 20
    with pytest.raises(ValueError, match="non-empty"):
        ReplayQuery(start_time_ns=20, end_time_ns=20)
    with pytest.raises(ValueError, match="unique"):
        ReplayQuery(markets=("spot", "spot"))
    with pytest.raises(TypeError, match="clock"):
        ReplayQuery(clock="RECEIVE_TIME")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="tuples"):
        ReplayQuery(markets=["spot"])  # type: ignore[arg-type]
