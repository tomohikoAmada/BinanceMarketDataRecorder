from __future__ import annotations

import asyncio

import pytest

from binance_market_data_recorder.binance.spot.websocket import ReconnectBackoff


def test_reconnect_backoff_is_bounded_and_testable() -> None:
    policy = ReconnectBackoff(initial_seconds=1, maximum_seconds=8, jitter_ratio=0.2)
    assert policy.delay(1, random_value=0.5) == 1
    assert policy.delay(4, random_value=0.5) == 8
    assert policy.delay(20, random_value=0.0) == pytest.approx(6.4)
    assert policy.delay(20, random_value=1.0) == 8


def test_backoff_rejects_invalid_attempt() -> None:
    with pytest.raises(ValueError, match="positive"):
        ReconnectBackoff().delay(0)


def test_asyncio_cancelled_error_is_not_anordinary_exception() -> None:
    assert not issubclass(asyncio.CancelledError, Exception)
