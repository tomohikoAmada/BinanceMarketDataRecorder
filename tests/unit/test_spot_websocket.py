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


@pytest.mark.parametrize("failures", [1024, 1025, 100_000, 10**100])
def test_reconnect_backoff_caps_before_exponent_overflow(failures: int) -> None:
    policy = ReconnectBackoff(initial_seconds=0.1, maximum_seconds=60)
    assert policy.delay(failures, random_value=0.5) == 60
    assert policy.delay(failures, random_value=0) == 48
    assert policy.delay(failures, random_value=1) == 60


@pytest.mark.parametrize("initial,maximum", [(0.1, 60.0), (2.0, 10.0), (1e-300, 1e300)])
def test_reconnect_backoff_preserves_finite_prefix_and_extreme_bounds(
    initial: float, maximum: float,
) -> None:
    policy = ReconnectBackoff(initial_seconds=initial, maximum_seconds=maximum)
    for failures in range(1, 30):
        assert policy.delay(failures, random_value=0.5) == min(
            maximum, initial * 2.0 ** (failures - 1)
        )
    assert policy.delay(10**100, random_value=0.5) == maximum


def test_asyncio_cancelled_error_is_not_anordinary_exception() -> None:
    assert not issubclass(asyncio.CancelledError, Exception)
