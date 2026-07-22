from __future__ import annotations

import random

import pytest

from binance_market_data_recorder.orderbook.model import (
    BookSnapshot,
    BookTicker,
    DepthUpdate,
    OrderBookDataError,
)
from binance_market_data_recorder.orderbook.reconstructor import (
    BookUnavailableError,
    LocalBookReconstructor,
    ReconstructionState,
    SynchronizeResult,
    TickerComparison,
)


def update(
    market: str,
    first: int,
    final: int | None = None,
    *,
    pu: int | None = None,
    bids: tuple[tuple[str, str], ...] = (),
    asks: tuple[tuple[str, str], ...] = (),
) -> DepthUpdate:
    resolved_final = first if final is None else final
    return DepthUpdate(
        market=market,  # type: ignore[arg-type]
        symbol="BTCUSDT",
        first_update_id=first,
        final_update_id=resolved_final,
        previous_final_update_id=pu,
        bids=bids,
        asks=asks,
        receive_time_utc_ns=resolved_final,
    )


def snapshot(market: str, last: int = 160) -> BookSnapshot:
    return BookSnapshot(
        market=market,  # type: ignore[arg-type]
        symbol="BTCUSDT",
        last_update_id=last,
        bids=(("99", "1"),),
        asks=(("101", "1"),),
    )


def test_official_spot_buffer_snapshot_bridge_and_live_rule() -> None:
    reconstructor = LocalBookReconstructor("spot")
    reconstructor.offer(update("spot", 157, 160, bids=(("99", "2"),)))
    reconstructor.offer(update("spot", 160, 161, asks=(("101", "3"),)))
    assert reconstructor.synchronize(snapshot("spot")) is SynchronizeResult.SYNCHRONIZED
    assert reconstructor.book.update_id == 161
    assert reconstructor.offer(update("spot", 162, bids=(("100", "4"),)))
    assert reconstructor.book.update_id == 162


def test_snapshot_cannot_skip_the_required_initial_diff_buffer() -> None:
    reconstructor = LocalBookReconstructor("spot")
    assert reconstructor.synchronize(snapshot("spot")) is SynchronizeResult.NEED_MORE_EVENTS
    assert reconstructor.state is ReconstructionState.BUFFERING


def test_official_usdm_bridge_and_pu_continuity_are_distinct_from_spot() -> None:
    reconstructor = LocalBookReconstructor("um_perpetual")
    reconstructor.offer(update("um_perpetual", 157, 160, pu=149))
    reconstructor.offer(update("um_perpetual", 161, pu=160, bids=(("100", "2"),)))
    assert reconstructor.synchronize(snapshot("um_perpetual")) is SynchronizeResult.SYNCHRONIZED
    assert reconstructor.book.update_id == 161
    assert reconstructor.offer(update("um_perpetual", 162, pu=161))


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_deleting_one_sequential_update_always_creates_incomplete_gap(market: str) -> None:
    reconstructor = LocalBookReconstructor(market)  # type: ignore[arg-type]
    if market == "spot":
        reconstructor.offer(update(market, 160, 161))
    else:
        reconstructor.offer(update(market, 160, pu=159))
    reconstructor.synchronize(snapshot(market))
    if market == "spot":
        accepted = reconstructor.offer(update(market, 163))
    else:
        accepted = reconstructor.offer(update(market, 163, pu=162))
    assert not accepted
    assert reconstructor.state is ReconstructionState.RESYNC_REQUIRED
    assert reconstructor.unreliable_intervals[-1].complete is False
    with pytest.raises(BookUnavailableError):
        _ = reconstructor.book


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
def test_snapshot_resync_closes_but_never_marks_gap_complete(market: str) -> None:
    reconstructor = LocalBookReconstructor(market)  # type: ignore[arg-type]
    first = update(market, 160, 161, pu=159 if market == "um_perpetual" else None)
    reconstructor.offer(first)
    reconstructor.synchronize(snapshot(market))
    gap = update(market, 163, 165, pu=162 if market == "um_perpetual" else None)
    assert not reconstructor.offer(gap)
    assert reconstructor.synchronize(snapshot(market, 164)) is SynchronizeResult.SYNCHRONIZED
    interval = reconstructor.unreliable_intervals[-1]
    assert interval.ended_at_update_id == 165
    assert interval.complete is False
    assert reconstructor.audits[-1].kind == "orderbook_resync"


def test_spot_and_usdm_inputs_cannot_be_mixed() -> None:
    reconstructor = LocalBookReconstructor("spot")
    with pytest.raises(OrderBookDataError, match="cannot be mixed"):
        reconstructor.offer(update("um_perpetual", 1, pu=0))


def test_crossed_empty_and_book_ticker_comparison_are_quality_audits() -> None:
    reconstructor = LocalBookReconstructor("spot")
    reconstructor.offer(update("spot", 160, 161, bids=(("102", "1"),)))
    reconstructor.synchronize(snapshot("spot"))
    assert any(audit.kind == "crossed_book" for audit in reconstructor.audits)
    ticker = BookTicker("spot", "BTCUSDT", 161, "102", "1", "101", "1")
    assert reconstructor.compare_book_ticker(ticker) is TickerComparison.MATCH
    mismatch = BookTicker("spot", "BTCUSDT", 161, "100", "1", "101", "1")
    assert reconstructor.compare_book_ticker(mismatch) is TickerComparison.MISMATCH
    reconstructor.offer(update("spot", 162, bids=(("102", "0"), ("99", "0"))))
    assert any(audit.kind == "empty_book_side" for audit in reconstructor.audits)


def test_randomized_absolute_replay_has_a_stable_hash() -> None:
    rng = random.Random(20260722)
    updates: list[DepthUpdate] = [update("spot", 160, 161)]
    for sequence in range(162, 500):
        price = str(90 + rng.randrange(20))
        quantity = "0" if rng.randrange(7) == 0 else str(1 + rng.randrange(20))
        side = rng.choice(("bid", "ask"))
        updates.append(
            update(
                "spot",
                sequence,
                bids=((price, quantity),) if side == "bid" else (),
                asks=((price, quantity),) if side == "ask" else (),
            )
        )

    def replay() -> str:
        reconstructor = LocalBookReconstructor("spot")
        for item in updates:
            reconstructor.offer(item)
        assert reconstructor.synchronize(snapshot("spot")) is SynchronizeResult.SYNCHRONIZED
        return reconstructor.book.logical_hash()

    expected = "2db5156e7f047c4b9e60eee3007821d94ca7ac6b3f391a43405bd81855fda666"
    assert replay() == replay() == expected
