from __future__ import annotations

import random

import pytest

from binance_market_data_recorder.domain.event import Market
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import (
    LocalBookReconstructor,
    ReconstructionState,
)


def event(market: Market, sequence: int) -> DepthUpdate:
    return DepthUpdate(
        market=market,
        symbol="BTCUSDT",
        first_update_id=sequence,
        final_update_id=sequence,
        previous_final_update_id=sequence - 1 if market == "um_perpetual" else None,
        bids=((str(90 + sequence % 10), str(sequence)),),
        asks=(),
        receive_time_utc_ns=sequence,
    )


@pytest.mark.parametrize("market", ["spot", "um_perpetual"])
@pytest.mark.parametrize("seed", range(20))
def test_randomly_deleted_depth_update_is_never_silently_complete(
    market: Market, seed: int
) -> None:
    rng = random.Random(seed)
    reconstructor = LocalBookReconstructor(market)
    if market == "spot":
        reconstructor.offer(DepthUpdate(market, "BTCUSDT", 100, 101, None, (), ()))
        start = 102
    else:
        reconstructor.offer(event(market, 100))
        start = 101
    reconstructor.synchronize(BookSnapshot(market, "BTCUSDT", 100, (("99", "1"),), (("101", "1"),)))
    missing = rng.randrange(start, 199)
    for sequence in range(start, 201):
        if sequence != missing:
            reconstructor.offer(event(market, sequence))
    assert reconstructor.state is ReconstructionState.RESYNC_REQUIRED
    assert any(audit.kind == "sequence_gap" for audit in reconstructor.audits)
    assert all(not interval.complete for interval in reconstructor.unreliable_intervals)
