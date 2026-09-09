from __future__ import annotations

import asyncio

import pytest

from binance_market_data_recorder.collector.supervisor import (
    CoreMarketTerminalFailure,
    MarketCollectorSupervisor,
)
from binance_market_data_recorder.domain.event import Market
from binance_market_data_recorder.domain.product import ProductKey


class FailingCollector:
    async def run(self, stop: asyncio.Event) -> None:
        raise RuntimeError("injected market crash")


class HealthyCollector:
    def __init__(self) -> None:
        self.ticks = 0
        self.stopped = False

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.ticks += 1
            await asyncio.sleep(0.001)
        self.stopped = True


class NormallyReturningCollector:
    async def run(self, stop: asyncio.Event) -> None:
        return


@pytest.mark.parametrize("failed_market", ["spot", "um_perpetual"])
def test_one_market_crash_stops_and_seals_the_other(failed_market: Market) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        healthy = HealthyCollector()
        other: Market = "um_perpetual" if failed_market == "spot" else "spot"
        supervisor = MarketCollectorSupervisor(
            {
                ProductKey(failed_market, "BTCUSDT"): FailingCollector(),
                ProductKey(other, "BTCUSDT"): healthy,
            }
        )
        with pytest.raises(CoreMarketTerminalFailure, match=failed_market):
            await supervisor.run(stop)
        assert healthy.ticks >= 1
        assert healthy.stopped
        assert ProductKey(failed_market, "BTCUSDT") in supervisor.failures

    asyncio.run(exercise())


def test_normal_return_is_immediate_terminal_failure() -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        healthy = HealthyCollector()
        observed: list[tuple[ProductKey, BaseException]] = []
        supervisor = MarketCollectorSupervisor(
            {
                ProductKey("spot", "BTCUSDT"): NormallyReturningCollector(),
                ProductKey("um_perpetual", "BTCUSDT"): healthy,
            },
            terminal_failure_observer=lambda market, exc: observed.append((market, exc)),
        )
        with pytest.raises(CoreMarketTerminalFailure, match="spot"):
            await asyncio.wait_for(supervisor.run(stop), timeout=1)
        assert healthy.stopped
        assert isinstance(supervisor.failures[ProductKey("spot", "BTCUSDT")], RuntimeError)
        assert observed == [
            (ProductKey("spot", "BTCUSDT"), supervisor.failures[ProductKey("spot", "BTCUSDT")])
        ]

    asyncio.run(exercise())
