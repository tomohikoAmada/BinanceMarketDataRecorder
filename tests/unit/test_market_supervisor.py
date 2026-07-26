from __future__ import annotations

import asyncio

import pytest

from binance_market_data_recorder.collector.supervisor import (
    CoreMarketTerminalFailure,
    MarketCollectorSupervisor,
)


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
def test_one_market_crash_stops_and_seals_the_other(failed_market: str) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        healthy = HealthyCollector()
        other = "um_perpetual" if failed_market == "spot" else "spot"
        supervisor = MarketCollectorSupervisor({failed_market: FailingCollector(), other: healthy})
        with pytest.raises(CoreMarketTerminalFailure, match=failed_market):
            await supervisor.run(stop)
        assert healthy.ticks >= 1
        assert healthy.stopped
        assert failed_market in supervisor.failures

    asyncio.run(exercise())


def test_normal_return_is_immediate_terminal_failure() -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        healthy = HealthyCollector()
        observed: list[tuple[str, BaseException]] = []
        supervisor = MarketCollectorSupervisor(
            {"spot": NormallyReturningCollector(), "um_perpetual": healthy},
            terminal_failure_observer=lambda market, exc: observed.append(
                (market, exc)
            ),
        )
        with pytest.raises(CoreMarketTerminalFailure, match="spot"):
            await asyncio.wait_for(supervisor.run(stop), timeout=1)
        assert healthy.stopped
        assert isinstance(supervisor.failures["spot"], RuntimeError)
        assert observed == [("spot", supervisor.failures["spot"])]

    asyncio.run(exercise())
