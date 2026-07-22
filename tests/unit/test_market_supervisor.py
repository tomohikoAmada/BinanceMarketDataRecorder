from __future__ import annotations

import asyncio

import pytest

from binance_market_data_recorder.collector.supervisor import MarketCollectorSupervisor


class FailingCollector:
    async def run(self, stop: asyncio.Event) -> None:
        raise RuntimeError("injected market crash")


class HealthyCollector:
    def __init__(self) -> None:
        self.ticks = 0

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.ticks += 1
            await asyncio.sleep(0.001)


@pytest.mark.parametrize("failed_market", ["spot", "um_perpetual"])
def test_one_market_crash_does_not_stop_the_other(failed_market: str) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        healthy = HealthyCollector()
        other = "um_perpetual" if failed_market == "spot" else "spot"
        supervisor = MarketCollectorSupervisor({failed_market: FailingCollector(), other: healthy})
        task = asyncio.create_task(supervisor.run(stop))
        await asyncio.sleep(0.03)
        assert healthy.ticks > 1
        assert failed_market in supervisor.failures
        assert not task.done()
        stop.set()
        await task

    asyncio.run(exercise())
