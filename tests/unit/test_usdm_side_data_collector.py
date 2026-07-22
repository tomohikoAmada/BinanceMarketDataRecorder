from __future__ import annotations

import asyncio
import logging

import pytest

from binance_market_data_recorder.binance.usdm.side_data_rest import RestSideDataKind
from binance_market_data_recorder.binance.usdm.side_data_schema import UsdMSideStream
from binance_market_data_recorder.collector.usdm_side_data import (
    SideDataStats,
    SideDataSupervisor,
    UsdMSideDataSettings,
)


def test_each_side_data_kind_can_be_enabled_independently() -> None:
    settings = UsdMSideDataSettings(
        mark_price_enabled=False,
        liquidation_enabled=True,
        premium_index_enabled=False,
        funding_history_enabled=True,
        funding_info_enabled=False,
        open_interest_enabled=True,
        exchange_info_enabled=False,
    )
    assert not settings.stream_enabled(UsdMSideStream.MARK_PRICE)
    assert settings.stream_enabled(UsdMSideStream.LIQUIDATION)
    assert not settings.rest_enabled(RestSideDataKind.PREMIUM_INDEX)
    assert settings.rest_enabled(RestSideDataKind.FUNDING_HISTORY)
    assert not settings.rest_enabled(RestSideDataKind.FUNDING_INFO)
    assert settings.rest_enabled(RestSideDataKind.OPEN_INTEREST)
    assert not settings.rest_enabled(RestSideDataKind.EXCHANGE_INFO)


def test_polling_intervals_must_be_positive() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        UsdMSideDataSettings(open_interest_interval_seconds=0)


def test_terminal_side_failure_does_not_set_shared_core_stop() -> None:
    class Failing:
        async def run(self, stop: asyncio.Event) -> None:
            raise RuntimeError("injected terminal side failure")

    class Healthy:
        def __init__(self) -> None:
            self.started = asyncio.Event()

        async def run(self, stop: asyncio.Event) -> None:
            self.started.set()
            await stop.wait()

    async def exercise() -> tuple[SideDataSupervisor, asyncio.Event]:
        stop = asyncio.Event()
        healthy = Healthy()
        stats = {"failing": SideDataStats(True), "healthy": SideDataStats(True)}
        supervisor = SideDataSupervisor(
            {"failing": Failing(), "healthy": healthy},
            stats,
            logging.getLogger("test.side-supervisor"),
        )
        task = asyncio.create_task(supervisor.run(stop))
        await asyncio.wait_for(healthy.started.wait(), timeout=1)
        for _ in range(100):
            if "failing" in supervisor.failures:
                break
            await asyncio.sleep(0)
        assert not stop.is_set()
        assert not task.done()
        stop.set()
        await asyncio.wait_for(task, timeout=1)
        return supervisor, stop

    supervisor, stop = asyncio.run(exercise())
    assert stop.is_set()
    assert isinstance(supervisor.failures["failing"], RuntimeError)
    assert supervisor.stats["failing"].failures == 1
