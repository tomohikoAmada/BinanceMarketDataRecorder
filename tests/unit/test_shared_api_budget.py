from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest

from binance_market_data_recorder.binance import request_budget as budgets
from binance_market_data_recorder.binance.request_budget import RequestBudget, usdm_request_charges
from binance_market_data_recorder.collector.usdm_side_data import UsdMRestCooldown


def test_rolling_weight_and_special_windows_do_not_reset_on_recovery() -> None:
    now = [0.0]
    budget = RequestBudget(clock=lambda: now[0])
    assert budget.take({"spot_weight": 2000}) == 0
    assert budget.take({"spot_weight": 20}) == 60
    assert budget.take({"usdm_weight": 20}) == 0
    now[0] = 59.999
    assert budget.take({"spot_weight": 50}) == pytest.approx(0.001)
    now[0] = 60
    assert budget.take({"spot_weight": 50}) == 0
    assert budget.take({"usdm_statistics": 800}) == 0
    assert budget.take(usdm_request_charges("basis_5m")) == 300
    assert budget.take({"usdm_funding": 400}) == 0
    assert budget.take(usdm_request_charges("funding_history")) == 300
    assert budget.take(usdm_request_charges("funding_info")) == 300
    assert budget.take({"spot_connections": 180}) == 0
    assert budget.take({"spot_connections": 1}) == 300
    with pytest.raises(ValueError):
        budget.take({"spot_weight": 2001})


def test_statistics_wait_releases_wire_lock_for_depth_and_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        budget = RequestBudget()
        monkeypatch.setitem(budgets._BUDGETS, asyncio.get_running_loop(), budget)
        assert budget.take({"usdm_statistics": 800}) == 0
        lock = asyncio.Lock()
        cooldown = UsdMRestCooldown()
        stop = asyncio.Event()
        attempted: list[str] = []

        async def statistics() -> None:
            async with cooldown.request_slot(
                lock, stop, usdm_request_charges("basis_5m")
            ) as admitted:
                if admitted:
                    attempted.append("statistics")

        task = asyncio.create_task(statistics())
        await asyncio.sleep(0)
        async with cooldown.request_slot(
            lock, stop, usdm_request_charges("depth_snapshot")
        ) as admitted:
            assert admitted
            attempted.append("depth")
        stop.set()
        await asyncio.wait_for(task, timeout=0.2)
        assert attempted == ["depth"]

    asyncio.run(exercise())


@pytest.mark.parametrize("market", ["spot", "usdm"])
def test_real_websocket_opener_gates_all_attempts_and_cancels_without_wire(
    monkeypatch: pytest.MonkeyPatch, market: str
) -> None:
    async def exercise() -> None:
        from binance_market_data_recorder.binance.spot import websocket as spot
        from binance_market_data_recorder.binance.usdm import websocket as usdm

        module = spot if market == "spot" else usdm
        opener = spot.open_spot_websocket if market == "spot" else usdm.open_usdm_websocket
        budget = RequestBudget()
        monkeypatch.setitem(budgets._BUDGETS, asyncio.get_running_loop(), budget)
        calls: list[dict[str, Any]] = []

        @asynccontextmanager
        async def wire(*args: Any, **kwargs: Any) -> Any:
            calls.append(kwargs)
            yield object()

        monkeypatch.setattr(module, "connect", wire)
        async with opener("wss://fixture.invalid"):
            pass
        assert len(calls) == 1 and calls[0]["ping_interval"] is None
        assert budget.take({market + "_connections": 179}) == 0

        async def blocked() -> None:
            async with opener("wss://fixture.invalid"):
                pytest.fail("quota-exhausted connection reached the wire")

        task = asyncio.create_task(blocked())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(calls) == 1

    asyncio.run(exercise())


def test_actual_spot_requester_shutdown_cancels_queued_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from binance_market_data_recorder.binance.spot.rest import SpotSnapshotRequester
    from tests.unit.test_spot_rate_limit import Response, SequencedApi, _limiter

    async def exercise() -> None:
        budget = RequestBudget()
        monkeypatch.setitem(budgets._BUDGETS, asyncio.get_running_loop(), budget)
        assert budget.take({"spot_weight": 2000}) == 0
        api = SequencedApi([Response()])
        requester = SpotSnapshotRequester(rest_api=api, rate_limiter=_limiter())
        task = asyncio.create_task(
            requester.capture(
                symbol="BTCUSDT",
                collector_instance_id="test",
                collector_version="test",
                limit=1000,
                timeout_ms=1000,
            )
        )
        for _ in range(5):
            await asyncio.sleep(0)
        assert requester.inflight_count() == 1 and api.calls == 0
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(requester.wait_for_idle(cancel_queued=True), timeout=0.2)
        assert api.calls == 0 and requester.inflight_count() == 0

    asyncio.run(exercise())


@pytest.mark.parametrize("market", ["spot", "usdm"])
@pytest.mark.parametrize("status", [429, 418])
def test_websocket_rejection_blocks_market_rest_and_preserves_longer_deadline(
    monkeypatch: pytest.MonkeyPatch,
    market: str,
    status: int,
) -> None:
    from websockets.datastructures import Headers
    from websockets.exceptions import InvalidStatus
    from websockets.http11 import Response

    async def exercise() -> None:
        from binance_market_data_recorder.binance.spot import websocket as spot
        from binance_market_data_recorder.binance.usdm import websocket as usdm

        module = spot if market == "spot" else usdm
        opener = spot.open_spot_websocket if market == "spot" else usdm.open_usdm_websocket
        now = [0.0]
        budget = RequestBudget(clock=lambda: now[0])
        monkeypatch.setitem(budgets._BUDGETS, asyncio.get_running_loop(), budget)

        @asynccontextmanager
        async def wire(*args: Any, **kwargs: Any) -> Any:
            raise InvalidStatus(Response(status, "limited", Headers({"Retry-After": "42"})))
            yield

        monkeypatch.setattr(module, "connect", wire)
        with pytest.raises(InvalidStatus):
            async with opener("wss://fixture.invalid"):
                pass
        assert budget.take({market + "_weight": 1}) == pytest.approx(42)
        budget.block(market, 1)
        assert budget.take({market + "_connections": 1}) == pytest.approx(42)
        assert budget.take({("usdm" if market == "spot" else "spot") + "_weight": 1}) == 0

    asyncio.run(exercise())
