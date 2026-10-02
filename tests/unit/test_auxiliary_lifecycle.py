from __future__ import annotations

import asyncio
import logging
import threading
from typing import Any, cast

import pytest

from binance_market_data_recorder.binance.spot.rate_limit import SpotIpRateLimiter
from binance_market_data_recorder.collector.spot_side_data import SpotExchangeInfoPoller
from binance_market_data_recorder.collector.usdm_side_data import SideDataStats, SideDataSupervisor
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.stream import StreamSpool
from tests.factories import event


class Spool:
    symbol = "BTCUSDT"

    def __init__(self) -> None:
        self.closed = False
        self.events: list[EventEnvelope] = []

    def enqueue(self, envelope: EventEnvelope) -> None:
        self.events.append(envelope)

    def drain_all(self) -> int:
        return len(self.events)

    def sync(self) -> None:
        pass

    def close_and_seal(self) -> None:
        self.closed = True


@pytest.mark.parametrize("gate", ["ban", "slot", "slot-stop-race", "inflight"])
def test_spot_auxiliary_stop_releases_waits_and_owns_inflight_worker(
    monkeypatch: pytest.MonkeyPatch, gate: str
) -> None:
    spool = Spool()
    entered, release = threading.Event(), threading.Event()
    calls = 0

    def capture(**_kwargs: Any) -> EventEnvelope:
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(timeout=3)
        return event(1).model_copy(update={"raw_payload": b'{"response":{"headers":{}}}'})

    async def exercise() -> None:
        limiter = SpotIpRateLimiter(weight_budget_per_minute=1_000_000_000)
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.spot_side_data.shared_spot_ip_rate_limiter",
            lambda: limiter,
        )
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.spot_side_data.capture_spot_exchange_info",
            capture,
        )
        stop = asyncio.Event()
        poller = SpotExchangeInfoPoller(
            interval_seconds=3600,
            spool=cast(StreamSpool, spool),
            stats=SideDataStats(True),
            collector_instance_id="test",
            collector_version="test",
            rest_api=None,
            timeout_ms=1000,
            logger=logging.getLogger("test.spot-aux-stop"),
        )
        if gate == "ban":
            await limiter.observe_weight_rejection(
                status=418,
                weight=20,
                headers={"Retry-After": "86400"},
                body_text="ban",
            )
        slot = limiter.request_slot() if gate.startswith("slot") else None
        if slot is not None:
            await slot.__aenter__()
        task = asyncio.create_task(poller.run(stop))
        try:
            if gate == "inflight":
                for _ in range(1000):
                    if entered.is_set():
                        break
                    await asyncio.sleep(0.001)
                assert entered.is_set()
                stop.set()
                await asyncio.sleep(0.01)
                assert not task.done() and not spool.closed
                release.set()
            else:
                for _ in range(5):
                    await asyncio.sleep(0)
                if gate == "slot-stop-race":
                    assert slot is not None
                    await slot.__aexit__(None, None, None)
                    slot = None
                stop.set()
            await asyncio.wait_for(task, timeout=1)
            assert spool.closed
            assert calls == (1 if gate == "inflight" else 0)
            # The test's independent holder still owns the slot in this case.
            assert limiter._request_lock.locked() == (slot is not None)
        finally:
            release.set()
            stop.set()
            if slot is not None:
                await slot.__aexit__(None, None, None)
            await asyncio.gather(task, return_exceptions=True)
            assert not limiter._request_lock.locked()

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["child-cancel", "factory", "owner-cancel"])
def test_auxiliary_termination_is_visible_and_drains_without_stopping_core(failure: str) -> None:
    class Healthy:
        terminal_on_failure = True

        def __init__(self) -> None:
            self.entered = asyncio.Event()
            self.drained = False

        async def run(self, stop: asyncio.Event) -> None:
            self.entered.set()
            try:
                await stop.wait()
            finally:
                self.drained = True

    async def exercise() -> None:
        core_stop = asyncio.Event()
        healthy = Healthy()
        stats = {"mark_price": SideDataStats(True)}

        def factory() -> Healthy:
            if failure == "factory":
                raise OSError("factory failed after enabling auxiliary stream")
            return healthy

        supervisor = SideDataSupervisor(
            {"mark_price": factory},
            stats,
            logging.getLogger("test.aux-termination"),
            task_name_prefix="test-auxiliary-lifecycle",
        )
        owner = asyncio.create_task(supervisor.run(core_stop))
        try:
            if failure != "factory":
                await asyncio.wait_for(healthy.entered.wait(), timeout=1)
            if failure == "child-cancel":
                child = next(
                    t
                    for t in asyncio.all_tasks()
                    if t.get_name() == "test-auxiliary-lifecycle:mark_price"
                )
                child.cancel()
                await asyncio.gather(child, return_exceptions=True)
            elif failure == "owner-cancel":
                owner.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(owner, timeout=1)
                assert healthy.drained
                assert stats["mark_price"].failures == 0
                assert stats["mark_price"].status == "STOPPED"
                return
            else:
                for _ in range(5):
                    await asyncio.sleep(0)
            assert not core_stop.is_set() and not owner.done()
            assert stats["mark_price"].status == "FAILED"
            assert not stats["mark_price"].running
            assert stats["mark_price"].failures == 1
            assert "mark_price" in supervisor.failures
        finally:
            assert not core_stop.is_set()
            core_stop.set()
            await asyncio.gather(owner, return_exceptions=True)

    asyncio.run(exercise())


@pytest.mark.parametrize("status", ["RUNNING", "RETRYING", "FAILED", "STOPPED"])
@pytest.mark.parametrize("sparse", [True, False])
def test_quiet_sparse_stream_and_terminal_health_preserve_their_meaning(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    sparse: bool,
) -> None:
    monkeypatch.setattr(
        "binance_market_data_recorder.collector.usdm_side_data.time.time_ns",
        lambda: 1_000_000_000_000,
    )
    stats = SideDataStats(
        True,
        status=status,
        event_sparse=sparse,
        connected=status == "RUNNING",
        last_success_at_utc_ns=1,
    )
    state = stats.public_dict(degraded_after_seconds=900)
    assert state["status"] == ("STALE" if status == "RUNNING" and not sparse else status)
    assert state["connected"] == (status == "RUNNING")


@pytest.mark.parametrize("fail_sync", [False, True])
def test_spot_metadata_success_requires_post_frame_sync(
    monkeypatch: pytest.MonkeyPatch,
    fail_sync: bool,
) -> None:
    class DurableSpool(Spool):
        synced = False

        def sync(self) -> None:
            assert self.events
            if fail_sync:
                raise OSError("injected metadata fsync failure")
            self.synced = True

    spool = DurableSpool()
    stats = SideDataStats(True)
    monkeypatch.setattr(
        "binance_market_data_recorder.collector.spot_side_data.shared_spot_ip_rate_limiter",
        lambda: SpotIpRateLimiter(weight_budget_per_minute=1_000_000_000),
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.collector.spot_side_data.capture_spot_exchange_info",
        lambda **kwargs: event(1, payload=b'{"response":{"headers":{}}}'),
    )

    async def exercise() -> None:
        stop = asyncio.Event()
        poller = SpotExchangeInfoPoller(
            interval_seconds=3600,
            spool=cast(StreamSpool, spool),
            stats=stats,
            collector_instance_id="test",
            collector_version="test",
            rest_api=None,
            timeout_ms=1000,
            logger=logging.getLogger("test.metadata-durability"),
        )
        task = asyncio.create_task(poller.run(stop))
        try:
            for _ in range(1000):
                if stats.accepted or stats.failures:
                    break
                await asyncio.sleep(0.001)
            assert stats.accepted == (0 if fail_sync else 1)
            assert spool.synced == (not fail_sync)
            assert stats.failures == int(fail_sync)
        finally:
            stop.set()
            await asyncio.wait_for(task, 1)

    asyncio.run(exercise())
