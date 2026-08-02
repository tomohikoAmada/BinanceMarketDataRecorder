from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest

from binance_market_data_recorder.binance.usdm.rest import (
    DepthResponse,
    UsdMSnapshotResponseError,
)
from binance_market_data_recorder.collector.spot import (
    SpotCollector,
    SpotCollectorSettings,
)
from binance_market_data_recorder.collector.usdm import (
    UsdMCollector,
    UsdMCollectorSettings,
)


class FailingRestApi:
    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        raise RuntimeError("transient controlled snapshot failure")


class InvalidResponse:
    status = 429
    headers: ClassVar[dict[str, object]] = {}

    def data(self) -> Any:
        raise AssertionError("an unacceptable HTTP response must not be parsed")


class InvalidResponseApi:
    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        return cast(DepthResponse, InvalidResponse())


class ValidModel:
    last_update_id = 100

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": self.last_update_id, "bids": [], "asks": []}


class ValidResponse:
    status = 200
    headers: ClassVar[dict[str, object]] = {}

    def data(self) -> ValidModel:
        return ValidModel()


class BlockingValidRestApi:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        self.started.set()
        assert self.release.wait(timeout=5)
        return cast(DepthResponse, ValidResponse())


class IdleStream:
    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()


def _collector(tmp_path: Path, rest_api: Any) -> UsdMCollector:
    collector = UsdMCollector(
        UsdMCollectorSettings(
            data_root=tmp_path,
            collector_instance_id="m21-3-usdm",
            collector_version="0.1.0+test",
            durability_interval_seconds=0,
            snapshot_retry_initial_seconds=0.001,
            snapshot_retry_maximum_seconds=0.001,
            snapshot_retry_jitter_ratio=0,
        ),
        logger=logging.getLogger("test.m21-3.usdm"),
        rest_api=rest_api,
    )
    cast(Any, collector).streams = (IdleStream(),)
    return collector


async def _close_collector(collector: UsdMCollector) -> None:
    await asyncio.to_thread(collector.snapshot_spool.close_and_seal)
    collector.catalog.close()


def test_usdm_snapshot_returns_when_session_stop_precedes_first_snapshot(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        collector = _collector(tmp_path, FailingRestApi())
        stop = asyncio.Event()
        stop.set()
        await collector._capture_snapshot(stop)
        assert collector.readiness_snapshot().snapshot_persisted is False
        await _close_collector(collector)

    asyncio.run(exercise())


def test_usdm_global_stop_before_first_snapshot_is_not_terminal(tmp_path: Path) -> None:
    async def exercise() -> None:
        collector = _collector(tmp_path, FailingRestApi())
        stop = asyncio.Event()
        stop.set()
        await collector._run_capture_session(stop)
        assert collector.readiness_snapshot().snapshot_persisted is False
        await _close_collector(collector)

    asyncio.run(exercise())


def test_usdm_snapshot_finishing_after_session_stop_is_not_applied(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        rest_api = BlockingValidRestApi()
        collector = _collector(tmp_path, rest_api)
        stop = asyncio.Event()
        task = asyncio.create_task(collector._capture_snapshot(stop))
        assert await asyncio.to_thread(rest_api.started.wait, 5)
        stop.set()
        rest_api.release.set()
        await task
        assert collector.readiness_snapshot().snapshot_persisted is False
        assert collector.snapshot_spool.queue.depth == 0
        await _close_collector(collector)

    asyncio.run(exercise())


def test_usdm_snapshot_task_cancellation_awaits_worker_and_propagates(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        rest_api = BlockingValidRestApi()
        collector = _collector(tmp_path, rest_api)
        task = asyncio.create_task(collector._capture_snapshot(asyncio.Event()))
        assert await asyncio.to_thread(rest_api.started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert task.done() is False
        rest_api.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        leaked = [
            pending
            for pending in asyncio.all_tasks()
            if pending is not asyncio.current_task() and not pending.done()
        ]
        assert leaked == []
        await _close_collector(collector)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "reason",
    ["unexpected_disconnect", "planned_rotation", "server_shutdown", "outer_resync"],
)
def test_usdm_resync_reason_ends_pre_snapshot_session_cleanly(
    tmp_path: Path, reason: str
) -> None:
    async def exercise() -> None:
        collector = _collector(tmp_path, FailingRestApi())
        collector.resync.request(reason)
        await collector._run_capture_session(asyncio.Event())
        assert collector.resync.requested.is_set()
        assert collector.resync.active is not None
        assert collector.resync.active.reason == reason
        assert collector.readiness_snapshot().ready is False
        await _close_collector(collector)

    asyncio.run(exercise())


def test_usdm_true_snapshot_task_exception_still_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        collector = _collector(tmp_path, FailingRestApi())

        def fail_snapshot(**_kwargs: object) -> Any:
            raise ValueError("fatal snapshot implementation failure")

        monkeypatch.setattr(
            "binance_market_data_recorder.collector.usdm.capture_depth_snapshot",
            fail_snapshot,
        )
        with pytest.raises(ExceptionGroup) as captured:
            await collector._run_capture_session(asyncio.Event())
        fatal = captured.value.subgroup(ValueError)
        assert fatal is not None
        assert any(
            isinstance(exc, ValueError)
            and str(exc) == "fatal snapshot implementation failure"
            for exc in fatal.exceptions
        )
        leaked = [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        assert leaked == []
        await _close_collector(collector)

    asyncio.run(exercise())


def test_usdm_invalid_snapshot_response_is_not_retried_or_swallowed(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        collector = _collector(tmp_path, InvalidResponseApi())
        with pytest.raises(UsdMSnapshotResponseError, match="HTTP 429"):
            await collector._capture_snapshot(asyncio.Event())
        await _close_collector(collector)

    asyncio.run(exercise())


def test_usdm_one_hundred_resync_sessions_leave_no_tasks_or_exception_groups(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        collector = _collector(tmp_path, FailingRestApi())
        baseline = set(asyncio.all_tasks())
        for ordinal in range(100):
            collector.resync.prepare_restart()
            collector.resync.request(f"controlled_resync_{ordinal}")
            await collector._run_capture_session(asyncio.Event())
        await asyncio.sleep(0)
        leaked = [
            task
            for task in asyncio.all_tasks()
            if task not in baseline and task is not asyncio.current_task() and not task.done()
        ]
        assert leaked == []
        assert collector.readiness_snapshot().ready is False
        await _close_collector(collector)

    asyncio.run(exercise())


def test_spot_pre_snapshot_session_stop_remains_clean(tmp_path: Path) -> None:
    async def exercise() -> None:
        collector = SpotCollector(
            SpotCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="m21-3-spot",
                collector_version="0.1.0+test",
            ),
            logger=logging.getLogger("test.m21-3.spot"),
        )
        stop = asyncio.Event()
        stop.set()
        await collector._capture_snapshot(stop)
        assert collector.snapshot_requester.inflight_count() == 0
        assert collector.readiness_snapshot().ready is False
        collector.catalog.close()

    asyncio.run(exercise())
