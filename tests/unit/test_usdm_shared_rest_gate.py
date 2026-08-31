from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

import pytest
from binance_common.errors import RateLimitBanError, TooManyRequestsError

from binance_market_data_recorder.binance.usdm.rest import (
    DepthResponse,
    UsdMSnapshotHttpError,
)
from binance_market_data_recorder.binance.usdm.side_data_rest import (
    PublicResponse,
    RestSideDataKind,
    UsdMSideRestApi,
)
from binance_market_data_recorder.collector.usdm import (
    UsdMCollector,
    UsdMCollectorSettings,
)
from binance_market_data_recorder.collector.usdm_side_data import (
    RestSideDataPoller,
    UsdMRestCooldown,
    UsdMSideDataSettings,
)

BASE_MONOTONIC = 100.0
BASE_UTC_NS = 1_000_000_000_000


class SnapshotModel:
    last_update_id = 100

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": 100, "bids": [], "asks": []}


class SnapshotResponse:
    status = 200
    headers: ClassVar[dict[str, object]] = {}

    def data(self) -> SnapshotModel:
        return SnapshotModel()


class CountingSnapshotApi:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.called = threading.Event()

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        self.called.set()
        if self.error is not None:
            raise self.error
        return SnapshotResponse()


class FailingSideApi:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.called = threading.Event()

    def open_interest(self, symbol: str | None) -> PublicResponse:
        assert symbol == "BTCUSDT"
        self.called.set()
        raise self.error


class CountingSideApi:
    def __init__(self) -> None:
        self.called = threading.Event()

    def open_interest(self, symbol: str | None) -> PublicResponse:
        assert symbol == "BTCUSDT"
        self.called.set()
        return SideResponse()


class SideModel:
    def to_dict(self) -> dict[str, object]:
        return {"symbol": "BTCUSDT", "openInterest": "1", "time": 1}


class SideResponse:
    status = 200
    headers: ClassVar[dict[str, object]] = {}

    def data(self) -> SideModel:
        return SideModel()


class BlockingSnapshotApi:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.started = threading.Event()
        self.release = threading.Event()

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        self.started.set()
        if not self.release.wait(timeout=5):
            raise AssertionError("blocking snapshot worker was not released")
        raise self.error


class ObservedCooldown(UsdMRestCooldown):
    def __init__(self) -> None:
        self.install_finished = asyncio.Event()
        self.next_wait_started = asyncio.Event()
        self._next_wait_armed = False
        self.lock_to_observe: asyncio.Lock | None = None
        self.install_lock_was_locked: bool | None = None
        super().__init__(
            utc_clock_ns=lambda: BASE_UTC_NS,
            monotonic_clock=lambda: BASE_MONOTONIC,
        )

    def install(
        self,
        *,
        status: int,
        retry_after_seconds: float | None = None,
        retry_at_utc_ns: int | None = None,
    ) -> tuple[float, int, str]:
        if self.lock_to_observe is not None:
            self.install_lock_was_locked = self.lock_to_observe.locked()
        result = super().install(
            status=status,
            retry_after_seconds=retry_after_seconds,
            retry_at_utc_ns=retry_at_utc_ns,
        )
        self.install_finished.set()
        return result

    def arm_next_wait(self) -> None:
        self.next_wait_started.clear()
        self._next_wait_armed = True

    async def wait(self, stop: asyncio.Event) -> None:
        if self._next_wait_armed:
            self._next_wait_armed = False
            self.next_wait_started.set()
        await super().wait(stop)


class LockProbe(asyncio.Lock):
    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.acquire_count = 0
        self.acquire_started = asyncio.Event()
        self.second_acquire_started = asyncio.Event()

    async def acquire(self) -> Literal[True]:
        self.acquire_count += 1
        if self.armed:
            self.acquire_started.set()
            if self.acquire_count == 2:
                self.second_acquire_started.set()
        return await super().acquire()


def _side_settings() -> UsdMSideDataSettings:
    return UsdMSideDataSettings(
        mark_price_enabled=False,
        liquidation_enabled=False,
        premium_index_enabled=False,
        funding_history_enabled=False,
        funding_info_enabled=False,
        open_interest_enabled=True,
        exchange_info_enabled=False,
    )


def _collector(
    tmp_path: Path,
    *,
    rest_api: Any = None,
    side_rest_api: Any = None,
    with_side_data: bool = True,
) -> UsdMCollector:
    return UsdMCollector(
        UsdMCollectorSettings(
            data_root=tmp_path,
            collector_instance_id="shared-rest-gate-test",
            collector_version="test",
            durability_interval_seconds=0,
            snapshot_retry_initial_seconds=0.001,
            snapshot_retry_maximum_seconds=0.001,
            snapshot_retry_jitter_ratio=0,
            side_data=_side_settings() if with_side_data else None,
        ),
        logger=logging.getLogger("test.usdm.shared-rest-gate"),
        rest_api=rest_api,
        side_rest_api=cast(UsdMSideRestApi | None, side_rest_api),
    )


def _side_poller(collector: UsdMCollector) -> RestSideDataPoller:
    assert collector.side_data is not None
    factory = collector.side_data.supervisor.factories[RestSideDataKind.OPEN_INTEREST.value]
    return cast(RestSideDataPoller, factory())


async def _close_collector(
    collector: UsdMCollector, poller: RestSideDataPoller | None = None
) -> None:
    if poller is not None:
        await asyncio.to_thread(poller.spool.close_and_seal)
    await asyncio.to_thread(collector.snapshot_spool.close_and_seal)
    collector.catalog.close()


def _use_gate(
    collector: UsdMCollector,
    cooldown: ObservedCooldown,
    request_lock: asyncio.Lock | None = None,
) -> None:
    if request_lock is not None:
        collector.public_rest_request_lock = request_lock
    collector.public_rest_cooldown = cooldown
    if collector.side_data is not None:
        collector.side_data.rest_cooldown = cooldown
        collector.side_data.rest_request_lock = collector.public_rest_request_lock


def test_collector_injects_exact_shared_usdm_gate_objects(tmp_path: Path) -> None:
    collector = _collector(tmp_path)
    assert collector.side_data is not None
    assert collector.side_data.rest_cooldown is collector.public_rest_cooldown
    assert collector.side_data.rest_request_lock is collector.public_rest_request_lock
    collector.catalog.close()


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (TooManyRequestsError(status_code=429), 429),
        (RateLimitBanError(status_code=418), 418),
    ],
)
def test_side_rate_limit_blocks_core_snapshot_on_shared_cooldown(
    tmp_path: Path,
    error: BaseException,
    status: int,
) -> None:
    async def exercise() -> None:
        side_api = FailingSideApi(error)
        core_api = CountingSnapshotApi()
        collector = _collector(
            tmp_path, rest_api=core_api, side_rest_api=side_api
        )
        cooldown = ObservedCooldown()
        _use_gate(collector, cooldown)
        poller = _side_poller(collector)
        stop = asyncio.Event()
        side_task = asyncio.create_task(poller.run(stop))
        assert await asyncio.to_thread(side_api.called.wait, 1)
        await asyncio.wait_for(cooldown.install_finished.wait(), timeout=1)
        assert cooldown.status == status

        cooldown.arm_next_wait()
        core_task = asyncio.create_task(collector._capture_snapshot(stop))
        await asyncio.wait_for(cooldown.next_wait_started.wait(), timeout=1)
        assert not core_api.called.is_set()
        stop.set()
        await asyncio.wait_for(core_task, timeout=1)
        await asyncio.wait_for(side_task, timeout=1)
        await _close_collector(collector, poller)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("error", "status", "duration_seconds"),
    [
        (TooManyRequestsError(status_code=429), 429, 60.0),
        (RateLimitBanError(status_code=418), 418, 24.0 * 60.0 * 60.0),
    ],
)
def test_core_typed_rate_limit_blocks_side_route_and_uses_fallback(
    tmp_path: Path,
    error: BaseException,
    status: int,
    duration_seconds: float,
) -> None:
    async def exercise() -> None:
        core_api = CountingSnapshotApi(error)
        side_api = FailingSideApi(AssertionError("side REST must be gated"))
        collector = _collector(
            tmp_path, rest_api=core_api, side_rest_api=side_api
        )
        cooldown = ObservedCooldown()
        _use_gate(collector, cooldown)
        stop = asyncio.Event()
        core_task = asyncio.create_task(collector._capture_snapshot(stop))
        assert await asyncio.to_thread(core_api.called.wait, 1)
        await asyncio.wait_for(cooldown.install_finished.wait(), timeout=1)
        assert cooldown.status == status
        assert cooldown.reason == "fallback"
        assert cooldown.retry_at_utc_ns == BASE_UTC_NS + int(
            duration_seconds * 1_000_000_000
        )

        poller = _side_poller(collector)
        poller._active_stop = stop
        cooldown.arm_next_wait()
        side_task = asyncio.create_task(poller._request())
        await asyncio.wait_for(cooldown.next_wait_started.wait(), timeout=1)
        assert not side_api.called.is_set()
        stop.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(side_task, timeout=1)
        await asyncio.wait_for(core_task, timeout=1)
        await _close_collector(collector, poller)

    asyncio.run(exercise())


@pytest.mark.parametrize("status", [418, 429])
def test_core_http_rate_limit_installs_known_shared_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    async def exercise() -> None:
        called = threading.Event()
        known_retry_at = BASE_UTC_NS + 42 * 1_000_000_000

        def fail_snapshot(**_: object) -> Any:
            called.set()
            raise UsdMSnapshotHttpError(
                status=status,
                headers={"retry-after": "42"},
                retry_after_seconds=42.0,
                retry_at_utc_ns=known_retry_at,
            )

        monkeypatch.setattr(
            "binance_market_data_recorder.collector.usdm.capture_depth_snapshot",
            fail_snapshot,
        )
        collector = _collector(tmp_path, with_side_data=False)
        cooldown = ObservedCooldown()
        _use_gate(collector, cooldown)
        stop = asyncio.Event()
        task = asyncio.create_task(collector._capture_snapshot(stop))
        assert await asyncio.to_thread(called.wait, 1)
        await asyncio.wait_for(cooldown.install_finished.wait(), timeout=1)
        assert cooldown.status == status
        assert cooldown.reason == "retry_after"
        assert cooldown.retry_at_utc_ns == known_retry_at
        stop.set()
        await asyncio.wait_for(task, timeout=1)
        await _close_collector(collector)

    asyncio.run(exercise())


def test_core_second_gate_check_wins_shared_lock_toctou_race(tmp_path: Path) -> None:
    async def exercise() -> None:
        core_api = CountingSnapshotApi()
        collector = _collector(tmp_path, rest_api=core_api, with_side_data=False)
        cooldown = ObservedCooldown()
        _use_gate(collector, cooldown)
        lock = LockProbe()
        await lock.acquire()
        lock.armed = True
        collector.public_rest_request_lock = lock
        stop = asyncio.Event()
        core_task = asyncio.create_task(collector._capture_snapshot(stop))
        await asyncio.wait_for(lock.acquire_started.wait(), timeout=1)

        cooldown.install(status=418)
        cooldown.arm_next_wait()
        lock.release()
        await asyncio.wait_for(cooldown.next_wait_started.wait(), timeout=1)
        assert not core_api.called.is_set()
        stop.set()
        await asyncio.wait_for(core_task, timeout=1)
        await _close_collector(collector)

    asyncio.run(exercise())


def test_core_long_cooldown_stop_is_cooperative(tmp_path: Path) -> None:
    async def exercise() -> None:
        core_api = CountingSnapshotApi()
        collector = _collector(tmp_path, rest_api=core_api, with_side_data=False)
        cooldown = ObservedCooldown()
        _use_gate(collector, cooldown)
        cooldown.install(status=418)
        stop = asyncio.Event()
        task = asyncio.create_task(collector._capture_snapshot(stop))
        cooldown.arm_next_wait()
        await asyncio.wait_for(cooldown.next_wait_started.wait(), timeout=1)
        stop.set()
        await asyncio.wait_for(task, timeout=0.2)
        assert not core_api.called.is_set()
        await _close_collector(collector)

    asyncio.run(exercise())


async def _exercise_cancelled_snapshot(
    tmp_path: Path,
    worker_error: BaseException,
    *,
    expected_status: int | None,
    expected_retry_at_utc_ns: int | None,
) -> None:
    core_api = BlockingSnapshotApi(worker_error)
    side_api = CountingSideApi()
    collector = _collector(tmp_path, rest_api=core_api, side_rest_api=side_api)
    cooldown = ObservedCooldown()
    request_lock = LockProbe()
    request_lock.armed = True
    _use_gate(collector, cooldown, request_lock)
    cooldown.lock_to_observe = request_lock
    poller = _side_poller(collector)
    stop = asyncio.Event()
    core_task = asyncio.create_task(collector._capture_snapshot(stop))
    assert await asyncio.to_thread(core_api.started.wait, 1)

    core_task.cancel()
    poller._active_stop = stop
    side_task = asyncio.create_task(poller._request())
    await asyncio.wait_for(request_lock.second_acquire_started.wait(), timeout=1)
    cooldown.arm_next_wait()
    core_api.release.set()

    if expected_status is not None:
        await asyncio.wait_for(cooldown.install_finished.wait(), timeout=1)
        assert cooldown.status == expected_status
        assert cooldown.retry_at_utc_ns == expected_retry_at_utc_ns
        assert cooldown.install_lock_was_locked is True
        await asyncio.wait_for(cooldown.next_wait_started.wait(), timeout=1)
        assert not side_api.called.is_set()
    else:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(core_task, timeout=1)
        assert cooldown.status is None

    if expected_status is not None:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(core_task, timeout=1)
    stop.set()
    if expected_status is not None:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(side_task, timeout=1)
    else:
        await asyncio.wait_for(side_task, timeout=1)
    await _close_collector(collector, poller)


def test_cancelled_core_typed_429_installs_shared_gate_before_lock_release(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _exercise_cancelled_snapshot(
            tmp_path,
            TooManyRequestsError(status_code=429),
            expected_status=429,
            expected_retry_at_utc_ns=BASE_UTC_NS + 60 * 1_000_000_000,
        )
    )


def test_cancelled_core_typed_418_installs_shared_gate_before_lock_release(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _exercise_cancelled_snapshot(
            tmp_path,
            RateLimitBanError(status_code=418),
            expected_status=418,
            expected_retry_at_utc_ns=BASE_UTC_NS + 24 * 60 * 60 * 1_000_000_000,
        )
    )


@pytest.mark.parametrize("status", [418, 429])
def test_cancelled_core_http_rate_limit_preserves_known_timing(
    tmp_path: Path, status: int
) -> None:
    retry_at_utc_ns = BASE_UTC_NS + 42 * 1_000_000_000
    asyncio.run(
        _exercise_cancelled_snapshot(
            tmp_path,
            UsdMSnapshotHttpError(
                status=status,
                headers={"retry-after": "42"},
                retry_after_seconds=42.0,
                retry_at_utc_ns=retry_at_utc_ns,
            ),
            expected_status=status,
            expected_retry_at_utc_ns=retry_at_utc_ns,
        )
    )


def test_cancelled_core_non_rate_limit_failure_does_not_install_cooldown(
    tmp_path: Path,
) -> None:
    asyncio.run(
        _exercise_cancelled_snapshot(
            tmp_path,
            RuntimeError("ordinary snapshot failure"),
            expected_status=None,
            expected_retry_at_utc_ns=None,
        )
    )


def test_second_cancellation_does_not_orphan_snapshot_worker(tmp_path: Path) -> None:
    async def exercise() -> None:
        core_api = BlockingSnapshotApi(RuntimeError("ordinary snapshot failure"))
        collector = _collector(tmp_path, rest_api=core_api, with_side_data=False)
        cooldown = ObservedCooldown()
        request_lock = LockProbe()
        request_lock.armed = True
        _use_gate(collector, cooldown, request_lock)
        cleanup_started = asyncio.Event()
        collector_any = cast(Any, collector)
        original = collector_any._await_snapshot_worker_outcome

        async def wrapped(request_task: Any) -> Any:
            cleanup_started.set()
            return await original(request_task)

        collector_any._await_snapshot_worker_outcome = wrapped
        task = asyncio.create_task(collector._capture_snapshot(asyncio.Event()))
        assert await asyncio.to_thread(core_api.started.wait, 1)
        task.cancel()
        await asyncio.wait_for(cleanup_started.wait(), timeout=1)
        task.cancel()
        assert not task.done()
        core_api.release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert cooldown.status is None
        await _close_collector(collector)

    asyncio.run(exercise())


def test_shared_cooldown_success_observation_never_clears_future_deadline() -> None:
    cooldown = UsdMRestCooldown(
        utc_clock_ns=lambda: BASE_UTC_NS,
        monotonic_clock=lambda: BASE_MONOTONIC,
    )
    cooldown.install(status=418)
    deadline = cooldown.retry_at_utc_ns
    cooldown.observe_success()
    assert cooldown.status == 418
    assert cooldown.retry_at_utc_ns == deadline
