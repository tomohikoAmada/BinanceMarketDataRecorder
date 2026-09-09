from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from typing import Literal

import pytest
from binance_common.errors import TooManyRequestsError

from binance_market_data_recorder.binance.spot.rate_limit import SpotIpRateLimiter
from binance_market_data_recorder.binance.spot.rest import (
    DepthResponse,
    SpotRateLimitBlocked,
    SpotSnapshotRequester,
)
from binance_market_data_recorder.collector import spot_side_data
from binance_market_data_recorder.collector.spot_side_data import SpotExchangeInfoPoller
from binance_market_data_recorder.collector.usdm_side_data import (
    SideDataStats,
    UsdMRestCooldown,
)


@dataclass
class _Model:
    last_update_id: int = 42

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": self.last_update_id, "bids": [], "asks": []}


@dataclass
class _Response:
    status: int = 200
    headers: dict[str, object] = field(
        default_factory=lambda: {"X-MBX-USED-WEIGHT-1M": "50"}
    )
    raw_body: bytes = b'{"lastUpdateId":42,"bids":[],"asks":[]}'
    model: _Model = field(default_factory=_Model)

    def data(self) -> _Model:
        return self.model


class _PreFixRaceApi:
    def __init__(self) -> None:
        self.a_started = threading.Event()
        self.a_release = threading.Event()
        self.b_wire_started = threading.Event()

    def depth(self, symbol: str, limit: int) -> DepthResponse:
        assert limit == 1000
        if symbol == "BTCUSDT":
            self.a_started.set()
            if not self.a_release.wait(timeout=2):
                raise AssertionError("test did not release owner A")
            return _Response(
                status=429,
                headers={"Retry-After": "60"},
                raw_body=b"rate limited",
            )
        assert symbol == "ETHUSDT"
        self.b_wire_started.set()
        return _Response()


class _FiniteSpotApi:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def depth(self, symbol: str, limit: int) -> DepthResponse:
        assert limit == 1000
        self.calls.append(symbol)
        return _Response()


class _QueueObservedLock(asyncio.Lock):
    def __init__(self) -> None:
        super().__init__()
        self.waiter_enqueued = asyncio.Event()

    async def acquire(self) -> Literal[True]:
        if self.locked():
            self.waiter_enqueued.set()
        return await super().acquire()


class _DelayedSpotRejectionLimiter(SpotIpRateLimiter):
    def __init__(self, clock: _FakeClock) -> None:
        super().__init__(
            weight_budget_per_minute=10**30,
            utc_clock_ns=lambda: clock.utc_ns,
            monotonic_clock=lambda: clock.monotonic,
            sleep=clock.sleep,
        )
        self.rejection_known = asyncio.Event()
        self.allow_install = asyncio.Event()
        self.install_while_slot_owned: bool | None = None

    async def observe_rejection(self, **kwargs: object) -> SpotRateLimitBlocked:
        self.install_while_slot_owned = self._request_lock.locked()
        self.rejection_known.set()
        await self.allow_install.wait()
        return await super().observe_rejection(**kwargs)  # type: ignore[arg-type]


class _FakeClock:
    def __init__(self) -> None:
        self.monotonic = 100.0
        self.utc_ns = 1_700_000_000_000_000_000
        self.sleeps: list[float] = []

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        self.monotonic += delay
        self.utc_ns += int(delay * 1_000_000_000)


def test_spot_rejection_is_installed_before_queued_wire_owner() -> None:
    """F10-B: a known rejection cannot leak to the next wire owner.

    The same deterministic trace reproduced the pre-fix crossover on the
    frozen MS3-A main.  After the fix, A owns the request slot while its 429
    block is installed, and B remains off-wire until the fake deadline.
    """

    async def exercise() -> None:
        api = _PreFixRaceApi()
        clock = _FakeClock()
        limiter = _DelayedSpotRejectionLimiter(clock)
        request_lock = _QueueObservedLock()
        limiter._request_lock = request_lock
        requester_a = SpotSnapshotRequester(rest_api=api, rate_limiter=limiter)
        requester_b = SpotSnapshotRequester(rest_api=api, rate_limiter=limiter)

        owner = asyncio.create_task(
            requester_a.capture(
                symbol="BTCUSDT",
                collector_instance_id="a",
                collector_version="test",
                limit=1000,
                timeout_ms=1000,
            )
        )
        assert await asyncio.to_thread(api.a_started.wait, 1)
        queued = asyncio.create_task(
            requester_b.capture(
                symbol="ETHUSDT",
                collector_instance_id="b",
                collector_version="test",
                limit=1000,
                timeout_ms=1000,
            )
        )
        await asyncio.wait_for(request_lock.waiter_enqueued.wait(), timeout=1)

        api.a_release.set()
        await asyncio.wait_for(limiter.rejection_known.wait(), timeout=1)

        assert limiter.install_while_slot_owned is True
        assert not api.b_wire_started.is_set()
        assert not limiter.allow_install.is_set()

        limiter.allow_install.set()
        await asyncio.wait_for(
            asyncio.to_thread(api.b_wire_started.wait, 1), timeout=1
        )
        owner_result, queued_result = await asyncio.gather(
            owner, queued, return_exceptions=True
        )
        assert isinstance(owner_result, SpotRateLimitBlocked)
        assert not isinstance(queued_result, BaseException)
        assert clock.sleeps == [60.0]

    asyncio.run(exercise())


def test_spot_request_slot_rechecks_an_installed_block_without_weight_charge() -> None:
    """F10-A: a waiter admitted before a ban performs a block-only recheck."""

    async def exercise() -> None:
        clock = _FakeClock()
        sleep_started = asyncio.Event()
        release_sleep = asyncio.Event()

        async def controlled_sleep(delay: float) -> None:
            clock.sleeps.append(delay)
            sleep_started.set()
            await release_sleep.wait()
            clock.monotonic += delay
            clock.utc_ns += int(delay * 1_000_000_000)

        limiter = SpotIpRateLimiter(
            weight_budget_per_minute=10**30,
            utc_clock_ns=lambda: clock.utc_ns,
            monotonic_clock=lambda: clock.monotonic,
            sleep=controlled_sleep,
        )
        await limiter.observe_rejection(
            status=429,
            limit=1000,
            headers={"Retry-After": "5"},
            body_text="rate limited",
        )
        wire = False

        async def wire_owner() -> None:
            nonlocal wire
            async with limiter.request_slot():
                wire = True

        task = asyncio.create_task(wire_owner())
        await asyncio.wait_for(sleep_started.wait(), timeout=1)
        assert not wire
        assert limiter._next_request_monotonic == 0.0
        release_sleep.set()
        await asyncio.wait_for(task, timeout=1)
        assert wire
        assert clock.sleeps == [5.0]

    asyncio.run(exercise())


def test_spot_request_slot_cancellation_releases_fifo_slot() -> None:
    async def exercise() -> None:
        clock = _FakeClock()
        sleep_started = asyncio.Event()

        async def blocked_sleep(_delay: float) -> None:
            sleep_started.set()
            await asyncio.Event().wait()

        limiter = SpotIpRateLimiter(
            utc_clock_ns=lambda: clock.utc_ns,
            monotonic_clock=lambda: clock.monotonic,
            sleep=blocked_sleep,
        )
        await limiter.observe_rejection(
            status=429,
            limit=1000,
            headers={"Retry-After": "5"},
            body_text="rate limited",
        )
        waiting = asyncio.create_task(limiter.request_slot().__aenter__())
        await asyncio.wait_for(sleep_started.wait(), timeout=1)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert not limiter._request_lock.locked()

        clock.monotonic = 105.0
        async with limiter.request_slot():
            pass

    asyncio.run(exercise())


def test_spot_finite_cohort_reaches_request_slot_without_admission_starvation() -> None:
    async def exercise() -> None:
        clock = _FakeClock()
        api = _FiniteSpotApi()
        limiter = SpotIpRateLimiter(
            weight_budget_per_minute=10**30,
            utc_clock_ns=lambda: clock.utc_ns,
            monotonic_clock=lambda: clock.monotonic,
            sleep=clock.sleep,
        )
        symbols = (
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "XRPUSDT",
            "DOGEUSDT",
            "SUIUSDT",
            "LINKUSDT",
        )
        requester = SpotSnapshotRequester(rest_api=api, rate_limiter=limiter)
        await asyncio.gather(
            *(
                requester.capture(
                    symbol=symbol,
                    collector_instance_id=f"collector-{symbol}",
                    collector_version="test",
                    limit=1000,
                    timeout_ms=1000,
                )
                for symbol in symbols
            )
        )
        assert set(api.calls) == set(symbols)
        assert len(api.calls) == len(symbols)

    asyncio.run(exercise())


class _ExchangeInfoSpool:
    symbol = "BTCUSDT"

    def close_and_seal(self) -> None:
        return None


class _ObservedWeightedRejectionLimiter(SpotIpRateLimiter):
    def __init__(self, stop: asyncio.Event) -> None:
        super().__init__(weight_budget_per_minute=10**30)
        self.stop = stop
        self.install_while_slot_owned: bool | None = None

    async def observe_weight_rejection(self, **kwargs: object) -> SpotRateLimitBlocked:
        self.install_while_slot_owned = self._request_lock.locked()
        result = await super().observe_weight_rejection(**kwargs)  # type: ignore[arg-type]
        self.stop.set()
        return result


def test_spot_exchange_info_installs_weight_rejection_while_slot_is_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        limiter = _ObservedWeightedRejectionLimiter(stop)
        monkeypatch.setattr(
            spot_side_data, "shared_spot_ip_rate_limiter", lambda: limiter
        )

        def reject(**_kwargs: object) -> object:
            raise TooManyRequestsError(status_code=429)

        monkeypatch.setattr(spot_side_data, "capture_spot_exchange_info", reject)
        stats = SideDataStats(enabled=True)
        poller = SpotExchangeInfoPoller(
            interval_seconds=3600,
            spool=_ExchangeInfoSpool(),  # type: ignore[arg-type]
            stats=stats,
            collector_instance_id="test",
            collector_version="test",
            rest_api=None,
            timeout_ms=1000,
            logger=__import__("logging").getLogger("test.ms3b.spot"),
        )
        await asyncio.wait_for(poller.run(stop), timeout=1)
        assert limiter.install_while_slot_owned is True
        assert stats.failures == 1

    asyncio.run(exercise())


@dataclass(slots=True)
class _RestTrace:
    sequence_no: int
    attempt_id: str
    market: str
    scope: str
    product: str
    request_kind: str
    attempt: int
    phase: str
    eligible_seq: int | None = None
    lock_enqueue_seq: int | None = None
    holder_ahead: int = 0
    waiters_ahead: int = 0
    grants_at_enqueue: int = 0
    grant_seq: int | None = None
    competing_grants_before_grant: int = 0
    starvation_bound: int = 0
    wire_start_seq: int | None = None
    wire_end_seq: int | None = None
    result: str | None = None
    cooldown_or_block_deadline: float | None = None
    shared_gate_identity: tuple[int, int] | None = None
    logical_time: float = 0.0


class _DeterministicUsdMCooldown(UsdMRestCooldown):
    """Test-only cooldown waiter that advances only on an explicit command."""

    def __init__(self, clock: _FakeClock) -> None:
        super().__init__(
            utc_clock_ns=lambda: clock.utc_ns,
            monotonic_clock=lambda: clock.monotonic,
        )
        self.clock = clock
        self.advance_event = asyncio.Event()
        self.waiting_event = asyncio.Event()

    async def wait(self, stop: asyncio.Event) -> None:
        while self._until_monotonic > self._monotonic_clock():
            self.waiting_event.set()
            stop_task = asyncio.create_task(stop.wait())
            advance_task = asyncio.create_task(self.advance_event.wait())
            try:
                done, _ = await asyncio.wait(
                    (stop_task, advance_task),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done and stop.is_set():
                    return
                if advance_task in done:
                    self.advance_event.clear()
            finally:
                for task in (stop_task, advance_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(stop_task, advance_task, return_exceptions=True)

    def advance(self, seconds: float) -> None:
        self.clock.monotonic += seconds
        self.clock.utc_ns += int(seconds * 1_000_000_000)
        self.advance_event.set()


class _UsdMRestTraceHarness:
    """Small deterministic model around the production shared gate pair."""

    def __init__(self, clock: _FakeClock | None = None) -> None:
        self.clock = clock or _FakeClock()
        self.lock = asyncio.Lock()
        self.cooldown = _DeterministicUsdMCooldown(self.clock)
        self.stop = asyncio.Event()
        self.trace: list[_RestTrace] = []
        self._sequence = 0
        self._pending = 0
        self._grant_count = 0
        self.wire_active = 0
        self.max_wire = 0
        self.slow_release = asyncio.Event()
        self._phase_events: dict[tuple[str, str], asyncio.Event] = {}

    def _event_for(self, attempt_id: str, phase: str) -> asyncio.Event:
        return self._phase_events.setdefault((attempt_id, phase), asyncio.Event())

    def _record(
        self,
        *,
        attempt_id: str,
        market: str,
        scope: str,
        product: str,
        request_kind: str,
        attempt: int,
        phase: str,
        **values: object,
    ) -> _RestTrace:
        self._sequence += 1
        item = _RestTrace(
            sequence_no=self._sequence,
            attempt_id=attempt_id,
            market=market,
            scope=scope,
            product=product,
            request_kind=request_kind,
            attempt=attempt,
            phase=phase,
            shared_gate_identity=(id(self.lock), id(self.cooldown)),
            logical_time=self.clock.monotonic,
        )
        for name, value in values.items():
            setattr(item, name, value)
        self.trace.append(item)
        self._event_for(attempt_id, phase).set()
        return item

    async def wait_phase(self, attempt_id: str, phase: str) -> None:
        await self._event_for(attempt_id, phase).wait()

    async def request(
        self,
        *,
        attempt_id: str,
        product: str,
        request_kind: str,
        attempt: int = 1,
        scope: str = "product",
        outcome: str = "SUCCESS",
        retry_after_seconds: float | None = None,
    ) -> str:
        market = "um_perpetual"
        eligible = self._record(
            attempt_id=attempt_id,
            market=market,
            scope=scope,
            product=product,
            request_kind=request_kind,
            attempt=attempt,
            phase="ELIGIBLE",
        )
        self._record(
            attempt_id=attempt_id,
            market=market,
            scope=scope,
            product=product,
            request_kind=request_kind,
            attempt=attempt,
            phase="COOLDOWN_WAIT",
            eligible_seq=eligible.sequence_no,
        )
        try:
            await self.cooldown.wait(self.stop)
            if self.stop.is_set():
                self._record(
                    attempt_id=attempt_id,
                    market=market,
                    scope=scope,
                    product=product,
                    request_kind=request_kind,
                    attempt=attempt,
                    phase="STOPPED",
                    eligible_seq=eligible.sequence_no,
                )
                return "STOPPED"
            holder_ahead = 1 if self.lock.locked() else 0
            waiters_ahead = self._pending
            self._pending += 1
            enqueued = self._record(
                attempt_id=attempt_id,
                market=market,
                scope=scope,
                product=product,
                request_kind=request_kind,
                attempt=attempt,
                phase="LOCK_ENQUEUE",
                eligible_seq=eligible.sequence_no,
                holder_ahead=holder_ahead,
                waiters_ahead=waiters_ahead,
                grants_at_enqueue=self._grant_count,
            )
            try:
                await self.lock.acquire()
            except asyncio.CancelledError:
                self._pending -= 1
                self._record(
                    attempt_id=attempt_id,
                    market=market,
                    scope=scope,
                    product=product,
                    request_kind=request_kind,
                    attempt=attempt,
                    phase="CANCELLED",
                    eligible_seq=eligible.sequence_no,
                    lock_enqueue_seq=enqueued.sequence_no,
                )
                raise
            self._pending -= 1
            self._grant_count += 1
            competing = self._grant_count - enqueued.grants_at_enqueue - 1
            bound = holder_ahead + waiters_ahead
            granted = self._record(
                attempt_id=attempt_id,
                market=market,
                scope=scope,
                product=product,
                request_kind=request_kind,
                attempt=attempt,
                phase="LOCK_GRANTED",
                eligible_seq=eligible.sequence_no,
                lock_enqueue_seq=enqueued.sequence_no,
                holder_ahead=holder_ahead,
                waiters_ahead=waiters_ahead,
                grants_at_enqueue=enqueued.grants_at_enqueue,
                grant_seq=self._grant_count,
                competing_grants_before_grant=competing,
                starvation_bound=bound,
            )
            try:
                self._record(
                    attempt_id=attempt_id,
                    market=market,
                    scope=scope,
                    product=product,
                    request_kind=request_kind,
                    attempt=attempt,
                    phase="COOLDOWN_WAIT_INNER",
                    eligible_seq=eligible.sequence_no,
                    lock_enqueue_seq=enqueued.sequence_no,
                    grant_seq=granted.grant_seq,
                )
                await self.cooldown.wait(self.stop)
                if self.stop.is_set():
                    self._record(
                        attempt_id=attempt_id,
                        market=market,
                        scope=scope,
                        product=product,
                        request_kind=request_kind,
                        attempt=attempt,
                        phase="STOPPED",
                        eligible_seq=eligible.sequence_no,
                        lock_enqueue_seq=enqueued.sequence_no,
                        grant_seq=granted.grant_seq,
                    )
                    return "STOPPED"
                self.wire_active += 1
                self.max_wire = max(self.max_wire, self.wire_active)
                wire_start = self._record(
                    attempt_id=attempt_id,
                    market=market,
                    scope=scope,
                    product=product,
                    request_kind=request_kind,
                    attempt=attempt,
                    phase="WIRE_START",
                    eligible_seq=eligible.sequence_no,
                    lock_enqueue_seq=enqueued.sequence_no,
                    waiters_ahead=waiters_ahead,
                    grant_seq=granted.grant_seq,
                )
                if outcome.startswith("SLOW_"):
                    await self.slow_release.wait()
                result = outcome.removeprefix("SLOW_")
                if result in {"HTTP_429", "HTTP_418"}:
                    status = int(result.split("_", 1)[1])
                    deadline, _utc, _reason = self.cooldown.install(
                        status=status,
                        retry_after_seconds=(
                            5.0 if retry_after_seconds is None else retry_after_seconds
                        ),
                    )
                    self._record(
                        attempt_id=attempt_id,
                        market=market,
                        scope=scope,
                        product=product,
                        request_kind=request_kind,
                        attempt=attempt,
                        phase="COOLDOWN_INSTALLED",
                        eligible_seq=eligible.sequence_no,
                        lock_enqueue_seq=enqueued.sequence_no,
                        grant_seq=granted.grant_seq,
                        cooldown_or_block_deadline=deadline,
                    )
                wire_end = self._record(
                    attempt_id=attempt_id,
                    market=market,
                    scope=scope,
                    product=product,
                    request_kind=request_kind,
                    attempt=attempt,
                    phase="WIRE_END",
                    eligible_seq=eligible.sequence_no,
                    lock_enqueue_seq=enqueued.sequence_no,
                    grant_seq=granted.grant_seq,
                    wire_start_seq=wire_start.sequence_no,
                    result=result,
                )
                wire_end.wire_end_seq = wire_end.sequence_no
                return result
            finally:
                self.wire_active -= 1 if self.wire_active else 0
                self.lock.release()
        except asyncio.CancelledError:
            raise

    def grant_order(self) -> list[str]:
        return [item.attempt_id for item in self.trace if item.phase == "LOCK_GRANTED"]

    def starvation_count(self) -> int:
        return sum(
            item.competing_grants_before_grant > item.starvation_bound
            for item in self.trace
            if item.phase == "LOCK_GRANTED"
        )


USDM_PROFILE_D_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "SUIUSDT",
    "LINKUSDT",
)


def test_f1_simultaneous_usdm_core_bootstrap_is_serial_and_fair() -> None:
    async def exercise() -> None:
        harness = _UsdMRestTraceHarness()
        barrier = asyncio.Barrier(len(USDM_PROFILE_D_SYMBOLS))

        async def start_core(symbol: str) -> str:
            await barrier.wait()
            return await harness.request(
                attempt_id=f"core:{symbol}",
                product=symbol,
                request_kind="depth_snapshot",
            )

        results = await asyncio.gather(
            *(start_core(symbol) for symbol in USDM_PROFILE_D_SYMBOLS)
        )
        assert results == ["SUCCESS"] * 7
        assert harness.max_wire == 1
        assert harness.starvation_count() == 0
        assert len(
            {
                item.shared_gate_identity
                for item in harness.trace
                if item.phase == "LOCK_GRANTED"
            }
        ) == 1
        assert len(
            {
                item.product
                for item in harness.trace
                if item.phase == "WIRE_START"
            }
        ) == 7

    asyncio.run(exercise())


def test_f2_queued_core_is_not_overtaken_by_later_side_reacquisition() -> None:
    async def exercise() -> None:
        harness = _UsdMRestTraceHarness()
        holder = asyncio.create_task(
            harness.request(
                attempt_id="side:holder",
                product="BTCUSDT",
                request_kind="open_interest",
                outcome="SLOW_SUCCESS",
            )
        )
        await harness.wait_phase("side:holder", "WIRE_START")
        core = asyncio.create_task(
            harness.request(
                attempt_id="core:queued",
                product="ETHUSDT",
                request_kind="depth_snapshot",
            )
        )
        await harness.wait_phase("core:queued", "LOCK_ENQUEUE")
        later_side = asyncio.create_task(
            harness.request(
                attempt_id="side:later",
                product="SOLUSDT",
                request_kind="funding_info",
            )
        )
        await harness.wait_phase("side:later", "LOCK_ENQUEUE")
        harness.slow_release.set()
        assert await holder == "SUCCESS"
        assert await core == "SUCCESS"
        assert await later_side == "SUCCESS"
        assert harness.grant_order() == [
            "side:holder",
            "core:queued",
            "side:later",
        ]
        assert harness.starvation_count() == 0

    asyncio.run(exercise())


def test_f3_global_and_product_routes_have_one_usdm_owner_pair() -> None:
    async def exercise() -> None:
        harness = _UsdMRestTraceHarness()
        requests = [
            harness.request(
                attempt_id="global:funding_info",
                product="GLOBAL",
                scope="global",
                request_kind="funding_info",
            ),
            harness.request(
                attempt_id="global:exchange_info",
                product="GLOBAL",
                scope="global",
                request_kind="exchange_info",
            ),
            harness.request(
                attempt_id="product:BTCUSDT",
                product="BTCUSDT",
                request_kind="open_interest",
            ),
            harness.request(
                attempt_id="core:SOLUSDT",
                product="SOLUSDT",
                request_kind="depth_snapshot",
            ),
        ]
        assert await asyncio.gather(*requests) == ["SUCCESS"] * 4
        identities = {
            item.shared_gate_identity
            for item in harness.trace
            if item.phase == "LOCK_GRANTED"
        }
        assert len(identities) == 1
        assert sum(
            item.scope == "global" and item.phase == "WIRE_START"
            for item in harness.trace
        ) == 2
        assert harness.max_wire == 1

    asyncio.run(exercise())


def test_f4_five_minute_pages_release_lock_between_reacquisitions() -> None:
    async def exercise() -> None:
        harness = _UsdMRestTraceHarness()
        cursors: dict[str, int | None] = {"BTCUSDT": None, "ETHUSDT": None}

        async def catch_up() -> None:
            for page in range(3):
                result = await harness.request(
                    attempt_id=f"catchup:{page}",
                    product="BTCUSDT",
                    request_kind="five_minute_page",
                    attempt=page + 1,
                    outcome="SLOW_SUCCESS" if page == 0 else "SUCCESS",
                )
                assert result == "SUCCESS"
                cursors["BTCUSDT"] = page

        first_page = asyncio.create_task(catch_up())
        await harness.wait_phase("catchup:0", "WIRE_START")
        sibling = asyncio.create_task(
            harness.request(
                attempt_id="side:older-waiter",
                product="ETHUSDT",
                request_kind="open_interest",
            )
        )
        await harness.wait_phase("side:older-waiter", "LOCK_ENQUEUE")
        harness.slow_release.set()
        await first_page
        assert await sibling == "SUCCESS"
        assert cursors == {"BTCUSDT": 2, "ETHUSDT": None}
        assert harness.grant_order()[:3] == [
            "catchup:0",
            "side:older-waiter",
            "catchup:1",
        ]
        assert harness.starvation_count() == 0

    asyncio.run(exercise())


def test_f5_repeated_core_retry_releases_lock_for_healthy_sibling() -> None:
    async def exercise() -> None:
        harness = _UsdMRestTraceHarness()

        async def retrying_core() -> None:
            for attempt, outcome in enumerate(
                ("SLOW_HTTP_5XX", "TRANSPORT_ERROR", "SUCCESS"), start=1
            ):
                await harness.request(
                    attempt_id=f"core:retry:{attempt}",
                    product="BTCUSDT",
                    request_kind="depth_snapshot",
                    attempt=attempt,
                    outcome=outcome,
                )

        retry = asyncio.create_task(retrying_core())
        await harness.wait_phase("core:retry:1", "WIRE_START")
        sibling = asyncio.create_task(
            harness.request(
                attempt_id="core:healthy-sibling",
                product="ETHUSDT",
                request_kind="depth_snapshot",
            )
        )
        await harness.wait_phase("core:healthy-sibling", "LOCK_ENQUEUE")
        harness.slow_release.set()
        await retry
        assert await sibling == "SUCCESS"
        assert "core:healthy-sibling" in harness.grant_order()
        assert harness.grant_order().index("core:healthy-sibling") < harness.grant_order().index(
            "core:retry:2"
        )
        assert harness.starvation_count() == 0

    asyncio.run(exercise())


def test_f6_usdm_429_blocks_wire_until_deadline_and_shorter_evidence_does_not_shorten() -> None:
    async def exercise() -> None:
        clock = _FakeClock()
        harness = _UsdMRestTraceHarness(clock)
        assert await harness.request(
            attempt_id="core:429",
            product="BTCUSDT",
            request_kind="depth_snapshot",
            outcome="HTTP_429",
            retry_after_seconds=5,
        ) == "HTTP_429"
        initial_deadline = harness.cooldown._until_monotonic
        harness.cooldown.install(status=429, retry_after_seconds=1)
        assert harness.cooldown._until_monotonic == initial_deadline
        waiting = asyncio.create_task(
            harness.request(
                attempt_id="side:blocked",
                product="ETHUSDT",
                request_kind="open_interest",
            )
        )
        await harness.wait_phase("side:blocked", "COOLDOWN_WAIT")
        await asyncio.wait_for(harness.cooldown.waiting_event.wait(), timeout=1)
        assert not any(
            item.attempt_id == "side:blocked" and item.phase == "WIRE_START"
            for item in harness.trace
        )
        harness.cooldown.advance(5)
        assert await waiting == "SUCCESS"
        assert harness.max_wire == 1

    asyncio.run(exercise())


def test_f7_usdm_418_fallback_escalates_one_two_three_days() -> None:
    clock = _FakeClock()
    clock.monotonic = 0.0
    cooldown = UsdMRestCooldown(
        utc_clock_ns=lambda: clock.utc_ns,
        monotonic_clock=lambda: clock.monotonic,
    )
    deadlines = [
        cooldown.install(status=418)[0],
        cooldown.install(status=418)[0],
        cooldown.install(status=418)[0],
    ]
    assert deadlines == [
        1 * 24 * 60 * 60,
        2 * 24 * 60 * 60,
        3 * 24 * 60 * 60,
    ]
    assert cooldown.install(status=418)[0] == deadlines[-1]


def test_f8_usdm_cancellation_does_not_leave_lock_or_cooldown_ownership() -> None:
    async def exercise() -> None:
        harness = _UsdMRestTraceHarness()
        await harness.lock.acquire()
        lock_waiter = asyncio.create_task(
            harness.request(
                attempt_id="cancel:lock",
                product="BTCUSDT",
                request_kind="depth_snapshot",
            )
        )
        await harness.wait_phase("cancel:lock", "LOCK_ENQUEUE")
        lock_waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await lock_waiter
        assert harness.lock.locked()
        harness.lock.release()

        harness.cooldown.install(status=429, retry_after_seconds=5)
        cooldown_waiter = asyncio.create_task(
            harness.request(
                attempt_id="cancel:outer-cooldown",
                product="ETHUSDT",
                request_kind="open_interest",
            )
        )
        await harness.wait_phase("cancel:outer-cooldown", "COOLDOWN_WAIT")
        cooldown_waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cooldown_waiter

        harness.cooldown.advance(5)
        await harness.lock.acquire()
        inner_waiter = asyncio.create_task(
            harness.request(
                attempt_id="cancel:inner-cooldown",
                product="SOLUSDT",
                request_kind="open_interest",
            )
        )
        await harness.wait_phase("cancel:inner-cooldown", "LOCK_ENQUEUE")
        harness.cooldown.install(status=429, retry_after_seconds=5)
        harness.lock.release()
        await harness.wait_phase("cancel:inner-cooldown", "COOLDOWN_WAIT_INNER")
        inner_waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await inner_waiter
        assert not harness.lock.locked()
        harness.cooldown.advance(5)
        assert await harness.request(
            attempt_id="after:cancel",
            product="LINKUSDT",
            request_kind="depth_snapshot",
        ) == "SUCCESS"
        assert not harness.lock.locked()

    asyncio.run(exercise())


def test_f9_usdm_stop_converges_cooldown_and_periodic_waiters_without_new_wire() -> None:
    async def exercise() -> None:
        harness = _UsdMRestTraceHarness()
        harness.cooldown.install(status=418, retry_after_seconds=100)
        blocked = asyncio.create_task(
            harness.request(
                attempt_id="stop:blocked",
                product="BTCUSDT",
                request_kind="depth_snapshot",
            )
        )
        await harness.wait_phase("stop:blocked", "COOLDOWN_WAIT")
        periodic = asyncio.create_task(harness.stop.wait())
        harness.stop.set()
        assert await blocked == "STOPPED"
        await periodic
        assert not any(
            item.phase == "WIRE_START" and item.attempt_id.startswith("stop:")
            for item in harness.trace
        )

    asyncio.run(exercise())
