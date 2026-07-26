from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from typing import ClassVar

import pytest

from binance_market_data_recorder.binance.spot.rate_limit import (
    FullJitterBackoff,
    SpotIpRateLimiter,
    SpotRateLimitBlocked,
    depth_request_weight,
)
from binance_market_data_recorder.binance.spot.rest import (
    DepthResponse,
    SpotSnapshotHttpError,
    SpotSnapshotRequester,
)


@dataclass
class Model:
    last_update_id: int = 42

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": self.last_update_id, "bids": [], "asks": []}


@dataclass
class Response:
    status: int = 200
    headers: dict[str, object] = field(
        default_factory=lambda: {"X-MBX-USED-WEIGHT-1M": "50"}
    )
    raw_body: bytes = b'{"lastUpdateId":42,"bids":[],"asks":[]}'
    model: Model = field(default_factory=Model)

    def data(self) -> Model:
        return self.model


class SequencedApi:
    def __init__(self, responses: list[Response]) -> None:
        self.responses = responses
        self.calls = 0

    def depth(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        selected = self.responses[self.calls]
        self.calls += 1
        return selected


class BlockingApi:
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "50"}

    def __init__(self) -> None:
        self.calls = 0
        self.started = threading.Event()
        self.release = threading.Event()

    def depth(self, symbol: str, limit: int) -> DepthResponse:
        self.calls += 1
        self.started.set()
        assert self.release.wait(timeout=2)
        return Response()


class ConcurrentApi:
    def __init__(self) -> None:
        self.calls = 0
        self.active = 0
        self.maximum_active = 0
        self.lock = threading.Lock()

    def depth(self, symbol: str, limit: int) -> DepthResponse:
        with self.lock:
            self.calls += 1
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        time.sleep(0.02)
        with self.lock:
            self.active -= 1
        return Response()


def _limiter() -> SpotIpRateLimiter:
    return SpotIpRateLimiter(weight_budget_per_minute=1_000_000_000)


def test_documented_depth_weights_are_explicit() -> None:
    assert [(limit, depth_request_weight(limit)) for limit in (100, 500, 1000, 5000)] == [
        (100, 5),
        (500, 25),
        (1000, 50),
        (5000, 250),
    ]


def test_full_jitter_backoff_has_a_bounded_zero_to_cap_range() -> None:
    backoff = FullJitterBackoff(initial_seconds=2, maximum_seconds=10)
    assert backoff.delay(1, random_value=0) == 0
    assert backoff.delay(2, random_value=0.5) == 2
    assert backoff.delay(10, random_value=1) == 10


def test_429_retry_after_blocks_and_then_recovers() -> None:
    async def exercise() -> None:
        utc_ns = 1_000_000_000
        monotonic = 1.0
        sleeps: list[float] = []

        async def sleep(delay: float) -> None:
            nonlocal utc_ns, monotonic
            sleeps.append(delay)
            monotonic += delay
            utc_ns += int(delay * 1_000_000_000)

        limiter = SpotIpRateLimiter(
            weight_budget_per_minute=1_000_000_000,
            utc_clock_ns=lambda: utc_ns,
            monotonic_clock=lambda: monotonic,
            sleep=sleep,
        )
        blocked = await limiter.observe_rejection(
            status=429,
            limit=1000,
            headers={"Retry-After": "7", "X-MBX-USED-WEIGHT-1M": "6000"},
            body_text="too many requests",
        )
        assert blocked.retry_at_utc_ns == 8_000_000_000
        await limiter.acquire(limit=1000)
        assert sleeps == [7.0]
        await limiter.observe_success(
            limit=1000,
            headers={"X-MBX-USED-WEIGHT-1M": "50"},
        )
        assert limiter.state().blocked_until_utc_ns is None

    asyncio.run(exercise())


def test_418_ban_timestamp_stops_all_spot_rest_callers() -> None:
    async def exercise() -> None:
        utc_ns = 1_700_000_000_000_000_000
        monotonic = 10.0
        sleeps: list[float] = []

        async def sleep(delay: float) -> None:
            nonlocal utc_ns, monotonic
            sleeps.append(delay)
            monotonic += delay
            utc_ns += int(delay * 1_000_000_000)

        limiter = SpotIpRateLimiter(
            utc_clock_ns=lambda: utc_ns,
            monotonic_clock=lambda: monotonic,
            sleep=sleep,
        )
        ban_ms = 1_700_000_030_000
        blocked = await limiter.observe_rejection(
            status=418,
            limit=1000,
            headers={"X-MBX-USED-WEIGHT-1M": "6000"},
            body_text=f"Way too much request weight used; IP banned until {ban_ms}.",
        )
        assert blocked.retry_at_utc_ns == ban_ms * 1_000_000
        await asyncio.gather(
            limiter.acquire(limit=100),
            limiter.acquire(limit=500),
        )
        assert sleeps
        assert sleeps[0] == 30.0
        assert limiter.state().ban_until_utc_ns == ban_ms * 1_000_000

    asyncio.run(exercise())


def test_snapshot_requester_deduplicates_concurrent_same_symbol_calls() -> None:
    async def exercise() -> None:
        api = SequencedApi([Response()])
        requester = SpotSnapshotRequester(rest_api=api, rate_limiter=_limiter())
        captures = await asyncio.gather(
            *(
                requester.capture(
                    collector_instance_id="collector",
                    collector_version="test",
                    limit=1000,
                    timeout_ms=1000,
                )
                for _ in range(8)
            )
        )
        assert api.calls == 1
        assert {capture.source_sequence["lastUpdateId"] for capture in captures} == {42}
        assert requester.inflight_count() == 0

    asyncio.run(exercise())


def test_snapshot_cancellation_reclaims_singleflight_worker() -> None:
    async def exercise() -> None:
        api = BlockingApi()
        requester = SpotSnapshotRequester(rest_api=api, rate_limiter=_limiter())
        task = asyncio.create_task(
            requester.capture(
                collector_instance_id="collector",
                collector_version="test",
                limit=1000,
                timeout_ms=1000,
            )
        )
        assert await asyncio.to_thread(api.started.wait, 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert requester.inflight_count() == 1
        api.release.set()
        await requester.wait_for_idle()
        await asyncio.sleep(0)
        assert requester.inflight_count() == 0
        assert api.calls == 1

    asyncio.run(exercise())


def test_requester_classifies_429_418_and_5xx_without_retry_loop() -> None:
    async def exercise() -> None:
        responses = [
            Response(status=429, headers={"Retry-After": "0"}, raw_body=b"rate"),
            Response(
                status=418,
                headers={"Retry-After": "0"},
                raw_body=b'{"msg":"banned"}',
            ),
            Response(status=503, raw_body=b"unavailable"),
        ]
        api = SequencedApi(responses)
        requester = SpotSnapshotRequester(rest_api=api, rate_limiter=_limiter())
        with pytest.raises(SpotRateLimitBlocked) as limited:
            await requester.capture(
                collector_instance_id="collector",
                collector_version="test",
                limit=1000,
                timeout_ms=1000,
            )
        assert limited.value.status == 429
        with pytest.raises(SpotRateLimitBlocked) as banned:
            await requester.capture(
                collector_instance_id="collector",
                collector_version="test",
                limit=1000,
                timeout_ms=1000,
            )
        assert banned.value.status == 418
        with pytest.raises(SpotSnapshotHttpError) as server:
            await requester.capture(
                collector_instance_id="collector",
                collector_version="test",
                limit=1000,
                timeout_ms=1000,
            )
        assert server.value.status == 503
        assert api.calls == 3

    asyncio.run(exercise())


def test_snapshot_success_provenance_contains_limit_weight_headers_and_exact_body() -> None:
    async def exercise() -> None:
        requester = SpotSnapshotRequester(
            rest_api=SequencedApi([Response()]),
            rate_limiter=_limiter(),
        )
        envelope = await requester.capture(
            collector_instance_id="collector",
            collector_version="test",
            limit=1000,
            timeout_ms=1000,
        )
        provenance = json.loads(envelope.raw_payload)
        assert provenance["request"]["limit"] == 1000
        assert provenance["request"]["request_weight"] == 50
        assert provenance["response"]["headers"]["x-mbx-used-weight-1m"] == "50"
        assert provenance["transport"]["raw_http_body_available"] is True
        assert "exact_rest_http_body_preserved" in envelope.capture_flags

    asyncio.run(exercise())


def test_shared_limiter_paces_distinct_callers_instead_of_busy_looping() -> None:
    async def exercise() -> None:
        monotonic = 0.0
        sleeps: list[float] = []

        async def sleep(delay: float) -> None:
            nonlocal monotonic
            sleeps.append(delay)
            monotonic += delay

        limiter = SpotIpRateLimiter(
            weight_budget_per_minute=100,
            monotonic_clock=lambda: monotonic,
            sleep=sleep,
        )
        await limiter.acquire(limit=100)
        await limiter.acquire(limit=100)
        assert sleeps == [3.0]

    asyncio.run(exercise())


def test_shared_limiter_allows_only_one_snapshot_request_on_the_wire() -> None:
    async def exercise() -> None:
        api = ConcurrentApi()
        limiter = _limiter()
        requesters = [
            SpotSnapshotRequester(rest_api=api, rate_limiter=limiter)
            for _ in range(2)
        ]
        captures = await asyncio.gather(
            *(
                requester.capture(
                    collector_instance_id=f"collector-{ordinal}",
                    collector_version="test",
                    limit=1000,
                    timeout_ms=1000,
                )
                for ordinal, requester in enumerate(requesters)
            )
        )
        assert api.calls == 2
        assert api.maximum_active == 1
        assert {capture.collector_instance_id for capture in captures} == {
            "collector-0",
            "collector-1",
        }

    asyncio.run(exercise())


def test_exchange_info_weight_shares_the_same_ban_and_pacing_state() -> None:
    async def exercise() -> None:
        limiter = SpotIpRateLimiter(weight_budget_per_minute=1_000_000_000)
        await limiter.acquire_weight(weight=20)
        await limiter.observe_success_weight(
            weight=20, headers={"X-MBX-USED-WEIGHT-1M": "70"}
        )
        assert limiter.state().last_limit is None
        assert limiter.state().last_weight == 20
        assert limiter.state().last_headers["x-mbx-used-weight-1m"] == "70"
        blocked = await limiter.observe_weight_rejection(
            status=429,
            weight=20,
            headers={"Retry-After": "1"},
            body_text="rate limited",
        )
        assert blocked.status == 429
        assert limiter.state().blocked_until_utc_ns is not None

    asyncio.run(exercise())
