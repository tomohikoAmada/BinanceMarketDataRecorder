"""Process-shared IP-weight pacing and ban containment for public Spot REST."""

from __future__ import annotations

import asyncio
import email.utils
import random
import re
import time
import weakref
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

SPOT_WEIGHT_PER_MINUTE_BUDGET = 6_000
_BAN_UNTIL_PATTERN = re.compile(r"\buntil\s+(\d{13})\b", re.IGNORECASE)


def depth_request_weight(limit: int) -> int:
    """Return the currently documented IP request weight for ``GET /api/v3/depth``."""

    if not 1 <= limit <= 5_000:
        raise ValueError("Spot depth snapshot limit must be between 1 and 5000")
    if limit <= 100:
        return 5
    if limit <= 500:
        return 25
    if limit <= 1_000:
        return 50
    return 250


@dataclass(frozen=True, slots=True)
class RateLimitState:
    blocked_until_utc_ns: int | None
    ban_until_utc_ns: int | None
    last_status: int | None
    last_headers: dict[str, str]
    last_limit: int | None
    last_weight: int | None


class SpotRateLimitBlocked(RuntimeError):
    """A 429 or 418 response established a mandatory no-request interval."""

    def __init__(
        self,
        *,
        status: int,
        retry_at_utc_ns: int,
        headers: Mapping[str, str],
        detail: str,
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.retry_at_utc_ns = retry_at_utc_ns
        self.headers = dict(headers)


class SpotIpRateLimiter:
    """Serialize and pace all public Spot REST work sharing one process IP."""

    def __init__(
        self,
        *,
        weight_budget_per_minute: int = SPOT_WEIGHT_PER_MINUTE_BUDGET,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if weight_budget_per_minute < 1:
            raise ValueError("Spot weight budget must be positive")
        self.weight_budget_per_minute = weight_budget_per_minute
        self._utc_clock_ns = utc_clock_ns
        self._monotonic_clock = monotonic_clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()
        self._next_request_monotonic = 0.0
        self._blocked_until_monotonic = 0.0
        self._state = RateLimitState(None, None, None, {}, None, None)

    async def acquire(self, *, limit: int) -> int:
        """Wait for both the current ban gate and conservative IP-weight pacing."""

        weight = depth_request_weight(limit)
        while True:
            async with self._lock:
                now = self._monotonic_clock()
                ready_at = max(
                    now,
                    self._next_request_monotonic,
                    self._blocked_until_monotonic,
                )
                delay = ready_at - now
                if delay <= 0:
                    self._next_request_monotonic = (
                        now + (60.0 * weight / self.weight_budget_per_minute)
                    )
                    self._state = RateLimitState(
                        self._state.blocked_until_utc_ns,
                        self._state.ban_until_utc_ns,
                        self._state.last_status,
                        dict(self._state.last_headers),
                        limit,
                        weight,
                    )
                    return weight
            if self._sleep is None:
                await asyncio.sleep(delay)
            else:
                await self._sleep(delay)

    @asynccontextmanager
    async def request_slot(self) -> AsyncIterator[None]:
        """Permit at most one process-local Spot REST request on the wire."""

        async with self._request_lock:
            yield

    async def observe_success(
        self,
        *,
        limit: int,
        headers: Mapping[str, str],
    ) -> None:
        async with self._lock:
            now_utc_ns = self._utc_clock_ns()
            blocked_until = self._state.blocked_until_utc_ns
            still_blocked = blocked_until is not None and now_utc_ns < blocked_until
            self._state = RateLimitState(
                blocked_until if still_blocked else None,
                self._state.ban_until_utc_ns if still_blocked else None,
                200,
                _normalized_headers(headers),
                limit,
                depth_request_weight(limit),
            )

    async def observe_rejection(
        self,
        *,
        status: int,
        limit: int,
        headers: Mapping[str, str],
        body_text: str,
    ) -> SpotRateLimitBlocked:
        if status not in {418, 429}:
            raise ValueError("only HTTP 418/429 establish a Spot REST block")
        normalized = _normalized_headers(headers)
        now_utc_ns = self._utc_clock_ns()
        now_monotonic = self._monotonic_clock()
        retry_at_utc_ns = _retry_at_utc_ns(
            status=status,
            headers=normalized,
            body_text=body_text,
            now_utc_ns=now_utc_ns,
        )
        blocked_seconds = max(0.0, (retry_at_utc_ns - now_utc_ns) / 1_000_000_000)
        async with self._lock:
            self._blocked_until_monotonic = max(
                self._blocked_until_monotonic,
                now_monotonic + blocked_seconds,
            )
            self._state = RateLimitState(
                retry_at_utc_ns,
                retry_at_utc_ns if status == 418 else self._state.ban_until_utc_ns,
                status,
                normalized,
                limit,
                depth_request_weight(limit),
            )
        return SpotRateLimitBlocked(
            status=status,
            retry_at_utc_ns=retry_at_utc_ns,
            headers=normalized,
            detail=(
                f"Spot REST HTTP {status}; requests blocked until "
                f"{datetime.fromtimestamp(retry_at_utc_ns / 1e9, tz=UTC).isoformat()}"
            ),
        )

    def state(self) -> RateLimitState:
        return self._state


@dataclass(frozen=True, slots=True)
class FullJitterBackoff:
    initial_seconds: float = 1.0
    maximum_seconds: float = 60.0

    def delay(self, failures: int, *, random_value: float | None = None) -> float:
        if failures < 1:
            raise ValueError("failures must be positive")
        if self.initial_seconds <= 0 or self.maximum_seconds < self.initial_seconds:
            raise ValueError("invalid backoff bounds")
        ceiling = min(
            self.maximum_seconds,
            self.initial_seconds * (2.0 ** (failures - 1)),
        )
        sample = random.random() if random_value is None else random_value
        if not 0 <= sample <= 1:
            raise ValueError("random value must be between zero and one")
        return float(ceiling * sample)


_SHARED_LIMITERS: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, SpotIpRateLimiter
] = weakref.WeakKeyDictionary()


def shared_spot_ip_rate_limiter() -> SpotIpRateLimiter:
    """Return the one Spot IP limiter for the current event loop/process."""

    loop = asyncio.get_running_loop()
    limiter = _SHARED_LIMITERS.get(loop)
    if limiter is None:
        limiter = SpotIpRateLimiter()
        _SHARED_LIMITERS[loop] = limiter
    return limiter


def _normalized_headers(headers: Mapping[str, object]) -> dict[str, str]:
    return {str(name).lower(): str(value) for name, value in headers.items()}


def _retry_at_utc_ns(
    *,
    status: int,
    headers: Mapping[str, str],
    body_text: str,
    now_utc_ns: int,
) -> int:
    retry_after = headers.get("retry-after")
    if retry_after:
        stripped = retry_after.strip()
        try:
            seconds = float(stripped)
        except ValueError:
            parsed = email.utils.parsedate_to_datetime(stripped)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            return max(now_utc_ns, int(parsed.timestamp() * 1_000_000_000))
        return now_utc_ns + max(0, int(seconds * 1_000_000_000))
    match = _BAN_UNTIL_PATTERN.search(body_text)
    if status == 418 and match is not None:
        return max(now_utc_ns, int(match.group(1)) * 1_000_000)
    # Missing Retry-After is itself bad evidence. Fail closed instead of issuing
    # an immediate probe: one minute for 429, one day for an unknown 418 ban.
    fallback_seconds = 60 if status == 429 else 24 * 60 * 60
    return now_utc_ns + fallback_seconds * 1_000_000_000
