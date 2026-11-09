"""Recorder's fixed share of a public IP also running one Gateway.

Rolling windows count every wire attempt, including failed requests. State is
process local and survives product reconnects, not process restarts. Deployment
must exclude overlapping instances and reserve the documented shares for other
IP users; see docs/runbooks/R3-shared-ip-budget.md.
"""

from __future__ import annotations

import asyncio
import time
import weakref
from collections import deque
from collections.abc import Callable, Mapping
from contextlib import suppress

# Gateway reserves 3000/600 weight per minute and 60 connections per five
# minutes in each market. The remainder is explicit shared-IP headroom.
RECORDER_WINDOWS = {
    "spot_weight": (2000, 60.0),
    "usdm_weight": (1200, 60.0),
    "usdm_funding": (400, 300.0),
    "usdm_statistics": (800, 300.0),
    "spot_connections": (180, 300.0),
    "usdm_connections": (180, 300.0),
}


class RequestBudget:
    """Small event-loop-owned rolling counters; no I/O or background tasks."""

    def __init__(
        self,
        windows: Mapping[str, tuple[int, float]] = RECORDER_WINDOWS,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.windows = dict(windows)
        self.clock = clock
        self._blocked_until: dict[str, float] = {}
        self._events: dict[str, deque[tuple[float, int]]] = {name: deque() for name in windows}

    def delay(self, charges: Mapping[str, int]) -> float:
        now = self.clock()
        delay = 0.0
        for name, cost in charges.items():
            capacity, seconds = self.windows[name]
            market = name.split("_", 1)[0]
            delay = max(delay, self._blocked_until.get(market, 0.0) - now)
            if not 0 < cost <= capacity:
                raise ValueError("request cost must fit its assigned window")
            events = self._events[name]
            while events and events[0][0] + seconds <= now:
                events.popleft()
            excess = sum(value for _, value in events) + cost - capacity
            for at, value in events:
                if excess <= 0:
                    break
                delay = max(delay, at + seconds - now)
                excess -= value
        return delay

    def take(self, charges: Mapping[str, int]) -> float:
        delay = self.delay(charges)
        if delay <= 0:
            now = self.clock()
            for name, cost in charges.items():
                self._events[name].append((now, cost))
        return delay

    def block(self, market: str, seconds: float) -> None:
        self._blocked_until[market] = max(
            self._blocked_until.get(market, 0.0), self.clock() + seconds
        )

    async def wait_ready(self, charges: Mapping[str, int], stop: asyncio.Event) -> None:
        while (delay := self.delay(charges)) > 0 and not stop.is_set():
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)

    async def acquire(self, charges: Mapping[str, int]) -> None:
        while (delay := self.take(charges)) > 0:
            await asyncio.sleep(delay)


_BUDGETS: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, RequestBudget] = (
    weakref.WeakKeyDictionary()
)


def shared_request_budget() -> RequestBudget:
    loop = asyncio.get_running_loop()
    if loop not in _BUDGETS:
        _BUDGETS[loop] = RequestBudget()
    return _BUDGETS[loop]


def usdm_request_charges(kind: str, *, depth_limit: int = 1000) -> dict[str, int]:
    if kind == "depth_snapshot":
        if depth_limit <= 50:
            weight = 2
        elif depth_limit <= 100:
            weight = 5
        else:
            weight = 10 if depth_limit <= 500 else 20
        return {"usdm_weight": weight}
    if kind in {"funding_history", "funding_info"}:
        return {"usdm_funding": 1}
    if kind.endswith("_5m"):
        # Conservatively share the strict statistics count across all six
        # routes, including basis; zero weight never means unlimited calls.
        return {"usdm_statistics": 1}
    return {"usdm_weight": 1}


def observe_websocket_rejection(market: str, status: int, headers: Mapping[str, str]) -> None:
    if status not in {418, 429}:
        return
    # Reuse the REST Retry-After/date/fallback interpretation. Import here to
    # avoid coupling the pure window counters to the Spot requester module.
    from .spot.rate_limit import _retry_at_utc_ns

    now = time.time_ns()
    until = _retry_at_utc_ns(
        status=status,
        headers={key.lower(): value for key, value in headers.items()},
        body_text="",
        now_utc_ns=now,
    )
    shared_request_budget().block(market, max(0.0, (until - now) / 1e9))
