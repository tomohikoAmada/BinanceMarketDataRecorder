"""Source-local availability evidence; callbacks only update bounded memory.

The Catalog worker flushes immutable interval identities and current progress.
No event payload is stored here and no health is inferred from message volume.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from itertools import islice
from typing import Any
from uuid import uuid4


@dataclass
class Availability:
    source_id: str
    instance_id: str
    observed_at: int
    last_valid: int | None = None
    unavailable_since: int | None = None
    gap_id: str | None = None
    reason: str | None = None
    failures: int = 0
    recoveries: int = 0
    last_error: str | None = None
    last_error_at: int | None = None
    checkpoint_at: int = 0
    eligible_since: int = 0
    eligibility_id: str | None = None
    dormant: bool = False

    def public_dict(self, now: int) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "collector_instance_id": self.instance_id,
            "observed_at_utc_ns": self.observed_at,
            "last_valid_utc_ns": self.last_valid,
            "unavailable_since_utc_ns": self.unavailable_since,
            "unavailable_seconds": (max(0, now - self.unavailable_since) / 1e9)
            if self.unavailable_since is not None
            else 0,
            "gap_id": self.gap_id,
            "reason": self.reason,
            "eligible_since_utc_ns": self.eligible_since,
            "eligibility_id": self.eligibility_id,
            "dormant": self.dormant,
            "failure_count": self.failures,
            "recovery_count": self.recoveries,
            "last_error_type": self.last_error,
            "last_error_utc_ns": self.last_error_at,
        }


class AvailabilityJournal:
    def __init__(self, instance_id: str, *, capacity: int = 65536) -> None:
        self.instance_id = instance_id
        self.capacity = capacity
        self.sources: dict[str, Availability] = {}
        self.pending: deque[dict[str, Any]] = deque()
        self.previous: dict[str, dict[str, Any]] = {}

    def _emit(self, state: Availability, gap: dict[str, object] | None = None) -> None:
        if len(self.pending) >= self.capacity:
            raise RuntimeError("availability journal exceeded its bounded persistence queue")
        self.pending.append(
            {
                "state": state.public_dict(state.observed_at),
                "gap": gap,
                "eligibility": {
                    "interval_id": state.eligibility_id,
                    "source_id": state.source_id,
                    "started_at_utc_ns": state.eligible_since,
                    "ended_at_utc_ns": state.observed_at if state.dormant else None,
                },
            }
        )
        state.checkpoint_at = state.observed_at

    def ensure(self, key: str, now: int) -> Availability:
        if key not in self.sources:
            old = self.previous.get(key, {})
            state = self.sources[key] = Availability(key, self.instance_id, now)
            state.failures = int(old.get("failure_count", 0))
            state.recoveries = int(old.get("recovery_count", 0))
            state.last_error = old.get("last_error_type")
            state.last_error_at = old.get("last_error_utc_ns")
            state.last_valid = old.get("last_valid_utc_ns")
            state.eligible_since = int(old.get("eligible_since_utc_ns", now))
            state.eligibility_id = old.get("eligibility_id") or str(uuid4())
            if old.get("dormant"):
                state.eligible_since, state.eligibility_id = now, str(uuid4())
            # A service restart is unknown from the last durable monitoring boundary.
            since = old.get("unavailable_since_utc_ns")
            if since is None:
                since = old.get("observed_at_utc_ns", now)
            state.gap_id = old.get("gap_id")
            self.unavailable(key, now, old.get("reason") or "STARTUP_UNKNOWN", since=int(since))
        return self.sources[key]

    def suspend(self, key: str, now: int) -> None:
        state = self.ensure(key, now)
        if state.dormant:
            return
        state.observed_at = now
        gap: dict[str, object] | None = None
        if state.unavailable_since is not None:
            gap = {
                "gap_id": state.gap_id,
                "source_id": key,
                "reason": state.reason,
                "started_at_utc_ns": state.unavailable_since,
                "ended_at_utc_ns": now,
            }
        state.unavailable_since = state.gap_id = None
        state.reason, state.dormant = "DORMANT_LIFECYCLE", True
        self._emit(state, gap)

    def resume(self, key: str, now: int) -> None:
        state = self.ensure(key, now)
        if state.dormant:
            state.dormant = False
            state.eligible_since, state.eligibility_id = now, str(uuid4())
            self.unavailable(key, now, "LIFECYCLE_RESUMED")

    def unavailable(self, key: str, now: int, reason: str, *, since: int | None = None) -> None:
        state = self.ensure(key, now) if key not in self.sources else self.sources[key]
        state.observed_at = now
        if state.unavailable_since is not None:
            return
        state.unavailable_since = now if since is None else since
        state.gap_id = state.gap_id or str(uuid4())
        state.reason = reason
        self._emit(
            state,
            {
                "gap_id": state.gap_id,
                "source_id": key,
                "reason": reason,
                "started_at_utc_ns": state.unavailable_since,
                "ended_at_utc_ns": None,
            },
        )

    def failure(self, key: str, now: int, error: str, *, since: int | None = None) -> None:
        state = self.ensure(key, now)
        state.failures += 1
        state.last_error, state.last_error_at = error, now
        self.unavailable(key, now, error, since=since)
        self._emit(state)

    def valid(self, key: str, now: int) -> None:
        state = self.ensure(key, now)
        state.observed_at = now
        state.last_valid = now
        if state.unavailable_since is not None:
            gap: dict[str, object] = {
                "gap_id": state.gap_id,
                "source_id": key,
                "reason": state.reason,
                "started_at_utc_ns": state.unavailable_since,
                "ended_at_utc_ns": now,
            }
            state.unavailable_since = state.gap_id = state.reason = None
            state.recoveries += 1
            self._emit(state, gap)
        elif now - state.checkpoint_at >= 10_000_000_000:
            self._emit(state)

    def batch(self, limit: int = 128) -> list[dict[str, Any]]:
        return list(islice(self.pending, limit))

    def acknowledge(self, count: int) -> None:
        for _ in range(count):
            self.pending.popleft()
