"""Bound a continuous observed fault without treating recovered faults as fatal.

Only core/heartbeat recovery belongs here. The caller still checks incarnation,
clock, resources, strict endpoints and saved-data integrity. Extra recovery
observations must stay outside the fixed-cadence monitoring denominator.
"""

from __future__ import annotations

from typing import Any

RECOVERY_LIMIT_NS = 60_000_000_000
RECOVERY_POLL_NS = 5_000_000_000
POLICY = "max60s_per_core_episode_independent_auxiliary_freshness_v2"


class CoreRecovery:
    def __init__(self) -> None:
        self.first_bad: int | None = None
        self.first_bad_utc_ns = 0
        self.total_ns = 0
        self.episodes = 0

    def check(self, row: dict[str, Any], *, checked_at_ns: int | None = None) -> bool:
        now: int = row["boottime_ns"]
        bad = row["core_ready"] is not True or any(
            reason in row["auxiliary_gate"]["reasons"]
            for reason in ("core_not_ready", "heartbeat_not_fresh")
        )
        if bad and self.first_bad is None:
            self.first_bad = now
            self.first_bad_utc_ns = row["utc_ns"]
            self.episodes += 1
        if self.first_bad is not None:
            elapsed = now - self.first_bad
            checked_elapsed = (now if checked_at_ns is None else checked_at_ns) - self.first_bad
            if elapsed < 0 or checked_elapsed < elapsed or checked_elapsed > RECOVERY_LIMIT_NS:
                raise RuntimeError("core_heartbeat_recovery_deadline_exceeded")
            if not bad:
                # A recent, bounded five-minute publication retry is already
                # admitted by the capture gate. It must not turn a recovered
                # core connection into a fatal fault. Strict endpoints remain
                # separate and still require every auxiliary context RUNNING.
                if row["auxiliary_gate"]["result"] != "PASS":
                    raise RuntimeError("core_recovery_requires_fresh_auxiliary_PASS")
                if row["heartbeat_at_utc_ns"] < self.first_bad_utc_ns:
                    raise RuntimeError("core_recovery_publication_predates_fault")
                self.total_ns += elapsed
                self.first_bad = None
                self.first_bad_utc_ns = 0
            elif checked_elapsed == RECOVERY_LIMIT_NS:
                raise RuntimeError("core_heartbeat_not_recovered_at_deadline")
        return bad

    def endpoint(self, row: dict[str, Any], *, checked_at_ns: int | None = None) -> None:
        if row["strict_auxiliary_gate"]["result"] != "PASS":
            raise RuntimeError("strict_target_endpoint_failed")
        self.check(row, checked_at_ns=checked_at_ns)
        if self.first_bad is not None:
            raise RuntimeError("target_during_core_degradation")

    def next_check_ns(self, now_ns: int, monitor_due_ns: int, target_ns: int) -> int:
        """Wake at the next regular check, fault probe, or fixed deadline."""
        due = min(monitor_due_ns, target_ns)
        if self.first_bad is not None:
            due = min(due, now_ns + RECOVERY_POLL_NS, self.first_bad + RECOVERY_LIMIT_NS)
        return due

    def summary(self) -> dict[str, Any]:
        return {
            "policy": POLICY,
            "episodes": self.episodes,
            "observed_recovery_seconds": self.total_ns / 1_000_000_000,
            "open_episode_boottime_ns": self.first_bad,
            "timing_note": (
                "First observed bad through strict recovery; not exact network downtime."
            ),
        }
