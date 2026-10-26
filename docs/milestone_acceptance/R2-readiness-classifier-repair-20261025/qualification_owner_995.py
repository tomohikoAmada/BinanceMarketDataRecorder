"""Future owner capacity runs: tolerate measured faults, never invent evidence.

This module does not change historical runs or the production collector. Monitor
availability has fixed nominal slots; saved-frame conformance is audited apart.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import Any

POLICY = "owner995_log_recoverable_faults_v1"
FINALIZATION_POLICY = "minimum_duration_last_trusted_endpoint_max60s_v1"
RECOVERY_POLL_NS = 5_000_000_000
FINALIZATION_RESERVE_NS = 60_000_000_000


def check_write_safety(
    publication: Mapping[str, Any], *, allow_starting_capacity: bool = False,
) -> bool:
    """Only explicit terminal/write-safety facts; not transient NOT_READY."""
    if publication["failure"] is not None:
        raise RuntimeError("critical_service_failure")
    if publication["shutdown_reason"] is not None:
        raise RuntimeError("critical_service_shutdown")
    for products in publication["products"].values():
        for product in products.values():
            # The production publication distinguishes supervisor termination
            # (FAILED) from transient readiness/resync reasons (CONNECTING).
            # Both share the legacy "failure" field; only exact source-defined
            # recovery reasons may remain a quality fault instead of a stop.
            status = product["status"]
            if status == "FAILED":
                raise RuntimeError("critical_product_terminal_failure")
            if (type(status) is not str or status not in {"READY", "CONNECTING"}
                    or type(product["ready"]) is not bool
                    or (status == "READY") != product["ready"]):
                raise ValueError("untrusted_product_readiness_state")
            failure = product["failure"]
            if failure is None:
                continue
            if not (
                publication["status"] == "RUNNING"
                and status == "CONNECTING" and product["ready"] is False
                and type(failure) is str
                and failure in {
                    "planned_rotation", "unexpected_disconnect", "server_shutdown",
                    "ingress_backpressure", "sequence_gap",
                }
            ):
                raise RuntimeError("critical_product_terminal_failure")
    capacity = publication["capacity"]
    # The collector first publishes STARTING before its initial disk census.
    # This honest pending state is allowed only during bounded pre-T0 warm-up.
    if allow_starting_capacity and publication["status"] == "STARTING" and capacity is None:
        return False
    if (not isinstance(capacity, dict)
            or type(capacity.get("actual_hard_reserve_reached")) is not bool):
        raise ValueError("untrusted_write_safety_publication")
    if capacity["actual_hard_reserve_reached"]:
        raise RuntimeError("critical_actual_hard_reserve")
    return True


class RecoveryObserver:
    """Observe core episodes without a per-episode quality-stop deadline."""

    def __init__(self) -> None:
        self.first_bad: int | None = None
        self.total_ns = 0
        self.episodes = 0
        self.previous_ns: int | None = None

    def check(self, row: dict[str, Any], *, checked_at_ns: int | None = None) -> bool:
        now = row["boottime_ns"]
        if (self.previous_ns is not None and now < self.previous_ns) or (
            checked_at_ns is not None and checked_at_ns < now
        ):
            raise ValueError("untrusted_recovery_clock")
        self.previous_ns = now
        bad = row["core_ready"] is not True or any(
            reason in row["auxiliary_gate"]["reasons"]
            for reason in ("core_not_ready", "heartbeat_not_fresh")
        )
        if bad and self.first_bad is None:
            self.first_bad = now
            self.episodes += 1
        elif not bad and self.first_bad is not None:
            self.total_ns += now - self.first_bad
            self.first_bad = None
        return bad

    def finish_endpoint(self, row: dict[str, Any], *, checked_at_ns: int | None = None) -> bool:
        bad = self.check(row, checked_at_ns=checked_at_ns)
        return not bad and row["strict_auxiliary_gate"]["result"] == "PASS"

    def endpoint(self, row: dict[str, Any], *, checked_at_ns: int | None = None) -> None:
        self.check(row, checked_at_ns=checked_at_ns)

    def next_check_ns(self, now_ns: int, monitor_due_ns: int, target_ns: int) -> int:
        due = min(monitor_due_ns, target_ns)
        if self.first_bad is not None:
            due = min(due, now_ns + RECOVERY_POLL_NS)
        return due

    def summary(self) -> dict[str, Any]:
        return {
            "policy": POLICY, "episodes": self.episodes,
            "observed_recovery_seconds": self.total_ns / 1_000_000_000,
            "open_episode_boottime_ns": self.first_bad,
            "timing_note": (
                "First observed bad to observed core recovery; not exact network downtime."
            ),
        }


def evaluate_fixed_slots(
    lines: Iterable[bytes], *, start_ns: int, end_ns: int,
    start_boottime_ns: int, config_sha256: str,
    expected_contexts: frozenset[str], sample_interval_ns: int = 30_000_000_000,
) -> dict[str, Any]:
    duration = end_ns - start_ns
    if duration <= 0 or sample_interval_ns <= 0 or duration % sample_interval_ns:
        raise ValueError("invalid fixed monitor duration or cadence")
    total = duration // sample_interval_ns
    digest = hashlib.sha256()
    seen: set[int] = set()
    passed = outside = 0
    failures: list[dict[str, Any]] = []
    previous: int | None = None
    for ordinal, line in enumerate(lines, 1):
        digest.update(line)
        row = json.loads(line)
        role = row["observation_role"]
        if role in {"recovery_probe", "finish_probe"}:
            outside += 1
            continue
        if role != "nominal_monitor":
            raise ValueError("untrusted monitoring role")
        slot = row["monitor_slot"]
        if type(slot) is not int or not 0 <= slot < total or slot in seen:
            raise ValueError("invalid or duplicate nominal slot")
        scheduled = start_boottime_ns + slot * sample_interval_ns
        observed = row["boottime_ns"]
        if type(observed) is not int or observed < scheduled:
            raise ValueError("invalid monitor clock")
        utc = row["utc_ns"]
        if type(utc) is not int or abs(
            (utc - start_ns) - (observed - start_boottime_ns)
        ) >= 2_000_000_000:
            raise ValueError("untrusted monitor UTC/BOOTTIME projection")
        if previous is not None and observed <= previous:
            raise ValueError("unordered monitoring observations")
        previous = observed
        if (type(row["scheduled_monitor_boottime_ns"]) is not int
                or row["scheduled_monitor_boottime_ns"] != scheduled):
            raise ValueError("nominal schedule differs")
        skipped = row["skipped_monitor_slots"]
        if type(skipped) is not int or skipped != (observed - scheduled) // sample_interval_ns:
            raise ValueError("untrusted skipped-slot evidence")
        if row["config_sha256"] != config_sha256:
            raise ValueError("monitoring configuration differs")
        gate = row["auxiliary_gate"]
        if gate["result"] not in {"PASS", "BLOCK"} or not isinstance(gate["reasons"], list):
            raise ValueError("invalid monitor gate")
        if (gate["result"] == "PASS") != (not gate["reasons"]):
            raise ValueError("monitor result and reasons disagree")
        if (gate["expected_auxiliary_contexts"] != len(expected_contexts)
                or frozenset(gate["contexts"]) != expected_contexts):
            raise ValueError("monitoring profile differs")
        seen.add(slot)
        reasons = list(gate["reasons"])
        if observed >= scheduled + sample_interval_ns:
            reasons.append("late_monitor_observation")
        if reasons:
            failures.append({"slot": slot, "line": ordinal, "reasons": reasons,
                             "line_sha256": hashlib.sha256(line).hexdigest()})
        else:
            passed += 1
    missing = sorted(set(range(total)) - seen)
    failures.extend({"slot": slot, "reasons": ["missing_nominal_monitor"]} for slot in missing)
    return {
        "policy": "owner995_fixed_nominal_slots_v1", "passed": passed,
        "failed": total - passed, "total": total, "recorded_nominal": len(seen),
        "missing_slots": missing, "outside_nominal": outside,
        "pass_percent": passed * 100 / total, "failed_checks": failures,
        "result": "PASS" if passed * 1000 >= total * 995 else "BLOCK",
        "threshold_numerator": 995, "threshold_denominator": 1000,
        "start_utc_ns": start_ns, "end_utc_ns": end_ns,
        "start_boottime_ns": start_boottime_ns,
        "config_sha256": config_sha256, "jsonl_sha256": digest.hexdigest(),
        "note": (
            "Official auxiliary/core gate unchanged; strict gate and resource peaks diagnostic; "
            "saved frames separate."
        ),
    }
