from __future__ import annotations

from typing import Any

import pytest

from tools.qualification_core_recovery import CoreRecovery

SECOND = 1_000_000_000


def observation(seconds: float, *, bad: bool = False, strict: bool = True) -> dict[str, Any]:
    return {
        "boottime_ns": round(seconds * SECOND),
        "utc_ns": round(seconds * SECOND),
        "heartbeat_at_utc_ns": round(seconds * SECOND),
        "core_ready": not bad,
        "auxiliary_gate": {"result": "BLOCK" if bad else "PASS",
                           "reasons": ["core_not_ready"] if bad else []},
        "strict_auxiliary_gate": {"result": "PASS" if strict and not bad else "BLOCK"},
    }


def test_three_recovered_episodes_do_not_consume_a_run_wide_kill_budget() -> None:
    recovery = CoreRecovery()
    for begin in (0, 100, 200):
        recovery.check(observation(begin, bad=True))
        recovery.check(observation(begin + 30))
    assert recovery.episodes == 3
    assert recovery.total_ns == 90 * SECOND
    assert recovery.first_bad is None
    recovery.endpoint(observation(43200))


@pytest.mark.parametrize("seconds", [60, 60.000000001])
def test_unrecovered_continuous_fault_stops_at_the_original_deadline(seconds: float) -> None:
    recovery = CoreRecovery()
    recovery.check(observation(0, bad=True))
    with pytest.raises(RuntimeError, match="deadline"):
        recovery.check(observation(seconds, bad=True))


def test_recovery_at_exact_deadline_is_allowed_but_late_recovery_is_rejected() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(0, bad=True))
    recovery.check(observation(60))
    recovery.check(observation(100, bad=True))
    with pytest.raises(RuntimeError, match="deadline_exceeded"):
        recovery.check(observation(160.000000001))


def test_a_new_fault_during_one_open_episode_cannot_reset_its_deadline() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(0, bad=True))
    heartbeat = observation(45)
    heartbeat["auxiliary_gate"]["reasons"] = ["heartbeat_not_fresh"]
    assert recovery.check(heartbeat)
    assert recovery.first_bad == 0
    with pytest.raises(RuntimeError, match="deadline"):
        recovery.check(observation(60, bad=True))


def test_bounded_empty_publication_retry_does_not_fail_recovered_core() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(0, bad=True))
    recovering = observation(5, strict=False)
    recovering["auxiliary_gate"]["recovering_contexts"] = [
        "um_perpetual:BTCUSDT:taker_buy_sell_volume_5m"
    ]
    assert recovery.check(recovering) is False
    assert recovery.first_bad is None
    assert recovery.total_ns == 5 * SECOND
    # Capture recovery is not permission to accept a non-strict endpoint.
    with pytest.raises(RuntimeError, match="strict_target_endpoint_failed"):
        recovery.endpoint(recovering)


def test_recovery_still_rejects_non_admitted_auxiliary_failure() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(0, bad=True))
    failed = observation(2, strict=False)
    failed["auxiliary_gate"] = {
        "result": "BLOCK", "reasons": ["auxiliary_capture_not_fresh:context"]
    }
    with pytest.raises(RuntimeError, match="fresh_auxiliary_PASS"):
        recovery.check(failed)
    assert recovery.first_bad == 0


def test_target_never_waits_for_recovery_or_accepts_a_degraded_endpoint() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(43199, bad=True))
    assert recovery.next_check_ns(43199 * SECOND, 43230 * SECOND, 43200 * SECOND) == 43200 * SECOND
    with pytest.raises(RuntimeError, match="strict_target_endpoint_failed"):
        recovery.endpoint(observation(43200, bad=True))


def test_only_an_open_fault_adds_checks_without_moving_regular_cadence() -> None:
    recovery = CoreRecovery()
    assert recovery.next_check_ns(0, 30 * SECOND, 43200 * SECOND) == 30 * SECOND
    recovery.check(observation(0, bad=True))
    assert recovery.next_check_ns(0, 30 * SECOND, 43200 * SECOND) == 5 * SECOND
    recovery.check(observation(2))
    assert recovery.next_check_ns(2 * SECOND, 30 * SECOND, 43200 * SECOND) == 30 * SECOND


def test_probe_cannot_sleep_past_the_fault_deadline() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(0, bad=True))
    assert recovery.next_check_ns(59 * SECOND, 90 * SECOND, 43200 * SECOND) == 60 * SECOND


def test_backwards_time_inside_recovery_is_rejected() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(2, bad=True))
    with pytest.raises(RuntimeError, match="deadline_exceeded"):
        recovery.check(observation(1))


def test_stale_pre_fault_publication_cannot_close_an_episode() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(5, bad=True))
    stale = observation(6)
    stale["heartbeat_at_utc_ns"] = 4 * SECOND
    with pytest.raises(RuntimeError, match="publication_predates_fault"):
        recovery.check(stale)
    assert recovery.first_bad == 5 * SECOND


def test_collection_latency_counts_towards_the_recovery_deadline() -> None:
    recovery = CoreRecovery()
    recovery.check(observation(0, bad=True))
    with pytest.raises(RuntimeError, match="deadline_exceeded"):
        recovery.check(observation(59), checked_at_ns=61 * SECOND)
