from __future__ import annotations

import json

import pytest

from tools.qualification_owner_995 import (
    RecoveryObserver,
    check_write_safety,
    evaluate_fixed_slots,
)

SECOND = 10**9
CONTEXTS = frozenset({"configured"})


def observations(count: int = 1000, failed: int = 0) -> list[dict]:
    return [{
        "utc_ns": slot * 30 * SECOND,
        "boottime_ns": slot * 30 * SECOND,
        "scheduled_monitor_boottime_ns": slot * 30 * SECOND,
        "observation_role": "nominal_monitor", "monitor_slot": slot,
        "skipped_monitor_slots": 0,
        "config_sha256": "config",
        "auxiliary_gate": {
            "result": "BLOCK" if slot < failed else "PASS",
            "reasons": ["core_not_ready"] if slot < failed else [],
            "expected_auxiliary_contexts": 1, "contexts": {"configured": {}},
        },
    } for slot in range(count)]


def evaluate(rows: list[dict], count: int = 1000) -> dict:
    return evaluate_fixed_slots(
        [json.dumps(row).encode() + b"\n" for row in rows],
        start_ns=0, end_ns=count * 30 * SECOND, start_boottime_ns=0,
        config_sha256="config", expected_contexts=CONTEXTS,
    )


@pytest.mark.parametrize("count,failed,result", [
    (1000, 5, "PASS"), (1000, 6, "BLOCK"),
    (2880, 14, "PASS"), (2880, 15, "BLOCK"),
])
def test_inclusive_threshold_and_fixed_denominator(count, failed, result):
    rate = evaluate(observations(count, failed), count)
    assert rate["total"] == count and rate["failed"] == failed
    assert rate["result"] == result


def test_missing_slots_fail_without_shrinking_denominator():
    rows = observations()
    rate = evaluate(rows[:100] + rows[106:])
    assert rate["missing_slots"] == list(range(100, 106))
    assert rate["total"] == 1000 and rate["passed"] == 994
    assert rate["result"] == "BLOCK"


def test_probes_and_finish_cannot_dilute_failures():
    rows = observations(failed=6)
    probes = [{"observation_role": "recovery_probe"}] * 5000
    finish = [{"observation_role": "finish_probe"}] * 5000
    rate = evaluate(rows + probes + finish)
    assert rate["outside_nominal"] == 10000
    assert rate["total"] == 1000 and rate["result"] == "BLOCK"


def test_late_measurement_does_not_backfill_missing_time():
    rows = observations()
    rows[0]["boottime_ns"] = 30 * SECOND
    rows[0]["utc_ns"] = 30 * SECOND
    rows[0]["skipped_monitor_slots"] = 1
    del rows[1]
    rate = evaluate(rows)
    assert rate["failed"] == 2
    assert rate["failed_checks"][0]["reasons"] == ["late_monitor_observation"]


@pytest.mark.parametrize("mutation", [
    "duplicate", "config", "schedule", "context", "contradiction", "boolslot", "clock", "role",
    "utc_missing", "utc_bool", "utc_drift", "skipped", "float_schedule",
])
def test_untrusted_evidence_cannot_establish_percentage(mutation):
    rows = observations()
    if mutation == "duplicate":
        rows.insert(2, rows[1])
    elif mutation == "config":
        rows[1]["config_sha256"] = "other"
    elif mutation == "schedule":
        rows[1]["scheduled_monitor_boottime_ns"] += 1
    elif mutation == "context":
        rows[1]["auxiliary_gate"]["contexts"] = {"wrong": {}}
    elif mutation == "contradiction":
        rows[1]["auxiliary_gate"]["reasons"] = ["error"]
    elif mutation == "boolslot":
        rows[1]["monitor_slot"] = True
    elif mutation == "clock":
        rows[1]["boottime_ns"] = 1
    elif mutation == "utc_missing":
        del rows[1]["utc_ns"]
    elif mutation == "utc_bool":
        rows[1]["utc_ns"] = True
    elif mutation == "utc_drift":
        rows[1]["utc_ns"] += 2 * SECOND
    elif mutation == "skipped":
        rows[1]["skipped_monitor_slots"] = 2
    elif mutation == "float_schedule":
        rows[1]["scheduled_monitor_boottime_ns"] = float(30 * SECOND)
    else:
        rows[1]["observation_role"] = "unknown"
    with pytest.raises((ValueError, KeyError)):
        evaluate(rows)


def episode_row(seconds: int, bad: bool, *, auxiliary_block: bool = False) -> dict:
    return {
        "boottime_ns": seconds * SECOND, "utc_ns": seconds * SECOND,
        "core_ready": not bad,
        "auxiliary_gate": {"result": "BLOCK" if bad or auxiliary_block else "PASS",
                           "reasons": ["core_not_ready"] if bad else []},
        "strict_auxiliary_gate": {"result": "BLOCK" if bad or auxiliary_block else "PASS"},
    }


def test_long_recoverable_episode_logs_without_quality_abort_or_busy_loop():
    observer = RecoveryObserver()
    assert observer.check(episode_row(0, True))
    assert observer.check(episode_row(120, True))
    assert observer.next_check_ns(120 * SECOND, 150 * SECOND, 86400 * SECOND) == 125 * SECOND
    assert not observer.check(episode_row(125, False, auxiliary_block=True))
    assert observer.summary()["observed_recovery_seconds"] == 125
    assert observer.summary()["open_episode_boottime_ns"] is None


def test_actual_block_endpoint_is_preserved_without_fabricating_strict_pass():
    observer = RecoveryObserver()
    assert not observer.finish_endpoint(episode_row(86400, False, auxiliary_block=True))
    observer.endpoint(episode_row(86460, False, auxiliary_block=True))
    assert observer.summary()["episodes"] == 0


def test_clock_reversal_remains_critical():
    observer = RecoveryObserver()
    observer.check(episode_row(5, True))
    with pytest.raises(ValueError):
        observer.check(episode_row(4, False))


def safe_publication() -> dict:
    return {"failure": None, "shutdown_reason": None,
            "capacity": {"actual_hard_reserve_reached": False},
            "products": {"spot": {"BTCUSDT": {"failure": None}}}}


@pytest.mark.parametrize("mutation", ["service", "product", "reserve", "missing", "shutdown"])
def test_terminal_or_untrusted_write_safety_pauses(mutation):
    state = safe_publication()
    if mutation == "service":
        state["failure"] = {"error_type": "OSError"}
    elif mutation == "product":
        state["products"]["spot"]["BTCUSDT"]["failure"] = {"error_type": "RuntimeError"}
    elif mutation == "reserve":
        state["capacity"]["actual_hard_reserve_reached"] = True
    elif mutation == "shutdown":
        state["shutdown_reason"] = "hard_reserve"
    else:
        del state["capacity"]["actual_hard_reserve_reached"]
    with pytest.raises((RuntimeError, ValueError)):
        check_write_safety(state)


def test_core_or_transport_degradation_is_not_inferred_write_failure():
    state = safe_publication()
    state.update(core_ready=False, network_status="DEGRADED")
    state["products"]["spot"]["BTCUSDT"].update(ready=False)
    check_write_safety(state)


def test_early_starting_capacity_pending_is_only_allowed_pre_t0():
    state = safe_publication()
    state.update(status="STARTING", capacity=None)
    assert check_write_safety(state, allow_starting_capacity=True) is False
    with pytest.raises(ValueError):
        check_write_safety(state)
    state["status"] = "RUNNING"
    with pytest.raises(ValueError):
        check_write_safety(state, allow_starting_capacity=True)
    state.update(status="STARTING", failure={"error_type": "OSError"})
    with pytest.raises(RuntimeError):
        check_write_safety(state, allow_starting_capacity=True)
