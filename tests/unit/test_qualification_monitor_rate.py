from __future__ import annotations

import json

import pytest

from tools.qualification_monitor_rate import evaluate


def rows(count: int, failures: int = 0) -> list[bytes]:
    return [json.dumps({
        "utc_ns": i * 30_000_000_000, "config_sha256": "config",
        "auxiliary_gate": {
            "result": "BLOCK" if i < failures else "PASS",
            "reasons": ["NetworkError"] if i < failures else [],
            "expected_auxiliary_contexts": 26,
            "contexts": {str(j): {} for j in range(26)},
        },
    }).encode() + b"\n" for i in range(count)]


@pytest.mark.parametrize("count,failures,result", [
    (1000, 1, "PASS"), (1000, 2, "BLOCK"), (1436, 1, "PASS"), (240, 1, "BLOCK"),
])
def test_exact_threshold_and_stage_specific_denominator(
    count: int, failures: int, result: str,
) -> None:
    actual = evaluate(
        rows(count, failures), start_ns=0, end_ns=count * 30_000_000_000,
        config_sha256="config",
    )
    assert actual["result"] == result
    assert actual["failed"] == failures
    assert len(actual["failed_checks"]) == failures


def test_outside_rows_do_not_dilute_stage_failures() -> None:
    actual = evaluate(
        rows(1000, 1), start_ns=0, end_ns=240 * 30_000_000_000 - 1,
        config_sha256="config",
    )
    assert actual["total"] == 240 and actual["outside_period"] == 760
    assert actual["result"] == "BLOCK"


@pytest.mark.parametrize("mutation", ["empty", "gap", "duplicate", "malformed", "config"])
def test_invalid_or_missing_evidence_cannot_pass(mutation: str) -> None:
    evidence = rows(10)
    if mutation == "empty":
        evidence = []
    elif mutation == "gap":
        evidence = evidence[:2] + evidence[7:]
    elif mutation == "duplicate":
        evidence[2] = evidence[1]
    elif mutation == "malformed":
        evidence[2] = b"{}\n"
    else:
        evidence[2] = evidence[2].replace(b'"config"', b'"other"')
    with pytest.raises((ValueError, KeyError)):
        evaluate(evidence, start_ns=0, end_ns=300_000_000_000, config_sha256="config")
