"""Replay the owner-approved sampled monitor gate; this is not data completeness.

The denominator includes every recorded check between the native T0 and target.
Original BLOCK rows remain failures. Missing/invalid sampling evidence blocks the
rate calculation; strict endpoints and both LIVE data audits remain separate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def evaluate(
    lines: Iterable[bytes], *, start_ns: int, end_ns: int, config_sha256: str,
    sample_interval_ns: int = 30_000_000_000,
) -> dict[str, Any]:
    if end_ns <= start_ns or sample_interval_ns <= 0:
        raise ValueError("invalid monitoring period or cadence")
    digest = hashlib.sha256()
    passed = total = outside = 0
    first: int | None = None
    previous: int | None = None
    last = start_ns
    maximum_gap = 0
    failures: list[dict[str, Any]] = []
    for ordinal, line in enumerate(lines, 1):
        digest.update(line)
        row = json.loads(line)
        now = row["utc_ns"]
        if not isinstance(now, int) or isinstance(now, bool):
            raise ValueError("invalid monitoring timestamp")
        if previous is not None and now <= previous:
            raise ValueError("duplicate or unordered monitoring checks")
        previous = now
        if not start_ns <= now <= end_ns:
            outside += 1
            continue
        if row["config_sha256"] != config_sha256:
            raise ValueError("monitoring configuration differs")
        gate = row["auxiliary_gate"]
        if gate["result"] not in {"PASS", "BLOCK"} or not isinstance(gate["reasons"], list):
            raise ValueError("invalid recorded monitoring result")
        if (gate["result"] == "PASS") != (not gate["reasons"]):
            raise ValueError("recorded result and reasons disagree")
        if gate["expected_auxiliary_contexts"] != 26 or len(gate["contexts"]) != 26:
            raise ValueError("monitoring profile differs")
        if first is None:
            first = now
            maximum_gap = now - start_ns
        else:
            maximum_gap = max(maximum_gap, now - last)
        last = now
        total += 1
        if gate["result"] == "PASS":
            passed += 1
        else:
            failures.append({
                "line": ordinal, "utc_ns": now,
                "line_sha256": hashlib.sha256(line).hexdigest(),
                "reasons": gate["reasons"],
            })
    if first is None:
        raise ValueError("no in-period monitoring evidence")
    maximum_gap = max(maximum_gap, end_ns - last)
    if maximum_gap > sample_interval_ns * 2:
        raise ValueError("monitoring evidence has an uncovered sampling interval")
    return {
        "policy": "owner-monitor-rate-999-per-1000-v1",
        "start_utc_ns": start_ns, "end_utc_ns": end_ns,
        "config_sha256": config_sha256, "jsonl_sha256": digest.hexdigest(),
        "passed": passed, "failed": total - passed, "total": total,
        "outside_period": outside, "pass_percent": passed * 100 / total,
        "threshold_numerator": 999, "threshold_denominator": 1000,
        "result": "PASS" if passed * 1000 >= total * 999 else "BLOCK",
        "maximum_sampling_gap_ns": maximum_gap, "failed_checks": failures,
        "note": "Sampled monitor rate only; strict endpoints, core and LIVE audits separate.",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--monitoring", type=Path, required=True)
    parser.add_argument("--stage-target", type=Path, required=True)
    parser.add_argument("--config-sha256", required=True)
    args = parser.parse_args()
    target = json.loads(args.stage_target.read_bytes())
    with args.monitoring.open("rb") as source:
        result = evaluate(
            source, start_ns=target["t0_utc_ns"], end_ns=target["terminal_utc_ns"],
            config_sha256=args.config_sha256,
        )
    print(json.dumps(result, sort_keys=True))
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
