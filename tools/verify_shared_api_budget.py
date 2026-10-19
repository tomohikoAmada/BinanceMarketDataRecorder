"""Offline two-process proof of Recorder/Gateway's production admission policies.

Run with --gateway-probe <bmd_gateway_public_api_budget_tests> --output <new.json>.
The subprocesses use virtual monotonic clocks. They run the production budget
and route-weight functions, not a second limiter model or live networking.
Transport binding, cancellation and source recovery are tested separately.
"""

from __future__ import annotations

import argparse
import asyncio
import heapq
import json
import subprocess
import sys
from collections import deque
from pathlib import Path
from typing import Any

from binance_market_data_recorder.binance.request_budget import (
    RECORDER_WINDOWS,
    RequestBudget,
    usdm_request_charges,
)
from binance_market_data_recorder.binance.spot.rate_limit import (
    SpotIpRateLimiter,
    depth_request_weight,
)
from binance_market_data_recorder.collector.usdm_side_data import UsdMRestCooldown

Operation = tuple[int, int, int, str, int, str]


def charges(usdm: int, kind: str) -> dict[str, int]:
    if kind == "W":
        return {("usdm" if usdm else "spot") + "_connections": 1}
    if not usdm:
        return {"spot_weight": depth_request_weight(1000) if kind == "D" else 20}
    return usdm_request_charges(
        {
            "D": "depth_snapshot",
            "M": "exchange_info",
            "S": "basis_5m",
            "F": "funding_history",
            "P": "premium_index_snapshot",
        }[kind]
    )


async def recorder_probe() -> None:
    work: list[Operation] = []
    for line in sys.stdin:
        a, b, c, d, e, f = line.split()
        work.append((int(a), int(b), int(c), d, int(e), f))
    heapq.heapify(work)
    now = [0.0]
    budget = RequestBudget(clock=lambda: now[0])
    spot = SpotIpRateLimiter(utc_clock_ns=lambda: int(now[0] * 1e9), monotonic_clock=lambda: now[0])
    usdm_gate = UsdMRestCooldown(
        utc_clock_ns=lambda: int(now[0] * 1e9), monotonic_clock=lambda: now[0]
    )
    while work:
        at, ident, usdm, kind, status, retry = heapq.heappop(work)
        now[0] = at / 1000
        cost = charges(usdm, kind)
        block = usdm_gate._deadline() if usdm else spot._blocked_until_monotonic
        delay = max(budget.delay(cost), (block - now[0]) if kind != "W" else 0)
        if delay > 0:
            # All fixture times/boundaries are integer milliseconds.
            heapq.heappush(work, (at + round(delay * 1000), ident, usdm, kind, status, retry))
            continue
        assert budget.take(cost) == 0
        amount = next(iter(cost.values()))
        print(ident, at, usdm, kind, amount)
        if status in {418, 429}:
            if usdm:
                usdm_gate.install(status=status, retry_after_seconds=float(retry))
            else:
                await spot.observe_weight_rejection(
                    status=status,
                    weight=amount,
                    headers={"Retry-After": retry},
                    body_text="",
                )


def workload(name: str) -> tuple[list[Operation], list[Operation]]:
    rec: list[Operation] = []
    gateway: list[Operation] = []

    def add(
        out: list[Operation],
        at: int,
        usdm: int,
        kind: str,
        count: int,
        status: int = 200,
        retry: str = "-",
    ) -> None:
        for _ in range(count):
            out.append((at, len(out), usdm, kind, status, retry))

    for market in (0, 1):
        add(rec, 0, market, "M", 1 if market else 10)
        add(gateway, 0, market, "M", 1)
    rounds = {
        "cold_start": 1,
        "single_key_repeated": 100,
        "all_keys_short_live": 12,
        "side_catchup": 2,
        "429_418_cooldown": 3,
    }[name]
    products = 1 if name == "single_key_repeated" else 10
    for turn in range(rounds):
        for market in (0, 1):
            for out in (rec, gateway):
                add(out, turn * 1000, market, "D", products)
                ws_per_key = (5 if market else 3) if out is rec else 1
                add(out, turn * 1000, market, "W", products * ws_per_key)
    add(rec, 0, 1, "S", 2400 if name == "side_catchup" else 120)
    add(rec, 0, 1, "F", 600 if name == "side_catchup" else 11)
    add(rec, 0, 1, "P", 20)
    if name == "429_418_cooldown":
        for out in (rec, gateway):
            # Unrelated owners already queued when a wire response extends the
            # process-shared gate. Later shorter cooldown cannot shorten it.
            add(out, 10000, 0, "D", 1, 429, "42")
            add(out, 11000, 0, "D", 1)
            add(out, 60000, 1, "D", 1, 418, "90")
            add(out, 61000, 1, "D", 1, 429, "1")
            add(out, 62000, 1, "D", 10)
    return sorted(rec), sorted(gateway)


def run_probe(command: list[str], operations: list[Operation]) -> list[dict[str, Any]]:
    result = subprocess.run(
        command,
        input="".join(" ".join(map(str, op)) + "\n" for op in operations),
        text=True,
        capture_output=True,
        timeout=20,
        check=True,
    )
    rows: list[dict[str, Any]] = []
    for line in result.stdout.splitlines():
        ident, at, usdm, kind, cost = line.split()
        rows.append(
            {"id": int(ident), "at_ms": int(at), "usdm": int(usdm), "kind": kind, "cost": int(cost)}
        )
    assert sorted(row["id"] for row in rows) == list(range(len(operations)))
    return rows


def peak(rows: list[dict[str, Any]], key: str, seconds: float, role: str) -> int:
    entries: list[tuple[int, int]] = []
    for row in rows:
        if role == "recorder":
            cost = charges(row["usdm"], row["kind"]).get(key, 0)
        else:
            market = "usdm" if row["usdm"] else "spot"
            scope = "connections" if row["kind"] == "W" else "weight"
            cost = row["cost"] if key == market + "_" + scope else 0
        if cost:
            entries.append((row["at_ms"], cost))
    queue: deque[tuple[int, int]] = deque()
    total = maximum = 0
    for at, cost in sorted(entries):
        while queue and queue[0][0] + seconds * 1000 <= at:
            total -= queue.popleft()[1]
        queue.append((at, cost))
        total += cost
        maximum = max(maximum, total)
    return maximum


def verify_cooldowns(
    rows: list[dict[str, Any]], operations: list[Operation], *, gateway: bool
) -> None:
    deadlines = {0: 0, 1: 0}
    by_id = {op[1]: op for op in operations}
    for row in sorted(rows, key=lambda value: (value["at_ms"], value["id"])):
        op = by_id[row["id"]]
        assert row["at_ms"] >= op[0]
        if gateway or row["kind"] != "W":
            assert row["at_ms"] >= deadlines[row["usdm"]]
        if op[4] in {418, 429}:
            deadlines[row["usdm"]] = max(
                deadlines[row["usdm"]], row["at_ms"] + int(op[5]) * 1000
            )


def verify(gateway_probe: Path) -> dict[str, Any]:
    gateway_caps = {
        "spot_weight": 3000,
        "usdm_weight": 600,
        "spot_connections": 60,
        "usdm_connections": 60,
    }
    results = []
    for name in (
        "cold_start",
        "single_key_repeated",
        "all_keys_short_live",
        "side_catchup",
        "429_418_cooldown",
    ):
        rec_ops, gateway_ops = workload(name)
        rec = run_probe(
            [sys.executable, str(Path(__file__).resolve()), "--recorder-probe"], rec_ops
        )
        gateway = run_probe([str(gateway_probe), "--trace"], gateway_ops)
        verify_cooldowns(rec, rec_ops, gateway=False)
        verify_cooldowns(gateway, gateway_ops, gateway=True)
        maxima = {}
        for key, (cap, seconds) in RECORDER_WINDOWS.items():
            r = peak(rec, key, seconds, "recorder")
            g = peak(gateway, key, seconds, "gateway")
            assert r <= cap and g <= gateway_caps.get(key, 0)
            # Merge actual independent admissions, not sums of unrelated peaks.
            merged = []
            for role, rows in (("recorder", rec), ("gateway", gateway)):
                for row in rows:
                    copied = dict(row)
                    copied["role"] = role
                    merged.append(copied)
            events = []
            for row in merged:
                market = "usdm" if row["usdm"] else "spot"
                scope = "connections" if row["kind"] == "W" else "weight"
                cost = (
                    charges(row["usdm"], row["kind"]).get(key, 0)
                    if row["role"] == "recorder"
                    else row["cost"]
                    if key == market + "_" + scope
                    else 0
                )
                if cost:
                    events.append(
                        {
                            "id": 0,
                            "at_ms": row["at_ms"],
                            "usdm": row["usdm"],
                            "kind": "W" if key.endswith("connections") else "D",
                            "cost": cost,
                        }
                    )
            if key in gateway_caps:
                combined = peak(events, key, seconds, "gateway")
                assert combined <= cap + gateway_caps[key]
            else:
                combined = r
            maxima[key] = {"recorder": r, "gateway": g, "combined": combined}
        assert max(row["at_ms"] for row in rec + gateway) < 3600000
        results.append(
            {
                "scenario": name,
                "recorder_attempts": len(rec),
                "gateway_attempts": len(gateway),
                "maxima": maxima,
                "last_admission_ms": max(row["at_ms"] for row in rec + gateway),
            }
        )
    return {
        "scope": "OFFLINE_TWO_PROCESS_PRODUCTION_POLICY_COMPOSITION",
        "production_network_calls": 0,
        "passed": True,
        "scenarios": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gateway-probe", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--recorder-probe", action="store_true")
    args = parser.parse_args()
    if args.recorder_probe:
        asyncio.run(recorder_probe())
        return
    if args.gateway_probe is None:
        parser.error("--gateway-probe is required")
    result = verify(args.gateway_probe.resolve())
    if args.output:
        with args.output.open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
