"""Matched-input P2 encoding/size comparison. Local evidence, not VPS capacity."""

from __future__ import annotations

import argparse
import base64
import json
import statistics
import time
from pathlib import Path
from typing import Any

import zstandard

from binance_market_data_recorder.binance.recent_trades import RecentTradesCompactor
from binance_market_data_recorder.binance.rest_representation import response_document


def encoded(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def measure(function: Any) -> tuple[float, int, Any]:
    started = time.process_time_ns()
    records = function()
    elapsed = (time.process_time_ns() - started) / 1e6
    packed = b"\n".join(encoded(record) for record in records)
    return elapsed, len(zstandard.ZstdCompressor(level=3).compress(packed)), records


def comparison() -> dict[str, Any]:
    model = {
        "lastUpdateId": 123456789,
        "bids": [[str(90000 - i) + ".00000000", "1.23000000"] for i in range(1000)],
        "asks": [[str(90001 + i) + ".00000000", "2.34000000"] for i in range(1000)],
    }
    raw = json.dumps(model, indent=1).encode()
    body = base64.b64encode(raw).decode()

    def snapshots(dual: bool) -> list[dict[str, Any]]:
        records = []
        for i in range(100):
            response = {"raw_body_base64": body, "status": 200, "receive_time_utc_ns": i}
            if dual:
                response["model"] = model
            record = {"schema_version": "legacy.v2" if dual else "single.v3", "response": response}
            encoded(record)
            records.append(record)
        return records

    windows: list[list[dict[str, Any]]] = [
        [
            {
                "id": i,
                "price": f"{90000 + i / 100:.8f}",
                "qty": "0.00100000",
                "quoteQty": f"{90 + i / 100000:.8f}",
                "time": 1700000000000 + i,
                "isBuyerMaker": bool(i % 2),
            }
            for i in range(at, at + 1000)
        ]
        for at in range(0, 100, 2)
    ]
    windows.extend([[], [*windows[-1][:-1], {**windows[-1][-1], "price": "99999.00000000"}]])

    def trades(compact: bool) -> list[dict[str, Any]]:
        cache = RecentTradesCompactor()
        records = []
        for i, window in enumerate(windows):
            record = {"received_ns": i, "model": cache.observe(window, i) if compact else window}
            encoded(record)
            records.append(record)
        return records

    measurements: dict[str, list[tuple[float, int]]] = {
        name: [] for name in ("snapshot_dual", "snapshot_single", "d06_full", "d06_compact")
    }
    for reverse in (False, True, True, False):
        ops = [
            ("snapshot_dual", lambda: snapshots(True)),
            ("snapshot_single", lambda: snapshots(False)),
            ("d06_full", lambda: trades(False)),
            ("d06_compact", lambda: trades(True)),
        ]
        for name, operation in reversed(ops) if reverse else ops:
            cpu, size, records = measure(operation)
            measurements[name].append((cpu, size))
            if name.startswith("snapshot"):
                assert all(response_document(record) == model for record in records)
    compact = trades(True)
    # Compare all observed new/changed facts and their first-observed times.
    expected: dict[int, tuple[dict[str, Any], int]] = {}
    wanted = []
    for i, window in enumerate(windows):
        for row in window:
            old = expected.get(row["id"])
            first = old[1] if old is not None else i
            if old is None or old[0] != row:
                wanted.append((row, first, i))
            expected[row["id"]] = (row, first)
    got = [
        (change["trade"], change["first_observed_utc_ns"], change["observed_utc_ns"])
        for record in compact
        for change in record["model"]["changes"]
    ]
    assert got == wanted
    stats: dict[str, Any] = {
        name: {
            "cpu_ms": statistics.median(x[0] for x in values),
            "compressed_bytes": values[0][1],
            "rounds": values,
        }
        for name, values in measurements.items()
    }
    return {
        "scope": "local fixed input; encoding and compressed bytes only",
        "representation_equivalence": True,
        "d06_new_changed_first_observed_equal": True,
        "measurements": stats,
        "d06_compact_selected": (
            stats["d06_compact"]["cpu_ms"] < stats["d06_full"]["cpu_ms"]
            and stats["d06_compact"]["compressed_bytes"] < stats["d06_full"]["compressed_bytes"]
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # Refuse before doing work; reruns never overwrite prior evidence.
    with args.output.open("x") as handle:
        result = comparison()
        handle.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
