"""Bounded P2 daily-file observation study; ideal UTC-phase polling, not live coverage.

Input is the explicit downloaded-files evidence list. Never downloads or discovers data.
"""

from __future__ import annotations

import argparse
import json
import time
from array import array
from datetime import date
from pathlib import Path
from typing import Any

from binance_market_data_recorder.backfill.daily_trades import (
    build_daily_trade_plan,
    daily_trade_rows,
)
from binance_market_data_recorder.domain.product import ProductKey


def analyze(record: dict[str, Any]) -> dict[str, Any]:
    if record.get("status") != 200:
        return {**record, "analyzed": False}
    entry = build_daily_trade_plan(
        [ProductKey(record["market"], record["symbol"])],
        date.fromisoformat(record["date"]),
        date.fromisoformat(record["date"]),
    ).entries[0]
    bins = array("I", [0]) * 86400
    first_id = previous = None
    id_gaps = count = 0
    sample = None
    for row in daily_trade_rows(entry, Path(record["local_path"])):
        count += 1
        trade_id = row["trade_id"]
        if previous is not None:
            id_gaps += max(0, trade_id - previous - 1)
        if first_id is None:
            first_id = trade_id
            sample = row
        previous = trade_id
        bins[(row["archive_event_time_utc_ns"] // 1_000_000_000) % 86400] += 1
    ideal = {}
    for seconds in (1, 2, 3, 6, 10, 15, 30, 60):
        loads = [sum(bins[i : i + seconds]) for i in range(0, 86400, seconds)]
        missing = sum(max(0, n - 1000) for n in loads)
        ideal[str(seconds)] = {
            "new_trades_beyond_window": missing,
            "fraction": missing / count if count else 0,
            "max_arrivals_per_poll": max(loads),
            "weight_per_minute": 300 / seconds,
        }
    return {k: v for k, v in record.items() if k not in {"sample", "local_path"}} | {
        "analyzed": True,
        "rows": count,
        "first_id": first_id,
        "last_id": previous,
        "id_gaps_in_file": id_gaps,
        "sample_fields": sample,
        "ideal_polling": ideal,
        "timestamp_unit": entry.timestamp_unit,
        "isRPITrade_available": False,
    }


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    with args.output.open("x") as output:
        wall, cpu = time.monotonic(), time.process_time()
        rows = [analyze(r) for r in json.loads(args.input.read_text())]
        json.dump(
            {
                "schema_version": "p2-daily-window-study.v1",
                "assumptions": (
                    "1000 newest rows available instantly; UTC phase 0; no RTT/errors/publication "
                    "delay; excludes day-start warmup. Does not prove live loss or 99.5%."
                ),
                "wall_seconds": time.monotonic() - wall,
                "cpu_seconds": time.process_time() - cpu,
                "products": rows,
            },
            output,
            indent=2,
        )
        output.write("\n")


if __name__ == "__main__":
    main()
