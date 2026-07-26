#!/usr/bin/env python3
"""Compare both disputed Spot bootstrap targets against one Raw window."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Evaluation:
    target: int
    result: str
    bridge_event_index: int | None
    bridge_u: int | None


def evaluate(last_update_id: int, events: list[dict[str, int]], offset: int) -> Evaluation:
    target = last_update_id + offset
    for index, event in enumerate(events):
        first, final = event["U"], event["u"]
        if final < target:
            continue
        if first > target:
            return Evaluation(target, "GAP", None, None)
        return Evaluation(target, "BRIDGED", index, final)
    return Evaluation(target, "NEED_MORE_EVENTS", None, None)


def compare(last_update_id: int, events: list[dict[str, int]]) -> dict[str, object]:
    documented = evaluate(last_update_id, events, 0)
    adjacent = evaluate(last_update_id, events, 1)
    return {
        "last_update_id": last_update_id,
        "event_count": len(events),
        "target_last_update_id": asdict(documented),
        "target_last_update_id_plus_one": asdict(adjacent),
        "different_bridge_event": (
            documented.bridge_event_index != adjacent.bridge_event_index
        ),
        "different_gap_result": documented.result != adjacent.result,
        "book_hash_comparison": (
            "SAME_INPUT_SUFFIX"
            if documented.bridge_event_index == adjacent.bridge_event_index
            else "REQUIRES_FULL_BOOK_FIXTURE"
        ),
        "decision": "EVIDENCE_ONLY_R034_REMAINS_OPEN",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("window", type=Path)
    args = parser.parse_args()
    document = json.loads(args.window.read_text(encoding="utf-8"))
    print(
        json.dumps(
            compare(int(document["lastUpdateId"]), list(document["events"])),
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
