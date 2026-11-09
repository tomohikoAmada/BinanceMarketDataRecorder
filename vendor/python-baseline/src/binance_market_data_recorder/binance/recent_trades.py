"""Bounded D06 overlap reduction; native IDs plus retained-field equality, no hashes."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any


class RecentTradesCompactor:
    def __init__(self, capacity: int = 2000) -> None:
        if capacity < 1000:
            raise ValueError("recent-trade cache must hold the requested 1000-trade window")
        self.capacity = capacity
        self.rows: OrderedDict[int, tuple[dict[str, Any], int]] = OrderedDict()
        self.last_max_id: int | None = None

    def observe(self, trades: list[dict[str, Any]], received_ns: int) -> dict[str, Any]:
        changes = []
        ids = [row["id"] for row in trades]
        times = [row["time"] for row in trades]
        gap = None
        if ids and self.last_max_id is not None and min(ids) > self.last_max_id + 1:
            gap = {
                "first_unobserved_id": self.last_max_id + 1,
                "last_unobserved_id": min(ids) - 1,
                "reason": "recent_window_did_not_overlap",
            }
        initial = self.last_max_id is None
        for row in trades:
            trade_id = row["id"]
            previous = self.rows.get(trade_id)
            first_seen = received_ns if previous is None else previous[1]
            if previous is None or previous[0] != row:
                changes.append(
                    {
                        "trade": row,
                        "first_observed_utc_ns": first_seen,
                        "observed_utc_ns": received_ns,
                        "same_id_changed": previous is not None,
                    }
                )
            self.rows[trade_id] = (row, first_seen)
            self.rows.move_to_end(trade_id)
            if len(self.rows) > self.capacity:
                self.rows.popitem(last=False)
        if ids:
            self.last_max_id = max(max(ids), self.last_max_id or 0)
        return {
            "schema_version": "recent-trades-compact.v1",
            "changes": changes,
            "poll": {
                "returned_count": len(trades),
                "empty": not trades,
                "min_id": min(ids) if ids else None,
                "max_id": max(ids) if ids else None,
                "min_trade_time_ms": min(times) if times else None,
                "max_trade_time_ms": max(times) if times else None,
                "received_utc_ns": received_ns,
                "initial_window": initial,
                "window_gap": gap,
            },
            "response_list_reconstructible": False,
            "cache_capacity": self.capacity,
        }
