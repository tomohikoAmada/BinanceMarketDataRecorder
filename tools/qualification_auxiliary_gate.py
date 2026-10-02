"""Read-only supplementary coverage gate for the frozen all-enabled VPS profile.

Core readiness/AcceptanceObserver retain their existing authority. Operators bind
these checks to the stage evidence and reject qualification if coverage fails.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from binance_market_data_recorder.binance.usdm.side_data_rest import (
    FIVE_MINUTE_KINDS,
    FIVE_MINUTE_PERIOD_MS,
    RestSideDataKind,
)
from binance_market_data_recorder.config import RecorderConfig, load_config


def check(
    state: dict[str, Any], config: RecorderConfig, *, now_ns: int,
    allow_empty_recovery: bool = False,
) -> dict[str, Any]:
    reasons: list[str] = []
    recovering_contexts: list[str] = []
    flags = [
        name
        for name in RecorderConfig.model_fields
        if name.endswith("_enabled")
        and (name.startswith("side_") or name == "spot_exchange_info_enabled")
    ]
    if not all(getattr(config, name) for name in flags):
        reasons.append("capture_flags_not_all_enabled")
    if (config.spot_symbols, config.usdm_symbols) != (
        ("BTCUSDT", "ETHUSDT"),
        ("BTCUSDT", "ETHUSDT"),
    ):
        reasons.append("unexpected_qualification_products")
    if state.get("status") != "RUNNING" or state.get("core_ready") is not True:
        reasons.append("core_not_ready")
    heartbeat = state.get("heartbeat_at_utc_ns", 0)
    if not 0 <= now_ns - heartbeat <= int(3 * config.heartbeat_seconds * 1e9):
        reasons.append("heartbeat_not_fresh")
    product_rest = {kind.value for kind in RestSideDataKind} - {"funding_info", "exchange_info"}
    expected: dict[str, dict[str, Any]] = {}
    for market, symbols in (("spot", config.spot_symbols), ("um_perpetual", config.usdm_symbols)):
        for symbol in symbols:
            product = state.get("products", {}).get(market, {}).get(symbol, {})
            sides = product.get("side_data", {})
            kinds = (
                {"exchange_info"}
                if market == "spot"
                else product_rest | {"mark_price", "liquidation"}
            )
            if set(sides) != kinds:
                reasons.append(f"auxiliary_set_differs:{market}:{symbol}")
            for kind in sorted(kinds):
                expected[f"{market}:{symbol}:{kind}"] = sides.get(kind, {})
    global_sides = state.get("global_usdm_side_data", {})
    if set(global_sides) != {"funding_info", "exchange_info"}:
        reasons.append("global_auxiliary_set_differs")
    for kind in ("funding_info", "exchange_info"):
        expected[f"um_perpetual:GLOBAL:{kind}"] = global_sides.get(kind, {})
    grace = config.side_degraded_after_seconds
    for context, item in expected.items():
        kind = context.rsplit(":", 1)[-1]
        recovering = (
            allow_empty_recovery
            and kind in {entry.value for entry in FIVE_MINUTE_KINDS}
            and item.get("status") == "RETRYING"
            and item.get("last_error_type") == "EmptySideDataResponse"
        )
        if (
            item.get("enabled") is not True
            or item.get("running") is not True
            or (item.get("status") != "RUNNING" and not recovering)
        ):
            reasons.append(f"auxiliary_not_running:{context}")
        if recovering:
            recovering_contexts.append(context)
        if kind in {"mark_price", "liquidation"} and item.get("connected") is not True:
            reasons.append(f"auxiliary_not_connected:{context}")
        if kind == "liquidation":
            continue  # The public feed may legitimately have no events.
        interval_name = {
            "premium_index_snapshot": "side_premium_index_interval_seconds",
            "open_interest_statistics_5m": "side_open_interest_statistics_interval_seconds",
            "taker_buy_sell_volume_5m": "side_taker_buy_sell_volume_interval_seconds",
            "global_long_short_ratio_5m": "side_global_long_short_ratio_interval_seconds",
            "top_long_short_account_ratio_5m": "side_top_long_short_account_ratio_interval_seconds",
            "top_long_short_position_ratio_5m": (
                "side_top_long_short_position_ratio_interval_seconds"
            ),
            "basis_5m": "side_basis_interval_seconds",
        }.get(kind, f"side_{kind}_interval_seconds")
        if context.startswith("spot:"):
            interval_name = "spot_exchange_info_interval_seconds"
        interval = 0 if kind == "mark_price" else getattr(config, interval_name)
        success = item.get("last_success_at_utc_ns")
        if (
            item.get("accepted", 0) < 1
            or not isinstance(success, int)
            or not 0 <= now_ns - success <= int((grace + interval) * 1e9)
        ):
            reasons.append(f"auxiliary_capture_not_fresh:{context}")
        if kind in {entry.value for entry in FIVE_MINUTE_KINDS}:
            cursor = item.get("cursor") or {}
            timestamp = cursor.get("last_persisted_period_timestamp")
            closed = (now_ns // 1_000_000 // FIVE_MINUTE_PERIOD_MS - 1) * FIVE_MINUTE_PERIOD_MS
            if (
                cursor.get("symbol") != context.split(":")[1]
                or cursor.get("kind") != kind
                or not isinstance(timestamp, int)
                or not 0 <= closed - timestamp <= int((grace + interval) * 1000)
            ):
                reasons.append(f"auxiliary_cursor_not_caught_up:{context}")
    return {
        "result": "PASS" if not reasons else "BLOCK",
        "observed_at_utc_ns": now_ns,
        "expected_auxiliary_contexts": 26,
        "contexts": expected,
        "allow_empty_recovery": allow_empty_recovery,
        "recovering_contexts": recovering_contexts,
        "reasons": reasons,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--allow-empty-recovery", action="store_true")
    args = parser.parse_args()
    config = load_config(config_file=args.config, environ={}).config
    result = check(
        json.loads(args.state.read_text()), config, now_ns=time.time_ns(),
        allow_empty_recovery=args.allow_empty_recovery,
    )
    print(json.dumps(result, sort_keys=True))
    return 0 if result["result"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
