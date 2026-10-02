from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.usdm.side_data_rest import (
    FIVE_MINUTE_KINDS,
    FIVE_MINUTE_PERIOD_MS,
    RestSideDataKind,
)
from binance_market_data_recorder.config import RecorderConfig
from tools.qualification_auxiliary_gate import check


def ready(tmp_path: Path) -> tuple[dict[str, Any], RecorderConfig, int]:
    now = 1_800_000_000_000_000_000
    config = RecorderConfig(
        data_root=tmp_path, spot_symbols=("BTCUSDT", "ETHUSDT"), usdm_symbols=("BTCUSDT", "ETHUSDT")
    )
    closed = (now // 1_000_000 // FIVE_MINUTE_PERIOD_MS - 1) * FIVE_MINUTE_PERIOD_MS

    def item(kind: str, symbol: str) -> dict[str, Any]:
        result: dict[str, Any] = dict(
            enabled=True,
            running=True,
            status="RUNNING",
            connected=True,
            accepted=1,
            last_success_at_utc_ns=now,
        )
        if kind == "liquidation":
            result.update(accepted=0, last_success_at_utc_ns=None)
        if kind in {k.value for k in FIVE_MINUTE_KINDS}:
            result["cursor"] = dict(
                kind=kind, symbol=symbol, last_persisted_period_timestamp=closed
            )
        return result

    kinds = {k.value for k in RestSideDataKind} - {"funding_info", "exchange_info"}
    products = {
        market: {
            symbol: {"side_data": {kind: item(kind, symbol) for kind in selected}}
            for symbol in ("BTCUSDT", "ETHUSDT")
        }
        for market, selected in (
            ("spot", {"exchange_info"}),
            ("um_perpetual", kinds | {"mark_price", "liquidation"}),
        )
    }
    return (
        dict(
            status="RUNNING",
            core_ready=True,
            heartbeat_at_utc_ns=now,
            products=products,
            global_usdm_side_data={k: item(k, "GLOBAL") for k in ("funding_info", "exchange_info")},
        ),
        config,
        now,
    )


def test_empty_funding_info_and_quiet_connected_liquidation_are_valid(tmp_path: Path) -> None:
    state, config, now = ready(tmp_path)
    result = check(state, config, now_ns=now)
    assert result["result"] == "PASS" and len(result["contexts"]) == 26


@pytest.mark.parametrize(
    "mutation", ["failed", "disconnected", "missing", "cursor", "heartbeat", "disabled"]
)
def test_core_ready_does_not_hide_incomplete_auxiliary_coverage(
    tmp_path: Path, mutation: str
) -> None:
    state, config, now = ready(tmp_path)
    state = copy.deepcopy(state)
    sides = state["products"]["um_perpetual"]["ETHUSDT"]["side_data"]
    if mutation == "failed":
        sides["mark_price"]["status"] = "FAILED"
    elif mutation == "disconnected":
        sides["liquidation"]["connected"] = False
    elif mutation == "missing":
        del sides["open_interest"]
    elif mutation == "cursor":
        sides["basis_5m"]["cursor"]["symbol"] = "BTCUSDT"
    elif mutation == "heartbeat":
        state["heartbeat_at_utc_ns"] -= 16_000_000_000
    else:
        config = config.model_copy(update={"side_basis_enabled": False})
    result = check(state, config, now_ns=now)
    assert state["core_ready"] and result["result"] == "BLOCK" and result["reasons"]


def test_only_fresh_empty_period_recovery_is_allowed_between_strict_endpoints(
    tmp_path: Path,
) -> None:
    state, config, now = ready(tmp_path)
    item = state["products"]["um_perpetual"]["BTCUSDT"]["side_data"]["taker_buy_sell_volume_5m"]
    item.update(status="RETRYING", last_error_type="EmptySideDataResponse")
    item["cursor"]["last_persisted_period_timestamp"] -= FIVE_MINUTE_PERIOD_MS
    assert check(state, config, now_ns=now)["result"] == "BLOCK"
    result = check(state, config, now_ns=now, allow_empty_recovery=True)
    assert result["result"] == "PASS"
    assert result["recovering_contexts"] == ["um_perpetual:BTCUSDT:taker_buy_sell_volume_5m"]
    assert result["contexts"][result["recovering_contexts"][0]]["status"] == "RETRYING"


@pytest.mark.parametrize("mutation", [
    "catalog", "generic", "failed", "stopped", "stale", "no_success",
    "old_success", "future_success", "old_cursor", "future_cursor", "wrong_symbol",
    "disabled", "not_running", "other_rest", "websocket",
])
def test_empty_recovery_does_not_hide_absent_or_failed_auxiliary_data(
    tmp_path: Path, mutation: str,
) -> None:
    state, config, now = ready(tmp_path)
    sides = state["products"]["um_perpetual"]["BTCUSDT"]["side_data"]
    key = "mark_price" if mutation == "websocket" else (
        "open_interest" if mutation == "other_rest" else "taker_buy_sell_volume_5m"
    )
    item = sides[key]
    item.update(status="RETRYING", last_error_type="EmptySideDataResponse")
    if mutation in {"catalog", "generic"}:
        item["last_error_type"] = "CatalogStateError" if mutation == "catalog" else "RuntimeError"
    elif mutation in {"failed", "stopped", "stale"}:
        item["status"] = mutation.upper()
    elif mutation == "no_success":
        item["accepted"] = 0
    elif mutation == "old_success":
        item["last_success_at_utc_ns"] = now - 1200 * 10**9 - 1
    elif mutation == "future_success":
        item["last_success_at_utc_ns"] = now + 1
    elif mutation == "old_cursor":
        item["cursor"]["last_persisted_period_timestamp"] -= 1_500_000
    elif mutation == "future_cursor":
        item["cursor"]["last_persisted_period_timestamp"] += FIVE_MINUTE_PERIOD_MS
    elif mutation == "wrong_symbol":
        item["cursor"]["symbol"] = "ETHUSDT"
    elif mutation == "disabled":
        item["enabled"] = False
    elif mutation == "not_running":
        item["running"] = False
    result = check(state, config, now_ns=now, allow_empty_recovery=True)
    assert result["result"] == "BLOCK" and result["reasons"]
