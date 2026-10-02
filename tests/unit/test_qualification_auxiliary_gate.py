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
