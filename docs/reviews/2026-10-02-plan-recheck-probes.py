"""Offline reproduction of auxiliary FAILED status under otherwise ready cores.

Run with PYTHONPATH=src:. against review head 7877082. The native state builder
is real; Collector readiness and auxiliary terminal state are injected fixtures.
No service, network, archive volume, or production data is opened.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.service.runtime import ServiceRuntime
from tests.unit.test_service_runtime import FakeCollector, _config


def main() -> None:
    with TemporaryDirectory(prefix="recorder-plan-recheck-") as temporary:
        runtime = ServiceRuntime(
            config=_config(Path(temporary)), logger=logging.getLogger("recheck")
        )
        for market in ("spot", "um_perpetual"):
            collector = FakeCollector(market, f"collector-{market}")
            collector.started = True
            runtime._collectors[ProductKey(market, "BTCUSDT")] = collector
        runtime.global_side_data = SimpleNamespace(  # type: ignore[assignment]
            status=lambda: {"funding_info": {"enabled": True, "status": "FAILED"}}
        )
        state = runtime._state_document()
        print(
            json.dumps(
                {
                    "kind": "OFFLINE_REVIEW_REPRODUCTION_NOT_FORMAL_CREDIT",
                    "core_ready": state["core_ready"],
                    "network_status": state["network_status"],
                    "global_usdm_side_data": state["global_usdm_side_data"],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
