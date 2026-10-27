"""Offline P2-R4 reproduction; no exchange requests or production data access.

Run: PYTHONPATH=. .venv/bin/python docs/milestones/P2/reproduce_rest_cooldown.py
Exit 1 means the server's 600-second Retry-After was not respected.
"""

from __future__ import annotations

import asyncio
import json
import logging
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_sources import directory_source
from binance_market_data_recorder.binance.request_budget import RequestBudget
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


class Response:
    def __init__(self, status: int) -> None:
        self.status = status
        self.headers = {"Retry-After": "600"}

    def data(self) -> dict[str, object]:
        return {"code": -1003, "msg": "Synthetic rate-limit response"}


class Client:
    def __init__(self, status: int) -> None:
        self.status = status

    def request(self, source: Any, parameters: Any) -> Response:
        return Response(self.status)

    def close(self) -> None:
        pass


async def case(venue: str, status: int) -> dict[str, object]:
    budget = RequestBudget(clock=lambda: 0.0)
    with tempfile.TemporaryDirectory(prefix="recorder-p2-r4-") as temporary:
        root = Path(temporary)
        layout = ensure_storage_layout(root)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(
                    data_root=root,
                    coverage=CoverageConfig(underlying_assets=("BTC", "ETH")),
                ),
                logging.getLogger("p2-r4-review"),
                "review",
                "offline",
                layout,
                catalog,
                clients={v: Client(status) for v in ("spot", "usdm", "coinm")},
            )
            source = directory_source(venue, owner.coverage)
            with patch(
                "binance_market_data_recorder.collector.coverage.shared_request_budget",
                return_value=budget,
            ):
                result = await owner.poll_once(owner.sources[source.key])
            saved = json.loads(result.envelope.raw_payload)
            actual = budget.delay(source.charges)
            for spool in owner.spools.values():
                spool.close_and_seal()
            return {
                "venue": venue,
                "http_status": status,
                "saved_retry_after": saved["response"]["headers"]["retry-after"],
                "expected_wait_seconds": 600,
                "actual_wait_seconds": actual,
                "failure_observation_retained": not result.valid
                and "source_failed" in result.envelope.capture_flags,
                "passed": 599 <= actual <= 601,
            }


async def main() -> bool:
    results = [
        await case(venue, status)
        for venue in ("spot", "usdm", "coinm")
        for status in (429, 418)
    ]
    print(json.dumps({"cases": results}, indent=2))
    return all(row["passed"] for row in results)


if __name__ == "__main__":
    raise SystemExit(0 if asyncio.run(main()) else 1)
