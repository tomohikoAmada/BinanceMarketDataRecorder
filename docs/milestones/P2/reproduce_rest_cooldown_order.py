"""Offline P2-R4 admission-order reproduction; no real network or production data.

Run: PYTHONPATH=. .venv/bin/python docs/milestones/P2/reproduce_rest_cooldown_order.py
Exit 1 means a new request escaped after a known rejection, while persistence waited.
"""

from __future__ import annotations

import asyncio
import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

from binance_market_data_recorder.binance.public_rest import _ObservedResponse
from binance_market_data_recorder.binance.public_sources import directory_source, source_plan
from binance_market_data_recorder.binance.request_budget import RequestBudget
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_p2_coverage import SMALL, products


async def case(venue: str, status: int) -> dict[str, object]:
    budget = RequestBudget()
    persistence_entered, release = asyncio.Event(), asyncio.Event()
    calls: list[tuple[str, str]] = []

    class Client:
        def request(self, source: Any, parameters: Any) -> _ObservedResponse:
            calls.append((source.venue_market, source.stream))
            rejected = source.venue_market == venue and source.stream == "exchange_info"
            return _ObservedResponse(
                status if rejected else 500,
                {"Retry-After": "600"} if rejected else {},
                {"code": -1003, "msg": "Offline review fixture"},
                time.time_ns(),
                time.monotonic_ns(),
            )

        def close(self) -> None:
            pass

    with tempfile.TemporaryDirectory(prefix="recorder-p2-r4-order-") as temporary:
        root = Path(temporary)
        layout = ensure_storage_layout(root)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=root, coverage=SMALL),
                logging.getLogger("p2-r4-order-review"),
                "review",
                "offline",
                layout,
                catalog,
                clients={v: Client() for v in ("spot", "usdm", "coinm")},
            )
            first = directory_source(venue, owner.coverage)
            successor_venue = {"spot": "spot", "usdm": "coinm", "coinm": "usdm"}[venue]
            second = next(
                s for s in source_plan(products(), SMALL)
                if s.transport == "rest"
                and s.venue_market == successor_venue
                and s.stream != "exchange_info"
            )
            owner.add_source(second)
            persist = owner.persist_rest

            async def held_persist(envelope: EventEnvelope) -> None:
                if envelope.stream == "exchange_info":
                    persistence_entered.set()
                    await release.wait()
                await persist(envelope)

            with (
                patch.object(owner, "persist_rest", held_persist),
                patch(
                    "binance_market_data_recorder.collector.coverage.shared_request_budget",
                    return_value=budget,
                ),
            ):
                first_task = asyncio.create_task(owner.poll_once(owner.sources[first.key]))
                await asyncio.wait_for(persistence_entered.wait(), timeout=2)
                delay = budget.delay(second.charges)
                second_task = asyncio.create_task(owner.poll_once(owner.sources[second.key]))
                # A corrected gate leaves this task waiting; never wait out a real ban.
                await asyncio.wait({second_task}, timeout=0.1)
                successor_sent = len(calls) > 1
                release.set()
                result = await first_task
                if not second_task.done():
                    second_task.cancel()
                await asyncio.gather(second_task, return_exceptions=True)
            records = 0
            for spool in owner.spools.values():
                manifest = spool.close_and_seal()
                if manifest is not None:
                    records += int(manifest["record_count"])
            retained = not result.valid and "source_failed" in result.envelope.capture_flags
            return {
                "venue": venue,
                "http_status": status,
                "successor_venue": successor_venue,
                "server_retry_after_seconds": 600,
                "delay_while_persistence_waits": delay,
                "new_request_sent_while_persistence_waits": successor_sent,
                "delay_after_persistence": budget.delay(second.charges),
                "failure_observation_retained": retained,
                "sealed_record_count": records,
                "passed": delay >= 599 and not successor_sent and retained,
            }


async def main() -> bool:
    cases = [
        await case(venue, status)
        for venue in ("spot", "usdm", "coinm")
        for status in (429, 418)
    ]
    print(json.dumps({"cases": cases}, indent=2))
    return all(row["passed"] for row in cases)


if __name__ == "__main__":
    raise SystemExit(0 if asyncio.run(main()) else 1)
