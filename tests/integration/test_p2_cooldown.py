from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_rest import _ObservedResponse
from binance_market_data_recorder.binance.public_sources import directory_source
from binance_market_data_recorder.binance.request_budget import RequestBudget
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

RECEIVED = 1_700_000_000_000_000_000


@pytest.mark.parametrize("venue", ["spot", "usdm", "coinm"])
@pytest.mark.parametrize(
    "status,headers,body,expected",
    [
        (429, {"Retry-After": "7"}, {}, 5),
        (429, {"Retry-After": "600"}, {}, 598),
        (418, {"Retry-After": "600"}, {}, 598),
        (429, {"Retry-After": "Tue, 14 Nov 2023 22:18:20 GMT"}, {}, 298),
        (418, {}, {"msg": "IP banned until 1700000600000."}, 598),
        (429, {}, {}, 58),
        (418, {}, {}, 86398),
    ],
)
def test_rest_retry_boundaries_use_received_evidence_and_shared_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    venue: str,
    status: int,
    headers: dict[str, str],
    body: dict[str, Any],
    expected: int,
) -> None:
    class Client:
        def request(self, source: Any, params: Any) -> _ObservedResponse:
            return _ObservedResponse(status, headers, body, RECEIVED, 1000)

        def close(self) -> None:
            pass

    async def exercise() -> None:
        budget = RequestBudget(clock=lambda: 0.0)
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.coverage.shared_request_budget", lambda: budget
        )
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.coverage.time.time_ns",
            lambda: RECEIVED + 2_000_000_000,
        )
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(
                    data_root=tmp_path, coverage=CoverageConfig(underlying_assets=("BTC",))
                ),
                logging.getLogger("p2.cooldown"),
                "v",
                "i",
                layout,
                catalog,
                clients={v: Client() for v in ("spot", "usdm", "coinm")},
            )
            source = directory_source(venue, owner.coverage)
            result = await owner.poll_once(owner.sources[source.key])
            assert not result.valid and "source_failed" in result.envelope.capture_flags
            saved = json.loads(result.envelope.raw_payload)
            assert saved["response"]["model"] == body
            assert saved["response"]["headers"] == {k.lower(): v for k, v in headers.items()}
            assert owner.sources[source.key].last_success is None
            pool = "spot" if venue == "spot" else "usdm"
            assert budget.delay({pool + "_weight": 1}) == expected
            other = "usdm" if pool == "spot" else "spot"
            assert budget.delay({other + "_weight": 1}) == 0
            if pool == "usdm":
                # CM charges the same pool; distinct host names do not bypass the ban.
                assert budget.delay(directory_source("coinm", owner.coverage).charges) == expected
                assert budget.delay(directory_source("usdm", owner.coverage).charges) == expected
            for spool in owner.spools.values():
                spool.close_and_seal()

    asyncio.run(exercise())
