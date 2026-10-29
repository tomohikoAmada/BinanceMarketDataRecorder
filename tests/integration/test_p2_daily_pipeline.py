from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.backfill import HistoricalImporter
from binance_market_data_recorder.binance.public_sources import product_sources, source_plan
from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_p2_capture import Client
from tests.unit.test_p2_coverage import SMALL, directory, products, trade
from tests.unit.test_p2_daily_trades import source


def test_new_product_over_steady_budget_is_visible_and_existing_products_stay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def idle(self: ProductCapture, stop: asyncio.Event) -> None:
        await stop.wait()

    async def offline_shared(self: SelectedAssetCapture) -> None:
        pass

    monkeypatch.setattr(ProductCapture, "run", idle)
    monkeypatch.setattr(SelectedAssetCapture, "_reconcile_shared", offline_shared)
    intervals = {
        f"{p.key.market}:{p.key.symbol}": 30.0 for p in products() if p.venue_market != "spot"
    }
    intervals["um_perpetual:BTCNEW"] = 1.0
    config = SMALL.model_copy(
        update={"recent_trades_interval_seconds": 30.0, "recent_trades_intervals": intervals}
    )
    plan = source_plan(products(), config)
    steady = (
        sum(
            s.weight * 60 / s.cadence_seconds
            for s in plan
            if s.transport == "rest"
            and s.venue_market != "spot"
            and s.cadence_seconds > 0
            and s.lifecycle != "after_delivery"
        )
        + 120 / config.directory_interval_seconds
    )
    config = config.model_copy(update={"max_steady_derivative_weight_per_minute": steady + 100})

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=config),
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients={"fixture": Client()},
            )
            for venue in ("spot", "usdm", "coinm"):
                await owner.reconcile(venue, directory(venue))
            original = set(owner.collectors)
            assert len(original) == 50
            d = directory("usdm")
            row = next(r for r in d["symbols"] if r["symbol"] == "BTCUSDT")
            d["symbols"].append({**row, "symbol": "BTCNEW", "pair": "BTCNEW"})
            await owner.reconcile("usdm", d)
            assert set(owner.collectors) == original
            assert (
                owner.admission[ProductKey("um_perpetual", "BTCNEW")] == "NOT_ADMITTED_REST_BUDGET"
            )
            assert any(p["key"]["symbol"] == "BTCNEW" for p in owner.status()["products"])
            for stop in owner.product_stops.values():
                stop.set()
            await asyncio.gather(*owner.product_tasks.values())

    asyncio.run(exercise())


def test_supplement_download_failure_does_not_cancel_live_persistence(tmp_path: Path) -> None:
    class BrokenDownload:
        def __call__(self, request: Any, timeout: int) -> Any:
            raise OSError("isolated supplementary download failed")

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path / "live")
        importer = HistoricalImporter(data_root=tmp_path / "supplement", opener=BrokenDownload())
        entry, _ = source()
        failed = asyncio.create_task(asyncio.to_thread(importer.import_entry, entry))
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path / "live", coverage=SMALL),
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients={"usdm": Client([trade(1)])},
            )
            p = next(p for p in products() if p.key.market == "um_perpetual")
            s = next(s for s in product_sources(p, SMALL) if s.family == "D06")
            owner.add_source(s)
            result = await owner.poll_once(owner.sources[s.key])
            assert result.valid and owner.sources[s.key].last_success is not None
            with pytest.raises(OSError, match="isolated"):
                await failed
            for spool in owner.spools.values():
                manifest = spool.close_and_seal()
                assert manifest is not None and manifest["record_count"] == 1

    asyncio.run(exercise())
