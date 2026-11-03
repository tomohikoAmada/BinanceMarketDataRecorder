from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.collector.coverage import ProductCapture
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_p1_depth_continuity import depth, snapshot
from tests.unit.test_p2_coverage import SMALL, products
from tests.unit.test_p3_availability import owner_at


@pytest.mark.parametrize("market,rpi", [("spot", False), ("um_perpetual", False),
                                         ("cm_perpetual", False), ("um_perpetual", True)])
@pytest.mark.parametrize("too_old", [False, True])
def test_wait_for_ws_to_reach_snapshot_and_retry_only_when_too_old(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: str, rpi: bool, too_old: bool,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        product = next(p for p in products() if p.key.market == market)
        for source in product_sources(product, SMALL):
            owner.add_source(source)
            owner.sources[source.key].connected = True
        capture = ProductCapture(owner, product)
        request = Mock()
        monkeypatch.setattr(owner, "request_snapshot", request)
        stream = "rpi_depth" if rpi else "diff_depth"

        snap = snapshot(product.key.market, 100).model_copy(update={
            "symbol": product.key.symbol, "collector_instance_id": owner.instance_id,
            "collector_version": owner.version,
            "stream": "rpi_depth_snapshot" if rpi else "depth_snapshot",
        })
        capture.snapshot_persisted(snap)
        first, last = (150, 160) if too_old else (80, 90)
        old = depth(product.key.market, first, last, first - 1).model_copy(update={
            "symbol": product.key.symbol, "collector_instance_id": owner.instance_id,
            "collector_version": owner.version, "stream": stream,
        })
        capture.persisted(old)
        if too_old:
            request.assert_called_once()
            return
        request.assert_not_called()
        # Even with a >1s receive/persist delay, the original target is retained.
        caught_up = depth(product.key.market, 99, 101, 98).model_copy(update={
            "symbol": product.key.symbol, "collector_instance_id": owner.instance_id,
            "collector_version": owner.version, "stream": stream,
            "receive_time_utc_ns": old.receive_time_utc_ns + 2_000_000_000,
        })
        capture.persisted(caught_up)
        assert (capture.rpi.is_reliable if rpi
                else capture.readiness_snapshot().orderbook_synchronized)
        request.assert_not_called()
