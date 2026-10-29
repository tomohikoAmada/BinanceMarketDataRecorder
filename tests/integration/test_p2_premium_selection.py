from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.binance.products import discover_products
from binance_market_data_recorder.binance.public_sources import (
    nominal_derivative_weight_per_minute,
    product_sources,
    source_plan,
)
from binance_market_data_recorder.binance.rest_representation import response_document
from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_p2_capture import Client
from tests.unit.test_p2_coverage import SMALL, Response, directory, products


@pytest.fixture
def offline(monkeypatch: pytest.MonkeyPatch) -> None:
    async def idle(self: ProductCapture, stop: asyncio.Event) -> None:
        await stop.wait()

    async def no_shared_network(self: SelectedAssetCapture) -> None:
        pass

    monkeypatch.setattr(ProductCapture, "run", idle)
    monkeypatch.setattr(SelectedAssetCapture, "_reconcile_shared", no_shared_network)


@pytest.mark.parametrize("venue", ["usdm", "coinm"])
def test_new_dated_contract_is_excluded_before_admission_and_polling(
    tmp_path: Path, offline: None, venue: str
) -> None:
    initial = SMALL.model_copy(update={"capture_dated_premium_klines": False})
    prefix = "um" if venue == "usdm" else "cm"
    p = next(p for p in products() if p.key.market == prefix + "_delivery")
    perp = next(
        q for q in products() if q.key.market == prefix + "_perpetual" and q.pair == p.pair
    )
    document = directory(venue)
    dated_row = next(r for r in document["symbols"] if r["symbol"] == p.key.symbol)
    document["symbols"] = [r for r in document["symbols"] if r["symbol"] == perp.key.symbol]
    new_symbol = p.pair + "_281229"
    expanded = {
        **document,
        "symbols": [
            *document["symbols"],
            {**dated_row, "symbol": new_symbol, "deliveryDate": 2_000_000_000_000},
        ],
    }
    candidates = discover_products(venue, expanded, initial)
    selected_weight = nominal_derivative_weight_per_minute(
        source_plan(candidates, initial), initial
    )
    full_weight = nominal_derivative_weight_per_minute(source_plan(candidates, SMALL), SMALL)
    assert full_weight > selected_weight
    initial = initial.model_copy(
        update={
            "max_steady_derivative_weight_per_minute": (selected_weight + full_weight) / 2,
        }
    )
    calls = []

    class RecordingClient(Client):
        def request(self, source: Any, params: Any) -> Response:
            calls.append(source)
            assert not (
                source.market.endswith("delivery")
                and source.endpoint.endswith("/premiumIndexKlines")
            )
            return Response([[0, "0.1", "0.2", "0.01", "0.1", "0", 59_999, "0", 0, "0", "0", "0"]])

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=initial),
                logging.getLogger("p2"),
                "v",
                "initial",
                layout,
                catalog,
                clients={venue: RecordingClient()},
            )
            await owner.reconcile(venue, document)
            original_started = owner.status()["source_selection"]["effective_from_utc_ns"]
            # Admission must use the reduced plan, rather than dropping work afterwards.
            await owner.reconcile(venue, expanded)
            assert any(k.symbol == new_symbol for k in owner.collectors)
            report = owner.status(include_sources=False)["source_selection"]
            assert report["effective_from_utc_ns"] == original_started
            assert report["collector_instance_id"] == "initial"
            assert report["uncaptured_source_count"] == 1
            assert report["uncaptured_sources"][0]["symbol"] == new_symbol
            assert report["nominal_steady_derivative_weight_per_minute"] == pytest.approx(
                selected_weight
            )
            # Selected includes the D31 source waiting for its delivery lifecycle.
            assert report["selected_source_count"] + 3 == len(owner.sources)
            events = catalog.operational_events(event_type="PRODUCT_DIRECTORY_OBSERVED")
            evidence = cast(dict[str, Any], events[-1]["evidence"])
            assert evidence["source_selection"] == report
            assert evidence["config"]["capture_dated_premium_klines"] is False
            stop = asyncio.Event()
            due = []
            for state in owner.sources.values():
                selected_bar = state.source.family == "D23" and state.source.transport == "rest"
                state.next_due = 0 if selected_bar else float("inf")
                if selected_bar:
                    due.append(state.source.key)
            assert len(due) == 4  # Per-product mark/premium; index shared once per pair.
            persisted = owner.persist_rest
            seen = set()

            async def observe(envelope: Any) -> None:
                await persisted(envelope)
                seen.add((envelope.market, envelope.symbol, envelope.stream))
                if len(seen) == len(due):
                    stop.set()

            owner.persist_rest = observe  # type: ignore[method-assign]
            await asyncio.wait_for(owner.run(stop), timeout=3)
            assert {s.key for s in calls} == set(due)
            assert all(s.failures == 0 for s in owner.sources.values())
            assert any(
                s.market.endswith("perpetual") and s.stream.startswith("premium_") for s in calls
            )
            assert {s.stream for s in calls if s.symbol == new_symbol} == {"mark_price_bars_1m"}
            shared_index = next(s for s in calls if s.stream == "index_price_bars_1m")
            assert new_symbol in shared_index.products and perp.key.symbol in shared_index.products

    asyncio.run(exercise())


@pytest.mark.parametrize("market", ["um_delivery", "cm_delivery"])
def test_new_selection_does_not_rewrite_saved_dated_failure_or_old_scope(
    tmp_path: Path, offline: None, market: str
) -> None:
    class FailedResponse(Response):
        status = 400

    class FailedClient(Client):
        def request(self, source: Any, params: Any) -> Response:
            return FailedResponse({"code": -1121, "msg": "Invalid symbol."})

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        p = next(p for p in products() if p.key.market == market)
        with Catalog(layout.catalog) as catalog:
            legacy = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=SMALL),
                logging.getLogger("p2"),
                "v",
                "old",
                layout,
                catalog,
                clients={p.venue_market: FailedClient()},
            )
            await legacy.reconcile(p.venue_market, directory(p.venue_market))
            source = next(
                s for s in product_sources(p, SMALL) if s.stream == "premium_price_bars_1m"
            )
            result = await legacy.poll_once(legacy.sources[source.key])
            assert not result.valid and result.status == 400
            for spool in legacy.spools.values():
                spool.close_and_seal()
            before = catalog.operational_events(event_type="PRODUCT_DIRECTORY_OBSERVED")
            original = cast(dict[str, Any], before[-1]["evidence"])
            assert original["source_selection"]["uncaptured_source_count"] == 0
            assert original["config"]["capture_dated_premium_klines"] is True
            for stop in legacy.product_stops.values():
                stop.set()
            await asyncio.gather(*legacy.product_tasks.values())
            initial = SelectedAssetCapture(
                RecorderConfig(
                    data_root=tmp_path,
                    coverage=SMALL.model_copy(update={"capture_dated_premium_klines": False}),
                ),
                logging.getLogger("p2"),
                "v",
                "new",
                layout,
                catalog,
                clients={p.venue_market: Client()},
            )
            await initial.reconcile(p.venue_market, directory(p.venue_market))
            assert source.key not in initial.sources
            after = catalog.operational_events(event_type="PRODUCT_DIRECTORY_OBSERVED")
            assert after[: len(before)] == before
            current = cast(dict[str, Any], after[-1]["evidence"])
            assert (
                current["source_selection"]["effective_from_utc_ns"]
                > original["source_selection"]["effective_from_utc_ns"]
            )
            restored = [
                r.envelope
                for chunk in load_source_chunks(layout=layout, catalog=catalog)
                for r in iter_source_records(chunk)
            ]
            assert len(restored) == 1
            assert restored[0].raw_payload == result.envelope.raw_payload
            assert "source_failed" in restored[0].capture_flags
            assert response_document(json.loads(restored[0].raw_payload)) == {
                "code": -1121,
                "msg": "Invalid symbol.",
            }
            for stop in initial.product_stops.values():
                stop.set()
            await asyncio.gather(*initial.product_tasks.values())

    asyncio.run(exercise())
