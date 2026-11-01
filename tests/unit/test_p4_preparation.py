"""Regressions from P4's retained live preparation failures."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.public_sources import (
    product_sources,
    shared_sources,
    source_plan,
    source_selection_report,
)
from binance_market_data_recorder.metrics.model import MetricAggregate
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_p2_capture import Client
from tests.unit.test_p2_coverage import SMALL, products
from tests.unit.test_p3_availability import owner_at


@pytest.mark.parametrize("count", [1, 15, 100, 101])
def test_execution_rule_array_matches_exchange_character_constraint_and_weight(count: int) -> None:
    template = next(p for p in products() if p.key.market == "spot")
    selected = tuple(replace(template, key=replace(template.key, symbol=f"BTCQ{i}"))
                     for i in range(count))
    batches = [s for s in shared_sources(selected) if s.family == "S09"]
    assert len(batches) == (count + 99) // 100
    symbols = []
    for batch in batches:
        wire = dict(batch.parameters)["symbols"]
        assert isinstance(wire, str) and " " not in wire
        decoded = json.loads(wire)
        assert batch.charges["spot_weight"] == min(2 * len(decoded), 40)
        symbols.extend(decoded)
    assert symbols == sorted(p.key.symbol for p in selected)


@pytest.mark.parametrize("market", ["um_perpetual", "cm_perpetual"])
@pytest.mark.parametrize("case", ["overlap", "empty", "only_old", "too_old", "future"])
def test_taker_overlap_keeps_raw_and_evaluates_only_the_closed_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, market: str, case: str,
) -> None:
    # Fix a closed 5-minute boundary so request and result cannot straddle a wall-clock tick.
    now = 1_800_000_123_000_000_000
    monkeypatch.setattr("binance_market_data_recorder.collector.coverage.time.time_ns", lambda: now)

    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            product = next(p for p in products() if p.key.market == market)
            source = replace(next(s for s in product_sources(product, SMALL) if s.family == "D16"),
                             required_fields=("timestamp",))
            params = owner._parameters(source)
            start, end = params["startTime"], params["endTime"] - 300_000
            timestamps = {
                "overlap": [start - 300_000, start, start + 300_000],
                "empty": [], "only_old": [start - 300_000],
                "too_old": [start - 600_000, start], "future": [start, end + 1],
            }[case]
            rows = [{"timestamp": t, "buyVol": "1.000000000000000001"} for t in timestamps]
            owner.clients[source.venue_market] = Client(rows)
            owner.add_source(source)
            state = owner.sources[source.key]
            result = await owner.poll_once(state)
            assert result.valid
            assert params["limit"] == 3
            expected_error = {"only_old": "DataPublicationStale",
                              "too_old": "DataOutsideRequestedPeriod",
                              "future": "DataOutsideRequestedPeriod"}.get(case)
            assert state.error == expected_error
            if case == "overlap":
                assert state.publication_time_ms == start + 300_000
                assert state.publication_status == "CURRENT"
            elif case == "empty":
                assert state.publication_time_ms is None
                assert state.publication_status == "EMPTY_EVALUATED_QUERY_WINDOW"
            else:
                health = owner.availability.sources["/".join(source.key)]
                assert health.unavailable_since is not None
            for spool in owner.spools.values():
                spool.close_and_seal()
            owner.metrics.flush()
            assert owner.metrics.failure_count == 0
            batches = list(catalog.iter_metric_batches_through("2099-01-01"))
            counters: dict[str, int] = {}
            for batch in batches:
                aggregate = MetricAggregate.from_document(batch["aggregate"])
                for key, value in aggregate.counters.items():
                    counters[key] = counters.get(key, 0) + value
            assert counters["rest_responses"] == counters["raw_records_written"] == 1
            assert counters["sealed_chunks"] == 1
            assert counters.get("websocket_messages", 0) == 0
            saved = list(iter_source_records(load_source_chunks(layout=layout, catalog=catalog)[0]))
            assert len(saved) == 1
            record = json.loads(saved[0].envelope.raw_payload)
            assert record["response"]["model"] == rows
            assert record["request"]["parameters"] == params
            assert record["request"]["evaluated_period"] == {
                "start_time_ms": start, "end_time_ms": end,
                "allowed_preceding_overlap_ms": 300_000,
            }

    asyncio.run(scenario())


def test_dated_adl_exception_is_explicit_and_does_not_remove_perpetual_or_other_sources() -> None:
    complete = source_plan(products(), SMALL)
    config = SMALL.model_copy(update={"capture_dated_adl_risk": False})
    selected = source_plan(products(), config)
    omitted = {s.key for s in complete} - {s.key for s in selected}
    expected = {s.key for s in complete if s.market == "um_delivery" and s.family == "D28"}
    assert omitted == expected and expected
    assert {s.key for s in selected} - {s.key for s in complete} == set()
    report = source_selection_report(products(), config, selected)
    rows = report["uncaptured_sources"]
    assert isinstance(rows, list) and len(rows) == len(expected)
    assert all(r["official_applicability"] == "UNKNOWN"
               and r["status"] == "OWNER_APPROVED_UNCAPTURED" for r in rows)
    assert report["full_target_source_count"] == len(complete)
    # Future dated identities receive the same explicit selection policy before scheduling.
    dated = next(p for p in products() if p.key.market == "um_delivery")
    future = replace(dated, key=replace(dated.key, symbol="BTCUSDT_990625"))
    assert not any(s.family == "D28" for s in product_sources(future, config))
    assert any(s.family == "D28" for s in product_sources(future, SMALL))


def test_btc_sized_availability_queue_drains_multiple_bounded_transactions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        # Isolate the deterministic transaction/count bound from test-host I/O speed.
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.coverage.AVAILABILITY_FLUSH_SECONDS", 60.0,
        )
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            batch_sizes = []
            persist = catalog.persist_source_availability

            def measured(batch: list[dict[str, Any]]) -> None:
                batch_sizes.append(len(batch))
                persist(batch)

            monkeypatch.setattr(catalog, "persist_source_availability", measured)
            for i in range(2233):
                owner.availability.valid(f"source-{i}", 1_800_000_000_000_000_000)
            queued = len(owner.availability.pending)
            await owner._flush_availability()
            assert 1 < len(batch_sizes) <= 32
            assert max(batch_sizes) <= 128
            assert queued - len(owner.availability.pending) == sum(batch_sizes)
            await owner._flush_availability(all_pending=True)
            assert not owner.availability.pending
            assert len(catalog.source_availability()) == 2233
            assert all(s["unavailable_since_utc_ns"] is None
                       for s in catalog.source_availability().values())
            # A full ten-second checkpoint for all BTC sources must make progress
            # without requiring capture/shutdown to drain the retained journal.
            for i in range(2233):
                owner.availability.valid(f"source-{i}", 1_800_000_011_000_000_000)
            for _ in range(4):
                await owner._flush_availability()
            assert not owner.availability.pending

    asyncio.run(scenario())
