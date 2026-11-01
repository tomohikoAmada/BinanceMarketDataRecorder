from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from pathlib import Path

import pytest

from binance_market_data_recorder.binance.public_sources import Source, product_sources
from binance_market_data_recorder.collector.availability import AvailabilityJournal
from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_p2_capture import Client
from tests.unit.test_p2_coverage import SMALL, products

BASE = 1_800_000_000_000_000_000


def flush(journal: AvailabilityJournal, catalog: Catalog) -> None:
    while journal.pending:
        batch = journal.batch()
        catalog.persist_source_availability(batch)
        journal.acknowledge(len(batch))


def test_exact_failure_recovery_and_restart_preserve_unavailable_history(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    key = "usdm/BTCUSDT/open_interest/rest"
    with Catalog(layout.catalog) as catalog:
        journal = AvailabilityJournal("one")
        journal.valid(key, BASE)
        journal.failure(key, BASE + 2_000_000_000, "TimeoutError")
        journal.valid(key, BASE + 5_000_000_000)
        flush(journal, catalog)
        # Crash/restart uncertainty begins at the last durable boundary, not the new start.
        restarted = AvailabilityJournal("two")
        restarted.previous = catalog.source_availability()
        state = restarted.ensure(key, BASE + 10_000_000_000)
        assert state.unavailable_since == BASE + 5_000_000_000
        restarted.valid(key, BASE + 12_000_000_000)
        flush(restarted, catalog)
        report = continuity_report(catalog, start=BASE, end=BASE + 12_000_000_000)
        source = report["sources"][0]
        assert source["unavailable_intervals"] == [(BASE + 2_000_000_000, BASE + 12_000_000_000)]
        assert source["unavailable_or_unknown_ns"] == 10_000_000_000
        assert source["latest_state"]["failure_count"] == 1
        assert source["latest_state"]["last_error_type"] == "TimeoutError"
        assert source["latest_state"]["unavailable_since_utc_ns"] is None


def test_open_gap_keeps_identity_and_original_cause_after_restart(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        journal = AvailabilityJournal("one")
        journal.valid("s", BASE)
        journal.failure("s", BASE + 1, "HttpResponseError")
        identity = journal.sources["s"].gap_id
        flush(journal, catalog)
        restarted = AvailabilityJournal("two")
        restarted.previous = catalog.source_availability()
        restarted.ensure("s", BASE + 10)
        restarted.valid("s", BASE + 20)
        flush(restarted, catalog)
        gaps = catalog.source_availability_intervals("s", BASE + 1, BASE + 21)
        assert len(gaps) == 1 and gaps[0]["gap_id"] == identity
        assert "HttpResponseError" in gaps[0]["evidence_json"]


def test_eligibility_ends_without_rewriting_prior_failure_and_can_resume(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        journal = AvailabilityJournal("one")
        journal.valid("s", BASE)
        journal.failure("s", BASE + 2, "TimeoutError")
        journal.suspend("s", BASE + 4)
        journal.resume("s", BASE + 8)
        journal.valid("s", BASE + 9)
        flush(journal, catalog)
        source = continuity_report(catalog, start=BASE, end=BASE + 9)["sources"][0]
        assert source["eligible_intervals"] == [(BASE, BASE + 4), (BASE + 8, BASE + 9)]
        assert source["unavailable_or_unknown_ns"] == 3
        assert source["eligible_ns"] == 5
        assert source["coverage"] == 0.4


def test_unknown_monitoring_tail_is_never_healthy_and_missing_source_stays_visible(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        journal = AvailabilityJournal("one")
        journal.valid("s", BASE)
        flush(journal, catalog)
        report = continuity_report(catalog, start=BASE, end=BASE + 10, source_ids=["s", "missing"])
        assert report["sources"][0]["status"] == "UNKNOWN_SOURCE"
        assert report["sources"][1]["coverage"] == 0


def test_journal_capacity_fails_explicitly_instead_of_dropping_evidence() -> None:
    journal = AvailabilityJournal("one", capacity=1)
    with pytest.raises(RuntimeError, match="bounded persistence queue"):
        journal.valid("s", BASE)
    assert len(journal.pending) == 1


def owner_at(root: Path, catalog: Catalog) -> SelectedAssetCapture:
    return SelectedAssetCapture(
        RecorderConfig(data_root=root, coverage=SMALL),
        logging.getLogger("p3"),
        "v",
        "i",
        ensure_storage_layout(root),
        catalog,
        clients={v: Client() for v in ("spot", "usdm", "coinm")},
    )


def test_rest_deadline_is_source_local_and_valid_recovery_retains_failure(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        owner.sources.clear()
        for symbol in ("BTCUSDT", "ETHUSDT"):
            owner.add_source(
                Source(
                    "D14",
                    "usdm",
                    "um_perpetual",
                    symbol,
                    "open_interest",
                    "rest",
                    "/fapi/v1/openInterest",
                    30,
                )
            )
        btc, eth = owner.sources.values()
        btc.first_due = eth.first_due = BASE
        btc.last_success = BASE
        eth.last_success = BASE + 50_000_000_000
        owner._observe_availability(BASE + 50_000_000_000)
        assert btc.availability["unavailable_since_utc_ns"] == BASE
        assert eth.availability["unavailable_since_utc_ns"] is None
        btc.error = "TimeoutError"
        owner.availability.failure("/".join(btc.source.key), BASE + 51_000_000_000, btc.error)
        btc.last_success, btc.error = BASE + 52_000_000_000, None
        owner._observe_availability(BASE + 52_000_000_000)
        assert btc.availability["last_error_type"] == "TimeoutError"
        assert btc.availability["unavailable_since_utc_ns"] is None
        assert not owner.product_tasks and not owner.failures


def test_depth_requires_bridge_sparse_connection_does_not_require_events(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        owner.sources.clear()
        product = next(p for p in products() if p.key.market == "um_perpetual")
        sources = product_sources(product, SMALL)
        depth = next(s for s in sources if s.stream == "diff_depth")
        sparse = replace(depth, stream="liquidation", family="D11")
        for source in (depth, sparse):
            owner.add_source(source)
            owner.sources[source.key].connected = True
        capture = ProductCapture(owner, product)
        owner.collectors[product.key] = capture
        owner._observe_availability(BASE)
        assert owner.sources[depth.key].availability["unavailable_since_utc_ns"] is not None
        assert owner.sources[sparse.key].availability["unavailable_since_utc_ns"] is None
        assert owner.sources[sparse.key].last_success is None


def test_availability_flush_failure_does_not_acknowledge_lost_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        owner.availability.valid("s", BASE)

        def broken(batch: object) -> None:
            raise OSError("disk full")

        monkeypatch.setattr(catalog, "persist_source_availability", broken)
        with pytest.raises(OSError, match="disk full"):
            asyncio.run(owner._flush_availability())
        assert len(owner.availability.pending) == 2


def test_shared_writer_stall_affects_only_its_group_and_recovery_is_explicit(
    tmp_path: Path,
) -> None:
    from binance_market_data_recorder.binance.public_websocket import stream_groups

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        owner.sources.clear()
        product = next(p for p in products() if p.key.market == "um_perpetual")
        source = next(s for s in product_sources(product, SMALL) if s.stream == "book_ticker")
        owner.add_source(source)
        state = owner.sources[source.key]
        state.first_due, state.connected = BASE, True
        collector = owner.make_group(
            stream_groups((source,), 128)[0], product, lambda e: None, lambda e: None
        )
        collector._pending_since_utc_ns = BASE
        collector._received_total = 1
        owner._observe_availability(BASE + 11_000_000_000)
        assert state.availability["unavailable_since_utc_ns"] == BASE
        collector._persisted_total = 1
        collector._pending_since_utc_ns = None
        owner._observe_availability(BASE + 12_000_000_000)
        assert state.availability["unavailable_since_utc_ns"] is None
        assert not owner.failures


def test_clock_reversal_and_monitoring_pause_are_unknown_until_recovery(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = owner_at(tmp_path, catalog)
        owner.sources.clear()
        source = Source(
            "D14",
            "usdm",
            "um_perpetual",
            "BTCUSDT",
            "open_interest",
            "rest",
            "/fapi/v1/openInterest",
            300,
        )
        owner.add_source(source)
        state = owner.sources[source.key]
        state.first_due = state.last_success = BASE
        owner._observe_availability(BASE)
        owner._observe_availability(BASE - 10)
        assert state.availability["reason"] == "CLOCK_DISCONTINUITY"
        owner._observe_availability(BASE - 5)
        assert state.availability["unavailable_since_utc_ns"] == BASE - 10
        owner._observe_availability(BASE + 1)
        assert state.availability["unavailable_since_utc_ns"] is None
        owner._observe_availability(BASE + 10_000_000_000)
        flush(owner.availability, catalog)
        gaps = catalog.source_availability_intervals(
            "/".join(source.key), BASE + 1, BASE + 10_000_000_001
        )
        assert any(gap["started_at_utc_ns"] == BASE + 1 for gap in gaps)


@pytest.mark.parametrize("empty", [False, True])
def test_closed_bar_old_or_empty_publication_never_refreshes_data_freshness(
    tmp_path: Path,
    empty: bool,
) -> None:
    import time

    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            product = next(p for p in products() if p.key.market == "um_perpetual")
            source = next(s for s in product_sources(product, SMALL) if s.lifecycle == "closed_bar")
            owner.add_source(source)
            now_ms = time.time_ns() // 1_000_000
            closed = now_ms - 600_000
            rows = (
                []
                if empty
                else [[closed - 59_999, "1", "1", "1", "1", "0", closed, "0", 0, "0", "0", "0"]]
            )
            owner.clients["usdm"] = Client(rows)
            state = owner.sources[source.key]
            result = await owner.poll_once(state)
            assert result.valid  # A valid saved response is distinct from a current native bar.
            assert state.error in {"DataPublicationPending", "DataPublicationStale"}
            assert state.publication_status == "PENDING"
            assert owner.product_tasks == {}
            for spool in owner.spools.values():
                spool.close_and_seal()

    asyncio.run(scenario())


@pytest.mark.parametrize("empty", [False, True])
def test_statistical_period_is_evaluated_against_requested_window(
    tmp_path: Path,
    empty: bool,
) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            product = next(p for p in products() if p.key.market == "cm_perpetual")
            source = next(
                s
                for s in product_sources(product, SMALL)
                if s.endpoint.startswith("/futures/data/") and s.family != "D31"
            )
            params = owner._parameters(source)
            rows = [] if empty else [{"timestamp": params["startTime"] - 1}]
            owner.clients[source.venue_market] = Client(rows)
            # Isolate publication-window validation from each family's required schema fields.
            source = replace(source, required_fields=())
            owner.add_source(source)
            state = owner.sources[source.key]
            result = await owner.poll_once(state)
            assert result.valid
            if empty:
                assert state.publication_status == "EMPTY_EVALUATED_QUERY_WINDOW"
                assert state.error is None
            else:
                assert state.error == "DataOutsideRequestedPeriod"
            for spool in owner.spools.values():
                spool.close_and_seal()

    asyncio.run(scenario())
