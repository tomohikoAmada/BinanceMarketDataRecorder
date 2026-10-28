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


@pytest.mark.parametrize("venue", ["spot", "usdm", "coinm"])
@pytest.mark.parametrize("status", [429, 418])
def test_rejection_blocks_new_admission_before_persistence_wait(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    venue: str,
    status: int,
) -> None:
    from binance_market_data_recorder.binance.public_sources import Source
    from binance_market_data_recorder.domain.event import EventEnvelope, Market
    from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks

    async def exercise() -> None:
        second_admission = asyncio.Event()
        persistence_entered, release = asyncio.Event(), asyncio.Event()
        calls: list[tuple[str, str]] = []

        class ObservedBudget(RequestBudget):
            admissions = 0

            async def acquire(self, charges: Any) -> None:
                self.admissions += 1
                if self.admissions == 2:
                    second_admission.set()
                await super().acquire(charges)

        budget = ObservedBudget()
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.coverage.shared_request_budget", lambda: budget
        )

        class Client:
            def request(self, source: Any, params: Any) -> _ObservedResponse:
                import time

                calls.append((source.venue_market, source.stream))
                rejected = source.venue_market == venue and source.stream == "exchange_info"
                return _ObservedResponse(
                    status if rejected else 500,
                    {"Retry-After": "600"} if rejected else {},
                    {"code": -1003, "msg": "offline rejection"},
                    time.time_ns(),
                    time.monotonic_ns(),
                )

            def close(self) -> None:
                pass

        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(
                    data_root=tmp_path, coverage=CoverageConfig(underlying_assets=("BTC",))
                ),
                logging.getLogger("p2.cooldown.order"),
                "v",
                "i",
                layout,
                catalog,
                clients={v: Client() for v in ("spot", "usdm", "coinm")},
            )
            first = directory_source(venue, owner.coverage)
            successor_venue = {"spot": "spot", "usdm": "coinm", "coinm": "usdm"}[venue]
            routes: dict[str, tuple[Market, str, str]] = {
                "spot": ("spot", "BTCUSDT", "/api/v3/depth"),
                "usdm": ("um_perpetual", "BTCUSDT", "/fapi/v1/depth"),
                "coinm": ("cm_perpetual", "BTCUSD_PERP", "/dapi/v1/depth"),
            }
            market, symbol, path = routes[successor_venue]
            # A separate source, including the other derivative host.
            second = Source(
                "D02", successor_venue, market, symbol, "depth_snapshot", "rest", path, 0
            )
            owner.add_source(second)
            persist = owner.persist_rest

            async def held_persist(envelope: EventEnvelope) -> None:
                if envelope.market == first.market and envelope.stream == "exchange_info":
                    persistence_entered.set()
                    await release.wait()
                await persist(envelope)

            monkeypatch.setattr(owner, "persist_rest", held_persist)
            first_task = asyncio.create_task(owner.poll_once(owner.sources[first.key]))
            second_task = None
            try:
                await asyncio.wait_for(persistence_entered.wait(), 2)
                second_task = asyncio.create_task(owner.poll_once(owner.sources[second.key]))
                await asyncio.wait_for(second_admission.wait(), 2)
                # This checkpoint deterministically follows take(): no scheduler
                # yield separates admission and last_attempt. No sleep/race needed.
                assert owner.sources[second.key].last_attempt is None
                assert second.key not in owner._wire_started
                assert calls == [(venue, "exchange_info")]
                assert budget.delay(second.charges) > 599
                second_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await second_task
                # Storage waiting in one pool must not freeze the other pool.
                independent = directory_source(
                    "usdm" if venue == "spot" else "spot", owner.coverage
                )
                await asyncio.wait_for(owner.poll_once(owner.sources[independent.key]), 2)
                release.set()
                result = await asyncio.wait_for(first_task, 2)
                assert not result.valid and owner.sources[first.key].last_success is None
            finally:
                release.set()
                if second_task is not None and not second_task.done():
                    second_task.cancel()
                await asyncio.gather(
                    first_task, *([second_task] if second_task else []), return_exceptions=True
                )
                for spool in owner.spools.values():
                    spool.close_and_seal()
            records = [
                r.envelope
                for c in load_source_chunks(layout=layout, catalog=catalog)
                for r in iter_source_records(c)
            ]
            rejected = next(r for r in records if r.market == first.market)
            assert rejected.raw_payload == result.envelope.raw_payload
            assert "source_failed" in rejected.capture_flags
            assert len(records) == 2  # Rejection plus the independent pool; no escaped request.

    asyncio.run(exercise())


@pytest.mark.parametrize("status", [429, 418])
def test_cooldown_is_active_even_if_failure_persistence_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    import time

    from binance_market_data_recorder.domain.event import EventEnvelope

    async def exercise() -> None:
        budget = RequestBudget()
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.coverage.shared_request_budget", lambda: budget
        )

        class Client:
            def request(self, source: Any, params: Any) -> _ObservedResponse:
                return _ObservedResponse(
                    status, {"Retry-After": "600"}, {}, time.time_ns(), time.monotonic_ns()
                )

            def close(self) -> None:
                pass

        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(
                    data_root=tmp_path, coverage=CoverageConfig(underlying_assets=("BTC",))
                ),
                logging.getLogger("p2.cooldown.storage"),
                "v",
                "i",
                layout,
                catalog,
                clients={"usdm": Client()},
            )
            source = directory_source("usdm", owner.coverage)

            async def fail(envelope: EventEnvelope) -> None:
                assert budget.delay(source.charges) > 599
                assert "source_failed" in envelope.capture_flags
                raise OSError("synthetic persistence failure")

            monkeypatch.setattr(owner, "persist_rest", fail)
            with pytest.raises(OSError, match="synthetic persistence failure"):
                await owner.poll_once(owner.sources[source.key])
            assert budget.delay(directory_source("coinm", owner.coverage).charges) > 599
            assert owner.sources[source.key].last_success is None
            assert owner.sources[source.key].count == 0

    asyncio.run(exercise())
