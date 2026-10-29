from __future__ import annotations

import asyncio
import base64
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.binance.public_websocket import envelope_for_group, stream_groups
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.replay import ManifestCatalog, ReplayQuery
from binance_market_data_recorder.replay.model import GapPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_p2_coverage import FIXTURES, SMALL, Response, directory, products


class Client:
    def __init__(self, model: Any = None) -> None:
        self.model = model
        self.closed = False

    def request(self, source: Any, params: Any) -> Response:
        return Response(self.model)

    def close(self) -> None:
        self.closed = True


def test_each_product_kind_flows_through_bounded_writer_and_independent_reader(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=SMALL),
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients={v: Client() for v in ("spot", "usdm", "coinm")},
            )
            expected = []
            samples = json.loads((FIXTURES / "ws-public-samples.json").read_bytes())
            for market in ("spot", "um_perpetual", "um_delivery", "cm_perpetual", "cm_delivery"):
                product = next(
                    p
                    for p in products()
                    if p.key.market == market
                    and p.key.symbol
                    in {"BTCUSDT", "BTCUSDT_261225", "BTCUSD_PERP", "BTCUSD_261225"}
                )
                source = next(
                    s for s in product_sources(product, SMALL) if s.stream == "diff_depth"
                )
                group = stream_groups((source,), 128)[0]
                wire = group.wire_names[0]
                sample = next(
                    s
                    for s in samples
                    if s["market"] == market
                    and s["symbol"] == product.key.symbol
                    and any(o["wire_stream"] == wire for o in s["observations"])
                )
                raw = base64.b64decode(
                    next(
                        o["raw_base64"] for o in sample["observations"] if o["wire_stream"] == wire
                    )
                )
                expected.append(raw)
                stop = asyncio.Event()

                class Connection:
                    delivered = False

                    def __init__(self, payload: bytes) -> None:
                        self.payload = payload

                    async def recv(self, decode: bool | None = None) -> bytes:
                        if not self.delivered:
                            self.delivered = True
                            return self.payload
                        await asyncio.Event().wait()
                        raise AssertionError("unreachable")

                    async def close(self, code: int = 1000, reason: str = "") -> None:
                        pass

                connection = Connection(raw)

                @asynccontextmanager
                async def opener(
                    url: str, connection: Connection = connection
                ) -> AsyncIterator[WebSocketConnection]:
                    assert "stream?streams=" in url
                    yield connection

                def persisted(envelope: EventEnvelope, selected_stop: asyncio.Event = stop) -> None:
                    selected_stop.set()

                collector = UsdMStreamCollector(
                    stream=group.name,
                    symbol=product.key.symbol,
                    route=group.route,
                    wire_name=wire,
                    combined_streams=(wire,),
                    websocket_root=group.root,
                    spool=owner.spool(product.key.market, product.key.symbol, group.name),
                    collector_instance_id="i",
                    collector_version="v",
                    logger=logging.getLogger("p2"),
                    envelope_factory=partial(envelope_for_group, group, product),
                    envelope_observer=persisted,
                    opener=opener,
                )
                await asyncio.wait_for(collector.run(stop), 3)
            chunks = load_source_chunks(layout=layout, catalog=catalog)
            restored = [
                record.envelope for chunk in chunks for record in iter_source_records(chunk)
            ]
            assert sorted(e.raw_payload for e in restored) == sorted(expected)
            assert {e.market for e in restored} == {p.key.market for p in products()}
            normalized = Normalizer(layout=layout, catalog=catalog).run()
            assert normalized.normalized_rows == len(expected)
            assert normalized.build_id is not None
            dataset = ManifestCatalog(tmp_path).open_build(normalized.build_id)
            for envelope in restored:
                replayed = list(
                    dataset.replay(
                        ReplayQuery(
                            markets=(envelope.market,),
                            streams=(envelope.stream,),
                            symbol=envelope.symbol,
                            gap_policy=GapPolicy.INCLUDE,
                        )
                    )
                )
                assert len(replayed) == 1

    asyncio.run(exercise())


def test_admission_new_product_missing_and_expiry_preserve_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_network(self: ProductCapture, stop: asyncio.Event) -> None:
        await stop.wait()

    monkeypatch.setattr(ProductCapture, "run", no_network)

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        config = RecorderConfig(
            data_root=tmp_path, coverage=SMALL.model_copy(update={"max_products": 1})
        )
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                config,
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients={v: Client() for v in ("spot", "usdm", "coinm")},
            )
            spot = directory("spot")
            spot["symbols"] = [r for r in spot["symbols"] if r["symbol"] in {"BTCUSDT", "ETHUSDT"}]
            await owner.reconcile("spot", spot)
            assert len(owner.collectors) == 1
            assert "NOT_ADMITTED_PRODUCT_BUDGET" in owner.admission.values()
            admitted = next(iter(owner.collectors))
            await owner.reconcile("spot", {"symbols": [], "rateLimits": []})
            assert admitted in owner.collectors
            assert owner.admission[admitted] == "MISSING_FROM_DIRECTORY"
            for event in owner.product_stops.values():
                event.set()
            await asyncio.gather(*owner.product_tasks.values())

    asyncio.run(exercise())


def test_polling_empty_and_failure_are_saved_without_refreshing_success(tmp_path: Path) -> None:
    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        clients = {v: Client([]) for v in ("spot", "usdm", "coinm")}
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=SMALL),
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients=clients,
            )
            source = next(
                s
                for p in products()
                if p.key.market == "cm_perpetual"
                for s in product_sources(p, SMALL)
                if s.family == "D06"
            )
            owner.add_source(source)
            state = owner.sources[source.key]
            empty = await owner.poll_once(state)
            success = state.last_success
            assert empty.valid and state.recent_trades is not None
            assert json.loads(empty.envelope.raw_payload)["response"]["model"]["poll"]["empty"]
            clients["coinm"].model = [{"id": 1}]  # Received but invalid, retained in Raw.
            failed = await owner.poll_once(state)
            assert not failed.valid and state.last_success == success and state.error
            for spool in owner.spools.values():
                spool.close_and_seal()
            records = [
                r.envelope
                for c in load_source_chunks(layout=layout, catalog=catalog)
                for r in iter_source_records(c)
            ]
            assert len(records) == 2 and "source_failed" in records[1].capture_flags

    asyncio.run(exercise())


def test_rest_storage_failure_is_fatal_and_does_not_become_a_network_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=SMALL),
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients={v: Client([]) for v in ("spot", "usdm", "coinm")},
            )
            source = next(
                s
                for p in products()
                if p.key.market == "cm_perpetual"
                for s in product_sources(p, SMALL)
                if s.family == "D06"
            )
            owner.add_source(source)
            spool = owner.spool(source.market, source.symbol, "rest_observations")

            def failed_write() -> int:
                raise OSError("synthetic write failure")

            monkeypatch.setattr(spool, "drain_all", failed_write)
            with pytest.raises(OSError, match="synthetic write failure"):
                await owner.poll_once(owner.sources[source.key])
            assert owner.sources[source.key].last_success is None

    asyncio.run(exercise())


def test_normal_stop_cancels_polls_still_waiting_for_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class WaitingBudget:
        async def acquire(self, charges: Any) -> None:
            await asyncio.Event().wait()

    monkeypatch.setattr(
        "binance_market_data_recorder.collector.coverage.shared_request_budget",
        lambda: WaitingBudget(),
    )

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        clients = {v: Client() for v in ("spot", "usdm", "coinm")}
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=SMALL),
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients=clients,
            )
            stop = asyncio.Event()
            task = asyncio.create_task(owner.run(stop))
            await asyncio.sleep(0.02)
            stop.set()
            await asyncio.wait_for(task, 1)
            assert all(client.closed for client in clients.values())
            assert all(state.last_success is None for state in owner.sources.values())

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "market", ["spot", "um_perpetual", "um_delivery", "cm_perpetual", "cm_delivery"]
)
def test_product_sequence_health_uses_its_directory_identity_and_snapshot(
    tmp_path: Path,
    market: str,
) -> None:
    from binance_market_data_recorder.binance.public_rest import capture_public_response

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = SelectedAssetCapture(
            RecorderConfig(data_root=tmp_path, coverage=SMALL),
            logging.getLogger("p2"),
            "v",
            "i",
            layout,
            catalog,
            clients={"fixture": Client()},
        )
        product = next(p for p in products() if p.key.market == market)
        sources = product_sources(product, SMALL)
        for source in sources:
            owner.add_source(source)
        capture = ProductCapture(owner, product)
        for group in stream_groups(
            tuple(s for s in sources if s.stream in {"diff_depth", "book_ticker", "agg_trade"}), 128
        ):
            capture.lifecycle(group, "connected")

        def receive(stream: str, values: dict[str, Any]) -> EventEnvelope:
            source = next(s for s in sources if s.stream == stream)
            group = stream_groups((source,), 128)[0]
            data = {
                "s": product.key.symbol,
                "ps": product.pair,
                "st": product.symbol_type,
                "E": 1000,
                "T": 999,
                **values,
            }
            if market == "spot":
                data.pop("st")
            return envelope_for_group(
                group,
                product,
                raw_payload=json.dumps({"stream": group.wire_names[0], "data": data}).encode(),
                connection_id="c",
                collector_instance_id="i",
                collector_version="v",
                receive_time_utc_ns=1001,
                receive_monotonic_ns=2001,
            )

        capture.persisted(
            receive(
                "diff_depth", {"e": "depthUpdate", "U": 41, "u": 42, "pu": 40, "b": [], "a": []}
            )
        )
        capture.persisted(
            receive(
                "book_ticker", {"e": "bookTicker", "u": 42, "b": "1", "B": "2", "a": "3", "A": "4"}
            )
        )
        capture.persisted(
            receive(
                "agg_trade",
                {"e": "aggTrade", "a": 5, "f": 6, "l": 7, "p": "1", "q": "2", "m": False},
            )
        )
        source = next(s for s in sources if s.stream == "depth_snapshot")
        snapshot = capture_public_response(
            source,
            lambda s, p: Response({"lastUpdateId": 41, "bids": [["1", "2"]], "asks": [["3", "4"]]}),
            collector_instance_id="i",
            collector_version="v",
        ).envelope
        capture.snapshot_persisted(snapshot)
        assert capture.readiness_snapshot().ready
        assert capture.readiness_snapshot().public_dict()["full_orderbook_evaluated"] is False
        capture.persisted(
            receive(
                "diff_depth", {"e": "depthUpdate", "U": 51, "u": 52, "pu": 49, "b": [], "a": []}
            )
        )
        assert not capture.readiness_snapshot().ready


def test_repeated_snapshot_requests_do_not_push_recovery_into_the_future(tmp_path: Path) -> None:
    import time

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = SelectedAssetCapture(
            RecorderConfig(data_root=tmp_path, coverage=SMALL),
            logging.getLogger("p2"),
            "v",
            "i",
            layout,
            catalog,
            clients={"fixture": Client()},
        )
        product = next(p for p in products() if p.key.market == "um_perpetual")
        for source in product_sources(product, SMALL):
            owner.add_source(source)
        key = (product.venue_market, product.key.symbol, "depth_snapshot", "rest")
        state = owner.sources[key]
        state.next_due = float("inf")
        owner.request_snapshot(product)
        first = state.next_due
        for _ in range(1000):
            owner.request_snapshot(product)
        assert state.next_due == first
        assert owner._snapshot_after[key] == first + 1
        state.error = "HttpResponseError"
        state.next_due = time.monotonic() + 60
        retry = state.next_due
        owner.request_snapshot(product)
        assert state.next_due == retry


@pytest.mark.parametrize("stream", ["diff_depth", "rpi_depth"])
def test_depth_bootstrap_overflow_can_recover_with_a_new_snapshot(
    tmp_path: Path,
    stream: str,
) -> None:
    from binance_market_data_recorder.binance.public_rest import capture_public_response

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = SelectedAssetCapture(
            RecorderConfig(data_root=tmp_path, coverage=SMALL, ingress_queue_capacity=2),
            logging.getLogger("p2"),
            "v",
            "i",
            layout,
            catalog,
            clients={"fixture": Client()},
        )
        product = next(p for p in products() if p.key.market == "um_perpetual")
        sources = product_sources(product, SMALL)
        for source in sources:
            owner.add_source(source)
        capture = ProductCapture(owner, product)
        source = next(s for s in sources if s.stream == stream)
        group = stream_groups((source,), 128)[0]

        def receive(first: int, final: int, previous: int) -> None:
            raw = json.dumps(
                {
                    "stream": group.wire_names[0],
                    "data": {
                        "s": product.key.symbol,
                        "ps": product.pair,
                        "st": 1,
                        "e": "depthUpdate",
                        "E": 1000,
                        "T": 999,
                        "U": first,
                        "u": final,
                        "pu": previous,
                        "b": [],
                        "a": [],
                    },
                }
            ).encode()
            capture.persisted(
                envelope_for_group(
                    group,
                    product,
                    raw_payload=raw,
                    connection_id="c",
                    collector_instance_id="i",
                    collector_version="v",
                    receive_time_utc_ns=1001,
                    receive_monotonic_ns=2001,
                )
            )

        # Three arrivals exhaust a two-entry bootstrap buffer. Raw is already
        # persisted; recovering health must not require a process restart.
        receive(41, 42, 40)
        receive(43, 44, 42)
        receive(45, 46, 44)
        receive(47, 48, 46)
        snapshot_source = next(
            s
            for s in sources
            if s.stream == ("rpi_depth_snapshot" if stream == "rpi_depth" else "depth_snapshot")
        )
        snapshot = capture_public_response(
            snapshot_source,
            lambda s, p: Response({"lastUpdateId": 47, "bids": [], "asks": []}),
            collector_instance_id="i",
            collector_version="v",
        ).envelope
        capture.snapshot_persisted(snapshot)
        if stream == "rpi_depth":
            assert capture.rpi.is_reliable
        else:
            assert capture.readiness_snapshot().orderbook_synchronized


def test_service_selects_discovery_owner_and_reports_all_physical_markets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from binance_market_data_recorder.service.runtime import ServiceRuntime
    from tests.unit.test_service_runtime import FakePowerAssertion, FakeSleepObserver

    clients = {v: Client(directory(v)) for v in ("spot", "usdm", "coinm")}
    monkeypatch.setattr(
        "binance_market_data_recorder.collector.coverage.PublicRestClient",
        lambda venue, proxy: clients[venue],
    )

    async def idle_product(self: ProductCapture, stop: asyncio.Event) -> None:
        await stop.wait()

    async def offline_shared(self: SelectedAssetCapture) -> None:
        pass

    def legacy_factory(*args: Any) -> Any:
        raise AssertionError("discovery mode must not start the legacy fixed-symbol owner")

    monkeypatch.setattr(ProductCapture, "run", idle_product)
    monkeypatch.setattr(SelectedAssetCapture, "_reconcile_shared", offline_shared)

    async def exercise() -> None:
        runtime = ServiceRuntime(
            config=RecorderConfig(data_root=tmp_path, coverage=SMALL, prevent_sleep=False),
            logger=logging.getLogger("p2.runtime"),
            collector_factory=legacy_factory,
            sleep_observer_factory=FakeSleepObserver,
            power_assertion=FakePowerAssertion(),
        )
        task = asyncio.create_task(runtime.run())
        try:
            for _ in range(200):
                if task.done():
                    task.result()
                    pytest.fail("service returned during discovery")
                owner = runtime._supervisor
                if (
                    isinstance(owner, SelectedAssetCapture)
                    and len(owner.directory.observed_markets) == 3
                ):
                    break
                await asyncio.sleep(0.01)
            else:
                pytest.fail("public fixture directories were not discovered")
            state = runtime._state_document()
            assert state["expected_product_count"] == len(products())
            product_states = state["products"]
            assert isinstance(product_states, dict)
            assert {k for k, values in product_states.items() if values} == {
                "spot",
                "um_perpetual",
                "um_delivery",
                "cm_perpetual",
                "cm_delivery",
            }
            assert state["core_ready"] is False  # No fixture WS was received.
            assert runtime.global_side_data is None
        finally:
            runtime.request_stop("TEST_STOP")
            await asyncio.wait_for(task, 5)
        final = runtime.state_store.read()
        assert final is not None and final["status"] == "STOPPED"
        assert all(client.closed for client in clients.values())

    asyncio.run(exercise())


def test_successful_rest_polls_do_not_invent_connection_loss_in_the_manifest(
    tmp_path: Path,
) -> None:
    from binance_market_data_recorder.binance.public_rest import capture_public_response
    from tests.unit.test_p2_coverage import trade

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=tmp_path, coverage=SMALL),
                logging.getLogger("p2"),
                "v",
                "i",
                layout,
                catalog,
                clients={"fixture": Client()},
            )
            product = next(p for p in products() if p.key.market == "cm_perpetual")
            source = next(s for s in product_sources(product, SMALL) if s.family == "D06")
            first = capture_public_response(
                source,
                lambda s, p: Response([trade(1)]),
                collector_instance_id="i",
                collector_version="v",
            )
            second = capture_public_response(
                source,
                lambda s, p: Response([trade(1), trade(2)]),
                collector_instance_id="i",
                collector_version="v",
            )
            assert first.envelope.connection_id == second.envelope.connection_id
            assert (
                json.loads(first.envelope.raw_payload)["request"]["request_id"]
                != (json.loads(second.envelope.raw_payload)["request"]["request_id"])
            )
            await owner.persist_rest(first.envelope)
            await owner.persist_rest(second.envelope)
            spool = owner.spool(source.market, source.symbol, "rest_observations")
            manifest = spool.close_and_seal()
            assert manifest is not None and manifest["complete"] is True
            assert manifest["gap"] is False

    asyncio.run(exercise())
