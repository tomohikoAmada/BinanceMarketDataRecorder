"""Deterministic MS2 configuration, topology, transport and ownership acceptance."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from binance_market_data_recorder.binance.public_websocket import stream_groups
from binance_market_data_recorder.binance.spot.exchange_info import capture_spot_exchange_info
from binance_market_data_recorder.binance.spot.rest import (
    SpotSnapshotRequester,
)
from binance_market_data_recorder.binance.spot.rest import (
    capture_depth_snapshot as spot_depth,
)
from binance_market_data_recorder.binance.spot.schema import (
    SpotStream,
)
from binance_market_data_recorder.binance.spot.schema import (
    envelope_from_websocket_frame as spot_frame,
)
from binance_market_data_recorder.binance.usdm.rest import capture_depth_snapshot as usdm_depth
from binance_market_data_recorder.binance.usdm.schema import (
    UsdMStream,
)
from binance_market_data_recorder.binance.usdm.schema import (
    envelope_from_websocket_frame as usdm_frame,
)
from binance_market_data_recorder.binance.usdm.side_data_rest import (
    FIVE_MINUTE_KINDS,
)
from binance_market_data_recorder.binance.usdm.side_data_schema import (
    UsdMSideStream,
    envelope_from_side_stream_frame,
)
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import ConfigurationError, RecorderConfig, load_config
from binance_market_data_recorder.domain.product import ProductKey, configured_products
from binance_market_data_recorder.service.runtime import ServiceRuntime
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.supervisor.readiness import CORE_STREAMS, ReadinessSnapshot
from tests.r3_support import R3_SYMBOLS

FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance"


@pytest.mark.parametrize(
    ("text", "spot", "usdm"),
    [
        ("", ("BTCUSDT",), ("BTCUSDT",)),
        ('spot_symbols=["ETHUSDT"]', ("ETHUSDT",), ()),
        ('usdm_symbols=["SOLUSDT"]', (), ("SOLUSDT",)),
        ('spot_symbols=[]\nusdm_symbols=["ETHUSDT"]', (), ("ETHUSDT",)),
        ('spot_symbols=["ETHUSDT"]\nusdm_symbols=[]', ("ETHUSDT",), ()),
        (
            'spot_symbols=["ethusdt", "btcusdt"]\nusdm_symbols=["ethusdt"]',
            ("ETHUSDT", "BTCUSDT"),
            ("ETHUSDT",),
        ),
        ('spot_symbols=["NOTALISTEDPRODUCT"]', ("NOTALISTEDPRODUCT",), ()),
    ],
)
def test_toml_presence_and_canonical_topology(
    tmp_path: Path,
    text: str,
    spot: tuple[str, ...],
    usdm: tuple[str, ...],
) -> None:
    path = tmp_path / "recorder.toml"
    path.write_text(f'[recorder]\ndata_root="{tmp_path / "data"}"\n{text}\n')
    config = load_config(config_file=path, environ={}).config
    assert config.spot_symbols == spot
    assert config.usdm_symbols == usdm
    assert config.public_dict()["spot_symbols"] == list(spot)
    assert config.public_dict()["usdm_symbols"] == list(usdm)
    products = configured_products(config.spot_symbols, config.usdm_symbols)
    assert products == tuple(sorted(set(products)))
    assert len(products) == len(spot) + len(usdm)


@pytest.mark.parametrize(
    "values",
    [
        {"spot_symbols": [], "usdm_symbols": []},
        {"spot_symbols": []},
        {"usdm_symbols": []},
        {"spot_symbols": ["ethusdt", "ETHUSDT"]},
        *(
            {"spot_symbols": [symbol]}
            for symbol in [
                "",
                " ETHUSDT",
                "ETH USDT",
                "ETH\nUSDT",
                "ETH\x00USDT",
                "ETH\x85USDT",
                "ETH\u200bUSDT",
                "../ETHUSDT",
                "ETH@aggTrade",
                "ETH?symbol=BTC",
            ]
        ),
        {"spot_symbols": "ETHUSDT"},
        {"spot_symbols": [1]},
        {"spot_symbols": None},
    ],
)
def test_invalid_configuration_fails_at_boundary(tmp_path: Path, values: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        RecorderConfig(data_root=tmp_path, **values)


def test_no_symbol_environment_surface(tmp_path: Path) -> None:
    with pytest.raises(ConfigurationError, match="unknown environment"):
        load_config(environ={"BINANCE_MARKET_RECORDER_SPOT_SYMBOLS": "ETHUSDT"})


@pytest.fixture
def constructed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    calls: list[str] = []
    runtimes: list[ServiceRuntime] = []

    class Client:
        def __init__(self, venue: str, proxy: Any) -> None:
            calls.append(venue)

        def request(self, source: Any, params: Any) -> Any:
            raise AssertionError("construction must not perform network I/O")

        def close(self) -> None:
            pass

    monkeypatch.setattr("binance_market_data_recorder.collector.coverage.PublicRestClient", Client)

    def build(**values: Any) -> tuple[ServiceRuntime, list[str]]:
        runtime = ServiceRuntime(
            config=RecorderConfig(data_root=tmp_path, **values),
            logger=logging.getLogger("test.ms2"),
        )
        runtime._catalog = Catalog(runtime.layout.catalog)
        owner = runtime.supervisor_factory(
            runtime.config,
            runtime.logger,
            "test",
            runtime.service_instance_id,
            runtime.layout,
            runtime._catalog,
        )
        runtime._supervisor = owner
        runtime._collectors = owner.collectors
        runtimes.append(runtime)
        return runtime, calls

    yield build
    for runtime in runtimes:
        assert runtime._catalog is not None
        runtime._catalog.close()


def make_groups(owner: SelectedAssetCapture) -> None:
    for collector in owner.collectors.values():
        sources = tuple(
            state.source
            for state in collector.source_states.values()
            if state.source.transport == "ws"
        )
        for group in stream_groups(sources, owner.coverage.streams_per_connection):
            from functools import partial

            collector.groups.append(
                owner.make_group(
                    group,
                    collector.product,
                    collector.persisted,
                    partial(collector.lifecycle, group),
                )
            )


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"spot_symbols": ["ETHUSDT"]},
        {"usdm_symbols": ["SOLUSDT"]},
        {"spot_symbols": ["SOLUSDT", "BTCUSDT", "ETHUSDT"], "usdm_symbols": ["ETHUSDT", "SOLUSDT"]},
        {"spot_symbols": ["ETHUSDT"], "usdm_symbols": ["ETHUSDT"]},
        {"spot_symbols": ["UNUSUALQUOTE"]},
    ],
)
def test_exact_assembly_routes_and_zero_market_ownership(constructed: Any, values: Any) -> None:
    runtime, calls = constructed(**values)
    owner = runtime._supervisor
    assert isinstance(owner, SelectedAssetCapture)
    assert tuple(owner.collectors) == configured_products(
        runtime.config.spot_symbols, runtime.config.usdm_symbols
    )
    make_groups(owner)
    for key, collector in owner.collectors.items():
        assert collector.product.key == key
        assert collector.readiness_snapshot().symbol == key.symbol
        for stream in collector.groups:
            assert stream.symbol == stream.spool.symbol == key.symbol
            assert all(
                name.startswith(key.symbol.lower() + "@") for name in stream.combined_streams
            )
    assert set(calls) == ({"spot"} if runtime.config.spot_symbols else set()) | (
        {"usdm"} if runtime.config.usdm_symbols else set()
    )
    sources = [state.source for state in owner.sources.values()]
    assert not any("kline" in source.stream or source.venue_market == "coinm" for source in sources)
    for logical_stream in ("exchange_info", "funding_info"):
        assert sum(
            source.stream == logical_stream and source.venue_market == "usdm" for source in sources
        ) == bool(runtime.config.usdm_symbols)


def test_auxiliary_switches_and_intervals_map_without_scope_expansion(constructed: Any) -> None:
    runtime, _ = constructed(
        spot_symbols=["BTCUSDT", "ETHUSDT"],
        usdm_symbols=["ETHUSDT"],
        side_funding_info_enabled=False,
        side_exchange_info_enabled=False,
        side_mark_price_enabled=False,
        side_open_interest_interval_seconds=37.0,
        spot_exchange_info_interval_seconds=73.0,
    )
    owner = runtime._supervisor
    assert isinstance(owner, SelectedAssetCapture)
    sources = [state.source for state in owner.sources.values()]
    assert not {"funding_info", "mark_price"} & {s.stream for s in sources}
    assert not any(s.stream == "exchange_info" and s.venue_market == "usdm" for s in sources)
    assert [s.cadence_seconds for s in sources if s.stream == "open_interest"] == [37.0]
    assert [s.cadence_seconds for s in sources if s.stream == "exchange_info"] == [73.0, 73.0]


@pytest.mark.parametrize("market", ["spot", "usdm"])
@pytest.mark.parametrize("stream", ["diff_depth", "agg_trade", "book_ticker"])
@pytest.mark.parametrize("symbol", ["ETHUSDT", "SOLUSDT"])
def test_product_core_schema(market: str, stream: str, symbol: str) -> None:
    payload = (
        (FIXTURES / market / f"{stream}.json").read_bytes().replace(b"BTCUSDT", symbol.encode())
    )
    parser: Any = spot_frame if market == "spot" else usdm_frame
    kind: Any = SpotStream(stream) if market == "spot" else UsdMStream(stream)
    args = dict(
        raw_payload=payload,
        stream=kind,
        connection_id="c",
        collector_instance_id="i",
        collector_version="t",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    event = parser(symbol=symbol, **args)
    assert event.symbol == symbol and event.raw_payload == payload
    assert "malformed" not in event.capture_flags
    wrong = parser(symbol="BTCUSDT", **args)
    assert "malformed" in wrong.capture_flags and not wrong.source_sequence
    if market == "usdm" and stream != "agg_trade":
        decoded = json.loads(payload)
        decoded["ps"] = "BTCUSDT"
        args["raw_payload"] = json.dumps(decoded).encode()
        assert "malformed" in parser(symbol=symbol, **args).capture_flags


@pytest.mark.parametrize("stream", list(UsdMSideStream))
def test_product_side_websocket_schema(stream: UsdMSideStream) -> None:
    payload = (
        (FIXTURES / "usdm" / f"{stream.value}.json").read_bytes().replace(b"BTCUSDT", b"SOLUSDT")
    )
    args = dict(
        raw_payload=payload,
        stream=stream,
        connection_id="c",
        collector_instance_id="i",
        collector_version="t",
        receive_time_utc_ns=1,
        receive_monotonic_ns=2,
    )
    event = envelope_from_side_stream_frame(symbol="SOLUSDT", **args)  # type: ignore[arg-type]
    assert event.symbol == "SOLUSDT" and event.raw_payload == payload
    assert "malformed" not in event.capture_flags
    assert "malformed" in envelope_from_side_stream_frame(symbol="ETHUSDT", **args).capture_flags  # type: ignore[arg-type]


class Model:
    last_update_id = 100

    def __init__(self, value: Any = None) -> None:
        self.value = value if value is not None else {"lastUpdateId": 100, "bids": [], "asks": []}

    def to_dict(self) -> Any:
        return self.value


class Response:
    status = 200

    @property
    def headers(self) -> dict[str, object]:
        return {}

    def __init__(self, value: Any = None) -> None:
        self.value = value

    def data(self) -> Any:
        return [Model(v) for v in self.value] if isinstance(self.value, list) else Model(self.value)


class DepthApi:
    def __init__(self) -> None:
        self.symbols: list[str] = []

    def depth(self, symbol: str, limit: int) -> Any:
        self.symbols.append(symbol)
        return Response()

    def order_book(self, symbol: str, limit: int) -> Any:
        return self.depth(symbol, limit)


@pytest.mark.parametrize("capture", [spot_depth, usdm_depth])
def test_depth_request_envelope_and_provenance(capture: Any) -> None:
    api = DepthApi()
    event = capture(
        symbol="ETHUSDT", rest_api=api, collector_instance_id="i", collector_version="t"
    )
    assert api.symbols == ["ETHUSDT"]
    assert event.symbol == "ETHUSDT"
    assert json.loads(event.raw_payload)["request"]["symbol"] == "ETHUSDT"


def test_spot_singleflight_does_not_collapse_different_products() -> None:
    async def exercise() -> None:
        api = DepthApi()
        requester = SpotSnapshotRequester(rest_api=api)
        events = await asyncio.gather(
            *(
                requester.capture(
                    symbol=symbol,
                    collector_instance_id=symbol,
                    collector_version="t",
                    limit=1000,
                    timeout_ms=1000,
                )
                for symbol in ["BTCUSDT", "ETHUSDT", "BTCUSDT"]
            )
        )
        assert sorted(api.symbols) == ["BTCUSDT", "ETHUSDT"]
        assert [event.symbol for event in events] == ["BTCUSDT", "ETHUSDT", "BTCUSDT"]

    asyncio.run(exercise())


def test_spot_exchange_info_uses_product_and_validates_response() -> None:
    class Api:
        received: str | None = None
        response_symbol = "ETHUSDT"

        def exchange_info(self, symbol: str) -> Any:
            self.received = symbol
            return Response(
                {
                    "symbols": [
                        {
                            "symbol": self.response_symbol,
                            "filters": [],
                            "orderTypes": [],
                            "status": "TRADING",
                        }
                    ]
                }
            )

    api = Api()
    event = capture_spot_exchange_info(
        symbol="ETHUSDT", rest_api=cast(Any, api), collector_instance_id="i", collector_version="t"
    )
    assert api.received == event.symbol == "ETHUSDT"
    assert json.loads(event.raw_payload)["request"]["parameters"] == {"symbol": "ETHUSDT"}
    api.response_symbol = "BTCUSDT"
    with pytest.raises(RuntimeError, match="schema"):
        capture_spot_exchange_info(
            symbol="ETHUSDT",
            rest_api=cast(Any, api),
            collector_instance_id="i",
            collector_version="t",
        )


class ReadyCollector:
    def __init__(self, key: ProductKey, ready: bool = True) -> None:
        self.key = key
        self.snapshot = ReadinessSnapshot(
            market=key.market,
            symbol=key.symbol,
            collector_instance_id=f"i:{key}",
            collector_version="t",
            connected_streams=CORE_STREAMS,
            persisted_streams=CORE_STREAMS,
            snapshot_persisted=True,
            orderbook_synchronized=ready,
            event_count=1,
            last_receive_time_utc_ns=1,
            failure=None,
        )

    def readiness_snapshot(self) -> ReadinessSnapshot:
        return self.snapshot

    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"spot_symbols": ["ETHUSDT"]},
        {"usdm_symbols": ["SOLUSDT"]},
        {"spot_symbols": ["ETHUSDT", "SOLUSDT"], "usdm_symbols": ["ETHUSDT"]},
    ],
)
def test_readiness_is_exact_and_fail_closed(tmp_path: Path, values: Any) -> None:
    runtime = ServiceRuntime(
        config=RecorderConfig(data_root=tmp_path, **values), logger=logging.getLogger("test.ms2")
    )
    collectors = {key: ReadyCollector(key) for key in sorted(runtime.expected_products)}
    runtime._collectors = collectors
    state = runtime._state_document()
    assert state["core_ready"] is True
    assert state["expected_product_count"] == state["ready_product_count"] == len(collectors)
    key = next(iter(collectors))
    saved = collectors.pop(key)
    assert runtime._state_document()["core_ready"] is False
    collectors[key] = saved
    saved.snapshot = replace(saved.snapshot, orderbook_synchronized=False)
    assert runtime._state_document()["core_ready"] is False
    saved.snapshot = replace(saved.snapshot, orderbook_synchronized=True)
    extra = ProductKey("spot", "UNEXPECTED")
    collectors[extra] = ReadyCollector(extra)
    assert runtime._state_document()["core_ready"] is False
    del collectors[extra]
    saved.snapshot = replace(saved.snapshot, symbol="WRONG")
    assert runtime._state_document()["core_ready"] is False


def test_product_local_resync_readiness_queues_and_hard_reserve(constructed: Any) -> None:
    runtime, _ = constructed(
        spot_symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"], usdm_symbols=["ETHUSDT"]
    )
    owner = runtime._supervisor
    assert isinstance(owner, SelectedAssetCapture)
    make_groups(owner)
    target = owner.collectors[ProductKey("spot", "ETHUSDT")]
    siblings = [c for c in owner.collectors.values() if c is not target]
    before = [c.readiness_snapshot() for c in siblings]
    target.snapshot = cast(Any, object())
    assert target.groups[0].lifecycle_observer is not None
    target.groups[0].lifecycle_observer("unexpected_disconnect")
    assert target.snapshot is None
    assert before == [c.readiness_snapshot() for c in siblings]
    groups = [g for c in owner.collectors.values() for g in c.groups]
    assert len({id(g._receipts) for g in groups}) == len(groups)
    assert len({id(g.spool) for g in groups}) == len(groups)
    runtime._record_hard_reserve_stop({"observed_at_utc_ns": 100})
    assert set(runtime._catalog.unclosed_stream_discontinuities_by_stream()) == {
        (key.market, key.symbol, stream)
        for key in runtime.expected_products
        for stream in CORE_STREAMS
    }


def test_product_side_sources_and_independent_cursors(constructed: Any) -> None:
    runtime, _ = constructed(usdm_symbols=["ETHUSDT", "SOLUSDT"])
    owner = runtime._supervisor
    assert isinstance(owner, SelectedAssetCapture)
    for key, collector in owner.collectors.items():
        assert ProductKey("um_perpetual", "BTCUSDT") not in owner.collectors
        for kind in FIVE_MINUTE_KINDS:
            source = collector.source_states[kind.value].source
            assert source.symbol == key.symbol
            assert (
                dict(source.parameters).get("symbol", dict(source.parameters).get("pair"))
                == key.symbol
            )
            runtime._catalog.advance_side_data_cursor(
                kind=kind.value,
                symbol="ETHUSDT",
                last_persisted_period_timestamp=300000,
                updated_at_utc_ns=1,
                source_retention_window="latest_30_days",
                retention_window_ms=2592000000,
            )
            assert runtime._catalog.side_data_cursor(kind.value, "ETHUSDT") is not None
            assert runtime._catalog.side_data_cursor(kind.value, "SOLUSDT") is None


@pytest.mark.parametrize("status", [418, 429])
@pytest.mark.parametrize("origin", ["depth_snapshot", "open_interest", "funding_info"])
def test_observed_rate_limit_blocks_sibling_core_and_side_paths(
    constructed: Any,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    origin: str,
) -> None:
    import time

    from binance_market_data_recorder.binance.public_rest import RestObservation
    from binance_market_data_recorder.binance.request_budget import shared_request_budget
    from tests.factories import event

    async def exercise() -> None:
        runtime, _ = constructed(usdm_symbols=["ETHUSDT", "SOLUSDT"])
        owner = runtime._supervisor
        assert isinstance(owner, SelectedAssetCapture)
        source = next(s for s in owner.sources.values() if s.source.stream == origin)
        result = RestObservation(
            event().model_copy(update={"receive_time_utc_ns": time.time_ns()}),
            None,
            False,
            status,
            "HTTP" + str(status),
            {"retry-after": "60"},
        )
        blocked_before_persist = False

        async def persist(event: Any) -> None:
            nonlocal blocked_before_persist
            blocked_before_persist = shared_request_budget().delay({"usdm_weight": 1}) > 50

        monkeypatch.setattr(owner, "persist_rest", persist)
        monkeypatch.setattr(
            "binance_market_data_recorder.collector.coverage.capture_public_response",
            lambda *args, **kwargs: result,
        )
        await owner.poll_once(source)
        assert blocked_before_persist
        for state in owner.sources.values():
            if state.source.transport == "rest":
                assert shared_request_budget().delay(state.source.charges) > 50

    asyncio.run(exercise())


@pytest.mark.parametrize("values", [{"spot_symbols": ["ETHUSDT"]}, {"usdm_symbols": ["SOLUSDT"]}])
@pytest.mark.parametrize("mutation", ["none", "missing", "unexpected", "unready", "identity"])
def test_vps_evaluator_independently_checks_exact_products(
    tmp_path: Path,
    values: Any,
    mutation: str,
) -> None:
    from binance_market_data_recorder.service.readiness import VpsReadinessEvaluator
    from binance_market_data_recorder.service.state import ServiceStateStore
    from binance_market_data_recorder.service.systemd import SystemdManager
    from tests.unit.test_vps_service_readiness import (
        NOW,
        FakeSystemd,
        _identity,
        _state,
    )

    runtime = ServiceRuntime(
        config=RecorderConfig(data_root=tmp_path, **values), logger=logging.getLogger("test.ms2")
    )
    runtime._collectors = {key: ReadyCollector(key) for key in runtime.expected_products}
    identity = _identity(tmp_path)
    state = _state(identity)
    products = cast(dict[str, dict[str, Any]], runtime._state_document()["products"])
    key = next(iter(runtime.expected_products))
    product = products[key.market][key.symbol]
    if mutation == "missing":
        del products[key.market][key.symbol]
    elif mutation == "unexpected":
        products[key.market]["UNEXPECTED"] = dict(product, symbol="UNEXPECTED")
    elif mutation == "unready":
        product["orderbook_synchronized"] = False
    elif mutation == "identity":
        product["symbol"] = "WRONG"
    state["products"] = products
    state["core_ready"] = True  # The observer must not trust this producer summary.
    ServiceStateStore(tmp_path / "state" / "service_state.json").write(state)
    evaluator = VpsReadinessEvaluator(
        expected_products=runtime.expected_products,
        data_root=tmp_path,
        identity=identity,
        systemd_manager=cast(SystemdManager, FakeSystemd()),
        utc_clock_ns=lambda: NOW,
        process_alive=lambda _: True,
        catalog_ready=lambda _: True,
        identity_verifier=lambda selected: {"identity_sha256": selected.identity_sha256},
        process_environment=lambda _: {},
    )
    assert (evaluator.evaluate().state == "READY") == (mutation == "none")


@pytest.mark.parametrize(
    "values",
    [
        {"spot_symbols": ["ETHUSDT"]},
        {"usdm_symbols": ["SOLUSDT"]},
        {"spot_symbols": ["ETHUSDT", "SOLUSDT"], "usdm_symbols": ["ETHUSDT", "SOLUSDT"]},
        {
            "spot_symbols": ["BTCUSDT", "ETHUSDT"],
            "usdm_symbols": ["BTCUSDT", "ETHUSDT"],
            "all_enabled": True,
        },
        {"spot_symbols": list(R3_SYMBOLS), "usdm_symbols": list(R3_SYMBOLS), "all_enabled": True},
    ],
)
def test_real_runtime_fake_transports_ready_shutdown_and_raw_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    values: Any,
) -> None:
    from urllib.parse import unquote

    from binance_market_data_recorder.collector import coverage as coverage_module
    from binance_market_data_recorder.normalize.raw import SourceChunk, iter_source_records
    from binance_market_data_recorder.service.power import NoopSleepObserver
    from tests.unit.test_p2_coverage import Response as PublicResponse

    all_enabled = values.get("all_enabled", False)
    values = {k: v for k, v in values.items() if k != "all_enabled"}
    opened: set[tuple[str, str]] = set()
    requested: list[tuple[str, str]] = []
    received: dict[tuple[str, str], bytes] = {}

    class Socket:
        def __init__(self, payloads: list[bytes]) -> None:
            self.payloads = iter(payloads)

        async def recv(self, decode: bool | None = None) -> bytes:
            value = next(self.payloads, None)
            if value is not None:
                return value
            await asyncio.Future[None]()
            raise AssertionError("unreachable")

        async def close(self, **kwargs: Any) -> None:
            pass

    class Connection:
        def __init__(self, url: str, **kwargs: Any) -> None:
            self.url = url

        async def __aenter__(self) -> Socket:
            market = "um_perpetual" if "fstream" in self.url else "spot"
            payloads = []
            for wire in unquote(self.url.split("streams=", 1)[1]).split("/"):
                symbol, suffix = wire.split("@", 1)
                stream = {
                    "depth@100ms": "diff_depth",
                    "aggTrade": "agg_trade",
                    "bookTicker": "book_ticker",
                    "markPrice@1s": "mark_price",
                    "forceOrder": "liquidation",
                }[suffix]
                fixture_market = "usdm" if market == "um_perpetual" else "spot"
                data = json.loads(
                    (FIXTURES / fixture_market / f"{stream}.json")
                    .read_bytes()
                    .replace(b"BTCUSDT", symbol.upper().encode())
                )
                payload = json.dumps({"stream": wire, "data": data}).encode()
                received[market, wire] = payload
                payloads.append(payload)
                opened.add((market, wire))
            return Socket(payloads)

        async def __aexit__(self, *args: Any) -> None:
            pass

    class Client:
        def __init__(self, venue: str, proxy: Any) -> None:
            self.venue = venue

        def request(self, source: Any, params: Any) -> Any:
            symbol = params.get("symbol", params.get("pair", source.symbol))
            if source.stream == "depth_snapshot":
                requested.append((source.market, source.symbol))
                return PublicResponse({"lastUpdateId": 157, "bids": [], "asks": []})
            if source.stream == "exchange_info":
                return PublicResponse({"symbols": [{"symbol": symbol}], "rateLimits": []})
            if source.stream == "funding_info":
                return PublicResponse([])
            if source.stream.endswith("_5m"):
                from tests.unit.test_usdm_side_data_collector import _cursor_model

                if source.stream == "open_interest_statistics_5m" and symbol == "LTCUSDT":
                    return PublicResponse([])
                kind = next(k for k in FIVE_MINUTE_KINDS if k.value == source.stream)
                end = params["endTime"] - (300_000 if source.family == "D16" else 0)
                rows = []
                for timestamp in range(params["startTime"], end + 1, 300_000):
                    row = _cursor_model(kind, timestamp)
                    row["symbol"] = row["pair"] = symbol
                    rows.append(row)
                return PublicResponse(rows)
            fixture = {"premium_index_snapshot": "premium_index"}.get(source.stream, source.stream)
            return PublicResponse(
                json.loads(
                    (FIXTURES / "usdm" / f"{fixture}.json").read_text().replace("BTCUSDT", symbol)
                )
            )

        def close(self) -> None:
            pass

    monkeypatch.setattr(coverage_module, "connect", Connection)
    monkeypatch.setattr(coverage_module, "PublicRestClient", Client)
    disabled: dict[str, Any] = {
        name: all_enabled
        for name in RecorderConfig.model_fields
        if name.startswith("side_") and name.endswith("_enabled")
    }
    config = RecorderConfig(
        data_root=tmp_path, spot_exchange_info_enabled=all_enabled, **disabled, **values
    )
    runtime = ServiceRuntime(
        config=config,
        logger=logging.getLogger("test.ms2"),
        sleep_observer_factory=NoopSleepObserver,
    )

    async def exercise() -> None:
        task = asyncio.create_task(runtime.run())
        try:
            async with asyncio.timeout(20):
                while True:
                    if task.done():
                        task.result()
                    owner = runtime._supervisor
                    if (
                        isinstance(owner, SelectedAssetCapture)
                        and runtime._state_document()["core_ready"]
                    ):
                        rest = [s for s in owner.sources.values() if s.source.transport == "rest"]
                        if all(s.count > 0 and s.error is None for s in rest):
                            break
                    await asyncio.sleep(0.01)
            assert isinstance(runtime._supervisor, SelectedAssetCapture)
            assert set(runtime._collectors) == runtime.expected_products
            assert set(requested) == {(key.market, key.symbol) for key in runtime.expected_products}
            assert len(opened) == 3 * len(runtime.expected_products) + (
                2 * len(config.usdm_symbols) if all_enabled else 0
            )
            sources = [s.source for s in runtime._supervisor.sources.values()]
            assert sum(s.stream == "funding_info" for s in sources) == bool(
                all_enabled and config.usdm_symbols
            )
        finally:
            runtime.request_stop("test-complete")
            await asyncio.wait_for(task, 5)

    asyncio.run(exercise())
    final = runtime.state_store.read()
    assert final is not None and final["status"] == "STOPPED"
    manifests = [json.loads(path.read_text()) for path in runtime.layout.manifests.glob("*.json")]
    saved = [
        record.envelope
        for doc in manifests
        for record in iter_source_records(
            SourceChunk(
                doc["chunk_id"],
                runtime.layout.root / doc["relative_path"],
                runtime.layout.manifests / (doc["chunk_id"] + ".json"),
                "test",
                doc,
            )
        )
    ]
    ws = [e for e in saved if e.payload_encoding == "utf-8-json-combined.v1"]
    assert {e.raw_payload for e in ws} == set(received.values())
    assert {
        (e.market, e.symbol, e.source_sequence["sourceStream"])
        for e in saved
        if e.source_sequence.get("sourceStream") in {*CORE_STREAMS, "depth_snapshot"}
    } == {
        (key.market, key.symbol, stream)
        for key in runtime.expected_products
        for stream in {*CORE_STREAMS, "depth_snapshot"}
    }
    assert all(e.symbol in (*config.spot_symbols, *config.usdm_symbols, "BTCUSDT") for e in saved)
    assert not list(runtime.layout.active.rglob("*.partial"))
    if all_enabled and "LTCUSDT" in config.usdm_symbols:
        with Catalog(runtime.layout.catalog) as catalog:
            sparse = [
                row["evidence"]
                for row in catalog.operational_events(event_type="SIDE_DATA_RANGE_EVALUATED")
                if cast(dict[str, Any], row["evidence"])["symbol"] == "LTCUSDT"
            ]
            assert sparse
            assert all(
                cast(dict[str, Any], row)["range_complete"]
                and cast(dict[str, Any], row)["response_record_count"] == 0
                for row in sparse
            )


def test_wrong_symbol_frames_cannot_satisfy_readiness() -> None:
    from binance_market_data_recorder.supervisor.readiness import CollectorReadiness

    readiness = CollectorReadiness(
        market="um_perpetual", symbol="ETHUSDT", collector_instance_id="i", collector_version="t"
    )
    for stream in UsdMStream:
        payload = (FIXTURES / "usdm" / f"{stream.value}.json").read_bytes()
        event = usdm_frame(
            symbol="ETHUSDT",
            raw_payload=payload,
            stream=stream,
            collector_instance_id="i",
            collector_version="t",
            connection_id="c",
            receive_time_utc_ns=1,
            receive_monotonic_ns=2,
        )
        readiness.observe_connected(stream.value)
        readiness.observe_persisted(event)
    assert readiness.snapshot().persisted_streams == frozenset()
    assert readiness.snapshot().ready is False
    assert readiness.reliable_update_id is None
