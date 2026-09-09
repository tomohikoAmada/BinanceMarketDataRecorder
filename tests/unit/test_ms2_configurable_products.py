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
    GLOBAL_SIDE_DATA_SYMBOL,
)
from binance_market_data_recorder.binance.usdm.side_data_schema import (
    UsdMSideStream,
    envelope_from_side_stream_frame,
)
from binance_market_data_recorder.collector.spot import SpotCollector
from binance_market_data_recorder.collector.usdm import UsdMCollector
from binance_market_data_recorder.collector.usdm_side_data import RestSideDataPoller
from binance_market_data_recorder.config import ConfigurationError, RecorderConfig, load_config
from binance_market_data_recorder.domain.product import ProductKey, configured_products
from binance_market_data_recorder.service import runtime as runtime_module
from binance_market_data_recorder.service.runtime import ServiceRuntime, _collector_factory
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.supervisor.readiness import CORE_STREAMS, ReadinessSnapshot

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

    def factory(market: str) -> Any:
        def create(**kwargs: Any) -> Any:
            calls.append(market)
            return object()

        return create

    monkeypatch.setattr(runtime_module, "PublicSpotRestApi", factory("spot_depth"))
    monkeypatch.setattr(runtime_module, "create_spot_exchange_info_api", factory("spot_info"))
    monkeypatch.setattr(runtime_module, "create_usdm_rest_api", factory("usdm_depth"))
    monkeypatch.setattr(runtime_module, "create_usdm_side_rest_api", factory("usdm_side"))

    def build(**values: Any) -> tuple[ServiceRuntime, list[str]]:
        config = RecorderConfig(data_root=tmp_path, **values)
        runtime = ServiceRuntime(config=config, logger=logging.getLogger("test.ms2"))
        runtime._catalog = Catalog(runtime.layout.catalog)
        runtime._collectors = _collector_factory(
            config,
            runtime.logger,
            "test",
            runtime.service_instance_id,
            runtime.usdm_request_lock,
            runtime.usdm_cooldown,
        )
        runtime._create_global_side_data()
        runtimes.append(runtime)
        return runtime, calls

    yield build
    for runtime in runtimes:
        for collector in runtime._collectors.values():
            cast(Any, collector).catalog.close()
        assert runtime._catalog is not None
        runtime._catalog.close()


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"spot_symbols": ["ETHUSDT"]},
        {"usdm_symbols": ["SOLUSDT"]},
        {"spot_symbols": ["SOLUSDT", "BTCUSDT", "ETHUSDT"], "usdm_symbols": ["ETHUSDT", "SOLUSDT"]},
        {"spot_symbols": ["ETHUSDT"], "usdm_symbols": ["ETHUSDT"]},
    ],
)
def test_exact_assembly_routes_and_zero_market_ownership(constructed: Any, values: Any) -> None:
    runtime, calls = constructed(**values)
    expected = configured_products(runtime.config.spot_symbols, runtime.config.usdm_symbols)
    assert tuple(runtime._collectors) == expected
    ids = []
    for key, collector in runtime._collectors.items():
        assert collector.settings.symbol == key.symbol
        assert isinstance(collector, SpotCollector if key.market == "spot" else UsdMCollector)
        ids.append(collector.settings.collector_instance_id)
        assert ids[-1] == f"{runtime.service_instance_id}:{key.market}:{key.symbol}"
        for stream in collector.streams:
            assert stream.symbol == stream.spool.symbol == key.symbol
            assert f"/{key.symbol.lower()}@" in stream.url
        assert (
            collector.snapshot_spool.symbol == collector.readiness_snapshot().symbol == key.symbol
        )
    assert len(ids) == len(set(ids))
    if not runtime.config.spot_symbols:
        assert not any(call.startswith("spot") for call in calls)
        assert not any(isinstance(c, SpotCollector) for c in runtime._collectors.values())
    if not runtime.config.usdm_symbols:
        assert not any(call.startswith("usdm") for call in calls)
        assert runtime.global_side_data is None
        assert runtime.usdm_request_lock is runtime.usdm_cooldown is None
    else:
        assert runtime.global_side_data is not None
        assert set(runtime.global_side_data.supervisor.factories) == {
            "funding_info",
            "exchange_info",
        }
        assert runtime.global_side_data.cursor_state == {}
        assert runtime.global_side_data.symbol == GLOBAL_SIDE_DATA_SYMBOL == "BTCUSDT"
        for collector in runtime._collectors.values():
            if isinstance(collector, UsdMCollector):
                assert collector.public_rest_request_lock is runtime.usdm_request_lock
                assert collector.public_rest_cooldown is runtime.usdm_cooldown
                assert collector.side_data is not None
                assert collector.side_data.rest_request_lock is runtime.usdm_request_lock
                assert collector.side_data.rest_cooldown is runtime.usdm_cooldown
                assert not {"funding_info", "exchange_info"} & set(
                    collector.side_data.supervisor.factories
                )
        assert runtime.global_side_data.rest_request_lock is runtime.usdm_request_lock
        assert runtime.global_side_data.rest_cooldown is runtime.usdm_cooldown


def test_global_owner_disabled_and_shared_spot_limiter(constructed: Any) -> None:
    async def exercise() -> None:
        runtime, _ = constructed(
            spot_symbols=["BTCUSDT", "ETHUSDT"],
            usdm_symbols=["ETHUSDT"],
            side_funding_info_enabled=False,
            side_exchange_info_enabled=False,
        )
        assert runtime.global_side_data is None
        collectors = [c for c in runtime._collectors.values() if isinstance(c, SpotCollector)]
        assert (
            collectors[0].snapshot_requester.rate_limiter
            is collectors[1].snapshot_requester.rate_limiter
        )

    asyncio.run(exercise())


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
    target = runtime._collectors[ProductKey("spot", "ETHUSDT")]
    siblings = [c for c in runtime._collectors.values() if c is not target]
    before = [c.readiness_snapshot() for c in siblings]
    target.streams[0].lifecycle_observer("unexpected_disconnect")
    assert target.resync.requested.is_set()
    assert target.readiness_snapshot().failure == "unexpected_disconnect"
    assert all(not c.resync.requested.is_set() for c in siblings)
    assert before == [c.readiness_snapshot() for c in siblings]
    assert len({id(c.resync) for c in runtime._collectors.values()}) == 4
    assert (
        len({id(stream._receipts) for c in runtime._collectors.values() for stream in c.streams})
        == 12
    )
    assert (
        len({id(stream.spool) for c in runtime._collectors.values() for stream in c.streams}) == 12
    )
    runtime._record_hard_reserve_stop({"observed_at_utc_ns": 100})
    catalog = runtime._catalog
    gaps = catalog.unclosed_stream_discontinuities_by_stream()
    assert set(gaps) == {
        (key.market, key.symbol, stream)
        for key in runtime.expected_products
        for stream in CORE_STREAMS
    }
    rows_before = catalog.operational_events(event_type="STREAM_DISCONTINUITY_STARTED")
    # Removed products retain historical evidence; re-added products restore only their own gap.
    changed, _ = constructed(spot_symbols=["ETHUSDT"])
    restored = changed._collectors[ProductKey("spot", "ETHUSDT")]
    assert all(stream._pending_gap is not None for stream in restored.streams)
    assert rows_before == catalog.operational_events(event_type="STREAM_DISCONTINUITY_STARTED")


def test_product_side_factories_and_independent_cursors(constructed: Any) -> None:
    runtime, _ = constructed(usdm_symbols=["ETHUSDT", "SOLUSDT"])
    assert ProductKey("um_perpetual", "BTCUSDT") not in runtime.expected_products
    for key, collector in runtime._collectors.items():
        manager = collector.side_data
        assert manager.symbol == key.symbol and manager.scope == "product"
        for factory in manager.supervisor.factories.values():
            extension = factory()
            if isinstance(extension, RestSideDataPoller):
                assert extension.symbol == extension.spool.symbol == key.symbol
                assert extension.request_lock is runtime.usdm_request_lock
                assert extension.cooldown is runtime.usdm_cooldown
            else:
                assert extension.collector.symbol == key.symbol
                assert f"/{key.symbol.lower()}@" in extension.collector.url
        for kind in FIVE_MINUTE_KINDS:
            assert kind.value in manager.cursor_state
    catalog = runtime._catalog
    for kind in FIVE_MINUTE_KINDS:
        catalog.advance_side_data_cursor(
            kind=kind.value,
            symbol="ETHUSDT",
            last_persisted_period_timestamp=300000,
            updated_at_utc_ns=1,
            source_retention_window="latest_30_days",
            retention_window_ms=2592000000,
        )
        assert catalog.side_data_cursor(kind.value, "ETHUSDT") is not None
        assert catalog.side_data_cursor(kind.value, "SOLUSDT") is None
        assert catalog.side_data_cursor(kind.value, "BTCUSDT") is None


@pytest.mark.parametrize("status", [418, 429])
@pytest.mark.parametrize("origin", ["core", "product_side", "global_side"])
def test_observed_rate_limit_blocks_sibling_core_and_side_paths(
    constructed: Any,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    origin: str,
) -> None:
    from binance_common.errors import RateLimitBanError, TooManyRequestsError

    from tests.unit.test_usdm_shared_rest_gate import ObservedCooldown

    async def exercise() -> None:
        cooldown = ObservedCooldown()
        monkeypatch.setattr(runtime_module, "UsdMRestCooldown", lambda: cooldown)
        runtime, _ = constructed(usdm_symbols=["ETHUSDT", "SOLUSDT"])
        eth = runtime._collectors[ProductKey("um_perpetual", "ETHUSDT")]
        sol = runtime._collectors[ProductKey("um_perpetual", "SOLUSDT")]
        assert eth.side_data is not None and sol.side_data is not None
        assert runtime.global_side_data is not None
        eth_side = eth.side_data.supervisor.factories["open_interest"]()
        sol_side = sol.side_data.supervisor.factories["open_interest"]()
        global_side = runtime.global_side_data.supervisor.factories["funding_info"]()
        called: list[str] = []
        error = (
            RateLimitBanError(status_code=418)
            if status == 418
            else TooManyRequestsError(status_code=429)
        )

        class Api:
            def __init__(self, name: str) -> None:
                self.name = name

            def order_book(self, symbol: str, limit: int) -> Any:
                assert symbol == ("ETHUSDT" if self.name == "core" else "SOLUSDT")
                called.append(self.name)
                raise error

            def open_interest(self, symbol: str) -> Any:
                assert symbol == ("ETHUSDT" if self.name == "product_side" else "SOLUSDT")
                called.append(self.name)
                raise error

            def get_funding_rate_info(self) -> Any:
                called.append(self.name)
                raise error

        eth.rest_api = Api("core")
        sol.rest_api = Api("sibling_core")
        eth_side.rest_api = Api("product_side")
        sol_side.rest_api = Api("sibling_side")
        global_side.rest_api = Api("global_side")
        stop = asyncio.Event()
        source = {"core": eth, "product_side": eth_side, "global_side": global_side}[origin]
        source_task = asyncio.create_task(
            source._capture_snapshot(stop) if origin == "core" else source.run(stop)
        )
        await asyncio.wait_for(cooldown.install_finished.wait(), 1)
        assert called == [origin] and cooldown.status == status
        blocked_tasks = []
        for target in [sol, sol_side, global_side] if origin != "global_side" else [sol, sol_side]:
            cooldown.arm_next_wait()
            if isinstance(target, UsdMCollector):
                task = asyncio.create_task(target._capture_snapshot(stop))
            else:
                target._active_stop = stop
                task = asyncio.create_task(target._request())
            blocked_tasks.append(task)
            await asyncio.wait_for(cooldown.next_wait_started.wait(), 1)
            assert called == [origin]
        stop.set()
        await asyncio.wait_for(
            asyncio.gather(source_task, *blocked_tasks, return_exceptions=True), 1
        )
        assert called == [origin]
        for poller in [eth_side, sol_side, global_side]:
            poller.spool.close_and_seal()

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
    ],
)
def test_real_runtime_fake_transports_ready_shutdown_and_raw_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    values: Any,
) -> None:
    from collections.abc import AsyncIterator
    from contextlib import asynccontextmanager

    from binance_market_data_recorder.service.power import NoopSleepObserver

    opened: set[tuple[str, str]] = set()
    requested: list[tuple[str, str]] = []

    class Api:
        def depth(self, symbol: str, limit: int) -> Any:
            requested.append(("spot", symbol))
            return self.response()

        def order_book(self, symbol: str, limit: int) -> Any:
            requested.append(("um_perpetual", symbol))
            return self.response()

        def response(self) -> Any:
            model = Model({"lastUpdateId": 157, "bids": [], "asks": []})
            model.last_update_id = 157

            class SnapshotResponse(Response):
                def data(self) -> Any:
                    return model

            return SnapshotResponse()

    class Socket:
        def __init__(self, payload: bytes) -> None:
            self.payload = payload
            self.sent = False

        async def recv(self, decode: bool | None = None) -> bytes:
            if not self.sent:
                self.sent = True
                return self.payload
            await asyncio.Future[None]()
            raise AssertionError("unreachable")

        async def close(self, **kwargs: Any) -> None:
            pass

    @asynccontextmanager
    async def opener(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        market = "um_perpetual" if "fstream" in url else "spot"
        wire = url.rsplit("/", 1)[-1]
        symbol, suffix = wire.split("@", 1)
        stream = {
            "depth@100ms": "diff_depth",
            "aggTrade": "agg_trade",
            "bookTicker": "book_ticker",
        }[suffix]
        opened.add((market, wire))
        fixture_market = "usdm" if market == "um_perpetual" else "spot"
        payload = (FIXTURES / fixture_market / f"{stream}.json").read_bytes()
        yield Socket(payload.replace(b"BTCUSDT", symbol.upper().encode()))

    monkeypatch.setattr(runtime_module, "open_spot_websocket", opener)
    monkeypatch.setattr(runtime_module, "open_usdm_websocket", opener)
    monkeypatch.setattr(runtime_module, "PublicSpotRestApi", lambda **_: Api())
    monkeypatch.setattr(runtime_module, "create_usdm_rest_api", lambda **_: Api())
    monkeypatch.setattr(runtime_module, "create_usdm_side_rest_api", lambda **_: object())
    monkeypatch.setattr(runtime_module, "create_spot_exchange_info_api", lambda **_: object())
    disabled: dict[str, Any] = {
        name: False
        for name in RecorderConfig.model_fields
        if name.startswith("side_") and name.endswith("_enabled")
    }
    config = RecorderConfig(
        data_root=tmp_path, spot_exchange_info_enabled=False, **disabled, **values
    )
    runtime = ServiceRuntime(
        config=config,
        logger=logging.getLogger("test.ms2"),
        sleep_observer_factory=NoopSleepObserver,
    )

    async def exercise() -> None:
        task = asyncio.create_task(runtime.run())
        try:
            for _ in range(500):
                if runtime._state_document()["core_ready"]:
                    break
                if task.done():
                    await task
                await asyncio.sleep(0.01)
            assert runtime._state_document()["core_ready"] is True
            assert len(opened) == 3 * len(runtime.expected_products)
            assert set(requested) == {(key.market, key.symbol) for key in runtime.expected_products}
        finally:
            runtime.request_stop("test-complete")
            await asyncio.wait_for(task, 5)

    asyncio.run(exercise())
    assert runtime.state_store.read()["status"] == "STOPPED"  # type: ignore[index]
    manifests = [json.loads(path.read_text()) for path in runtime.layout.manifests.glob("*.json")]
    assert {(doc["market"], doc["symbol"], doc["stream"]) for doc in manifests} == {
        (key.market, key.symbol, stream)
        for key in runtime.expected_products
        for stream in (*CORE_STREAMS, "depth_snapshot")
    }


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
