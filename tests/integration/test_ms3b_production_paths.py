from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

import pytest
from binance_common.errors import RateLimitBanError, TooManyRequestsError

from binance_market_data_recorder.binance.spot.rate_limit import SpotIpRateLimiter
from binance_market_data_recorder.binance.spot.rest import SpotSnapshotRequester
from binance_market_data_recorder.binance.usdm.rest import DepthResponse
from binance_market_data_recorder.binance.usdm.schema import (
    UsdMStream,
    envelope_from_websocket_frame,
)
from binance_market_data_recorder.binance.usdm.side_data_rest import (
    FIVE_MINUTE_PERIOD_MS,
    FIVE_MINUTE_RETENTION,
    GLOBAL_SIDE_DATA_SYMBOL,
    PublicResponse,
    RestSideDataKind,
    UsdMSideRestApi,
)
from binance_market_data_recorder.binance.usdm.websocket import (
    ConnectionOpener,
    WebSocketConnection,
    open_usdm_websocket,
)
from binance_market_data_recorder.collector.spot import (
    SpotCollector,
    SpotCollectorSettings,
)
from binance_market_data_recorder.collector.usdm import (
    UsdMCollector,
    UsdMCollectorSettings,
)
from binance_market_data_recorder.collector.usdm_side_data import (
    RestSideDataPoller,
    UsdMRestCooldown,
    UsdMSideDataManager,
    UsdMSideDataSettings,
)
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.integration.test_usdm_stream_collector import envelopes

FIXED_NOW_MS = 50 * 24 * 60 * 60 * 1000 + 1


class _DepthModel:
    def __init__(self, last_update_id: int = 100) -> None:
        self.last_update_id = last_update_id

    def to_dict(self) -> dict[str, object]:
        return {
            "lastUpdateId": self.last_update_id,
            "bids": [],
            "asks": [],
        }


class _DepthResponse:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "1"}

    def __init__(self, last_update_id: int = 100) -> None:
        self.last_update_id = last_update_id

    def data(self) -> _DepthModel:
        return _DepthModel(self.last_update_id)


class _SdkModel:
    def __init__(self, value: Any) -> None:
        self.value = value

    def to_dict(self) -> Any:
        return self.value


class _SideResponse:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "1"}

    def __init__(self, value: Any) -> None:
        self.value = value

    def data(self) -> object:
        if isinstance(self.value, list):
            return [_SdkModel(item) for item in self.value]
        return _SdkModel(self.value)


class _RecordingLock(asyncio.Lock):
    def __init__(self, *, expected_initial_requests: int | None = None) -> None:
        super().__init__()
        self.request_count = 0
        self.acquire_count = 0
        self.release_count = 0
        self.events: list[str] = []
        self.expected_initial_requests = expected_initial_requests
        self.initial_requests_reached = asyncio.Event()

    async def acquire(self) -> Literal[True]:
        self.request_count += 1
        self.events.append("requested")
        if (
            self.expected_initial_requests is not None
            and self.request_count >= self.expected_initial_requests
        ):
            self.initial_requests_reached.set()
        try:
            result = await super().acquire()
        except asyncio.CancelledError:
            self.events.append("cancelled")
            raise
        self.acquire_count += 1
        self.events.append("acquired")
        return result

    def release(self) -> None:
        self.release_count += 1
        self.events.append("released")
        super().release()


class _BlockingCancellationApi:
    def __init__(self) -> None:
        self.side_started = threading.Event()
        self.side_release = threading.Event()
        self.core_called = threading.Event()
        self.stop_event: asyncio.Event | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self._guard = threading.Lock()
        self._active = 0
        self.max_active = 0

    def _enter(self) -> None:
        with self._guard:
            self._active += 1
            self.max_active = max(self.max_active, self._active)

    def _leave(self) -> None:
        with self._guard:
            self._active -= 1

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert limit == 1000
        self._enter()
        try:
            self.core_called.set()
            if self.stop_event is not None and self.loop is not None:
                self.loop.call_soon_threadsafe(self.stop_event.set)
            return _DepthResponse()
        finally:
            self._leave()

    def open_interest(self, symbol: str | None) -> PublicResponse:
        assert symbol == "BTCUSDT"
        self._enter()
        try:
            self.side_started.set()
            if not self.side_release.wait(timeout=3):
                raise AssertionError("test did not release the side request")
            return _SideResponse(
                {"symbol": "BTCUSDT", "openInterest": "1", "time": 1}
            )
        finally:
            self._leave()


class _FiniteGateApi:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.first_started = threading.Event()
        self.release_first = threading.Event()
        self._guard = threading.Lock()
        self._active = 0
        self.max_active = 0

    def _call(self, label: str, response: Callable[[], Any]) -> Any:
        with self._guard:
            self.calls.append(label)
            self._active += 1
            self.max_active = max(self.max_active, self._active)
            first = len(self.calls) == 1
        try:
            if first:
                self.first_started.set()
                if not self.release_first.wait(timeout=3):
                    raise AssertionError("test did not release the first REST request")
            return response()
        finally:
            with self._guard:
                self._active -= 1

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert limit == 1000
        return cast(DepthResponse, self._call(f"core:{symbol}", _DepthResponse))

    def open_interest(self, symbol: str | None) -> PublicResponse:
        assert symbol is not None
        return cast(
            PublicResponse,
            self._call(
                f"product:open_interest:{symbol}",
                lambda: _SideResponse(
                    {"symbol": symbol, "openInterest": "1", "time": 1}
                ),
            ),
        )

    def get_funding_rate_info(self) -> PublicResponse:
        return cast(
            PublicResponse,
            self._call(
                "global:funding_info",
                lambda: _SideResponse(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "adjustedFundingRateCap": "0.1",
                            "adjustedFundingRateFloor": "-0.1",
                            "fundingIntervalHours": 8,
                        }
                    ]
                ),
            ),
        )

    def long_short_ratio(
        self,
        symbol: str | None,
        _period: object,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> PublicResponse:
        assert symbol is not None
        assert limit is not None
        assert start_time is not None
        assert end_time is not None
        timestamps = list(
            range(start_time, end_time + 1, FIVE_MINUTE_PERIOD_MS)
        )[:limit]
        return cast(
            PublicResponse,
            self._call(
                f"cursor:{symbol}:{start_time}",
                lambda: _SideResponse(
                    [
                        {
                            "timestamp": timestamp,
                            "symbol": symbol,
                            "longShortRatio": "1",
                            "longAccount": "0.5",
                            "shortAccount": "0.5",
                        }
                        for timestamp in timestamps
                    ]
                ),
            ),
        )


class _RateLimitSideApi:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.side_called = threading.Event()
        self.core_called = threading.Event()

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert limit == 1000
        self.core_called.set()
        return _DepthResponse()

    def open_interest(self, symbol: str | None) -> PublicResponse:
        assert symbol == "BTCUSDT"
        self.side_called.set()
        raise self.error


PROFILE_D_SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "SUIUSDT",
    "LINKUSDT",
)
PROFILE_D_PRODUCTS = tuple(
    (market, symbol)
    for market in ("spot", "um_perpetual")
    for symbol in PROFILE_D_SYMBOLS
)


def _profile_d_frame(
    market: str, symbol: str, stream: str, ordinal: int
) -> bytes:
    if stream == UsdMStream.DIFF_DEPTH.value:
        if market == "spot":
            first, last = (101, 102) if ordinal == 1 else (103, 104)
            previous = None
        else:
            first, last, previous = (
                (100, 101, 99) if ordinal == 1 else (102, 103, 101)
            )
        payload: dict[str, object] = {
            "e": "depthUpdate",
            "E": ordinal,
            "T": ordinal,
            "s": symbol,
            "U": first,
            "u": last,
            "b": [],
            "a": [],
        }
        if market == "um_perpetual":
            payload.update({"ps": symbol, "st": 1, "pu": previous})
    elif stream == UsdMStream.AGG_TRADE.value:
        payload = {
            "e": "aggTrade",
            "E": ordinal,
            "T": ordinal,
            "s": symbol,
            "a": ordinal,
            "f": ordinal,
            "l": ordinal,
            "p": "1",
            "q": "1",
            "m": False,
            "M": False,
        }
        if market == "um_perpetual":
            payload["st"] = 1
    else:
        payload = {
            "e": "bookTicker",
            "u": ordinal,
            "s": symbol,
            "b": "1",
            "B": "1",
            "a": "2",
            "A": "1",
        }
        if market == "um_perpetual":
            payload.update({"ps": symbol, "st": 1})
    return json.dumps(payload, separators=(",", ":")).encode()


class _ProfileDWebSocket:
    def __init__(
        self,
        harness: _ProfileDWebSocketHarness,
        market: str,
        symbol: str,
        stream: str,
    ) -> None:
        self.harness = harness
        self.market = market
        self.symbol = symbol
        self.stream = stream
        self.receive_count = 0

    async def recv(self, decode: bool | None = None) -> bytes:
        del decode
        self.receive_count += 1
        if self.receive_count == 1:
            await self.harness.all_opened.wait()
            return _profile_d_frame(self.market, self.symbol, self.stream, 1)
        if self.receive_count == 2:
            await self.harness.second_wave.wait()
            return _profile_d_frame(self.market, self.symbol, self.stream, 2)
        if (
            self.market == self.harness.target_market
            and self.symbol == self.harness.target_symbol
            and self.stream == UsdMStream.AGG_TRADE.value
            and self.receive_count == 3
        ):
            self.harness.target_third_ready.set()
            return _profile_d_frame(self.market, self.symbol, self.stream, 3)
        await self.harness.stop.wait()
        raise OSError("profile D finite run stopped")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        del code, reason


class _ProfileDWebSocketHarness:
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop
        self.all_opened = asyncio.Event()
        self.all_opened_threading = threading.Event()
        self.second_wave = asyncio.Event()
        self.target_third_ready = asyncio.Event()
        self.opened_count = 0
        self.target_market = "um_perpetual"
        self.target_symbol = PROFILE_D_SYMBOLS[0]
        self.depth_persisted = {
            product: threading.Event() for product in PROFILE_D_PRODUCTS
        }
        self.snapshot_persisted = {
            product: threading.Event() for product in PROFILE_D_PRODUCTS
        }
        self.counts: dict[tuple[str, str, str], int] = {}

    @asynccontextmanager
    async def opener(self, url: str) -> AsyncIterator[WebSocketConnection]:
        wire_name = url.rsplit("/", 1)[-1]
        symbol = wire_name.split("@", 1)[0].upper()
        market = "um_perpetual" if "fstream.binance.com" in url else "spot"
        if "@depth@100ms" in wire_name:
            stream = UsdMStream.DIFF_DEPTH.value
        elif "@aggTrade" in wire_name:
            stream = UsdMStream.AGG_TRADE.value
        elif "@bookTicker" in wire_name:
            stream = UsdMStream.BOOK_TICKER.value
        else:
            raise AssertionError(f"unexpected market wire name: {wire_name}")
        assert (market, symbol) in PROFILE_D_PRODUCTS
        self.opened_count += 1
        if self.opened_count == len(PROFILE_D_PRODUCTS) * 3:
            self.all_opened_threading.set()
            self.all_opened.set()
        yield _ProfileDWebSocket(self, market, symbol, stream)

    def observe(self, envelope: object) -> None:
        event = cast(Any, envelope)
        key = (str(event.market), str(event.symbol), str(event.stream))
        self.counts[key] = self.counts.get(key, 0) + 1
        if event.stream == UsdMStream.DIFF_DEPTH.value and self.counts[key] == 1:
            self.depth_persisted[(str(event.market), str(event.symbol))].set()

    def observe_snapshot(self, market: str, symbol: str) -> None:
        self.snapshot_persisted[(market, symbol)].set()


class _ProfileDDepthApi:
    def __init__(self, harness: _ProfileDWebSocketHarness, market: str) -> None:
        self.harness = harness
        self.market = market

    def _response(self, symbol: str, limit: int) -> DepthResponse:
        assert limit == 1000
        if not self.harness.depth_persisted[(self.market, symbol)].wait(timeout=3):
            raise AssertionError(f"depth did not persist for {symbol}")
        return _DepthResponse()

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        return self._response(symbol, limit)

    def depth(self, symbol: str, limit: int) -> DepthResponse:
        return self._response(symbol, limit)


def _side_settings() -> UsdMSideDataSettings:
    return UsdMSideDataSettings(
        mark_price_enabled=False,
        liquidation_enabled=False,
        premium_index_enabled=False,
        funding_history_enabled=False,
        funding_info_enabled=False,
        open_interest_enabled=True,
        exchange_info_enabled=False,
    )


def _product_cursor_settings() -> UsdMSideDataSettings:
    return UsdMSideDataSettings(
        mark_price_enabled=False,
        liquidation_enabled=False,
        premium_index_enabled=False,
        funding_history_enabled=False,
        funding_info_enabled=False,
        open_interest_enabled=True,
        exchange_info_enabled=False,
        global_long_short_ratio_enabled=True,
    )


def _collector(
    tmp_path: Path,
    *,
    request_lock: asyncio.Lock,
    cooldown: UsdMRestCooldown,
    api: Any,
    symbol: str = "BTCUSDT",
    collector_instance_id: str = "ms3b-production-cancellation",
    side_data: UsdMSideDataSettings | None = None,
    include_side_data: bool = True,
    queue_capacity: int = 8_192,
    receipt_queue_capacity: int = 1_024,
    websocket_opener: ConnectionOpener = open_usdm_websocket,
) -> UsdMCollector:
    return UsdMCollector(
        UsdMCollectorSettings(
            symbol=symbol,
            data_root=tmp_path,
            collector_instance_id=collector_instance_id,
            collector_version="test",
            queue_capacity=queue_capacity,
            receipt_queue_capacity=receipt_queue_capacity,
            durability_interval_seconds=0,
            snapshot_retry_initial_seconds=0.001,
            snapshot_retry_maximum_seconds=0.001,
            snapshot_retry_jitter_ratio=0,
            side_data=(
                (_side_settings() if side_data is None else side_data)
                if include_side_data
                else None
            ),
        ),
        request_lock=request_lock,
        cooldown=cooldown,
        logger=logging.getLogger("test.ms3b.production-paths"),
        rest_api=cast(Any, api),
        side_rest_api=cast(UsdMSideRestApi, api),
        websocket_opener=websocket_opener,
    )


def _spot_collector(
    tmp_path: Path,
    *,
    api: Any,
    symbol: str,
    collector_instance_id: str,
    websocket_opener: ConnectionOpener,
    snapshot_rate_limiter: SpotIpRateLimiter | None = None,
    queue_capacity: int = 8_192,
    receipt_queue_capacity: int = 1_024,
) -> SpotCollector:
    return SpotCollector(
        SpotCollectorSettings(
            symbol=symbol,
            data_root=tmp_path,
            collector_instance_id=collector_instance_id,
            collector_version="test",
            queue_capacity=queue_capacity,
            receipt_queue_capacity=receipt_queue_capacity,
            durability_interval_seconds=0,
            snapshot_retry_initial_seconds=0.001,
            snapshot_retry_maximum_seconds=0.001,
            exchange_info_enabled=False,
        ),
        logger=logging.getLogger("test.ms3b.production-paths"),
        rest_api=cast(Any, api),
        websocket_opener=websocket_opener,
        snapshot_requester=SpotSnapshotRequester(
            rest_api=cast(Any, api), rate_limiter=snapshot_rate_limiter
        ),
    )


def _poller(collector: UsdMCollector) -> RestSideDataPoller:
    assert collector.side_data is not None
    factory = collector.side_data.supervisor.factories[RestSideDataKind.OPEN_INTEREST.value]
    return cast(RestSideDataPoller, factory())


def _manager_poller(
    manager: UsdMSideDataManager, kind: RestSideDataKind
) -> RestSideDataPoller:
    factory = manager.supervisor.factories[kind.value]
    return cast(RestSideDataPoller, factory())


@asynccontextmanager
async def _unused_opener(_url: str) -> AsyncIterator[WebSocketConnection]:
    raise AssertionError("the production REST-path test must not open a WebSocket")
    yield cast(WebSocketConnection, None)


def _global_manager(
    collector: UsdMCollector,
    *,
    request_lock: asyncio.Lock,
    cooldown: UsdMRestCooldown,
    api: Any,
) -> UsdMSideDataManager:
    return UsdMSideDataManager(
        settings=UsdMSideDataSettings(
            mark_price_enabled=False,
            liquidation_enabled=False,
            premium_index_enabled=False,
            funding_history_enabled=False,
            funding_info_enabled=True,
            open_interest_enabled=False,
            exchange_info_enabled=False,
        ),
        symbol=GLOBAL_SIDE_DATA_SYMBOL,
        scope="global",
        layout=collector.layout,
        catalog=collector.catalog,
        collector_instance_id="ms3b-production-global",
        collector_version="test",
        logger=logging.getLogger("test.ms3b.production-global"),
        queue_capacity=8,
        receipt_queue_capacity=8,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
        planned_rotation_seconds=23 * 60 * 60 + 50 * 60,
        rest_timeout_ms=1000,
        rest_api=cast(UsdMSideRestApi, api),
        websocket_opener=_unused_opener,
        request_lock=request_lock,
        cooldown=cooldown,
    )


def _prime_depth(collector: UsdMCollector) -> None:
    symbol = collector.settings.symbol
    raw_payload = json.dumps(
        {
            "e": "depthUpdate",
            "E": 1,
            "T": 1,
            "s": symbol,
            "ps": symbol,
            "st": 1,
            "U": 100,
            "u": 101,
            "pu": 99,
            "b": [],
            "a": [],
        },
        separators=(",", ":"),
    ).encode()
    envelope = envelope_from_websocket_frame(
        symbol=symbol,
        raw_payload=raw_payload,
        stream=UsdMStream.DIFF_DEPTH,
        connection_id="test-depth",
        collector_instance_id=collector.settings.collector_instance_id,
        collector_version=collector.settings.collector_version,
        receive_time_utc_ns=1,
        receive_monotonic_ns=1,
    )
    collector.readiness.observe_persisted(envelope)


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("deterministic production-path condition was not reached")
        await asyncio.sleep(0)


async def _close_artifacts(
    collector: UsdMCollector,
    poller: RestSideDataPoller,
) -> None:
    await asyncio.to_thread(poller.spool.close_and_seal)
    await asyncio.to_thread(collector.snapshot_spool.close_and_seal)
    collector.catalog.close()


async def _close_many_artifacts(
    collectors: list[UsdMCollector], pollers: list[RestSideDataPoller]
) -> None:
    for poller in pollers:
        await asyncio.to_thread(poller.spool.close_and_seal)
    for collector in collectors:
        await asyncio.to_thread(collector.snapshot_spool.close_and_seal)
    for collector in collectors:
        collector.catalog.close()


def test_production_side_cancel_retains_shared_lock_until_sdk_worker_finishes(
    tmp_path: Path,
) -> None:
    """An in-flight side REST cancellation must not release the real gate early."""

    async def exercise() -> None:
        api = _BlockingCancellationApi()
        request_lock = _RecordingLock()
        cooldown = UsdMRestCooldown()
        collector = _collector(
            tmp_path,
            request_lock=request_lock,
            cooldown=cooldown,
            api=api,
        )
        poller = _poller(collector)
        stop = asyncio.Event()
        api.stop_event = stop
        api.loop = asyncio.get_running_loop()
        poller._active_stop = stop

        side_task = asyncio.create_task(poller._request())
        assert await asyncio.to_thread(api.side_started.wait, 1)
        side_task.cancel()
        await asyncio.sleep(0)
        assert not side_task.done()

        core_task = asyncio.create_task(collector._capture_snapshot(stop))
        await _wait_until(lambda: request_lock.request_count >= 2)
        assert not api.core_called.is_set()

        api.side_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(side_task, timeout=1)
        assert await asyncio.to_thread(api.core_called.wait, 1)
        await asyncio.wait_for(core_task, timeout=1)
        assert api.max_active == 1
        assert request_lock.acquire_count == request_lock.release_count == 2
        await _close_artifacts(collector, poller)

    asyncio.run(exercise())


def test_production_waiting_cancel_and_post_stop_request_have_no_wire_side_effect(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        api = _BlockingCancellationApi()
        request_lock = _RecordingLock()
        cooldown = UsdMRestCooldown()
        collector = _collector(
            tmp_path,
            request_lock=request_lock,
            cooldown=cooldown,
            api=api,
        )
        poller = _poller(collector)
        stop = asyncio.Event()
        poller._active_stop = stop

        await request_lock.acquire()
        waiting = asyncio.create_task(poller._request())
        await _wait_until(lambda: request_lock.request_count == 1)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert not api.side_started.is_set()
        assert request_lock.locked()
        request_lock.release()

        stop.set()
        with pytest.raises(asyncio.CancelledError):
            await poller._request()
        assert not api.side_started.is_set()
        await _close_artifacts(collector, poller)

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (TooManyRequestsError(status_code=429), 429),
        (RateLimitBanError(status_code=418), 418),
    ],
)
def test_production_side_rate_limit_blocks_core_collector_on_shared_cooldown(
    tmp_path: Path,
    error: BaseException,
    status: int,
) -> None:
    async def exercise() -> None:
        api = _RateLimitSideApi(error)
        request_lock = _RecordingLock()
        cooldown = UsdMRestCooldown()
        collector = _collector(
            tmp_path,
            request_lock=request_lock,
            cooldown=cooldown,
            api=api,
        )
        poller = _poller(collector)
        stop = asyncio.Event()
        side_task = asyncio.create_task(poller.run(stop))
        assert await asyncio.to_thread(api.side_called.wait, 1)
        await _wait_until(lambda: cooldown.status == status)

        core_task = asyncio.create_task(collector._capture_snapshot(stop))
        await asyncio.sleep(0)
        assert not api.core_called.is_set()
        stop.set()
        await asyncio.wait_for(side_task, timeout=1)
        await asyncio.wait_for(core_task, timeout=1)
        await _close_artifacts(collector, poller)

    asyncio.run(exercise())


def test_production_finite_usdm_gate_cohort_covers_products_global_and_pages(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        request_lock = _RecordingLock(expected_initial_requests=7)
        cooldown = UsdMRestCooldown()
        api = _FiniteGateApi()
        product_settings = _product_cursor_settings()
        btc = _collector(
            tmp_path,
            request_lock=request_lock,
            cooldown=cooldown,
            api=api,
            symbol="BTCUSDT",
            collector_instance_id="ms3b-production-btc",
            side_data=product_settings,
        )
        eth = _collector(
            tmp_path,
            request_lock=request_lock,
            cooldown=cooldown,
            api=api,
            symbol="ETHUSDT",
            collector_instance_id="ms3b-production-eth",
            side_data=product_settings,
        )
        global_owner = _global_manager(
            btc,
            request_lock=request_lock,
            cooldown=cooldown,
            api=api,
        )
        assert btc.side_data is not None
        assert eth.side_data is not None
        assert btc.side_data.rest_request_lock is request_lock
        assert eth.side_data.rest_request_lock is request_lock
        assert global_owner.rest_request_lock is request_lock
        assert btc.side_data.rest_cooldown is cooldown
        assert eth.side_data.rest_cooldown is cooldown
        assert global_owner.rest_cooldown is cooldown

        btc_open_interest = _poller(btc)
        eth_open_interest = _poller(eth)
        btc_cursor = _manager_poller(
            btc.side_data, RestSideDataKind.GLOBAL_LONG_SHORT_RATIO
        )
        eth_cursor = _manager_poller(
            eth.side_data, RestSideDataKind.GLOBAL_LONG_SHORT_RATIO
        )
        global_funding = _manager_poller(
            global_owner, RestSideDataKind.FUNDING_INFO
        )
        all_pollers = [
            btc_open_interest,
            eth_open_interest,
            btc_cursor,
            eth_cursor,
            global_funding,
        ]
        stop = asyncio.Event()
        for poller in all_pollers:
            poller._active_stop = stop
            poller.utc_clock_ns = lambda: FIXED_NOW_MS * 1_000_000
        for poller in (btc_cursor, eth_cursor):
            poller.catchup_batch_limit = 2
            poller.catchup_batches_per_attempt = 2
        last_closed = (
            FIXED_NOW_MS // FIVE_MINUTE_PERIOD_MS
        ) * FIVE_MINUTE_PERIOD_MS - FIVE_MINUTE_PERIOD_MS
        retention_name, retention_ms = FIVE_MINUTE_RETENTION[
            RestSideDataKind.GLOBAL_LONG_SHORT_RATIO
        ]
        for collector in (btc, eth):
            collector.catalog.advance_side_data_cursor(
                kind=RestSideDataKind.GLOBAL_LONG_SHORT_RATIO.value,
                symbol=collector.settings.symbol,
                last_persisted_period_timestamp=(
                    last_closed - 4 * FIVE_MINUTE_PERIOD_MS
                ),
                updated_at_utc_ns=1,
                source_retention_window=retention_name,
                retention_window_ms=retention_ms,
            )
        _prime_depth(btc)
        _prime_depth(eth)

        tasks = [
            asyncio.create_task(btc._capture_snapshot(stop)),
            asyncio.create_task(eth._capture_snapshot(stop)),
            asyncio.create_task(btc_open_interest._capture_and_persist()),
            asyncio.create_task(eth_open_interest._capture_and_persist()),
            asyncio.create_task(btc_cursor._catch_up_five_minute(stop)),
            asyncio.create_task(eth_cursor._catch_up_five_minute(stop)),
            asyncio.create_task(global_funding._capture_and_persist()),
        ]
        try:
            assert await asyncio.to_thread(api.first_started.wait, 1)
            await asyncio.wait_for(request_lock.initial_requests_reached.wait(), 1)
            api.release_first.set()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            assert all(not isinstance(result, BaseException) for result in results), results
            assert all(task.done() for task in tasks)

            cursor_start = last_closed - 3 * FIVE_MINUTE_PERIOD_MS
            expected_calls = {
                "core:BTCUSDT": 1,
                "core:ETHUSDT": 1,
                "product:open_interest:BTCUSDT": 1,
                "product:open_interest:ETHUSDT": 1,
                f"cursor:BTCUSDT:{cursor_start}": 1,
                f"cursor:BTCUSDT:{cursor_start + 2 * FIVE_MINUTE_PERIOD_MS}": 1,
                f"cursor:ETHUSDT:{cursor_start}": 1,
                f"cursor:ETHUSDT:{cursor_start + 2 * FIVE_MINUTE_PERIOD_MS}": 1,
                "global:funding_info": 1,
            }
            assert {label: api.calls.count(label) for label in set(api.calls)} == expected_calls
            assert len(api.calls) == 9
            assert request_lock.request_count == 9
            assert request_lock.acquire_count == request_lock.release_count == 9
            assert "cancelled" not in request_lock.events
            assert api.max_active == 1
            first_release = request_lock.events.index("released")
            assert "requested" in request_lock.events[first_release + 1 :]

            for symbol, collector in (("BTCUSDT", btc), ("ETHUSDT", eth)):
                cursor = collector.catalog.side_data_cursor(
                    RestSideDataKind.GLOBAL_LONG_SHORT_RATIO.value,
                    symbol,
                )
                assert cursor is not None
                assert cursor["symbol"] == symbol
                assert cursor["last_persisted_period_timestamp"] == (
                    last_closed
                )
        finally:
            api.release_first.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await _close_many_artifacts([btc, eth], all_pollers)

        catalog = Catalog(ensure_storage_layout(tmp_path).catalog)
        try:
            assert len(catalog.chunks_in_states(ChunkState.SEALED)) == 7
            assert not catalog.chunks_in_states(ChunkState.ACTIVE, ChunkState.SEALING)
        finally:
            catalog.close()
        documents = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (tmp_path / "data" / "manifests").glob("*.json")
        ]
        assert len(documents) == 7
        for document in documents:
            if document["stream"] == RestSideDataKind.GLOBAL_LONG_SHORT_RATIO.value:
                # Each real page receives its own REST provenance connection ID.
                # Raw remains durable and cursor advancement is verified above;
                # the generic seal defense correctly fails closed for a chunk
                # containing multiple IDs rather than claiming a continuous
                # interval. M22.9 audit excludes REST request IDs as reconnects.
                assert document["complete"] is False
                assert document["gap"] is True
                assert "reconnect_gap" in document["capture_flags"]
            else:
                assert document["complete"] is True
                assert document["gap"] is False
        assert all(int(document["record_count"]) > 0 for document in documents)
        captured = envelopes(tmp_path)
        assert {
            (envelope.symbol, envelope.stream)
            for envelope in captured
        } == {
            ("BTCUSDT", "depth_snapshot"),
            ("ETHUSDT", "depth_snapshot"),
            ("BTCUSDT", "open_interest"),
            ("ETHUSDT", "open_interest"),
            ("BTCUSDT", "global_long_short_ratio_5m"),
            ("ETHUSDT", "global_long_short_ratio_5m"),
            (GLOBAL_SIDE_DATA_SYMBOL, "funding_info"),
        }

    asyncio.run(exercise())


def test_profile_d_runs_fourteen_collectors_with_forty_two_active_streams(
    tmp_path: Path,
) -> None:
    """Keep the mixed 14-product Profile D and all 42 core streams live."""

    async def exercise() -> None:
        stop = asyncio.Event()
        harness = _ProfileDWebSocketHarness(stop)
        spot_api = _ProfileDDepthApi(harness, "spot")
        usdm_api = _ProfileDDepthApi(harness, "um_perpetual")
        spot_limiter = SpotIpRateLimiter(weight_budget_per_minute=6_000_000)
        request_lock = _RecordingLock()
        cooldown = UsdMRestCooldown()
        spot_collectors: list[SpotCollector] = []
        usdm_collectors: list[UsdMCollector] = []
        for symbol in PROFILE_D_SYMBOLS:
            spot_collector = _spot_collector(
                tmp_path,
                api=spot_api,
                symbol=symbol,
                collector_instance_id=f"ms3b-profile-d-spot-{symbol}",
                queue_capacity=1,
                receipt_queue_capacity=1,
                websocket_opener=harness.opener,
                snapshot_rate_limiter=spot_limiter,
            )
            spot_collectors.append(spot_collector)
        for symbol in PROFILE_D_SYMBOLS:
            usdm_collector = _collector(
                tmp_path,
                request_lock=request_lock,
                cooldown=cooldown,
                api=usdm_api,
                symbol=symbol,
                collector_instance_id=f"ms3b-profile-d-um_perpetual-{symbol}",
                include_side_data=False,
                queue_capacity=1,
                receipt_queue_capacity=1,
                websocket_opener=harness.opener,
            )
            usdm_collectors.append(usdm_collector)
        collectors: list[SpotCollector | UsdMCollector] = [
            *spot_collectors,
            *usdm_collectors,
        ]
        for active_collector in collectors:
            collector_market = (
                "spot"
                if isinstance(active_collector, SpotCollector)
                else "um_perpetual"
            )
            for collector_stream in active_collector.streams:
                original_observer = collector_stream.envelope_observer

                def observe(
                    envelope: Any,
                    *,
                    original_observer: Callable[[Any], None] | None = (
                        original_observer
                    ),
                ) -> None:
                    if original_observer is not None:
                        original_observer(envelope)
                    harness.observe(envelope)

                collector_stream.envelope_observer = observe
            original_snapshot_observer = active_collector.snapshot_spool._event_observer

            def observe_snapshot(
                envelope: Any,
                frame_bytes: int,
                queue_depth: int,
                *,
                original_snapshot_observer: Callable[[Any, int, int], None]
                | None = original_snapshot_observer,
                market: str = collector_market,
                symbol: str = active_collector.settings.symbol,
            ) -> None:
                if original_snapshot_observer is not None:
                    original_snapshot_observer(envelope, frame_bytes, queue_depth)
                harness.observe_snapshot(market, symbol)

            active_collector.snapshot_spool._event_observer = observe_snapshot

        target = usdm_collectors[0]
        target_stream = next(
            collector_stream
            for collector_stream in target.streams
            if collector_stream.stream_name == UsdMStream.AGG_TRADE.value
        )
        target_drain_started = threading.Event()
        release_target_drain = threading.Event()
        original_drain = target_stream.spool.drain_all
        first_drain = True

        def blocked_drain() -> int:
            nonlocal first_drain
            if first_drain:
                first_drain = False
                target_drain_started.set()
                if not release_target_drain.wait(timeout=3):
                    raise AssertionError("test did not release the target drain")
            return original_drain()

        target_stream.spool.drain_all = blocked_drain  # type: ignore[method-assign]
        tasks = [asyncio.create_task(collector.run(stop)) for collector in collectors]
        try:
            assert await asyncio.to_thread(harness.all_opened_threading.wait, 1)
            await asyncio.wait_for(harness.all_opened.wait(), timeout=1)
            assert harness.opened_count == len(PROFILE_D_PRODUCTS) * 3
            assert await asyncio.to_thread(target_drain_started.wait, 1)

            harness.second_wave.set()
            await _wait_until(lambda: harness.target_third_ready.is_set())
            await _wait_until(lambda: target_stream.receipt_queue_stats.depth == 1)
            await _wait_until(
                lambda: all(
                    harness.counts.get((market, symbol, core_stream.value), 0) >= 2
                    for market, symbol in PROFILE_D_PRODUCTS
                    for core_stream in UsdMStream
                    if (market, symbol, core_stream.value)
                    != (
                        harness.target_market,
                        harness.target_symbol,
                        UsdMStream.AGG_TRADE.value,
                    )
                ),
                timeout=2,
            )
            assert target_stream.receipt_queue_stats.high_watermark == 1
            assert target_stream.receipt_queue_stats.depth == 1
            assert all(
                harness.counts[(market, symbol, core_stream.value)] >= 2
                for market, symbol in PROFILE_D_PRODUCTS
                for core_stream in UsdMStream
                if (market, symbol, core_stream.value)
                != (
                    harness.target_market,
                    harness.target_symbol,
                    UsdMStream.AGG_TRADE.value,
                )
            )

            release_target_drain.set()
            await _wait_until(
                lambda: target_stream.receipt_queue_stats.wait_count >= 1,
                timeout=2,
            )
            await _wait_until(
                lambda: harness.counts.get(
                    (
                        harness.target_market,
                        harness.target_symbol,
                        UsdMStream.AGG_TRADE.value,
                    ),
                    0,
                )
                >= 3,
                timeout=2,
            )
            assert target_stream.receipt_queue_stats.wait_count >= 1
            assert target_stream.receipt_queue_stats.depth == 0
            assert target_stream._backpressure_active is False
            await _wait_until(
                lambda: all(
                    event.is_set() for event in harness.snapshot_persisted.values()
                ),
                timeout=2,
            )

            expected_counts = {
                (market, symbol, core_stream.value): 2
                for market, symbol in PROFILE_D_PRODUCTS
                for core_stream in UsdMStream
            }
            expected_counts[
                (
                    harness.target_market,
                    harness.target_symbol,
                    UsdMStream.AGG_TRADE.value,
                )
            ] = 3
            assert harness.counts == expected_counts

            stop.set()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            assert all(not isinstance(result, BaseException) for result in results), [
                (type(result).__name__, str(result))
                for result in results
                if isinstance(result, BaseException)
            ]
        finally:
            release_target_drain.set()
            stop.set()
            await asyncio.gather(*tasks, return_exceptions=True)

        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        try:
            sealed = catalog.chunks_in_states(ChunkState.SEALED)
            assert len(sealed) == len(PROFILE_D_PRODUCTS) * 4
            assert not catalog.chunks_in_states(ChunkState.ACTIVE, ChunkState.SEALING)
        finally:
            catalog.close()

        assert not list(layout.active.glob("*.partial"))
        assert not list(layout.sealed.glob("*.partial"))
        assert not list(layout.manifests.glob("*.partial"))

        documents = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in layout.manifests.glob("*.json")
        ]
        assert len(documents) == len(PROFILE_D_PRODUCTS) * 4
        assert {document["market"] for document in documents} == {
            "spot",
            "um_perpetual",
        }
        assert all(document["complete"] is True for document in documents)
        assert all(document["gap"] is False for document in documents)
        assert all("reconnect_gap" not in document["capture_flags"] for document in documents)

        expected_manifest_counts = {
            (market, symbol, core_stream.value): 2
            for market, symbol in PROFILE_D_PRODUCTS
            for core_stream in UsdMStream
        }
        expected_manifest_counts[
            (
                harness.target_market,
                harness.target_symbol,
                UsdMStream.AGG_TRADE.value,
            )
        ] = 3
        expected_manifest_counts.update(
            {
                (market, symbol, "depth_snapshot"): 1
                for market, symbol in PROFILE_D_PRODUCTS
            }
        )
        assert {
            (document["market"], document["symbol"], document["stream"]): int(
                document["record_count"]
            )
            for document in documents
        } == expected_manifest_counts
        assert {
            (document["market"], document["symbol"], document["stream"]): document[
                "collector_instance_ids"
            ][0]
            for document in documents
        } == {
            key: f"ms3b-profile-d-{key[0]}-{key[1]}"
            for key in expected_manifest_counts
        }

        captured = envelopes(tmp_path)
        assert len(captured) == len(PROFILE_D_PRODUCTS) * 3 * 2 + 1 + len(
            PROFILE_D_PRODUCTS
        )
        assert {
            (envelope.market, envelope.symbol, envelope.stream)
            for envelope in captured
        } == set(expected_manifest_counts)
        for envelope in captured:
            assert envelope.collector_instance_id == (
                f"ms3b-profile-d-{envelope.market}-{envelope.symbol}"
            )
            assert envelope.raw_payload
            payload = json.loads(envelope.raw_payload)
            if envelope.stream == "depth_snapshot":
                assert payload["response"]["model"]["lastUpdateId"] == 100
            else:
                assert payload["s"] == envelope.symbol

        for market, symbol in PROFILE_D_PRODUCTS:
            for core_stream in UsdMStream:
                stream_events = [
                    envelope
                    for envelope in captured
                    if envelope.market == market
                    and envelope.symbol == symbol
                    and envelope.stream == core_stream.value
                ]
                assert len({envelope.connection_id for envelope in stream_events}) == 1

    asyncio.run(exercise())
