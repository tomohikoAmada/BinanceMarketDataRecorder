"""Binance USD-M WebSocket stream collector."""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Any, cast
from urllib.parse import quote

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from ...domain.event import EventEnvelope, Market
from ...network import WebSocketProxy
from ...spool.stream import StreamSpool
from ..public_websocket import (
    CombinedWebSocketStream,
    ConnectionOpener,
    EnvelopeFactory,
    EnvelopeObserver,
    FailureObserver,
    LifecycleObserver,
    PublicWebSocketStream,
    ReceivedFrame,
    ReconnectBackoff,
    WebSocketConnection,
    _CombinedWebSocketStream,
    _PublicWebSocketStream,
)
from ..websocket_common import (
    DEFAULT_RECEIPT_BYTE_CAPACITY,
    run_owned_blocking_call,
)
from .schema import UsdMStream, envelope_from_websocket_frame

USDM_WEBSOCKET_ROOT = "wss://fstream.binance.com"


async def _run_owned_blocking_call[**BlockingParams, BlockingResult](
    function: Callable[BlockingParams, BlockingResult],
    /,
    *args: BlockingParams.args,
    **kwargs: BlockingParams.kwargs,
) -> BlockingResult:
    """Re-export of ``run_owned_blocking_call`` kept for call-site stability."""

    return await run_owned_blocking_call(function, *args, **kwargs)


@asynccontextmanager
async def open_usdm_websocket(
    url: str,
    *,
    proxy: WebSocketProxy = None,
) -> AsyncIterator[WebSocketConnection]:
    """Open a routed USD-M raw stream with bounded library buffering."""

    from ..request_budget import observe_websocket_rejection, shared_request_budget

    await shared_request_budget().acquire({"usdm_connections": 1})
    try:
        async with connect(
            url,
            proxy=proxy,
            compression=None,
            ping_interval=None,
            max_queue=(4, 1),
            max_size=16 * 1024 * 1024,
            close_timeout=10,
        ) as websocket:
            yield websocket
    except InvalidStatus as exc:
        observe_websocket_rejection("usdm", exc.response.status_code, exc.response.headers)
        raise


class UsdMStreamCollector(CombinedWebSocketStream):
    """Receive, timestamp, persist, reconnect, and rotate one USD-M stream."""

    def __init__(
        self,
        *,
        stream: UsdMStream | str,
        symbol: str,
        route: str,
        wire_name: str,
        spool: StreamSpool,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        log_context: Mapping[str, object] | None = None,
        receipt_queue_capacity: int = 1024,
        receipt_byte_capacity: int = DEFAULT_RECEIPT_BYTE_CAPACITY,
        planned_rotation_seconds: float = 23 * 60 * 60 + 50 * 60,
        backoff: ReconnectBackoff | None = None,
        websocket_root: str = USDM_WEBSOCKET_ROOT,
        combined_streams: tuple[str, ...] = (),
        opener: ConnectionOpener = open_usdm_websocket,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
        envelope_factory: EnvelopeFactory | None = None,
        envelope_observer: EnvelopeObserver | None = None,
        failure_observer: FailureObserver | None = None,
        lifecycle_observer: LifecycleObserver | None = None,
        backpressure_put_timeout_seconds: float = 1.0,
        backpressure_saturation_timeout_seconds: float = 30.0,
        post_close_handoff_timeout_seconds: float = 5.0,
    ) -> None:
        if route not in {"public", "market", ""} or (not route and not combined_streams):
            raise ValueError("USD-M market data route must be public or market")
        if not symbol or spool.symbol != symbol:
            raise ValueError("USD-M collector symbol must match its spool")
        if envelope_factory is None and not isinstance(stream, UsdMStream):
            raise ValueError("a side-data stream requires an envelope factory")

        def default_envelope_factory(**kwargs: Any) -> EventEnvelope:
            if not isinstance(stream, UsdMStream):
                raise RuntimeError("missing USD-M envelope factory")
            return envelope_from_websocket_frame(
                symbol=symbol,
                raw_payload=kwargs["raw_payload"],
                stream=stream,
                connection_id=kwargs["connection_id"],
                collector_instance_id=kwargs["collector_instance_id"],
                collector_version=kwargs["collector_version"],
                receive_time_utc_ns=kwargs["receive_time_utc_ns"],
                receive_monotonic_ns=kwargs["receive_monotonic_ns"],
                additional_capture_flags=kwargs.get("additional_capture_flags", ()),
            )

        super().__init__(
            market=cast(Market, spool.market),
            stream=stream,
            symbol=symbol,
            route=route,
            wire_name=wire_name
            if combined_streams
            else (f"{quote(symbol.lower(), safe='')}@{wire_name.split('@', 1)[1]}"),
            spool=spool,
            collector_instance_id=collector_instance_id,
            collector_version=collector_version,
            logger=logger,
            log_context=log_context,
            receipt_queue_capacity=receipt_queue_capacity,
            receipt_byte_capacity=receipt_byte_capacity,
            planned_rotation_seconds=planned_rotation_seconds,
            backoff=backoff,
            websocket_root=websocket_root,
            combined_streams=combined_streams,
            opener=opener,
            utc_clock_ns=utc_clock_ns,
            monotonic_clock_ns=monotonic_clock_ns,
            envelope_factory=envelope_factory or default_envelope_factory,
            envelope_observer=envelope_observer,
            failure_observer=failure_observer,
            lifecycle_observer=lifecycle_observer,
            backpressure_put_timeout_seconds=backpressure_put_timeout_seconds,
            backpressure_saturation_timeout_seconds=backpressure_saturation_timeout_seconds,
            post_close_handoff_timeout_seconds=post_close_handoff_timeout_seconds,
            market_label="USD-M",
            market_prefix="usdm",
        )


__all__ = [
    "USDM_WEBSOCKET_ROOT",
    "CombinedWebSocketStream",
    "ConnectionOpener",
    "EnvelopeFactory",
    "EnvelopeObserver",
    "FailureObserver",
    "LifecycleObserver",
    "PublicWebSocketStream",
    "ReceivedFrame",
    "ReconnectBackoff",
    "UsdMStreamCollector",
    "WebSocketConnection",
    "_CombinedWebSocketStream",
    "_PublicWebSocketStream",
    "_run_owned_blocking_call",
    "open_usdm_websocket",
]
