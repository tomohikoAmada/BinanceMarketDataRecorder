"""Binance Spot WebSocket stream collector."""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import quote

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from ...domain.event import EventEnvelope
from ...network import WebSocketProxy
from ...spool.stream import StreamSpool
from ..public_websocket import (
    CombinedWebSocketStream,
    ConnectionOpener,
    EnvelopeObserver,
    LifecycleObserver,
    PublicWebSocketStream,
    ReceivedFrame,
    ReconnectBackoff,
    WebSocketConnection,
    _CombinedWebSocketStream,
    _PublicWebSocketStream,
)
from ..websocket_common import DEFAULT_RECEIPT_BYTE_CAPACITY
from .schema import SpotStream, envelope_from_websocket_frame

SPOT_WEBSOCKET_BASE_URL = "wss://stream.binance.com:443/ws"


@asynccontextmanager
async def open_spot_websocket(
    url: str,
    *,
    proxy: WebSocketProxy = None,
) -> AsyncIterator[WebSocketConnection]:
    """Open a raw stream with finite library buffering and no client Ping loop."""

    from ..request_budget import observe_websocket_rejection, shared_request_budget

    await shared_request_budget().acquire({"spot_connections": 1})
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
        observe_websocket_rejection("spot", exc.response.status_code, exc.response.headers)
        raise


class SpotStreamCollector(PublicWebSocketStream):
    """Receive, timestamp, persist, reconnect, and rotate one raw Spot stream."""

    def __init__(
        self,
        *,
        stream: SpotStream,
        symbol: str,
        wire_name: str,
        spool: StreamSpool,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        receipt_queue_capacity: int = 1024,
        receipt_byte_capacity: int = DEFAULT_RECEIPT_BYTE_CAPACITY,
        planned_rotation_seconds: float = 23 * 60 * 60 + 50 * 60,
        backoff: ReconnectBackoff | None = None,
        base_url: str = SPOT_WEBSOCKET_BASE_URL,
        opener: ConnectionOpener = open_spot_websocket,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
        lifecycle_observer: LifecycleObserver | None = None,
        envelope_observer: EnvelopeObserver | None = None,
        backpressure_put_timeout_seconds: float = 1.0,
        backpressure_saturation_timeout_seconds: float = 30.0,
        post_close_handoff_timeout_seconds: float = 5.0,
    ) -> None:
        def default_envelope_factory(**kwargs: Any) -> EventEnvelope:
            return envelope_from_websocket_frame(
                symbol=symbol,
                stream=stream,
                raw_payload=kwargs["raw_payload"],
                connection_id=kwargs["connection_id"],
                collector_instance_id=kwargs["collector_instance_id"],
                collector_version=kwargs["collector_version"],
                receive_time_utc_ns=kwargs["receive_time_utc_ns"],
                receive_monotonic_ns=kwargs["receive_monotonic_ns"],
                additional_capture_flags=kwargs.get("additional_capture_flags", ()),
            )

        super().__init__(
            market="spot",
            stream=stream,
            symbol=symbol,
            route="",
            wire_name=f"{quote(symbol.lower(), safe='')}@{wire_name.split('@', 1)[1]}",
            spool=spool,
            collector_instance_id=collector_instance_id,
            collector_version=collector_version,
            logger=logger,
            receipt_queue_capacity=receipt_queue_capacity,
            receipt_byte_capacity=receipt_byte_capacity,
            planned_rotation_seconds=planned_rotation_seconds,
            backoff=backoff,
            websocket_root=base_url,
            opener=opener,
            utc_clock_ns=utc_clock_ns,
            monotonic_clock_ns=monotonic_clock_ns,
            envelope_factory=default_envelope_factory,
            envelope_observer=envelope_observer,
            lifecycle_observer=lifecycle_observer,
            backpressure_put_timeout_seconds=backpressure_put_timeout_seconds,
            backpressure_saturation_timeout_seconds=backpressure_saturation_timeout_seconds,
            post_close_handoff_timeout_seconds=post_close_handoff_timeout_seconds,
            market_label="Spot",
            market_prefix="spot",
        )
        self.base_url = base_url.rstrip("/")


__all__ = [
    "SPOT_WEBSOCKET_BASE_URL",
    "CombinedWebSocketStream",
    "ConnectionOpener",
    "EnvelopeObserver",
    "LifecycleObserver",
    "PublicWebSocketStream",
    "ReceivedFrame",
    "ReconnectBackoff",
    "SpotStreamCollector",
    "WebSocketConnection",
    "_CombinedWebSocketStream",
    "_PublicWebSocketStream",
    "open_spot_websocket",
]
