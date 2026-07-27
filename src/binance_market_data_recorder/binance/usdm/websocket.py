"""Owned lifecycle for one Binance USD-M raw WebSocket stream."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from typing import Protocol
from uuid import uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from ...domain.event import EventEnvelope
from ...logging import log_event
from ...network import WebSocketProxy
from ...spool.queue import IngressQueueFull
from ...spool.stream import StreamSpool
from ..spot.websocket import ReceivedFrame, ReconnectBackoff
from .schema import UsdMStream, envelope_from_websocket_frame

USDM_WEBSOCKET_ROOT = "wss://fstream.binance.com"


class WebSocketConnection(Protocol):
    async def recv(self, decode: bool | None = None) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


ConnectionOpener = Callable[[str], AbstractAsyncContextManager[WebSocketConnection]]
EnvelopeFactory = Callable[..., EventEnvelope]
EnvelopeObserver = Callable[[EventEnvelope], None]
FailureObserver = Callable[[str], None]
LifecycleObserver = Callable[[str], None]


@asynccontextmanager
async def open_usdm_websocket(
    url: str,
    *,
    proxy: WebSocketProxy = None,
) -> AsyncIterator[WebSocketConnection]:
    """Open a routed USD-M raw stream with bounded library buffering."""

    async with connect(
        url,
        proxy=proxy,
        compression=None,
        ping_interval=None,
        max_queue=(32, 8),
        max_size=16 * 1024 * 1024,
        close_timeout=10,
    ) as websocket:
        yield websocket


class UsdMStreamCollector:
    """Receive, timestamp, persist, reconnect, and rotate one USD-M stream."""

    def __init__(
        self,
        *,
        stream: UsdMStream | str,
        route: str,
        wire_name: str,
        spool: StreamSpool,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        receipt_queue_capacity: int = 1024,
        planned_rotation_seconds: float = 23 * 60 * 60 + 50 * 60,
        backoff: ReconnectBackoff | None = None,
        websocket_root: str = USDM_WEBSOCKET_ROOT,
        opener: ConnectionOpener = open_usdm_websocket,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
        envelope_factory: EnvelopeFactory | None = None,
        envelope_observer: EnvelopeObserver | None = None,
        failure_observer: FailureObserver | None = None,
        lifecycle_observer: LifecycleObserver | None = None,
    ) -> None:
        if route not in {"public", "market"}:
            raise ValueError("USD-M market data route must be public or market")
        if receipt_queue_capacity < 1:
            raise ValueError("receipt queue capacity must be positive")
        if not 0 < planned_rotation_seconds < 24 * 60 * 60:
            raise ValueError("planned rotation must occur before the 24-hour limit")
        if envelope_factory is None and not isinstance(stream, UsdMStream):
            raise ValueError("a side-data stream requires an envelope factory")
        self.stream = stream
        self.stream_name = stream.value if isinstance(stream, UsdMStream) else stream
        self.route = route
        self.wire_name = wire_name
        self.spool = spool
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.logger = logger
        self.planned_rotation_seconds = planned_rotation_seconds
        self.backoff = backoff or ReconnectBackoff()
        self.websocket_root = websocket_root.rstrip("/")
        self.opener = opener
        self.utc_clock_ns = utc_clock_ns
        self.monotonic_clock_ns = monotonic_clock_ns
        self.envelope_factory = envelope_factory
        self.envelope_observer = envelope_observer
        self.failure_observer = failure_observer
        self.lifecycle_observer = lifecycle_observer
        self._capture_flags: tuple[str, ...] = ()
        self._receipts: asyncio.Queue[ReceivedFrame] = asyncio.Queue(maxsize=receipt_queue_capacity)

    @property
    def url(self) -> str:
        return f"{self.websocket_root}/{self.route}/ws/{self.wire_name}"

    def set_capture_flags(self, flags: tuple[str, ...]) -> None:
        self._capture_flags = flags

    async def _receive_once(
        self, websocket: WebSocketConnection, connection_id: str
    ) -> ReceivedFrame:
        message = await websocket.recv(decode=False)
        receive_time_utc_ns = self.utc_clock_ns()
        receive_monotonic_ns = self.monotonic_clock_ns()
        payload = message.encode() if isinstance(message, str) else message
        return ReceivedFrame(
            raw_payload=payload,
            connection_id=connection_id,
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            capture_flags=self._capture_flags,
        )

    def _accept(self, receipt: ReceivedFrame) -> None:
        try:
            self._receipts.put_nowait(receipt)
        except asyncio.QueueFull as exc:
            raise IngressQueueFull("WebSocket receipt queue is full") from exc

    async def _writer_loop(self, producer_done: asyncio.Event) -> None:
        while not producer_done.is_set() or not self._receipts.empty():
            batch: list[ReceivedFrame] = []
            persisted: list[EventEnvelope] = []
            try:
                batch.append(await asyncio.wait_for(self._receipts.get(), timeout=0.1))
            except TimeoutError:
                await asyncio.to_thread(self.spool.drain_all)
                continue
            for _ in range(min(255, self.spool.queue.capacity - 1)):
                try:
                    batch.append(self._receipts.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for receipt in batch:
                if self.envelope_factory is None:
                    if not isinstance(self.stream, UsdMStream):
                        raise RuntimeError("missing USD-M envelope factory")
                    envelope = envelope_from_websocket_frame(
                        raw_payload=receipt.raw_payload,
                        stream=self.stream,
                        connection_id=receipt.connection_id,
                        collector_instance_id=self.collector_instance_id,
                        collector_version=self.collector_version,
                        receive_time_utc_ns=receipt.receive_time_utc_ns,
                        receive_monotonic_ns=receipt.receive_monotonic_ns,
                        additional_capture_flags=receipt.capture_flags,
                    )
                else:
                    envelope = self.envelope_factory(
                        raw_payload=receipt.raw_payload,
                        connection_id=receipt.connection_id,
                        collector_instance_id=self.collector_instance_id,
                        collector_version=self.collector_version,
                        receive_time_utc_ns=receipt.receive_time_utc_ns,
                        receive_monotonic_ns=receipt.receive_monotonic_ns,
                    )
                    if receipt.capture_flags:
                        envelope = envelope.model_copy(
                            update={
                                "capture_flags": tuple(
                                    dict.fromkeys(
                                        (
                                            *envelope.capture_flags,
                                            *receipt.capture_flags,
                                        )
                                    )
                                )
                            }
                        )
                self.spool.enqueue(envelope)
                persisted.append(envelope)
                self._receipts.task_done()
            await asyncio.to_thread(self.spool.drain_all)
            if self.envelope_observer is not None:
                for envelope in persisted:
                    self.envelope_observer(envelope)
        await asyncio.to_thread(self.spool.close_and_seal)

    async def _receive_connection(
        self,
        websocket: WebSocketConnection,
        connection_id: str,
        stop: asyncio.Event,
        writer_task: asyncio.Task[None],
    ) -> str:
        deadline = asyncio.get_running_loop().time() + self.planned_rotation_seconds
        while True:
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            receive_task = asyncio.create_task(self._receive_once(websocket, connection_id))
            stop_task = asyncio.create_task(stop.wait())
            done, pending = await asyncio.wait(
                {receive_task, stop_task, writer_task},
                timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED,
            )
            cancelled = [task for task in pending if task is not writer_task]
            for task in cancelled:
                task.cancel()
            if cancelled:
                await asyncio.gather(*cancelled, return_exceptions=True)
            if writer_task in done:
                await writer_task
                raise RuntimeError("USD-M Raw writer stopped unexpectedly")
            if not done:
                receive_task.cancel()
                await websocket.close(code=1000, reason="planned 24-hour rotation")
                return "planned_rotation"
            if receive_task in done:
                try:
                    self._accept(receive_task.result())
                except (WebSocketException, OSError, TimeoutError):
                    if stop.is_set():
                        await websocket.close(code=1000, reason="collector shutdown")
                        return "graceful_shutdown"
                    raise
                continue
            if stop_task in done and stop_task.result():
                receive_task.cancel()
                await websocket.close(code=1000, reason="collector shutdown")
                return "graceful_shutdown"

    async def _connection_loop(self, stop: asyncio.Event, writer_task: asyncio.Task[None]) -> None:
        failures = 0
        while not stop.is_set():
            connection_id = str(uuid4())
            reason = "unexpected_disconnect"
            connected_at: float | None = None
            was_connected = False
            try:
                async with self.opener(self.url) as websocket:
                    connected_at = asyncio.get_running_loop().time()
                    was_connected = True
                    if self.lifecycle_observer is not None:
                        self.lifecycle_observer("connected")
                    log_event(
                        self.logger,
                        logging.INFO,
                        "usdm_websocket_connected",
                        "Binance USD-M raw stream connected",
                        stream=self.stream_name,
                        route=self.route,
                        connection_id=connection_id,
                    )
                    reason = await self._receive_connection(
                        websocket, connection_id, stop, writer_task
                    )
            except asyncio.CancelledError:
                raise
            except (WebSocketException, OSError, TimeoutError) as exc:
                if self.lifecycle_observer is not None:
                    self.lifecycle_observer("unexpected_disconnect")
                if self.failure_observer is not None:
                    self.failure_observer(type(exc).__name__)
                if (
                    connected_at is not None
                    and asyncio.get_running_loop().time() - connected_at >= 60
                ):
                    failures = 0
                failures += 1
                log_event(
                    self.logger,
                    logging.WARNING,
                    "usdm_websocket_disconnected",
                    "Binance USD-M stream disconnected unexpectedly",
                    stream=self.stream_name,
                    connection_id=connection_id,
                    error_type=type(exc).__name__,
                    retry=failures,
                )
            except IngressQueueFull:
                if self.failure_observer is not None:
                    self.failure_observer("IngressQueueFull")
                log_event(
                    self.logger,
                    logging.CRITICAL,
                    "usdm_ingress_overflow",
                    "bounded USD-M receipt queue is full; capture continuity is lost",
                    stream=self.stream_name,
                    connection_id=connection_id,
                )
                raise
            finally:
                if was_connected and self.lifecycle_observer is not None:
                    self.lifecycle_observer("disconnected")
            if stop.is_set() or reason == "graceful_shutdown":
                return
            if reason == "planned_rotation":
                failures = 0
                if self.lifecycle_observer is not None:
                    self.lifecycle_observer("planned_rotation")
                continue
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.backoff.delay(max(1, failures)))

    async def run(self, stop: asyncio.Event) -> None:
        producer_done = asyncio.Event()
        writer_task = asyncio.create_task(self._writer_loop(producer_done))
        try:
            await self._connection_loop(stop, writer_task)
        finally:
            producer_done.set()
            await writer_task
