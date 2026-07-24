"""Owned lifecycle for one Binance Spot raw WebSocket stream."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from ...domain.event import EventEnvelope
from ...logging import log_event
from ...spool.queue import IngressQueueFull
from ...spool.stream import StreamSpool
from .schema import SpotStream, envelope_from_websocket_frame

SPOT_WEBSOCKET_BASE_URL = "wss://stream.binance.com:443/ws"


class WebSocketConnection(Protocol):
    async def recv(self, decode: bool | None = None) -> str | bytes: ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


@dataclass(frozen=True)
class ReceivedFrame:
    raw_payload: bytes
    connection_id: str
    receive_time_utc_ns: int
    receive_monotonic_ns: int
    capture_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReconnectBackoff:
    initial_seconds: float = 1.0
    maximum_seconds: float = 60.0
    jitter_ratio: float = 0.2

    def delay(self, failures: int, *, random_value: float | None = None) -> float:
        if failures < 1:
            raise ValueError("failures must be positive")
        if self.initial_seconds <= 0 or self.maximum_seconds < self.initial_seconds:
            raise ValueError("invalid reconnect bounds")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter ratio must be between zero and one")
        base: float = min(
            self.maximum_seconds, self.initial_seconds * (2.0 ** (failures - 1))
        )
        sample: float = random.random() if random_value is None else random_value
        multiplier = 1 + self.jitter_ratio * (2 * sample - 1)
        return float(min(self.maximum_seconds, max(0.0, base * multiplier)))


ConnectionOpener = Callable[[str], AbstractAsyncContextManager[WebSocketConnection]]
LifecycleObserver = Callable[[str], None]
EnvelopeObserver = Callable[[EventEnvelope], None]


@asynccontextmanager
async def open_spot_websocket(url: str) -> AsyncIterator[WebSocketConnection]:
    """Open a raw stream with finite library buffering and no client Ping loop."""

    async with connect(
        url,
        proxy=None,
        compression=None,
        ping_interval=None,
        max_queue=(32, 8),
        max_size=16 * 1024 * 1024,
        close_timeout=10,
    ) as websocket:
        yield websocket


class SpotStreamCollector:
    """Receive, timestamp, persist, reconnect, and rotate one raw Spot stream."""

    def __init__(
        self,
        *,
        stream: SpotStream,
        wire_name: str,
        spool: StreamSpool,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        receipt_queue_capacity: int = 1024,
        planned_rotation_seconds: float = 23 * 60 * 60 + 50 * 60,
        backoff: ReconnectBackoff | None = None,
        base_url: str = SPOT_WEBSOCKET_BASE_URL,
        opener: ConnectionOpener = open_spot_websocket,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
        lifecycle_observer: LifecycleObserver | None = None,
        envelope_observer: EnvelopeObserver | None = None,
    ) -> None:
        if receipt_queue_capacity < 1:
            raise ValueError("receipt queue capacity must be positive")
        if not 0 < planned_rotation_seconds < 24 * 60 * 60:
            raise ValueError("planned rotation must occur before the 24-hour limit")
        self.stream = stream
        self.wire_name = wire_name
        self.spool = spool
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.logger = logger
        self.planned_rotation_seconds = planned_rotation_seconds
        self.backoff = backoff or ReconnectBackoff()
        self.base_url = base_url.rstrip("/")
        self.opener = opener
        self.utc_clock_ns = utc_clock_ns
        self.monotonic_clock_ns = monotonic_clock_ns
        self.lifecycle_observer = lifecycle_observer
        self.envelope_observer = envelope_observer
        self._capture_flags: tuple[str, ...] = ()
        self._receipts: asyncio.Queue[ReceivedFrame] = asyncio.Queue(
            maxsize=receipt_queue_capacity
        )
        self._server_shutdown = asyncio.Event()

    @property
    def url(self) -> str:
        return f"{self.base_url}/{self.wire_name}"

    def set_capture_flags(self, flags: tuple[str, ...]) -> None:
        self._capture_flags = flags

    async def _receive_once(
        self, websocket: WebSocketConnection, connection_id: str
    ) -> ReceivedFrame:
        message = await websocket.recv(decode=False)
        receive_time_utc_ns = self.utc_clock_ns()
        receive_monotonic_ns = self.monotonic_clock_ns()
        payload = message.encode("utf-8") if isinstance(message, str) else message
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
                first = await asyncio.wait_for(self._receipts.get(), timeout=0.1)
                batch.append(first)
            except TimeoutError:
                await asyncio.to_thread(self.spool.drain_all)
                continue
            for _ in range(min(255, self.spool.queue.capacity - 1)):
                try:
                    batch.append(self._receipts.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for receipt in batch:
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
                self.spool.enqueue(envelope)
                persisted.append(envelope)
                if "server_shutdown" in envelope.capture_flags:
                    self._server_shutdown.set()
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
            receive_task = asyncio.create_task(
                self._receive_once(websocket, connection_id)
            )
            stop_task = asyncio.create_task(stop.wait())
            shutdown_task = asyncio.create_task(self._server_shutdown.wait())
            done, pending = await asyncio.wait(
                {receive_task, stop_task, shutdown_task, writer_task},
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
                raise RuntimeError("Spot Raw writer stopped unexpectedly")
            if not done:
                receive_task.cancel()
                await websocket.close(code=1000, reason="planned 24-hour rotation")
                return "planned_rotation"
            if receive_task in done:
                try:
                    receipt = receive_task.result()
                except (WebSocketException, OSError, TimeoutError):
                    if stop.is_set():
                        await websocket.close(code=1000, reason="collector shutdown")
                        return "graceful_shutdown"
                    raise
                self._accept(receipt)
                continue
            if stop_task in done and stop_task.result():
                receive_task.cancel()
                await websocket.close(code=1000, reason="collector shutdown")
                return "graceful_shutdown"
            if shutdown_task in done and shutdown_task.result():
                receive_task.cancel()
                self._server_shutdown.clear()
                await websocket.close(code=1000, reason="serverShutdown received")
                return "server_shutdown"

    async def _connection_loop(
        self, stop: asyncio.Event, writer_task: asyncio.Task[None]
    ) -> None:
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
                        "spot_websocket_connected",
                        "Binance Spot raw stream connected",
                        stream=self.stream.value,
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
                if (
                    connected_at is not None
                    and asyncio.get_running_loop().time() - connected_at >= 60
                ):
                    failures = 0
                failures += 1
                log_event(
                    self.logger,
                    logging.WARNING,
                    "spot_websocket_disconnected",
                    "Binance Spot stream disconnected unexpectedly",
                    stream=self.stream.value,
                    connection_id=connection_id,
                    error_type=type(exc).__name__,
                    retry=failures,
                )
            except IngressQueueFull:
                log_event(
                    self.logger,
                    logging.CRITICAL,
                    "spot_ingress_overflow",
                    "bounded Spot receipt queue is full; capture continuity is lost",
                    stream=self.stream.value,
                    connection_id=connection_id,
                )
                raise
            finally:
                if was_connected and self.lifecycle_observer is not None:
                    self.lifecycle_observer("disconnected")
            if stop.is_set() or reason == "graceful_shutdown":
                return
            if reason in {"planned_rotation", "server_shutdown"}:
                failures = 0
                if reason == "planned_rotation" and self.lifecycle_observer is not None:
                    self.lifecycle_observer("planned_rotation")
                log_event(
                    self.logger,
                    logging.INFO,
                    f"spot_{reason}",
                    "Binance Spot connection will be replaced",
                    stream=self.stream.value,
                    connection_id=connection_id,
                )
                continue
            delay = self.backoff.delay(max(1, failures))
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)

    async def run(self, stop: asyncio.Event) -> None:
        """Run until requested to stop; seal all accepted records on exit."""

        producer_done = asyncio.Event()
        writer_task = asyncio.create_task(self._writer_loop(producer_done))
        try:
            await self._connection_loop(stop, writer_task)
        finally:
            producer_done.set()
            await writer_task
