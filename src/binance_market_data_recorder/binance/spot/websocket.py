"""Binance Spot 单条原始 WebSocket 流的自有生命周期。

SpotStreamCollector 管理一条 websocket 连接的生命周期,遵循以下不变量(ADR-0009):

- 接收时间(UTC 墙上时钟 + monotonic)在 recv(decode=False) 之后、JSON 解析或
  CBOR 编码之前立即记录。这确保解析异常、畸变负载和编码失败不影响计时记录。
- 有界接收队列(BoundedEventQueue,receipt_queue_capacity)防止背压下无限内存
  增长。若队列已满,抛出 IngressQueueFull 作为可见故障;事件永不静默丢弃。
- 连接在 planned_rotation_seconds(默认 23h50m)时轮换,早于 Binance 文档规定的
  24 小时断开。server_shutdown 事件触发立即重连。
- 每个流使用自己的 raw 端点和连接 ID。这即使对畸变 JSON 也能保留流身份,
  并避免组合流包装的歧义。
- M21.4.11: 任意 transport 边界(unexpected disconnect / planned rotation /
  server_shutdown / session restart)都走统一 reconnect-boundary 状态机:
  旧 generation drain + seal(必要时 manifest 级 reconnect_gap 强制不完整)
  -> Catalog STREAM_DISCONTINUITY_STARTED durable -> generation++ ->
  新连接 -> 首个新帧携带 sequence_gap -> Raw sync -> COMPLETED。
  exchange-side completeness 在 close 与首个新帧之间永远无法证明;
  intentional close 不是完整性豁免。Spot 的 IngressQueueFull 溢出仍保持
  进程级致命(不属于本修复范围)。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from dataclasses import dataclass, replace
from typing import Protocol
from uuid import uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from ...domain.event import EventEnvelope
from ...logging import log_event
from ...network import WebSocketProxy
from ...spool.queue import IngressGapStateConflict, IngressQueueFull
from ...spool.seal import RECONNECT_GAP_FLAG
from ...spool.stream import StreamSpool
from ..websocket_common import (
    RECONNECT_REASONS,
    run_owned_blocking_call,
)
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
async def open_spot_websocket(
    url: str,
    *,
    proxy: WebSocketProxy = None,
) -> AsyncIterator[WebSocketConnection]:
    """Open a raw stream with finite library buffering and no client Ping loop."""

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
        self._generation = 0
        self._recovery_flag_pending = False
        self._recovery_marker_enqueued = False
        self._pending_gap: dict[str, object] | None = None
        self._forced_seal_flags: frozenset[str] = frozenset()
        self._boundary_connection_id: str | None = None
        self._boundary_detected_at_utc_ns: int | None = None
        self._connection_receipt_count = 0
        self._restore_open_gap()

    @property
    def url(self) -> str:
        return f"{self.base_url}/{self.wire_name}"

    def set_capture_flags(self, flags: tuple[str, ...]) -> None:
        self._capture_flags = flags

    def _restore_open_gap(self) -> None:
        open_gaps = self.spool.catalog.unclosed_stream_discontinuities(
            market="spot",
            stream=self.stream.value,
        )
        if len(open_gaps) > 1:
            raise IngressGapStateConflict(
                f"Spot {self.stream.value} has {len(open_gaps)} conflicting "
                "unclosed stream discontinuities"
            )
        if not open_gaps:
            return
        evidence = open_gaps[0].get("evidence")
        if not isinstance(evidence, dict):
            raise IngressGapStateConflict(
                f"Spot {self.stream.value} has malformed open gap evidence"
            )
        gap_id = evidence.get("gap_id")
        started_at = evidence.get("gap_started_at_utc_ns")
        connection_id = evidence.get("original_connection_id")
        original_generation = evidence.get("original_generation")
        if (
            not isinstance(gap_id, str)
            or not gap_id
            or not isinstance(started_at, int)
            or isinstance(started_at, bool)
            or started_at < 0
            or not isinstance(connection_id, str)
            or not connection_id
            or not isinstance(original_generation, int)
            or isinstance(original_generation, bool)
            or original_generation < 0
            or evidence.get("reason") not in RECONNECT_REASONS
        ):
            raise IngressGapStateConflict(
                f"Spot {self.stream.value} has invalid open gap identity"
            )
        self._pending_gap = {
            "gap_id": gap_id,
            "gap_started_at_utc_ns": started_at,
            "original_connection_id": connection_id,
            "original_generation": original_generation,
            "reason": evidence["reason"],
        }
        self._generation = original_generation + 1
        self._recovery_flag_pending = True
        self._recovery_marker_enqueued = False
        log_event(
            self.logger,
            logging.WARNING,
            "spot_ingress_gap_recovered",
            "Spot stream recovered an unclosed discontinuity from Catalog",
            stream=self.stream.value,
            connection_id=connection_id,
            generation=self._generation,
            gap_id=gap_id,
            outcome="RECOVERY_PENDING",
        )

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

    def _with_recovery_marker(self, receipt: ReceivedFrame) -> ReceivedFrame:
        if not self._recovery_flag_pending or self._recovery_marker_enqueued:
            return receipt
        return replace(
            receipt,
            capture_flags=tuple(
                dict.fromkeys((*receipt.capture_flags, "sequence_gap"))
            ),
        )

    def _accept(self, receipt: ReceivedFrame) -> None:
        marker = self._with_recovery_marker(receipt)
        if marker is not receipt:
            self._recovery_marker_enqueued = True
        try:
            self._receipts.put_nowait(marker)
        except asyncio.QueueFull as exc:
            if marker is not receipt and self._pending_gap is not None:
                self._recovery_marker_enqueued = False
            raise IngressQueueFull("WebSocket receipt queue is full") from exc
        self._connection_receipt_count += 1

    async def _record_gap_started(
        self,
        boundary: ReceivedFrame | None,
        reason: str,
        *,
        started_at_utc_ns: int,
        connection_id: str,
    ) -> None:
        if reason not in RECONNECT_REASONS:
            raise ValueError(f"unknown Spot reconnect reason: {reason}")
        if self._pending_gap is not None:
            raise IngressGapStateConflict(
                f"Spot {self.stream.value} cannot start a second discontinuity "
                "while an earlier gap remains open"
            )
        gap_id = str(uuid4())
        evidence: dict[str, object] = {
            "gap_id": gap_id,
            "market": "spot",
            "stream": self.stream.value,
            "reason": reason,
            "interval_classification": "UNRELIABLE",
            "gap_started_at_utc_ns": started_at_utc_ns,
            "original_connection_id": connection_id,
            "original_generation": self._generation,
            "boundary_kind": (
                "last_frame_in_hand"
                if boundary is not None
                else "no_last_frame_available"
            ),
            "boundary_frame_persisted": boundary is not None,
            "boundary_precision": (
                "connection closed after the last Recorder-received frame; "
                "unread WebSocket/TCP buffers are indeterminate"
                if boundary is not None
                else (
                    "no unpersisted last-old frame exists at the boundary; "
                    "already persisted frames were not modified and no "
                    "boundary payload hash is fabricated"
                )
            ),
        }
        if boundary is not None:
            evidence["boundary_payload_sha256"] = hashlib.sha256(
                boundary.raw_payload
            ).hexdigest()
        await run_owned_blocking_call(
            self.spool.catalog.record_operational_event,
            event_id=f"stream-discontinuity-started:{gap_id}",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=started_at_utc_ns,
            evidence=evidence,
        )
        self._pending_gap = {
            "gap_id": gap_id,
            "gap_started_at_utc_ns": started_at_utc_ns,
            "original_connection_id": connection_id,
            "original_generation": self._generation,
            "reason": reason,
        }

    async def _record_gap_completed(self, envelope: EventEnvelope) -> None:
        gap = self._pending_gap
        if gap is None or "sequence_gap" not in envelope.capture_flags:
            return
        completed_at = envelope.receive_time_utc_ns
        gap_id = str(gap["gap_id"])
        await run_owned_blocking_call(self.spool.sync)
        await run_owned_blocking_call(
            self.spool.catalog.record_operational_event,
            event_id=f"stream-discontinuity-completed:{gap_id}",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=completed_at,
            evidence={
                **gap,
                "market": "spot",
                "stream": self.stream.value,
                "reason": gap["reason"],
                "interval_classification": "UNRELIABLE",
                "gap_ended_at_utc_ns": completed_at,
                "new_connection_id": envelope.connection_id,
                "new_generation": self._generation,
                "raw_gap_marker": "sequence_gap",
                "historical_continuity_restored": False,
            },
        )
        self._pending_gap = None
        self._recovery_flag_pending = False
        self._recovery_marker_enqueued = False

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
            for envelope in persisted:
                await self._record_gap_completed(envelope)
            if self.envelope_observer is not None:
                for envelope in persisted:
                    self.envelope_observer(envelope)
        await run_owned_blocking_call(
            self.spool.close_and_seal,
            forced_flags=self._forced_seal_flags,
        )

    async def _receive_connection(
        self,
        websocket: WebSocketConnection,
        connection_id: str,
        stop: asyncio.Event,
        writer_task: asyncio.Task[None],
        session_restart: asyncio.Event | None = None,
    ) -> str:
        deadline = asyncio.get_running_loop().time() + self.planned_rotation_seconds
        while True:
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            receive_task = asyncio.create_task(
                self._receive_once(websocket, connection_id)
            )
            stop_task = asyncio.create_task(stop.wait())
            shutdown_task = asyncio.create_task(self._server_shutdown.wait())
            try:
                done, pending = await asyncio.wait(
                    {receive_task, stop_task, shutdown_task, writer_task},
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except BaseException:
                for cleanup_task in (receive_task, stop_task, shutdown_task):
                    if not cleanup_task.done():
                        cleanup_task.cancel()
                await asyncio.gather(
                    receive_task,
                    stop_task,
                    shutdown_task,
                    return_exceptions=True,
                )
                raise
            cancelled = [task for task in pending if task is not writer_task]
            for cancelled_task in cancelled:
                cancelled_task.cancel()
            if cancelled:
                await asyncio.gather(*cancelled, return_exceptions=True)
            if writer_task in done:
                if receive_task in done:
                    await asyncio.gather(receive_task, return_exceptions=True)
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
                    if self._is_session_restart(session_restart):
                        await websocket.close(code=1000, reason="collector shutdown")
                        return "session_restart"
                    if stop.is_set():
                        await websocket.close(code=1000, reason="collector shutdown")
                        return "graceful_shutdown"
                    raise
                self._accept(receipt)
                continue
            if stop_task in done and stop_task.result():
                receive_task.cancel()
                await websocket.close(code=1000, reason="collector shutdown")
                if self._is_session_restart(session_restart):
                    return "session_restart"
                return "graceful_shutdown"
            if shutdown_task in done and shutdown_task.result():
                receive_task.cancel()
                self._server_shutdown.clear()
                await websocket.close(code=1000, reason="serverShutdown received")
                return "server_shutdown"

    @staticmethod
    def _is_session_restart(session_restart: asyncio.Event | None) -> bool:
        return session_restart is not None and session_restart.is_set()

    def _remember_boundary(self, connection_id: str) -> None:
        """Capture the closing connection identity before the generation seals."""
        self._boundary_connection_id = connection_id
        self._boundary_detected_at_utc_ns = self.utc_clock_ns()

    async def _connection_loop(
        self,
        stop: asyncio.Event,
        writer_task: asyncio.Task[None],
        session_restart: asyncio.Event | None = None,
    ) -> str:
        failures = 0
        while not stop.is_set():
            connection_id = str(uuid4())
            reason = "unexpected_disconnect"
            connected_at: float | None = None
            was_connected = False
            self._connection_receipt_count = 0
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
                        websocket,
                        connection_id,
                        stop,
                        writer_task,
                        session_restart=session_restart,
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
                if not was_connected or self._connection_receipt_count == 0:
                    # The opener failed or the connection closed before
                    # delivering any frame; an open pending gap simply
                    # continues with no new STARTED or generation bump.
                    with suppress(TimeoutError):
                        await asyncio.wait_for(
                            stop.wait(),
                            timeout=self.backoff.delay(max(1, failures)),
                        )
                    continue
                self._remember_boundary(connection_id)
                return "unexpected_disconnect"
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
            if reason in RECONNECT_REASONS:
                if reason == "planned_rotation":
                    failures = 0
                    if self.lifecycle_observer is not None:
                        self.lifecycle_observer("planned_rotation")
                elif reason == "server_shutdown" and self.lifecycle_observer is not None:
                    self.lifecycle_observer("server_shutdown")
                log_event(
                    self.logger,
                    logging.INFO,
                    f"spot_{reason}",
                    "Binance Spot connection will be replaced",
                    stream=self.stream.value,
                    connection_id=connection_id,
                )
                self._remember_boundary(connection_id)
                return reason
            if stop.is_set() or reason == "graceful_shutdown":
                return "stopped"
            delay = self.backoff.delay(max(1, failures))
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)
        return "stopped"

    async def run(
        self,
        stop: asyncio.Event,
        session_restart: asyncio.Event | None = None,
    ) -> None:
        """Run one generation at a time; every reconnect boundary seals it."""

        while not stop.is_set():
            producer_done = asyncio.Event()
            self._forced_seal_flags = frozenset()
            self._boundary_connection_id = None
            self._boundary_detected_at_utc_ns = None
            writer_task = asyncio.create_task(self._writer_loop(producer_done))
            outcome = "stopped"
            try:
                outcome = await self._connection_loop(
                    stop, writer_task, session_restart=session_restart
                )
            finally:
                if outcome in RECONNECT_REASONS:
                    # No unpersisted last-old frame exists on the Spot path;
                    # the sealed tail chunk gets the manifest-level
                    # reconnect_gap flag. Persisted Raw frames stay untouched.
                    self._forced_seal_flags = frozenset({RECONNECT_GAP_FLAG})
                producer_done.set()
                await writer_task
            if outcome == "stopped":
                return
            if outcome not in RECONNECT_REASONS:
                raise RuntimeError(f"unknown Spot connection outcome: {outcome}")
            if self._pending_gap is None:
                if (
                    self._boundary_connection_id is None
                    or self._boundary_detected_at_utc_ns is None
                ):
                    raise RuntimeError(f"missing Spot boundary identity for {outcome}")
                await self._record_gap_started(
                    None,
                    outcome,
                    started_at_utc_ns=self._boundary_detected_at_utc_ns,
                    connection_id=self._boundary_connection_id,
                )
                self._generation += 1
                self._recovery_flag_pending = True
                self._recovery_marker_enqueued = False
                self._forced_seal_flags = frozenset()
            else:
                # A pending gap already covers this boundary; it continues
                # until the first reliable new-generation frame is persisted.
                log_event(
                    self.logger,
                    logging.WARNING,
                    "spot_ingress_gap_extended",
                    "Spot reconnect boundary extends the pending discontinuity",
                    stream=self.stream.value,
                    connection_id=self._boundary_connection_id or "unknown",
                    generation=self._generation,
                    gap_id=self._pending_gap["gap_id"],
                    reason=outcome,
                    outcome="GAP_EXTENDED",
                )
                if self.stream == SpotStream.DIFF_DEPTH:
                    return
                continue
            log_event(
                self.logger,
                logging.WARNING,
                "spot_ingress_stream_recovery",
                "Spot stream is opening a new generation with persistent gap evidence",
                stream=self.stream.value,
                connection_id=self._boundary_connection_id or "unknown",
                generation=self._generation,
                outcome=(
                    "DEPTH_RESYNC_REQUIRED"
                    if self.stream == SpotStream.DIFF_DEPTH
                    else "STREAM_RECONNECT"
                ),
            )
            if self.stream == SpotStream.DIFF_DEPTH:
                return
