"""Owned lifecycle for one Binance USD-M raw WebSocket stream.

Every transport boundary that closes one connection and opens another is
routed through one reconnect-boundary state machine (M21.4.11):

    CONNECTED(generation N)
        -> boundary detected (backpressure / unexpected disconnect /
           planned rotation / server shutdown / session restart)
        -> drain old generation
        -> seal old generation (forced manifest gap when no in-hand frame)
        -> Catalog STREAM_DISCONTINUITY_STARTED durable
        -> generation N + 1
        -> open new connection
        -> first new frame carries sequence_gap
        -> Raw sync
        -> Catalog STREAM_DISCONTINUITY_COMPLETED
        -> CONNECTED(generation N + 1, historical continuity false)

Exchange-side completeness between close and the first new frame can never be
proven; intentional close (planned rotation) is not an exemption. When no
unpersisted last-old frame is available the old generation is sealed through
the manifest-level ``reconnect_gap`` marker; persisted Raw frames are never
mutated and no exchange payload is fabricated. A connection that fails before
delivering any frame extends the pending gap without a new STARTED or a new
generation.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager, suppress
from dataclasses import replace
from typing import Protocol
from uuid import uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from ...domain.event import EventEnvelope
from ...logging import log_event
from ...network import WebSocketProxy
from ...spool.async_queue import AsyncQueueStats, BoundedAsyncQueue
from ...spool.queue import (
    IngressBackpressureTimeout,
    IngressGapStateConflict,
    IngressPostCloseHandoffTimeout,
    IngressStopRequested,
)
from ...spool.seal import RECONNECT_GAP_FLAG
from ...spool.stream import StreamSpool
from ..spot.websocket import ReceivedFrame, ReconnectBackoff
from ..websocket_common import (
    RECONNECT_REASONS,
    release_writer_preserving_errors,
    run_owned_blocking_call,
)
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
        backpressure_put_timeout_seconds: float = 1.0,
        backpressure_saturation_timeout_seconds: float = 30.0,
        post_close_handoff_timeout_seconds: float = 5.0,
    ) -> None:
        if route not in {"public", "market"}:
            raise ValueError("USD-M market data route must be public or market")
        if receipt_queue_capacity < 1:
            raise ValueError("receipt queue capacity must be positive")
        if not 0 < planned_rotation_seconds < 24 * 60 * 60:
            raise ValueError("planned rotation must occur before the 24-hour limit")
        if post_close_handoff_timeout_seconds <= 0:
            raise ValueError("post-close handoff timeout must be positive")
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
        self._receipts = BoundedAsyncQueue[ReceivedFrame](
            receipt_queue_capacity,
            put_timeout_seconds=backpressure_put_timeout_seconds,
            saturation_timeout_seconds=backpressure_saturation_timeout_seconds,
        )
        self.post_close_handoff_timeout_seconds = post_close_handoff_timeout_seconds
        self._backpressure_active = False
        self._generation = 0
        self._recovery_flag_pending = False
        self._recovery_marker_enqueued = False
        self._pending_gap: dict[str, object] | None = None
        self._backpressure_boundary: ReceivedFrame | None = None
        self._forced_seal_flags: frozenset[str] = frozenset()
        self._seal_intent: dict[str, object] | None = None
        self._boundary_connection_id: str | None = None
        self._boundary_detected_at_utc_ns: int | None = None
        self._connection_receipt_count = 0
        self._last_writer_batch_size = 0
        self._last_writer_drain_ns = 0
        self._max_writer_drain_ns = 0
        self._active_connection_id: str | None = None
        self._restore_open_gap()

    @property
    def url(self) -> str:
        return f"{self.websocket_root}/{self.route}/ws/{self.wire_name}"

    def set_capture_flags(self, flags: tuple[str, ...]) -> None:
        self._capture_flags = flags

    @property
    def receipt_queue_stats(self) -> AsyncQueueStats:
        return self._receipts.snapshot()

    def _restore_open_gap(self) -> None:
        open_gaps = self.spool.catalog.unclosed_stream_discontinuities(
            market="um_perpetual",
            stream=self.stream_name,
        )
        if len(open_gaps) > 1:
            raise IngressGapStateConflict(
                f"USD-M {self.stream_name} has {len(open_gaps)} conflicting "
                "unclosed stream discontinuities"
            )
        if not open_gaps:
            return
        evidence = open_gaps[0].get("evidence")
        if not isinstance(evidence, dict):
            raise IngressGapStateConflict(
                f"USD-M {self.stream_name} has malformed open gap evidence"
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
                f"USD-M {self.stream_name} has invalid open gap identity"
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
            "usdm_ingress_gap_recovered",
            "USD-M stream recovered an unclosed discontinuity from Catalog",
            stream=self.stream_name,
            connection_id=connection_id,
            generation=self._generation,
            gap_id=gap_id,
            outcome="RECOVERY_PENDING",
            **self._queue_fields(),
        )

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

    def _with_recovery_marker(self, receipt: ReceivedFrame) -> ReceivedFrame:
        if not self._recovery_flag_pending or self._recovery_marker_enqueued:
            return receipt
        return replace(
            receipt,
            capture_flags=tuple(
                dict.fromkeys((*receipt.capture_flags, "sequence_gap"))
            ),
        )

    async def _accept(
        self,
        receipt: ReceivedFrame,
        writer_task: asyncio.Task[None],
        stop: asyncio.Event,
    ) -> None:
        if self._receipts.depth >= self._receipts.capacity and not self._backpressure_active:
            self._backpressure_active = True
            self._log_ingress_state(
                logging.WARNING,
                "usdm_ingress_backpressure_started",
                "bounded USD-M receipt queue started applying producer backpressure",
                connection_id=receipt.connection_id,
                outcome="WAITING",
            )
        recovery_marker = self._with_recovery_marker(receipt)
        reserved = recovery_marker is not receipt
        if reserved:
            self._recovery_marker_enqueued = True
        try:
            await self._receipts.put(
                recovery_marker,
                writer_task=writer_task,
                stop=stop,
            )
        except BaseException:
            if reserved and self._pending_gap is not None:
                self._recovery_marker_enqueued = False
            raise
        self._connection_receipt_count += 1

    def _queue_fields(self) -> dict[str, object]:
        stats = self._receipts.snapshot()
        operations = self.spool.operation_stats()
        return {
            "receipt_queue_capacity": stats.capacity,
            "receipt_queue_depth": stats.depth,
            "receipt_queue_high_watermark": stats.high_watermark,
            "queue_wait_count": stats.wait_count,
            "queue_wait_total_ns": stats.wait_total_ns,
            "queue_wait_max_ns": stats.wait_max_ns,
            "queue_wait_p50_ns": stats.wait_p50_ns,
            "queue_wait_p95_ns": stats.wait_p95_ns,
            "queue_wait_p99_ns": stats.wait_p99_ns,
            "saturation_seconds": stats.saturation_seconds,
            "writer_batch_size": self._last_writer_batch_size,
            "writer_drain_ns": self._last_writer_drain_ns,
            "writer_drain_max_ns": self._max_writer_drain_ns,
            "writer_append_ns": operations.append_last_ns,
            "writer_append_max_ns": operations.append_max_ns,
            "writer_write_ns": operations.write_last_ns,
            "writer_write_max_ns": operations.write_max_ns,
            "writer_fsync_ns": operations.fsync_last_ns,
            "writer_fsync_max_ns": operations.fsync_max_ns,
            "writer_seal_ns": operations.seal_last_ns,
            "writer_seal_max_ns": operations.seal_max_ns,
        }

    def _log_ingress_state(
        self,
        level: int,
        event: str,
        message: str,
        *,
        connection_id: str,
        outcome: str,
    ) -> None:
        log_event(
            self.logger,
            level,
            event,
            message,
            stream=self.stream_name,
            connection_id=connection_id,
            generation=self._generation,
            outcome=outcome,
            **self._queue_fields(),
        )

    async def _record_gap_started(
        self,
        boundary: ReceivedFrame | None,
        reason: str,
        *,
        gap_id: str,
        started_at_utc_ns: int,
        connection_id: str,
    ) -> None:
        if reason not in RECONNECT_REASONS:
            raise ValueError(f"unknown USD-M reconnect reason: {reason}")
        if not gap_id:
            raise ValueError("gap_id must be non-empty")
        if self._pending_gap is not None:
            raise IngressGapStateConflict(
                f"USD-M {self.stream_name} cannot start a second discontinuity "
                "while an earlier gap remains open"
            )
        if boundary is not None:
            boundary_frame_persisted = True
            boundary_payload_sha256 = hashlib.sha256(boundary.raw_payload).hexdigest()
            boundary_kind = "last_frame_in_hand"
        else:
            boundary_frame_persisted = False
            boundary_payload_sha256 = None
            boundary_kind = "no_last_frame_available"
        evidence: dict[str, object] = {
            "gap_id": gap_id,
            "market": "um_perpetual",
            "stream": self.stream_name,
            "reason": reason,
            "interval_classification": "UNRELIABLE",
            "gap_started_at_utc_ns": started_at_utc_ns,
            "original_connection_id": connection_id,
            "original_generation": self._generation,
            "boundary_kind": boundary_kind,
            "boundary_frame_persisted": boundary_frame_persisted,
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
            **self._queue_fields(),
        }
        if boundary_payload_sha256 is not None:
            evidence["boundary_payload_sha256"] = boundary_payload_sha256
        await _run_owned_blocking_call(
            self.spool.catalog.ensure_operational_event,
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
        if str(envelope.connection_id) == str(gap["original_connection_id"]):
            # The gap's own boundary frame (for example an ingress
            # backpressure sequence_gap marker drained with the old
            # generation) must not close the discontinuity: COMPLETED is
            # legal only after a first-new-generation frame is durably
            # synced. The identity check keeps the completion boundary-local.
            return
        completed_at = envelope.receive_time_utc_ns
        gap_id = str(gap["gap_id"])
        await _run_owned_blocking_call(self.spool.sync)
        await _run_owned_blocking_call(
            self.spool.catalog.ensure_operational_event,
            event_id=f"stream-discontinuity-completed:{gap_id}",
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=completed_at,
            evidence={
                **gap,
                "market": "um_perpetual",
                "stream": self.stream_name,
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

    def _build_seal_intent(
        self, outcome: str, boundary: ReceivedFrame | None
    ) -> dict[str, object] | None:
        """Build the durable seal-intent document for a reconnect boundary.

        The intent captures the required manifest-level seal semantics plus
        the exact boundary identity (gap_id, reason, original connection,
        original generation, boundary timestamp) BEFORE any storage mutation
        whose crash recovery depends on it. ``seal_partial`` persists it into
        the ChunkState.SEALING transition evidence; if the Catalog STARTED
        event then fails to commit, startup recovery reconstructs the
        fail-closed seal and the pending discontinuity from this evidence
        (P1-A, INV-005/INV-007). The gap_id is minted once per boundary and
        shared with the STARTED/COMPLETED pair so recovery never invents a
        second logical gap (INV-010, REQ-106).
        """
        if self._boundary_connection_id is None or self._boundary_detected_at_utc_ns is None:
            return None
        started_at = (
            boundary.receive_time_utc_ns
            if boundary is not None
            else self._boundary_detected_at_utc_ns
        )
        intent: dict[str, object] = {
            "required_forced_flags": sorted(self._forced_seal_flags),
            "gap_id": str(uuid4()),
            "reason": outcome,
            "market": "um_perpetual",
            "stream": self.stream_name,
            "original_connection_id": self._boundary_connection_id,
            "original_generation": self._generation,
            "gap_started_at_utc_ns": started_at,
            "boundary_kind": (
                "last_frame_in_hand"
                if boundary is not None
                else "no_last_frame_available"
            ),
            "boundary_frame_persisted": boundary is not None,
        }
        if boundary is not None:
            intent["boundary_payload_sha256"] = hashlib.sha256(
                boundary.raw_payload
            ).hexdigest()
        return intent

    async def _writer_loop(self, producer_done: asyncio.Event) -> None:
        try:
            await self._write_until_done(producer_done)
        except asyncio.CancelledError:
            await _run_owned_blocking_call(self.spool.abort_writer)
            raise
        except BaseException as writer_error:
            log_event(
                self.logger,
                logging.CRITICAL,
                "usdm_ingress_writer_failed",
                "USD-M Raw writer stopped before its ingress generation completed",
                stream=self.stream_name,
                connection_id=self._active_connection_id or "unavailable",
                generation=self._generation,
                outcome="FATAL",
                **self._queue_fields(),
            )
            try:
                await _run_owned_blocking_call(self.spool.abort_writer)
            except asyncio.CancelledError as cancellation:
                # Once the Writer has failed, a coincident cancellation during
                # descriptor cleanup must not hide the integrity failure.
                raise writer_error from cancellation
            except BaseException as abort_error:
                raise abort_error from writer_error
            raise

    async def _persist_batch(
        self, batch: list[ReceivedFrame]
    ) -> list[EventEnvelope]:
        persisted: list[EventEnvelope] = []
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
                                    (*envelope.capture_flags, *receipt.capture_flags)
                                )
                            )
                        }
                    )
            self.spool.enqueue(envelope)
            persisted.append(envelope)
        drain_started = time.perf_counter_ns()
        drained = await _run_owned_blocking_call(self.spool.drain_all)
        drain_duration = time.perf_counter_ns() - drain_started
        self._last_writer_batch_size = len(batch)
        self._last_writer_drain_ns = drain_duration
        self._max_writer_drain_ns = max(self._max_writer_drain_ns, drain_duration)
        if drained != len(batch):
            raise RuntimeError("USD-M Raw spool did not drain the complete writer batch")
        return persisted

    async def _write_until_done(self, producer_done: asyncio.Event) -> None:
        while not producer_done.is_set() or not self._receipts.empty():
            batch: list[ReceivedFrame] = []
            try:
                batch.append(await asyncio.wait_for(self._receipts.get(), timeout=0.1))
            except TimeoutError:
                await _run_owned_blocking_call(self.spool.drain_all)
                continue
            for _ in range(min(255, self.spool.queue.capacity - 1)):
                try:
                    batch.append(self._receipts.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                persisted = await self._persist_batch(batch)
            except BaseException:
                for _receipt in batch:
                    self._receipts.task_done()
                raise
            for _receipt in batch:
                self._receipts.task_done()
            for envelope in persisted:
                await self._record_gap_completed(envelope)
            if self.envelope_observer is not None:
                for envelope in persisted:
                    self.envelope_observer(envelope)
            if self._receipts.note_consumer_progress() and self._backpressure_active:
                self._backpressure_active = False
                connection_id = persisted[-1].connection_id if persisted else "unknown"
                self._log_ingress_state(
                    logging.INFO,
                    "usdm_ingress_backpressure_recovered",
                    "USD-M receipt queue recovered below its low watermark",
                    connection_id=connection_id,
                    outcome="RECOVERED",
                )
        await _run_owned_blocking_call(
            self.spool.close_and_seal,
            forced_flags=self._forced_seal_flags,
            seal_intent=self._seal_intent,
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
            receive_task = asyncio.create_task(self._receive_once(websocket, connection_id))
            stop_task = asyncio.create_task(stop.wait())
            try:
                done, pending = await asyncio.wait(
                    {receive_task, stop_task, writer_task},
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            except BaseException:
                for cleanup_task in (receive_task, stop_task):
                    if not cleanup_task.done():
                        cleanup_task.cancel()
                await asyncio.gather(
                    receive_task,
                    stop_task,
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
                raise RuntimeError("USD-M Raw writer stopped unexpectedly")
            if not done:
                receive_task.cancel()
                await websocket.close(code=1000, reason="planned 24-hour rotation")
                return "planned_rotation"
            if receive_task in done:
                try:
                    receipt = receive_task.result()
                    await self._accept(receipt, writer_task, stop)
                except IngressBackpressureTimeout:
                    self._log_ingress_state(
                        logging.ERROR,
                        "usdm_ingress_backpressure_timeout",
                        "USD-M receipt queue exceeded its bounded saturation budget",
                        connection_id=connection_id,
                        outcome="ROTATE_CONNECTION",
                    )
                    await websocket.close(
                        code=1013,
                        reason="bounded ingress backpressure",
                    )
                    boundary = replace(
                        receipt,
                        capture_flags=tuple(
                            dict.fromkeys((*receipt.capture_flags, "sequence_gap"))
                        ),
                    )
                    try:
                        await self._receipts.put_after_connection_close(
                            boundary,
                            writer_task=writer_task,
                            timeout_seconds=self.post_close_handoff_timeout_seconds,
                        )
                    except IngressPostCloseHandoffTimeout:
                        self._log_ingress_state(
                            logging.CRITICAL,
                            "usdm_ingress_post_close_handoff_timeout",
                            "received USD-M boundary frame couldn't enter its Raw writer queue",
                            connection_id=connection_id,
                            outcome="FATAL",
                        )
                        raise
                    if self._recovery_flag_pending:
                        self._recovery_marker_enqueued = True
                    self._backpressure_boundary = boundary
                    return "ingress_backpressure"
                except IngressStopRequested:
                    await websocket.close(code=1000, reason="collector shutdown")
                    receipt = self._with_recovery_marker(receipt)
                    await self._receipts.put_after_connection_close(
                        receipt,
                        writer_task=writer_task,
                        timeout_seconds=self.post_close_handoff_timeout_seconds,
                    )
                    if self._recovery_flag_pending:
                        self._recovery_marker_enqueued = True
                    if self._is_session_restart(session_restart):
                        return "session_restart"
                    return "graceful_shutdown"
                except (WebSocketException, OSError, TimeoutError):
                    if self._is_session_restart(session_restart):
                        await websocket.close(code=1000, reason="collector shutdown")
                        return "session_restart"
                    if stop.is_set():
                        await websocket.close(code=1000, reason="collector shutdown")
                        return "graceful_shutdown"
                    raise
                continue
            if stop_task in done and stop_task.result():
                receive_task.cancel()
                await websocket.close(code=1000, reason="collector shutdown")
                if self._is_session_restart(session_restart):
                    return "session_restart"
                return "graceful_shutdown"

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
                    self._active_connection_id = connection_id
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
                if not was_connected or self._connection_receipt_count == 0:
                    # The opener failed or the connection closed before
                    # delivering any frame. Nothing from this connection can
                    # reach Raw, so an open pending gap simply continues; no
                    # new STARTED and no generation bump for this attempt.
                    with suppress(TimeoutError):
                        await asyncio.wait_for(
                            stop.wait(),
                            timeout=self.backoff.delay(max(1, failures)),
                        )
                    continue
                self._remember_boundary(connection_id)
                return "unexpected_disconnect"
            finally:
                if was_connected and self.lifecycle_observer is not None:
                    self.lifecycle_observer("disconnected")
            if reason in RECONNECT_REASONS:
                if reason == "ingress_backpressure":
                    if self.failure_observer is not None:
                        self.failure_observer("IngressBackpressureTimeout")
                    if self.lifecycle_observer is not None:
                        self.lifecycle_observer("ingress_backpressure")
                elif reason in {"planned_rotation", "server_shutdown"}:
                    if self.lifecycle_observer is not None:
                        self.lifecycle_observer(reason)
                self._remember_boundary(connection_id)
                return reason
            if stop.is_set() or reason == "graceful_shutdown":
                return "stopped"
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.backoff.delay(max(1, failures)))
        return "stopped"

    async def run(
        self,
        stop: asyncio.Event,
        session_restart: asyncio.Event | None = None,
    ) -> None:
        """Run one generation at a time; every reconnect boundary seals it.

        Crash-durable ordering (M21.4.11-R1): the Catalog
        STREAM_DISCONTINUITY_STARTED intent is persisted BEFORE the old
        generation drains and seals. A crash at any persistence phase then
        leaves the durable intent behind, and startup recovery seals the old
        partial fail-closed (reconnect_gap) instead of claiming
        complete=true.
        """
        while not stop.is_set():
            producer_done = asyncio.Event()
            self._forced_seal_flags = frozenset()
            self._seal_intent = None
            self._boundary_connection_id = None
            self._boundary_detected_at_utc_ns = None
            writer_task = asyncio.create_task(self._writer_loop(producer_done))
            outcome = "stopped"
            gap_just_started = False
            try:
                outcome = await self._connection_loop(
                    stop, writer_task, session_restart=session_restart
                )
            finally:
                if outcome == "ingress_backpressure":
                    # The boundary frame itself carries a persisted
                    # sequence_gap marker; no manifest-level forcing is needed.
                    self._forced_seal_flags = frozenset()
                elif outcome in RECONNECT_REASONS:
                    # No unpersisted last-old frame exists to carry the
                    # marker; the sealed tail chunk gets the manifest-level
                    # reconnect_gap flag. Persisted Raw frames stay untouched.
                    self._forced_seal_flags = frozenset({RECONNECT_GAP_FLAG})
                if outcome in RECONNECT_REASONS:
                    # Build the durable seal intent BEFORE any storage
                    # mutation whose crash recovery depends on it (INV-007,
                    # P1-A): seal_partial persists it into the SEALING
                    # transition evidence, so even if the STARTED event below
                    # fails to commit, restart reconstruction still seals the
                    # old generation fail-closed. The gap_id is minted once
                    # per boundary and shared with STARTED/COMPLETED.
                    boundary = (
                        self._backpressure_boundary
                        if outcome == "ingress_backpressure"
                        else None
                    )
                    self._seal_intent = self._build_seal_intent(
                        outcome, boundary
                    )
                if outcome in RECONNECT_REASONS and self._pending_gap is None:
                    # Persist the reconnect intent BEFORE any storage mutation
                    # whose correct recovery depends on it (INV-007): the
                    # seal below is exactly such a mutation, because a crash
                    # during it must not later seal complete=true.
                    try:
                        if (
                            self._seal_intent is None
                            or self._boundary_connection_id is None
                            or self._boundary_detected_at_utc_ns is None
                        ):
                            raise RuntimeError(
                                f"missing USD-M boundary identity for {outcome}"
                            )
                        await self._record_gap_started(
                            boundary,
                            outcome,
                            gap_id=str(self._seal_intent["gap_id"]),
                            started_at_utc_ns=(
                                boundary.receive_time_utc_ns
                                if boundary is not None
                                else self._boundary_detected_at_utc_ns
                            ),
                            connection_id=self._boundary_connection_id,
                        )
                        gap_just_started = True
                        self._generation += 1
                        self._recovery_flag_pending = True
                        self._recovery_marker_enqueued = False
                    except BaseException as intent_error:
                        # The durable intent could not be recorded. Release
                        # the writer so no owned blocking work is abandoned:
                        # it drains and seals the old generation with the
                        # fail-closed flags and durable seal intent set
                        # above. If the writer also fails, both causal facts
                        # are preserved (REQ-109).
                        producer_done.set()
                        await release_writer_preserving_errors(
                            writer_task, intent_error
                        )
                producer_done.set()
                await writer_task
                if (
                    outcome in RECONNECT_REASONS
                    and self._pending_gap is None
                    and not gap_just_started
                ):
                    # The pre-seal intent decision above saw the pending gap's
                    # first-new frame still in the writer queue, so it skipped
                    # recording new intent; the drain then persisted that
                    # frame and completed the gap (COMPLETED only after Raw
                    # sync). This boundary therefore has no durable intent
                    # yet: the replacement generation must not deliver frames
                    # before STARTED is durable (INV-007/INV-009), so the
                    # intent is recorded now, before the next connection
                    # opens. There is no in-hand boundary frame for this
                    # transition: the old connection's frames were already
                    # drained with its generation. The gap identity is the
                    # same one the durable seal intent carried.
                    if (
                        self._seal_intent is None
                        or self._boundary_connection_id is None
                        or self._boundary_detected_at_utc_ns is None
                    ):
                        raise RuntimeError(
                            f"missing USD-M boundary identity for {outcome}"
                        )
                    await self._record_gap_started(
                        None,
                        outcome,
                        gap_id=str(self._seal_intent["gap_id"]),
                        started_at_utc_ns=self._boundary_detected_at_utc_ns,
                        connection_id=self._boundary_connection_id,
                    )
                    gap_just_started = True
                    self._generation += 1
                    self._recovery_flag_pending = True
                    self._recovery_marker_enqueued = False
            if outcome == "stopped":
                return
            if outcome == "ingress_backpressure":
                boundary = self._backpressure_boundary
                self._backpressure_boundary = None
                if boundary is None:
                    raise RuntimeError(
                        "missing USD-M backpressure generation boundary"
                    )
            elif outcome in RECONNECT_REASONS:
                boundary = None
            else:
                raise RuntimeError(f"unknown USD-M connection outcome: {outcome}")
            if self._pending_gap is not None and not gap_just_started:
                # A pending gap already covers this boundary (for example a
                # connection that failed before its first frame); it continues
                # until the first reliable new-generation frame is persisted.
                # Generation identity and gap evidence are unchanged.
                log_event(
                    self.logger,
                    logging.WARNING,
                    "usdm_ingress_gap_extended",
                    "USD-M reconnect boundary extends the pending discontinuity",
                    stream=self.stream_name,
                    connection_id=self._boundary_connection_id or "unknown",
                    generation=self._generation,
                    gap_id=self._pending_gap["gap_id"],
                    reason=outcome,
                    outcome="GAP_EXTENDED",
                    **self._queue_fields(),
                )
                if self.stream == UsdMStream.DIFF_DEPTH:
                    return
                continue
            self._backpressure_active = False
            self._receipts.note_consumer_progress()
            self._log_ingress_state(
                logging.WARNING,
                "usdm_ingress_stream_recovery",
                "USD-M stream is opening a new generation with persistent gap evidence",
                connection_id=self._boundary_connection_id or "unknown",
                outcome=(
                    "DEPTH_RESYNC_REQUIRED"
                    if self.stream == UsdMStream.DIFF_DEPTH
                    else "STREAM_RECONNECT"
                ),
            )
            if self.stream == UsdMStream.DIFF_DEPTH:
                return
