"""Combined-stream routing and market-agnostic WebSocket collector base."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass, field, replace
from hashlib import sha256
from typing import Any, Protocol, cast
from urllib.parse import quote
from uuid import uuid4

from websockets.exceptions import WebSocketException

from ..domain.event import EventEnvelope, Market
from ..domain.product import Product, product_log_fields
from ..logging import log_event
from ..spool.async_queue import AsyncQueueStats, BoundedAsyncQueue
from ..spool.queue import (
    IngressBackpressureTimeout,
    IngressGapStateConflict,
    IngressPostCloseHandoffTimeout,
    IngressStopRequested,
)
from ..spool.seal import RECONNECT_GAP_FLAG, RECONNECT_INTENT_SCHEMA_V2
from ..spool.stream import StreamSpool
from ..storage.catalog import stream_discontinuity_event_id
from .public_sources import Source
from .websocket_common import (
    DEFAULT_RECEIPT_BYTE_CAPACITY,
    RECONNECT_REASONS,
    ConnectionReceiveControl,
    capped_backoff_seconds,
    release_writer_preserving_errors,
    run_connection_receiver,
    run_owned_blocking_call,
    websocket_disconnect_log_fields,
)

_run_owned_blocking_call = run_owned_blocking_call


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
        base = capped_backoff_seconds(self.initial_seconds, self.maximum_seconds, failures)
        sample: float = random.random() if random_value is None else random_value
        multiplier = 1 + self.jitter_ratio * (2 * sample - 1)
        return float(min(self.maximum_seconds, max(0.0, base * multiplier)))


ConnectionOpener = Callable[[str], AbstractAsyncContextManager[WebSocketConnection]]
LifecycleObserver = Callable[[str], None]
EnvelopeObserver = Callable[[EventEnvelope], None]
FailureObserver = Callable[[str], None]
EnvelopeFactory = Callable[..., EventEnvelope | None]


def _market_prefix(market: Market | str) -> str:
    m = str(market).lower()
    if m == "spot":
        return "spot"
    if m in {"usdm", "um_perpetual", "um_delivery", "um_shared"}:
        return "usdm"
    if m in {"coinm", "cm_perpetual", "cm_delivery", "cm_shared"}:
        return "coinm"
    return m.split("_")[0]


def _market_label(market: Market | str) -> str:
    m = str(market).lower()
    if m == "spot":
        return "Spot"
    if m in {"usdm", "um_perpetual", "um_delivery", "um_shared"}:
        return "USD-M"
    if m in {"coinm", "cm_perpetual", "cm_delivery", "cm_shared"}:
        return "COIN-M"
    return m.upper()


class PublicWebSocketStream:
    """Market-agnostic Binance WebSocket stream collector."""

    def __init__(
        self,
        *,
        stream: Any,
        symbol: str,
        wire_name: str,
        spool: StreamSpool,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        market: Market | str | None = None,
        log_context: Mapping[str, object] | None = None,
        route: str = "",
        websocket_root: str = "",
        combined_streams: tuple[str, ...] = (),
        receipt_queue_capacity: int = 1024,
        receipt_byte_capacity: int = DEFAULT_RECEIPT_BYTE_CAPACITY,
        planned_rotation_seconds: float = 23 * 60 * 60 + 50 * 60,
        backoff: ReconnectBackoff | None = None,
        opener: ConnectionOpener,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
        envelope_factory: EnvelopeFactory | None = None,
        envelope_observer: EnvelopeObserver | None = None,
        failure_observer: FailureObserver | None = None,
        lifecycle_observer: LifecycleObserver | None = None,
        backpressure_put_timeout_seconds: float = 1.0,
        backpressure_saturation_timeout_seconds: float = 30.0,
        post_close_handoff_timeout_seconds: float = 5.0,
        market_label: str | None = None,
        market_prefix: str | None = None,
    ) -> None:
        if market is None:
            market = spool.market
        if receipt_queue_capacity < 1:
            raise ValueError("receipt queue capacity must be positive")
        if not 0 < planned_rotation_seconds < 24 * 60 * 60:
            raise ValueError("planned rotation must occur before the 24-hour limit")
        if post_close_handoff_timeout_seconds <= 0:
            raise ValueError("post-close handoff timeout must be positive")
        if not symbol or spool.symbol != symbol:
            label = market_label or _market_label(market)
            raise ValueError(f"{label} collector symbol must match its spool")
        self.market = cast(Market, market)
        self.market_prefix = market_prefix or _market_prefix(self.market)
        self.market_label = market_label or _market_label(self.market)
        self.combined_streams = combined_streams
        self.stream = stream
        self.symbol = symbol
        self.stream_name = stream.value if hasattr(stream, "value") else str(stream)
        self.route = route
        self.wire_name = wire_name
        self.spool = spool
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.logger = logger
        self.log_context = dict(log_context or product_log_fields(self.market, symbol))
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
            byte_capacity=receipt_byte_capacity,
            size_of=lambda frame: len(frame.raw_payload),
        )
        self.post_close_handoff_timeout_seconds = post_close_handoff_timeout_seconds
        self._backpressure_active = False
        self._generation = 0
        self._capture_startup_recorded = False
        self._recovery_flag_pending = False
        self._recovery_marker_enqueued = False
        self._pending_gap: dict[str, object] | None = None
        self._backpressure_boundary: ReceivedFrame | None = None
        self._backpressure_boundary_handoff_succeeded: bool | None = None
        self._post_close_handoff_outcome: str | None = None
        self._forced_seal_flags: frozenset[str] = frozenset()
        self._seal_intent: dict[str, object] | None = None
        self._boundary_connection_id: str | None = None
        self._boundary_detected_at_utc_ns: int | None = None
        self._connection_receipt_count = 0
        self._received_total = 0
        self._persisted_total = 0
        self._filtered_total = 0
        self._pending_since_utc_ns: int | None = None
        self._last_write_utc_ns: int | None = None
        self._last_writer_batch_size = 0
        self._last_writer_drain_ns = 0
        self._max_writer_drain_ns = 0
        self._active_connection_id: str | None = None
        self._server_shutdown = asyncio.Event()
        self._restore_open_gap()

    @property
    def base_url(self) -> str:
        return self.websocket_root

    @base_url.setter
    def base_url(self, value: str) -> None:
        self.websocket_root = value.rstrip("/")

    @property
    def url(self) -> str:
        prefix = self.websocket_root + ("/" + self.route if self.route else "")
        if self.combined_streams:
            names = "/".join(quote(name, safe="@!_,:") for name in self.combined_streams)
            return f"{prefix}/stream?streams={names}"
        if prefix.endswith("/ws"):
            return f"{prefix}/{self.wire_name}"
        if self.route:
            return f"{prefix}/ws/{self.wire_name}"
        return f"{prefix}/ws/{self.wire_name}"

    def request_rotation(self) -> None:
        self._server_shutdown.set()

    def set_capture_flags(self, flags: tuple[str, ...]) -> None:
        self._capture_flags = flags

    @property
    def receipt_queue_stats(self) -> AsyncQueueStats:
        return self._receipts.snapshot()

    def _restore_open_gap(self) -> None:
        open_gaps = self.spool.catalog.unclosed_stream_discontinuities(
            market=self.market,
            symbol=self.symbol,
            stream=self.stream_name,
        )
        if len(open_gaps) > 1:
            raise IngressGapStateConflict(
                f"{self.market_label} {self.stream_name} has {len(open_gaps)} conflicting "
                "unclosed stream discontinuities"
            )
        if not open_gaps:
            return
        evidence = open_gaps[0].get("evidence")
        if not isinstance(evidence, dict):
            raise IngressGapStateConflict(
                f"{self.market_label} {self.stream_name} has malformed open gap evidence"
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
                f"{self.market_label} {self.stream_name} has invalid open gap identity"
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
            f"{self.market_prefix}_ingress_gap_recovered",
            f"{self.market_label} stream recovered an unclosed discontinuity from Catalog",
            **self.log_context,
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
        if self._received_total == self._persisted_total:
            self._pending_since_utc_ns = receive_time_utc_ns
        self._received_total += 1
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
            capture_flags=tuple(dict.fromkeys((*receipt.capture_flags, "sequence_gap"))),
        )

    async def _accept(
        self,
        receipt: ReceivedFrame,
        writer_task: asyncio.Task[None],
        stop: asyncio.Event,
    ) -> None:
        if self._receipts.would_block(receipt) and not self._backpressure_active:
            self._backpressure_active = True
            self._log_ingress_state(
                logging.WARNING,
                f"{self.market_prefix}_ingress_backpressure_started",
                f"bounded {self.market_label} receipt queue started applying producer backpressure",
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
        progress = self.spool.persistence_progress()
        return {
            "raw_written_frames": progress.written_frames,
            "raw_synchronized_frames": progress.synchronized_frames,
            "spool_queued_bytes": self.spool.queue.queued_bytes,
            "spool_byte_capacity": self.spool.queue.byte_capacity,
            "receipt_queue_capacity": stats.capacity,
            "receipt_queue_depth": stats.depth,
            "receipt_queue_high_watermark": stats.high_watermark,
            "receipt_byte_capacity": stats.byte_capacity,
            "receipt_queued_bytes": stats.queued_bytes,
            "receipt_owned_bytes": stats.owned_bytes,
            "receipt_owned_bytes_high_watermark": stats.high_watermark_bytes,
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
            **self.log_context,
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
        boundary_frame_persisted: bool | None = None,
    ) -> None:
        if reason not in RECONNECT_REASONS:
            raise ValueError(f"unknown {self.market_label} reconnect reason: {reason}")
        if not gap_id:
            raise ValueError("gap_id must be non-empty")
        if self._pending_gap is not None:
            raise IngressGapStateConflict(
                f"{self.market_label} {self.stream_name} cannot start a second discontinuity "
                "while an earlier gap remains open"
            )
        if boundary_frame_persisted is None:
            boundary_frame_persisted = boundary is not None
        if boundary is not None:
            boundary_payload_sha256 = hashlib.sha256(boundary.raw_payload).hexdigest()
            boundary_kind = "last_frame_in_hand"
        else:
            boundary_frame_persisted = False
            boundary_payload_sha256 = None
            boundary_kind = "no_last_frame_available"
        evidence: dict[str, object] = {
            "gap_id": gap_id,
            "market": self.market,
            "symbol": self.symbol,
            "stream": self.stream_name,
            "reason": reason,
            "interval_classification": "UNRELIABLE",
            "gap_started_at_utc_ns": started_at_utc_ns,
            "original_connection_id": connection_id,
            "original_generation": self._generation,
            "boundary_kind": boundary_kind,
            "boundary_frame_persisted": boundary_frame_persisted,
            "boundary_precision": (
                (
                    "connection closed after the last Recorder-received frame; "
                    "the boundary frame was admitted to the bounded Raw-writer "
                    "handoff and unread WebSocket/TCP buffers are indeterminate"
                )
                if boundary_frame_persisted
                else (
                    "connection closed after the last Recorder-received frame; "
                    "the boundary frame could not enter the Raw-writer handoff "
                    "and unread WebSocket/TCP buffers are indeterminate"
                )
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
            event_id=stream_discontinuity_event_id(
                event_type="STREAM_DISCONTINUITY_STARTED",
                market=self.market,
                symbol=self.symbol,
                stream=self.stream_name,
                gap_id=gap_id,
            ),
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=started_at_utc_ns,
            evidence=evidence,
            symbol=self.symbol,
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
            return
        completed_at = envelope.receive_time_utc_ns
        gap_id = str(gap["gap_id"])
        await _run_owned_blocking_call(self.spool.sync)
        await _run_owned_blocking_call(
            self.spool.catalog.ensure_operational_event,
            event_id=stream_discontinuity_event_id(
                event_type="STREAM_DISCONTINUITY_COMPLETED",
                market=self.market,
                symbol=self.symbol,
                stream=self.stream_name,
                gap_id=gap_id,
            ),
            event_type="STREAM_DISCONTINUITY_COMPLETED",
            occurred_at_utc_ns=completed_at,
            evidence={
                **gap,
                "market": self.market,
                "symbol": self.symbol,
                "stream": self.stream_name,
                "reason": gap["reason"],
                "interval_classification": "UNRELIABLE",
                "gap_ended_at_utc_ns": completed_at,
                "new_connection_id": envelope.connection_id,
                "new_generation": self._generation,
                "raw_gap_marker": "sequence_gap",
                "historical_continuity_restored": False,
            },
            symbol=self.symbol,
        )
        self._pending_gap = None
        self._recovery_flag_pending = False
        self._recovery_marker_enqueued = False

    def _build_seal_intent(
        self, outcome: str, boundary: ReceivedFrame | None
    ) -> dict[str, object] | None:
        if self._boundary_connection_id is None or self._boundary_detected_at_utc_ns is None:
            return None
        boundary_persisted = (
            self._backpressure_boundary_handoff_succeeded if boundary is not None else False
        )
        started_at = (
            boundary.receive_time_utc_ns
            if boundary is not None
            else self._boundary_detected_at_utc_ns
        )
        if self._pending_gap is not None and not self._recovery_marker_enqueued:
            parent = self._pending_gap
            return {
                "required_forced_flags": sorted(self._forced_seal_flags),
                "intent_schema": RECONNECT_INTENT_SCHEMA_V2,
                "gap_id": str(parent["gap_id"]),
                "reason": str(parent["reason"]),
                "market": self.market,
                "symbol": self.symbol,
                "stream": self.stream_name,
                "original_connection_id": str(parent["original_connection_id"]),
                "original_generation": int(cast(int, parent["original_generation"])),
                "gap_started_at_utc_ns": int(cast(int, parent["gap_started_at_utc_ns"])),
                "boundary_kind": "no_last_frame_available",
                "boundary_frame_persisted": False,
                "extension": {
                    "attempt_connection_id": self._boundary_connection_id,
                    "attempt_generation": self._generation,
                    "attempt_reason": outcome,
                    "detected_at_utc_ns": self._boundary_detected_at_utc_ns,
                },
            }
        intent: dict[str, object] = {
            "required_forced_flags": sorted(self._forced_seal_flags),
            "intent_schema": RECONNECT_INTENT_SCHEMA_V2,
            "gap_id": str(uuid4()),
            "reason": outcome,
            "market": self.market,
            "symbol": self.symbol,
            "stream": self.stream_name,
            "original_connection_id": self._boundary_connection_id,
            "original_generation": self._generation,
            "gap_started_at_utc_ns": started_at,
            "boundary_kind": (
                "last_frame_in_hand" if boundary is not None else "no_last_frame_available"
            ),
            "boundary_frame_persisted": boundary_persisted,
        }
        if boundary is not None:
            intent["boundary_payload_sha256"] = hashlib.sha256(boundary.raw_payload).hexdigest()
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
                f"{self.market_prefix}_ingress_writer_failed",
                f"{self.market_label} Raw writer stopped before its ingress generation completed",
                **self.log_context,
                stream=self.stream_name,
                connection_id=self._active_connection_id or "unavailable",
                generation=self._generation,
                outcome="FATAL",
                **self._queue_fields(),
            )
            try:
                await _run_owned_blocking_call(self.spool.abort_writer)
            except asyncio.CancelledError as cancellation:
                raise writer_error from cancellation
            except BaseException as abort_error:
                raise abort_error from writer_error
            raise

    async def _persist_batch(self, batch: list[ReceivedFrame]) -> list[EventEnvelope]:
        persisted: list[EventEnvelope] = []
        for receipt in batch:
            envelope: EventEnvelope | None = None
            if self.envelope_factory is not None:
                envelope = self.envelope_factory(
                    raw_payload=receipt.raw_payload,
                    connection_id=receipt.connection_id,
                    collector_instance_id=self.collector_instance_id,
                    collector_version=self.collector_version,
                    receive_time_utc_ns=receipt.receive_time_utc_ns,
                    receive_monotonic_ns=receipt.receive_monotonic_ns,
                )
                if envelope is not None and receipt.capture_flags:
                    envelope = envelope.model_copy(
                        update={
                            "capture_flags": tuple(
                                dict.fromkeys((*envelope.capture_flags, *receipt.capture_flags))
                            )
                        }
                    )
            else:
                raise RuntimeError(f"missing {self.market_label} envelope factory")
            if envelope is not None:
                self.spool.enqueue(envelope)
                persisted.append(envelope)
                if (
                    "server_shutdown" in envelope.capture_flags
                    or envelope.source_sequence.get("sourceStream") == "server_shutdown"
                ):
                    self._server_shutdown.set()
            else:
                self._filtered_total += 1
        drain_started = time.perf_counter_ns()
        drained = await _run_owned_blocking_call(self.spool.drain_all)
        drain_duration = time.perf_counter_ns() - drain_started
        self._last_writer_batch_size = len(batch)
        self._last_writer_drain_ns = drain_duration
        self._max_writer_drain_ns = max(self._max_writer_drain_ns, drain_duration)
        if drained != len(persisted):
            raise RuntimeError(
                f"{self.market_label} Raw spool did not drain the complete writer batch"
            )
        self._persisted_total += len(persisted)
        self._last_write_utc_ns = self.utc_clock_ns()
        if self._received_total == self._persisted_total + self._filtered_total:
            self._pending_since_utc_ns = None
        return persisted

    def capture_progress(self) -> dict[str, int | None]:
        return {
            "received_frames": self._received_total,
            "persisted_frames": self._persisted_total,
            "filtered_frames": self._filtered_total,
            "pending_since_utc_ns": self._pending_since_utc_ns,
            "last_write_utc_ns": self._last_write_utc_ns,
        }

    async def _write_until_done(self, producer_done: asyncio.Event) -> None:
        while not producer_done.is_set() or not self._receipts.empty():
            batch: list[ReceivedFrame] = []
            try:
                batch.append(await asyncio.wait_for(self._receipts.get(), timeout=0.1))
            except TimeoutError:
                await _run_owned_blocking_call(self.spool.drain_all)
                continue
            batch_bytes = len(batch[0].raw_payload)
            for _ in range(min(255, self.spool.queue.capacity - 1)):
                if batch_bytes >= 1024 * 1024:
                    break
                try:
                    batch.append(self._receipts.get_nowait())
                    batch_bytes += len(batch[-1].raw_payload)
                except asyncio.QueueEmpty:
                    break
            persisted = await self._persist_batch(batch)
            for envelope in persisted:
                await self._record_gap_completed(envelope)
            if self.envelope_observer is not None:
                for envelope in persisted:
                    self.envelope_observer(envelope)
            connection_id = persisted[-1].connection_id if persisted else batch[-1].connection_id
            for receipt in batch:
                self._receipts.task_done(receipt)
            batch.clear()
            persisted.clear()
            if self._receipts.note_consumer_progress() and self._backpressure_active:
                self._backpressure_active = False
                self._log_ingress_state(
                    logging.INFO,
                    f"{self.market_prefix}_ingress_backpressure_recovered",
                    f"{self.market_label} receipt queue recovered below its low watermark",
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
        async def receive_frames(control: ConnectionReceiveControl) -> str:
            while True:
                if control.retirement is not None:
                    return control.retirement
                try:
                    receipt = await control.receive(
                        lambda: self._receive_once(websocket, connection_id)
                    )
                    await self._accept(receipt, writer_task, stop)
                    if stop.is_set():
                        await websocket.close(code=1000, reason="collector shutdown")
                        if self._is_session_restart(session_restart):
                            return "session_restart"
                        return "graceful_shutdown"
                except IngressBackpressureTimeout:
                    self._log_ingress_state(
                        logging.ERROR,
                        f"{self.market_prefix}_ingress_backpressure_timeout",
                        f"{self.market_label} receipt queue exceeded its bounded saturation budget",
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
                        self._backpressure_boundary = boundary
                        self._backpressure_boundary_handoff_succeeded = False
                        self._post_close_handoff_outcome = "ingress_backpressure"
                        self._remember_boundary(connection_id)
                        self._log_ingress_state(
                            logging.CRITICAL,
                            f"{self.market_prefix}_ingress_post_close_handoff_timeout",
                            (
                                f"received {self.market_label} boundary frame couldn't "
                                "enter its Raw writer queue"
                            ),
                            connection_id=connection_id,
                            outcome="FATAL",
                        )
                        raise
                    if self._recovery_flag_pending:
                        self._recovery_marker_enqueued = True
                    self._backpressure_boundary = boundary
                    self._backpressure_boundary_handoff_succeeded = True
                    return "ingress_backpressure"
                except IngressStopRequested:
                    post_close_outcome = (
                        "session_restart"
                        if self._is_session_restart(session_restart)
                        else "stopped"
                    )
                    await websocket.close(code=1000, reason="collector shutdown")
                    receipt = self._with_recovery_marker(receipt)
                    try:
                        await self._receipts.put_after_connection_close(
                            receipt,
                            writer_task=writer_task,
                            timeout_seconds=self.post_close_handoff_timeout_seconds,
                        )
                    except IngressPostCloseHandoffTimeout:
                        self._backpressure_boundary = receipt
                        self._backpressure_boundary_handoff_succeeded = False
                        self._post_close_handoff_outcome = post_close_outcome
                        self._remember_boundary(connection_id)
                        raise
                    if self._recovery_flag_pending:
                        self._recovery_marker_enqueued = True
                    if post_close_outcome == "session_restart":
                        return "session_restart"
                    return "graceful_shutdown"

                await asyncio.sleep(0)

        try:
            outcome = await run_connection_receiver(
                receive_frames,
                stop=stop,
                writer_task=writer_task,
                rotation_seconds=self.planned_rotation_seconds,
                shutdown=self._server_shutdown,
            )
        except (WebSocketException, OSError, TimeoutError):
            if self._is_session_restart(session_restart):
                await websocket.close(code=1000, reason="collector shutdown")
                return "session_restart"
            if stop.is_set():
                await websocket.close(code=1000, reason="collector shutdown")
                return "graceful_shutdown"
            raise

        if outcome == "stopped":
            await websocket.close(code=1000, reason="collector shutdown")
            return (
                "session_restart"
                if self._is_session_restart(session_restart)
                else "graceful_shutdown"
            )
        if outcome == "planned_rotation":
            await websocket.close(code=1000, reason="planned 24-hour rotation")
        elif outcome == "server_shutdown":
            self._server_shutdown.clear()
            await websocket.close(code=1000, reason="serverShutdown received")
        return outcome

    @staticmethod
    def _is_session_restart(session_restart: asyncio.Event | None) -> bool:
        return session_restart is not None and session_restart.is_set()

    def _remember_boundary(self, connection_id: str) -> None:
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
                self._server_shutdown.clear()
                async with self.opener(self.url) as websocket:
                    connected_at = asyncio.get_running_loop().time()
                    was_connected = True
                    self._active_connection_id = connection_id
                    if self.lifecycle_observer is not None:
                        self.lifecycle_observer("connected")
                    log_event(
                        self.logger,
                        logging.INFO,
                        f"{self.market_prefix}_websocket_connected",
                        f"Binance {self.market_label} raw stream connected",
                        **self.log_context,
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
                    f"{self.market_prefix}_websocket_disconnected",
                    f"Binance {self.market_label} stream disconnected unexpectedly",
                    **self.log_context,
                    stream=self.stream_name,
                    connection_id=connection_id,
                    error_type=type(exc).__name__,
                    retry=failures,
                    **websocket_disconnect_log_fields(
                        exc,
                        connected_at=connected_at,
                        observed_at=asyncio.get_running_loop().time(),
                    ),
                )
                if not was_connected or self._connection_receipt_count == 0:
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
                log_event(
                    self.logger,
                    logging.INFO,
                    f"{self.market_prefix}_{reason}",
                    f"Binance {self.market_label} connection will be replaced",
                    **self.log_context,
                    stream=self.stream_name,
                    connection_id=connection_id,
                )
                self._remember_boundary(connection_id)
                return reason
            if stop.is_set() or reason == "graceful_shutdown":
                return "stopped"
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.backoff.delay(max(1, failures)))
        return "stopped"

    def _record_capture_startup(self) -> None:
        self._capture_startup_recorded = True

    async def run(
        self,
        stop: asyncio.Event,
        session_restart: asyncio.Event | None = None,
    ) -> None:
        if not stop.is_set() and not self._capture_startup_recorded:
            await _run_owned_blocking_call(self._record_capture_startup)

        while not stop.is_set():
            producer_done = asyncio.Event()
            self._forced_seal_flags = frozenset()
            self._seal_intent = None
            self._boundary_connection_id = None
            self._boundary_detected_at_utc_ns = None
            self._backpressure_boundary = None
            self._backpressure_boundary_handoff_succeeded = None
            self._post_close_handoff_outcome = None
            self._active_connection_id = None
            writer_task = asyncio.create_task(
                self._writer_loop(producer_done),
                name=f"raw-writer:{self.market}:{self.symbol}:{self.stream_name}",
            )
            outcome = "stopped"
            gap_just_started = False
            try:
                outcome = await self._connection_loop(
                    stop, writer_task, session_restart=session_restart
                )
            except IngressPostCloseHandoffTimeout as handoff_error:
                handoff_outcome = self._post_close_handoff_outcome
                if handoff_outcome is None or (
                    handoff_outcome != "stopped" and handoff_outcome not in RECONNECT_REASONS
                ):
                    raise RuntimeError(
                        f"{self.market_label} post-close handoff timeout lost its boundary context"
                    ) from handoff_error
                outcome = handoff_outcome
                raise
            finally:
                if outcome == "ingress_backpressure":
                    self._forced_seal_flags = (
                        frozenset()
                        if self._backpressure_boundary_handoff_succeeded
                        else frozenset({RECONNECT_GAP_FLAG})
                    )
                elif outcome in RECONNECT_REASONS:
                    self._forced_seal_flags = frozenset({RECONNECT_GAP_FLAG})
                if outcome in RECONNECT_REASONS:
                    boundary = (
                        self._backpressure_boundary
                        if self._backpressure_boundary_handoff_succeeded is not None
                        else None
                    )
                    self._seal_intent = self._build_seal_intent(outcome, boundary)
                if outcome in RECONNECT_REASONS and self._pending_gap is None:
                    try:
                        if (
                            self._seal_intent is None
                            or self._boundary_connection_id is None
                            or self._boundary_detected_at_utc_ns is None
                        ):
                            raise RuntimeError(
                                f"missing {self.market_label} boundary identity for {outcome}"
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
                            boundary_frame_persisted=(
                                self._backpressure_boundary_handoff_succeeded
                                if boundary is not None
                                else None
                            ),
                        )
                        gap_just_started = True
                        self._generation += 1
                        self._recovery_flag_pending = True
                        self._recovery_marker_enqueued = False
                    except BaseException as intent_error:
                        producer_done.set()
                        await release_writer_preserving_errors(writer_task, intent_error)
                producer_done.set()
                await writer_task
                if (
                    outcome in RECONNECT_REASONS
                    and self._pending_gap is None
                    and not gap_just_started
                ):
                    if (
                        self._seal_intent is None
                        or self._boundary_connection_id is None
                        or self._boundary_detected_at_utc_ns is None
                    ):
                        raise RuntimeError(
                            f"missing {self.market_label} boundary identity for {outcome}"
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
                        boundary_frame_persisted=(
                            self._backpressure_boundary_handoff_succeeded
                            if boundary is not None
                            else None
                        ),
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
                        f"missing {self.market_label} backpressure generation boundary"
                    )
            elif outcome in RECONNECT_REASONS:
                boundary = None
            else:
                raise RuntimeError(f"unknown {self.market_label} connection outcome: {outcome}")
            is_diff_depth = (
                self.stream_name == "diff_depth"
                or getattr(self.stream, "value", None) == "diff_depth"
            )
            if self._pending_gap is not None and not gap_just_started:
                log_event(
                    self.logger,
                    logging.WARNING,
                    f"{self.market_prefix}_ingress_gap_extended",
                    f"{self.market_label} reconnect boundary extends the pending discontinuity",
                    **self.log_context,
                    stream=self.stream_name,
                    connection_id=self._boundary_connection_id or "unknown",
                    generation=self._generation,
                    gap_id=self._pending_gap["gap_id"],
                    reason=outcome,
                    outcome="GAP_EXTENDED",
                    **self._queue_fields(),
                )
                if is_diff_depth:
                    return
                continue
            self._backpressure_active = False
            self._receipts.note_consumer_progress()
            self._log_ingress_state(
                logging.WARNING,
                f"{self.market_prefix}_ingress_stream_recovery",
                (
                    f"{self.market_label} stream is opening a new generation "
                    "with persistent gap evidence"
                ),
                connection_id=self._boundary_connection_id or "unknown",
                outcome=("DEPTH_RESYNC_REQUIRED" if is_diff_depth else "STREAM_RECONNECT"),
            )
            if is_diff_depth:
                return


class CombinedWebSocketStream(PublicWebSocketStream):
    """Multiplexed combined WebSocket stream collector."""

    pass


_PublicWebSocketStream = PublicWebSocketStream
_CombinedWebSocketStream = CombinedWebSocketStream


@dataclass(frozen=True, slots=True)
class StreamGroup:
    root: str
    route: str
    name: str
    sources: tuple[Source, ...]
    wire_names: tuple[str, ...] = field(init=False)
    by_wire: dict[str, Source] = field(init=False, compare=False, repr=False)

    def __post_init__(self) -> None:
        names = tuple(s.endpoint.rsplit("/", 1)[1] for s in self.sources)
        object.__setattr__(self, "wire_names", names)
        object.__setattr__(self, "by_wire", dict(zip(names, self.sources, strict=True)))


def stream_groups(sources: tuple[Source, ...], limit: int) -> tuple[StreamGroup, ...]:
    if not 1 <= limit <= 1024:
        raise ValueError("combined-stream limit must be between 1 and 1024")
    groups: dict[str, list[Source]] = {}
    for source in sources:
        if source.transport == "ws":
            groups.setdefault(source.endpoint.rsplit("/", 1)[0], []).append(source)
    result = []
    for endpoint, members in sorted(groups.items()):
        members.sort(key=lambda s: s.endpoint)
        route = ""
        root = endpoint
        if endpoint.endswith(("/market", "/public")):
            root, route = endpoint.rsplit("/", 1)
        for offset in range(0, len(members), limit):
            selected = tuple(members[offset : offset + limit])
            identity = sha256("\0".join(s.endpoint for s in selected).encode()).hexdigest()[:16]
            result.append(StreamGroup(root, route, f"ws_{route or 'market'}_{identity}", selected))
    return tuple(result)


def wire_document(envelope: EventEnvelope) -> Any:
    decoded = json.loads(envelope.raw_payload)
    if envelope.payload_encoding == "utf-8-json-combined.v1":
        return decoded["data"]
    return decoded


def logical_envelope(envelope: EventEnvelope) -> EventEnvelope:
    """Expose logical stream to existing sequence/read consumers; retain exact bytes."""
    stream = envelope.source_sequence.get("sourceStream")
    if not isinstance(stream, str):
        return envelope
    return envelope.model_copy(update={"stream": stream})


def envelope_for_group(
    group: StreamGroup,
    product: Product | None,
    *,
    raw_payload: bytes,
    connection_id: str,
    collector_instance_id: str,
    collector_version: str,
    receive_time_utc_ns: int,
    receive_monotonic_ns: int,
) -> EventEnvelope:
    source = group.sources[0]
    sequence: dict[str, int | str] = {}
    flags: tuple[str, ...] = ()
    event_time = transaction_time = trade_time = None
    try:
        combined = json.loads(raw_payload)
        wire = combined["stream"]
        if wire == "!serverShutdown":
            sequence["sourceStream"] = "server_shutdown"
        else:
            source = group.by_wire[wire]
            sequence.update(sourceStream=source.stream, sourceFamily=source.family, wireStream=wire)
        data = combined["data"]
        if "kline" in source.stream and not isinstance(data, dict):
            raise ValueError("invalid kline data")
        if isinstance(data, dict):
            if wire != "!serverShutdown" and any(k not in data for k in source.required_fields):
                raise ValueError("required WS field is missing")
            if source.stream == "index_price" and data.get("i") != source.symbol:
                raise ValueError("index pair does not match subscription")
            if product is not None:
                if "s" in data and data["s"] != product.key.symbol:
                    raise ValueError("payload symbol does not match directory")
                if "st" in data and data["st"] != product.symbol_type:
                    raise ValueError("symbol type does not match directory")
                if source.stream in {"diff_depth", "rpi_depth", "book_ticker", "agg_trade"}:
                    if data.get("s") != product.key.symbol:
                        raise ValueError("stream symbol does not match directory")
                    if product.venue_market != "spot" and data.get("st") != product.symbol_type:
                        raise ValueError("derivative stream lacks symbol type")
                    if "ps" in data and data["ps"] != product.pair:
                        raise ValueError("stream pair does not match directory")
                if (
                    source.stream == "liquidation"
                    and data.get("o", {}).get("s") != product.key.symbol
                ):
                    raise ValueError("liquidation symbol does not match directory")
            for key in ("E", "T"):
                value = data.get(key)
                if value is not None and (
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or not 0 <= value <= 2**63 - 1
                ):
                    raise ValueError("invalid exchange timestamp")
            event_time = data.get("E")
            if source.stream in {"agg_trade", "trade", "block_trade"}:
                trade_time = data.get("T")
            elif source.stream in {"diff_depth", "rpi_depth", "book_ticker"}:
                transaction_time = data.get("T")
            for key in ("U", "u", "pu", "a", "f", "l", "t"):
                value = data.get(key)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    sequence[key] = value
            if "kline" in source.stream or (
                isinstance(data.get("e"), str) and "kline" in data["e"].lower()
            ):
                k = data.get("k")
                if not isinstance(k, dict):
                    raise ValueError("missing or invalid kline object")
                if any(field not in k for field in ("i", "o", "c", "h", "l", "v")):
                    raise ValueError("missing required kline fields")
                expected_interval = source.stream.removesuffix("_utc8").rsplit("_", 1)[-1]
                if k["i"] != expected_interval:
                    raise ValueError("kline interval does not match subscription")
                x = k.get("x")
                t = k.get("t")
                T = k.get("T")
                if not isinstance(x, bool):
                    raise ValueError("kline x must be boolean")
                if not isinstance(t, int) or isinstance(t, bool) or t < 0:
                    raise ValueError("invalid kline openTime")
                if not isinstance(T, int) or isinstance(T, bool) or T < 0:
                    raise ValueError("invalid kline closeTime")
                if t > T:
                    raise ValueError("kline openTime exceeds closeTime")
                if (
                    product is not None
                    and source.stream.startswith("kline_")
                    and k.get("s") != product.key.symbol
                ):
                    raise ValueError("kline symbol does not match directory")
                sequence["klineClosed"] = 1 if x else 0
                sequence["openTime"] = t
                sequence["closeTime"] = T
                if "i" in k and isinstance(k["i"], str):
                    sequence["interval"] = k["i"]
            if source.stream in {"diff_depth", "rpi_depth"}:
                required = ("U", "u") if source.venue_market == "spot" else ("U", "u", "pu")
                if any(key not in sequence for key in required) or int(sequence["U"]) > int(
                    sequence["u"]
                ):
                    raise ValueError("invalid depth sequence")
                if not isinstance(data.get("b"), list) or not isinstance(data.get("a"), list):
                    raise ValueError("invalid depth levels")
        elif not isinstance(data, list):
            raise ValueError("invalid combined data")
    except (ValueError, KeyError, TypeError, AttributeError, RecursionError):
        flags = ("malformed",)
        event_time = transaction_time = trade_time = None
    return EventEnvelope(
        market=source.market,
        symbol=source.symbol if product is None else product.key.symbol,
        stream=group.name,
        module="binance.public.websocket.v1",
        connection_id=connection_id,
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_time_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        exchange_event_time=event_time,
        exchange_transaction_time=transaction_time,
        exchange_trade_time=trade_time,
        source_sequence=sequence,
        raw_payload=raw_payload,
        payload_encoding="utf-8-json-combined.v1",
        capture_flags=flags,
    )


class DepthSequenceObserver:
    """Mark known native discontinuities before append; two IDs, no full book."""

    def __init__(self) -> None:
        self._last: dict[str, tuple[str, int]] = {}

    def observe(self, envelope: EventEnvelope) -> EventEnvelope:
        stream = envelope.source_sequence.get("sourceStream")
        if stream not in {"diff_depth", "rpi_depth"}:
            return envelope
        assert isinstance(stream, str)
        if "malformed" in envelope.capture_flags:
            self._last.pop(stream, None)
            return envelope
        first, final, previous = (envelope.source_sequence.get(k) for k in ("U", "u", "pu"))
        if not isinstance(first, int) or not isinstance(final, int):
            return envelope
        old = self._last.get(stream)
        if old is None or old[0] != envelope.connection_id:
            self._last[stream] = envelope.connection_id, final
            return envelope
        if final <= old[1]:
            return envelope
        self._last[stream] = envelope.connection_id, final
        gap = first > old[1] + 1 if envelope.market == "spot" else previous != old[1]
        if gap:
            return envelope.model_copy(
                update={
                    "capture_flags": tuple(dict.fromkeys((*envelope.capture_flags, "sequence_gap")))
                }
            )
        return envelope
