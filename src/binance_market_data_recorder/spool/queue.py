"""Bounded ingress queue with explicit overload semantics."""

from __future__ import annotations

from queue import Empty, Full, Queue
from threading import Lock

from ..domain.event import EventEnvelope


class IngressQueueFull(RuntimeError):
    """Raised instead of silently dropping a market event."""


class IngressBackpressureTimeout(IngressQueueFull):
    """A bounded receipt handoff stayed saturated beyond its continuity budget."""


class IngressPostCloseHandoffTimeout(IngressQueueFull):
    """A closed connection's last received frame couldn't enter the writer queue."""


class IngressGapStateConflict(RuntimeError):
    """Catalog contains ambiguous open stream-discontinuity evidence."""


class IngressWriterStopped(RuntimeError):
    """The receipt producer observed that its Raw writer had already terminated."""


class IngressStopRequested(RuntimeError):
    """Shutdown interrupted a receipt producer that was waiting for queue space."""


class BoundedEventQueue:
    def __init__(self, capacity: int, *, byte_capacity: int = 64 * 1024 * 1024) -> None:
        if capacity < 1:
            raise ValueError("queue capacity must be positive")
        if byte_capacity < 1:
            raise ValueError("queue byte capacity must be positive")
        self._queue: Queue[tuple[EventEnvelope, int]] = Queue(maxsize=capacity)
        self.capacity = capacity
        self.byte_capacity = byte_capacity
        self._queued_bytes = 0
        self._lock = Lock()

    def put_nowait(self, envelope: EventEnvelope) -> None:
        # Account payload plus a conservative metadata allowance without another
        # CBOR serialization. EventEnvelope bounds each text field to 256 chars.
        size = len(envelope.raw_payload) + 4096 + sum(
            4 * len(value) for value in (
                envelope.schema_version, envelope.venue, envelope.market, envelope.symbol,
                envelope.stream, envelope.module, envelope.connection_id,
                envelope.collector_instance_id, envelope.collector_version,
                envelope.payload_encoding,
            )
        ) + sum(
            4 * len(key) + 4 * len(str(value))
            for key, value in envelope.source_sequence.items()
        ) + sum(4 * len(flag) for flag in envelope.capture_flags)
        with self._lock:
            if self._queued_bytes + size > self.byte_capacity:
                raise IngressQueueFull("ingress queue byte capacity exhausted")
            try:
                self._queue.put_nowait((envelope, size))
            except Full as exc:
                raise IngressQueueFull(
                    f"ingress queue capacity {self.capacity} exhausted"
                ) from exc
            self._queued_bytes += size

    def get_nowait(self) -> EventEnvelope | None:
        with self._lock:
            try:
                envelope, size = self._queue.get_nowait()
            except Empty:
                return None
            self._queued_bytes -= size
            return envelope

    @property
    def queued_bytes(self) -> int:
        with self._lock:
            return self._queued_bytes

    @property
    def depth(self) -> int:
        return self._queue.qsize()
