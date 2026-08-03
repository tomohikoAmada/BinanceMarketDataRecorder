"""Bounded ingress queue with explicit overload semantics."""

from __future__ import annotations

from queue import Empty, Full, Queue

from ..domain.event import EventEnvelope


class IngressQueueFull(RuntimeError):
    """Raised instead of silently dropping a market event."""


class IngressBackpressureTimeout(IngressQueueFull):
    """A bounded receipt handoff stayed saturated beyond its continuity budget."""


class IngressPersistenceTimeout(IngressQueueFull):
    """A received frame couldn't reach the writer after its connection was closed."""


class IngressWriterStopped(RuntimeError):
    """The receipt producer observed that its Raw writer had already terminated."""


class IngressStopRequested(RuntimeError):
    """Shutdown interrupted a receipt producer that was waiting for queue space."""


class BoundedEventQueue:
    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("queue capacity must be positive")
        self._queue: Queue[EventEnvelope] = Queue(maxsize=capacity)
        self.capacity = capacity

    def put_nowait(self, envelope: EventEnvelope) -> None:
        try:
            self._queue.put_nowait(envelope)
        except Full as exc:
            raise IngressQueueFull(
                f"ingress queue capacity {self.capacity} exhausted"
            ) from exc

    def get_nowait(self) -> EventEnvelope | None:
        try:
            return self._queue.get_nowait()
        except Empty:
            return None

    @property
    def depth(self) -> int:
        return self._queue.qsize()
