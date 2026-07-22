"""Bounded ingress queue with explicit overload semantics."""

from __future__ import annotations

from queue import Empty, Full, Queue

from ..domain.event import EventEnvelope


class IngressQueueFull(RuntimeError):
    """Raised instead of silently dropping a market event."""


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
