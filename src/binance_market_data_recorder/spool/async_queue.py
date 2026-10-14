"""Bounded asyncio handoff with writer-aware, time-bounded backpressure."""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from .queue import (
    IngressBackpressureTimeout,
    IngressPostCloseHandoffTimeout,
    IngressStopRequested,
    IngressWriterStopped,
)

T = TypeVar("T")

WAIT_BUCKETS_NS = (
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    5_000_000_000,
)


@dataclass(frozen=True, slots=True)
class AsyncQueueStats:
    capacity: int
    depth: int
    high_watermark: int
    wait_count: int
    wait_total_ns: int
    wait_max_ns: int
    wait_p50_ns: int | str
    wait_p95_ns: int | str
    wait_p99_ns: int | str
    saturation_seconds: float | None
    byte_capacity: int | None = None
    queued_bytes: int = 0
    owned_bytes: int = 0
    high_watermark_bytes: int = 0


@dataclass(frozen=True, slots=True)
class PutResult:
    waited_ns: int
    saturation_started: bool


class BoundedAsyncQueue[T]:
    """Finite queue whose producer wait is bounded and observes writer failure."""

    def __init__(
        self,
        capacity: int,
        *,
        put_timeout_seconds: float,
        saturation_timeout_seconds: float,
        byte_capacity: int | None = None,
        size_of: Callable[[T], int] | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("queue capacity must be positive")
        if put_timeout_seconds <= 0:
            raise ValueError("put timeout must be positive")
        if saturation_timeout_seconds < put_timeout_seconds:
            raise ValueError("saturation timeout must be at least the put timeout")
        if (byte_capacity is None) != (size_of is None):
            raise ValueError("byte capacity and item size function must be supplied together")
        if byte_capacity is not None and byte_capacity < 1:
            raise ValueError("byte capacity must be positive")
        self.capacity = capacity
        self.byte_capacity = byte_capacity
        self._size_of = size_of
        self.low_watermark = max(0, capacity // 2)
        self.put_timeout_seconds = put_timeout_seconds
        self.saturation_timeout_seconds = saturation_timeout_seconds
        self._queue: asyncio.Queue[tuple[T, int]] = asyncio.Queue(maxsize=capacity)
        self._queued_bytes = 0
        self._owned_bytes = 0
        self._high_watermark_bytes = 0
        self._inflight: dict[int, deque[int]] = {}
        self._space_changed = asyncio.Event()
        self._high_watermark = 0
        self._wait_count = 0
        self._wait_total_ns = 0
        self._wait_max_ns = 0
        self._wait_histogram = {bound: 0 for bound in WAIT_BUCKETS_NS}
        self._wait_overflow = 0
        self._saturation_started_ns: int | None = None

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()

    async def get(self) -> T:
        return self._take(await self._queue.get())

    def get_nowait(self) -> T:
        return self._take(self._queue.get_nowait())

    def _take(self, entry: tuple[T, int]) -> T:
        item, size = entry
        self._queued_bytes -= size
        if self.byte_capacity is not None:
            self._inflight.setdefault(id(item), deque()).append(size)
        self._space_changed.set()
        return item

    def task_done(self, item: T | None = None) -> None:
        """Release a weighted item only after its consumer has finished using it."""

        sizes = None if item is None else self._inflight.get(id(item))
        if self.byte_capacity is not None and not sizes:
            raise ValueError("weighted task_done requires an in-flight item")
        self._queue.task_done()
        if sizes is not None:
            self._owned_bytes -= sizes.popleft()
            if not sizes:
                del self._inflight[id(item)]
            self._space_changed.set()

    def _size(self, item: T) -> int:
        size = 0 if self._size_of is None else self._size_of(item)
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise ValueError("item size must be a non-negative integer")
        return size

    def would_block(self, item: T) -> bool:
        return self._full(self._size(item))

    def _full(self, size: int) -> bool:
        return self._queue.full() or (
            self.byte_capacity is not None and self._owned_bytes + size > self.byte_capacity
        )

    def _insert(self, item: T, size: int) -> None:
        self._queue.put_nowait((item, size))
        self._queued_bytes += size
        self._owned_bytes += size
        self._high_watermark_bytes = max(self._high_watermark_bytes, self._owned_bytes)

    async def _put_when_space(self, item: T, size: int) -> None:
        while True:
            self._space_changed.clear()
            if not self._full(size):
                self._insert(item, size)
                return
            await self._space_changed.wait()

    async def put(
        self,
        item: T,
        *,
        writer_task: asyncio.Task[None],
        stop: asyncio.Event | None = None,
    ) -> PutResult:
        """Handoff with both per-put and sustained-saturation deadlines."""

        if writer_task.done():
            self._raise_writer(writer_task)
        size = self._size(item)
        if self.byte_capacity is not None and size > self.byte_capacity:
            raise IngressBackpressureTimeout("frame exceeds the receipt byte capacity")
        if not self._full(size):
            self._insert(item, size)
            self._observe_depth()
            return PutResult(waited_ns=0, saturation_started=False)

        started_ns = time.monotonic_ns()
        saturation_started = self._saturation_started_ns is None
        if self._saturation_started_ns is None:
            self._saturation_started_ns = started_ns
        while True:
            saturation_remaining = self.saturation_timeout_seconds - (
                time.monotonic_ns() - self._saturation_started_ns
            ) / 1_000_000_000
            if saturation_remaining <= 0:
                self._observe_wait(time.monotonic_ns() - started_ns)
                raise IngressBackpressureTimeout(
                    "receipt queue remained saturated beyond its bounded continuity budget"
                )
            inserted = await self._put_waiting(
                item,
                size=size,
                writer_task=writer_task,
                timeout_seconds=min(self.put_timeout_seconds, saturation_remaining),
                timeout_type=None,
                stop=stop,
            )
            if inserted:
                break
        waited_ns = time.monotonic_ns() - started_ns
        self._observe_wait(waited_ns)
        self._observe_depth()
        return PutResult(waited_ns=waited_ns, saturation_started=saturation_started)

    async def put_after_connection_close(
        self,
        item: T,
        *,
        writer_task: asyncio.Task[None],
        timeout_seconds: float,
    ) -> int:
        """Hand off the already-received boundary frame after stopping its producer."""

        if timeout_seconds <= 0:
            raise ValueError("post-close put timeout must be positive")
        size = self._size(item)
        if self.byte_capacity is not None and size > self.byte_capacity:
            raise IngressPostCloseHandoffTimeout("boundary frame exceeds receipt byte capacity")
        started_ns = time.monotonic_ns()
        inserted = await self._put_waiting(
            item,
            size=size,
            writer_task=writer_task,
            timeout_seconds=timeout_seconds,
            timeout_type=IngressPostCloseHandoffTimeout,
        )
        if not inserted:  # pragma: no cover - timeout_type guarantees an exception
            raise AssertionError("post-close handoff timed out without an exception")
        waited_ns = time.monotonic_ns() - started_ns
        self._observe_wait(waited_ns)
        self._observe_depth()
        return waited_ns

    async def _put_waiting(
        self,
        item: T,
        *,
        size: int,
        writer_task: asyncio.Task[None],
        timeout_seconds: float,
        timeout_type: type[IngressPostCloseHandoffTimeout] | None,
        stop: asyncio.Event | None = None,
    ) -> bool:
        put_task = asyncio.create_task(self._put_when_space(item, size))
        stop_task = asyncio.create_task(stop.wait()) if stop is not None else None
        try:
            watched: set[asyncio.Task[object]] = {put_task, writer_task}
            if stop_task is not None:
                watched.add(stop_task)
            done, _pending = await asyncio.wait(
                watched,
                timeout=timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if writer_task in done:
                put_task.cancel()
                await asyncio.gather(put_task, return_exceptions=True)
                self._raise_writer(writer_task)
            if put_task in done:
                await put_task
                return True
            if stop_task is not None and stop_task in done:
                put_task.cancel()
                await asyncio.gather(put_task, return_exceptions=True)
                raise IngressStopRequested(
                    "shutdown interrupted a receipt waiting for writer queue space"
                )
            put_task.cancel()
            await asyncio.gather(put_task, return_exceptions=True)
            if timeout_type is not None:
                raise timeout_type(
                    "received frame couldn't enter the bounded writer handoff before timeout"
                )
            return False
        except BaseException:
            if not put_task.done():
                put_task.cancel()
            await asyncio.gather(put_task, return_exceptions=True)
            raise
        finally:
            if stop_task is not None:
                if not stop_task.done():
                    stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)

    def note_consumer_progress(self) -> bool:
        """Return true once when saturation falls below the low watermark."""

        if (
            self._saturation_started_ns is None
            or self.depth > self.low_watermark
            or (
                self.byte_capacity is not None
                and self._owned_bytes > self.byte_capacity // 2
            )
        ):
            return False
        self._saturation_started_ns = None
        return True

    def snapshot(self) -> AsyncQueueStats:
        now_ns = time.monotonic_ns()
        return AsyncQueueStats(
            capacity=self.capacity,
            depth=self.depth,
            high_watermark=self._high_watermark,
            wait_count=self._wait_count,
            wait_total_ns=self._wait_total_ns,
            wait_max_ns=self._wait_max_ns,
            wait_p50_ns=self._percentile(0.50),
            wait_p95_ns=self._percentile(0.95),
            wait_p99_ns=self._percentile(0.99),
            saturation_seconds=(
                None
                if self._saturation_started_ns is None
                else (now_ns - self._saturation_started_ns) / 1_000_000_000
            ),
            byte_capacity=self.byte_capacity,
            queued_bytes=self._queued_bytes,
            owned_bytes=self._owned_bytes,
            high_watermark_bytes=self._high_watermark_bytes,
        )

    def _observe_depth(self) -> None:
        self._high_watermark = max(self._high_watermark, self.depth)

    def _observe_wait(self, duration_ns: int) -> None:
        self._wait_count += 1
        self._wait_total_ns += duration_ns
        self._wait_max_ns = max(self._wait_max_ns, duration_ns)
        for bound in WAIT_BUCKETS_NS:
            if duration_ns <= bound:
                self._wait_histogram[bound] += 1
                return
        self._wait_overflow += 1

    def _percentile(self, percentile: float) -> int | str:
        if self._wait_count == 0:
            return "INSUFFICIENT_DATA"
        rank = max(1, math.ceil(self._wait_count * percentile))
        cumulative = 0
        for bound in WAIT_BUCKETS_NS:
            cumulative += self._wait_histogram[bound]
            if cumulative >= rank:
                return bound
        return "overflow"

    @staticmethod
    def _raise_writer(writer_task: asyncio.Task[None]) -> None:
        if writer_task.cancelled():
            raise IngressWriterStopped("Raw writer was cancelled")
        error = writer_task.exception()
        if error is not None:
            raise IngressWriterStopped("Raw writer stopped with an exception") from error
        raise IngressWriterStopped("Raw writer returned unexpectedly")
