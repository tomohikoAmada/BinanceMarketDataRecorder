"""流 spool 编排: 有界入队, 旋转和密封。

StreamSpool 将 RawChunkWriter、BoundedEventQueue 和 seal_partial 组装为一个生命周期:

1. 事件从 asyncio 回调通过 enqueue() 入队, 若队列满则抛出 IngressQueueFull。
2. drain 循环从队列取出事件, append 到活跃 chunk, 按 durability_interval_seconds
   fsync, 并在 should_rotate 触发时调用 seal_partial 密封当前 chunk 并创建新 chunk。
3. 空闲时 sync_if_due() 确保低流量流也能在间隔内持久化。
4. 关闭流程: 停止 drain, 密封活跃 chunk, 等待密封完成。
5. 不直接管理 WebSocket 连接或 Catalog 事务; 这些由 Collector 和 Catalog 分别处理。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from threading import Lock

from ..domain.event import EventEnvelope
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from .queue import BoundedEventQueue
from .seal import seal_partial
from .writer import RawChunkWriter, RotationPolicy


@dataclass(frozen=True, slots=True)
class SpoolOperationStats:
    append_last_ns: int
    append_max_ns: int
    write_last_ns: int
    write_max_ns: int
    fsync_last_ns: int
    fsync_max_ns: int
    seal_last_ns: int
    seal_max_ns: int


class StreamSpool:
    """One homogeneous stream's queue and current chunk; no network ownership."""

    def __init__(
        self,
        *,
        layout: StorageLayout,
        catalog: Catalog,
        market: str,
        symbol: str,
        stream: str,
        collector_instance_id: str,
        collector_version: str,
        queue_capacity: int,
        rotation: RotationPolicy,
        durability_interval_seconds: float,
        max_frame_bytes: int,
        writer_factory: Callable[..., RawChunkWriter] = RawChunkWriter,
        event_observer: Callable[[EventEnvelope, int, int], None] | None = None,
        operation_observer: Callable[[str, int], None] | None = None,
        seal_observer: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        self.layout = layout
        self.catalog = catalog
        self.market = market
        self.symbol = symbol
        self.stream = stream
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.rotation = rotation
        self.durability_interval_seconds = durability_interval_seconds
        self.max_frame_bytes = max_frame_bytes
        self.queue = BoundedEventQueue(queue_capacity)
        self._writer_factory = writer_factory
        self._event_observer = event_observer
        self._operation_observer = operation_observer
        self._seal_observer = seal_observer
        self._writer: RawChunkWriter | None = None
        self._operation_lock = Lock()
        self._operation_last_ns = {
            "append_latency_ns": 0,
            "write_latency_ns": 0,
            "fsync_latency_ns": 0,
            "seal_latency_ns": 0,
        }
        self._operation_max_ns = dict(self._operation_last_ns)

    def enqueue(self, envelope: EventEnvelope) -> None:
        self.queue.put_nowait(envelope)

    def _new_writer(self) -> RawChunkWriter:
        return self._writer_factory(
            layout=self.layout,
            catalog=self.catalog,
            market=self.market,
            symbol=self.symbol,
            stream=self.stream,
            collector_instance_id=self.collector_instance_id,
            collector_version=self.collector_version,
            rotation=self.rotation,
            durability_interval_seconds=self.durability_interval_seconds,
            max_frame_bytes=self.max_frame_bytes,
            operation_observer=self._observe_writer_operation,
        )

    def _observe_duration(self, name: str, duration_ns: int) -> None:
        with self._operation_lock:
            self._operation_last_ns[name] = duration_ns
            self._operation_max_ns[name] = max(
                self._operation_max_ns[name], duration_ns
            )

    def _observe_writer_operation(self, name: str, duration_ns: int) -> None:
        self._observe_duration(name, duration_ns)
        if self._operation_observer is not None:
            self._operation_observer(name, duration_ns)

    def operation_stats(self) -> SpoolOperationStats:
        with self._operation_lock:
            return SpoolOperationStats(
                append_last_ns=self._operation_last_ns["append_latency_ns"],
                append_max_ns=self._operation_max_ns["append_latency_ns"],
                write_last_ns=self._operation_last_ns["write_latency_ns"],
                write_max_ns=self._operation_max_ns["write_latency_ns"],
                fsync_last_ns=self._operation_last_ns["fsync_latency_ns"],
                fsync_max_ns=self._operation_max_ns["fsync_latency_ns"],
                seal_last_ns=self._operation_last_ns["seal_latency_ns"],
                seal_max_ns=self._operation_max_ns["seal_latency_ns"],
            )

    def drain_one(self) -> bool:
        envelope = self.queue.get_nowait()
        if envelope is None:
            if self._writer is not None:
                if self._writer.should_rotate():
                    self._seal_current()
                else:
                    self._writer.sync_if_due()
            return False
        writer_created = self._writer is None
        if writer_created:
            self._writer = self._new_writer()
        writer = self._writer
        if writer is None:
            raise RuntimeError("stream writer was not created")
        before = 0 if writer_created else writer.bytes_written
        append_started = time.perf_counter_ns()
        try:
            writer.append(envelope)
        finally:
            self._observe_duration(
                "append_latency_ns", time.perf_counter_ns() - append_started
            )
        raw_frame_bytes = writer.bytes_written - before
        if self._event_observer is not None:
            self._event_observer(envelope, raw_frame_bytes, self.queue.depth)
        if writer.should_rotate():
            self._seal_current()
        return True

    def drain_all(self) -> int:
        count = 0
        while self.drain_one():
            count += 1
        return count

    def sync(self) -> None:
        """Make the current partial durable before committing external progress."""

        if self._writer is not None:
            self._writer.sync()

    def _seal_current(
        self, forced_flags: frozenset[str] = frozenset()
    ) -> dict[str, object] | None:
        if self._writer is None:
            return None
        writer = self._writer
        seal_started = time.perf_counter_ns()
        try:
            writer.close()
            manifest = seal_partial(
                writer.path,
                layout=self.layout,
                catalog=self.catalog,
                forced_flags=forced_flags,
            )
        except BaseException:
            with suppress(OSError):
                writer.abort()
            raise
        finally:
            self._writer = None
            self._observe_duration(
                "seal_latency_ns", time.perf_counter_ns() - seal_started
            )
        if self._seal_observer is not None:
            self._seal_observer(manifest)
        return manifest

    def close_and_seal(
        self, forced_flags: frozenset[str] = frozenset()
    ) -> dict[str, object] | None:
        self.drain_all()
        return self._seal_current(forced_flags=forced_flags)

    def abort_writer(self) -> None:
        """Release a failed writer descriptor while retaining its recoverable partial."""

        writer = self._writer
        self._writer = None
        if writer is not None:
            writer.abort()
