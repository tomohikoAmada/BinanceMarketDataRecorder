"""仅追加的 Raw chunk 写入器,具有有界轮换和持久性。

RawChunkWriter 为单个 market/symbol/stream 元组持有一个 .partial 文件。
关键设计约束:

- 写入器使用 os.open 配合 O_EXCL 以防止非干净关闭后意外复用 partial 文件。
  恢复必须扫描和协调所有 partial。
- append() 写入 CBOR 编码帧(长度前缀 + CRC32C + body),并根据
  durability_interval_seconds(最大 1 秒)有条件地 fsync。spool 循环在空闲时
  也调用 sync_if_due(),确保即使在低流量流上,最后写入的事件也在间隔内持久化。
- should_rotate() 检查时间(自打开以来的耗时)和大小(已写入字节),
  触发当前 chunk 的密封和新 chunk 的创建。
- header(chunk 身份、创建时间、schema 版本)在 Catalog register_active 调用
  之前被写入和 fsync,因此 header 之后、第一个帧之前的崩溃留下可恢复的 partial。
- close() 是幂等的;密封转换由 StreamSpool 处理。
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID, uuid4

from ..domain.event import EventEnvelope
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, fsync_directory
from .format import ChunkHeader, encode_chunk_header, encode_frame


@dataclass(frozen=True)
class RotationPolicy:
    seconds: float = 60.0
    bytes: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError("rotation seconds must be positive")
        if self.bytes < 1024 * 1024:
            raise ValueError("rotation bytes must be at least 1 MiB")


class RawChunkWriter:
    """Single-owner writer for one market/symbol/stream partial chunk."""

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
        rotation: RotationPolicy | None = None,
        durability_interval_seconds: float = 1.0,
        max_frame_bytes: int = 16 * 1024 * 1024,
        chunk_id: UUID | None = None,
        created_at_utc_ns: int | None = None,
        operation_observer: Callable[[str, int], None] | None = None,
    ) -> None:
        if not 0 <= durability_interval_seconds <= 1.0:
            raise ValueError("durability interval must be between 0 and 1 second")
        if not 1024 <= max_frame_bytes <= 64 * 1024 * 1024:
            raise ValueError("max frame bytes must be between 1 KiB and 64 MiB")
        identities = {
            "collector_instance_id": collector_instance_id,
            "collector_version": collector_version,
            "market": market,
            "stream": stream,
            "symbol": symbol,
        }
        if any(not value for value in identities.values()):
            raise ValueError("chunk identity fields must be non-empty")
        self.layout = layout
        self.catalog = catalog
        self.rotation = rotation or RotationPolicy()
        self.durability_interval_seconds = durability_interval_seconds
        self.header = ChunkHeader(
            chunk_id=chunk_id or uuid4(),
            created_at_utc_ns=created_at_utc_ns or time.time_ns(),
            collector_instance_id=collector_instance_id,
            collector_version=collector_version,
            market=market,
            symbol=symbol,
            stream=stream,
            max_frame_bytes=max_frame_bytes,
        )
        self.path = layout.active / f"{self.header.chunk_id.hex}.bmdr.partial"
        self._descriptor = os.open(
            self.path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        self._closed = False
        self._record_count = 0
        self._bytes_written = 0
        self._opened_monotonic = time.monotonic()
        self._last_sync_monotonic = self._opened_monotonic
        self.operation_observer = operation_observer
        try:
            header_bytes = encode_chunk_header(self.header)
            self._write_all(header_bytes)
            self.sync()
            fsync_directory(layout.active)
            catalog.register_active(
                chunk_id=str(self.header.chunk_id),
                partial_path=layout.relative(self.path),
                created_at_utc_ns=self.header.created_at_utc_ns,
            )
        except Exception:
            os.close(self._descriptor)
            self._closed = True
            raise

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def bytes_written(self) -> int:
        return self._bytes_written

    @property
    def closed(self) -> bool:
        return self._closed

    def _write_all(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            written = os.write(self._descriptor, view)
            if written <= 0:
                raise OSError("write returned no progress")
            self._bytes_written += written
            view = view[written:]

    def append(self, envelope: EventEnvelope) -> int:
        if self._closed:
            raise RuntimeError("chunk writer is closed")
        if (
            envelope.market != self.header.market
            or envelope.symbol != self.header.symbol
            or envelope.stream != self.header.stream
        ):
            raise ValueError("envelope identity differs from chunk identity")
        frame = encode_frame(envelope, max_frame_bytes=self.header.max_frame_bytes)
        ordinal = self._record_count
        started = time.perf_counter_ns()
        self._write_all(frame)
        if self.operation_observer is not None:
            self.operation_observer("write_latency_ns", time.perf_counter_ns() - started)
        self._record_count += 1
        now = time.monotonic()
        self.sync_if_due(now_monotonic=now)
        return ordinal

    def sync_if_due(self, *, now_monotonic: float | None = None) -> bool:
        """Fsync when due; a spool loop calls this even when the stream is idle."""

        now = time.monotonic() if now_monotonic is None else now_monotonic
        if (
            self.durability_interval_seconds == 0
            or now - self._last_sync_monotonic >= self.durability_interval_seconds
        ):
            self.sync()
            return True
        return False

    def sync(self) -> None:
        if self._closed:
            raise RuntimeError("chunk writer is closed")
        started = time.perf_counter_ns()
        os.fsync(self._descriptor)
        if self.operation_observer is not None:
            self.operation_observer("fsync_latency_ns", time.perf_counter_ns() - started)
        self._last_sync_monotonic = time.monotonic()

    def should_rotate(self, *, now_monotonic: float | None = None) -> bool:
        now = time.monotonic() if now_monotonic is None else now_monotonic
        return (
            self._bytes_written >= self.rotation.bytes
            or now - self._opened_monotonic >= self.rotation.seconds
        )

    def close(self) -> None:
        if self._closed:
            return
        self.sync()
        os.close(self._descriptor)
        self._closed = True

    def __enter__(self) -> RawChunkWriter:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
