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
- 每帧先复制成 private immutable semantic snapshot; exact frame bytes、stats、
  connection transition、SHA-256 与 verified byte count 只在完整写入后共同提交。
- 任意不明确的 write/evidence/fsync/close 失败永久 poison clean evidence;
  final fsync + close 成功后 evidence 只能被 live seal 入口消费一次。
- close() 是幂等的;密封转换由 StreamSpool 处理。
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from types import MappingProxyType
from uuid import UUID, uuid4

from ..domain.event import EventEnvelope
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, fsync_directory
from .evidence import _ChunkStatisticsSnapshot, _VerifiedChunkEvidence
from .format import (
    ChunkHeader,
    ChunkStatistics,
    ConnectionTransition,
    encode_chunk_header,
    encode_frame,
)


@dataclass(frozen=True)
class RotationPolicy:
    seconds: float = 60.0
    bytes: int = 128 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValueError("rotation seconds must be positive")
        if self.bytes < 1024 * 1024:
            raise ValueError("rotation bytes must be at least 1 MiB")


class _WriterState(StrEnum):
    OPEN_CLEAN = "OPEN_CLEAN"
    POISONED = "POISONED"
    CLOSED_CLEAN = "CLOSED_CLEAN"
    EVIDENCE_CONSUMED = "EVIDENCE_CONSUMED"


@dataclass(frozen=True, slots=True)
class _ImmutableEnvelopeSnapshot:
    schema_version: str
    venue: str
    market: str
    symbol: str
    stream: str
    module: str
    connection_id: str
    collector_instance_id: str
    collector_version: str
    receive_time_utc_ns: int
    receive_monotonic_ns: int
    exchange_event_time: int | None
    exchange_transaction_time: int | None
    exchange_trade_time: int | None
    source_sequence: Mapping[str, int | str]
    payload_encoding: str
    raw_payload: bytes
    capture_flags: tuple[str, ...]

    @classmethod
    def from_envelope(cls, envelope: EventEnvelope) -> _ImmutableEnvelopeSnapshot:
        return cls(
            schema_version=envelope.schema_version,
            venue=envelope.venue,
            market=envelope.market,
            symbol=envelope.symbol,
            stream=envelope.stream,
            module=envelope.module,
            connection_id=envelope.connection_id,
            collector_instance_id=envelope.collector_instance_id,
            collector_version=envelope.collector_version,
            receive_time_utc_ns=envelope.receive_time_utc_ns,
            receive_monotonic_ns=envelope.receive_monotonic_ns,
            exchange_event_time=envelope.exchange_event_time,
            exchange_transaction_time=envelope.exchange_transaction_time,
            exchange_trade_time=envelope.exchange_trade_time,
            source_sequence=MappingProxyType(dict(envelope.source_sequence)),
            payload_encoding=envelope.payload_encoding,
            raw_payload=bytes(envelope.raw_payload),
            capture_flags=tuple(envelope.capture_flags),
        )

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "venue": self.venue,
            "market": self.market,
            "symbol": self.symbol,
            "stream": self.stream,
            "module": self.module,
            "connection_id": self.connection_id,
            "collector_instance_id": self.collector_instance_id,
            "collector_version": self.collector_version,
            "receive_time_utc_ns": self.receive_time_utc_ns,
            "receive_monotonic_ns": self.receive_monotonic_ns,
            "exchange_event_time": self.exchange_event_time,
            "exchange_transaction_time": self.exchange_transaction_time,
            "exchange_trade_time": self.exchange_trade_time,
            "source_sequence": dict(self.source_sequence),
            "payload_encoding": self.payload_encoding,
            "raw_payload": self.raw_payload,
            "capture_flags": self.capture_flags,
        }


@dataclass(frozen=True, slots=True)
class _PreparedFrame:
    snapshot: _ImmutableEnvelopeSnapshot
    exact_bytes: bytes
    capture_flags: frozenset[str]
    connection_transition: ConnectionTransition | None
    expected_ordinal: int


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
        self.layout = layout
        self.catalog = catalog
        self.rotation = rotation or RotationPolicy()
        self.durability_interval_seconds = durability_interval_seconds
        self.header = ChunkHeader(
            chunk_id=chunk_id or uuid4(),
            created_at_utc_ns=(
                time.time_ns() if created_at_utc_ns is None else created_at_utc_ns
            ),
            collector_instance_id=collector_instance_id,
            collector_version=collector_version,
            market=market,
            symbol=symbol,
            stream=stream,
            max_frame_bytes=max_frame_bytes,
        )
        header_bytes = encode_chunk_header(self.header)
        self.path = layout.active / f"{self.header.chunk_id.hex}.bmdr.partial"
        self._state = _WriterState.OPEN_CLEAN
        self._closed = False
        self._record_count = 0
        self._bytes_written = 0
        self._verified_bytes = 0
        self._raw_digest = sha256()
        self._statistics = ChunkStatistics()
        self._connection_transitions: list[ConnectionTransition] = []
        self._previous_connection_id: str | None = None
        self._previous_capture_flags: frozenset[str] = frozenset()
        self.operation_observer = operation_observer
        self._descriptor = os.open(
            self.path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        self._opened_monotonic = time.monotonic()
        self._rotation_deadline_monotonic = _rotation_deadline(
            opened_monotonic=self._opened_monotonic,
            period_seconds=self.rotation.seconds,
            market=market,
            stream=stream,
        )
        self._last_sync_monotonic = self._opened_monotonic
        try:
            self._write_all(header_bytes)
            self._raw_digest.update(header_bytes)
            self._verified_bytes = len(header_bytes)
            self.sync()
            fsync_directory(layout.active)
            catalog.register_active(
                chunk_id=str(self.header.chunk_id),
                partial_path=layout.relative(self.path),
                created_at_utc_ns=self.header.created_at_utc_ns,
            )
        except BaseException:
            self._poison()
            try:
                os.close(self._descriptor)
            finally:
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

    @property
    def poisoned(self) -> bool:
        return self._state is _WriterState.POISONED

    def _poison(self) -> None:
        if self._state is not _WriterState.EVIDENCE_CONSUMED:
            self._state = _WriterState.POISONED

    def _require_open_clean(self) -> None:
        if self._closed:
            raise RuntimeError("chunk writer is closed")
        if self._state is not _WriterState.OPEN_CLEAN:
            raise RuntimeError("chunk writer is poisoned")

    def _write_all(self, data: bytes) -> None:
        view = memoryview(data)
        try:
            while view:
                written = os.write(self._descriptor, view)
                if written <= 0:
                    raise OSError("write returned no progress")
                self._bytes_written += written
                view = view[written:]
        except BaseException:
            self._poison()
            raise

    def _prepare_frame(self, envelope: EventEnvelope) -> _PreparedFrame:
        snapshot = _ImmutableEnvelopeSnapshot.from_envelope(envelope)
        if (
            snapshot.market != self.header.market
            or snapshot.symbol != self.header.symbol
            or snapshot.stream != self.header.stream
        ):
            raise ValueError("envelope identity differs from chunk identity")
        capture_flags = frozenset(snapshot.capture_flags)
        transition: ConnectionTransition | None = None
        if (
            self._previous_connection_id is not None
            and snapshot.connection_id != self._previous_connection_id
        ):
            transition = (
                self._previous_connection_id,
                snapshot.connection_id,
                self._previous_capture_flags,
                capture_flags,
            )
        return _PreparedFrame(
            snapshot=snapshot,
            exact_bytes=encode_frame(
                snapshot, max_frame_bytes=self.header.max_frame_bytes
            ),
            capture_flags=capture_flags,
            connection_transition=transition,
            expected_ordinal=self._record_count,
        )

    def _commit_prepared_frame(self, prepared: _PreparedFrame) -> None:
        snapshot = prepared.snapshot
        if prepared.expected_ordinal != self._record_count:
            raise RuntimeError("prepared Raw frame ordinal is stale")
        if prepared.connection_transition is not None:
            self._connection_transitions.append(prepared.connection_transition)
        self._statistics.add(snapshot)
        self._raw_digest.update(prepared.exact_bytes)
        self._verified_bytes += len(prepared.exact_bytes)
        self._record_count += 1
        self._previous_connection_id = snapshot.connection_id
        self._previous_capture_flags = prepared.capture_flags
        if (
            self._statistics.record_count != self._record_count
            or self._verified_bytes != self._bytes_written
        ):
            raise RuntimeError("incremental Raw evidence invariant failed")

    def append(self, envelope: EventEnvelope) -> int:
        self._require_open_clean()
        prepared = self._prepare_frame(envelope)
        ordinal = self._record_count
        started = time.perf_counter_ns()
        self._write_all(prepared.exact_bytes)
        try:
            self._commit_prepared_frame(prepared)
            if self.operation_observer is not None:
                self.operation_observer(
                    "write_latency_ns", time.perf_counter_ns() - started
                )
        except BaseException:
            self._poison()
            raise
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
        self._require_open_clean()
        started = time.perf_counter_ns()
        try:
            os.fsync(self._descriptor)
            if self.operation_observer is not None:
                self.operation_observer(
                    "fsync_latency_ns", time.perf_counter_ns() - started
                )
        except BaseException:
            self._poison()
            raise
        self._last_sync_monotonic = time.monotonic()

    def should_rotate(self, *, now_monotonic: float | None = None) -> bool:
        now = time.monotonic() if now_monotonic is None else now_monotonic
        return (
            self._bytes_written >= self.rotation.bytes
            or now >= self._rotation_deadline_monotonic
        )

    def close(self) -> None:
        if self._closed:
            return
        error: BaseException | None = None
        if self._state is _WriterState.OPEN_CLEAN:
            try:
                self.sync()
            except BaseException as exc:
                error = exc
        try:
            os.close(self._descriptor)
        except BaseException as exc:
            self._poison()
            if error is None:
                error = exc
        finally:
            # A failed POSIX close leaves descriptor ownership ambiguous. Never
            # retry a possibly reused integer descriptor inside this process.
            self._closed = True
        if error is not None:
            raise error
        if self._state is _WriterState.OPEN_CLEAN:
            self._state = _WriterState.CLOSED_CLEAN

    def _take_clean_seal_evidence(self) -> _VerifiedChunkEvidence:
        """Finalize and consume this writer's memory-only clean authority once."""

        if self._state is _WriterState.EVIDENCE_CONSUMED:
            raise RuntimeError("clean seal evidence was already consumed")
        self.close()
        if self._state is not _WriterState.CLOSED_CLEAN:
            raise RuntimeError("poisoned writer cannot issue clean seal evidence")
        try:
            physical_size = self.path.stat().st_size
            if physical_size != self._verified_bytes:
                raise RuntimeError("closed Raw size differs from verified byte count")
            evidence = _VerifiedChunkEvidence(
                path=self.path,
                header=self.header,
                statistics=_ChunkStatisticsSnapshot.from_statistics(self._statistics),
                file_size=self._verified_bytes,
                uncompressed_sha256=self._raw_digest.hexdigest(),
                connection_transitions=tuple(self._connection_transitions),
            )
        except BaseException:
            self._poison()
            raise
        self._state = _WriterState.EVIDENCE_CONSUMED
        return evidence

    def abort(self) -> None:
        """Close without sealing after a write failure; recovery owns the partial."""

        if self._closed:
            return
        self._poison()
        try:
            os.close(self._descriptor)
        finally:
            self._closed = True

    def __enter__(self) -> RawChunkWriter:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def _rotation_deadline(
    *,
    opened_monotonic: float,
    period_seconds: float,
    market: str,
    stream: str,
) -> float:
    """Spread stream seals across one bounded period using a stable phase."""

    identity = f"{market}\0{stream}".encode()
    phase_ratio = int.from_bytes(sha256(identity).digest()[:8], "big") / 2**64
    phase_seconds = period_seconds * phase_ratio
    cycle_start = (opened_monotonic // period_seconds) * period_seconds
    deadline = cycle_start + phase_seconds
    if deadline <= opened_monotonic:
        deadline += period_seconds
    return deadline
