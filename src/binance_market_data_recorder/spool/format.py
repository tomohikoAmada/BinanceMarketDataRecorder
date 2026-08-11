"""Raw chunk v1 帧格式、规范 CBOR、CRC32C 和有界扫描器。

ADR-0010 冻结了准确的字节布局:

- 固定头(28 字节):魔数 "BMRCHNK\x1a" + 版本号(1.0)+ BOM(0xFEFF)
  + flags(恒为 0)+ CBOR body 长度 + CRC32C。Header CRC 覆盖固定前缀和
  CBOR body。CBOR body 必须为规范格式(RFC 8949 第 4.2.1 节),恰好 11 个键;
  任何偏差视为 INVALID_HEADER。
- 每个帧:12 字节前缀(body length + flags=0 + reserved=0 + CRC32C),
  然后是规范 CBOR body。CRC32C 覆盖前缀(字节 0-7)和 body。
  帧长度必须在 0 到 max_frame_bytes(默认 16 MiB)之间。
- scan_chunk() 在一次正向遍历中读取文件,不缓冲完整帧。它验证每个帧的 CRC
  和 envelope 身份,累积统计信息。任何失败时,返回第一个无效字节位置的
  ScanIssue。这在帧数量上为 O(1) 内存。

Envelope CBOR 必须为规范格式。decode_envelope() 通过重新编码并比较来强制执行。
非规范 CBOR 被视为无效,即使它解码到相同逻辑值。这确保确定性哈希。
Raw 中的 capture_flags 是 JSON(或 CBOR)数组;decode_envelope() 将其转换为
Python tuple 以兼容 Pydantic。
"""

from __future__ import annotations

import hashlib
import io
import struct
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import BinaryIO, Final
from uuid import UUID

import cbor2
import google_crc32c
from pydantic import ValidationError

from ..domain.event import EventEnvelope

MAGIC: Final = b"BMRCHNK\x1a"
FORMAT_MAJOR: Final = 1
FORMAT_MINOR: Final = 0
BYTE_ORDER_MARKER: Final = 0xFEFF
CHUNK_SCHEMA_VERSION: Final = "raw-chunk.v1"
ENVELOPE_SCHEMA_VERSION: Final = "event-envelope.v1"
DEFAULT_MAX_FRAME_BYTES: Final = 16 * 1024 * 1024
FIXED_HEADER = struct.Struct(">8sBBHIII")
FRAME_PREFIX_WITHOUT_CRC = struct.Struct(">IHH")
FRAME_PREFIX = struct.Struct(">IHHI")


class ChunkFormatError(ValueError):
    """Raised when bytes violate Raw chunk v1."""


class ScanIssue(StrEnum):
    NONE = "none"
    TRUNCATED_TAIL = "truncated_tail"
    INVALID_HEADER = "invalid_header"
    INVALID_FRAME_LENGTH = "invalid_frame_length"
    UNSUPPORTED_FLAGS = "unsupported_flags"
    CHECKSUM_FAILURE = "checksum_failure"
    INVALID_ENVELOPE = "invalid_envelope"


@dataclass(frozen=True)
class ChunkHeader:
    chunk_id: UUID
    created_at_utc_ns: int
    collector_instance_id: str
    collector_version: str
    market: str
    symbol: str
    stream: str
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES
    chunk_schema_version: str = CHUNK_SCHEMA_VERSION
    envelope_schema_version: str = ENVELOPE_SCHEMA_VERSION

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "chunk_id": self.chunk_id.bytes,
            "chunk_schema_version": self.chunk_schema_version,
            "collector_instance_id": self.collector_instance_id,
            "collector_version": self.collector_version,
            "created_at_utc_ns": self.created_at_utc_ns,
            "envelope_schema_version": self.envelope_schema_version,
            "format": "bmdr-raw-chunk",
            "market": self.market,
            "max_frame_bytes": self.max_frame_bytes,
            "stream": self.stream,
            "symbol": self.symbol,
        }


@dataclass
class ChunkStatistics:
    record_count: int = 0
    receive_time_utc_min_ns: int | None = None
    receive_time_utc_max_ns: int | None = None
    receive_monotonic_min_ns: int | None = None
    receive_monotonic_max_ns: int | None = None
    exchange_time_ranges: dict[str, list[int]] = field(default_factory=dict)
    sequence_values: dict[str, list[int | str]] = field(default_factory=dict)
    connection_ids: set[str] = field(default_factory=set)
    collector_instance_ids: set[str] = field(default_factory=set)
    capture_flags: set[str] = field(default_factory=set)

    def add(self, envelope: EventEnvelope) -> None:
        self.record_count += 1
        self.receive_time_utc_min_ns = _minimum(
            self.receive_time_utc_min_ns, envelope.receive_time_utc_ns
        )
        self.receive_time_utc_max_ns = _maximum(
            self.receive_time_utc_max_ns, envelope.receive_time_utc_ns
        )
        self.receive_monotonic_min_ns = _minimum(
            self.receive_monotonic_min_ns, envelope.receive_monotonic_ns
        )
        self.receive_monotonic_max_ns = _maximum(
            self.receive_monotonic_max_ns, envelope.receive_monotonic_ns
        )
        exchange_times = {
            "event": envelope.exchange_event_time,
            "transaction": envelope.exchange_transaction_time,
            "trade": envelope.exchange_trade_time,
        }
        for name, exchange_value in exchange_times.items():
            if exchange_value is not None:
                self.exchange_time_ranges.setdefault(name, [exchange_value, exchange_value])
                self.exchange_time_ranges[name][0] = min(
                    self.exchange_time_ranges[name][0], exchange_value
                )
                self.exchange_time_ranges[name][1] = max(
                    self.exchange_time_ranges[name][1], exchange_value
                )
        for name, sequence_value in envelope.source_sequence.items():
            sequence_range = self.sequence_values.setdefault(
                name, [sequence_value, sequence_value]
            )
            updated = _updated_sequence_range(
                sequence_range[0], sequence_range[1], sequence_value
            )
            if updated is None:
                self.capture_flags.add("mixed_sequence_type")
            else:
                sequence_range[:] = updated
        self.connection_ids.add(envelope.connection_id)
        self.collector_instance_ids.add(envelope.collector_instance_id)
        self.capture_flags.update(envelope.capture_flags)

    def sequence_ranges(self) -> dict[str, dict[str, int | str]]:
        return {
            name: {"min": values[0], "max": values[1]}
            for name, values in sorted(self.sequence_values.items())
        }


#: One connection_id transition observed inside a chunk: the connection that
#: held the previous frame, the connection that holds the following frame, and
#: the per-frame capture flags of both boundary frames. Transition evidence is
#: boundary-local: a flag on one transition never proves another transition.
ConnectionTransition = tuple[
    str, str, frozenset[str], frozenset[str]
]


@dataclass(frozen=True)
class ScanResult:
    path: Path
    header: ChunkHeader | None
    statistics: ChunkStatistics
    valid_end: int
    file_size: int
    issue: ScanIssue
    detail: str | None
    uncompressed_sha256: str | None
    connection_transitions: tuple[ConnectionTransition, ...] = ()

    @property
    def is_clean(self) -> bool:
        return self.issue is ScanIssue.NONE

    @property
    def is_tail_truncatable(self) -> bool:
        return self.issue is ScanIssue.TRUNCATED_TAIL and self.header is not None


def _minimum(current: int | None, value: int) -> int:
    return value if current is None else min(current, value)


def _maximum(current: int | None, value: int) -> int:
    return value if current is None else max(current, value)


def _updated_sequence_range(
    low: int | str, high: int | str, value: int | str
) -> tuple[int | str, int | str] | None:
    if isinstance(low, int) and isinstance(high, int) and isinstance(value, int):
        return min(low, value), max(high, value)
    if isinstance(low, str) and isinstance(high, str) and isinstance(value, str):
        return min(low, value), max(high, value)
    return None


def _canonical_cbor(value: object) -> bytes:
    return cbor2.dumps(value, canonical=True)


def _decode_one_cbor(body: bytes) -> object:
    source = io.BytesIO(body)
    value = cbor2.CBORDecoder(source).decode()
    if source.read(1):
        raise ChunkFormatError("CBOR value has trailing bytes")
    if _canonical_cbor(value) != body:
        raise ChunkFormatError("CBOR value is not canonical")
    return value


def _read_up_to(source: BinaryIO, length: int) -> bytes:
    blocks: list[bytes] = []
    remaining = length
    while remaining:
        block = source.read(remaining)
        if not block:
            break
        blocks.append(block)
        remaining -= len(block)
    return b"".join(blocks)


def encode_envelope(envelope: EventEnvelope) -> bytes:
    return _canonical_cbor(envelope.canonical_mapping())


def decode_envelope(body: bytes) -> EventEnvelope:
    try:
        value = _decode_one_cbor(body)
        if not isinstance(value, dict):
            raise ChunkFormatError("envelope CBOR root is not a map")
        if isinstance(value.get("capture_flags"), list):
            value = dict(value)
            value["capture_flags"] = tuple(value["capture_flags"])
        return EventEnvelope.model_validate(value)
    except (cbor2.CBORDecodeError, ValidationError, ChunkFormatError) as exc:
        raise ChunkFormatError(f"invalid EventEnvelope: {exc}") from exc


def encode_chunk_header(header: ChunkHeader) -> bytes:
    body = _canonical_cbor(header.canonical_mapping())
    fixed_without_crc = struct.pack(
        ">8sBBHII",
        MAGIC,
        FORMAT_MAJOR,
        FORMAT_MINOR,
        BYTE_ORDER_MARKER,
        0,
        len(body),
    )
    checksum = google_crc32c.value(fixed_without_crc + body)
    return fixed_without_crc + struct.pack(">I", checksum) + body


def decode_chunk_header(source: BinaryIO) -> tuple[ChunkHeader, bytes]:
    fixed = _read_up_to(source, FIXED_HEADER.size)
    if len(fixed) != FIXED_HEADER.size:
        raise ChunkFormatError("truncated fixed header")
    magic, major, minor, marker, flags, body_length, expected_crc = FIXED_HEADER.unpack(fixed)
    if magic != MAGIC:
        raise ChunkFormatError("bad magic")
    if (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR):
        raise ChunkFormatError(f"unsupported chunk version {major}.{minor}")
    if marker != BYTE_ORDER_MARKER:
        raise ChunkFormatError("invalid byte-order marker")
    if flags != 0:
        raise ChunkFormatError("unsupported chunk flags")
    if body_length > 64 * 1024:
        raise ChunkFormatError("header exceeds 64 KiB")
    body = _read_up_to(source, body_length)
    if len(body) != body_length:
        raise ChunkFormatError("truncated CBOR header")
    without_crc = fixed[: FIXED_HEADER.size - 4]
    if google_crc32c.value(without_crc + body) != expected_crc:
        raise ChunkFormatError("header checksum failure")
    try:
        value = _decode_one_cbor(body)
        if not isinstance(value, dict):
            raise ChunkFormatError("header CBOR root is not a map")
        expected_keys = {
            "chunk_id",
            "chunk_schema_version",
            "collector_instance_id",
            "collector_version",
            "created_at_utc_ns",
            "envelope_schema_version",
            "format",
            "market",
            "max_frame_bytes",
            "stream",
            "symbol",
        }
        if set(value) != expected_keys:
            raise ChunkFormatError("header keys do not match raw-chunk.v1")
        if value["format"] != "bmdr-raw-chunk":
            raise ChunkFormatError("invalid format identifier")
        if value["chunk_schema_version"] != CHUNK_SCHEMA_VERSION:
            raise ChunkFormatError("unsupported chunk schema")
        if value["envelope_schema_version"] != ENVELOPE_SCHEMA_VERSION:
            raise ChunkFormatError("unsupported envelope schema")
        chunk_id_bytes = value["chunk_id"]
        if not isinstance(chunk_id_bytes, bytes) or len(chunk_id_bytes) != 16:
            raise ChunkFormatError("chunk_id must contain 16 UUID bytes")
        created_at = value["created_at_utc_ns"]
        if not isinstance(created_at, int) or isinstance(created_at, bool) or created_at < 0:
            raise ChunkFormatError("invalid created_at_utc_ns")
        for text_field in (
            "collector_instance_id",
            "collector_version",
            "market",
            "stream",
            "symbol",
        ):
            if not isinstance(value[text_field], str) or not value[text_field]:
                raise ChunkFormatError(f"invalid {text_field}")
        max_frame_bytes = value["max_frame_bytes"]
        if not isinstance(max_frame_bytes, int) or isinstance(max_frame_bytes, bool):
            raise ChunkFormatError("invalid max_frame_bytes")
        if not 1024 <= max_frame_bytes <= 64 * 1024 * 1024:
            raise ChunkFormatError("max_frame_bytes outside supported bounds")
        header = ChunkHeader(
            chunk_id=UUID(bytes=chunk_id_bytes),
            created_at_utc_ns=created_at,
            collector_instance_id=value["collector_instance_id"],
            collector_version=value["collector_version"],
            market=value["market"],
            symbol=value["symbol"],
            stream=value["stream"],
            max_frame_bytes=max_frame_bytes,
        )
    except (KeyError, TypeError, ValueError, cbor2.CBORDecodeError) as exc:
        raise ChunkFormatError(f"invalid chunk header: {exc}") from exc
    return header, fixed + body


def encode_frame(envelope: EventEnvelope, *, max_frame_bytes: int) -> bytes:
    if len(envelope.raw_payload) > max_frame_bytes:
        raise ChunkFormatError(
            f"raw payload {len(envelope.raw_payload)} exceeds maximum {max_frame_bytes}"
        )
    body = encode_envelope(envelope)
    if len(body) > max_frame_bytes:
        raise ChunkFormatError(
            f"encoded frame body {len(body)} exceeds maximum {max_frame_bytes}"
        )
    covered_prefix = FRAME_PREFIX_WITHOUT_CRC.pack(len(body), 0, 0)
    checksum = google_crc32c.value(covered_prefix + body)
    return covered_prefix + struct.pack(">I", checksum) + body


def scan_chunk(path: Path) -> ScanResult:
    """Scan one uncompressed chunk without retaining event bodies in memory."""

    file_size = path.stat().st_size
    statistics = ChunkStatistics()
    digest = hashlib.sha256()
    transitions: list[ConnectionTransition] = []
    previous_connection_id: str | None = None
    previous_flags: frozenset[str] = frozenset()
    try:
        with path.open("rb", buffering=0) as source:
            try:
                header, header_bytes = decode_chunk_header(source)
            except ChunkFormatError as exc:
                return ScanResult(
                    path,
                    None,
                    statistics,
                    0,
                    file_size,
                    ScanIssue.INVALID_HEADER,
                    str(exc),
                    None,
                )
            digest.update(header_bytes)
            valid_end = len(header_bytes)
            while True:
                prefix = _read_up_to(source, FRAME_PREFIX.size)
                if not prefix:
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.NONE,
                        None,
                        digest.hexdigest(),
                        tuple(transitions),
                    )
                if len(prefix) != FRAME_PREFIX.size:
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.TRUNCATED_TAIL,
                        "incomplete frame prefix",
                        None,
                    )
                body_length, flags, reserved, expected_crc = FRAME_PREFIX.unpack(prefix)
                if body_length > header.max_frame_bytes:
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.INVALID_FRAME_LENGTH,
                        f"declared frame body {body_length} exceeds maximum",
                        None,
                    )
                if flags != 0 or reserved != 0:
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.UNSUPPORTED_FLAGS,
                        f"flags={flags}, reserved={reserved}",
                        None,
                    )
                body = _read_up_to(source, body_length)
                if len(body) != body_length:
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.TRUNCATED_TAIL,
                        "incomplete frame body",
                        None,
                    )
                covered_prefix = prefix[: FRAME_PREFIX_WITHOUT_CRC.size]
                if google_crc32c.value(covered_prefix + body) != expected_crc:
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.CHECKSUM_FAILURE,
                        "frame CRC32C mismatch",
                        None,
                    )
                try:
                    envelope = decode_envelope(body)
                except ChunkFormatError as exc:
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.INVALID_ENVELOPE,
                        str(exc),
                        None,
                    )
                if (
                    envelope.market != header.market
                    or envelope.symbol != header.symbol
                    or envelope.stream != header.stream
                ):
                    return ScanResult(
                        path,
                        header,
                        statistics,
                        valid_end,
                        file_size,
                        ScanIssue.INVALID_ENVELOPE,
                        "envelope identity differs from chunk header",
                        None,
                    )
                statistics.add(envelope)
                if (
                    previous_connection_id is not None
                    and envelope.connection_id != previous_connection_id
                ):
                    transitions.append(
                        (
                            previous_connection_id,
                            envelope.connection_id,
                            previous_flags,
                            frozenset(envelope.capture_flags),
                        )
                    )
                previous_connection_id = envelope.connection_id
                previous_flags = frozenset(envelope.capture_flags)
                digest.update(prefix)
                digest.update(body)
                valid_end += len(prefix) + len(body)
    except OSError as exc:
        raise ChunkFormatError(f"cannot scan chunk {path}: {exc}") from exc
