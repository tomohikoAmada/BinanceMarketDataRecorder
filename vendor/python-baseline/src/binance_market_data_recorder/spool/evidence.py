"""Private immutable evidence passed into the shared durable seal protocol."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .format import ChunkHeader, ChunkStatistics, ConnectionTransition, ScanResult


@dataclass(frozen=True, slots=True)
class _ChunkStatisticsSnapshot:
    record_count: int
    receive_time_utc_min_ns: int | None
    receive_time_utc_max_ns: int | None
    receive_monotonic_min_ns: int | None
    receive_monotonic_max_ns: int | None
    exchange_time_ranges: tuple[tuple[str, tuple[int, int]], ...]
    sequence_values: tuple[tuple[str, tuple[int | str, int | str]], ...]
    connection_ids: frozenset[str]
    collector_instance_ids: frozenset[str]
    capture_flags: frozenset[str]

    @classmethod
    def from_statistics(cls, statistics: ChunkStatistics) -> _ChunkStatisticsSnapshot:
        return cls(
            record_count=statistics.record_count,
            receive_time_utc_min_ns=statistics.receive_time_utc_min_ns,
            receive_time_utc_max_ns=statistics.receive_time_utc_max_ns,
            receive_monotonic_min_ns=statistics.receive_monotonic_min_ns,
            receive_monotonic_max_ns=statistics.receive_monotonic_max_ns,
            exchange_time_ranges=tuple(
                (name, (values[0], values[1]))
                for name, values in sorted(statistics.exchange_time_ranges.items())
            ),
            sequence_values=tuple(
                (name, (values[0], values[1]))
                for name, values in sorted(statistics.sequence_values.items())
            ),
            connection_ids=frozenset(statistics.connection_ids),
            collector_instance_ids=frozenset(statistics.collector_instance_ids),
            capture_flags=frozenset(statistics.capture_flags),
        )

    def mutable_copy(self) -> ChunkStatistics:
        return ChunkStatistics(
            record_count=self.record_count,
            receive_time_utc_min_ns=self.receive_time_utc_min_ns,
            receive_time_utc_max_ns=self.receive_time_utc_max_ns,
            receive_monotonic_min_ns=self.receive_monotonic_min_ns,
            receive_monotonic_max_ns=self.receive_monotonic_max_ns,
            exchange_time_ranges={
                name: [values[0], values[1]] for name, values in self.exchange_time_ranges
            },
            sequence_values={
                name: [values[0], values[1]] for name, values in self.sequence_values
            },
            connection_ids=set(self.connection_ids),
            collector_instance_ids=set(self.collector_instance_ids),
            capture_flags=set(self.capture_flags),
        )


@dataclass(frozen=True, slots=True)
class _VerifiedChunkEvidence:
    """Complete semantic and byte identity for one clean closed Raw partial."""

    path: Path
    header: ChunkHeader
    statistics: _ChunkStatisticsSnapshot
    file_size: int
    uncompressed_sha256: str
    connection_transitions: tuple[ConnectionTransition, ...]

    @classmethod
    def from_scan(cls, scan: ScanResult) -> _VerifiedChunkEvidence:
        if not scan.is_clean or scan.header is None or scan.uncompressed_sha256 is None:
            raise ValueError("clean scan evidence required")
        return cls(
            path=scan.path,
            header=scan.header,
            statistics=_ChunkStatisticsSnapshot.from_statistics(scan.statistics),
            file_size=scan.file_size,
            uncompressed_sha256=scan.uncompressed_sha256,
            connection_transitions=scan.connection_transitions,
        )
