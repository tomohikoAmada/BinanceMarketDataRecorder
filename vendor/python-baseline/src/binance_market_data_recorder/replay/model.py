"""Public immutable models for deterministic normalized-data replay."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType

REPLAY_ORDER_VERSION = "replay-order.v1"
CONSUMER_CONTRACT_VERSION = "consumer-contract.v1"


class ReplayError(RuntimeError):
    """A replay request cannot preserve its published semantics."""


class ReplayGapError(ReplayError):
    """The selected range contains explicitly unreliable source data."""


class MissingExchangeTimeError(ReplayError):
    """Exchange-time replay encountered a row without that clock."""


class CheckpointSeekError(ReplayError):
    """A checkpoint cannot safely seed the selected replay query."""


class ReplayClock(StrEnum):
    RECEIVE_TIME = "RECEIVE_TIME"
    EXCHANGE_TIME = "EXCHANGE_TIME"


class GapPolicy(StrEnum):
    ERROR = "ERROR"
    INCLUDE = "INCLUDE"
    EXCLUDE = "EXCLUDE"


class MissingExchangeTimePolicy(StrEnum):
    ERROR = "ERROR"
    EXCLUDE = "EXCLUDE"
    FALLBACK_RECEIVE = "FALLBACK_RECEIVE"


@dataclass(frozen=True, slots=True)
class ReplayQuery:
    clock: ReplayClock = ReplayClock.RECEIVE_TIME
    markets: tuple[str, ...] = ()
    streams: tuple[str, ...] = ()
    symbol: str = "BTCUSDT"
    start_time_ns: int | None = None
    end_time_ns: int | None = None
    gap_policy: GapPolicy = GapPolicy.ERROR
    missing_exchange_time: MissingExchangeTimePolicy = (
        MissingExchangeTimePolicy.ERROR
    )
    checkpoint_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.clock, ReplayClock):
            raise TypeError("clock must be ReplayClock")
        if not isinstance(self.gap_policy, GapPolicy):
            raise TypeError("gap_policy must be GapPolicy")
        if not isinstance(self.missing_exchange_time, MissingExchangeTimePolicy):
            raise TypeError(
                "missing_exchange_time must be MissingExchangeTimePolicy"
            )
        if not isinstance(self.markets, tuple) or not isinstance(
            self.streams, tuple
        ):
            raise TypeError("market and stream filters must be tuples")
        if any(
            not isinstance(value, str) or not value
            for values in (self.markets, self.streams)
            for value in values
        ):
            raise ValueError("market and stream filters must be non-empty strings")
        if len(set(self.markets)) != len(self.markets) or len(
            set(self.streams)
        ) != len(self.streams):
            raise ValueError("market and stream filters must be unique")
        if not isinstance(self.symbol, str) or not self.symbol:
            raise ValueError("symbol must be non-empty")
        for name, value in (
            ("start_time_ns", self.start_time_ns),
            ("end_time_ns", self.end_time_ns),
        ):
            if value is not None and (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(f"{name} must be a non-negative integer")
        if (
            self.start_time_ns is not None
            and self.end_time_ns is not None
            and self.start_time_ns >= self.end_time_ns
        ):
            raise ValueError("replay time range must be non-empty")
        if self.checkpoint_id is not None and (
            not isinstance(self.checkpoint_id, str) or not self.checkpoint_id
        ):
            raise ValueError("checkpoint_id must be non-empty text")


@dataclass(frozen=True, slots=True)
class BuildSummary:
    build_id: str
    dataset_version: str
    dedup_version: str
    parquet_profile: str
    partition_count: int
    normalized_rows: int
    source_chunk_count: int
    checkpoint_count: int


@dataclass(frozen=True, slots=True)
class PartitionDescriptor:
    market: str
    stream: str
    date: str
    hour: str
    schema_version: str
    logical_sha256: str
    stored_sha256: str
    stored_bytes: int
    row_count: int
    receive_time_utc_min_ns: int
    receive_time_utc_max_ns: int
    source_chunk_hashes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CheckpointDescriptor:
    checkpoint_id: str
    schema_version: str
    algorithm_version: str
    market: str
    symbol: str
    update_id: int
    created_at_utc_ns: int
    book_hash: str
    file_sha256: str
    source_chunk_hashes: tuple[str, ...]
    book: Mapping[str, object]
    unreliable_intervals: tuple[Mapping[str, object], ...]


@dataclass(frozen=True, slots=True)
class ReplayEvent:
    dataset_version: str
    build_id: str
    order_version: str
    clock: ReplayClock
    event_time_ns: int
    used_receive_time_fallback: bool
    is_unreliable: bool
    row: Mapping[str, object]

    @classmethod
    def create(
        cls,
        *,
        dataset_version: str,
        build_id: str,
        clock: ReplayClock,
        event_time_ns: int,
        used_receive_time_fallback: bool,
        is_unreliable: bool,
        row: dict[str, object],
    ) -> ReplayEvent:
        return cls(
            dataset_version=dataset_version,
            build_id=build_id,
            order_version=REPLAY_ORDER_VERSION,
            clock=clock,
            event_time_ns=event_time_ns,
            used_receive_time_fallback=used_receive_time_fallback,
            is_unreliable=is_unreliable,
            row=MappingProxyType(dict(row)),
        )
