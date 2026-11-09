"""Public generic consumer API for verified deterministic replay."""

from .catalog import ManifestCatalog, ReplayCatalogError
from .clock import ClockReading, EventClock
from .model import (
    CONSUMER_CONTRACT_VERSION,
    REPLAY_ORDER_VERSION,
    BuildSummary,
    CheckpointDescriptor,
    CheckpointSeekError,
    GapPolicy,
    MissingExchangeTimeError,
    MissingExchangeTimePolicy,
    PartitionDescriptor,
    ReplayClock,
    ReplayError,
    ReplayEvent,
    ReplayGapError,
    ReplayQuery,
)
from .reader import ReplayDataset

__all__ = [
    "CONSUMER_CONTRACT_VERSION",
    "REPLAY_ORDER_VERSION",
    "BuildSummary",
    "CheckpointDescriptor",
    "CheckpointSeekError",
    "ClockReading",
    "EventClock",
    "GapPolicy",
    "ManifestCatalog",
    "MissingExchangeTimeError",
    "MissingExchangeTimePolicy",
    "PartitionDescriptor",
    "ReplayCatalogError",
    "ReplayClock",
    "ReplayDataset",
    "ReplayError",
    "ReplayEvent",
    "ReplayGapError",
    "ReplayQuery",
]
