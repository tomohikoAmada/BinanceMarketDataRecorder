"""Crash-recoverable internal Raw spool."""

from .async_queue import AsyncQueueStats, BoundedAsyncQueue, PutResult
from .format import ChunkHeader, ScanIssue, ScanResult, decode_envelope, encode_envelope
from .queue import (
    BoundedEventQueue,
    IngressBackpressureTimeout,
    IngressPersistenceTimeout,
    IngressQueueFull,
    IngressStopRequested,
    IngressWriterStopped,
)
from .recovery import RecoveryAction, recover_storage
from .seal import SealError, seal_partial
from .stream import StreamSpool
from .writer import RawChunkWriter, RotationPolicy

__all__ = [
    "AsyncQueueStats",
    "BoundedAsyncQueue",
    "BoundedEventQueue",
    "ChunkHeader",
    "IngressBackpressureTimeout",
    "IngressPersistenceTimeout",
    "IngressQueueFull",
    "IngressStopRequested",
    "IngressWriterStopped",
    "PutResult",
    "RawChunkWriter",
    "RecoveryAction",
    "RotationPolicy",
    "ScanIssue",
    "ScanResult",
    "SealError",
    "StreamSpool",
    "decode_envelope",
    "encode_envelope",
    "recover_storage",
    "seal_partial",
]
