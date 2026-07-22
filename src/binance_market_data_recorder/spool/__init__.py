"""Crash-recoverable internal Raw spool."""

from .format import ChunkHeader, ScanIssue, ScanResult, decode_envelope, encode_envelope
from .queue import BoundedEventQueue, IngressQueueFull
from .recovery import RecoveryAction, recover_storage
from .seal import SealError, seal_partial
from .stream import StreamSpool
from .writer import RawChunkWriter, RotationPolicy

__all__ = [
    "BoundedEventQueue",
    "ChunkHeader",
    "IngressQueueFull",
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
