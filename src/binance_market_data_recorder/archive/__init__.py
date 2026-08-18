"""Verified internal-spool to registered-directory archival."""

from .drain import archive_drain
from .manager import (
    ArchiveError,
    ArchiveManager,
    ArchiveResult,
    ArchiveTarget,
)
from .remote_source import (
    REMOTE_SOURCE_DESCRIPTOR_SCHEMA,
    RemoteSourceDescriptor,
    RemoteSourceError,
    RemoteSourceExporter,
    RemoteSourceSelection,
    canonical_descriptor_bytes,
    descriptor_sha256,
)

__all__ = [
    "REMOTE_SOURCE_DESCRIPTOR_SCHEMA",
    "ArchiveError",
    "ArchiveManager",
    "ArchiveResult",
    "ArchiveTarget",
    "RemoteSourceDescriptor",
    "RemoteSourceError",
    "RemoteSourceExporter",
    "RemoteSourceSelection",
    "archive_drain",
    "canonical_descriptor_bytes",
    "descriptor_sha256",
]
