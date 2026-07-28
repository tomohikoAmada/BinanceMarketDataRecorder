"""Verified internal-spool to registered-directory archival."""

from .drain import archive_drain
from .manager import (
    ArchiveError,
    ArchiveManager,
    ArchiveResult,
    ArchiveTarget,
)

__all__ = ["ArchiveError", "ArchiveManager", "ArchiveResult", "ArchiveTarget",
           "archive_drain"]
