"""Verified internal-spool to registered-directory archival."""

from .manager import (
    ArchiveError,
    ArchiveManager,
    ArchiveResult,
    ArchiveTarget,
)

__all__ = ["ArchiveError", "ArchiveManager", "ArchiveResult", "ArchiveTarget"]
