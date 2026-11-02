"""Catalog lifecycle types, state machine enums, and transition rules."""

from __future__ import annotations

from enum import StrEnum

RAW_VERIFICATION_VERSION = "raw-independent.v2"


class ChunkState(StrEnum):
    ACTIVE = "ACTIVE"
    RECOVERED = "RECOVERED"
    SEALING = "SEALING"
    SEALED = "SEALED"
    ARCHIVE_COPYING = "ARCHIVE_COPYING"
    ARCHIVE_VERIFYING = "ARCHIVE_VERIFYING"
    ARCHIVED_VERIFIED = "ARCHIVED_VERIFIED"
    LOCAL_DELETE_PENDING = "LOCAL_DELETE_PENDING"
    LOCAL_DELETED = "LOCAL_DELETED"
    QUARANTINED = "QUARANTINED"


class ArchiveState(StrEnum):
    COPYING = "COPYING"
    VERIFYING = "VERIFYING"
    VERIFIED = "VERIFIED"
    LOCAL_DELETE_PENDING = "LOCAL_DELETE_PENDING"
    LOCAL_DELETED = "LOCAL_DELETED"


class RemoteArchiveState(StrEnum):
    """Separate remote source lifecycle; physical ChunkState remains SEALED."""

    REMOTE_DELETE_PENDING = "REMOTE_DELETE_PENDING"
    REMOTE_DELETED = "REMOTE_DELETED"


class StorageControlState(StrEnum):
    ACTIVE = "ACTIVE"
    EJECT_PENDING = "EJECT_PENDING"
    SAFE_TO_REMOVE = "SAFE_TO_REMOVE"


class DeploymentState(StrEnum):
    CANDIDATE_STARTING = "CANDIDATE_STARTING"
    CANDIDATE_READY = "CANDIDATE_READY"
    OVERLAP_CONFIRMED = "OVERLAP_CONFIRMED"
    CUTOVER_COMPLETE = "CUTOVER_COMPLETE"
    ROLLED_BACK = "ROLLED_BACK"


class CatalogStateError(RuntimeError):
    """Raised for invalid Catalog lifecycle state or access mode."""


ALLOWED_TRANSITIONS = {
    ChunkState.ACTIVE: {ChunkState.RECOVERED, ChunkState.SEALING, ChunkState.QUARANTINED},
    ChunkState.RECOVERED: {ChunkState.SEALING, ChunkState.QUARANTINED},
    ChunkState.SEALING: {ChunkState.SEALED, ChunkState.QUARANTINED},
    ChunkState.SEALED: {ChunkState.ARCHIVE_COPYING},
    ChunkState.ARCHIVE_COPYING: {ChunkState.ARCHIVE_VERIFYING},
    ChunkState.ARCHIVE_VERIFYING: {ChunkState.ARCHIVED_VERIFIED},
    ChunkState.ARCHIVED_VERIFIED: {ChunkState.LOCAL_DELETE_PENDING},
    ChunkState.LOCAL_DELETE_PENDING: {ChunkState.LOCAL_DELETED},
    ChunkState.LOCAL_DELETED: set(),
    ChunkState.QUARANTINED: set(),
}

ARCHIVE_TRANSITIONS = {
    ArchiveState.COPYING: {ArchiveState.VERIFYING},
    ArchiveState.VERIFYING: {ArchiveState.VERIFIED},
    ArchiveState.VERIFIED: {ArchiveState.LOCAL_DELETE_PENDING},
    ArchiveState.LOCAL_DELETE_PENDING: {ArchiveState.LOCAL_DELETED},
    ArchiveState.LOCAL_DELETED: set(),
}

ARCHIVE_CHUNK_STATES = {
    ArchiveState.COPYING: ChunkState.ARCHIVE_COPYING,
    ArchiveState.VERIFYING: ChunkState.ARCHIVE_VERIFYING,
    ArchiveState.VERIFIED: ChunkState.ARCHIVED_VERIFIED,
    ArchiveState.LOCAL_DELETE_PENDING: ChunkState.LOCAL_DELETE_PENDING,
    ArchiveState.LOCAL_DELETED: ChunkState.LOCAL_DELETED,
}

DEPLOYMENT_TRANSITIONS = {
    DeploymentState.CANDIDATE_STARTING: {
        DeploymentState.CANDIDATE_READY,
        DeploymentState.ROLLED_BACK,
    },
    DeploymentState.CANDIDATE_READY: {
        DeploymentState.OVERLAP_CONFIRMED,
        DeploymentState.ROLLED_BACK,
    },
    DeploymentState.OVERLAP_CONFIRMED: {
        DeploymentState.CUTOVER_COMPLETE,
        DeploymentState.ROLLED_BACK,
    },
    DeploymentState.CUTOVER_COMPLETE: set(),
    DeploymentState.ROLLED_BACK: set(),
}

LEGACY_SINGLE_SYMBOL = "BTCUSDT"

_MS1_MIGRATION_FAULT_PHASES = frozenset(
    {
        "after_rename",
        "mid_copy",
        "after_drop",
        "before_index",
        "after_index",
        "before_commit",
    }
)

_MS1_FRESH_INITIALIZATION_FAULT_PHASES = frozenset(
    {
        "after_operational_events",
        "after_cursor_table",
        "after_fresh_identity_commit",
    }
)

_MS1_IDENTITY_FAULT_PHASES = _MS1_MIGRATION_FAULT_PHASES | _MS1_FRESH_INITIALIZATION_FAULT_PHASES
