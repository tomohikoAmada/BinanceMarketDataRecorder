"""SQLite lifecycle Catalog; market-event payloads never enter this database.

Catalog is split into domain modules:
- types.py: Enums, exceptions, transitions, schemas
- base.py: Core SQLite connection management, locking, and atomic transactions
- schema.py: DDL execution, schema version migrations, and table structure validation
- chunks.py: Raw chunk lifecycle, verification records, and quarantined artifacts
- availability.py: Operational events, availability intervals, watermarks, side-data cursors
- archive_ops.py: Local and remote archival operations, storage targets, ejection control
- reports.py: Space forecasting, daily metric batches, and deployment audit tracking
"""

from __future__ import annotations

from .archive_ops import (
    CatalogArchiveMixin,
    _is_lower_sha256,
    _parse_remote_receipt,
    _validate_relative_path,
)
from .availability import (
    _DISCONTINUITY_EVENT_TYPES,
    _SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES,
    CatalogAvailabilityMixin,
    _closed_lifecycle_identity_valid,
    _discontinuity_identity,
    _optional_identity_text,
    stream_discontinuity_event_id,
)
from .base import CatalogBase
from .chunks import CatalogChunksMixin
from .reports import CatalogReportsMixin
from .schema import (
    _CREATE_CURRENT_OPERATIONAL_EVENTS_SQL,
    _CREATE_CURRENT_SIDE_DATA_CURSORS_SQL,
    _CREATE_DISCONTINUITY_IDENTITY_INDEX_SQL,
    _CURRENT_OPERATIONAL_EVENT_COLUMNS,
    _CURRENT_SIDE_DATA_CURSOR_COLUMNS,
    _DISCONTINUITY_IDENTITY_INDEX,
    _LEGACY_OPERATIONAL_EVENT_COLUMNS,
    _LEGACY_SIDE_DATA_CURSOR_COLUMNS,
    _REMOTE_EVENT_COLUMNS,
    _REMOTE_TRANSACTION_COLUMNS,
    CatalogSchemaMixin,
)
from .types import (
    _MS1_FRESH_INITIALIZATION_FAULT_PHASES,
    _MS1_IDENTITY_FAULT_PHASES,
    _MS1_MIGRATION_FAULT_PHASES,
    ALLOWED_TRANSITIONS,
    ARCHIVE_CHUNK_STATES,
    ARCHIVE_TRANSITIONS,
    DEPLOYMENT_TRANSITIONS,
    LEGACY_SINGLE_SYMBOL,
    RAW_VERIFICATION_VERSION,
    ArchiveState,
    CatalogStateError,
    ChunkState,
    DeploymentState,
    RemoteArchiveState,
    StorageControlState,
)


class Catalog(
    CatalogChunksMixin,
    CatalogAvailabilityMixin,
    CatalogArchiveMixin,
    CatalogReportsMixin,
    CatalogSchemaMixin,
    CatalogBase,
):
    """SQLite lifecycle Catalog; market-event payloads never enter this database."""


__all__ = [
    "ALLOWED_TRANSITIONS",
    "ARCHIVE_CHUNK_STATES",
    "ARCHIVE_TRANSITIONS",
    "DEPLOYMENT_TRANSITIONS",
    "LEGACY_SINGLE_SYMBOL",
    "RAW_VERIFICATION_VERSION",
    "_CREATE_CURRENT_OPERATIONAL_EVENTS_SQL",
    "_CREATE_CURRENT_SIDE_DATA_CURSORS_SQL",
    "_CREATE_DISCONTINUITY_IDENTITY_INDEX_SQL",
    "_CURRENT_OPERATIONAL_EVENT_COLUMNS",
    "_CURRENT_SIDE_DATA_CURSOR_COLUMNS",
    "_DISCONTINUITY_EVENT_TYPES",
    "_DISCONTINUITY_IDENTITY_INDEX",
    "_LEGACY_OPERATIONAL_EVENT_COLUMNS",
    "_LEGACY_SIDE_DATA_CURSOR_COLUMNS",
    "_MS1_FRESH_INITIALIZATION_FAULT_PHASES",
    "_MS1_IDENTITY_FAULT_PHASES",
    "_MS1_MIGRATION_FAULT_PHASES",
    "_REMOTE_EVENT_COLUMNS",
    "_REMOTE_TRANSACTION_COLUMNS",
    "_SYMBOL_SPECIFIC_SIDE_DATA_EVENT_TYPES",
    "ArchiveState",
    "Catalog",
    "CatalogArchiveMixin",
    "CatalogAvailabilityMixin",
    "CatalogBase",
    "CatalogChunksMixin",
    "CatalogReportsMixin",
    "CatalogSchemaMixin",
    "CatalogStateError",
    "ChunkState",
    "DeploymentState",
    "RemoteArchiveState",
    "StorageControlState",
    "_closed_lifecycle_identity_valid",
    "_discontinuity_identity",
    "_is_lower_sha256",
    "_optional_identity_text",
    "_parse_remote_receipt",
    "_validate_relative_path",
    "stream_discontinuity_event_id",
]
