"""Resolve a Raw observation location without importing a derived-data consumer."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from .catalog import ArchiveState, Catalog


class RawSourceError(RuntimeError):
    """A selected immutable Raw source cannot be verified or decoded."""


def safe_relative(base: Path, value: object) -> Path:
    if not isinstance(value, str) or not value:
        raise RawSourceError("manifest contains an invalid relative path")
    candidate = (base / value).resolve()
    try:
        candidate.relative_to(base.resolve())
    except ValueError as exc:
        raise RawSourceError("manifest path escapes its Recorder root") from exc
    return candidate


def archived_artifact(
    *,
    chunk_id: str,
    catalog: Catalog,
    external_roots: Mapping[str, Path],
) -> Path | None:
    transaction = catalog.archive_transaction_for_chunk(chunk_id)
    if transaction is None:
        return None
    state = ArchiveState(str(transaction["state"]))
    if state not in {
        ArchiveState.VERIFIED,
        ArchiveState.LOCAL_DELETE_PENDING,
        ArchiveState.LOCAL_DELETED,
    }:
        return None
    storage_id = str(transaction["storage_id"])
    root = external_roots.get(storage_id)
    if root is None:
        return None
    return safe_relative(root.resolve(), transaction["target_relative_path"])
