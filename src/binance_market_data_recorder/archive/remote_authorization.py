"""Non-destructive M22.4A remote authorization and recovery interpretation.

This module may persist only ``REMOTE_DELETE_PENDING``.  It never mutates the
source filesystem and intentionally exposes no terminal deletion transition.
"""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import cast

from ..storage.catalog import (
    Catalog,
    CatalogStateError,
    ChunkState,
    RemoteArchiveState,
)
from ..storage.layout import StorageLayout
from .remote_receive import RemoteArchiveReceipt, RemoteReceiveError
from .remote_source import (
    RemoteSourceDescriptor,
    RemoteSourceError,
    RemoteSourceSelection,
    canonical_descriptor_bytes,
    descriptor_from_retained_manifest,
    descriptor_sha256,
    revalidate_remote_source_selection,
)


class RemoteAuthorizationError(RuntimeError):
    """Remote lifecycle authority or source evidence fails closed."""


class RemoteRecoveryCase(StrEnum):
    NORMAL = "NORMAL"
    CASE_A = "CASE_A"
    CASE_B = "CASE_B"
    CASE_C = "CASE_C"
    CASE_D = "CASE_D"
    TERMINAL_ABSENT = "TERMINAL_ABSENT"
    TERMINAL_PRESENT_CONTRADICTION = "TERMINAL_PRESENT_CONTRADICTION"
    IMPOSSIBLE_SAME_HOST_REMOTE_OVERLAP = "IMPOSSIBLE_SAME_HOST_REMOTE_OVERLAP"


class RemoteSourceObservation(StrEnum):
    PRESENT_MATCHING = "PRESENT_MATCHING"
    PRESENT_MISMATCH = "PRESENT_MISMATCH"
    ABSENT = "ABSENT"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class RemotePendingAuthorization:
    receipt_id: str
    chunk_id: str
    state: RemoteArchiveState
    receipt_bytes: bytes
    created_at_utc_ns: int


@dataclass(frozen=True, slots=True)
class RemoteRecoveryDecision:
    case: RemoteRecoveryCase
    observation: RemoteSourceObservation
    detail: str

    @property
    def fail_closed(self) -> bool:
        return self.case in {
            RemoteRecoveryCase.CASE_C,
            RemoteRecoveryCase.CASE_D,
            RemoteRecoveryCase.TERMINAL_PRESENT_CONTRADICTION,
            RemoteRecoveryCase.IMPOSSIBLE_SAME_HOST_REMOTE_OVERLAP,
        }


class RemoteAuthorizer:
    """Persist one exact receipt/source-bound remote pending authorization."""

    def __init__(
        self,
        *,
        layout: StorageLayout,
        catalog: Catalog,
        fault_hook: Callable[[str], None] | None = None,
        utc_clock_ns: Callable[[], int] | None = None,
    ) -> None:
        self.layout = layout
        self.catalog = catalog
        self.fault_hook = fault_hook
        self.utc_clock_ns = utc_clock_ns

    def authorize(
        self, receipt_bytes: bytes, selection: RemoteSourceSelection
    ) -> RemotePendingAuthorization:
        try:
            receipt = RemoteArchiveReceipt.from_bytes(receipt_bytes)
            current = revalidate_remote_source_selection(
                layout=self.layout,
                catalog=self.catalog,
                selection=selection,
                permitted_remote_receipt_id=receipt.receipt_id,
            )
            _require_receipt_descriptor_binding(receipt, current)
            timestamp = self.utc_clock_ns() if self.utc_clock_ns is not None else None
            row = self.catalog.reserve_remote_archive_transaction(
                receipt_bytes=receipt_bytes,
                market=current.descriptor.market,
                stream=current.descriptor.stream,
                expected_chunk=_required_chunk_snapshot(self.catalog, receipt.chunk_id),
                occurred_at_utc_ns=timestamp,
                fault_hook=self.fault_hook,
            )
            _require_persisted_source_binding(
                layout=self.layout,
                catalog=self.catalog,
                row=row,
            )
            return RemotePendingAuthorization(
                receipt_id=receipt.receipt_id,
                chunk_id=receipt.chunk_id,
                state=RemoteArchiveState(str(row["state"])),
                receipt_bytes=cast(bytes, row["receipt_bytes"]),
                created_at_utc_ns=cast(int, row["created_at_utc_ns"]),
            )
        except RemoteAuthorizationError:
            raise
        except (
            CatalogStateError,
            OSError,
            RemoteReceiveError,
            RemoteSourceError,
            ValueError,
        ) as exc:
            raise RemoteAuthorizationError(
                f"remote authorization failed closed: {exc}"
            ) from exc


def validated_remote_authorizations_between(
    *, layout: StorageLayout, catalog: Catalog, start_utc_ns: int, end_utc_ns: int
) -> list[dict[str, object]]:
    """Return report rows after retained-manifest descriptor revalidation."""

    try:
        rows = catalog.remote_authorizations_between(start_utc_ns, end_utc_ns)
        validated: list[dict[str, object]] = []
        for row in rows:
            chunk = catalog.chunk(str(row["chunk_id"]))
            if chunk is None:
                raise CatalogStateError("remote report source chunk is missing")
            manifest_bytes = _read_retained_manifest(layout=layout, chunk=chunk)
            descriptor = descriptor_from_retained_manifest(
                layout=layout,
                catalog=catalog,
                row=chunk,
                manifest_bytes=manifest_bytes,
            )
            _require_remote_descriptor_binding(row, descriptor)
            validated.append(row)
        return validated
    except CatalogStateError:
        raise
    except (
        OSError,
        RemoteAuthorizationError,
        RemoteReceiveError,
        RemoteSourceError,
        ValueError,
    ) as exc:
        raise CatalogStateError("remote report authority validation failed") from exc


def classify_remote_recovery(
    *, layout: StorageLayout, catalog: Catalog, chunk_id: str
) -> RemoteRecoveryDecision:
    """Classify current durable facts without changing Catalog or filesystem."""

    try:
        chunk, same_host, remote = catalog.source_lifecycle_snapshot(chunk_id)
    except CatalogStateError as exc:
        return _decision(RemoteRecoveryCase.CASE_D, RemoteSourceObservation.UNKNOWN, exc)
    if chunk is None:
        return _decision(
            RemoteRecoveryCase.CASE_D,
            RemoteSourceObservation.UNKNOWN,
            "chunk missing from Catalog",
        )
    if same_host is not None and remote is not None:
        return _decision(
            RemoteRecoveryCase.IMPOSSIBLE_SAME_HOST_REMOTE_OVERLAP,
            RemoteSourceObservation.UNKNOWN,
            "same-host and remote ownership overlap",
        )
    if remote is not None and chunk.get("state") != ChunkState.SEALED.value:
        return _decision(
            RemoteRecoveryCase.IMPOSSIBLE_SAME_HOST_REMOTE_OVERLAP,
            RemoteSourceObservation.UNKNOWN,
            "remote ownership requires physical SEALED state",
        )
    if same_host is not None:
        return _decision(
            RemoteRecoveryCase.NORMAL,
            RemoteSourceObservation.UNKNOWN,
            "ordinary same-host archive lifecycle",
        )
    if chunk.get("state") != ChunkState.SEALED.value:
        return _decision(
            RemoteRecoveryCase.CASE_D,
            RemoteSourceObservation.UNKNOWN,
            "unsupported physical state for remote recovery",
        )

    observation = observe_remote_source(layout=layout, chunk=chunk)
    try:
        manifest_bytes = _read_retained_manifest(layout=layout, chunk=chunk)
        descriptor = descriptor_from_retained_manifest(
            layout=layout,
            catalog=catalog,
            row=chunk,
            manifest_bytes=manifest_bytes,
        )
    except (OSError, RemoteSourceError, ValueError) as exc:
        return _decision(RemoteRecoveryCase.CASE_D, observation, exc)
    try:
        final_chunk, final_same_host, final_remote = catalog.source_lifecycle_snapshot(
            chunk_id
        )
    except CatalogStateError as exc:
        return _decision(RemoteRecoveryCase.CASE_D, observation, exc)
    if (final_chunk, final_same_host, final_remote) != (chunk, same_host, remote):
        return _decision(
            RemoteRecoveryCase.CASE_D,
            RemoteSourceObservation.UNKNOWN,
            "Catalog lifecycle changed during remote recovery observation",
        )

    if remote is None:
        if observation is RemoteSourceObservation.PRESENT_MATCHING:
            return _decision(
                RemoteRecoveryCase.NORMAL,
                observation,
                "ordinary unowned sealed source",
            )
        if observation is RemoteSourceObservation.ABSENT:
            return _decision(
                RemoteRecoveryCase.CASE_C,
                observation,
                "unexplained source loss without durable remote authorization",
            )
        return _decision(
            RemoteRecoveryCase.CASE_D,
            observation,
            "unowned source evidence is mismatched or unknown",
        )

    try:
        _require_remote_descriptor_binding(remote, descriptor)
        state = RemoteArchiveState(str(remote["state"]))
    except (
        KeyError,
        TypeError,
        ValueError,
        RemoteAuthorizationError,
        RemoteReceiveError,
    ) as exc:
        return _decision(RemoteRecoveryCase.CASE_D, observation, exc)

    if state is RemoteArchiveState.REMOTE_DELETE_PENDING:
        if observation is RemoteSourceObservation.PRESENT_MATCHING:
            return _decision(
                RemoteRecoveryCase.CASE_A,
                observation,
                "authorized remote deletion remains pending",
            )
        if observation is RemoteSourceObservation.ABSENT:
            return _decision(
                RemoteRecoveryCase.CASE_B,
                observation,
                "authorized crash-interrupted deletion candidate",
            )
        return _decision(
            RemoteRecoveryCase.CASE_D,
            observation,
            "pending source evidence is mismatched or unknown",
        )
    if observation is RemoteSourceObservation.ABSENT:
        return _decision(
            RemoteRecoveryCase.TERMINAL_ABSENT,
            observation,
            "terminal remote lifecycle with exact retained evidence",
        )
    if observation in {
        RemoteSourceObservation.PRESENT_MATCHING,
        RemoteSourceObservation.PRESENT_MISMATCH,
    }:
        return _decision(
            RemoteRecoveryCase.TERMINAL_PRESENT_CONTRADICTION,
            observation,
            "REMOTE_DELETED contradicts a present source object",
        )
    return _decision(
        RemoteRecoveryCase.CASE_D,
        observation,
        "terminal source absence cannot be established safely",
    )


def observe_remote_source(
    *, layout: StorageLayout, chunk: Mapping[str, object]
) -> RemoteSourceObservation:
    """Observe the exact Raw leaf without equating arbitrary errors with absence."""

    relative = chunk.get("sealed_path")
    if not isinstance(relative, str) or not relative:
        return RemoteSourceObservation.UNKNOWN
    try:
        path = _exact_layout_path(layout, relative, layout.sealed)
        parent_status = os.lstat(path.parent)
        if stat.S_ISLNK(parent_status.st_mode) or not stat.S_ISDIR(parent_status.st_mode):
            return RemoteSourceObservation.UNKNOWN
    except OSError:
        return RemoteSourceObservation.UNKNOWN
    try:
        before = os.lstat(path)
    except FileNotFoundError:
        return RemoteSourceObservation.ABSENT
    except OSError:
        return RemoteSourceObservation.UNKNOWN
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        return RemoteSourceObservation.PRESENT_MISMATCH
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        return RemoteSourceObservation.UNKNOWN
    try:
        descriptor = os.open(path, flags | no_follow)
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                return RemoteSourceObservation.PRESENT_MISMATCH
            digest = hashlib.sha256()
            total = 0
            while block := os.read(descriptor, 1024 * 1024):
                digest.update(block)
                total += len(block)
        finally:
            os.close(descriptor)
        after = os.lstat(path)
    except OSError:
        return RemoteSourceObservation.UNKNOWN
    stable_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    stable_opened = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
    stable_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if stable_before != stable_opened or stable_opened != stable_after:
        return RemoteSourceObservation.UNKNOWN
    expected_size = chunk.get("stored_bytes")
    expected_hash = chunk.get("stored_sha256")
    if total != expected_size or digest.hexdigest() != expected_hash:
        return RemoteSourceObservation.PRESENT_MISMATCH
    return RemoteSourceObservation.PRESENT_MATCHING


def _required_chunk_snapshot(catalog: Catalog, chunk_id: str) -> dict[str, object]:
    chunk, _local, _remote = catalog.source_lifecycle_snapshot(chunk_id)
    if chunk is None:
        raise RemoteAuthorizationError("source chunk disappeared from Catalog")
    return chunk


def _require_receipt_descriptor_binding(
    receipt: RemoteArchiveReceipt, selection: RemoteSourceSelection
) -> None:
    descriptor = selection.descriptor
    expected = {
        "source_descriptor_schema_version": descriptor.descriptor_schema_version,
        "source_descriptor_sha256": selection.descriptor_sha256,
        "chunk_id": descriptor.chunk_id,
        "source_relative_path": descriptor.source_relative_path,
        "source_manifest_relative_path": descriptor.source_manifest_relative_path,
        "source_manifest_sha256": descriptor.source_manifest_sha256,
        "stored_bytes": descriptor.stored_bytes,
        "stored_sha256": descriptor.stored_sha256,
    }
    if any(getattr(receipt, key) != value for key, value in expected.items()):
        raise RemoteAuthorizationError("receipt/source descriptor identity mismatch")


def _require_persisted_source_binding(
    *, layout: StorageLayout, catalog: Catalog, row: Mapping[str, object]
) -> None:
    chunk, same_host, remote = catalog.source_lifecycle_snapshot(str(row["chunk_id"]))
    if chunk is None or same_host is not None or remote is None:
        raise RemoteAuthorizationError("persisted remote ownership snapshot mismatch")
    manifest_bytes = _read_retained_manifest(layout=layout, chunk=chunk)
    descriptor = descriptor_from_retained_manifest(
        layout=layout, catalog=catalog, row=chunk, manifest_bytes=manifest_bytes
    )
    _require_remote_descriptor_binding(remote, descriptor)


def _require_remote_descriptor_binding(
    row: Mapping[str, object], descriptor: RemoteSourceDescriptor
) -> None:
    canonical = canonical_descriptor_bytes(descriptor)
    digest = descriptor_sha256(canonical)
    expected = {
        "source_descriptor_schema_version": descriptor.descriptor_schema_version,
        "source_descriptor_sha256": digest,
        "chunk_id": descriptor.chunk_id,
        "market": descriptor.market,
        "stream": descriptor.stream,
        "source_relative_path": descriptor.source_relative_path,
        "source_manifest_relative_path": descriptor.source_manifest_relative_path,
        "source_manifest_sha256": descriptor.source_manifest_sha256,
        "stored_bytes": descriptor.stored_bytes,
        "stored_sha256": descriptor.stored_sha256,
    }
    if any(row.get(key) != value for key, value in expected.items()):
        raise RemoteAuthorizationError("remote row/source descriptor identity mismatch")
    receipt_bytes = row.get("receipt_bytes")
    if not isinstance(receipt_bytes, bytes):
        raise RemoteAuthorizationError("remote row receipt bytes are invalid")
    receipt = RemoteArchiveReceipt.from_bytes(receipt_bytes)
    if receipt.source_descriptor_sha256 != digest:
        raise RemoteAuthorizationError("receipt/source descriptor digest mismatch")


def _read_retained_manifest(
    *, layout: StorageLayout, chunk: Mapping[str, object]
) -> bytes:
    relative = chunk.get("manifest_path")
    if not isinstance(relative, str) or not relative:
        raise OSError("Catalog manifest path is invalid")
    path = _exact_layout_path(layout, relative, layout.manifests)
    parent_status = os.lstat(path.parent)
    if stat.S_ISLNK(parent_status.st_mode) or not stat.S_ISDIR(parent_status.st_mode):
        raise OSError("source manifest parent is unsafe")
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise OSError("safe no-follow file open is unavailable")
    before = os.lstat(path)
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise OSError("source manifest is not a regular file")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | no_follow)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise OSError("source manifest is not a regular file")
        blocks: list[bytes] = []
        while block := os.read(descriptor, 1024 * 1024):
            blocks.append(block)
    finally:
        os.close(descriptor)
    after = os.lstat(path)
    stable_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    stable_opened = (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
    stable_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if stable_before != stable_opened or stable_opened != stable_after:
        raise OSError("source manifest changed during observation")
    return b"".join(blocks)


def _exact_layout_path(layout: StorageLayout, relative: str, directory: Path) -> Path:
    candidate = Path(relative)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise OSError("unsafe Recorder-relative path")
    lexical = layout.root / candidate
    if directory.is_symlink() or lexical.parent.resolve() != directory.resolve():
        raise OSError("path escapes exact Recorder directory")
    return lexical


def _decision(
    case: RemoteRecoveryCase,
    observation: RemoteSourceObservation,
    detail: object,
) -> RemoteRecoveryDecision:
    return RemoteRecoveryDecision(case, observation, str(detail))
