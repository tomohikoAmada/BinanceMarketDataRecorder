"""Crash-safe coordination around non-forced macOS Disk Arbitration eject."""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from ..catalog import Catalog, CatalogStateError
from .model import PlatformEjectResult, StorageState, VolumeInfo
from .registry import StorageRegistrationError, validate_registered_root
from .volumes import PlatformVolumeError


class EjectError(RuntimeError):
    """An eject request could not be safely attempted."""


class EjectPlatform(Protocol):
    def inventory(self) -> list[VolumeInfo]: ...

    def request_eject(
        self, volume: VolumeInfo, *, timeout_seconds: float = 30.0
    ) -> PlatformEjectResult: ...


@dataclass(frozen=True, slots=True)
class EjectResult:
    storage_id: str
    request_id: str
    status: str
    safe_to_remove: bool
    message: str
    platform: dict[str, object] | None
    active_transactions: tuple[dict[str, object], ...] = ()
    internal_source_preserved: bool = True
    forced_removal: bool = False

    def public_dict(self) -> dict[str, object]:
        return {
            "storage_id": self.storage_id,
            "request_id": self.request_id,
            "status": self.status,
            "safe_to_remove": self.safe_to_remove,
            "message": self.message,
            "platform": self.platform,
            "active_transactions": list(self.active_transactions),
            "internal_source_preserved": self.internal_source_preserved,
            "forced": self.forced_removal,
        }


class SafeEjectCoordinator:
    """Serialize archive allocation with system-confirmed unmount and eject."""

    def __init__(
        self,
        *,
        catalog: Catalog,
        platform: EjectPlatform,
        utc_clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self._catalog = catalog
        self._platform = platform
        self._utc_clock_ns = utc_clock_ns

    def eject(self, storage_id: str, *, timeout_seconds: float = 30.0) -> EjectResult:
        if not storage_id:
            raise EjectError("storage_id must be non-empty")
        if timeout_seconds <= 0:
            raise EjectError("eject timeout must be positive")
        target = next(
            (
                row
                for row in self._catalog.storage_targets()
                if row["storage_id"] == storage_id
            ),
            None,
        )
        if target is None:
            raise EjectError(f"unknown storage_id: {storage_id}")
        volume = next(
            (
                item
                for item in self._platform.inventory()
                if item.volume_uuid == target["volume_uuid"]
            ),
            None,
        )
        if volume is None:
            raise EjectError("registered volume is absent")
        if volume.mountpoint is None:
            raise EjectError("registered volume is not mounted")
        root = (volume.mountpoint / str(target["relative_path"])).resolve()
        try:
            validate_registered_root(
                root,
                volume_uuid=str(target["volume_uuid"]),
                relative_path=str(target["relative_path"]),
                storage_id=storage_id,
                marker_nonce=str(target["marker_nonce"]),
            )
        except StorageRegistrationError as exc:
            raise EjectError(f"registered target identity unavailable: {exc}") from exc

        request_id = str(uuid.uuid4())
        requested_at = self._utc_clock_ns()
        active = self._catalog.begin_storage_eject(
            storage_id=storage_id,
            request_id=request_id,
            occurred_at_utc_ns=requested_at,
        )
        if active:
            self._record(
                request_id,
                "STORAGE_EJECT_BUSY",
                requested_at,
                {"storage_id": storage_id, "active_transactions": active},
            )
            return EjectResult(
                storage_id=storage_id,
                request_id=request_id,
                status="BUSY",
                safe_to_remove=False,
                message=(
                    "Archive work is incomplete; run archive retry to completion "
                    "before requesting eject again."
                ),
                platform=None,
                active_transactions=tuple(active),
            )

        try:
            _fsync_archive_directories(root)
            self._catalog.checkpoint()
        except (CatalogStateError, OSError) as exc:
            completed_at = self._utc_clock_ns()
            self._catalog.finish_storage_eject(
                storage_id=storage_id,
                request_id=request_id,
                succeeded=False,
                occurred_at_utc_ns=completed_at,
                evidence={"error": str(exc), "safe_to_remove": False},
            )
            self._record(
                request_id,
                "STORAGE_EJECT_ERROR",
                completed_at,
                {"storage_id": storage_id, "error": str(exc)},
            )
            raise EjectError(f"safe eject preparation failed: {exc}") from exc
        try:
            platform_result = self._platform.request_eject(
                volume, timeout_seconds=timeout_seconds
            )
        except (OSError, PlatformVolumeError, ValueError) as exc:
            completed_at = self._utc_clock_ns()
            self._catalog.retain_storage_eject_pending(
                storage_id=storage_id,
                request_id=request_id,
                occurred_at_utc_ns=completed_at,
                evidence={
                    "error": str(exc),
                    "safe_to_remove": False,
                    "outcome": "UNCERTAIN_AFTER_SYSTEM_REQUEST",
                },
            )
            self._record(
                request_id,
                "STORAGE_EJECT_OUTCOME_UNCERTAIN",
                completed_at,
                {"storage_id": storage_id, "error": str(exc)},
            )
            raise EjectError(
                "Disk Arbitration outcome is uncertain; archive allocation "
                f"remains blocked: {exc}"
            ) from exc

        completed_at = self._utc_clock_ns()
        platform_document = platform_result.public_dict()
        succeeded = platform_result.safe_to_remove
        forced_removal = False
        if not succeeded:
            try:
                forced_removal = not any(
                    item.volume_uuid == target["volume_uuid"]
                    for item in self._platform.inventory()
                )
            except PlatformVolumeError:
                forced_removal = False
        if platform_result.failed_stage == "timeout" and not forced_removal:
            self._catalog.retain_storage_eject_pending(
                storage_id=storage_id,
                request_id=request_id,
                occurred_at_utc_ns=completed_at,
                evidence={
                    **platform_document,
                    "outcome": "UNCERTAIN_AFTER_TIMEOUT",
                },
            )
        else:
            self._catalog.finish_storage_eject(
                storage_id=storage_id,
                request_id=request_id,
                succeeded=succeeded,
                occurred_at_utc_ns=completed_at,
                evidence=platform_document,
            )
        event_type = (
            "STORAGE_EJECT_SUCCEEDED"
            if succeeded
            else (
                "STORAGE_FORCED_REMOVAL"
                if forced_removal
                else (
                    "STORAGE_EJECT_TIMEOUT"
                    if platform_result.failed_stage == "timeout"
                    else "STORAGE_EJECT_REFUSED"
                )
            )
        )
        self._record(
            request_id,
            event_type,
            completed_at,
            {"storage_id": storage_id, **platform_document},
        )
        if succeeded:
            return EjectResult(
                storage_id=storage_id,
                request_id=request_id,
                status=StorageState.SAFE_TO_REMOVE.value,
                safe_to_remove=True,
                message=(
                    "Disk Arbitration confirmed unmount and eject; "
                    "safe to remove (可以拔出)."
                ),
                platform=platform_document,
            )
        if forced_removal:
            return EjectResult(
                storage_id=storage_id,
                request_id=request_id,
                status="FORCED_REMOVAL",
                safe_to_remove=False,
                message=(
                    "The volume disappeared without successful eject confirmation; "
                    "internal source data was preserved."
                ),
                platform=platform_document,
                forced_removal=True,
            )
        if platform_result.failed_stage == "timeout":
            return EjectResult(
                storage_id=storage_id,
                request_id=request_id,
                status="EJECT_TIMEOUT",
                safe_to_remove=False,
                message=(
                    "Disk Arbitration did not confirm completion before timeout; "
                    "archive allocation remains blocked until an explicit retry."
                ),
                platform=platform_document,
            )
        return EjectResult(
            storage_id=storage_id,
            request_id=request_id,
            status="EJECT_REFUSED",
            safe_to_remove=False,
            message=(
                "Disk Arbitration did not confirm both unmount and eject; "
                "do not treat the device as safe to remove."
            ),
            platform=platform_document,
        )

    def _record(
        self,
        request_id: str,
        event_type: str,
        occurred_at_utc_ns: int,
        evidence: dict[str, object],
    ) -> None:
        self._catalog.record_operational_event(
            event_id=f"storage-eject:{request_id}:{event_type}",
            event_type=event_type,
            occurred_at_utc_ns=occurred_at_utc_ns,
            evidence=evidence,
        )


def _fsync_archive_directories(root: Path) -> None:
    for path in (root / "raw", root / "manifests", root):
        if not path.exists():
            continue
        if not path.is_dir():
            raise OSError(f"archive path is not a directory: {path.name}")
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
