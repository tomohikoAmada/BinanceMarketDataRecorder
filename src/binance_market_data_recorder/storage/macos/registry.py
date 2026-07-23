"""Registered external-directory identity, capability probe, and status resolution."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from ..catalog import Catalog
from ..forecast import SpaceSeverity, space_severity
from .model import StorageState, VolumeInfo

MARKER_NAME = ".binance-market-data-recorder-storage.json"
MARKER_SCHEMA = "registered-storage.v1"
PROBE_PREFIX = ".binance-market-data-recorder-probe-"


class StorageRegistrationError(RuntimeError):
    """Registration or readiness evidence is invalid."""


class VolumeInventory(Protocol):
    def inventory(self) -> list[VolumeInfo]: ...


class StorageRegistry:
    def __init__(self, *, catalog: Catalog, volumes: VolumeInventory) -> None:
        self._catalog = catalog
        self._volumes = volumes

    def list_volumes(self) -> list[dict[str, object]]:
        return [volume.public_dict() for volume in self._volumes.inventory()]

    def inspect(self, path: Path) -> dict[str, object]:
        return inspect_path(path, self._volumes.inventory())

    def register(self, path: Path) -> dict[str, object]:
        folder = path.expanduser().resolve()
        volume = self._containing_volume(folder, self._volumes.inventory())
        reason = self._registrable_reason(folder, volume)
        if reason is not None or volume is None or volume.mountpoint is None:
            raise StorageRegistrationError(reason or "volume is unavailable")
        relative_path = folder.relative_to(volume.mountpoint).as_posix()
        probe = probe_directory(folder)
        marker_path = folder / MARKER_NAME
        existing = _read_marker(marker_path) if marker_path.exists() else None
        catalog_target = self._catalog.storage_target_for_location(
            volume_uuid=volume.volume_uuid, relative_path=relative_path
        )
        if existing is None and catalog_target is not None:
            raise StorageRegistrationError(
                "Catalog registration exists but its marker is missing"
            )
        if existing is not None:
            _validate_marker(
                existing,
                volume_uuid=volume.volume_uuid,
                relative_path=relative_path,
            )
            storage_id = _required_marker_text(existing, "storage_id")
            marker_nonce = _required_marker_text(existing, "marker_nonce")
            if (
                catalog_target is not None
                and catalog_target["storage_id"] != storage_id
            ):
                raise StorageRegistrationError(
                    "Catalog and marker storage_id do not agree"
                )
            created_value = existing.get("created_at_utc_ns")
            if not isinstance(created_value, int) or isinstance(created_value, bool):
                raise StorageRegistrationError(
                    "storage marker missing created_at_utc_ns"
                )
            created_at_utc_ns = created_value
        else:
            storage_id = str(uuid.uuid4())
            marker_nonce = uuid.uuid4().hex
            created_at_utc_ns = time.time_ns()
            marker = {
                "schema": MARKER_SCHEMA,
                "storage_id": storage_id,
                "marker_nonce": marker_nonce,
                "volume_uuid": volume.volume_uuid,
                "registered_relative_path": relative_path,
                "created_at_utc_ns": created_at_utc_ns,
            }
            _write_marker(marker_path, marker)
        self._catalog.register_storage_target(
            storage_id=storage_id,
            volume_uuid=volume.volume_uuid,
            volume_name=volume.name,
            filesystem_type=volume.filesystem_type,
            relative_path=relative_path,
            marker_nonce=marker_nonce,
            registered_at_utc_ns=created_at_utc_ns,
        )
        usage = shutil.disk_usage(folder)
        severity = space_severity(usage.total, usage.free)
        return {
            "storage_id": storage_id,
            "state": (
                StorageState.READY.value
                if severity is SpaceSeverity.OK
                else StorageState.LOW_SPACE.value
            ),
            "space_severity": severity.value,
            "free_bytes": usage.free,
            "total_bytes": usage.total,
            "volume_uuid": volume.volume_uuid,
            "volume_name": volume.name,
            "filesystem_type": volume.filesystem_type,
            "registered_relative_path": relative_path,
            "resolved_path": str(folder),
            "marker_path": str(marker_path),
            "probe": probe,
            "observed_at_utc_ns": time.time_ns(),
        }

    def unregister(self, storage_id: str) -> dict[str, object]:
        removed = self._catalog.unregister_storage_target(storage_id)
        if not removed:
            raise StorageRegistrationError(f"unknown storage_id: {storage_id}")
        return {
            "storage_id": storage_id,
            "status": "UNREGISTERED",
            "marker_deleted": False,
            "archive_data_deleted": False,
        }

    def statuses(self) -> list[dict[str, object]]:
        volumes = {volume.volume_uuid: volume for volume in self._volumes.inventory()}
        return [self._resolve_target(target, volumes) for target in self._catalog.storage_targets()]

    def _resolve_target(
        self,
        target: dict[str, object],
        volumes: dict[str, VolumeInfo],
    ) -> dict[str, object]:
        storage_id = str(target["storage_id"])
        volume_uuid = str(target["volume_uuid"])
        base: dict[str, object] = {
            "storage_id": storage_id,
            "volume_uuid": volume_uuid,
            "registered_relative_path": str(target["relative_path"]),
            "observed_volume_name": target["volume_name"],
            "observed_filesystem_type": target["filesystem_type"],
            "observed_at_utc_ns": time.time_ns(),
        }
        control = self._catalog.storage_control(storage_id)
        control_state = str(control["state"])
        base["control"] = control
        volume = volumes.get(volume_uuid)
        if volume is None:
            if control_state == StorageState.SAFE_TO_REMOVE.value:
                return {
                    **base,
                    "state": StorageState.SAFE_TO_REMOVE.value,
                    "resolved_path": None,
                }
            return {**base, "state": StorageState.ABSENT.value, "resolved_path": None}
        base["current_volume"] = volume.public_dict()
        if volume.mountpoint is None:
            return {
                **base,
                "state": (
                    StorageState.SAFE_TO_REMOVE.value
                    if control_state == StorageState.SAFE_TO_REMOVE.value
                    else StorageState.PRESENT_UNMOUNTED.value
                ),
                "resolved_path": None,
            }
        relative_path = str(target["relative_path"])
        folder = (volume.mountpoint / relative_path).resolve()
        base["resolved_path"] = str(folder)
        if control_state == StorageState.EJECT_PENDING.value:
            return {**base, "state": StorageState.EJECT_PENDING.value}
        try:
            resolved_relative = folder.relative_to(volume.mountpoint).as_posix()
        except ValueError:
            resolved_relative = ""
        if resolved_relative != relative_path:
            return {
                **base,
                "state": StorageState.ERROR.value,
                "reason": "registered path now resolves through a different alias",
            }
        if volume.writable is False:
            return {**base, "state": StorageState.READ_ONLY.value}
        if not folder.is_dir():
            return {**base, "state": StorageState.DEGRADED.value, "reason": "directory_missing"}
        try:
            marker = _read_marker(folder / MARKER_NAME)
            _validate_marker(
                marker,
                volume_uuid=volume_uuid,
                relative_path=str(target["relative_path"]),
                storage_id=storage_id,
                marker_nonce=str(target["marker_nonce"]),
            )
            probe = probe_directory(folder)
        except (OSError, ValueError, StorageRegistrationError) as exc:
            return {
                **base,
                "state": StorageState.ERROR.value,
                "reason": str(exc),
            }
        usage = shutil.disk_usage(folder)
        severity = space_severity(usage.total, usage.free)
        self._catalog.activate_storage_target(
            storage_id, occurred_at_utc_ns=time.time_ns()
        )
        return {
            **base,
            "state": (
                StorageState.READY.value
                if severity is SpaceSeverity.OK
                else StorageState.LOW_SPACE.value
            ),
            "space_severity": severity.value,
            "free_bytes": usage.free,
            "total_bytes": usage.total,
            "probe": probe,
        }

    @staticmethod
    def _containing_volume(path: Path, volumes: Sequence[VolumeInfo]) -> VolumeInfo | None:
        candidates: list[VolumeInfo] = []
        for volume in volumes:
            if volume.mountpoint is None:
                continue
            try:
                path.relative_to(volume.mountpoint)
            except ValueError:
                continue
            candidates.append(volume)
        return max(
            candidates,
            key=lambda volume: len(volume.mountpoint.parts) if volume.mountpoint else 0,
            default=None,
        )

    @staticmethod
    def _registrable_reason(path: Path, volume: VolumeInfo | None) -> str | None:
        if volume is None:
            return "path is not on a discovered external volume"
        if volume.internal is not False:
            return "path is not on an external volume"
        if volume.mountpoint is None:
            return "volume is not mounted"
        if path == volume.mountpoint:
            return "volume root cannot be registered"
        if not path.exists():
            return "registered folder must already exist"
        if not path.is_dir():
            return "registered path is not a directory"
        if volume.writable is False:
            return "volume is read-only"
        return None


def probe_directory(folder: Path) -> dict[str, object]:
    """Prove write/fsync/rename/reopen/readback inside exactly ``folder``."""

    token = uuid.uuid4().hex
    partial = folder / f"{PROBE_PREFIX}{token}.partial"
    renamed = folder / f"{PROBE_PREFIX}{token}.verify"
    payload = os.urandom(64)
    expected = hashlib.sha256(payload).hexdigest()
    completed = False
    try:
        descriptor = os.open(partial, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            written = os.write(descriptor, payload)
            if written != len(payload):
                raise StorageRegistrationError("probe short write")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(partial, renamed)
        _fsync_directory(folder)
        observed = renamed.read_bytes()
        if observed != payload:
            raise StorageRegistrationError("probe readback mismatch")
        completed = True
    except OSError as exc:
        raise StorageRegistrationError(f"directory capability probe failed: {exc}") from exc
    finally:
        for candidate in (partial, renamed):
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                completed = False
        try:
            _fsync_directory(folder)
        except OSError:
            completed = False
    if not completed:
        raise StorageRegistrationError("directory probe cleanup/fsync failed")
    return {
        "write": "PASS",
        "file_fsync": "PASS",
        "atomic_rename": "PASS",
        "readback": "PASS",
        "directory_fsync": "PASS",
        "payload_sha256": expected,
        "residual_files": 0,
    }


def inspect_path(path: Path, volumes: Sequence[VolumeInfo]) -> dict[str, object]:
    """Describe a path without creating a Catalog or touching the filesystem."""

    folder = path.expanduser().resolve()
    volume = StorageRegistry._containing_volume(folder, volumes)
    reason = StorageRegistry._registrable_reason(folder, volume)
    return {
        "path": str(folder),
        "exists": folder.exists(),
        "is_directory": folder.is_dir(),
        "volume": volume.public_dict() if volume else None,
        "registrable": reason is None,
        "reason": reason,
        "filesystem_mutated": False,
    }


def validate_registered_root(
    path: Path,
    *,
    volume_uuid: str,
    relative_path: str,
    storage_id: str,
    marker_nonce: str,
) -> None:
    """Revalidate the M9 identity marker before an archive filesystem action."""

    folder = path.resolve()
    if not folder.is_dir():
        raise StorageRegistrationError("registered storage directory is unavailable")
    marker = _read_marker(folder / MARKER_NAME)
    _validate_marker(
        marker,
        volume_uuid=volume_uuid,
        relative_path=relative_path,
        storage_id=storage_id,
        marker_nonce=marker_nonce,
    )


def _write_marker(path: Path, marker: dict[str, object]) -> None:
    temporary = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    encoded = (json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n").encode()
    try:
        descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            if os.write(descriptor, encoded) != len(encoded):
                raise StorageRegistrationError("marker short write")
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise StorageRegistrationError(f"cannot commit storage marker: {exc}") from exc
    finally:
        temporary.unlink(missing_ok=True)


def _read_marker(path: Path) -> dict[str, object]:
    try:
        decoded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StorageRegistrationError(f"invalid storage marker: {exc}") from exc
    if not isinstance(decoded, dict):
        raise StorageRegistrationError("invalid storage marker: expected object")
    return decoded


def _validate_marker(
    marker: dict[str, object],
    *,
    volume_uuid: str,
    relative_path: str,
    storage_id: str | None = None,
    marker_nonce: str | None = None,
) -> None:
    expected: dict[str, str] = {
        "schema": MARKER_SCHEMA,
        "volume_uuid": volume_uuid,
        "registered_relative_path": relative_path,
    }
    if storage_id is not None:
        expected["storage_id"] = storage_id
    if marker_nonce is not None:
        expected["marker_nonce"] = marker_nonce
    mismatches = [key for key, value in expected.items() if marker.get(key) != value]
    if mismatches:
        raise StorageRegistrationError(
            f"storage marker identity mismatch: {', '.join(sorted(mismatches))}"
        )
    _required_marker_text(marker, "storage_id")
    _required_marker_text(marker, "marker_nonce")
    created = marker.get("created_at_utc_ns")
    if not isinstance(created, int) or isinstance(created, bool):
        raise StorageRegistrationError("storage marker missing created_at_utc_ns")


def _required_marker_text(marker: dict[str, object], key: str) -> str:
    value = marker.get(key)
    if not isinstance(value, str) or not value:
        raise StorageRegistrationError(f"storage marker missing {key}")
    return value


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
