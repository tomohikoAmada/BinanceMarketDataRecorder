"""Read-only Disk Arbitration discovery for external macOS volumes."""

from __future__ import annotations

import importlib
import os
import shutil
import sys
import time
from collections.abc import Callable
from pathlib import Path
from threading import Event
from types import ModuleType
from typing import Any, Literal, cast

from .model import PlatformEjectResult, VolumeInfo, VolumeLifecycleEvent


class PlatformVolumeError(RuntimeError):
    """Disk Arbitration is unavailable or returned unusable evidence."""


def _load_frameworks() -> tuple[ModuleType, ModuleType, ModuleType]:
    if sys.platform != "darwin":
        raise PlatformVolumeError("Disk Arbitration discovery requires macOS")
    try:
        return (
            importlib.import_module("DiskArbitration"),
            importlib.import_module("CoreFoundation"),
            importlib.import_module("objc"),
        )
    except ImportError as exc:
        raise PlatformVolumeError(
            "pyobjc-framework-DiskArbitration is required on macOS"
        ) from exc


class DiskArbitrationAdapter:
    """Observe external volumes and request non-forced system eject."""

    def __init__(self, *, startup_window_seconds: float = 0.35) -> None:
        if startup_window_seconds <= 0:
            raise ValueError("startup discovery window must be positive")
        self._startup_window_seconds = startup_window_seconds

    @staticmethod
    def capability() -> dict[str, object]:
        try:
            disk_arbitration, _, _ = _load_frameworks()
            session = disk_arbitration.DASessionCreate(None)
        except PlatformVolumeError as exc:
            return {"available": False, "reason": str(exc)}
        return {
            "available": session is not None,
            "framework": "DiskArbitration",
            "binding": "PyObjC",
        }

    @staticmethod
    def callback_probe(*, duration_seconds: float = 0.15) -> dict[str, object]:
        """Prove callback bridging and startup delivery without mutating any disk."""

        if duration_seconds <= 0:
            raise ValueError("callback probe duration must be positive")
        da, cf, objc = _load_frameworks()
        observed = 0

        @objc.callbackFor(da.DARegisterDiskAppearedCallback)  # type: ignore[untyped-decorator]
        def appeared(_disk: object, _context: object) -> None:
            nonlocal observed
            observed += 1

        @objc.callbackFor(da.DARegisterDiskDisappearedCallback)  # type: ignore[untyped-decorator]
        def disappeared(_disk: object, _context: object) -> None:
            return None

        session = da.DASessionCreate(None)
        if session is None:
            raise PlatformVolumeError("DASessionCreate returned no session")
        run_loop = cf.CFRunLoopGetCurrent()
        da.DARegisterDiskAppearedCallback(session, None, appeared, None)
        da.DARegisterDiskDisappearedCallback(session, None, disappeared, None)
        da.DASessionScheduleWithRunLoop(session, run_loop, cf.kCFRunLoopDefaultMode)
        try:
            cf.CFRunLoopRunInMode(
                cf.kCFRunLoopDefaultMode, duration_seconds, False
            )
        finally:
            da.DASessionUnscheduleFromRunLoop(
                session, run_loop, cf.kCFRunLoopDefaultMode
            )
        return {
            "session_created": True,
            "appeared_callback_registered": True,
            "disappeared_callback_registered": True,
            "startup_objects_observed": observed,
            "filesystem_mutated": False,
        }

    def inventory(self) -> list[VolumeInfo]:
        """Collect startup callbacks; Disk Arbitration sends current disks on schedule."""

        da, cf, objc = _load_frameworks()
        observations: dict[str, VolumeInfo] = {}

        @objc.callbackFor(da.DARegisterDiskAppearedCallback)  # type: ignore[untyped-decorator]
        def appeared(disk: object, _context: object) -> None:
            volume = self._volume_from_disk(da, cf, disk)
            if volume is not None:
                observations[volume.disk_id] = volume

        session = da.DASessionCreate(None)
        if session is None:
            raise PlatformVolumeError("DASessionCreate returned no session")
        run_loop = cf.CFRunLoopGetCurrent()
        da.DARegisterDiskAppearedCallback(session, None, appeared, None)
        da.DASessionScheduleWithRunLoop(session, run_loop, cf.kCFRunLoopDefaultMode)
        try:
            cf.CFRunLoopRunInMode(
                cf.kCFRunLoopDefaultMode,
                self._startup_window_seconds,
                False,
            )
        finally:
            da.DASessionUnscheduleFromRunLoop(
                session, run_loop, cf.kCFRunLoopDefaultMode
            )
        return sorted(observations.values(), key=lambda volume: volume.disk_id)

    def observe(
        self,
        callback: Callable[[VolumeLifecycleEvent], None],
        stop_event: Event,
        *,
        run_loop_slice_seconds: float = 0.25,
    ) -> None:
        """Deliver appeared/changed/disappeared callbacks until ``stop_event`` is set."""

        if run_loop_slice_seconds <= 0:
            raise ValueError("run-loop slice must be positive")
        da, cf, objc = _load_frameworks()
        cache: dict[str, VolumeInfo] = {}

        @objc.callbackFor(da.DARegisterDiskAppearedCallback)  # type: ignore[untyped-decorator]
        def appeared(disk: object, _context: object) -> None:
            volume = self._volume_from_disk(da, cf, disk)
            if volume is not None:
                cache[volume.disk_id] = volume
                callback(VolumeLifecycleEvent("appeared", volume))

        @objc.callbackFor(da.DARegisterDiskDescriptionChangedCallback)  # type: ignore[untyped-decorator]
        def changed(disk: object, _keys: object, _context: object) -> None:
            volume = self._volume_from_disk(da, cf, disk)
            if volume is not None:
                cache[volume.disk_id] = volume
                callback(VolumeLifecycleEvent("changed", volume))

        @objc.callbackFor(da.DARegisterDiskDisappearedCallback)  # type: ignore[untyped-decorator]
        def disappeared(disk: object, _context: object) -> None:
            disk_id = self._disk_id(da, disk)
            volume = cache.pop(disk_id, None)
            if volume is None:
                volume = self._volume_from_disk(da, cf, disk)
            if volume is not None:
                callback(VolumeLifecycleEvent("disappeared", volume))

        session = da.DASessionCreate(None)
        if session is None:
            raise PlatformVolumeError("DASessionCreate returned no session")
        run_loop = cf.CFRunLoopGetCurrent()
        da.DARegisterDiskAppearedCallback(session, None, appeared, None)
        da.DARegisterDiskDescriptionChangedCallback(session, None, None, changed, None)
        da.DARegisterDiskDisappearedCallback(session, None, disappeared, None)
        da.DASessionScheduleWithRunLoop(session, run_loop, cf.kCFRunLoopDefaultMode)
        try:
            while not stop_event.is_set():
                cf.CFRunLoopRunInMode(
                    cf.kCFRunLoopDefaultMode, run_loop_slice_seconds, False
                )
        finally:
            da.DASessionUnscheduleFromRunLoop(
                session, run_loop, cf.kCFRunLoopDefaultMode
            )

    def request_eject(
        self, volume: VolumeInfo, *, timeout_seconds: float = 30.0
    ) -> PlatformEjectResult:
        """Unmount one volume and eject its media only after system callbacks succeed."""

        if timeout_seconds <= 0:
            raise ValueError("eject timeout must be positive")
        if volume.internal is not False:
            raise PlatformVolumeError("refusing to eject a non-external volume")
        if not volume.disk_id or volume.disk_id == "unknown":
            raise PlatformVolumeError("volume lacks a usable BSD disk identifier")
        da, cf, objc = _load_frameworks()
        session = da.DASessionCreate(None)
        if session is None:
            raise PlatformVolumeError("DASessionCreate returned no session")
        disk = da.DADiskCreateFromBSDName(None, session, volume.disk_id.encode("utf-8"))
        if disk is None:
            raise PlatformVolumeError(
                f"cannot create Disk Arbitration object for {volume.disk_id}"
            )
        whole_disk = da.DADiskCopyWholeDisk(disk)
        if whole_disk is None:
            raise PlatformVolumeError(
                f"cannot resolve whole-disk media for {volume.disk_id}"
            )
        run_loop = cf.CFRunLoopGetCurrent()
        state: dict[str, object] = {
            "unmounted": volume.mountpoint is None,
            "ejected": False,
            "failed_stage": None,
            "dissenter_status": None,
            "dissenter_message": None,
            "complete": False,
        }

        def finish(stage: str, dissenter: object | None) -> None:
            state["complete"] = True
            state["failed_stage"] = stage
            if dissenter is not None:
                state["dissenter_status"] = int(da.DADissenterGetStatus(dissenter))
                message = da.DADissenterGetStatusString(dissenter)
                state["dissenter_message"] = str(message) if message is not None else None
            cf.CFRunLoopStop(run_loop)

        @objc.callbackFor(da.DADiskEject)  # type: ignore[untyped-decorator]
        def ejected(
            _disk: object, dissenter: object | None, _context: object
        ) -> None:
            if dissenter is not None:
                finish("eject", dissenter)
                return
            state["ejected"] = True
            state["complete"] = True
            cf.CFRunLoopStop(run_loop)

        @objc.callbackFor(da.DADiskUnmount)  # type: ignore[untyped-decorator]
        def unmounted(
            _callback_disk: object,
            dissenter: object | None,
            _context: object,
        ) -> None:
            if dissenter is not None:
                finish("unmount", dissenter)
                return
            state["unmounted"] = True
            da.DADiskEject(
                whole_disk,
                da.kDADiskEjectOptionDefault,
                ejected,
                None,
            )

        da.DASessionScheduleWithRunLoop(session, run_loop, cf.kCFRunLoopDefaultMode)
        try:
            if volume.mountpoint is None:
                da.DADiskEject(
                    whole_disk,
                    da.kDADiskEjectOptionDefault,
                    ejected,
                    None,
                )
            else:
                da.DADiskUnmount(
                    disk,
                    da.kDADiskUnmountOptionDefault,
                    unmounted,
                    None,
                )
            deadline = time.monotonic() + timeout_seconds
            while not bool(state["complete"]):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    state["failed_stage"] = "timeout"
                    break
                cf.CFRunLoopRunInMode(
                    cf.kCFRunLoopDefaultMode,
                    min(remaining, 0.25),
                    False,
                )
        finally:
            da.DASessionUnscheduleFromRunLoop(
                session, run_loop, cf.kCFRunLoopDefaultMode
            )
        failed_stage = state["failed_stage"]
        if failed_stage not in {None, "unmount", "eject", "timeout"}:
            raise PlatformVolumeError("invalid Disk Arbitration eject outcome")
        return PlatformEjectResult(
            disk_id=volume.disk_id,
            unmounted=bool(state["unmounted"]),
            ejected=bool(state["ejected"]),
            failed_stage=cast(
                Literal["unmount", "eject", "timeout"] | None, failed_stage
            ),
            dissenter_status=cast(int | None, state["dissenter_status"]),
            dissenter_message=cast(str | None, state["dissenter_message"]),
        )

    @staticmethod
    def _disk_id(da: ModuleType, disk: object) -> str:
        value = da.DADiskGetBSDName(disk)
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            return value
        return "unknown"

    @classmethod
    def _volume_from_disk(
        cls, da: ModuleType, cf: ModuleType, disk: object
    ) -> VolumeInfo | None:
        description = cast(dict[object, Any], da.DADiskCopyDescription(disk) or {})
        internal_value = description.get(da.kDADiskDescriptionDeviceInternalKey)
        if internal_value is not False:
            return None
        uuid_value = description.get(da.kDADiskDescriptionVolumeUUIDKey)
        if uuid_value is None:
            return None
        volume_uuid = str(cf.CFUUIDCreateString(None, uuid_value)).upper()
        path_value = description.get(da.kDADiskDescriptionVolumePathKey)
        mountpoint = None
        if path_value is not None and hasattr(path_value, "path"):
            mountpoint = Path(str(path_value.path())).resolve()

        media_writable = description.get(da.kDADiskDescriptionMediaWritableKey)
        writable = bool(media_writable) if media_writable is not None else None
        total_bytes = _optional_int(
            description.get(da.kDADiskDescriptionMediaSizeKey)
        )
        free_bytes: int | None = None
        if mountpoint is not None:
            try:
                stat = os.statvfs(mountpoint)
                read_only = bool(stat.f_flag & getattr(os, "ST_RDONLY", 1))
                writable = bool(writable) and not read_only
                usage = shutil.disk_usage(mountpoint)
                total_bytes = usage.total
                free_bytes = usage.free
            except OSError:
                writable = False

        return VolumeInfo(
            disk_id=cls._disk_id(da, disk),
            volume_uuid=volume_uuid,
            name=_optional_text(description.get(da.kDADiskDescriptionVolumeNameKey)),
            filesystem_type=_optional_text(
                description.get(da.kDADiskDescriptionVolumeKindKey)
            ),
            mountpoint=mountpoint,
            writable=writable,
            internal=False,
            removable=_optional_bool(
                description.get(da.kDADiskDescriptionMediaRemovableKey)
            ),
            total_bytes=total_bytes,
            free_bytes=free_bytes,
            observed_at_utc_ns=time.time_ns(),
        )


def _optional_text(value: object) -> str | None:
    return str(value) if value is not None else None


def _optional_int(value: object) -> int | None:
    return int(value) if isinstance(value, int | float) else None


def _optional_bool(value: object) -> bool | None:
    return bool(value) if value is not None else None
