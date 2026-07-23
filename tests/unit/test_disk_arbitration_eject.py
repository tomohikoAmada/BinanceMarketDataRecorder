from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

from binance_market_data_recorder.storage.macos import (
    DiskArbitrationAdapter,
    VolumeInfo,
)


class FakeCoreFoundation:
    kCFRunLoopDefaultMode = "default"

    @staticmethod
    def CFRunLoopGetCurrent() -> object:
        return object()

    @staticmethod
    def CFRunLoopStop(_loop: object) -> None:
        return None

    @staticmethod
    def CFRunLoopRunInMode(
        _mode: object, _duration: float, _return_after_source_handled: bool
    ) -> None:
        return None


class FakeObjC:
    @staticmethod
    def callbackFor(_function: object) -> object:
        return lambda callback: callback


class FakeDiskArbitration:
    kDADiskUnmountOptionDefault = 0
    kDADiskEjectOptionDefault = 0

    def __init__(self, *, unmount_dissenter: object | None = None) -> None:
        self.unmount_dissenter = unmount_dissenter
        self.unmount_options: int | None = None
        self.eject_options: int | None = None
        self.bsd_name: bytes | None = None

    @staticmethod
    def DASessionCreate(_allocator: object) -> object:
        return object()

    def DADiskCreateFromBSDName(
        self, _allocator: object, _session: object, bsd_name: bytes
    ) -> object:
        self.bsd_name = bsd_name
        return object()

    @staticmethod
    def DASessionScheduleWithRunLoop(
        _session: object, _loop: object, _mode: object
    ) -> None:
        return None

    @staticmethod
    def DASessionUnscheduleFromRunLoop(
        _session: object, _loop: object, _mode: object
    ) -> None:
        return None

    def DADiskUnmount(
        self,
        disk: object,
        options: int,
        callback: Callable[[object, object | None, object], None],
        context: object,
    ) -> None:
        self.unmount_options = options
        callback(disk, self.unmount_dissenter, context)

    def DADiskEject(
        self,
        disk: object,
        options: int,
        callback: Callable[[object, object | None, object], None],
        context: object,
    ) -> None:
        self.eject_options = options
        callback(disk, None, context)

    @staticmethod
    def DADissenterGetStatus(_dissenter: object) -> int:
        return 49153

    @staticmethod
    def DADissenterGetStatusString(_dissenter: object) -> str:
        return "volume busy"


def _volume() -> VolumeInfo:
    return VolumeInfo(
        disk_id="disk9s1",
        volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        name="Archive",
        filesystem_type="apfs",
        mountpoint=Path("/Volumes/Archive"),
        writable=True,
        internal=False,
        removable=True,
        total_bytes=100,
        free_bytes=90,
        observed_at_utc_ns=1,
    )


def test_adapter_uses_only_default_non_forced_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    da = FakeDiskArbitration()
    monkeypatch.setattr(
        "binance_market_data_recorder.storage.macos.volumes._load_frameworks",
        lambda: (da, FakeCoreFoundation, FakeObjC),
    )
    result = DiskArbitrationAdapter().request_eject(_volume(), timeout_seconds=1)
    assert result.safe_to_remove is True
    assert da.bsd_name == b"disk9s1"
    assert da.unmount_options == 0
    assert da.eject_options == 0


def test_adapter_stops_on_unmount_dissenter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    da = FakeDiskArbitration(unmount_dissenter=SimpleNamespace())
    monkeypatch.setattr(
        "binance_market_data_recorder.storage.macos.volumes._load_frameworks",
        lambda: (da, FakeCoreFoundation, FakeObjC),
    )
    result = DiskArbitrationAdapter().request_eject(_volume(), timeout_seconds=1)
    assert result.safe_to_remove is False
    assert result.failed_stage == "unmount"
    assert result.dissenter_status == 49153
    assert result.dissenter_message == "volume busy"
    assert da.eject_options is None
