"""Public storage states and platform-neutral volume observations."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal


class StorageState(StrEnum):
    ABSENT = "ABSENT"
    PRESENT_UNMOUNTED = "PRESENT_UNMOUNTED"
    MOUNTED = "MOUNTED"
    UNREGISTERED = "UNREGISTERED"
    PROBING = "PROBING"
    READY = "READY"
    READ_ONLY = "READ_ONLY"
    LOW_SPACE = "LOW_SPACE"
    COPYING = "COPYING"
    VERIFYING = "VERIFYING"
    EJECT_PENDING = "EJECT_PENDING"
    DISAPPEARED_DURING_COPY = "DISAPPEARED_DURING_COPY"
    DEGRADED = "DEGRADED"
    ERROR = "ERROR"


@dataclass(frozen=True, slots=True)
class VolumeInfo:
    disk_id: str
    volume_uuid: str
    name: str | None
    filesystem_type: str | None
    mountpoint: Path | None
    writable: bool | None
    internal: bool | None
    removable: bool | None
    total_bytes: int | None
    free_bytes: int | None
    observed_at_utc_ns: int

    @property
    def mounted(self) -> bool:
        return self.mountpoint is not None

    def public_dict(self, *, state: StorageState | None = None) -> dict[str, object]:
        observed_state = state
        if observed_state is None:
            if not self.mounted:
                observed_state = StorageState.PRESENT_UNMOUNTED
            elif self.writable is False:
                observed_state = StorageState.READ_ONLY
            else:
                observed_state = StorageState.UNREGISTERED
        return {
            "disk_id": self.disk_id,
            "volume_uuid": self.volume_uuid,
            "volume_name": self.name,
            "filesystem_type": self.filesystem_type,
            "mountpoint": str(self.mountpoint) if self.mountpoint else None,
            "mounted": self.mounted,
            "writable": self.writable,
            "internal": self.internal,
            "removable": self.removable,
            "total_bytes": self.total_bytes,
            "free_bytes": self.free_bytes,
            "observed_at_utc_ns": self.observed_at_utc_ns,
            "state": observed_state.value,
        }


@dataclass(frozen=True, slots=True)
class VolumeLifecycleEvent:
    kind: Literal["appeared", "changed", "disappeared"]
    volume: VolumeInfo
