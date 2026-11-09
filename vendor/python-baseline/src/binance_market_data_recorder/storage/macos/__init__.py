"""macOS volume discovery and registered-directory storage support."""

from .eject import EjectError, EjectResult, SafeEjectCoordinator
from .model import PlatformEjectResult, StorageState, VolumeInfo, VolumeLifecycleEvent
from .registry import (
    StorageRegistrationError,
    StorageRegistry,
    inspect_path,
    validate_registered_root,
)
from .volumes import DiskArbitrationAdapter, PlatformVolumeError

__all__ = [
    "DiskArbitrationAdapter",
    "EjectError",
    "EjectResult",
    "PlatformEjectResult",
    "PlatformVolumeError",
    "SafeEjectCoordinator",
    "StorageRegistrationError",
    "StorageRegistry",
    "StorageState",
    "VolumeInfo",
    "VolumeLifecycleEvent",
    "inspect_path",
    "validate_registered_root",
]
