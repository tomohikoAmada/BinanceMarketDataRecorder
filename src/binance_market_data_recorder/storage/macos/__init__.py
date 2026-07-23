"""macOS volume discovery and registered-directory storage support."""

from .model import StorageState, VolumeInfo, VolumeLifecycleEvent
from .registry import (
    StorageRegistrationError,
    StorageRegistry,
    inspect_path,
    validate_registered_root,
)
from .volumes import DiskArbitrationAdapter, PlatformVolumeError

__all__ = [
    "DiskArbitrationAdapter",
    "PlatformVolumeError",
    "StorageRegistrationError",
    "StorageRegistry",
    "StorageState",
    "VolumeInfo",
    "VolumeLifecycleEvent",
    "inspect_path",
    "validate_registered_root",
]
