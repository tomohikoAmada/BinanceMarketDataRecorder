"""Select the native read-only volume adapter without importing PyObjC on Linux."""

from __future__ import annotations

import sys

from .linux import LinuxVolumeAdapter
from .macos import DiskArbitrationAdapter


def volume_adapter() -> DiskArbitrationAdapter | LinuxVolumeAdapter:
    if sys.platform == "darwin":
        return DiskArbitrationAdapter()
    if sys.platform.startswith("linux"):
        return LinuxVolumeAdapter()
    raise RuntimeError(f"unsupported storage platform: {sys.platform}")
