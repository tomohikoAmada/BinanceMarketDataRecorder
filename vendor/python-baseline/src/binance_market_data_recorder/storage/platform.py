"""Select the native read-only volume adapter without importing PyObjC on Linux."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .linux import LinuxVolumeAdapter
    from .macos import DiskArbitrationAdapter


def volume_adapter() -> DiskArbitrationAdapter | LinuxVolumeAdapter:
    if sys.platform == "darwin":
        from .macos import DiskArbitrationAdapter

        return DiskArbitrationAdapter()
    if sys.platform.startswith("linux"):
        from .linux import LinuxVolumeAdapter

        return LinuxVolumeAdapter()
    raise RuntimeError(f"unsupported storage platform: {sys.platform}")
