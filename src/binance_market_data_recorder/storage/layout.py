"""Internal directory layout used by the Raw spool."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StorageLayout:
    root: Path
    active: Path
    sealed: Path
    manifests: Path
    quarantine: Path
    state: Path
    catalog: Path

    def relative(self, path: Path) -> str:
        return path.resolve().relative_to(self.root.resolve()).as_posix()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def ensure_storage_layout(root: Path) -> StorageLayout:
    """Create only the Recorder-owned internal directories below a selected root."""

    root = root.resolve()
    data = root / "data"
    layout = StorageLayout(
        root=root,
        active=data / "active",
        sealed=data / "sealed",
        manifests=data / "manifests",
        quarantine=data / "quarantine",
        state=root / "state",
        catalog=root / "state" / "catalog.sqlite",
    )
    for directory in (
        layout.root,
        data,
        layout.active,
        layout.sealed,
        layout.manifests,
        layout.quarantine,
        layout.state,
    ):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    _fsync_directory(layout.root)
    _fsync_directory(data)
    return layout


def fsync_directory(path: Path) -> None:
    _fsync_directory(path)
