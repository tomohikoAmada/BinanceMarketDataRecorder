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
    checkpoints: Path
    quarantine: Path
    reports: Path
    daily_reports: Path
    state: Path
    catalog: Path

    def relative(self, path: Path) -> str:
        return path.resolve().relative_to(self.root.resolve()).as_posix()

    @classmethod
    def from_root(cls, root: Path) -> StorageLayout:
        """Derive the internal layout WITHOUT any filesystem mutation.

        Read-only tools (the legacy reconnect preflight) use this instead of
        ``ensure_storage_layout``: they must never mkdir, touch, chmod, or
        fsync-create a missing directory.  Callers validate required paths
        explicitly.
        """
        root = root.resolve()
        data = root / "data"
        return cls(
            root=root,
            active=data / "active",
            sealed=data / "sealed",
            manifests=data / "manifests",
            checkpoints=data / "checkpoints",
            quarantine=data / "quarantine",
            reports=data / "reports",
            daily_reports=data / "reports" / "daily",
            state=root / "state",
            catalog=root / "state" / "catalog.sqlite",
        )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def ensure_storage_layout(root: Path) -> StorageLayout:
    """Create only the Recorder-owned internal directories below a selected root."""

    layout = StorageLayout.from_root(root)
    data = root.resolve() / "data"
    for directory in (
        layout.root,
        data,
        layout.active,
        layout.sealed,
        layout.manifests,
        layout.checkpoints,
        layout.quarantine,
        layout.reports,
        layout.daily_reports,
        layout.state,
    ):
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    _fsync_directory(layout.root)
    _fsync_directory(data)
    return layout


def fsync_directory(path: Path) -> None:
    _fsync_directory(path)
