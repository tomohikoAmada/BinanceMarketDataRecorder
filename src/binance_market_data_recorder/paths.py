"""Platform paths and persistent-data safety checks."""

from __future__ import annotations

import sys
from pathlib import Path

APPLICATION_DIRECTORY = "BinanceMarketDataRecorder"
REPOSITORY_DIRECTORY = "BinanceMarketDataRecorder"


class UnsafeDataRootError(ValueError):
    """Raised when a persistent data root violates the storage contract."""

    def __init__(self, path: Path, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"unsafe data root ({reason}): {path}")


def default_data_root(
    *,
    home: Path | None = None,
    platform: str | None = None,
) -> Path:
    """Return the non-creating interactive data path for the current platform."""

    base = home if home is not None else Path.home()
    selected_platform = sys.platform if platform is None else platform
    if selected_platform == "darwin":
        return base / "Library" / "Application Support" / APPLICATION_DIRECTORY
    if selected_platform.startswith("linux"):
        return base / ".local" / "share" / APPLICATION_DIRECTORY
    raise RuntimeError(f"unsupported platform: {selected_platform}")


def discover_repository_root(start: Path | None = None) -> Path | None:
    """Find the nearest Git worktree without invoking Git or changing state."""

    candidates = [start or Path.cwd(), Path(__file__).resolve()]
    for candidate in candidates:
        current = candidate if candidate.is_dir() else candidate.parent
        for directory in (current, *current.parents):
            if (directory / ".git").exists():
                return directory.resolve()
    return None


def _expand_home(path: Path, home: Path) -> Path:
    text = str(path)
    if text == "~":
        return home
    if text.startswith("~/"):
        return home / text[2:]
    return path


def _is_within(path: Path, parent: Path) -> bool:
    return path == parent or path.is_relative_to(parent)


def validate_data_root(
    path: str | Path,
    *,
    repository_root: Path | None = None,
    home: Path | None = None,
) -> Path:
    """Normalize and validate a persistent data root without creating it."""

    selected_home = (home if home is not None else Path.home()).resolve()
    candidate = _expand_home(Path(path), selected_home)
    if not candidate.is_absolute():
        raise UnsafeDataRootError(candidate, "path_must_be_absolute")
    resolved = candidate.resolve(strict=False)

    if resolved == Path("/"):
        raise UnsafeDataRootError(resolved, "forbidden_persistent_location:/")

    if resolved == selected_home:
        raise UnsafeDataRootError(resolved, f"forbidden_persistent_location:{selected_home}")

    forbidden_roots = {
        (selected_home / "Desktop").resolve(strict=False),
        (selected_home / "Documents").resolve(strict=False),
        (selected_home / "Library" / "Mobile Documents").resolve(strict=False),
        (selected_home / "Library" / "CloudStorage").resolve(strict=False),
        Path("/tmp").resolve(strict=False),
        Path("/private/tmp").resolve(strict=False),
    }
    for forbidden in forbidden_roots:
        if _is_within(resolved, forbidden):
            raise UnsafeDataRootError(resolved, f"forbidden_persistent_location:{forbidden}")

    detected_repository = (
        repository_root.resolve() if repository_root else discover_repository_root()
    )
    if detected_repository is not None:
        workspace = detected_repository.parent
        if _is_within(resolved, detected_repository):
            raise UnsafeDataRootError(resolved, "repository_or_workspace")
        # A common Linux checkout is directly under $HOME. Treating all of
        # $HOME as the workspace would reject the XDG default by construction.
        if workspace != selected_home and _is_within(resolved, workspace):
            raise UnsafeDataRootError(resolved, "repository_or_workspace")

    for directory in (resolved, *resolved.parents):
        if (directory / ".git").exists():
            raise UnsafeDataRootError(resolved, "inside_git_repository")

    return resolved
