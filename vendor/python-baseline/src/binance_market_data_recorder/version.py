"""Package and source revision information."""

from __future__ import annotations

import os
import subprocess
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from .paths import discover_repository_root

DIST_NAME = "binance-market-data-recorder"
FALLBACK_VERSION = "0.1.0a1"
GIT_COMMIT_ENV = "BINANCE_MARKET_RECORDER_GIT_COMMIT"


def package_version() -> str:
    try:
        return version(DIST_NAME)
    except PackageNotFoundError:
        return FALLBACK_VERSION


__version__ = package_version()


def git_commit(*, repository_root: Path | None = None) -> str | None:
    injected = os.environ.get(GIT_COMMIT_ENV)
    if injected:
        return injected
    root = repository_root or discover_repository_root()
    if root is None:
        return None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = result.stdout.strip()
    return commit or None


def version_string() -> str:
    commit = git_commit() or "unknown"
    return f"{DIST_NAME} {__version__} (git {commit})"
