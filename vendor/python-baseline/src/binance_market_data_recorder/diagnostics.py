"""Offline, non-mutating platform and path doctor checks."""

from __future__ import annotations

import os
import platform
import sys
from pathlib import Path
from typing import Any

from .config import LoadedConfig


def _nearest_existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current != current.parent:
        current = current.parent
    return current


def run_doctor(loaded: LoadedConfig, *, repository_root: Path | None = None) -> dict[str, Any]:
    """Return structured readiness evidence without creating files or directories."""

    checks: list[dict[str, object]] = []

    python_ok = sys.version_info[:2] == (3, 12)
    checks.append(
        {
            "name": "python_version",
            "status": "PASS" if python_ok else "FAIL",
            "observed": platform.python_version(),
            "required": ">=3.12,<3.13",
        }
    )

    machine = platform.machine().lower()
    certified_platform = (
        sys.platform == "darwin" and machine in {"arm64", "aarch64"}
    ) or (sys.platform.startswith("linux") and machine == "aarch64")
    checks.append(
        {
            "name": "certified_platform",
            "status": "PASS" if certified_platform else "WARN",
            "observed": {"system": platform.system(), "machine": platform.machine()},
            "required": "macOS Apple Silicon or Ubuntu Linux ARM64",
            "support_level": (
                "DEVELOPER_PREVIEW"
                if sys.platform.startswith("linux")
                else "SUPPORTED"
            ),
        }
    )

    data_root = loaded.config.data_root
    parent = _nearest_existing_parent(data_root)
    writable = parent.is_dir() and os.access(parent, os.W_OK | os.X_OK)
    checks.append(
        {
            "name": "data_root_parent_access",
            "status": "PASS" if writable else "FAIL",
            "data_root": str(data_root),
            "nearest_existing_parent": str(parent),
            "data_root_exists": data_root.exists(),
            "mutated": False,
        }
    )

    if repository_root is not None:
        resolved_repository = repository_root.resolve()
        workspace = resolved_repository.parent
        separate = not data_root.is_relative_to(resolved_repository) and (
            workspace == Path.home().resolve()
            or not data_root.is_relative_to(workspace)
        )
        checks.append(
            {
                "name": "data_root_outside_repository_workspace",
                "status": "PASS" if separate else "FAIL",
                "repository_root": str(repository_root.resolve()),
            }
        )

    proxy_status = loaded.config.proxy_policy().status().public_dict()
    checks.append(
        {
            "name": "proxy_policy",
            "status": "PASS",
            **proxy_status,
            "raw_proxy_url_exposed": False,
        }
    )

    statuses = {str(check["status"]) for check in checks}
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WARN" in statuses:
        overall = "PASS_WITH_WARNINGS"
    else:
        overall = "PASS"
    return {
        "command": "doctor",
        "status": overall,
        "checks": checks,
        "network_accessed": False,
        "filesystem_mutated": False,
    }
