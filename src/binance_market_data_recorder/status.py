"""Honest structured runtime and storage status without service fabrication."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .storage.catalog import Catalog, ChunkState
from .storage.forecast import space_severity
from .storage.macos import DiskArbitrationAdapter, PlatformVolumeError, StorageRegistry


def _nearest_existing(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def service_status(data_root: Path) -> dict[str, Any]:
    """Read current evidence; missing service state remains explicitly NOT_RUNNING."""

    root = data_root.resolve()
    state_path = root / "state" / "service_state.json"
    service_state: dict[str, object] | None = None
    state_error: str | None = None
    if state_path.is_file():
        try:
            decoded = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(decoded, dict):
                service_state = decoded
            else:
                state_error = "service_state_not_object"
        except (OSError, json.JSONDecodeError) as exc:
            state_error = type(exc).__name__

    catalog_path = root / "state" / "catalog.sqlite"
    catalog_summary: dict[str, object] = {
        "available": False,
        "active_chunks": None,
        "sealed_chunks": None,
    }
    external_storage: dict[str, object] = {
        "status": "NO_REGISTERED_TARGETS",
        "targets": [],
    }
    if catalog_path.is_file():
        with Catalog(catalog_path) as catalog:
            catalog_summary = {
                "available": True,
                "active_chunks": len(
                    catalog.chunks_in_states(
                        ChunkState.ACTIVE, ChunkState.RECOVERED, ChunkState.SEALING
                    )
                ),
                "sealed_chunks": len(catalog.chunks_in_states(ChunkState.SEALED)),
            }
            try:
                targets = StorageRegistry(
                    catalog=catalog, volumes=DiskArbitrationAdapter()
                ).statuses()
                external_storage = {
                    "status": (
                        "LOW_SPACE"
                        if any(target["state"] == "LOW_SPACE" for target in targets)
                        else ("OK" if targets else "NO_REGISTERED_TARGETS")
                    ),
                    "targets": targets,
                }
            except (OSError, PlatformVolumeError) as exc:
                external_storage = {
                    "status": "PLATFORM_UNAVAILABLE",
                    "targets": [],
                    "reason": str(exc),
                }

    reports = sorted((root / "data" / "reports" / "daily").glob("*.json"))
    disk = shutil.disk_usage(_nearest_existing(root))
    internal_severity = space_severity(disk.total, disk.free)
    return {
        "command": "status",
        "status": "NOT_RUNNING",
        "service_implemented": False,
        "collector_implemented": True,
        "implemented_markets": ["spot", "um_perpetual"],
        "network_connected": False,
        "network_status": "UNAVAILABLE_NO_SUPERVISED_SERVICE",
        "observed_service_state": service_state,
        "service_state_path": str(state_path),
        "service_state_error": state_error,
        "catalog": catalog_summary,
        "latest_daily_report": str(reports[-1]) if reports else None,
        "internal_storage": {
            "path": str(root),
            "free_bytes": disk.free,
            "total_bytes": disk.total,
            "space_severity": internal_severity.value,
            "state": (
                "READY" if internal_severity.value == "OK" else "LOW_SPACE"
            ),
        },
        "external_storage": external_storage,
        "runtime_metrics": {
            "cpu_percent": {"value": None, "status": "NOT_RUNNING"},
            "rss_memory_bytes": {"value": None, "status": "NOT_RUNNING"},
            "queue_depth": {"value": None, "status": "NOT_RUNNING"},
            "last_event_age_ns": {"value": None, "status": "NOT_RUNNING"},
        },
        "detail": (
            "Collector libraries, M8 observability, and M9 external storage discovery "
            "are implemented; supervised service state is not implemented, so observed "
            "files cannot claim RUNNING."
        ),
    }
