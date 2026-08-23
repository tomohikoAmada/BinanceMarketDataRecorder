"""Honest structured runtime and storage status without service fabrication."""

from __future__ import annotations

import os
import shutil
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

from .service.launchd import (
    LaunchAgentError,
    LaunchAgentManager,
    installed_service_label,
)
from .service.state import ServiceStateError, ServiceStateStore
from .service.systemd import SystemdError, SystemdManager
from .storage.catalog import Catalog, ChunkState
from .storage.forecast import space_severity
from .storage.macos import PlatformVolumeError, StorageRegistry
from .storage.platform import volume_adapter


def _nearest_existing(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def service_status(
    data_root: Path,
    *,
    configured_proxy_status: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    """Read current evidence; missing service state remains explicitly NOT_RUNNING."""

    root = data_root.resolve()
    state_path = root / "state" / "service_state.json"
    service_state: dict[str, object] | None = None
    state_error: str | None = None
    try:
        service_state = ServiceStateStore(state_path).read()
    except ServiceStateError as exc:
        state_error = str(exc)

    current_status = "NOT_RUNNING"
    network_connected = False
    network_status = "SERVICE_NOT_RUNNING"
    if service_state is not None:
        observed_status = service_state.get("status")
        pid = service_state.get("pid")
        heartbeat = service_state.get("heartbeat_at_utc_ns")
        interval = service_state.get("heartbeat_interval_seconds")
        if (
            observed_status in {"STARTING", "RUNNING", "STOPPING"}
            and isinstance(pid, int)
            and not isinstance(pid, bool)
            and isinstance(heartbeat, int)
            and not isinstance(heartbeat, bool)
            and isinstance(interval, (int, float))
            and not isinstance(interval, bool)
        ):
            age_ns = time.time_ns() - heartbeat
            maximum_age_ns = int(max(30.0, float(interval) * 3) * 1_000_000_000)
            if age_ns < -5_000_000_000:
                current_status = "STALE"
                state_error = "heartbeat_in_future"
            elif not _process_alive(pid):
                current_status = "STALE"
                state_error = "service_pid_not_running"
            elif age_ns > maximum_age_ns:
                current_status = "STALE"
                state_error = "service_heartbeat_stale"
            else:
                current_status = str(observed_status)
                network_connected = bool(service_state.get("network_connected"))
                network_status = str(
                    service_state.get("network_status", "UNKNOWN")
                )
        elif observed_status == "FAILED":
            current_status = "FAILED"
            network_status = "SERVICE_FAILED"
        elif observed_status == "STOPPED":
            current_status = "NOT_RUNNING"
        else:
            state_error = state_error or "invalid_service_state_fields"

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
        with Catalog(catalog_path, read_only=True) as catalog:
            lifecycle = catalog.source_lifecycle_aggregate()
            catalog_summary = {
                "available": True,
                "active_chunks": len(
                    catalog.chunks_in_states(
                        ChunkState.ACTIVE, ChunkState.RECOVERED, ChunkState.SEALING
                    )
                ),
                "sealed_chunks": len(catalog.chunks_in_states(ChunkState.SEALED)),
                "ordinary_sealed_chunks": lifecycle["ordinary_sealed_files"],
                "remote_delete_pending_chunks": lifecycle["remote_pending_files"],
                "remote_deleted_chunks": lifecycle["remote_deleted_files"],
                "remote_pending_source_bytes": lifecycle[
                    "remote_pending_source_bytes"
                ],
            }
            try:
                targets = StorageRegistry(
                    catalog=catalog, volumes=volume_adapter()
                ).observe_statuses()
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
    launchagent: dict[str, object] = {
        "status": "NOT_INSTALLED",
        "loaded": False,
    }
    if sys.platform == "darwin":
        try:
            label = installed_service_label(root)
            if label is not None:
                launchagent = LaunchAgentManager(
                    data_root=root,
                    label=label,
                ).status()
        except (LaunchAgentError, OSError) as exc:
            launchagent = {
                "status": "ERROR",
                "loaded": False,
                "reason": str(exc),
            }
    systemd: dict[str, object] = {
        "installed": False,
        "enabled": False,
        "running": False,
    }
    if sys.platform.startswith("linux"):
        try:
            systemd = SystemdManager(
                data_root=root,
                config_file=Path("/etc/binance-market-data-recorder/recorder.toml"),
                user="",
                group="",
            ).status()
        except (OSError, SystemdError) as exc:
            systemd = {
                "installed": False,
                "enabled": False,
                "running": False,
                "reason": str(exc),
            }
    runtime_state_metrics: dict[str, object] = {}
    markets: dict[str, object] = {}
    if service_state is not None:
        metrics_value = service_state.get("runtime_metrics")
        markets_value = service_state.get("markets")
        if isinstance(metrics_value, dict):
            runtime_state_metrics = cast(dict[str, object], metrics_value)
        if isinstance(markets_value, dict):
            markets = cast(dict[str, object], markets_value)
    last_receive_times = [
        value
        for market in markets.values()
        if isinstance(market, dict)
        and isinstance((value := market.get("last_receive_time_utc_ns")), int)
        and not isinstance(value, bool)
    ]
    last_event_age_ns = (
        max(0, time.time_ns() - max(last_receive_times))
        if last_receive_times and current_status == "RUNNING"
        else None
    )
    return {
        "command": "status",
        "status": current_status,
        "service_implemented": True,
        "collector_implemented": True,
        "implemented_markets": ["spot", "um_perpetual"],
        "network_connected": network_connected,
        "network_status": network_status,
        "proxy_mode": (
            service_state.get("proxy_mode")
            if service_state is not None
            else (
                configured_proxy_status.get("proxy_mode")
                if configured_proxy_status is not None
                else None
            )
        ),
        "proxy_scheme": (
            service_state.get("proxy_scheme")
            if service_state is not None
            else (
                configured_proxy_status.get("proxy_scheme")
                if configured_proxy_status is not None
                else None
            )
        ),
        "proxy_loopback": (
            service_state.get("proxy_loopback")
            if service_state is not None
            else (
                configured_proxy_status.get("proxy_loopback")
                if configured_proxy_status is not None
                else None
            )
        ),
        "proxy_port": (
            service_state.get("proxy_port")
            if service_state is not None
            else (
                configured_proxy_status.get("proxy_port")
                if configured_proxy_status is not None
                else None
            )
        ),
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
        "launchagent": launchagent,
        "systemd": systemd,
        "runtime_metrics": {
            "process_cpu_seconds": {
                "value": runtime_state_metrics.get("process_cpu_seconds"),
                "status": current_status,
            },
            "current_rss_bytes": {
                "value": runtime_state_metrics.get("current_rss_bytes"),
                "status": current_status,
            },
            "peak_rss_bytes": {
                "value": runtime_state_metrics.get("peak_rss_bytes"),
                "status": current_status,
            },
            "queue_depth": {"value": None, "status": "UNAVAILABLE"},
            "last_event_age_ns": {
                "value": last_event_age_ns,
                "status": current_status,
            },
        },
        "detail": (
            "RUNNING requires a live PID and a fresh atomic service heartbeat; "
            "a stale or malformed state file never fabricates service health."
        ),
    }
