"""Collect a single time-point observation for M21 long-term soak monitoring.

Each invocation appends one JSON line to the configured output file
using O_APPEND plus an advisory file lock to prevent interleaving.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import platform
import socket
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..service.state import ServiceStateError, ServiceStateStore
from ..storage.catalog import Catalog, CatalogStateError
from ..storage.forecast import space_severity
from ..storage.macos import (
    PlatformVolumeError,
    StorageRegistrationError,
    StorageRegistry,
)
from ..storage.platform import volume_adapter

SOAK_SCHEMA_VERSION = "m21-soak-sample.v2"


@dataclass(frozen=True, slots=True)
class _ServiceStateEvidence:
    document: dict[str, object] | None
    observed_application_status: str | None
    service_state_pid: int | None
    heartbeat_at_utc_ns: int | None
    heartbeat_age_ns: int | None
    service_state_fresh: bool
    service_state_error: str | None


# ── public entry point ──────────────────────────────────────────────

def soak_sample(
    *,
    data_root: Path,
    output_path: Path,
    storage_id: str | None,
    config_dict: dict[str, object],
    recorder_version: str,
    utc_clock_ns: Any = time.time_ns,
) -> dict[str, object]:
    sampled_at = utc_clock_ns()
    root = data_root.resolve()

    sample: dict[str, object] = {
        "schema_version": SOAK_SCHEMA_VERSION,
        "sampled_at_utc_ns": sampled_at,
        "boot_id": _boot_id(),
        "hostname": socket.gethostname(),
        "platform": sys.platform,
        "architecture": platform.machine(),
        "recorder_version": recorder_version,
        "config_hash": _config_hash(config_dict),
        "sample_id": f"soak-{sampled_at}",
    }

    systemd = _systemd_status()
    state_evidence = _service_state_evidence(root, sampled_at)
    service = _service_health(systemd, state_evidence)
    sample["systemd"] = systemd
    sample["service"] = service
    sample["process"] = _process_metrics(systemd, state_evidence, service)
    sample["markets"] = _market_state(state_evidence, service)
    sample["archive"] = _archive_state(root, storage_id)
    sample["disk"] = _disk_state(root, storage_id)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(sample, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode()
    fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
    try:
        _acquire_append_lock(fd)
        _write_all(fd, line)
        os.fsync(fd)
    finally:
        _release_append_lock(fd)
        os.close(fd)

    return sample


# ── locking helpers ──────────────────────────────────────────────────

def _acquire_append_lock(fd: int) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except BlockingIOError as exc:
        raise OSError("soak sample lock is blocked") from exc


def _release_append_lock(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("write returned no progress")
        view = view[written:]


# ── systemd ─────────────────────────────────────────────────────────

def _systemd_status() -> dict[str, object]:
    result: dict[str, object] = {
        "recorder_active_state": "UNKNOWN",
        "recorder_sub_state": "UNKNOWN",
        "recorder_main_pid": None,
        "recorder_nrestarts": None,
        "recorder_active_enter_timestamp_monotonic": None,
        "recorder_service_result": None,
        "recorder_error": None,
        "archive_timer_active_state": "UNKNOWN",
        "archive_service_result": None,
    }
    if not sys.platform.startswith("linux"):
        result["recorder_error"] = "systemd_unavailable_on_platform"
        return result

    try:
        out = subprocess.run(
            [
                "systemctl", "show",
                "binance-market-data-recorder.service",
                "-p", "ActiveState",
                "-p", "SubState",
                "-p", "MainPID",
                "-p", "NRestarts",
                "-p", "ActiveEnterTimestampMonotonic",
                "-p", "Result",
            ],
            capture_output=True, text=True, timeout=15, check=False,
        )
        if out.returncode != 0:
            result["recorder_error"] = f"systemctl_show_failed:{out.returncode}"
        else:
            parsed_keys: set[str] = set()
            for line in out.stdout.strip().split("\n"):
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                parsed_keys.add(key)
                if key == "ActiveState":
                    result["recorder_active_state"] = val or "UNKNOWN"
                elif key == "SubState":
                    result["recorder_sub_state"] = val or "UNKNOWN"
                elif key == "MainPID":
                    result["recorder_main_pid"] = (
                        int(val) if val.isdigit() and val != "0" else None
                    )
                elif key == "NRestarts":
                    result["recorder_nrestarts"] = int(val) if val.isdigit() else None
                elif key == "ActiveEnterTimestampMonotonic":
                    result["recorder_active_enter_timestamp_monotonic"] = (
                        int(val) if val.isdigit() else None
                    )
                elif key == "Result":
                    result["recorder_service_result"] = val or None
            required = {"ActiveState", "SubState", "MainPID"}
            if not required <= parsed_keys:
                result["recorder_error"] = "systemctl_show_incomplete"
    except (OSError, subprocess.SubprocessError) as exc:
        result["recorder_error"] = f"systemctl_unavailable:{type(exc).__name__}"

    try:
        out = subprocess.run(
            ["systemctl", "is-active", "binance-market-data-archive.timer"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        result["archive_timer_active_state"] = out.stdout.strip() or "inactive"
    except (OSError, subprocess.SubprocessError):
        pass
    try:
        out = subprocess.run(
            ["systemctl", "show", "-p", "Result",
             "binance-market-data-archive.service"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        for line in out.stdout.strip().split("\n"):
            if line.startswith("Result="):
                result["archive_service_result"] = line.split("=", 1)[1] or None
    except (OSError, subprocess.SubprocessError):
        pass
    return result


# ── service state and health ────────────────────────────────────────

def _service_state_evidence(
    root: Path,
    sampled_at_utc_ns: int,
) -> _ServiceStateEvidence:
    state_path = root / "state" / "service_state.json"
    try:
        document = ServiceStateStore(state_path).read()
    except ServiceStateError as exc:
        return _ServiceStateEvidence(
            None, None, None, None, None, False, str(exc)
        )
    if document is None:
        return _ServiceStateEvidence(
            None, None, None, None, None, False, "service_state_missing"
        )

    observed_status = document.get("status")
    pid_value = document.get("pid")
    heartbeat_value = document.get("heartbeat_at_utc_ns")
    interval_value = document.get("heartbeat_interval_seconds")
    pid = (
        pid_value
        if isinstance(pid_value, int) and not isinstance(pid_value, bool)
        else None
    )
    heartbeat = (
        heartbeat_value
        if isinstance(heartbeat_value, int)
        and not isinstance(heartbeat_value, bool)
        else None
    )
    observed = observed_status if isinstance(observed_status, str) else None
    if (
        observed not in {"STARTING", "RUNNING", "STOPPING", "STOPPED", "FAILED"}
        or pid is None
        or heartbeat is None
        or not isinstance(interval_value, (int, float))
        or isinstance(interval_value, bool)
        or float(interval_value) <= 0
    ):
        return _ServiceStateEvidence(
            document,
            observed,
            pid,
            heartbeat,
            None if heartbeat is None else sampled_at_utc_ns - heartbeat,
            False,
            "invalid_service_state_fields",
        )

    heartbeat_age = sampled_at_utc_ns - heartbeat
    maximum_age_ns = int(
        max(30.0, float(interval_value) * 3.0) * 1_000_000_000
    )
    if heartbeat_age < -5_000_000_000:
        error = "heartbeat_in_future"
        fresh = False
    elif heartbeat_age > maximum_age_ns:
        error = "service_heartbeat_stale"
        fresh = False
    else:
        error = None
        fresh = True
    return _ServiceStateEvidence(
        document,
        observed,
        pid,
        heartbeat,
        heartbeat_age,
        fresh,
        error,
    )


def _service_health(
    systemd: dict[str, object],
    evidence: _ServiceStateEvidence,
) -> dict[str, object]:
    active_state = str(systemd.get("recorder_active_state", "UNKNOWN"))
    sub_state = str(systemd.get("recorder_sub_state", "UNKNOWN"))
    systemd_pid_value = systemd.get("recorder_main_pid")
    systemd_pid = (
        systemd_pid_value
        if isinstance(systemd_pid_value, int)
        and not isinstance(systemd_pid_value, bool)
        and systemd_pid_value > 0
        else None
    )
    service_pid = evidence.service_state_pid
    if systemd_pid is None and service_pid is None:
        pid_mismatch: bool | None = None
    else:
        pid_mismatch = systemd_pid != service_pid

    systemd_error = systemd.get("recorder_error")
    if systemd_error is not None or active_state == "UNKNOWN":
        application_status = "UNKNOWN"
    elif active_state == "failed":
        application_status = "FAILED"
    elif active_state != "active":
        application_status = "NOT_RUNNING"
    elif not evidence.service_state_fresh:
        application_status = "STALE"
    elif pid_mismatch is not False:
        application_status = "UNTRUSTED"
    else:
        application_status = evidence.observed_application_status or "UNKNOWN"

    return {
        "systemd_active_state": active_state,
        "systemd_sub_state": sub_state,
        "systemd_main_pid": systemd_pid,
        "application_status": application_status,
        "observed_application_status": evidence.observed_application_status,
        "service_state_pid": service_pid,
        "pid_mismatch": pid_mismatch,
        "heartbeat_at_utc_ns": evidence.heartbeat_at_utc_ns,
        "heartbeat_age_ns": evidence.heartbeat_age_ns,
        "service_state_fresh": evidence.service_state_fresh,
        "service_state_error": evidence.service_state_error,
    }


# ── process ─────────────────────────────────────────────────────────

def _process_metrics(
    systemd: dict[str, object],
    evidence: _ServiceStateEvidence,
    service: dict[str, object],
) -> dict[str, object]:
    systemd_pid_value = systemd.get("recorder_main_pid")
    systemd_pid = (
        systemd_pid_value
        if isinstance(systemd_pid_value, int)
        and not isinstance(systemd_pid_value, bool)
        and systemd_pid_value > 0
        else None
    )
    result: dict[str, object] = {
        "pid": systemd_pid,
        "pid_source": "SYSTEMD_MAIN_PID",
        "service_state_pid": evidence.service_state_pid,
        "pid_mismatch": service["pid_mismatch"],
        "current_rss_bytes": None,
        "peak_rss_bytes": None,
        "open_fd_count": None,
        "thread_count": None,
        "process_cpu_seconds": None,
        "process_start_time": None,
    }
    if systemd_pid is not None:
        result.update(_proc_metrics(systemd_pid))

    runtime_values: dict[str, object] = {}
    if evidence.document is not None:
        raw_metrics = evidence.document.get("runtime_metrics")
        if isinstance(raw_metrics, dict):
            runtime_values = {
                str(key): value
                for key, value in raw_metrics.items()
            }
    trusted = (
        evidence.service_state_fresh
        and service["pid_mismatch"] is False
        and service["systemd_active_state"] == "active"
    )
    if trusted:
        runtime_status = "CURRENT"
        current_runtime_metrics = runtime_values
    elif evidence.service_state_error == "service_heartbeat_stale":
        runtime_status = "STALE"
        current_runtime_metrics = {}
    else:
        runtime_status = "UNAVAILABLE"
        current_runtime_metrics = {}
    result["runtime_metrics_status"] = runtime_status
    result["runtime_metrics"] = current_runtime_metrics
    return result


def _proc_metrics(pid: int) -> dict[str, object]:
    result: dict[str, object] = {
        "current_rss_bytes": None,
        "peak_rss_bytes": None,
        "open_fd_count": None,
        "thread_count": None,
        "process_cpu_seconds": None,
        "process_start_time": None,
    }
    proc_dir = Path(f"/proc/{pid}")
    if not proc_dir.is_dir():
        return result
    try:
        for line in (proc_dir / "status").read_text(encoding="ascii").splitlines():
            key, separator, value = line.partition(":")
            if not separator:
                continue
            parts = value.split()
            if key in {"VmRSS", "VmHWM"} and parts and parts[0].isdigit():
                destination = (
                    "current_rss_bytes" if key == "VmRSS" else "peak_rss_bytes"
                )
                result[destination] = int(parts[0]) * 1024
            elif key == "Threads" and parts and parts[0].isdigit():
                result["thread_count"] = int(parts[0])
    except (OSError, UnicodeError):
        pass
    result["open_fd_count"] = _count_proc_files(proc_dir / "fd")
    result["thread_count"] = (
        result["thread_count"]
        if result["thread_count"] is not None
        else _count_proc_files(proc_dir / "task")
    )
    start_ticks = _proc_stat_field(pid, 22)
    user_ticks = _proc_stat_field(pid, 14)
    system_ticks = _proc_stat_field(pid, 15)
    result["process_start_time"] = start_ticks
    if user_ticks is not None and system_ticks is not None:
        try:
            ticks_per_second = int(os.sysconf("SC_CLK_TCK"))
        except (OSError, ValueError):
            ticks_per_second = 0
        if ticks_per_second > 0:
            result["process_cpu_seconds"] = (
                user_ticks + system_ticks
            ) / ticks_per_second
    return result


# ── market state from validated service evidence ────────────────────

def _market_state(
    evidence: _ServiceStateEvidence,
    service: dict[str, object],
) -> dict[str, object]:
    result: dict[str, object] = {
        "overall_network_state": "UNKNOWN",
        "spot_state": "UNKNOWN",
        "usdm_state": "UNKNOWN",
        "observed_overall_network_state": "UNKNOWN",
        "observed_spot_state": "UNKNOWN",
        "observed_usdm_state": "UNKNOWN",
    }
    document = evidence.document
    if document is not None:
        result["observed_overall_network_state"] = document.get(
            "network_status", "UNKNOWN"
        )
        markets = document.get("markets", {})
        if isinstance(markets, dict):
            for key, label in (("spot", "spot"), ("um_perpetual", "usdm")):
                market = markets.get(key)
                if not isinstance(market, dict):
                    continue
                result[f"observed_{label}_state"] = market.get(
                    "status", "UNKNOWN"
                )
                for state_key in (
                    "last_receive_time_utc_ns",
                    "last_error_type",
                    "recovery_action_count",
                    "orderbook_synchronized",
                ):
                    if state_key in market:
                        result[f"observed_{label}_{state_key}"] = market[state_key]

    application_status = service["application_status"]
    if application_status == "RUNNING":
        result["overall_network_state"] = result["observed_overall_network_state"]
        result["spot_state"] = result["observed_spot_state"]
        result["usdm_state"] = result["observed_usdm_state"]
    elif application_status == "STALE":
        result["overall_network_state"] = "STALE"
        result["spot_state"] = "STALE"
        result["usdm_state"] = "STALE"
    elif application_status == "UNKNOWN":
        pass
    else:
        result["overall_network_state"] = "UNTRUSTED"
        result["spot_state"] = "UNTRUSTED"
        result["usdm_state"] = "UNTRUSTED"
    return result


# ── archive state via bounded aggregation ───────────────────────────

def _stable_error_type(exc: Exception) -> str:
    if isinstance(exc, CatalogStateError):
        return "CatalogStateError"
    if isinstance(exc, sqlite3.Error):
        return "SQLiteError"
    if isinstance(exc, PlatformVolumeError):
        return "PlatformVolumeError"
    if isinstance(exc, StorageRegistrationError):
        return "StorageRegistrationError"
    if isinstance(exc, OSError):
        return "OSError"
    if isinstance(exc, ValueError):
        return "ValueError"
    return "UnexpectedError"


def _archive_state(root: Path, storage_id: str | None) -> dict[str, object]:
    catalog_path = root / "state" / "catalog.sqlite"
    result: dict[str, object] = {
        "storage_id": storage_id,
        "archive_evidence_status": "NO_CATALOG",
        "archive_error_type": None,
        "transactions_by_state": None,
        "archived_files": None,
        "archived_bytes": None,
        "local_deleted_files": None,
        "local_deleted_bytes": None,
        "unassigned_sealed_scope": None,
        "unassigned_sealed_files": None,
        "unassigned_sealed_bytes": None,
        "target_inflight_files": None,
        "target_inflight_bytes": None,
        "backlog_files": None,
        "backlog_bytes": None,
        "last_archive_success_at": None,
        "last_archive_error_type": None,
    }
    if not catalog_path.is_file():
        return result
    if storage_id is None:
        result["archive_evidence_status"] = "ERROR"
        result["archive_error_type"] = "MissingStorageId"
        return result
    try:
        with Catalog(catalog_path) as catalog:
            aggregate = catalog.archive_aggregate(storage_id)
    except Exception as exc:
        result["archive_evidence_status"] = "ERROR"
        result["archive_error_type"] = _stable_error_type(exc)
        return result

    raw_transactions = aggregate.get("transactions_by_state", {})
    result.update(
        {
            "archive_evidence_status": "OK",
            "transactions_by_state": (
                dict(raw_transactions)
                if isinstance(raw_transactions, dict)
                else {}
            ),
            "archived_files": aggregate.get("external_verified_files", 0),
            "archived_bytes": aggregate.get("external_verified_bytes", 0),
            "local_deleted_files": aggregate.get("local_deleted_files", 0),
            "local_deleted_bytes": aggregate.get("local_deleted_bytes", 0),
            "unassigned_sealed_scope": aggregate.get(
                "unassigned_sealed_scope", "GLOBAL"
            ),
            "unassigned_sealed_files": aggregate.get(
                "unassigned_sealed_files", 0
            ),
            "unassigned_sealed_bytes": aggregate.get(
                "unassigned_sealed_bytes", 0
            ),
            "target_inflight_files": aggregate.get(
                "target_inflight_files", 0
            ),
            "target_inflight_bytes": aggregate.get(
                "target_inflight_bytes", 0
            ),
            "backlog_files": aggregate.get("backlog_files", 0),
            "backlog_bytes": aggregate.get("backlog_bytes", 0),
            "last_archive_success_at": aggregate.get(
                "last_verified_at_utc_ns"
            ),
            "last_archive_error_type": aggregate.get("latest_error_type"),
        }
    )
    return result


# ── disk state via space_severity ────────────────────────────────────

def _disk_state(root: Path, storage_id: str | None) -> dict[str, object]:
    result: dict[str, object] = {
        "internal_evidence_status": "ERROR",
        "internal_error_type": None,
        "internal_total_bytes": None,
        "internal_free_bytes": None,
        "internal_free_fraction": None,
        "internal_space_severity": None,
        "external_storage_id": storage_id,
        "external_evidence_status": "NO_CATALOG",
        "external_error_type": None,
        "external_target_state": None,
        "external_total_bytes": None,
        "external_free_bytes": None,
        "external_free_fraction": None,
        "external_space_severity": None,
    }
    try:
        st = os.statvfs(root)
        internal_total = st.f_frsize * st.f_blocks
        internal_free = st.f_frsize * st.f_bavail
        result["internal_evidence_status"] = "OK"
        result["internal_total_bytes"] = internal_total
        result["internal_free_bytes"] = internal_free
        if internal_total > 0:
            result["internal_free_fraction"] = round(internal_free / internal_total, 4)
        result["internal_space_severity"] = space_severity(internal_total, internal_free).value
    except Exception as exc:
        result["internal_error_type"] = _stable_error_type(exc)

    catalog_path = root / "state" / "catalog.sqlite"
    if not catalog_path.is_file():
        return result
    if storage_id is None:
        result["external_evidence_status"] = "ERROR"
        result["external_error_type"] = "MissingStorageId"
        result["external_target_state"] = "ERROR"
        return result
    try:
        with Catalog(catalog_path) as catalog:
            targets = StorageRegistry(
                catalog=catalog, volumes=volume_adapter()
            ).statuses()
    except Exception as exc:
        result["external_evidence_status"] = "ERROR"
        result["external_error_type"] = _stable_error_type(exc)
        result["external_target_state"] = "ERROR"
        return result

    target = next(
        (
            candidate
            for candidate in targets
            if str(candidate.get("storage_id", "")) == storage_id
        ),
        None,
    )
    if target is None:
        result["external_evidence_status"] = "ABSENT"
        result["external_target_state"] = "ABSENT"
        result["external_space_severity"] = "ABSENT"
        return result

    state = str(target.get("state") or "ERROR")
    result["external_target_state"] = state
    if state in {"ABSENT", "PRESENT_UNMOUNTED"}:
        result["external_evidence_status"] = "ABSENT"
        result["external_space_severity"] = target.get(
            "space_severity", "ABSENT"
        )
        return result

    result["external_evidence_status"] = "OK"
    result["external_space_severity"] = target.get("space_severity", state)
    if state in {"READY", "LOW_SPACE"}:
        external_total = target.get("total_bytes")
        external_free = target.get("free_bytes")
        if isinstance(external_total, int) and not isinstance(external_total, bool):
            result["external_total_bytes"] = external_total
        if isinstance(external_free, int) and not isinstance(external_free, bool):
            result["external_free_bytes"] = external_free
        if (
            isinstance(external_total, int)
            and not isinstance(external_total, bool)
            and isinstance(external_free, int)
            and not isinstance(external_free, bool)
            and external_total > 0
        ):
            result["external_free_fraction"] = round(
                external_free / external_total, 4
            )
    return result


# ── helpers ─────────────────────────────────────────────────────────

def _boot_id() -> str | None:
    path = Path("/proc/sys/kernel/random/boot_id")
    try:
        return path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return None


def _config_hash(config_dict: dict[str, object]) -> str:
    clean = dict(config_dict)
    clean.pop("proxy_url", None)
    clean.pop("proxy_username", None)
    clean.pop("proxy_password", None)
    return hashlib.sha256(
        json.dumps(clean, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]


def _proc_stat_field(pid: int, field_index: int) -> int | None:
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        after_paren = text.rfind(")")
        if after_paren < 0:
            return None
        fields = text[after_paren + 2:].split()
        offset = field_index - 3
        if 0 <= offset < len(fields):
            return int(fields[offset])
    except (OSError, ValueError):
        pass
    return None


def _count_proc_files(dir_path: Path) -> int | None:
    try:
        return len(list(dir_path.iterdir()))
    except OSError:
        return None
