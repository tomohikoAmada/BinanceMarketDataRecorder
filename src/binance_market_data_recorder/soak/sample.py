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
import subprocess
import time
from pathlib import Path
from typing import Any

SOAK_SCHEMA_VERSION = "m21-soak-sample.v2"


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

    sample["systemd"] = _systemd_status()
    sample["process"] = _process_metrics(root)
    sample["markets"] = _market_state(root)
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
        "archive_timer_active_state": "UNKNOWN",
        "archive_service_result": None,
    }
    if sys.platform.startswith("linux"):
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
            for line in out.stdout.strip().split("\n"):
                if "=" in line:
                    key, val = line.split("=", 1)
                    if key == "ActiveState":
                        result["recorder_active_state"] = val or "UNKNOWN"
                    elif key == "SubState":
                        result["recorder_sub_state"] = val or "UNKNOWN"
                    elif key == "MainPID":
                        result["recorder_main_pid"] = int(val) if val.isdigit() else None
                    elif key == "NRestarts":
                        result["recorder_nrestarts"] = int(val) if val.isdigit() else None
                    elif key == "ActiveEnterTimestampMonotonic":
                        result["recorder_active_enter_timestamp_monotonic"] = (
                            int(val) if val.isdigit() else None
                        )
                    elif key == "Result":
                        result["recorder_service_result"] = val or None
        except Exception:
            pass
        try:
            out = subprocess.run(
                ["systemctl", "is-active", "binance-market-data-archive.timer"],
                capture_output=True, text=True, timeout=10, check=False,
            )
            result["archive_timer_active_state"] = out.stdout.strip() or "inactive"
        except Exception:
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
        except Exception:
            pass
    return result


# ── process ─────────────────────────────────────────────────────────

def _process_metrics(root: Path) -> dict[str, object]:
    state_path = root / "state" / "service_state.json"
    result: dict[str, object] = {
        "current_rss_bytes": None,
        "peak_rss_bytes": None,
        "open_fd_count": None,
        "thread_count": None,
        "process_cpu_seconds": None,
        "process_start_time": None,
        "pid": None,
        "pid_mismatch": None,
    }
    app_pid: int | None = None
    if state_path.is_file():
        try:
            body = json.loads(state_path.read_text(encoding="utf-8"))
            pid_val = body.get("pid")
            if isinstance(pid_val, int) and not isinstance(pid_val, bool):
                app_pid = pid_val
            metrics = body.get("runtime_metrics", {})
            if isinstance(metrics, dict):
                result["current_rss_bytes"] = metrics.get("current_rss_bytes")
                result["peak_rss_bytes"] = metrics.get("peak_rss_bytes")
                result["process_cpu_seconds"] = metrics.get("process_cpu_seconds")
        except (OSError, json.JSONDecodeError, ValueError):
            pass

    systemd_pid = _systemd_pid_get()
    if app_pid is not None and systemd_pid is not None:
        result["pid_mismatch"] = app_pid != systemd_pid
    elif app_pid is not None or systemd_pid is not None:
        result["pid_mismatch"] = True
    else:
        result["pid_mismatch"] = None

    proc_pid = app_pid if app_pid is not None else systemd_pid
    result["pid"] = proc_pid
    if proc_pid is not None:
        proc_dir = Path(f"/proc/{proc_pid}")
        if proc_dir.is_dir():
            result["process_start_time"] = _proc_stat_field(proc_pid, 21)
            result["thread_count"] = _count_proc_files(proc_dir / "task")
            result["open_fd_count"] = _count_proc_files(proc_dir / "fd")
    return result


def _systemd_pid_get() -> int | None:
    try:
        out = subprocess.run(
            ["systemctl", "show", "-p", "MainPID",
             "binance-market-data-recorder.service"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        for line in out.stdout.strip().split("\n"):
            if line.startswith("MainPID="):
                val = line.split("=", 1)[1]
                return int(val) if val.isdigit() and val != "0" else None
    except Exception:
        pass
    return None


# ── market state from real service_state ─────────────────────────────

def _market_state(root: Path) -> dict[str, object]:
    state_path = root / "state" / "service_state.json"
    result: dict[str, object] = {
        "overall_network_state": "UNKNOWN",
        "spot_state": "UNKNOWN",
        "usdm_state": "UNKNOWN",
    }
    if state_path.is_file():
        try:
            body = json.loads(state_path.read_text(encoding="utf-8"))
            result["overall_network_state"] = body.get("network_status", "UNKNOWN")
            markets = body.get("markets", {})
            if isinstance(markets, dict):
                for key, label in (("spot", "spot"), ("um_perpetual", "usdm")):
                    market = markets.get(key)
                    if isinstance(market, dict):
                        result[f"{label}_state"] = market.get("status", "UNKNOWN")
                        for sk in ("last_receive_time_utc_ns", "last_error_type",
                                   "recovery_action_count", "orderbook_synchronized"):
                            if sk in market:
                                result[f"{label}_{sk}"] = market[sk]
        except (OSError, json.JSONDecodeError, ValueError):
            pass
    return result


# ── archive state via bounded aggregation ───────────────────────────

def _archive_state(root: Path, storage_id: str | None) -> dict[str, object]:
    catalog_path = root / "state" / "catalog.sqlite"
    result: dict[str, object] = {
        "storage_id": storage_id,
        "transactions_by_state": {},
        "archived_files": 0,
        "archived_bytes": 0,
        "local_deleted_files": 0,
        "local_deleted_bytes": 0,
        "backlog_files": 0,
        "backlog_bytes": 0,
        "last_archive_success_at": None,
        "last_archive_error_type": None,
    }
    if catalog_path.is_file() and storage_id is not None:
        try:
            from ..storage.catalog import Catalog
            with Catalog(catalog_path) as catalog:
                agg = catalog.archive_aggregate(storage_id)
                raw_txn = agg.get("transactions_by_state", {})
                if isinstance(raw_txn, dict):
                    result["transactions_by_state"] = dict(raw_txn)
                result["archived_files"] = agg.get("external_verified_files", 0)
                result["archived_bytes"] = agg.get("external_verified_bytes", 0)
                result["local_deleted_files"] = agg.get("local_deleted_files", 0)
                result["local_deleted_bytes"] = agg.get("local_deleted_bytes", 0)
                result["backlog_files"] = agg.get("backlog_files", 0)
                result["backlog_bytes"] = agg.get("backlog_bytes", 0)
                result["last_archive_success_at"] = agg.get("last_verified_at_utc_ns")
                result["last_archive_error_type"] = agg.get("latest_error_type")
        except Exception:
            pass
    elif catalog_path.is_file() and storage_id is None:
        result["_note"] = "no storage_id provided; archive stats unavailable"
    return result


# ── disk state via space_severity ────────────────────────────────────

def _disk_state(root: Path, storage_id: str | None) -> dict[str, object]:
    result: dict[str, object] = {
        "internal_total_bytes": 0,
        "internal_free_bytes": 0,
        "internal_free_fraction": 0.0,
        "internal_space_severity": "UNKNOWN",
        "external_storage_id": storage_id,
        "external_total_bytes": None,
        "external_free_bytes": None,
        "external_free_fraction": None,
        "external_space_severity": "ABSENT",
    }
    try:
        from ..storage.forecast import space_severity
        st = os.statvfs(root)
        internal_total = st.f_frsize * st.f_blocks
        internal_free = st.f_frsize * st.f_bavail
        result["internal_total_bytes"] = internal_total
        result["internal_free_bytes"] = internal_free
        if internal_total > 0:
            result["internal_free_fraction"] = round(internal_free / internal_total, 4)
        result["internal_space_severity"] = space_severity(internal_total, internal_free).value
    except OSError:
        pass

    if storage_id is not None:
        try:
            from ..storage.catalog import Catalog
            from ..storage.macos import StorageRegistry
            from ..storage.platform import volume_adapter
            catalog_path = root / "state" / "catalog.sqlite"
            if catalog_path.is_file():
                with Catalog(catalog_path) as catalog:
                    targets = StorageRegistry(
                        catalog=catalog, volumes=volume_adapter()
                    ).statuses()
                    for t in targets:
                        if str(t.get("storage_id", "")) == storage_id:
                            state = t.get("state")
                            if state in ("READY", "LOW_SPACE"):
                                ext_total = t.get("total_bytes")
                                ext_free = t.get("free_bytes")
                                if isinstance(ext_total, int) and not isinstance(ext_total, bool):
                                    result["external_total_bytes"] = ext_total
                                if isinstance(ext_free, int) and not isinstance(ext_free, bool):
                                    result["external_free_bytes"] = ext_free
                                et = result.get("external_total_bytes")
                                ef = result.get("external_free_bytes")
                                if (
                                    isinstance(et, (int, float))
                                    and isinstance(ef, (int, float))
                                    and et > 0
                                ):
                                    result["external_free_fraction"] = round(ef / et, 4)
                            result["external_space_severity"] = t.get("space_severity", state)
                            break
                    if result["external_space_severity"] == "ABSENT":
                        pass
        except Exception:
            pass
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
        if field_index <= len(fields):
            return int(fields[field_index - 1])
    except (OSError, ValueError):
        pass
    return None


def _count_proc_files(dir_path: Path) -> int | None:
    try:
        return len(list(dir_path.iterdir()))
    except OSError:
        return None


import sys  # noqa: E402 (used above for platform checks)
