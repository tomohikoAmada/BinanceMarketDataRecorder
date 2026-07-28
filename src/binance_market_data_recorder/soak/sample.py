"""Collect a single time-point observation for M21 long-term soak monitoring.

Each invocation appends one JSON line to the configured output file
using O_APPEND plus an advisory file lock to prevent interleaving.
The sample is fsync'd after writing.  No network access, no Binance
calls, no AI calls, no modification of Raw/Manifest/Normalized data.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import platform
import socket
import struct
import sys
import time
from pathlib import Path
from typing import Any

# ── _LockFile ───────────────────────────────────────────────────────
_LOCK_BODY = struct.Struct("!I")


def _acquire_append_lock(fd: int) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
    except BlockingIOError as exc:
        raise OSError("soak sample lock is blocked") from exc


def _release_append_lock(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)


# ── public entry point ──────────────────────────────────────────────

def soak_sample(
    *,
    data_root: Path,
    output_path: Path,
    config_dict: dict[str, object],
    recorder_version: str,
    utc_clock_ns: Any = time.time_ns,
) -> dict[str, object]:
    sampled_at = utc_clock_ns()
    root = data_root.resolve()

    sample: dict[str, object] = {
        "schema_version": "m21-soak-sample.v1",
        "sampled_at_utc_ns": sampled_at,
        "boot_id": _boot_id(),
        "hostname": socket.gethostname(),
        "platform": sys.platform,
        "architecture": platform.machine(),
        "recorder_version": recorder_version,
        "config_hash": _config_hash(config_dict),
        "sample_id": _sample_id(sampled_at),
    }

    # ── systemd status ──────────────────────────────────────────
    sample["systemd"] = _systemd_status(root)

    # ── process metrics ─────────────────────────────────────────
    sample["process"] = _process_metrics(root)

    # ── market state ────────────────────────────────────────────
    sample["markets"] = _market_state(root)

    # ── archive state ───────────────────────────────────────────
    sample["archive"] = _archive_state(root)

    # ── disk ────────────────────────────────────────────────────
    sample["disk"] = _disk_state(root)

    # ── write ───────────────────────────────────────────────────
    output_path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(sample, sort_keys=True, separators=(",", ":"), default=str) + "\n").encode()
    fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
    try:
        _acquire_append_lock(fd)
        os.write(fd, line)
        os.fsync(fd)
    finally:
        _release_append_lock(fd)
        os.close(fd)

    return sample


# ── helpers ─────────────────────────────────────────────────────────

def _boot_id() -> str | None:
    path = Path("/proc/sys/kernel/random/boot_id")
    try:
        return path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeError):
        return None


def _sample_id(ts_ns: int) -> str:
    return f"soak-{ts_ns}"


def _config_hash(config_dict: dict[str, object]) -> str:
    clean = dict(config_dict)
    clean.pop("proxy_url", None)
    clean.pop("proxy_username", None)
    clean.pop("proxy_password", None)
    return hashlib.sha256(
        json.dumps(clean, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]


def _systemd_status(root: Path) -> dict[str, object]:
    state_path = root / "state" / "service_state.json"
    result: dict[str, object] = {
        "recorder_active_state": "UNKNOWN",
        "recorder_sub_state": "UNKNOWN",
        "recorder_main_pid": None,
        "recorder_nrestarts": None,
        "recorder_active_enter_timestamp": None,
        "archive_timer_active_state": "UNKNOWN",
        "archive_service_result": None,
    }
    if state_path.is_file():
        try:
            body = json.loads(state_path.read_text(encoding="utf-8"))
            result["recorder_active_state"] = body.get("status", "UNKNOWN")
            result["recorder_main_pid"] = body.get("pid")
        except (OSError, json.JSONDecodeError, ValueError):
            pass
    if sys.platform.startswith("linux"):
        try:
            pid_str = subprocess_run_check(
                ["systemctl", "show", "-p", "NRestarts",
                 "binance-market-data-recorder.service"]
            ).stdout.strip()
            if pid_str.startswith("NRestarts="):
                val = pid_str.split("=", 1)[1]
                result["recorder_nrestarts"] = int(val) if val.isdigit() else None
        except Exception:
            pass
        try:
            enter_str = subprocess_run_check(
                ["systemctl", "show", "-p", "ActiveEnterTimestampMonotonic",
                 "binance-market-data-recorder.service"]
            ).stdout.strip()
            if enter_str.startswith("ActiveEnterTimestampMonotonic="):
                val = enter_str.split("=", 1)[1]
                result["recorder_active_enter_timestamp"] = int(val) if val.isdigit() else None
        except Exception:
            pass
        try:
            archive_active = subprocess_run_check(
                ["systemctl", "is-active", "binance-market-data-archive.timer"],
                allow_failure=True,
            ).stdout.strip()
            result["archive_timer_active_state"] = archive_active or "inactive"
        except Exception:
            pass
        try:
            archive_result = subprocess_run_check(
                ["systemctl", "show", "-p", "Result",
                 "binance-market-data-archive.service"],
                allow_failure=True,
            ).stdout.strip()
            if archive_result.startswith("Result="):
                result["archive_service_result"] = archive_result.split("=", 1)[1] or None
        except Exception:
            pass
    return result


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
        "pid_changed_since_previous_sample": None,
    }
    if state_path.is_file():
        try:
            body = json.loads(state_path.read_text(encoding="utf-8"))
            pid_val = body.get("pid")
            if isinstance(pid_val, int) and not isinstance(pid_val, bool):
                result["pid"] = pid_val
                metrics = body.get("runtime_metrics", {})
                if isinstance(metrics, dict):
                    result["current_rss_bytes"] = metrics.get("current_rss_bytes")
                    result["peak_rss_bytes"] = metrics.get("peak_rss_bytes")
                    result["process_cpu_seconds"] = metrics.get("process_cpu_seconds")
                proc_dir = Path(f"/proc/{pid_val}")
                if proc_dir.is_dir():
                    result["process_start_time"] = _proc_stat_field(pid_val, 21)
                    result["thread_count"] = _count_proc_files(proc_dir / "task")
                    result["open_fd_count"] = _count_proc_files(proc_dir / "fd")
        except (OSError, json.JSONDecodeError, ValueError):
            pass
    return result


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
                for key in ("spot", "um_perpetual"):
                    market = markets.get(key)
                    if isinstance(market, dict):
                        result[f"{'spot' if key == 'spot' else 'usdm'}_state"] = \
                            market.get("status", "UNKNOWN")
                        for sk in ("last_error_type", "last_receive_time_utc_ns",
                                   "recovery_action_count"):
                            if sk in market:
                                result[f"{'spot' if key == 'spot' else 'usdm'}_{sk}"] = market[sk]
        except (OSError, json.JSONDecodeError, ValueError):
            pass
    return result


def _archive_state(root: Path) -> dict[str, object]:
    catalog_path = root / "state" / "catalog.sqlite"
    result: dict[str, object] = {
        "backlog_files": 0,
        "backlog_bytes": 0,
        "transactions_by_state": {},
        "archived_files": 0,
        "archived_bytes": 0,
        "local_deleted_bytes": 0,
        "last_archive_success_at": None,
        "last_archive_error_type": None,
    }
    if catalog_path.is_file():
        try:
            from ..storage.catalog import Catalog, ChunkState
            with Catalog(catalog_path) as catalog:
                backlog = catalog.chunks_in_states(
                    ChunkState.SEALED,
                    ChunkState.ARCHIVE_COPYING,
                    ChunkState.ARCHIVE_VERIFYING,
                    ChunkState.ARCHIVED_VERIFIED,
                    ChunkState.LOCAL_DELETE_PENDING,
                )
                result["backlog_files"] = len(backlog)
                result["backlog_bytes"] = sum(
                    v for row in backlog
                    if isinstance((v := row.get("stored_bytes")), int)
                    and not isinstance(v, bool)
                )
                transactions = catalog.archive_transactions()
                txn_states: dict[str, int] = {}
                archived_files = 0
                archived_bytes = 0
                local_deleted_bytes = 0
                last_archive_success_at: int | None = None
                last_error_type: str | None = None
                for txn in transactions:
                    s = str(txn.get("state", "UNKNOWN"))
                    txn_states[s] = txn_states.get(s, 0) + 1
                    if s == "LOCAL_DELETED":
                        archived_files += 1
                        b = txn.get("stored_bytes")
                        if isinstance(b, int) and not isinstance(b, bool):
                            archived_bytes += b
                            local_deleted_bytes += b
                    if (s == "VERIFIED" and txn.get("verified_at_utc_ns")):
                        vat = txn["verified_at_utc_ns"]
                        if (isinstance(vat, int) and not isinstance(vat, bool)
                                and (last_archive_success_at is None
                                     or vat > last_archive_success_at)):
                            last_archive_success_at = vat
                    err = txn.get("last_error")
                    if err is not None and err != "" and last_error_type is None:
                        last_error_type = str(err)[:200]
                result["archived_files"] = archived_files
                result["archived_bytes"] = archived_bytes
                result["local_deleted_bytes"] = local_deleted_bytes
                result["last_archive_success_at"] = last_archive_success_at
                result["last_archive_error_type"] = last_error_type
                result["transactions_by_state"] = txn_states
        except Exception:
            import traceback
            traceback.print_exc()
            result["_archive_state_error"] = "catalog_read_failed"
    return result


def _disk_state(root: Path) -> dict[str, object]:
    result: dict[str, object] = {
        "internal_total_bytes": 0,
        "internal_free_bytes": 0,
        "internal_free_fraction": 0.0,
        "internal_space_severity": "UNKNOWN",
        "external_total_bytes": None,
        "external_free_bytes": None,
        "external_free_fraction": None,
        "external_space_severity": "ABSENT",
    }
    try:
        st = os.statvfs(root)
        internal_total = st.f_frsize * st.f_blocks
        internal_free = st.f_frsize * st.f_bavail
        result["internal_total_bytes"] = internal_total
        result["internal_free_bytes"] = internal_free
        internal_free_fraction: float = 0.0
        if internal_total > 0:
            internal_free_fraction = round(internal_free / internal_total, 4)
        result["internal_free_fraction"] = internal_free_fraction
        if internal_free_fraction < 0.05:
            result["internal_space_severity"] = "CRITICAL"
        elif internal_free_fraction < 0.10:
            result["internal_space_severity"] = "LOW_SPACE"
        else:
            result["internal_space_severity"] = "OK"
            result["internal_space_severity"] = "OK"
    except OSError:
        pass

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
                if targets:
                    for t in targets:
                        if t.get("state") in ("READY", "LOW_SPACE"):
                            ext_total: int | None = None
                            ext_free: int | None = None
                            et = t.get("total_bytes")
                            ef = t.get("free_bytes")
                            if isinstance(et, int) and not isinstance(et, bool):
                                ext_total = et
                                result["external_total_bytes"] = ext_total
                            if isinstance(ef, int) and not isinstance(ef, bool):
                                ext_free = ef
                                result["external_free_bytes"] = ext_free
                            if (ext_total is not None and ext_total > 0
                                    and ext_free is not None):
                                result["external_free_fraction"] = round(
                                    ext_free / ext_total, 4
                                )
                            result["external_space_severity"] = t.get(
                                "space_severity", "OK"
                            )
                            break
    except Exception:
        pass

    return result


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


class _SimpleCompletedProcess:
    stdout: str
    def __init__(self, stdout: str = "", returncode: int = 0):
        self.stdout = stdout
        self.returncode = returncode


def subprocess_run_check(
    args: list[str], *, allow_failure: bool = False, timeout: int = 15
) -> _SimpleCompletedProcess:
    import subprocess
    try:
        result = subprocess.run(
            args, capture_output=True, text=True, timeout=timeout, check=False
        )
        return _SimpleCompletedProcess(stdout=result.stdout, returncode=result.returncode)
    except Exception:
        return _SimpleCompletedProcess(stdout="")
