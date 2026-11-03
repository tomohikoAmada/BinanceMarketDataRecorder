"""Non-intrusive external observation script for Binance Market Data Recorder.

Periodically samples:
1. systemd service status (ActiveState, SubState, MainPID, InvocationID, NRestarts, etc.)
2. Process CPU and RSS memory consumption
3. binance-market-recorder status (runtime state, catalog, network connectivity)
4. Filesystem capacity and available space on data root
5. Raw chunk counts and growth rates (sealed .zst, active .partial, manifests)

Produces structured JSON lines (.jsonl) without modifying any recorder state.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import tomllib
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_SERVICE_NAME = "binance-market-data-recorder.service"
DEFAULT_VPS_DATA_ROOT = Path("/var/lib/binance-market-data-recorder")
DEFAULT_VPS_CONFIG_PATH = Path("/etc/binance-market-data-recorder/recorder.toml")


@dataclass(frozen=True)
class SystemdObservation:
    available: bool
    active_state: str
    sub_state: str
    main_pid: int
    invocation_id: str
    n_restarts: int
    active_enter_timestamp: str | None
    result: str | None
    memory_current_bytes: int | None
    cpu_usage_nsec: int | None
    error: str | None = None


@dataclass(frozen=True)
class ProcessObservation:
    pid: int
    alive: bool
    cpu_percent: float
    cpu_time_seconds: float
    rss_bytes: int
    rss_mib: float


@dataclass(frozen=True)
class StatusObservation:
    available: bool
    service_status: str
    network_connected: bool
    network_status: str
    catalog_sealed_chunks: int | None
    catalog_active_chunks: int | None
    space_severity: str | None
    last_event_age_seconds: float | None
    detail: str | None = None
    availability_journal_pending: int | None = None
    expected_product_count: int | None = None
    ready_product_count: int | None = None


@dataclass(frozen=True)
class DiskObservation:
    path: str
    total_bytes: int
    free_bytes: int
    available_bytes: int
    used_bytes: int
    used_percent: float
    free_gib: float


@dataclass(frozen=True)
class RawChunksObservation:
    sealed_zst_count: int
    active_partial_count: int
    manifest_json_count: int
    total_sealed_bytes: int
    growth_rate_chunks_per_hour: float
    growth_rate_mb_per_hour: float


@dataclass(frozen=True)
class SampleRecord:
    timestamp_utc: str
    sample_index: int
    elapsed_seconds: float
    systemd: SystemdObservation
    process: ProcessObservation
    status: StatusObservation
    disk: DiskObservation
    raw_chunks: RawChunksObservation


class ProcessSampler:
    """Samples CPU time and RSS for a process across intervals."""

    def __init__(self) -> None:
        self._prev_pid: int = 0
        self._prev_cpu_time: float = 0.0
        self._prev_wall_time: float = 0.0

    def sample(self, pid: int) -> ProcessObservation:
        now = time.monotonic()
        if pid <= 0 or not self._is_alive(pid):
            return ProcessObservation(
                pid=pid,
                alive=False,
                cpu_percent=0.0,
                cpu_time_seconds=0.0,
                rss_bytes=0,
                rss_mib=0.0,
            )

        cpu_time, rss_bytes = self._read_proc_stats(pid)
        cpu_percent = 0.0
        if self._prev_pid == pid and self._prev_wall_time > 0:
            wall_delta = now - self._prev_wall_time
            cpu_delta = cpu_time - self._prev_cpu_time
            if wall_delta > 0:
                cpu_percent = round((cpu_delta / wall_delta) * 100.0, 2)

        self._prev_pid = pid
        self._prev_cpu_time = cpu_time
        self._prev_wall_time = now

        return ProcessObservation(
            pid=pid,
            alive=True,
            cpu_percent=max(0.0, cpu_percent),
            cpu_time_seconds=round(cpu_time, 4),
            rss_bytes=rss_bytes,
            rss_mib=round(rss_bytes / (1024 * 1024), 3),
        )

    @staticmethod
    def _is_alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    @classmethod
    def _read_proc_stats(cls, pid: int) -> tuple[float, int]:
        # Try Linux /proc first
        proc_stat = Path(f"/proc/{pid}/stat")
        proc_statm = Path(f"/proc/{pid}/statm")
        if proc_stat.is_file():
            try:
                stat_content = proc_stat.read_text(encoding="utf-8")
                # Format: pid (comm) state ... fields
                # Match right paren to handle process names with spaces
                rparen = stat_content.rfind(")")
                if rparen != -1:
                    fields = stat_content[rparen + 2 :].split()
                    # utime is field 14 (idx 11 after comm), stime is field 15 (idx 12)
                    utime_ticks = int(fields[11])
                    stime_ticks = int(fields[12])
                    ticks_per_sec = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
                    cpu_seconds = (utime_ticks + stime_ticks) / ticks_per_sec

                    rss_bytes = 0
                    if proc_statm.is_file():
                        pages = int(proc_statm.read_text(encoding="utf-8").split()[1])
                        page_size = os.sysconf(os.sysconf_names["SC_PAGE_SIZE"])
                        rss_bytes = pages * page_size
                    return cpu_seconds, rss_bytes
            except (OSError, ValueError, IndexError):
                pass

        # Cross-platform fallback: ps command
        try:
            result = subprocess.run(
                ["ps", "-o", "cputime=,rss=", "-p", str(pid)],
                capture_output=True,
                text=True,
                check=False,
                timeout=3,
            )
            if result.returncode == 0:
                parts = result.stdout.strip().split()
                if len(parts) >= 2:
                    cpu_str = parts[0]
                    rss_kb = int(parts[1])
                    cpu_seconds = cls._parse_cputime(cpu_str)
                    return cpu_seconds, rss_kb * 1024
        except (OSError, subprocess.SubprocessError, ValueError):
            pass

        return 0.0, 0

    @staticmethod
    def _parse_cputime(text: str) -> float:
        # Handles [[dd-]hh:]mm:ss or mm:ss.ss
        try:
            days = 0
            if "-" in text:
                d_part, text = text.split("-", 1)
                days = int(d_part)
            parts = text.split(":")
            if len(parts) == 3:
                hours, minutes, seconds = int(parts[0]), int(parts[1]), float(parts[2])
            elif len(parts) == 2:
                hours, minutes, seconds = 0, int(parts[0]), float(parts[1])
            else:
                return float(text)
            return days * 86400.0 + hours * 3600.0 + minutes * 60.0 + seconds
        except (ValueError, IndexError):
            return 0.0


class RawChunkTracker:
    """Tracks chunk counts, sizes, and growth rate across time."""

    def __init__(self) -> None:
        self._initial_time: float = 0.0
        self._initial_chunks: int = -1
        self._initial_bytes: int = -1

    def sample(self, data_root: Path, now_mono: float) -> RawChunksObservation:
        zst_count = 0
        partial_count = 0
        json_count = 0
        total_zst_bytes = 0

        data_dir = data_root / "data"
        for entry in (data_dir / "sealed").glob("*.zst"):
            try:
                total_zst_bytes += entry.stat().st_size
                zst_count += 1
            except FileNotFoundError:
                continue  # An archive worker can retire a chunk during sampling.
        partial_count = sum(1 for _ in (data_dir / "active").glob("*.partial"))
        json_count = sum(1 for _ in (data_dir / "manifests").glob("*.manifest.json"))

        if self._initial_chunks < 0:
            self._initial_time = now_mono
            self._initial_chunks = zst_count
            self._initial_bytes = total_zst_bytes

        elapsed_hours = (now_mono - self._initial_time) / 3600.0
        chunks_rate = 0.0
        mb_rate = 0.0
        if elapsed_hours > (1.0 / 3600.0):  # More than 1 second elapsed
            delta_chunks = max(0, zst_count - self._initial_chunks)
            delta_bytes = max(0, total_zst_bytes - self._initial_bytes)
            chunks_rate = round(delta_chunks / elapsed_hours, 2)
            mb_rate = round((delta_bytes / (1024 * 1024)) / elapsed_hours, 2)

        return RawChunksObservation(
            sealed_zst_count=zst_count,
            active_partial_count=partial_count,
            manifest_json_count=json_count,
            total_sealed_bytes=total_zst_bytes,
            growth_rate_chunks_per_hour=chunks_rate,
            growth_rate_mb_per_hour=mb_rate,
        )


def query_systemd(
    unit: str,
    *,
    systemctl_path: str = "systemctl",
) -> SystemdObservation:
    """Read systemctl show properties non-intrusively."""
    properties = (
        "ActiveState",
        "SubState",
        "MainPID",
        "InvocationID",
        "NRestarts",
        "ActiveEnterTimestamp",
        "Result",
        "MemoryCurrent",
        "CPUUsageNSec",
    )
    cmd = [
        systemctl_path,
        "show",
        unit,
        "--no-pager",
        *(f"--property={p}" for p in properties),
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except FileNotFoundError:
        return SystemdObservation(
            available=False,
            active_state="UNKNOWN",
            sub_state="UNKNOWN",
            main_pid=0,
            invocation_id="",
            n_restarts=0,
            active_enter_timestamp=None,
            result=None,
            memory_current_bytes=None,
            cpu_usage_nsec=None,
            error="systemctl_not_found",
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return SystemdObservation(
            available=False,
            active_state="UNKNOWN",
            sub_state="UNKNOWN",
            main_pid=0,
            invocation_id="",
            n_restarts=0,
            active_enter_timestamp=None,
            result=None,
            memory_current_bytes=None,
            cpu_usage_nsec=None,
            error=str(exc),
        )

    parsed: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        if "=" in line:
            key, val = line.split("=", 1)
            parsed[key.strip()] = val.strip()

    main_pid = int(parsed.get("MainPID", "0") or "0")
    n_restarts = int(parsed.get("NRestarts", "0") or "0")
    mem_str = parsed.get("MemoryCurrent", "")
    mem_bytes = int(mem_str) if mem_str.isdigit() and mem_str != "[not set]" else None
    cpu_str = parsed.get("CPUUsageNSec", "")
    cpu_nsec = int(cpu_str) if cpu_str.isdigit() else None

    return SystemdObservation(
        available=proc.returncode == 0,
        active_state=parsed.get("ActiveState", "UNKNOWN"),
        sub_state=parsed.get("SubState", "UNKNOWN"),
        main_pid=main_pid,
        invocation_id=parsed.get("InvocationID", ""),
        n_restarts=n_restarts,
        active_enter_timestamp=parsed.get("ActiveEnterTimestamp") or None,
        result=parsed.get("Result") or None,
        memory_current_bytes=mem_bytes,
        cpu_usage_nsec=cpu_nsec,
        error=None if proc.returncode == 0 else (proc.stderr.strip() or "systemctl_error"),
    )


def query_status_cli(
    config_path: Path | None,
    data_root: Path,
    *,
    cli_path: str | None = None,
) -> StatusObservation:
    """Non-intrusively invoke `binance-market-recorder status`."""
    executable = cli_path or sys.executable
    cmd: list[str] = (
        [executable]
        if cli_path
        else [executable, "-m", "binance_market_data_recorder"]
    )
    environment = os.environ.copy()
    if config_path is not None:
        cmd.extend(["--config", str(config_path)])
    else:
        environment["BINANCE_MARKET_RECORDER_DATA_ROOT"] = str(data_root)
    cmd.append("status")

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=8,
            env=environment,
        )
        if proc.returncode == 0:
            payload = json.loads(proc.stdout)
            catalog = payload.get("catalog", {})
            storage = payload.get("internal_storage", {})
            metrics = payload.get("runtime_metrics", {})
            last_event_ns = metrics.get("last_event_age_ns", {}).get("value")
            last_event_sec = (
                round(float(last_event_ns) / 1e9, 3)
                if isinstance(last_event_ns, (int, float))
                else None
            )
            state = (
                payload.get("observed_service_state") or {}
                if payload.get("status") == "RUNNING" else {}
            )
            coverage = state.get("selected_asset_coverage") or {}

            return StatusObservation(
                available=True,
                service_status=str(payload.get("status", "UNKNOWN")),
                network_connected=bool(payload.get("network_connected")),
                network_status=str(payload.get("network_status", "UNKNOWN")),
                catalog_sealed_chunks=catalog.get("sealed_chunks"),
                catalog_active_chunks=catalog.get("active_chunks"),
                space_severity=storage.get("space_severity"),
                last_event_age_seconds=last_event_sec,
                detail=payload.get("detail"),
                availability_journal_pending=coverage.get("availability_journal_pending"),
                expected_product_count=state.get("expected_product_count"),
                ready_product_count=state.get("ready_product_count"),
            )
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        pass

    return StatusObservation(
        available=False,
        service_status="UNKNOWN",
        network_connected=False,
        network_status="UNKNOWN",
        catalog_sealed_chunks=None,
        catalog_active_chunks=None,
        space_severity=None,
        last_event_age_seconds=None,
        detail="status_command_failed",
    )


def query_disk(data_root: Path) -> DiskObservation:
    """Sample filesystem space via statvfs."""
    target = data_root
    while not target.exists():
        target = target.parent
    stat = os.statvfs(target)
    total_bytes = stat.f_blocks * stat.f_frsize
    free_bytes = stat.f_bfree * stat.f_frsize
    avail_bytes = stat.f_bavail * stat.f_frsize
    used_bytes = total_bytes - free_bytes
    used_pct = round((used_bytes / total_bytes) * 100.0, 2) if total_bytes > 0 else 0.0

    return DiskObservation(
        path=str(target),
        total_bytes=total_bytes,
        free_bytes=free_bytes,
        available_bytes=avail_bytes,
        used_bytes=used_bytes,
        used_percent=used_pct,
        free_gib=round(free_bytes / (1024**3), 3),
    )


def take_sample(
    *,
    sample_index: int,
    start_mono: float,
    unit: str,
    config_path: Path | None,
    data_root: Path,
    systemctl_path: str,
    cli_path: str | None,
    proc_sampler: ProcessSampler,
    chunk_tracker: RawChunkTracker,
) -> SampleRecord:
    now_mono = time.monotonic()
    timestamp_utc = datetime.now(UTC).isoformat()
    elapsed = round(now_mono - start_mono, 3)

    systemd_obs = query_systemd(unit, systemctl_path=systemctl_path)

    # Determine PID to monitor: prefer systemd MainPID; fallback to service_state.json
    target_pid = systemd_obs.main_pid
    if target_pid <= 0:
        state_file = data_root / "state" / "service_state.json"
        if state_file.is_file():
            try:
                state_data = json.loads(state_file.read_text(encoding="utf-8"))
                if isinstance(state_data.get("pid"), int):
                    target_pid = int(state_data["pid"])
            except (OSError, json.JSONDecodeError):
                pass

    proc_obs = proc_sampler.sample(target_pid)
    status_obs = query_status_cli(config_path, data_root, cli_path=cli_path)
    disk_obs = query_disk(data_root)
    chunks_obs = chunk_tracker.sample(data_root, now_mono)

    return SampleRecord(
        timestamp_utc=timestamp_utc,
        sample_index=sample_index,
        elapsed_seconds=elapsed,
        systemd=systemd_obs,
        process=proc_obs,
        status=status_obs,
        disk=disk_obs,
        raw_chunks=chunks_obs,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_VPS_CONFIG_PATH if DEFAULT_VPS_CONFIG_PATH.is_file() else None,
        help="Path to recorder.toml config file",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        help="Path to recorder persistent data root",
    )
    parser.add_argument(
        "--unit",
        type=str,
        default=DEFAULT_SERVICE_NAME,
        help="systemd service unit name",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Sampling cadence in seconds (default: 10.0)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of samples to collect before stopping (default: unlimited)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Collect a single sample, output JSON, and exit immediately",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="File path to append JSON lines (.jsonl)",
    )
    parser.add_argument(
        "--systemctl-path",
        type=str,
        default="systemctl",
        help="Command or absolute path for systemctl",
    )
    parser.add_argument(
        "--cli-path",
        type=str,
        default=None,
        help="Path to binance-market-recorder binary",
    )

    args = parser.parse_args(argv)

    if args.once:
        args.count = 1

    data_root = args.data_root
    if args.config is not None:
        try:
            with args.config.open("rb") as handle:
                parsed_toml = tomllib.load(handle)
            configured_root = Path(parsed_toml["recorder"]["data_root"]).expanduser()
        except (OSError, KeyError, tomllib.TOMLDecodeError) as exc:
            parser.error(f"cannot read recorder data_root from config: {exc}")
        if data_root is not None and data_root.resolve() != configured_root.resolve():
            parser.error("--data-root must match the recorder config data_root")
        data_root = configured_root
    data_root = (data_root or DEFAULT_VPS_DATA_ROOT).expanduser().resolve()

    if args.interval <= 0 or (args.count is not None and args.count <= 0):
        parser.error("--interval and --count must be positive")

    proc_sampler = ProcessSampler()
    chunk_tracker = RawChunkTracker()
    start_mono = time.monotonic()
    sample_index = 0

    output_handle = None
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output_handle = args.output.open("a", encoding="utf-8")

    try:
        while True:
            sample_index += 1
            record = take_sample(
                sample_index=sample_index,
                start_mono=start_mono,
                unit=args.unit,
                config_path=args.config,
                data_root=data_root,
                systemctl_path=args.systemctl_path,
                cli_path=args.cli_path,
                proc_sampler=proc_sampler,
                chunk_tracker=chunk_tracker,
            )

            record_json = json.dumps(asdict(record), separators=(",", ":"))
            print(record_json, flush=True)
            if output_handle:
                output_handle.write(record_json + "\n")
                output_handle.flush()

            if args.count is not None and sample_index >= args.count:
                break

            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        if output_handle:
            output_handle.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
