"""Archive drain companion systemd service and timer unit management."""

from __future__ import annotations

import grp
import os
import pwd
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from ..storage.layout import fsync_directory

ARCHIVE_SERVICE_NAME = "binance-market-data-archive.service"
ARCHIVE_TIMER_NAME = "binance-market-data-archive.timer"
_ARCHIVE_UNIT_MARKER = "# Managed by BinanceMarketDataRecorder (archive)"
_ARCHIVE_UNIT_MARKER_BYTES = _ARCHIVE_UNIT_MARKER.encode()

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


class SystemdArchiveError(RuntimeError):
    pass


def _run_command(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(arguments),
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise SystemdArchiveError(
            f"cannot execute {' '.join(arguments[:2])}: {type(exc).__name__}"
        ) from exc


def _quote(value: str) -> str:
    if "\n" in value or "\r" in value or "\0" in value:
        raise SystemdArchiveError("argument contains forbidden control characters")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'"{escaped}"'


def _directive_path(value: str) -> str:
    if not value.startswith("/") or "\n" in value or "\r" in value or "\0" in value:
        raise SystemdArchiveError("directive path must be an absolute safe path")
    encoded: list[str] = []
    for byte in value.encode("utf-8"):
        character = chr(byte)
        if character.isascii() and (character.isalnum() or character in "/._-"):
            encoded.append(character)
        elif character == "%":
            encoded.append("%%")
        else:
            encoded.append(f"\\x{byte:02x}")
    return "".join(encoded)


def _atomic_write(path: Path, body: bytes, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.partial")
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(body)
            handle.flush()
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(partial, path)
    os.chmod(path, mode)
    fsync_directory(path.parent)


class ArchiveTimerManager:
    def __init__(
        self,
        *,
        config_file: Path,
        user: str,
        group: str,
        storage_id: str,
        interval_seconds: int = 60,
        max_runtime_seconds: int = 50,
        max_files: int = 1000,
        unit_dir: Path = Path("/etc/systemd/system"),
        python_executable: Path | None = None,
        command_runner: CommandRunner = _run_command,
    ) -> None:
        self.config_file = config_file.resolve()
        self.user = user
        self.group = group
        self.storage_id = storage_id
        self.interval_seconds = int(interval_seconds)
        self.max_runtime_seconds = int(max_runtime_seconds)
        self.max_files = int(max_files)
        self.unit_dir = unit_dir.resolve()
        self.service_path = self.unit_dir / ARCHIVE_SERVICE_NAME
        self.timer_path = self.unit_dir / ARCHIVE_TIMER_NAME
        selected = Path(sys.executable) if python_executable is None else python_executable
        self.python_executable = selected.expanduser().absolute()
        self.command_runner = command_runner

    def _run(self, *args: str, allow_failure: bool = False) -> subprocess.CompletedProcess[str]:
        result = self.command_runner(args)
        if result.returncode != 0 and not allow_failure:
            detail = (result.stderr or result.stdout).strip()
            raise SystemdArchiveError(
                f"{' '.join(args[:2])} failed ({result.returncode}): {detail}"
            )
        return result

    def _assert_install_inputs(self) -> None:
        if not sys.platform.startswith("linux"):
            raise SystemdArchiveError("systemd management requires Linux")
        try:
            account = pwd.getpwnam(self.user)
            selected_group = grp.getgrnam(self.group)
        except KeyError as exc:
            raise SystemdArchiveError("service user or group does not exist") from exc
        if account.pw_uid == 0 or selected_group.gr_gid == 0:
            raise SystemdArchiveError("refusing to run as root")
        if not self.python_executable.is_file() or not self.python_executable.is_absolute():
            raise SystemdArchiveError("Python executable must be absolute file")
        if not self.config_file.is_file():
            raise SystemdArchiveError("config file does not exist")
        if self.interval_seconds <= 0:
            raise SystemdArchiveError("interval-seconds must be > 0")
        if self.max_runtime_seconds <= 0:
            raise SystemdArchiveError("max-runtime-seconds must be > 0")
        if self.max_files <= 0:
            raise SystemdArchiveError("max-files must be > 0")

    def _check_managed_units(self) -> str:
        service_present = self.service_path.is_file()
        timer_present = self.timer_path.is_file()
        if not service_present and not timer_present:
            return "absent"
        if service_present and timer_present:
            svc_body = self.service_path.read_bytes()
            tmr_body = self.timer_path.read_bytes()
            if (svc_body.startswith(_ARCHIVE_UNIT_MARKER_BYTES)
                    and tmr_body.startswith(_ARCHIVE_UNIT_MARKER_BYTES)):
                return "managed"
        if service_present or timer_present:
            return "unmanaged"
        return "absent"

    def _render_service(self) -> str:
        cli = str(self.python_executable)
        env_file = "/etc/binance-market-data-recorder/archive-worker.env"
        timeout = self.max_runtime_seconds + 15
        lines = [
            _ARCHIVE_UNIT_MARKER,
            "[Unit]",
            "Description=BinanceMarketDataRecorder archive drain to external storage",
            "After=local-fs.target",
            "",
            "[Service]",
            "Type=oneshot",
            f"User={self.user}",
            f"Group={self.group}",
            f"WorkingDirectory={_directive_path('/var/tmp')}",
            "ExecStart=" + " ".join(
                _quote(a) for a in [
                    cli, "-m", "binance_market_data_recorder",
                    "--config", str(self.config_file),
                    "archive", "drain",
                    "--storage-id", self.storage_id,
                    "--max-runtime-seconds", str(self.max_runtime_seconds),
                    "--max-files", str(self.max_files),
                ]
            ),
            "UMask=0027",
            "NoNewPrivileges=true",
            "TimeoutStartSec=" + str(timeout),
            "Environment=PYTHONUNBUFFERED=1",
            f"EnvironmentFile=-{_directive_path(env_file)}",
            "",
        ]
        return "\n".join(lines)

    def _render_timer(self) -> str:
        lines = [
            _ARCHIVE_UNIT_MARKER,
            "[Unit]",
            "Description=BinanceMarketDataRecorder periodic archive drain trigger",
            "",
            "[Timer]",
            "OnBootSec=120s",
            f"OnUnitActiveSec={self.interval_seconds}s",
            "RandomizedDelaySec=10s",
            "AccuracySec=5s",
            "Persistent=true",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
        return "\n".join(lines)

    def install(self) -> dict[str, object]:
        self._assert_install_inputs()
        managed_state = self._check_managed_units()
        if managed_state == "unmanaged":
            raise SystemdArchiveError("refusing to overwrite unmanaged archive units")
        svc_body = self._render_service().encode()
        tmr_body = self._render_timer().encode()
        changed = False
        if (not self.service_path.exists()
                or self.service_path.read_bytes() != svc_body):
            _atomic_write(self.service_path, svc_body, mode=0o644)
            changed = True
        if (not self.timer_path.exists()
                or self.timer_path.read_bytes() != tmr_body):
            _atomic_write(self.timer_path, tmr_body, mode=0o644)
            changed = True
        self._run("/usr/bin/systemctl", "daemon-reload")
        self._run("/usr/bin/systemctl", "enable", ARCHIVE_TIMER_NAME)
        return {"status": "INSTALLED", "changed": changed, **self.status()}

    def start(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "absent":
            raise SystemdArchiveError("archive timer is not installed")
        if managed_state == "unmanaged":
            raise SystemdArchiveError("refusing to start unmanaged archive timer")
        self._run("/usr/bin/systemctl", "start", ARCHIVE_TIMER_NAME)
        return {"status": "START_REQUESTED", **self.status()}

    def stop(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "unmanaged":
            raise SystemdArchiveError("refusing to stop unmanaged archive timer")
        was_active = self.is_active()
        if managed_state == "managed" and was_active:
            self._run("/usr/bin/systemctl", "stop", ARCHIVE_TIMER_NAME)
        return {"status": "STOPPED", "was_active": was_active, **self.status()}

    def restart(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "absent":
            raise SystemdArchiveError("archive timer is not installed")
        if managed_state == "unmanaged":
            raise SystemdArchiveError("refusing to restart unmanaged archive timer")
        self._run("/usr/bin/systemctl", "restart", ARCHIVE_TIMER_NAME)
        return {"status": "RESTART_REQUESTED", **self.status()}

    def uninstall(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "unmanaged":
            raise SystemdArchiveError("refusing to uninstall unmanaged archive units")
        if managed_state == "absent":
            return {
                "status": "UNINSTALLED",
                "unit_removed": False,
                "enabled": False,
                "running": False,
            }
        self.stop()
        self._run("/usr/bin/systemctl", "disable", ARCHIVE_TIMER_NAME, allow_failure=True)
        removed = False
        for path in (self.service_path, self.timer_path):
            if path.exists():
                path.unlink()
                fsync_directory(path.parent)
                removed = True
        self._run("/usr/bin/systemctl", "daemon-reload")
        return {
            "status": "UNINSTALLED",
            "unit_removed": removed,
            "enabled": False,
            "running": False,
        }

    def is_active(self) -> bool:
        return (
            self._run("/usr/bin/systemctl", "is-active", "--quiet",
                      ARCHIVE_TIMER_NAME, allow_failure=True).returncode == 0
        )

    def is_enabled(self) -> bool:
        return (
            self._run("/usr/bin/systemctl", "is-enabled", "--quiet",
                      ARCHIVE_TIMER_NAME, allow_failure=True).returncode == 0
        )

    def status(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        installed = self.service_path.is_file() and self.timer_path.is_file()
        return {
            "unit": ARCHIVE_TIMER_NAME,
            "service_unit": ARCHIVE_SERVICE_NAME,
            "unit_dir": str(self.unit_dir),
            "installed": installed,
            "managed": managed_state == "managed",
            "enabled": self.is_enabled() if installed else False,
            "running": self.is_active() if installed else False,
        }
