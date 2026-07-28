"""M21 soak sampling companion systemd service and timer unit management."""

from __future__ import annotations

import grp
import os
import pwd
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from ..storage.layout import fsync_directory

SOAK_SERVICE_NAME = "binance-market-data-soak-sample.service"
SOAK_TIMER_NAME = "binance-market-data-soak-sample.timer"
_SOAK_UNIT_MARKER = "# Managed by BinanceMarketDataRecorder (soak)"
_SOAK_UNIT_MARKER_BYTES = _SOAK_UNIT_MARKER.encode()

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


class SystemdSoakError(RuntimeError):
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
        raise SystemdSoakError(
            f"cannot execute {' '.join(arguments[:2])}: {type(exc).__name__}"
        ) from exc


def _quote(value: str) -> str:
    if "\n" in value or "\r" in value or "\0" in value:
        raise SystemdSoakError("argument contains forbidden control characters")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'"{escaped}"'


def _directive_path(value: str) -> str:
    if not value.startswith("/") or "\n" in value or "\r" in value or "\0" in value:
        raise SystemdSoakError("directive path must be an absolute safe path")
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


class SoakTimerManager:
    def __init__(
        self,
        *,
        config_file: Path,
        user: str,
        group: str,
        storage_id: str,
        interval_seconds: int = 300,
        output_path: Path = Path(
            "/var/lib/binance-market-data-recorder/operations/soak/samples.jsonl"
        ),
        unit_dir: Path = Path("/etc/systemd/system"),
        python_executable: Path | None = None,
        command_runner: CommandRunner = _run_command,
    ) -> None:
        self.config_file = config_file.resolve()
        self.user = user
        self.group = group
        self.storage_id = storage_id
        self.interval_seconds = int(interval_seconds)
        self.output_path = output_path.resolve()
        self.unit_dir = unit_dir.resolve()
        self.service_path = self.unit_dir / SOAK_SERVICE_NAME
        self.timer_path = self.unit_dir / SOAK_TIMER_NAME
        selected = Path(sys.executable) if python_executable is None else python_executable
        self.python_executable = selected.expanduser().absolute()
        self.command_runner = command_runner

    def _run(self, *args: str, allow_failure: bool = False) -> subprocess.CompletedProcess[str]:
        result = self.command_runner(args)
        if result.returncode != 0 and not allow_failure:
            detail = (result.stderr or result.stdout).strip()
            raise SystemdSoakError(
                f"{' '.join(args[:2])} failed ({result.returncode}): {detail}"
            )
        return result

    def _assert_install_inputs(self) -> None:
        if not sys.platform.startswith("linux"):
            raise SystemdSoakError("systemd management requires Linux")
        try:
            account = pwd.getpwnam(self.user)
            selected_group = grp.getgrnam(self.group)
        except KeyError as exc:
            raise SystemdSoakError("service user or group does not exist") from exc
        if account.pw_uid == 0 or selected_group.gr_gid == 0:
            raise SystemdSoakError("refusing to run as root")
        if not self.python_executable.is_file() or not self.python_executable.is_absolute():
            raise SystemdSoakError("Python executable must be absolute file")
        if not self.config_file.is_file():
            raise SystemdSoakError("config file does not exist")
        if self.interval_seconds <= 0:
            raise SystemdSoakError("interval-seconds must be > 0")

    def _check_managed_units(self) -> str:
        service_present = self.service_path.is_file()
        timer_present = self.timer_path.is_file()
        if not service_present and not timer_present:
            return "absent"
        if service_present and timer_present:
            svc_body = self.service_path.read_bytes()
            tmr_body = self.timer_path.read_bytes()
            if (svc_body.startswith(_SOAK_UNIT_MARKER_BYTES)
                    and tmr_body.startswith(_SOAK_UNIT_MARKER_BYTES)):
                return "managed"
        if service_present or timer_present:
            return "unmanaged"
        return "absent"

    def _render_service(self) -> str:
        cli = str(self.python_executable)
        lines = [
            _SOAK_UNIT_MARKER,
            "[Unit]",
            "Description=BinanceMarketDataRecorder M21 soak sampling",
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
                    "soak", "sample",
                    "--storage-id", self.storage_id,
                    "--output", str(self.output_path),
                ]
            ),
            "UMask=0027",
            "NoNewPrivileges=true",
            "TimeoutStartSec=30",
            "Environment=PYTHONUNBUFFERED=1",
            "",
        ]
        return "\n".join(lines)

    def _render_timer(self) -> str:
        lines = [
            _SOAK_UNIT_MARKER,
            "[Unit]",
            "Description=BinanceMarketDataRecorder periodic soak sample trigger",
            "",
            "[Timer]",
            "OnBootSec=180s",
            f"OnUnitActiveSec={self.interval_seconds}s",
            "RandomizedDelaySec=15s",
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
            raise SystemdSoakError("refusing to overwrite unmanaged soak units")
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
        self._run("/usr/bin/systemctl", "enable", SOAK_TIMER_NAME)
        return {"status": "INSTALLED", "changed": changed, **self.status()}

    def start(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "absent":
            raise SystemdSoakError("soak timer is not installed")
        if managed_state == "unmanaged":
            raise SystemdSoakError("refusing to start unmanaged soak timer")
        self._run("/usr/bin/systemctl", "start", SOAK_TIMER_NAME)
        return {"status": "START_REQUESTED", **self.status()}

    def stop(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "unmanaged":
            raise SystemdSoakError("refusing to stop unmanaged soak timer")
        was_active = self.is_active()
        if managed_state == "managed" and was_active:
            self._run("/usr/bin/systemctl", "stop", SOAK_TIMER_NAME)
        return {"status": "STOPPED", "was_active": was_active, **self.status()}

    def restart(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "absent":
            raise SystemdSoakError("soak timer is not installed")
        if managed_state == "unmanaged":
            raise SystemdSoakError("refusing to restart unmanaged soak timer")
        self._run("/usr/bin/systemctl", "restart", SOAK_TIMER_NAME)
        return {"status": "RESTART_REQUESTED", **self.status()}

    def uninstall(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        if managed_state == "unmanaged":
            raise SystemdSoakError("refusing to uninstall unmanaged soak units")
        if managed_state == "absent":
            return {
                "status": "UNINSTALLED",
                "unit_removed": False,
                "enabled": False,
                "running": False,
            }
        self.stop()
        self._run("/usr/bin/systemctl", "disable", SOAK_TIMER_NAME, allow_failure=True)
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
                      SOAK_TIMER_NAME, allow_failure=True).returncode == 0
        )

    def is_enabled(self) -> bool:
        return (
            self._run("/usr/bin/systemctl", "is-enabled", "--quiet",
                      SOAK_TIMER_NAME, allow_failure=True).returncode == 0
        )

    def status(self) -> dict[str, object]:
        managed_state = self._check_managed_units()
        installed = self.service_path.is_file() and self.timer_path.is_file()
        return {
            "unit": SOAK_TIMER_NAME,
            "service_unit": SOAK_SERVICE_NAME,
            "unit_dir": str(self.unit_dir),
            "installed": installed,
            "managed": managed_state == "managed",
            "enabled": self.is_enabled() if installed else False,
            "running": self.is_active() if installed else False,
        }
