"""System-level systemd unit rendering and idempotent lifecycle operations."""

from __future__ import annotations

import grp
import os
import pwd
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from ..storage.layout import fsync_directory

SYSTEMD_SERVICE_NAME = "binance-market-data-recorder.service"
_UNIT_MARKER = "# Managed by BinanceMarketDataRecorder"


class SystemdError(RuntimeError):
    """A systemd operation is unsafe or failed."""


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


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
        raise SystemdError(
            f"cannot execute {' '.join(arguments[:2])}: {type(exc).__name__}"
        ) from exc


def _quote(value: str) -> str:
    if "\n" in value or "\r" in value or "\0" in value:
        raise SystemdError("systemd argument contains forbidden control characters")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'"{escaped}"'


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


class SystemdManager:
    """Manage one system unit; the Collector itself always runs unprivileged."""

    def __init__(
        self,
        *,
        data_root: Path,
        config_file: Path,
        user: str,
        group: str,
        unit_path: Path = Path("/etc/systemd/system") / SYSTEMD_SERVICE_NAME,
        python_executable: Path | None = None,
        command_runner: CommandRunner = _run_command,
    ) -> None:
        self.data_root = data_root.resolve()
        self.config_file = config_file.resolve()
        self.user = user
        self.group = group
        self.unit_path = unit_path.resolve()
        selected = Path(sys.executable) if python_executable is None else python_executable
        self.python_executable = selected.expanduser().absolute()
        self.command_runner = command_runner

    def _run(
        self,
        *arguments: str,
        allow_failure: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        result = self.command_runner(arguments)
        if result.returncode != 0 and not allow_failure:
            detail = (result.stderr or result.stdout).strip()
            raise SystemdError(
                f"{' '.join(arguments[:2])} failed ({result.returncode}): {detail}"
            )
        return result

    def _assert_install_inputs(self) -> None:
        if not sys.platform.startswith("linux"):
            raise SystemdError("systemd management requires Linux")
        try:
            account = pwd.getpwnam(self.user)
            selected_group = grp.getgrnam(self.group)
        except KeyError as exc:
            raise SystemdError("systemd service user or group does not exist") from exc
        if account.pw_uid == 0 or selected_group.gr_gid == 0:
            raise SystemdError("refusing to run the Collector as root")
        if not self.python_executable.is_file() or not self.python_executable.is_absolute():
            raise SystemdError("systemd Python executable must be an existing absolute file")
        if not self.config_file.is_file():
            raise SystemdError("systemd configuration file does not exist")
        if not self.data_root.is_dir():
            raise SystemdError("systemd data root does not exist")
        if self.data_root.stat().st_uid != account.pw_uid:
            raise SystemdError("systemd data root is not owned by the service user")
        config_mode = self.config_file.stat().st_mode & 0o777
        if config_mode & 0o007:
            raise SystemdError("systemd configuration must not be accessible by others")

    def unit(self) -> str:
        arguments = [
            str(self.python_executable),
            "-m",
            "binance_market_data_recorder",
            "--config",
            str(self.config_file),
            "_service",
            "run",
        ]
        return "\n".join(
            (
                _UNIT_MARKER,
                "[Unit]",
                "Description=Binance public market data recorder",
                "After=network-online.target mihomo.service",
                "Wants=network-online.target mihomo.service",
                "",
                "[Service]",
                "Type=simple",
                f"User={self.user}",
                f"Group={self.group}",
                f"WorkingDirectory={_quote(str(self.data_root))}",
                "ExecStart=" + " ".join(_quote(argument) for argument in arguments),
                "Environment=PYTHONUNBUFFERED=1",
                "Restart=on-failure",
                "RestartSec=10s",
                "TimeoutStopSec=90s",
                "KillSignal=SIGTERM",
                "UMask=0027",
                "NoNewPrivileges=true",
                "",
                "[Install]",
                "WantedBy=multi-user.target",
                "",
            )
        )

    def install(self) -> dict[str, object]:
        self._assert_install_inputs()
        encoded = self.unit().encode()
        changed = True
        if self.unit_path.is_file():
            existing = self.unit_path.read_bytes()
            if existing == encoded:
                changed = False
            elif not existing.startswith(_UNIT_MARKER.encode()):
                raise SystemdError("refusing to overwrite an unmanaged systemd unit")
        if changed:
            _atomic_write(self.unit_path, encoded, mode=0o644)
        self._run("/usr/bin/systemctl", "daemon-reload")
        self._run("/usr/bin/systemctl", "enable", SYSTEMD_SERVICE_NAME)
        return {"status": "INSTALLED", "changed": changed, **self.status()}

    def start(self) -> dict[str, object]:
        if not self.unit_path.is_file():
            raise SystemdError("systemd unit is not installed")
        self._run("/usr/bin/systemctl", "start", SYSTEMD_SERVICE_NAME)
        return {"status": "START_REQUESTED", **self.status()}

    def stop(self) -> dict[str, object]:
        was_active = self.is_active()
        if was_active:
            self._run("/usr/bin/systemctl", "stop", SYSTEMD_SERVICE_NAME)
        return {"status": "STOPPED", "was_active": was_active, **self.status()}

    def restart(self) -> dict[str, object]:
        if not self.unit_path.is_file():
            raise SystemdError("systemd unit is not installed")
        self._run("/usr/bin/systemctl", "restart", SYSTEMD_SERVICE_NAME)
        return {"status": "RESTART_REQUESTED", **self.status()}

    def uninstall(self) -> dict[str, object]:
        self.stop()
        self._run(
            "/usr/bin/systemctl",
            "disable",
            SYSTEMD_SERVICE_NAME,
            allow_failure=True,
        )
        removed = False
        if self.unit_path.exists():
            self.unit_path.unlink()
            fsync_directory(self.unit_path.parent)
            removed = True
        self._run("/usr/bin/systemctl", "daemon-reload")
        return {
            "status": "UNINSTALLED",
            "unit_removed": removed,
            "data_removed": False,
            "enabled": False,
            "running": False,
        }

    def is_active(self) -> bool:
        return (
            self._run(
                "/usr/bin/systemctl",
                "is-active",
                "--quiet",
                SYSTEMD_SERVICE_NAME,
                allow_failure=True,
            ).returncode
            == 0
        )

    def is_enabled(self) -> bool:
        return (
            self._run(
                "/usr/bin/systemctl",
                "is-enabled",
                "--quiet",
                SYSTEMD_SERVICE_NAME,
                allow_failure=True,
            ).returncode
            == 0
        )

    def status(self) -> dict[str, object]:
        return {
            "unit": SYSTEMD_SERVICE_NAME,
            "unit_path": str(self.unit_path),
            "installed": self.unit_path.is_file(),
            "enabled": self.is_enabled(),
            "running": self.is_active(),
        }
