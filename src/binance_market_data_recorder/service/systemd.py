"""System-level systemd unit rendering and idempotent lifecycle operations."""

from __future__ import annotations

import grp
import os
import pwd
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from ..storage.layout import fsync_directory

SYSTEMD_SERVICE_NAME = "binance-market-data-recorder.service"
_UNIT_MARKER = "# Managed by BinanceMarketDataRecorder"
_UNIT_MARKER_BYTES = _UNIT_MARKER.encode()
_GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{7,64}$")


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


def _directive_path(value: str) -> str:
    """Encode an absolute path for directives that don't use ExecStart parsing."""

    if not value.startswith("/") or "\n" in value or "\r" in value or "\0" in value:
        raise SystemdError("systemd directive path must be an absolute safe path")
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
        git_commit: str | None = None,
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
        if git_commit is not None and not _GIT_COMMIT_PATTERN.fullmatch(git_commit):
            raise SystemdError("Git commit provenance must be a hexadecimal revision")
        self.git_commit = git_commit

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

    def _check_managed_unit(self) -> str:
        if not self.unit_path.is_file():
            return "absent"
        existing = self.unit_path.read_bytes()
        if existing.startswith(_UNIT_MARKER_BYTES):
            return "managed"
        return "unmanaged"

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
        service_lines = [
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
            f"WorkingDirectory={_directive_path(str(self.data_root))}",
            "ExecStart=" + " ".join(_quote(argument) for argument in arguments),
            "Environment=PYTHONUNBUFFERED=1",
        ]
        if self.git_commit is not None:
            service_lines.append(
                "Environment="
                + _quote(f"BINANCE_MARKET_RECORDER_GIT_COMMIT={self.git_commit}")
            )
        service_lines.extend(
            (
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
        return "\n".join(service_lines)

    def install(self) -> dict[str, object]:
        self._assert_install_inputs()
        encoded = self.unit().encode()
        managed_state = self._check_managed_unit()
        if managed_state == "unmanaged":
            raise SystemdError("refusing to overwrite an unmanaged systemd unit")
        changed = managed_state != "managed" or self.unit_path.read_bytes() != encoded
        if changed:
            _atomic_write(self.unit_path, encoded, mode=0o644)
        self._run("/usr/bin/systemctl", "daemon-reload")
        self._run("/usr/bin/systemctl", "enable", SYSTEMD_SERVICE_NAME)
        return {"status": "INSTALLED", "changed": changed, **self.status()}

    def start(self) -> dict[str, object]:
        managed_state = self._check_managed_unit()
        if managed_state == "absent":
            raise SystemdError("systemd unit is not installed")
        if managed_state == "unmanaged":
            raise SystemdError("refusing to start an unmanaged systemd unit")
        self._run("/usr/bin/systemctl", "start", SYSTEMD_SERVICE_NAME)
        return {"status": "START_REQUESTED", **self.status()}

    def stop(self) -> dict[str, object]:
        managed_state = self._check_managed_unit()
        if managed_state == "unmanaged":
            raise SystemdError("refusing to stop an unmanaged systemd unit")
        was_active = self.is_active()
        if managed_state == "managed" and was_active:
            self._run("/usr/bin/systemctl", "stop", SYSTEMD_SERVICE_NAME)
        return {"status": "STOPPED", "was_active": was_active, **self.status()}

    def restart(self) -> dict[str, object]:
        managed_state = self._check_managed_unit()
        if managed_state == "absent":
            raise SystemdError("systemd unit is not installed")
        if managed_state == "unmanaged":
            raise SystemdError("refusing to restart an unmanaged systemd unit")
        self._run("/usr/bin/systemctl", "restart", SYSTEMD_SERVICE_NAME)
        return {"status": "RESTART_REQUESTED", **self.status()}

    def uninstall(self) -> dict[str, object]:
        managed_state = self._check_managed_unit()
        if managed_state == "unmanaged":
            raise SystemdError("refusing to uninstall an unmanaged systemd unit")
        if managed_state == "absent":
            return {
                "status": "UNINSTALLED",
                "unit_removed": False,
                "data_removed": False,
                "enabled": False,
                "running": False,
            }
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
        managed_state = self._check_managed_unit()
        installed = self.unit_path.is_file()
        return {
            "unit": SYSTEMD_SERVICE_NAME,
            "unit_path": str(self.unit_path),
            "installed": installed,
            "managed": managed_state == "managed" if installed else False,
            "enabled": self.is_enabled(),
            "running": self.is_active(),
        }
