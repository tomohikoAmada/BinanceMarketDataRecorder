"""System-level systemd unit rendering and idempotent lifecycle operations."""

from __future__ import annotations

import grp
import os
import pwd
import re
import shlex
import stat
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

from ..storage.layout import fsync_directory

SYSTEMD_SERVICE_NAME = "binance-market-data-recorder.service"
VPS_CAPACITY_PROFILE_ID = "vps-production-v1"
VPS_ARTIFACT_ROOT = Path("/opt/binance-market-data-recorder")
VPS_CONFIG_PATH = Path("/etc/binance-market-data-recorder/recorder.toml")
VPS_DATA_ROOT = Path("/var/lib/binance-market-data-recorder")
VPS_PYTHON_PATH = VPS_ARTIFACT_ROOT / "venv/bin/python"
VPS_UNIT_PATH = Path("/etc/systemd/system") / SYSTEMD_SERVICE_NAME
_UNIT_MARKER = "# Managed by BinanceMarketDataRecorder"
_UNIT_MARKER_BYTES = _UNIT_MARKER.encode()
_GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{7,64}$")
_SYSTEMD_DURATION = re.compile(
    r"(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>us|ms|s|min|h)"
)
_VPS_DIRECT_ENVIRONMENT = (
    "ALL_PROXY=",
    "HTTPS_PROXY=",
    "HTTP_PROXY=",
    "NO_PROXY=",
    "PYTHONUNBUFFERED=1",
    "all_proxy=",
    "http_proxy=",
    "https_proxy=",
    "no_proxy=",
)


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


def _systemd_words(value: str, *, field: str) -> list[str]:
    if value in {"", "(null)"}:
        return []
    try:
        words = shlex.split(value)
    except ValueError as exc:
        raise SystemdError(f"effective systemd {field} is malformed") from exc
    if any(not word for word in words):
        raise SystemdError(f"effective systemd {field} is malformed")
    return sorted(set(words))


def _systemd_duration_usec(value: str, *, field: str) -> int:
    multipliers = {
        "us": 1,
        "ms": 1_000,
        "s": 1_000_000,
        "min": 60_000_000,
        "h": 3_600_000_000,
    }
    position = 0
    total = 0.0
    matched = False
    while position < len(value):
        while position < len(value) and value[position].isspace():
            position += 1
        match = _SYSTEMD_DURATION.match(value, position)
        if match is None:
            raise SystemdError(f"effective systemd {field} is malformed")
        matched = True
        total += float(match.group("value")) * multipliers[match.group("unit")]
        position = match.end()
    if not matched or not total.is_integer():
        raise SystemdError(f"effective systemd {field} is malformed")
    return int(total)


def _effective_exec_start(value: str) -> list[str]:
    match = re.fullmatch(
        r"\{\s*path=(?P<path>[^;\s]+)\s*;\s*"
        r"argv\[\]=(?P<argv>.*?)\s*;\s*(?P<metadata>[^{}]*)\}",
        value,
    )
    if match is None:
        raise SystemdError("effective systemd ExecStart is malformed")
    try:
        arguments = shlex.split(match.group("argv"))
    except ValueError as exc:
        raise SystemdError("effective systemd ExecStart is malformed") from exc
    if not arguments or match.group("path") != arguments[0]:
        raise SystemdError("effective systemd ExecStart path/argv disagree")
    return arguments


def _effective_environment(value: str) -> list[str]:
    entries = _systemd_words(value, field="Environment")
    names: set[str] = set()
    for entry in entries:
        name, separator, _entry_value = entry.partition("=")
        if not separator or not name or name in names:
            raise SystemdError("effective systemd Environment is malformed")
        names.add(name)
    return entries


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
        capacity_profile_id: str | None = None,
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
        if capacity_profile_id not in {None, VPS_CAPACITY_PROFILE_ID}:
            raise SystemdError("unknown systemd capacity profile input")
        self.capacity_profile_id = capacity_profile_id

    @property
    def is_vps_profile(self) -> bool:
        return self.capacity_profile_id == VPS_CAPACITY_PROFILE_ID

    def exec_start_arguments(self) -> tuple[str, ...]:
        return (
            str(self.python_executable),
            "-m",
            "binance_market_data_recorder",
            "--config",
            str(self.config_file),
            "_service",
            "run",
        )

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
        if self.is_vps_profile:
            self._assert_vps_install_inputs(account.pw_uid, selected_group.gr_gid)

    def _assert_vps_install_inputs(self, service_uid: int, service_gid: int) -> None:
        if self.unit_path != VPS_UNIT_PATH:
            raise SystemdError("VPS unit path must be the canonical system unit path")
        if self.python_executable != VPS_PYTHON_PATH:
            raise SystemdError("VPS Python must be the canonical production venv executable")
        if self.config_file != VPS_CONFIG_PATH:
            raise SystemdError("VPS configuration path is not canonical")
        if self.data_root != VPS_DATA_ROOT:
            raise SystemdError("VPS data root is not canonical")
        config = self.config_file.stat()
        if config.st_uid != 0 or config.st_gid != service_gid or config.st_mode & 0o777 != 0o640:
            raise SystemdError("VPS configuration must be root:service-group mode 0640")
        config_directory = self.config_file.parent.stat()
        if config_directory.st_uid != 0 or config_directory.st_mode & 0o022:
            raise SystemdError("VPS configuration directory must be root-controlled")
        data = self.data_root.stat()
        if (
            data.st_uid != service_uid
            or data.st_gid != service_gid
            or data.st_mode & 0o777 != 0o750
        ):
            raise SystemdError("VPS data root must be service-owned mode 0750")
        for directory in (
            VPS_ARTIFACT_ROOT,
            VPS_ARTIFACT_ROOT / "venv",
            VPS_ARTIFACT_ROOT / "venv/bin",
        ):
            try:
                observed = directory.lstat()
            except OSError as exc:
                raise SystemdError(
                    f"cannot inspect VPS controlling directory {directory}"
                ) from exc
            if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
                raise SystemdError(
                    "VPS controlling deployment path must be an actual directory"
                )
            if observed.st_uid != 0 or observed.st_mode & 0o022:
                raise SystemdError(
                    "VPS controlling deployment directory must not be service-writable"
                )
        executable = self.python_executable.lstat()
        if not stat.S_ISREG(executable.st_mode) or stat.S_ISLNK(executable.st_mode):
            raise SystemdError("VPS Python executable must be an ordinary copied file")
        if executable.st_uid != 0 or executable.st_mode & 0o022:
            raise SystemdError("VPS Python executable must not be service-writable")

    def verify_install_contract(self) -> dict[str, object]:
        self._assert_install_inputs()
        if not self.is_vps_profile:
            raise SystemdError("VPS install verification requires the VPS profile")
        if not self.unit_path.is_file() or self.unit_path.is_symlink():
            raise SystemdError("VPS unit must be an ordinary installed file")
        unit = self.unit_path.stat()
        if unit.st_uid != 0 or unit.st_mode & 0o777 != 0o644:
            raise SystemdError("VPS unit must be root-owned mode 0644")
        return {
            "artifact_root": str(VPS_ARTIFACT_ROOT),
            "config_path": str(self.config_file),
            "data_root": str(self.data_root),
            "python_executable": str(self.python_executable),
            "unit_path": str(self.unit_path),
            "service_user": self.user,
            "service_group": self.group,
            "config_mode": "0640",
            "data_mode": "0750",
            "unit_mode": "0644",
            "service_non_root": True,
        }

    def _check_managed_unit(self) -> str:
        if not self.unit_path.is_file():
            return "absent"
        existing = self.unit_path.read_bytes()
        if existing.startswith(_UNIT_MARKER_BYTES):
            return "managed"
        return "unmanaged"

    def unit(self) -> str:
        arguments = self.exec_start_arguments()
        dependencies = (
            "network-online.target"
            if self.is_vps_profile
            else "network-online.target mihomo.service"
        )
        service_lines = [
            _UNIT_MARKER,
            "[Unit]",
            "Description=Binance public market data recorder",
            f"After={dependencies}",
            f"Wants={dependencies}",
            "",
            "[Service]",
            "Type=simple",
            f"User={self.user}",
            f"Group={self.group}",
            f"WorkingDirectory={_directive_path(str(self.data_root))}",
            "ExecStart=" + " ".join(_quote(argument) for argument in arguments),
            "Environment=PYTHONUNBUFFERED=1",
        ]
        if self.is_vps_profile:
            service_lines.extend(
                f"Environment={name}="
                for name in (
                    "HTTP_PROXY",
                    "HTTPS_PROXY",
                    "ALL_PROXY",
                    "NO_PROXY",
                    "http_proxy",
                    "https_proxy",
                    "all_proxy",
                    "no_proxy",
                )
            )
        if self.git_commit is not None and not self.is_vps_profile:
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
                "StandardOutput=journal",
                "StandardError=journal",
                "",
                "[Install]",
                "WantedBy=multi-user.target",
                "",
            )
        )
        return "\n".join(service_lines)

    def _show_properties(self) -> dict[str, str]:
        names = (
            "FragmentPath",
            "DropInPaths",
            "ExecStart",
            "User",
            "Group",
            "Restart",
            "RestartUSec",
            "TimeoutStopUSec",
            "UMask",
            "NoNewPrivileges",
            "WorkingDirectory",
            "Wants",
            "Requires",
            "After",
            "Environment",
            "EnvironmentFiles",
            "PassEnvironment",
            "Type",
            "KillSignal",
            "StandardOutput",
            "StandardError",
            "ActiveState",
            "SubState",
            "MainPID",
            "Result",
        )
        result = self._run(
            "/usr/bin/systemctl",
            "show",
            SYSTEMD_SERVICE_NAME,
            "--no-pager",
            *(f"--property={name}" for name in names),
        )
        output: dict[str, str] = {}
        for line in result.stdout.splitlines():
            if "=" not in line:
                raise SystemdError("systemctl show returned malformed evidence")
            name, value = line.split("=", 1)
            if name in output:
                raise SystemdError("systemctl show returned duplicate evidence")
            output[name] = value
        if set(output) != set(names):
            raise SystemdError("systemctl show omitted required evidence")
        return output

    def expected_effective_identity(self) -> dict[str, object]:
        return {
            "fragment_path": str(self.unit_path),
            "drop_in_paths": [],
            "exec_start": list(self.exec_start_arguments()),
            "user": self.user,
            "group": self.group,
            "restart": "on-failure",
            "restart_sec_usec": 10_000_000,
            "timeout_stop_sec_usec": 90_000_000,
            "umask": "0027",
            "no_new_privileges": True,
            "working_directory": str(self.data_root),
            "wants": ["network-online.target"],
            "requires": [],
            "after": ["network-online.target"],
            "environment": list(_VPS_DIRECT_ENVIRONMENT),
            "environment_files": [],
            "pass_environment": [],
            "service_type": "simple",
            "kill_signal": "SIGTERM",
            "standard_output": "journal",
            "standard_error": "journal",
        }

    def observed_service_principal(self) -> tuple[str, str]:
        properties = self._show_properties()
        user = properties["User"]
        group = properties["Group"]
        if not user or not group or user == "root" or group == "root":
            raise SystemdError("effective systemd service principal is invalid")
        return user, group

    def verify_effective_properties(
        self,
        *,
        expected: dict[str, object] | None = None,
    ) -> dict[str, object]:
        properties = self._show_properties()
        kill_signal = properties["KillSignal"]
        if kill_signal in {"15", "SIGTERM"}:
            kill_signal = "SIGTERM"
        observed: dict[str, object] = {
            "fragment_path": properties["FragmentPath"],
            "drop_in_paths": _systemd_words(
                properties["DropInPaths"], field="DropInPaths"
            ),
            "exec_start": _effective_exec_start(properties["ExecStart"]),
            "user": properties["User"],
            "group": properties["Group"],
            "restart": properties["Restart"],
            "restart_sec_usec": _systemd_duration_usec(
                properties["RestartUSec"], field="RestartUSec"
            ),
            "timeout_stop_sec_usec": _systemd_duration_usec(
                properties["TimeoutStopUSec"], field="TimeoutStopUSec"
            ),
            "umask": properties["UMask"],
            "no_new_privileges": properties["NoNewPrivileges"].casefold()
            in {"yes", "true", "1"},
            "working_directory": properties["WorkingDirectory"],
            "wants": _systemd_words(properties["Wants"], field="Wants"),
            "requires": _systemd_words(properties["Requires"], field="Requires"),
            "after": _systemd_words(properties["After"], field="After"),
            "environment": _effective_environment(properties["Environment"]),
            "environment_files": _systemd_words(
                properties["EnvironmentFiles"], field="EnvironmentFiles"
            ),
            "pass_environment": _systemd_words(
                properties["PassEnvironment"], field="PassEnvironment"
            ),
            "service_type": properties["Type"],
            "kill_signal": kill_signal,
            "standard_output": properties["StandardOutput"],
            "standard_error": properties["StandardError"],
        }
        baseline = self.expected_effective_identity()
        semantic_fields = {
            "wants",
            "requires",
            "after",
            "drop_in_paths",
            "environment_files",
            "pass_environment",
        }
        for field, baseline_value in baseline.items():
            if field in semantic_fields:
                continue
            if observed.get(field) != baseline_value:
                raise SystemdError(f"effective systemd {field} mismatch")
        if observed["drop_in_paths"]:
            raise SystemdError("systemd drop-ins are forbidden for the VPS profile")
        wants = set(cast(list[str], observed["wants"]))
        requires = set(cast(list[str], observed["requires"]))
        after = set(cast(list[str], observed["after"]))
        if "network-online.target" not in wants or "network-online.target" not in after:
            raise SystemdError("effective network-online.target relationship is absent")
        if "mihomo.service" in wants | requires | after:
            raise SystemdError("effective Mihomo dependency is forbidden")
        if observed["environment_files"]:
            raise SystemdError("systemd EnvironmentFile authority is forbidden")
        if observed["pass_environment"]:
            raise SystemdError("systemd PassEnvironment authority is forbidden")
        if expected is not None and observed != expected:
            raise SystemdError("effective systemd identity differs from deployment identity")
        return observed

    def runtime_properties(self) -> dict[str, object]:
        properties = self._show_properties()
        try:
            main_pid = int(properties["MainPID"])
        except ValueError as exc:
            raise SystemdError("effective systemd MainPID is malformed") from exc
        return {
            "active_state": properties["ActiveState"],
            "sub_state": properties["SubState"],
            "main_pid": main_pid,
            "result": properties["Result"],
        }

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
