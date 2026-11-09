"""User LaunchAgent rendering and `launchctl` lifecycle operations."""

from __future__ import annotations

import json
import os
import plistlib
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

from ..config import ALLOWED_ENV_SETTINGS, CONFIG_FILE_ENV
from ..storage.layout import ensure_storage_layout, fsync_directory

_LABEL_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9-]*(?:\.[A-Za-z0-9][A-Za-z0-9-]*){2,}$"
)
_FORBIDDEN_PREFIXES = tuple(
    f"{root}.{'binance'}." for root in ("com", "org", "io")
)
_PLACEHOLDER_COMPONENTS = frozenset({"example", "invalid", "localhost", "changeme"})
_PROJECT_LABEL_SUFFIX = ".BinanceMarketDataRecorder"
LAUNCHAGENT_METADATA_SCHEMA = "launchagent-install.v1"


class LaunchAgentError(RuntimeError):
    """A LaunchAgent operation is unsafe or rejected by macOS."""


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _run_command(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(arguments),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LaunchAgentError(
            f"cannot execute {' '.join(arguments[:2])}: {type(exc).__name__}"
        ) from exc


def validate_service_label(label: str) -> str:
    """Require an explicit author-controlled reverse-DNS label."""

    if not _LABEL_PATTERN.fullmatch(label):
        raise LaunchAgentError(
            "service label must be an author-controlled reverse-DNS name"
        )
    lowered = label.casefold()
    if lowered.startswith(_FORBIDDEN_PREFIXES):
        raise LaunchAgentError("Binance-owned-looking service namespaces are forbidden")
    if set(lowered.split(".")) & _PLACEHOLDER_COMPONENTS:
        raise LaunchAgentError("placeholder service namespaces are forbidden")
    if not label.endswith(_PROJECT_LABEL_SUFFIX):
        raise LaunchAgentError(
            f"service label must end with {_PROJECT_LABEL_SUFFIX}"
        )
    return label


def _atomic_write(path: Path, body: bytes, *, mode: int) -> None:
    partial = path.with_name(f".{path.name}.partial")
    descriptor = os.open(
        partial,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        mode,
    )
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


def installed_service_label(data_root: Path) -> str | None:
    path = data_root / "state" / "launchagent.json"
    if not path.is_file():
        return None
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LaunchAgentError(
            f"cannot read LaunchAgent metadata: {type(exc).__name__}"
        ) from exc
    if (
        not isinstance(document, dict)
        or document.get("schema_version") != LAUNCHAGENT_METADATA_SCHEMA
        or not isinstance(document.get("label"), str)
    ):
        raise LaunchAgentError("invalid LaunchAgent metadata")
    return validate_service_label(document["label"])


class LaunchAgentManager:
    """Install and control one user-owned LaunchAgent without root."""

    def __init__(
        self,
        *,
        data_root: Path,
        label: str,
        home: Path | None = None,
        uid: int | None = None,
        command_runner: CommandRunner = _run_command,
        python_executable: Path | None = None,
    ) -> None:
        self.data_root = data_root.resolve()
        self.label = validate_service_label(label)
        self.home = (home or Path.home()).resolve()
        self.uid = os.getuid() if uid is None else uid
        self.command_runner = command_runner
        selected_python = (
            Path(sys.executable) if python_executable is None else python_executable
        )
        # A virtual environment's interpreter is normally a symlink. Resolving
        # it would make launchd execute the base interpreter and could load a
        # different installed Recorder version.
        self.python_executable = selected_python.expanduser().absolute()
        self.launch_agents = self.home / "Library" / "LaunchAgents"
        self.plist_path = self.launch_agents / f"{self.label}.plist"
        self.metadata_path = self.data_root / "state" / "launchagent.json"

    @property
    def domain(self) -> str:
        return f"gui/{self.uid}"

    @property
    def service_target(self) -> str:
        return f"{self.domain}/{self.label}"

    def _run(
        self, *arguments: str, allow_failure: bool = False
    ) -> subprocess.CompletedProcess[str]:
        result = self.command_runner(arguments)
        if result.returncode != 0 and not allow_failure:
            detail = (result.stderr or result.stdout).strip()
            raise LaunchAgentError(
                f"{' '.join(arguments[:2])} failed ({result.returncode}): {detail}"
            )
        return result

    def _assert_platform(self) -> None:
        if sys.platform != "darwin":
            raise LaunchAgentError("LaunchAgent management requires macOS")
        if self.uid == 0:
            raise LaunchAgentError("refusing to install a root LaunchAgent")

    def _assert_secure_permissions(self, config_file: Path | None) -> None:
        layout = ensure_storage_layout(self.data_root)
        logs = layout.root / "logs"
        logs.mkdir(mode=0o700, exist_ok=True)
        for directory in (layout.root, layout.state, logs):
            stat = directory.stat()
            if stat.st_uid != self.uid:
                raise LaunchAgentError(f"path is not owned by the current user: {directory}")
            if stat.st_mode & 0o077:
                raise LaunchAgentError(f"path permissions must exclude group/other: {directory}")
        if config_file is None:
            return
        config = config_file.resolve()
        if not config.is_file():
            raise LaunchAgentError(f"configuration file does not exist: {config}")
        stat = config.stat()
        if stat.st_uid != self.uid or stat.st_mode & 0o077:
            raise LaunchAgentError(
                "configuration file must be current-user-owned and mode 0600"
            )

    def _assert_metadata_label_compatible(self) -> None:
        installed_label = installed_service_label(self.data_root)
        if installed_label is not None and installed_label != self.label:
            raise LaunchAgentError(
                "data root is registered to a different LaunchAgent label"
            )

    def plist(
        self,
        *,
        config_file: Path | None,
        git_commit: str | None,
        environment: Mapping[str, str],
    ) -> dict[str, object]:
        arguments = [
            str(self.python_executable),
            "-m",
            "binance_market_data_recorder",
        ]
        if config_file is not None:
            arguments.extend(("--config", str(config_file.resolve())))
        arguments.extend(("_service", "run"))
        invalid_environment = sorted(
            key
            for key in environment
            if key not in ALLOWED_ENV_SETTINGS or key == CONFIG_FILE_ENV
        )
        if invalid_environment:
            raise LaunchAgentError(
                f"invalid LaunchAgent environment: {', '.join(invalid_environment)}"
            )
        launch_environment = {"PYTHONUNBUFFERED": "1", **environment}
        if git_commit is not None:
            launch_environment["BINANCE_MARKET_RECORDER_GIT_COMMIT"] = git_commit
        return {
            "Label": self.label,
            "Program": str(self.python_executable),
            "ProgramArguments": arguments,
            "RunAtLoad": True,
            "KeepAlive": {"SuccessfulExit": False},
            "ThrottleInterval": 10,
            "ExitTimeOut": 60,
            "ProcessType": "Standard",
            "LimitLoadToSessionType": "Aqua",
            "WorkingDirectory": str(self.data_root),
            "StandardOutPath": str(self.data_root / "logs" / "launchd.stdout.log"),
            "StandardErrorPath": str(self.data_root / "logs" / "launchd.stderr.log"),
            "EnvironmentVariables": launch_environment,
            "Umask": 63,
        }

    def is_loaded(self) -> bool:
        return self._run(
            "/bin/launchctl",
            "print",
            self.service_target,
            allow_failure=True,
        ).returncode == 0

    def install(
        self,
        *,
        author_controls_namespace: bool,
        config_file: Path | None,
        git_commit: str | None,
        environment: Mapping[str, str],
    ) -> dict[str, object]:
        self._assert_platform()
        if not author_controls_namespace:
            raise LaunchAgentError(
                "installation requires explicit author namespace ownership confirmation"
            )
        self._assert_secure_permissions(config_file)
        self._assert_metadata_label_compatible()
        if not self.python_executable.is_file() or not self.python_executable.is_absolute():
            raise LaunchAgentError(
                "LaunchAgent Python executable must be an existing absolute file"
            )
        self.launch_agents.mkdir(mode=0o700, parents=True, exist_ok=True)
        document = self.plist(
            config_file=config_file,
            git_commit=git_commit,
            environment=environment,
        )
        encoded = plistlib.dumps(document, fmt=plistlib.FMT_XML, sort_keys=True)
        created_plist = False
        bootstrapped = False
        if self.plist_path.exists():
            if self.plist_path.read_bytes() != encoded:
                raise LaunchAgentError("refusing to overwrite a different LaunchAgent plist")
        else:
            _atomic_write(self.plist_path, encoded, mode=0o600)
            created_plist = True
        try:
            self._run("/usr/bin/plutil", "-lint", str(self.plist_path))
            if not self.is_loaded():
                self._run(
                    "/bin/launchctl",
                    "bootstrap",
                    self.domain,
                    str(self.plist_path),
                )
                bootstrapped = True
            metadata = {
                "schema_version": LAUNCHAGENT_METADATA_SCHEMA,
                "label": self.label,
                "plist_path": str(self.plist_path),
                "domain": self.domain,
                "config_file": str(config_file.resolve()) if config_file else None,
            }
            _atomic_write(
                self.metadata_path,
                json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode(),
                mode=0o600,
            )
        except BaseException:
            if bootstrapped:
                self._run(
                    "/bin/launchctl",
                    "bootout",
                    self.service_target,
                    allow_failure=True,
                )
            if created_plist and self.plist_path.exists():
                self.plist_path.unlink()
                fsync_directory(self.plist_path.parent)
            raise
        return {
            "status": "INSTALLED",
            "label": self.label,
            "plist_path": str(self.plist_path),
            "loaded": True,
            "root": False,
        }

    def start(self) -> dict[str, object]:
        self._assert_platform()
        self._assert_metadata_label_compatible()
        if not self.plist_path.is_file():
            raise LaunchAgentError("LaunchAgent plist is not installed")
        if self.is_loaded():
            self._run("/bin/launchctl", "kickstart", self.service_target)
        else:
            self._run(
                "/bin/launchctl",
                "bootstrap",
                self.domain,
                str(self.plist_path),
            )
        return {"status": "START_REQUESTED", **self.status()}

    def stop(self) -> dict[str, object]:
        self._assert_platform()
        self._assert_metadata_label_compatible()
        was_loaded = self.is_loaded()
        if was_loaded:
            self._run("/bin/launchctl", "bootout", self.service_target)
        return {
            "status": "STOPPED",
            "label": self.label,
            "plist_path": str(self.plist_path),
            "was_loaded": was_loaded,
            "loaded": False,
        }

    def uninstall(self) -> dict[str, object]:
        self._assert_platform()
        self._assert_metadata_label_compatible()
        self.stop()
        plist_removed = False
        metadata_removed = False
        if self.plist_path.exists():
            self.plist_path.unlink()
            fsync_directory(self.plist_path.parent)
            plist_removed = True
        if self.metadata_path.exists():
            self.metadata_path.unlink()
            fsync_directory(self.metadata_path.parent)
            metadata_removed = True
        return {
            "status": "UNINSTALLED",
            "label": self.label,
            "plist_removed": plist_removed,
            "metadata_removed": metadata_removed,
            "loaded": False,
        }

    def status(self) -> dict[str, object]:
        loaded = self.is_loaded()
        return {
            "status": "LOADED" if loaded else "UNLOADED",
            "label": self.label,
            "domain": self.domain,
            "service_target": self.service_target,
            "plist_path": str(self.plist_path),
            "plist_installed": self.plist_path.is_file(),
            "loaded": loaded,
        }
