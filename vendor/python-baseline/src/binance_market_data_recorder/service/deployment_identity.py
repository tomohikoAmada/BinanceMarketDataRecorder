"""Exact, canonical identity for the stopped VPS deployment artifact set."""

from __future__ import annotations

import base64
import grp
import hashlib
import json
import os
import pwd
import re
import stat
import sys
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from types import MappingProxyType
from urllib.parse import unquote, urlsplit
from urllib.request import url2pathname
from uuid import uuid4

from ..storage.catalog import Catalog
from ..storage.layout import fsync_directory
from ..version import DIST_NAME

DEPLOYMENT_IDENTITY_SCHEMA = "deployment-identity.v1"
DEPLOYMENT_IDENTITY_FILENAME = "deployment-identity.json"
VPS_CAPACITY_PROFILE_ID = "vps-production-v1"
VPS_ARTIFACT_ROOT = Path("/opt/binance-market-data-recorder")
VPS_CONFIG_PATH = Path("/etc/binance-market-data-recorder/recorder.toml")
VPS_DATA_ROOT = Path("/var/lib/binance-market-data-recorder")
VPS_UNIT_PATH = Path("/etc/systemd/system/binance-market-data-recorder.service")
VPS_LEGACY_RECONNECT_PATH = (
    VPS_CONFIG_PATH.parent / "legacy_reconnect_classifications.json"
)
LEGACY_RECONNECT_SIDECAR = "legacy_reconnect_classifications"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_SHA = re.compile(r"^[0-9a-f]{40}$")
_LOCKED_REQUIREMENT = re.compile(
    r"^(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)==(?P<version>[^\s;\\]+)\s*\\$"
)
_NORMALIZE_DISTRIBUTION = re.compile(r"[-_.]+")
_BOOTSTRAP_DISTRIBUTIONS = frozenset({"pip"})
_DIRECT_ENVIRONMENT = (
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
_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "source_git_sha",
        "wheel",
        "package_version",
        "python",
        "dependency_lock",
        "config",
        "systemd_unit",
        "capacity_profile_id",
        "startup_sidecars",
        "systemd_effective",
        "catalog_compatibility",
    }
)


class DeploymentIdentityError(RuntimeError):
    """Deployment evidence is absent, malformed, or does not match reality."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise DeploymentIdentityError(
            f"cannot hash deployment artifact {path}: {type(exc).__name__}"
        ) from exc
    return digest.hexdigest()


def _absolute_path(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value or not Path(value).is_absolute():
        raise DeploymentIdentityError(f"{field} must be an absolute path")
    return value


def _sha256(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise DeploymentIdentityError(f"{field} must be a lowercase SHA-256")
    return value


def _artifact(document: object, *, field: str) -> tuple[str, str]:
    if not isinstance(document, dict) or set(document) != {"path", "sha256"}:
        raise DeploymentIdentityError(f"{field} identity is malformed")
    return (
        _absolute_path(document["path"], field=f"{field}.path"),
        _sha256(document["sha256"], field=f"{field}.sha256"),
    )


def _canonical_json(document: Mapping[str, object]) -> bytes:
    return json.dumps(
        dict(document), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class DeploymentIdentity:
    source_git_sha: str
    wheel_path: str
    wheel_sha256: str
    package_version: str
    python_executable: str
    python_exact_version: str
    dependency_lock_path: str
    dependency_lock_sha256: str
    config_path: str
    config_sha256: str
    systemd_unit_path: str
    systemd_unit_sha256: str
    capacity_profile_id: str
    startup_sidecars: Mapping[str, Mapping[str, object]]
    systemd_effective: Mapping[str, object]
    remote_archive_states: tuple[str, ...]

    def document(self) -> dict[str, object]:
        return {
            "schema_version": DEPLOYMENT_IDENTITY_SCHEMA,
            "source_git_sha": self.source_git_sha,
            "wheel": {"path": self.wheel_path, "sha256": self.wheel_sha256},
            "package_version": self.package_version,
            "python": {
                "executable": self.python_executable,
                "exact_version": self.python_exact_version,
            },
            "dependency_lock": {
                "path": self.dependency_lock_path,
                "sha256": self.dependency_lock_sha256,
            },
            "config": {"path": self.config_path, "sha256": self.config_sha256},
            "systemd_unit": {
                "path": self.systemd_unit_path,
                "sha256": self.systemd_unit_sha256,
            },
            "capacity_profile_id": self.capacity_profile_id,
            "startup_sidecars": {
                name: dict(value) for name, value in sorted(self.startup_sidecars.items())
            },
            "systemd_effective": dict(self.systemd_effective),
            "catalog_compatibility": {
                "remote_archive_states": list(self.remote_archive_states)
            },
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.document())

    @property
    def identity_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    @classmethod
    def from_document(cls, document: object) -> DeploymentIdentity:
        if not isinstance(document, dict) or set(document) != _TOP_LEVEL_KEYS:
            raise DeploymentIdentityError("deployment identity fields are malformed")
        if document.get("schema_version") != DEPLOYMENT_IDENTITY_SCHEMA:
            raise DeploymentIdentityError("unsupported deployment identity schema")
        source_git_sha = document.get("source_git_sha")
        if not isinstance(source_git_sha, str) or _SOURCE_SHA.fullmatch(source_git_sha) is None:
            raise DeploymentIdentityError("source_git_sha must be a full lowercase Git SHA")
        wheel_path, wheel_sha256 = _artifact(document.get("wheel"), field="wheel")
        lock_path, lock_sha256 = _artifact(
            document.get("dependency_lock"), field="dependency_lock"
        )
        config_path, config_sha256 = _artifact(document.get("config"), field="config")
        unit_path, unit_sha256 = _artifact(
            document.get("systemd_unit"), field="systemd_unit"
        )
        package_version = document.get("package_version")
        if not isinstance(package_version, str) or not package_version:
            raise DeploymentIdentityError("package_version is malformed")
        python_document = document.get("python")
        if not isinstance(python_document, dict) or set(python_document) != {
            "executable",
            "exact_version",
        }:
            raise DeploymentIdentityError("python identity is malformed")
        python_executable = _absolute_path(
            python_document["executable"], field="python.executable"
        )
        python_exact_version = python_document["exact_version"]
        if not isinstance(python_exact_version, str) or not python_exact_version:
            raise DeploymentIdentityError("python.exact_version is malformed")
        capacity_profile_id = document.get("capacity_profile_id")
        if capacity_profile_id != VPS_CAPACITY_PROFILE_ID:
            raise DeploymentIdentityError("deployment capacity profile is not approved")
        sidecars_document = document.get("startup_sidecars")
        if not isinstance(sidecars_document, dict) or set(sidecars_document) != {
            LEGACY_RECONNECT_SIDECAR
        }:
            raise DeploymentIdentityError("startup sidecar identity is malformed")
        sidecars: dict[str, Mapping[str, object]] = {}
        for name, value in sidecars_document.items():
            if not isinstance(value, dict) or set(value) != {"path", "state", "sha256"}:
                raise DeploymentIdentityError("startup sidecar entry is malformed")
            _absolute_path(value["path"], field=f"startup_sidecars.{name}.path")
            state = value["state"]
            digest = value["sha256"]
            if state == "ABSENT" and digest is not None:
                raise DeploymentIdentityError("absent startup sidecar has a hash")
            if state == "PRESENT":
                _sha256(digest, field=f"startup_sidecars.{name}.sha256")
            elif state != "ABSENT":
                raise DeploymentIdentityError("startup sidecar state is malformed")
            sidecars[name] = MappingProxyType(dict(value))
        effective = document.get("systemd_effective")
        if not isinstance(effective, dict):
            raise DeploymentIdentityError("systemd effective identity is malformed")
        required_effective = {
            "fragment_path",
            "drop_in_paths",
            "exec_start",
            "user",
            "group",
            "restart",
            "restart_sec_usec",
            "timeout_stop_sec_usec",
            "umask",
            "no_new_privileges",
            "working_directory",
            "wants",
            "requires",
            "after",
            "environment",
            "environment_files",
            "pass_environment",
            "service_type",
            "kill_signal",
            "standard_output",
            "standard_error",
        }
        if set(effective) != required_effective:
            raise DeploymentIdentityError("systemd effective fields are malformed")
        if _absolute_path(
            effective["fragment_path"], field="systemd_effective.fragment_path"
        ) != unit_path:
            raise DeploymentIdentityError("systemd fragment does not match unit identity")
        if effective["drop_in_paths"] != []:
            raise DeploymentIdentityError("systemd drop-ins are forbidden")
        expected_exec_start = [
            python_executable,
            "-m",
            "binance_market_data_recorder",
            "--config",
            config_path,
            "_service",
            "run",
        ]
        if effective["exec_start"] != expected_exec_start:
            raise DeploymentIdentityError("systemd ExecStart identity is not canonical")
        for field in ("user", "group"):
            value = effective[field]
            if not isinstance(value, str) or not value or value == "root":
                raise DeploymentIdentityError(
                    f"systemd {field} must be an explicit non-root principal"
                )
        if effective["restart"] != "on-failure":
            raise DeploymentIdentityError("systemd restart policy is not canonical")
        if effective["restart_sec_usec"] != 10_000_000:
            raise DeploymentIdentityError("systemd restart delay is not canonical")
        if effective["timeout_stop_sec_usec"] != 90_000_000:
            raise DeploymentIdentityError("systemd stop timeout is not canonical")
        if effective["umask"] != "0027":
            raise DeploymentIdentityError("systemd UMask is not canonical")
        if effective["no_new_privileges"] is not True:
            raise DeploymentIdentityError(
                "systemd NoNewPrivileges is not canonical"
            )
        _absolute_path(
            effective["working_directory"],
            field="systemd_effective.working_directory",
        )
        for field in ("wants", "requires", "after"):
            values = effective[field]
            if (
                not isinstance(values, list)
                or any(not isinstance(value, str) or not value for value in values)
                or values != sorted(set(values))
            ):
                raise DeploymentIdentityError(
                    f"systemd {field} identity is not canonical"
                )
            if "mihomo.service" in values:
                raise DeploymentIdentityError(
                    f"systemd {field} has forbidden Mihomo authority"
                )
        if "network-online.target" not in effective["wants"]:
            raise DeploymentIdentityError("systemd network-online Wants is absent")
        if "network-online.target" not in effective["after"]:
            raise DeploymentIdentityError("systemd network-online ordering is absent")
        if effective["environment"] != list(_DIRECT_ENVIRONMENT):
            raise DeploymentIdentityError("systemd environment authority is not canonical")
        if effective["environment_files"] != []:
            raise DeploymentIdentityError("systemd environment files are forbidden")
        if effective["pass_environment"] != []:
            raise DeploymentIdentityError("systemd passed environment is forbidden")
        expected_service = {
            "service_type": "simple",
            "kill_signal": "SIGTERM",
            "standard_output": "journal",
            "standard_error": "journal",
        }
        for field, expected_value in expected_service.items():
            if effective[field] != expected_value:
                raise DeploymentIdentityError(
                    f"systemd {field} is not canonical"
                )
        compatibility = document.get("catalog_compatibility")
        if not isinstance(compatibility, dict) or set(compatibility) != {
            "remote_archive_states"
        }:
            raise DeploymentIdentityError("catalog compatibility is malformed")
        remote_states = compatibility["remote_archive_states"]
        if (
            not isinstance(remote_states, list)
            or any(not isinstance(state, str) for state in remote_states)
            or len(set(remote_states)) != len(remote_states)
        ):
            raise DeploymentIdentityError("remote archive compatibility is malformed")
        return cls(
            source_git_sha=source_git_sha,
            wheel_path=wheel_path,
            wheel_sha256=wheel_sha256,
            package_version=package_version,
            python_executable=python_executable,
            python_exact_version=python_exact_version,
            dependency_lock_path=lock_path,
            dependency_lock_sha256=lock_sha256,
            config_path=config_path,
            config_sha256=config_sha256,
            systemd_unit_path=unit_path,
            systemd_unit_sha256=unit_sha256,
            capacity_profile_id=capacity_profile_id,
            startup_sidecars=MappingProxyType(sidecars),
            systemd_effective=MappingProxyType(dict(effective)),
            remote_archive_states=tuple(remote_states),
        )


@dataclass(frozen=True, slots=True)
class RuntimeDeploymentIdentity:
    identity_sha256: str
    source_git_sha: str
    wheel_sha256: str
    config_sha256: str
    systemd_unit_sha256: str
    capacity_profile_id: str


def startup_sidecar_evidence(path: Path) -> dict[str, object]:
    selected = path.absolute()
    if not selected.exists():
        return {"path": str(selected), "state": "ABSENT", "sha256": None}
    if not selected.is_file() or selected.is_symlink():
        raise DeploymentIdentityError("startup sidecar must be an ordinary file")
    return {
        "path": str(selected),
        "state": "PRESENT",
        "sha256": sha256_file(selected),
    }


def create_deployment_identity(
    *,
    source_git_sha: str,
    wheel_path: Path,
    dependency_lock_path: Path,
    config_path: Path,
    systemd_unit_path: Path,
    capacity_profile_id: str,
    startup_sidecar_path: Path,
    systemd_effective: Mapping[str, object],
) -> DeploymentIdentity:
    document = {
        "schema_version": DEPLOYMENT_IDENTITY_SCHEMA,
        "source_git_sha": source_git_sha,
        "wheel": {
            "path": str(wheel_path.absolute()),
            "sha256": sha256_file(wheel_path),
        },
        "package_version": metadata.version(DIST_NAME),
        "python": {
            "executable": str(Path(sys.executable).absolute()),
            "exact_version": sys.version,
        },
        "dependency_lock": {
            "path": str(dependency_lock_path.absolute()),
            "sha256": sha256_file(dependency_lock_path),
        },
        "config": {
            "path": str(config_path.absolute()),
            "sha256": sha256_file(config_path),
        },
        "systemd_unit": {
            "path": str(systemd_unit_path.absolute()),
            "sha256": sha256_file(systemd_unit_path),
        },
        "capacity_profile_id": capacity_profile_id,
        "startup_sidecars": {
            LEGACY_RECONNECT_SIDECAR: startup_sidecar_evidence(startup_sidecar_path)
        },
        "systemd_effective": dict(systemd_effective),
        "catalog_compatibility": {
            "remote_archive_states": ["REMOTE_DELETE_PENDING", "REMOTE_DELETED"]
        },
    }
    return DeploymentIdentity.from_document(document)


def deployment_identity_path(config_path: Path) -> Path:
    return config_path.absolute().parent / DEPLOYMENT_IDENTITY_FILENAME


def load_deployment_identity(path: Path) -> DeploymentIdentity:
    try:
        raw = path.read_bytes()
        document = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise DeploymentIdentityError(
            f"cannot read deployment identity: {type(exc).__name__}"
        ) from exc
    identity = DeploymentIdentity.from_document(document)
    if raw != identity.canonical_bytes():
        raise DeploymentIdentityError("deployment identity is not canonical")
    return identity


def write_deployment_identity(
    path: Path,
    identity: DeploymentIdentity,
    *,
    owner_uid: int | None = None,
    group_gid: int | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    descriptor = -1
    published = False
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(temporary, flags, 0o640)
        os.fchmod(descriptor, 0o640)
        if owner_uid is not None or group_gid is not None:
            os.fchown(
                descriptor,
                -1 if owner_uid is None else owner_uid,
                -1 if group_gid is None else group_gid,
            )
        body = identity.canonical_bytes()
        view = memoryview(body)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("deployment identity write made no progress")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        published = True
        os.chmod(path, 0o640)
        fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if not published:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)


def verify_vps_identity_permissions(
    path: Path,
    *,
    expected_group: str,
) -> dict[str, object]:
    try:
        group_id = grp.getgrnam(expected_group).gr_gid
        identity_stat = path.stat()
        parent_stat = path.parent.stat()
    except (KeyError, OSError) as exc:
        raise DeploymentIdentityError(
            "cannot validate deployment identity ownership"
        ) from exc
    if not path.is_file() or path.is_symlink():
        raise DeploymentIdentityError("deployment identity is not an ordinary file")
    if (
        identity_stat.st_uid != 0
        or identity_stat.st_gid != group_id
        or identity_stat.st_mode & 0o777 != 0o640
    ):
        raise DeploymentIdentityError(
            "deployment identity must be root:service-group mode 0640"
        )
    if parent_stat.st_uid != 0 or parent_stat.st_mode & 0o022:
        raise DeploymentIdentityError(
            "deployment identity directory must be root-controlled"
        )
    return {
        "path": str(path),
        "owner_uid": identity_stat.st_uid,
        "group_gid": identity_stat.st_gid,
        "mode": "0640",
        "parent_root_controlled": True,
    }


def _verify_file(path_text: str, expected_sha256: str, *, field: str) -> None:
    path = Path(path_text)
    if not path.is_file() or path.is_symlink():
        raise DeploymentIdentityError(f"{field} is not an ordinary retained file")
    if sha256_file(path) != expected_sha256:
        raise DeploymentIdentityError(f"{field} SHA-256 mismatch")


def _verify_root_controlled(path: Path, *, field: str) -> None:
    try:
        observed = path.stat()
    except OSError as exc:
        raise DeploymentIdentityError(f"cannot inspect {field} ownership") from exc
    if observed.st_uid != 0 or observed.st_mode & 0o022:
        raise DeploymentIdentityError(
            f"{field} must be root-owned and not service-writable"
        )


def normalize_distribution_name(name: str) -> str:
    """Apply the Python packaging/PEP 503 distribution-name normalization."""

    normalized = _NORMALIZE_DISTRIBUTION.sub("-", name).casefold()
    if not normalized:
        raise DeploymentIdentityError("installed distribution name is malformed")
    return normalized


def locked_runtime_distributions(path: Path) -> dict[str, str]:
    """Parse the project's hash-locked, marker-free production requirement set."""

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise DeploymentIdentityError("production dependency lock is unreadable") from exc
    locked: dict[str, str] = {}
    current: str | None = None
    current_has_hash = False
    for line in lines:
        stripped = line.strip()
        match = _LOCKED_REQUIREMENT.fullmatch(stripped)
        if match is not None:
            if current is not None and not current_has_hash:
                raise DeploymentIdentityError(
                    f"locked distribution {current} has no SHA-256 authority"
                )
            raw_name = match.group("name")
            name = normalize_distribution_name(raw_name)
            if name in locked:
                raise DeploymentIdentityError(
                    f"duplicate normalized distribution in lock: {name}"
                )
            locked[name] = match.group("version")
            current = name
            current_has_hash = False
            continue
        if not stripped or stripped.startswith("#"):
            continue
        if current is not None and stripped.startswith("--hash=sha256:"):
            digest = stripped.removeprefix("--hash=sha256:").removesuffix("\\")
            if _SHA256.fullmatch(digest.strip()) is None:
                raise DeploymentIdentityError(
                    f"locked distribution {current} has a malformed SHA-256"
                )
            current_has_hash = True
            continue
        raise DeploymentIdentityError(
            "production dependency lock contains unsupported requirement syntax"
        )
    if current is not None and not current_has_hash:
        raise DeploymentIdentityError(
            f"locked distribution {current} has no SHA-256 authority"
        )
    if not locked:
        raise DeploymentIdentityError("production dependency lock is empty")
    return dict(sorted(locked.items()))


def _installed_distribution_inventory() -> tuple[
    dict[str, str], dict[str, metadata.Distribution]
]:
    inventory: dict[str, str] = {}
    authorities: dict[str, metadata.Distribution] = {}
    for distribution in metadata.distributions():
        raw_name = distribution.metadata.get("Name")
        if not isinstance(raw_name, str) or not raw_name:
            raise DeploymentIdentityError("installed distribution has no valid Name")
        name = normalize_distribution_name(raw_name)
        if name in inventory:
            raise DeploymentIdentityError(
                f"duplicate normalized installed distribution: {name}"
            )
        inventory[name] = distribution.version
        authorities[name] = distribution
    return inventory, authorities


def verify_installed_dependencies(
    dependency_lock_path: Path,
    *,
    installed_distributions: Mapping[str, str] | None = None,
    expected_venv: Path | None = None,
    require_root_controlled: bool = False,
) -> dict[str, object]:
    """Compare the exact locked and actual third-party runtime environments."""

    expected = locked_runtime_distributions(dependency_lock_path)
    authorities: dict[str, metadata.Distribution] = {}
    if installed_distributions is None:
        supplied, authorities = _installed_distribution_inventory()
    else:
        supplied = dict(installed_distributions)
    actual: dict[str, str] = {}
    for raw_name, version in supplied.items():
        if not isinstance(raw_name, str) or not isinstance(version, str) or not version:
            raise DeploymentIdentityError("installed distribution inventory is malformed")
        name = normalize_distribution_name(raw_name)
        if name in actual:
            raise DeploymentIdentityError(
                f"duplicate normalized installed distribution: {name}"
            )
        actual[name] = version
    recorder_name = normalize_distribution_name(DIST_NAME)
    recorder_version = actual.pop(recorder_name, None)
    bootstrap_present = sorted(set(actual) & _BOOTSTRAP_DISTRIBUTIONS)
    for name in bootstrap_present:
        actual.pop(name)
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    wrong_versions = {
        name: {"expected": expected[name], "actual": actual[name]}
        for name in sorted(set(expected) & set(actual))
        if expected[name] != actual[name]
    }
    if missing:
        raise DeploymentIdentityError(
            "locked runtime distributions are missing: " + ", ".join(missing)
        )
    if extra:
        raise DeploymentIdentityError(
            "unexpected runtime distributions are installed: " + ", ".join(extra)
        )
    if wrong_versions:
        details = ", ".join(
            f"{name}=={values['actual']} (locked {values['expected']})"
            for name, values in wrong_versions.items()
        )
        raise DeploymentIdentityError(
            "installed runtime distribution versions differ from lock: " + details
        )
    protected_files = 0
    protected_directories: set[Path] = set()
    if authorities:
        if sys.prefix == sys.base_prefix:
            raise DeploymentIdentityError(
                "installed dependency identity requires a production venv"
            )
        selected_venv = Path(sys.prefix) if expected_venv is None else expected_venv
        venv_root = Path(os.path.abspath(selected_venv))
        for name in [*expected, *bootstrap_present]:
            distribution = authorities[name]
            files = distribution.files
            if files is None:
                raise DeploymentIdentityError(
                    f"installed distribution file inventory is absent: {name}"
                )
            for entry in files:
                installed_path = Path(
                    os.path.abspath(str(distribution.locate_file(entry)))
                )
                if not installed_path.is_relative_to(venv_root):
                    raise DeploymentIdentityError(
                        f"installed distribution is outside the expected venv: {name}"
                    )
                if entry.name == "direct_url.json" and entry.parent.name.endswith(
                    ".dist-info"
                ):
                    try:
                        direct_url = json.loads(
                            installed_path.read_text(encoding="utf-8")
                        )
                    except (OSError, json.JSONDecodeError) as exc:
                        raise DeploymentIdentityError(
                            f"installed distribution provenance is unreadable: {name}"
                        ) from exc
                    if not isinstance(direct_url, dict) or "dir_info" in direct_url:
                        raise DeploymentIdentityError(
                            f"editable runtime distribution is forbidden: {name}"
                        )
                if require_root_controlled:
                    if not installed_path.is_file() or installed_path.is_symlink():
                        raise DeploymentIdentityError(
                            f"installed runtime file is not ordinary: {name}:{entry}"
                        )
                    _verify_root_controlled(
                        installed_path,
                        field=f"installed runtime file {name}:{entry}",
                    )
                    protected_directories.update(
                        _protected_directory_chain(
                            venv_root, installed_path, directory=False
                        )
                    )
                    protected_files += 1
        if require_root_controlled:
            for directory in sorted(
                protected_directories,
                key=lambda value: (len(value.parts), str(value)),
            ):
                _verify_protected_directory(directory)
    return {
        "locked_distribution_count": len(expected),
        "installed_distribution_count": len(actual),
        "locked_distributions": expected,
        "bootstrap_allowlist": sorted(_BOOTSTRAP_DISTRIBUTIONS),
        "bootstrap_present": bootstrap_present,
        "recorder_distribution_separate": recorder_version is not None,
        "editable": False,
        "protected_file_count": protected_files,
        "protected_directory_count": len(protected_directories),
        "exact_match": True,
    }


def _protected_directory_chain(root: Path, target: Path, *, directory: bool) -> list[Path]:
    selected_root = root.absolute()
    selected_target = target.absolute()
    try:
        relative = selected_target.relative_to(selected_root)
    except ValueError as exc:
        raise DeploymentIdentityError(
            f"deployment path is outside the artifact root: {selected_target}"
        ) from exc
    parts = relative.parts if directory else relative.parts[:-1]
    chain = [selected_root]
    current = selected_root
    for part in parts:
        current /= part
        chain.append(current)
    return chain


def _verify_protected_directory(path: Path) -> None:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise DeploymentIdentityError(
            f"cannot inspect controlling deployment directory {path}"
        ) from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
        raise DeploymentIdentityError(
            f"controlling deployment path is not an actual directory: {path}"
        )
    if observed.st_uid != 0 or observed.st_mode & 0o022:
        raise DeploymentIdentityError(
            f"controlling deployment directory is service-writable: {path}"
        )


def _verify_protected_file(path: Path, *, field: str) -> None:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise DeploymentIdentityError(f"cannot inspect {field}") from exc
    if stat.S_ISLNK(observed.st_mode) or not stat.S_ISREG(observed.st_mode):
        raise DeploymentIdentityError(f"{field} is not an actual ordinary file")
    if observed.st_uid != 0 or observed.st_mode & 0o022:
        raise DeploymentIdentityError(f"{field} is service-writable")


def verify_vps_control_chain(
    identity: DeploymentIdentity,
    installed_artifact: Mapping[str, object],
    *,
    service_user: str,
    service_group: str,
) -> dict[str, object]:
    """Prove root control of the namespace that can replace the venv/install."""

    try:
        account = pwd.getpwnam(service_user)
        group = grp.getgrnam(service_group)
    except KeyError as exc:
        raise DeploymentIdentityError("configured service principal is unknown") from exc
    if account.pw_uid == 0 or group.gr_gid == 0:
        raise DeploymentIdentityError("configured service principal must be non-root")
    root = VPS_ARTIFACT_ROOT.absolute()
    venv_root = root / "venv"
    python_path = Path(identity.python_executable).absolute()
    if python_path != venv_root / "bin/python":
        raise DeploymentIdentityError("Python executable is outside the production venv")
    directory_targets = [
        venv_root,
        Path(str(installed_artifact.get("dist_info_path", ""))),
    ]
    file_targets = [
        (python_path, "production Python executable"),
        (Path(identity.wheel_path), "retained Wheel"),
        (Path(identity.dependency_lock_path), "retained dependency lock"),
        (Path(str(installed_artifact.get("module_path", ""))), "installed module"),
        (
            Path(str(installed_artifact.get("direct_url_path", ""))),
            "installed Wheel provenance",
        ),
    ]
    directories: set[Path] = set()
    for target in directory_targets:
        directories.update(_protected_directory_chain(root, target, directory=True))
    for target, _field in file_targets:
        directories.update(_protected_directory_chain(root, target, directory=False))
    for directory_path in sorted(directories, key=lambda value: (len(value.parts), str(value))):
        _verify_protected_directory(directory_path)
    for file_path, field in file_targets:
        _verify_protected_file(file_path, field=field)
    return {
        "artifact_root": str(root),
        "venv_root": str(venv_root),
        "service_user": service_user,
        "service_group": service_group,
        "service_uid": account.pw_uid,
        "service_gid": group.gr_gid,
        "protected_directories": [str(path) for path in sorted(directories)],
        "symlink_free": True,
        "service_writable": False,
    }


def _direct_url_wheel_path(document: Mapping[str, object]) -> Path:
    url = document.get("url")
    if not isinstance(url, str):
        raise DeploymentIdentityError("installed direct_url URL is malformed")
    parsed = urlsplit(url)
    if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}:
        raise DeploymentIdentityError("installed artifact is not from a local Wheel")
    return Path(url2pathname(unquote(parsed.path))).absolute()


def verify_installed_artifact(
    identity: DeploymentIdentity,
    *,
    require_root_controlled: bool = False,
) -> dict[str, object]:
    if str(Path(sys.executable).absolute()) != identity.python_executable:
        raise DeploymentIdentityError("Python executable identity mismatch")
    if sys.version != identity.python_exact_version:
        raise DeploymentIdentityError("Python exact version mismatch")
    expected_venv = Path(identity.python_executable).parent.parent.absolute()
    if Path(sys.prefix).absolute() != expected_venv or sys.prefix == sys.base_prefix:
        raise DeploymentIdentityError("Python does not belong to the expected venv")
    try:
        distribution = metadata.distribution(DIST_NAME)
    except metadata.PackageNotFoundError as exc:
        raise DeploymentIdentityError("Recorder distribution is not installed") from exc
    if distribution.version != identity.package_version:
        raise DeploymentIdentityError("installed package version mismatch")
    files = distribution.files
    if files is None:
        raise DeploymentIdentityError("installed distribution file inventory is absent")
    direct_url_entries = [
        entry
        for entry in files
        if entry.name == "direct_url.json" and entry.parent.name.endswith(".dist-info")
    ]
    if len(direct_url_entries) != 1:
        raise DeploymentIdentityError("installed direct_url identity is absent or ambiguous")
    direct_url_path = Path(str(distribution.locate_file(direct_url_entries[0])))
    try:
        direct_url = json.loads(direct_url_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DeploymentIdentityError("installed direct_url identity is unreadable") from exc
    if not isinstance(direct_url, dict) or "dir_info" in direct_url:
        raise DeploymentIdentityError("editable or malformed installation is forbidden")
    if _direct_url_wheel_path(direct_url) != Path(identity.wheel_path).absolute():
        raise DeploymentIdentityError("installed Wheel provenance path mismatch")
    archive_info = direct_url.get("archive_info")
    if not isinstance(archive_info, dict):
        raise DeploymentIdentityError("installed Wheel archive identity is absent")
    hashes = archive_info.get("hashes")
    installed_hash = hashes.get("sha256") if isinstance(hashes, dict) else None
    legacy_hash = archive_info.get("hash")
    if installed_hash is None and isinstance(legacy_hash, str):
        installed_hash = legacy_hash.removeprefix("sha256=")
    if installed_hash != identity.wheel_sha256:
        raise DeploymentIdentityError("installed Wheel SHA-256 provenance mismatch")
    import binance_market_data_recorder as package

    module_path = Path(package.__file__ or "").absolute()
    expected_module_path = Path(
        str(distribution.locate_file("binance_market_data_recorder/__init__.py"))
    ).absolute()
    if module_path != expected_module_path:
        raise DeploymentIdentityError("module and distribution installation disagree")
    verified_files = 0
    for entry in files:
        installed_path = Path(str(distribution.locate_file(entry))).absolute()
        if not installed_path.is_relative_to(expected_venv):
            raise DeploymentIdentityError(
                f"installed Recorder file is outside the expected venv: {entry}"
            )
        if not installed_path.is_file() or installed_path.is_symlink():
            raise DeploymentIdentityError(
                f"installed Recorder file is not ordinary: {entry}"
            )
        if require_root_controlled:
            _verify_root_controlled(installed_path, field=f"installed file {entry}")
        file_hash = entry.hash
        if file_hash is None:
            continue
        if file_hash.mode != "sha256":
            raise DeploymentIdentityError("installed RECORD uses an unsupported hash")
        digest = hashlib.sha256(installed_path.read_bytes()).digest()
        encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        if encoded != file_hash.value:
            raise DeploymentIdentityError(f"installed RECORD mismatch: {entry}")
        if entry.size is not None and installed_path.stat().st_size != entry.size:
            raise DeploymentIdentityError(f"installed RECORD size mismatch: {entry}")
        verified_files += 1
    if verified_files == 0:
        raise DeploymentIdentityError("installed RECORD contains no verified files")
    return {
        "package_version": distribution.version,
        "module_path": str(module_path),
        "dist_info_path": str(direct_url_path.parent.absolute()),
        "direct_url_path": str(direct_url_path.absolute()),
        "venv_root": str(expected_venv),
        "record_verified_files": verified_files,
        "editable": False,
    }


def verify_identity_files(
    identity: DeploymentIdentity,
    *,
    expected_config_path: Path | None = None,
    expected_profile_id: str | None = None,
    verify_installed: bool = True,
    require_root_controlled: bool = False,
) -> dict[str, object]:
    if expected_config_path is not None and Path(identity.config_path) != Path(
        expected_config_path
    ).absolute():
        raise DeploymentIdentityError("deployment config path mismatch")
    if (
        expected_profile_id is not None
        and identity.capacity_profile_id != expected_profile_id
    ):
        raise DeploymentIdentityError("deployment capacity profile mismatch")
    _verify_file(identity.wheel_path, identity.wheel_sha256, field="Wheel")
    if require_root_controlled:
        _verify_root_controlled(Path(identity.wheel_path), field="retained Wheel")
    _verify_file(
        identity.dependency_lock_path,
        identity.dependency_lock_sha256,
        field="dependency lock",
    )
    if require_root_controlled:
        _verify_root_controlled(
            Path(identity.dependency_lock_path), field="retained dependency lock"
        )
    _verify_file(identity.config_path, identity.config_sha256, field="configuration")
    if require_root_controlled:
        _verify_root_controlled(Path(identity.config_path), field="configuration")
    _verify_file(
        identity.systemd_unit_path,
        identity.systemd_unit_sha256,
        field="systemd unit",
    )
    if require_root_controlled:
        _verify_root_controlled(Path(identity.systemd_unit_path), field="systemd unit")
    for name, evidence in identity.startup_sidecars.items():
        actual = startup_sidecar_evidence(Path(str(evidence["path"])))
        if actual != dict(evidence):
            raise DeploymentIdentityError(f"startup sidecar mismatch: {name}")
        if require_root_controlled and actual["state"] == "PRESENT":
            _verify_root_controlled(
                Path(str(actual["path"])), field=f"startup sidecar {name}"
            )
    installed_dependencies = (
        verify_installed_dependencies(
            Path(identity.dependency_lock_path),
            expected_venv=Path(identity.python_executable).parent.parent,
            require_root_controlled=require_root_controlled,
        )
        if verify_installed
        else None
    )
    installed_artifact = (
        verify_installed_artifact(
            identity, require_root_controlled=require_root_controlled
        )
        if verify_installed
        else None
    )
    control_chain = None
    if require_root_controlled and installed_artifact is not None:
        control_chain = verify_vps_control_chain(
            identity,
            installed_artifact,
            service_user=str(identity.systemd_effective.get("user", "")),
            service_group=str(identity.systemd_effective.get("group", "")),
        )
    return {
        "identity_sha256": identity.identity_sha256,
        "source_git_sha": identity.source_git_sha,
        "wheel_sha256": identity.wheel_sha256,
        "dependency_lock_sha256": identity.dependency_lock_sha256,
        "config_sha256": identity.config_sha256,
        "systemd_unit_sha256": identity.systemd_unit_sha256,
        "capacity_profile_id": identity.capacity_profile_id,
        "startup_sidecars": {
            name: dict(value) for name, value in identity.startup_sidecars.items()
        },
        "installed_dependencies": installed_dependencies,
        "installed_artifact": installed_artifact,
        "venv_control_chain": control_chain,
    }


def verify_retained_rollback_artifacts(
    identity: DeploymentIdentity,
) -> dict[str, object]:
    _verify_file(identity.wheel_path, identity.wheel_sha256, field="rollback Wheel")
    _verify_root_controlled(Path(identity.wheel_path), field="rollback Wheel")
    _verify_file(
        identity.dependency_lock_path,
        identity.dependency_lock_sha256,
        field="rollback dependency lock",
    )
    _verify_root_controlled(
        Path(identity.dependency_lock_path), field="rollback dependency lock"
    )
    return {
        "wheel_sha256": identity.wheel_sha256,
        "dependency_lock_sha256": identity.dependency_lock_sha256,
        "root_controlled": True,
    }


def runtime_deployment_identity(identity: DeploymentIdentity) -> RuntimeDeploymentIdentity:
    return RuntimeDeploymentIdentity(
        identity_sha256=identity.identity_sha256,
        source_git_sha=identity.source_git_sha,
        wheel_sha256=identity.wheel_sha256,
        config_sha256=identity.config_sha256,
        systemd_unit_sha256=identity.systemd_unit_sha256,
        capacity_profile_id=identity.capacity_profile_id,
    )


def enforce_vps_paths(identity: DeploymentIdentity) -> None:
    if Path(identity.python_executable) != VPS_ARTIFACT_ROOT / "venv/bin/python":
        raise DeploymentIdentityError("VPS Python executable path is not canonical")
    for field, value in (
        ("Wheel", identity.wheel_path),
        ("dependency lock", identity.dependency_lock_path),
    ):
        selected = Path(value)
        if (
            selected != selected.resolve()
            or not selected.is_relative_to(VPS_ARTIFACT_ROOT)
        ):
            raise DeploymentIdentityError(f"{field} is outside the VPS artifact root")
    if Path(identity.config_path) != VPS_CONFIG_PATH:
        raise DeploymentIdentityError("VPS config path is not canonical")
    if Path(identity.systemd_unit_path) != VPS_UNIT_PATH:
        raise DeploymentIdentityError("VPS unit path is not canonical")
    sidecar = identity.startup_sidecars.get(LEGACY_RECONNECT_SIDECAR)
    if sidecar is None or Path(str(sidecar.get("path"))) != VPS_LEGACY_RECONNECT_PATH:
        raise DeploymentIdentityError("VPS startup sidecar path is not canonical")
    if identity.systemd_effective.get("working_directory") != str(VPS_DATA_ROOT):
        raise DeploymentIdentityError("VPS data root is not canonical")


def rollback_compatibility(
    target: DeploymentIdentity,
    catalog: Catalog,
) -> dict[str, object]:
    rows = catalog.remote_archive_transactions()
    present_states = sorted({str(row["state"]) for row in rows})
    supported = set(target.remote_archive_states)
    unsupported = sorted(set(present_states) - supported)
    if unsupported:
        raise DeploymentIdentityError(
            "rollback target cannot interpret durable remote states: "
            + ", ".join(unsupported)
        )
    return {
        "compatible": True,
        "remote_transaction_count": len(rows),
        "durable_remote_states": present_states,
        "target_supported_remote_states": sorted(supported),
        "data_rollback": False,
    }


__all__ = [
    "DEPLOYMENT_IDENTITY_FILENAME",
    "DEPLOYMENT_IDENTITY_SCHEMA",
    "DeploymentIdentity",
    "DeploymentIdentityError",
    "RuntimeDeploymentIdentity",
    "create_deployment_identity",
    "deployment_identity_path",
    "enforce_vps_paths",
    "load_deployment_identity",
    "locked_runtime_distributions",
    "normalize_distribution_name",
    "rollback_compatibility",
    "runtime_deployment_identity",
    "sha256_file",
    "startup_sidecar_evidence",
    "verify_identity_files",
    "verify_installed_artifact",
    "verify_installed_dependencies",
    "verify_retained_rollback_artifacts",
    "verify_vps_control_chain",
    "verify_vps_identity_permissions",
    "write_deployment_identity",
]
