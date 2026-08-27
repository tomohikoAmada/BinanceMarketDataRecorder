from __future__ import annotations

import base64
import hashlib
import json
import stat
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.service.deployment_identity import (
    DeploymentIdentity,
    create_deployment_identity,
    verify_identity_files,
)
from binance_market_data_recorder.service.readiness import (
    DeploymentReadinessResult,
    VpsReadinessEvaluator,
    wait_for_readiness,
)
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.service.systemd import SystemdManager
from binance_market_data_recorder.storage.capacity import HARD_RESERVE_BYTES

NOW = 2_000_000_000_000
CORE = ["agg_trade", "book_ticker", "diff_depth"]


class _InstalledFile:
    def __init__(self, relative: str, root: Path, *, hashed: bool = True) -> None:
        self.relative = Path(relative)
        selected = root / self.relative
        self.name = self.relative.name
        self.parent = self.relative.parent
        self.size = selected.stat().st_size if hashed else None
        self.hash: object | None
        if hashed:
            digest = hashlib.sha256(selected.read_bytes()).digest()
            value = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
            self.hash = SimpleNamespace(mode="sha256", value=value)
        else:
            self.hash = None

    def __str__(self) -> str:
        return str(self.relative)


class _InstalledDistribution:
    def __init__(
        self,
        *,
        name: str,
        version: str,
        root: Path,
        files: list[_InstalledFile],
    ) -> None:
        self.metadata = {"Name": name}
        self.version = version
        self.root = root
        self.files = files

    def locate_file(self, entry: object) -> Path:
        return self.root / str(entry)


class FakeSystemd:
    def __init__(self, *, active_state: str = "active", main_pid: int = 321) -> None:
        self.active_state = active_state
        self.main_pid = main_pid

    def verify_effective_properties(
        self, *, expected: dict[str, object] | None = None
    ) -> dict[str, object]:
        return dict(expected or {})

    def verify_install_contract(self) -> dict[str, object]:
        return {"service_non_root": True}

    def runtime_properties(self) -> dict[str, object]:
        return {
            "active_state": self.active_state,
            "sub_state": "running" if self.active_state == "active" else "failed",
            "main_pid": self.main_pid,
            "result": "success" if self.active_state == "active" else "exit-code",
        }


def _identity(tmp_path: Path) -> DeploymentIdentity:
    effective = {
        "fragment_path": str(tmp_path / "service"),
        "drop_in_paths": [],
        "exec_start": ["/opt/venv/bin/python", "-m", "package"],
        "user": "recorder",
        "group": "recorder",
        "restart": "on-failure",
        "restart_sec_usec": 10_000_000,
        "timeout_stop_sec_usec": 90_000_000,
        "umask": "0027",
        "no_new_privileges": True,
        "working_directory": str(tmp_path),
        "wants": ["network-online.target"],
        "requires": [],
        "after": ["network-online.target"],
        "environment": [
            "ALL_PROXY=",
            "HTTPS_PROXY=",
            "HTTP_PROXY=",
            "NO_PROXY=",
            "PYTHONUNBUFFERED=1",
            "all_proxy=",
            "http_proxy=",
            "https_proxy=",
            "no_proxy=",
        ],
        "environment_files": [],
        "pass_environment": [],
        "service_type": "simple",
        "kill_signal": "SIGTERM",
        "standard_output": "journal",
        "standard_error": "journal",
    }
    return DeploymentIdentity(
        source_git_sha="a" * 40,
        wheel_path=str(tmp_path / "wheel"),
        wheel_sha256="b" * 64,
        package_version="0.1.0a1",
        python_executable="/opt/venv/bin/python",
        python_exact_version="3.12 exact",
        dependency_lock_path=str(tmp_path / "lock"),
        dependency_lock_sha256="c" * 64,
        config_path=str(tmp_path / "config"),
        config_sha256="d" * 64,
        systemd_unit_path=str(tmp_path / "service"),
        systemd_unit_sha256="e" * 64,
        capacity_profile_id="vps-production-v1",
        startup_sidecars={
            "legacy_reconnect_classifications": {
                "path": str(tmp_path / "legacy.json"),
                "state": "ABSENT",
                "sha256": None,
            }
        },
        systemd_effective=effective,
        remote_archive_states=("REMOTE_DELETE_PENDING", "REMOTE_DELETED"),
    )


def _market() -> dict[str, object]:
    return {
        "status": "READY",
        "ready": True,
        "connected_streams": CORE,
        "persisted_streams": CORE,
        "snapshot_persisted": True,
        "orderbook_synchronized": True,
        "failure": None,
    }


def _state(identity: DeploymentIdentity) -> dict[str, object]:
    return {
        "schema_version": "service-state.v1",
        "status": "RUNNING",
        "pid": 321,
        "heartbeat_at_utc_ns": NOW,
        "heartbeat_interval_seconds": 5.0,
        "deployment_identity": {
            "identity_sha256": identity.identity_sha256,
            "source_git_sha": identity.source_git_sha,
            "wheel_sha256": identity.wheel_sha256,
            "config_sha256": identity.config_sha256,
            "systemd_unit_sha256": identity.systemd_unit_sha256,
            "capacity_profile_id": identity.capacity_profile_id,
        },
        "capacity_profile_id": "vps-production-v1",
        "catalog_open": True,
        "startup_recovery_complete": True,
        "markets": {"spot": _market(), "um_perpetual": _market()},
        "capacity": {
            "observed_at_utc_ns": NOW,
            "total_bytes": 40 * 1024**3,
            "free_bytes": 20 * 1024**3,
            "capacity_profile": "vps-production-v1",
            "capacity_state": "NORMAL",
            "hard_reserve_eta": {"status": "NOT_APPROACHING"},
            "actual_hard_reserve_reached": False,
        },
    }


def _evaluate(
    tmp_path: Path,
    state: dict[str, object] | None,
    *,
    systemd: FakeSystemd | None = None,
    process_environment: dict[str, str] | None = None,
) -> DeploymentReadinessResult:
    identity = _identity(tmp_path)
    if state is not None:
        ServiceStateStore(tmp_path / "state" / "service_state.json").write(state)
    evaluator = VpsReadinessEvaluator(
        data_root=tmp_path,
        identity=identity,
        systemd_manager=cast(SystemdManager, systemd or FakeSystemd()),
        utc_clock_ns=lambda: NOW,
        process_alive=lambda _pid: True,
        catalog_ready=lambda _path: True,
        identity_verifier=lambda selected: {
            "identity_sha256": selected.identity_sha256
        },
        process_environment=lambda _pid: process_environment or {},
    )
    return evaluator.evaluate()


def test_systemd_active_alone_is_not_readiness(tmp_path: Path) -> None:
    assert _evaluate(tmp_path, None).state == "NOT_READY"


def test_wrong_main_pid_fails_readiness(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    result = _evaluate(tmp_path, _state(identity), systemd=FakeSystemd(main_pid=999))
    assert result.state == "FAILED"
    assert "main_pid_service_state_mismatch" in result.reasons


def test_stale_service_state_fails_readiness(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    state["heartbeat_at_utc_ns"] = 1
    result = _evaluate(tmp_path, state)
    assert result.state == "FAILED"
    assert "service_heartbeat_stale" in result.reasons


def test_runtime_identity_mismatch_fails_readiness(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    claimed = cast(dict[str, object], state["deployment_identity"])
    claimed["wheel_sha256"] = "f" * 64
    result = _evaluate(tmp_path, state)
    assert result.state == "FAILED"
    assert "runtime_deployment_identity_mismatch" in result.reasons


def test_recovery_incomplete_is_not_ready(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    state["startup_recovery_complete"] = False
    result = _evaluate(tmp_path, state)
    assert result.state == "NOT_READY"
    assert "startup_recovery_incomplete" in result.reasons


def test_fresh_starting_heartbeat_remains_not_ready_after_thirty_seconds(
    tmp_path: Path,
) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    state.update(
        {
            "status": "STARTING",
            "started_at_utc_ns": NOW - 31_000_000_000,
            "heartbeat_at_utc_ns": NOW,
            "startup_recovery_complete": False,
            "capacity": None,
            "markets": {},
        }
    )
    result = _evaluate(tmp_path, state)
    assert result.state == "NOT_READY"
    assert result.reasons == ("runtime_status_starting",)
    assert "service_heartbeat_stale" not in result.reasons


@pytest.mark.parametrize("market_name", ["spot", "um_perpetual"])
def test_each_core_market_must_reuse_existing_full_readiness(
    tmp_path: Path,
    market_name: str,
) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    markets = cast(dict[str, dict[str, object]], state["markets"])
    markets[market_name]["snapshot_persisted"] = False
    markets[market_name]["ready"] = False
    result = _evaluate(tmp_path, state)
    assert result.state == "NOT_READY"
    assert f"{market_name}_core_not_ready" in result.reasons


def test_capacity_unavailable_fails_readiness(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    state["capacity"] = None
    result = _evaluate(tmp_path, state)
    assert result.state == "FAILED"
    assert "capacity_observation_unavailable" in result.reasons


def test_actual_hard_reserve_is_not_ready(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    capacity = cast(dict[str, object], state["capacity"])
    capacity["free_bytes"] = HARD_RESERVE_BYTES
    capacity["capacity_state"] = "HARD_RESERVE"
    capacity["actual_hard_reserve_reached"] = True
    result = _evaluate(tmp_path, state)
    assert result.state == "NOT_READY"
    assert "hard_reserve_safety_stop" in result.reasons


@pytest.mark.parametrize(
    ("capacity_state", "free_gib"),
    [("WARNING", 18), ("CRITICAL", 14), ("EMERGENCY", 11)],
)
def test_degraded_capacity_above_actual_reserve_may_remain_ready(
    tmp_path: Path,
    capacity_state: str,
    free_gib: int,
) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    capacity = cast(dict[str, object], state["capacity"])
    capacity["capacity_state"] = capacity_state
    capacity["free_bytes"] = free_gib * 1024**3
    result = _evaluate(tmp_path, state)
    assert result.state == "READY"
    assert result.evidence["capacity_state"] == capacity_state


def test_insufficient_forecast_data_with_current_safe_observation_is_ready(
    tmp_path: Path,
) -> None:
    identity = _identity(tmp_path)
    state = _state(identity)
    capacity = cast(dict[str, object], state["capacity"])
    capacity["capacity_state"] = "WARNING"
    capacity["hard_reserve_eta"] = {"status": "INSUFFICIENT_DATA"}
    result = _evaluate(tmp_path, state)
    assert result.state == "READY"


def test_full_exact_evidence_is_ready(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    assert _evaluate(tmp_path, _state(identity)).state == "READY"


def test_full_ready_invokes_real_file_identity_and_effective_systemd_seams(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    data_root.mkdir()
    config = tmp_path / "recorder.toml"
    unit = tmp_path / "recorder.service"
    wheel = tmp_path / "recorder.whl"
    lock = tmp_path / "production.lock"
    config.write_text('[recorder]\ncapacity_profile="vps-production-v1"\n')
    unit.write_text("[Service]\nRestart=on-failure\n")
    wheel.write_bytes(b"exact wheel")
    lock.write_bytes(b"exact retained lock")

    manager = SystemdManager(
        data_root=data_root,
        config_file=config,
        user="recorder",
        group="recorder",
        unit_path=unit,
        python_executable=Path(sys.executable),
        capacity_profile_id="vps-production-v1",
        command_runner=lambda _arguments: subprocess.CompletedProcess(
            [], 1, "", "runner not initialized"
        ),
    )
    effective = manager.expected_effective_identity()
    identity = create_deployment_identity(
        source_git_sha="a" * 40,
        wheel_path=wheel,
        dependency_lock_path=lock,
        config_path=config,
        systemd_unit_path=unit,
        capacity_profile_id="vps-production-v1",
        startup_sidecar_path=tmp_path / "legacy.json",
        systemd_effective=effective,
    )
    properties = {
        "FragmentPath": str(unit),
        "DropInPaths": "",
        "ExecStart": (
            "{ path="
            + str(Path(sys.executable).absolute())
            + " ; argv[]="
            + " ".join(cast(list[str], effective["exec_start"]))
            + " ; }"
        ),
        "User": "recorder",
        "Group": "recorder",
        "Restart": "on-failure",
        "RestartUSec": "10s",
        "TimeoutStopUSec": "90s",
        "UMask": "0027",
        "NoNewPrivileges": "yes",
        "WorkingDirectory": str(data_root),
        "Wants": "network-online.target",
        "Requires": "",
        "After": "network-online.target",
        "Environment": " ".join(cast(list[str], effective["environment"])),
        "EnvironmentFiles": "",
        "PassEnvironment": "",
        "Type": "simple",
        "KillSignal": "15",
        "StandardOutput": "journal",
        "StandardError": "journal",
        "ActiveState": "active",
        "SubState": "running",
        "MainPID": "321",
        "Result": "success",
    }

    def show_runner(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
        if arguments[0] == "/usr/bin/busctl":
            if arguments[1] == "call":
                return subprocess.CompletedProcess(
                    list(arguments),
                    0,
                    'o "/org/freedesktop/systemd1/unit/recorder_2eservice"\n',
                    "",
                )
            if arguments[1] == "get-property":
                return subprocess.CompletedProcess(list(arguments), 0, "a(sb) 0\n", "")
        assert arguments[1] == "show"
        body = "\n".join(f"{name}={value}" for name, value in properties.items())
        return subprocess.CompletedProcess(list(arguments), 0, body + "\n", "")

    manager.command_runner = show_runner
    state = _state(identity)
    ServiceStateStore(data_root / "state" / "service_state.json").write(state)
    calls = {"identity": 0, "effective": 0}

    def real_file_identity(selected: DeploymentIdentity) -> dict[str, object]:
        calls["identity"] += 1
        return verify_identity_files(selected, verify_installed=False)

    original_effective = manager.verify_effective_properties

    def real_effective(
        *, expected: dict[str, object] | None = None
    ) -> dict[str, object]:
        calls["effective"] += 1
        return original_effective(expected=expected)

    manager.verify_effective_properties = real_effective  # type: ignore[method-assign]
    manager.verify_install_contract = (  # type: ignore[method-assign]
        lambda: {"service_non_root": True}
    )
    result = VpsReadinessEvaluator(
        data_root=data_root,
        identity=identity,
        systemd_manager=manager,
        utc_clock_ns=lambda: NOW,
        process_alive=lambda _pid: True,
        catalog_ready=lambda _path: True,
        identity_verifier=real_file_identity,
        process_environment=lambda _pid: {},
    ).evaluate()

    assert result.state == "READY"
    assert calls == {"identity": 1, "effective": 1}


def test_full_ready_invokes_complete_real_installed_identity_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import binance_market_data_recorder as package
    from binance_market_data_recorder.service import (
        deployment_identity as identity_module,
    )
    from binance_market_data_recorder.service import systemd as systemd_module

    artifact_root = tmp_path / "artifact"
    venv_root = artifact_root / "venv"
    site_packages = venv_root / "lib/python3.12/site-packages"
    package_root = site_packages / "binance_market_data_recorder"
    recorder_info = site_packages / "binance_market_data_recorder-0.1.0a1.dist-info"
    dependency_root = site_packages / "example_dependency"
    dependency_info = site_packages / "example_dependency-1.2.3.dist-info"
    release = artifact_root / "release"
    data_root = tmp_path / "data"
    config = tmp_path / "recorder.toml"
    unit = tmp_path / "recorder.service"
    identity_path = tmp_path / "deployment-identity.json"
    python = venv_root / "bin/python"
    wheel = release / "recorder.whl"
    lock = release / "production.lock"
    for directory in (
        package_root,
        recorder_info,
        dependency_root,
        dependency_info,
        python.parent,
        release,
        data_root,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    python.write_bytes(b"copied python")
    wheel.write_bytes(b"exact retained wheel")
    lock.write_text(
        "example-dependency==1.2.3 \\\n"
        f"    --hash=sha256:{'1' * 64}\n",
        encoding="utf-8",
    )
    config.write_text(
        '[recorder]\ncapacity_profile="vps-production-v1"\n',
        encoding="utf-8",
    )
    unit.write_text("[Service]\nRestart=on-failure\n", encoding="utf-8")
    module_path = package_root / "__init__.py"
    module_path.write_bytes(b"__version__ = '0.1.0a1'\n")
    direct_url_path = recorder_info / "direct_url.json"
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    direct_url_path.write_text(
        json.dumps(
            {
                "archive_info": {"hashes": {"sha256": wheel_sha}},
                "url": wheel.as_uri(),
            }
        ),
        encoding="utf-8",
    )
    recorder_record = recorder_info / "RECORD"
    recorder_record.write_bytes(b"")
    dependency_module = dependency_root / "__init__.py"
    dependency_module.write_bytes(b"VALUE = 1\n")
    dependency_metadata = dependency_info / "METADATA"
    dependency_metadata.write_bytes(b"Name: example-dependency\nVersion: 1.2.3\n")
    dependency_record = dependency_info / "RECORD"
    dependency_record.write_bytes(b"")
    recorder_distribution = _InstalledDistribution(
        name="binance-market-data-recorder",
        version="0.1.0a1",
        root=site_packages,
        files=[
            _InstalledFile(
                "binance_market_data_recorder/__init__.py", site_packages
            ),
            _InstalledFile(
                "binance_market_data_recorder-0.1.0a1.dist-info/direct_url.json",
                site_packages,
            ),
            _InstalledFile(
                "binance_market_data_recorder-0.1.0a1.dist-info/RECORD",
                site_packages,
                hashed=False,
            ),
        ],
    )
    dependency_distribution = _InstalledDistribution(
        name="Example_Dependency",
        version="1.2.3",
        root=site_packages,
        files=[
            _InstalledFile("example_dependency/__init__.py", site_packages),
            _InstalledFile(
                "example_dependency-1.2.3.dist-info/METADATA", site_packages
            ),
            _InstalledFile(
                "example_dependency-1.2.3.dist-info/RECORD",
                site_packages,
                hashed=False,
            ),
        ],
    )
    manager = SystemdManager(
        data_root=data_root,
        config_file=config,
        user="recorder",
        group="recorder",
        unit_path=unit,
        python_executable=python,
        capacity_profile_id="vps-production-v1",
        command_runner=lambda _arguments: subprocess.CompletedProcess([], 1, "", ""),
    )
    effective = manager.expected_effective_identity()
    identity = DeploymentIdentity(
        source_git_sha="a" * 40,
        wheel_path=str(wheel),
        wheel_sha256=wheel_sha,
        package_version="0.1.0a1",
        python_executable=str(python),
        python_exact_version=sys.version,
        dependency_lock_path=str(lock),
        dependency_lock_sha256=hashlib.sha256(lock.read_bytes()).hexdigest(),
        config_path=str(config),
        config_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
        systemd_unit_path=str(unit),
        systemd_unit_sha256=hashlib.sha256(unit.read_bytes()).hexdigest(),
        capacity_profile_id="vps-production-v1",
        startup_sidecars={
            "legacy_reconnect_classifications": {
                "path": str(tmp_path / "legacy.json"),
                "state": "ABSENT",
                "sha256": None,
            }
        },
        systemd_effective=effective,
        remote_archive_states=("REMOTE_DELETE_PENDING", "REMOTE_DELETED"),
    )
    identity_path.write_bytes(identity.canonical_bytes())
    properties = {
        "FragmentPath": str(unit),
        "DropInPaths": "",
        "ExecStart": (
            f"{{ path={python} ; argv[]="
            + " ".join(cast(list[str], effective["exec_start"]))
            + " ; }"
        ),
        "User": "recorder",
        "Group": "recorder",
        "Restart": "on-failure",
        "RestartUSec": "10s",
        "TimeoutStopUSec": "90s",
        "UMask": "0027",
        "NoNewPrivileges": "yes",
        "WorkingDirectory": str(data_root),
        "Wants": "network-online.target",
        "Requires": "",
        "After": "network-online.target",
        "Environment": " ".join(cast(list[str], effective["environment"])),
        "EnvironmentFiles": "",
        "PassEnvironment": "",
        "Type": "simple",
        "KillSignal": "15",
        "StandardOutput": "journal",
        "StandardError": "journal",
        "ActiveState": "active",
        "SubState": "running",
        "MainPID": "321",
        "Result": "success",
    }

    def show_runner(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
        if arguments[0] == "/usr/bin/busctl":
            if arguments[1] == "call":
                return subprocess.CompletedProcess(
                    list(arguments),
                    0,
                    'o "/org/freedesktop/systemd1/unit/recorder_2eservice"\n',
                    "",
                )
            if arguments[1] == "get-property":
                return subprocess.CompletedProcess(list(arguments), 0, "a(sb) 0\n", "")
        body = "\n".join(f"{name}={value}" for name, value in properties.items())
        return subprocess.CompletedProcess(list(arguments), 0, body + "\n", "")

    manager.command_runner = show_runner
    original_stat = Path.stat

    def protected_stat(path: Path, *, follow_symlinks: bool = True) -> object:
        observed = original_stat(path, follow_symlinks=follow_symlinks)
        permissions = observed.st_mode & 0o777
        owner_uid = 0
        owner_gid = 0
        if path == data_root:
            owner_uid, owner_gid, permissions = 123, 456, 0o750
        elif path in {config, identity_path}:
            owner_gid, permissions = 456, 0o640
        elif path == unit:
            permissions = 0o644
        elif stat.S_ISDIR(observed.st_mode) or path == python:
            permissions = 0o755
        else:
            permissions = 0o644
        return SimpleNamespace(
            st_uid=owner_uid,
            st_gid=owner_gid,
            st_mode=stat.S_IFMT(observed.st_mode) | permissions,
            st_size=observed.st_size,
        )

    monkeypatch.setattr(identity_module, "VPS_ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(systemd_module, "VPS_ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(systemd_module, "VPS_CONFIG_PATH", config)
    monkeypatch.setattr(systemd_module, "VPS_DATA_ROOT", data_root)
    monkeypatch.setattr(systemd_module, "VPS_PYTHON_PATH", python)
    monkeypatch.setattr(systemd_module, "VPS_UNIT_PATH", unit)
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.metadata.distributions",
        lambda: [dependency_distribution, recorder_distribution],
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.metadata.distribution",
        lambda _name: recorder_distribution,
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.pwd.getpwnam",
        lambda _name: SimpleNamespace(pw_uid=123),
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.deployment_identity.grp.getgrnam",
        lambda _name: SimpleNamespace(gr_gid=456),
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.systemd.pwd.getpwnam",
        lambda _name: SimpleNamespace(pw_uid=123),
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.systemd.grp.getgrnam",
        lambda _name: SimpleNamespace(gr_gid=456),
    )
    monkeypatch.setattr(sys, "executable", str(python))
    monkeypatch.setattr(sys, "prefix", str(venv_root))
    monkeypatch.setattr(sys, "base_prefix", "/usr/local")
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(package, "__file__", str(module_path))
    monkeypatch.setattr(Path, "stat", protected_stat)
    monkeypatch.setattr(
        Path,
        "lstat",
        lambda path: protected_stat(path, follow_symlinks=False),
    )
    ServiceStateStore(data_root / "state/service_state.json").write(_state(identity))
    result = VpsReadinessEvaluator(
        data_root=data_root,
        identity=identity,
        systemd_manager=manager,
        utc_clock_ns=lambda: NOW,
        process_alive=lambda _pid: True,
        catalog_ready=lambda _path: True,
        process_environment=lambda _pid: {},
    ).evaluate()

    assert result.state == "READY", result.public_dict()
    identity_evidence = cast(dict[str, object], result.evidence["identity"])
    dependencies = cast(dict[str, object], identity_evidence["installed_dependencies"])
    control_chain = cast(dict[str, object], identity_evidence["venv_control_chain"])
    assert dependencies["exact_match"] is True
    assert dependencies["protected_file_count"] == 3
    assert control_chain["service_writable"] is False


@pytest.mark.parametrize(
    "proxy_name", ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"]
)
def test_manager_or_process_uppercase_proxy_environment_fails_readiness(
    tmp_path: Path,
    proxy_name: str,
) -> None:
    identity = _identity(tmp_path)
    result = _evaluate(
        tmp_path,
        _state(identity),
        process_environment={proxy_name: "http://proxy.invalid:8080"},
    )

    assert result.state == "FAILED"
    assert result.reasons[0].startswith("process_environment_invalid:")


@pytest.mark.parametrize(
    "proxy_name", ["http_proxy", "https_proxy", "all_proxy", "no_proxy"]
)
def test_process_lowercase_proxy_environment_fails_readiness(
    tmp_path: Path,
    proxy_name: str,
) -> None:
    identity = _identity(tmp_path)
    result = _evaluate(
        tmp_path,
        _state(identity),
        process_environment={proxy_name: "http://proxy.invalid:8080"},
    )

    assert result.state == "FAILED"
    assert result.reasons[0].startswith("process_environment_invalid:")


def test_empty_proxy_environment_is_accepted_as_direct(
    tmp_path: Path,
) -> None:
    identity = _identity(tmp_path)
    result = _evaluate(
        tmp_path,
        _state(identity),
        process_environment={"HTTP_PROXY": "", "http_proxy": ""},
    )

    assert result.state == "READY"
    process_evidence = cast(dict[str, object], result.evidence["process_environment"])
    assert process_evidence["direct_network_environment"] is True


def test_systemd_failure_is_failed(tmp_path: Path) -> None:
    identity = _identity(tmp_path)
    result = _evaluate(
        tmp_path,
        _state(identity),
        systemd=FakeSystemd(active_state="failed", main_pid=0),
    )
    assert result.state == "FAILED"
    assert "systemd_service_failed" in result.reasons


def test_readiness_deadline_expiration_is_failed() -> None:
    class NeverReady:
        def evaluate(self) -> DeploymentReadinessResult:
            return DeploymentReadinessResult("NOT_READY", ("starting",), {})

    clock = [0.0]

    def sleep(seconds: float) -> None:
        clock[0] += seconds

    result = wait_for_readiness(
        cast(Any, NeverReady()),
        timeout_seconds=2.0,
        poll_seconds=1.0,
        monotonic_clock=lambda: clock[0],
        sleep=sleep,
    )
    assert result.state == "FAILED"
    assert "readiness_deadline_expired" in result.reasons
