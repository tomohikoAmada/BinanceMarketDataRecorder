from __future__ import annotations

import os
import stat
import subprocess
import sys
from collections import namedtuple
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace

import pytest

from binance_market_data_recorder.service import systemd as systemd_module
from binance_market_data_recorder.service.systemd import (
    SYSTEMD_SERVICE_NAME,
    SystemdError,
    SystemdManager,
)

_FakePwUid = namedtuple("_FakePwUid", ["pw_uid", "pw_gid", "pw_name"])
_FakeGrp = namedtuple("_FakeGrp", ["gr_gid", "gr_name"])


class FakeSystemctl:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.enabled = False
        self.running = False

    def __call__(
        self,
        arguments: Sequence[str],
    ) -> subprocess.CompletedProcess[str]:
        call = tuple(arguments)
        self.calls.append(call)
        action = call[1] if len(call) > 1 else ""
        if action == "enable":
            self.enabled = True
        elif action == "disable":
            self.enabled = False
        elif action in {"start", "restart"}:
            self.running = True
        elif action == "stop":
            self.running = False
        returncode = 0
        if action == "is-active" and not self.running:
            returncode = 3
        if action == "is-enabled" and not self.enabled:
            returncode = 1
        return subprocess.CompletedProcess(call, returncode, "", "")


class ShowSystemctl(FakeSystemctl):
    def __init__(self) -> None:
        super().__init__()
        self.properties: dict[str, str] = {}
        self.dbus_get_unit_output = (
            'o "/org/freedesktop/systemd1/unit/'
            "binance_2dmarket_2ddata_2drecorder_2eservice"
            '"\n'
        )
        self.dbus_property_output = "a(sb) 0\n"
        self.dbus_returncode = 0
        self.dbus_property_returncode = 0
        self.dbus_stderr = ""

    def __call__(
        self,
        arguments: Sequence[str],
    ) -> subprocess.CompletedProcess[str]:
        call = tuple(arguments)
        if len(call) > 1 and call[0] == "/usr/bin/busctl":
            self.calls.append(call)
            if call[1] == "call":
                return subprocess.CompletedProcess(
                    call, self.dbus_returncode, self.dbus_get_unit_output, self.dbus_stderr
                )
            if call[1] == "get-property":
                return subprocess.CompletedProcess(
                    call,
                    self.dbus_property_returncode,
                    self.dbus_property_output,
                    self.dbus_stderr,
                )
            raise AssertionError(f"unexpected busctl action: {call[1]}")
        if len(call) > 1 and call[1] == "show":
            self.calls.append(call)
            body = "\n".join(
                f"{name}={value}" for name, value in self.properties.items()
            )
            return subprocess.CompletedProcess(call, 0, body + "\n", "")
        return super().__call__(arguments)


def _mock_pwd_user(user: str) -> _FakePwUid:
    if user == "root":
        return _FakePwUid(pw_uid=0, pw_gid=0, pw_name="root")
    return _FakePwUid(pw_uid=os.getuid(), pw_gid=os.getgid(), pw_name=user)


def _mock_grp_group(group: str) -> _FakeGrp:
    if group == "root":
        return _FakeGrp(gr_gid=0, gr_name="root")
    return _FakeGrp(gr_gid=os.getgid(), gr_name=group)


@pytest.fixture(autouse=True)
def _patch_systemd_deps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(
        "binance_market_data_recorder.service.systemd.pwd.getpwnam",
        _mock_pwd_user,
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.service.systemd.grp.getgrnam",
        _mock_grp_group,
    )


def _manager(tmp_path: Path, runner: FakeSystemctl) -> SystemdManager:
    data_root = tmp_path / "data"
    data_root.mkdir()
    config = tmp_path / "recorder.toml"
    config.write_text("[recorder]\n", encoding="utf-8")
    config.chmod(0o640)
    return SystemdManager(
        data_root=data_root,
        config_file=config,
        user=os.getenv("USER", "testuser"),
        group=os.getenv("USER", "testuser"),
        unit_path=tmp_path / SYSTEMD_SERVICE_NAME,
        python_executable=Path(sys.executable),
        command_runner=runner,
        git_commit="abc1234",
    )


def _vps_manager(tmp_path: Path, runner: FakeSystemctl) -> SystemdManager:
    data_root = tmp_path / "data"
    data_root.mkdir()
    config = tmp_path / "recorder.toml"
    config.write_text(
        '[recorder]\ncapacity_profile="vps-production-v1"\n', encoding="utf-8"
    )
    return SystemdManager(
        data_root=data_root,
        config_file=config,
        user="recorder",
        group="recorder",
        unit_path=tmp_path / SYSTEMD_SERVICE_NAME,
        python_executable=Path("/opt/binance-market-data-recorder/venv/bin/python"),
        command_runner=runner,
        git_commit="abc1234",
        capacity_profile_id="vps-production-v1",
    )


def _effective_properties(manager: SystemdManager) -> dict[str, str]:
    argv = " ".join(manager.exec_start_arguments())
    return {
        "FragmentPath": str(manager.unit_path),
        "DropInPaths": "",
        "ExecStart": f"{{ path={manager.python_executable} ; argv[]={argv} ; }}",
        "User": manager.user,
        "Group": manager.group,
        "Restart": "on-failure",
        "RestartUSec": "10s",
        "TimeoutStopUSec": "1min 30s",
        "UMask": "0027",
        "NoNewPrivileges": "yes",
        "WorkingDirectory": str(manager.data_root),
        "Wants": "network-online.target",
        "Requires": "",
        "After": "network-online.target",
        "Environment": (
            "ALL_PROXY= HTTPS_PROXY= HTTP_PROXY= NO_PROXY= PYTHONUNBUFFERED=1 "
            "all_proxy= http_proxy= https_proxy= no_proxy="
        ),
        "EnvironmentFiles": "",
        "PassEnvironment": "",
        "Type": "simple",
        "KillSignal": "15",
        "StandardOutput": "journal",
        "StandardError": "journal",
        "ActiveState": "active",
        "SubState": "running",
        "MainPID": "123",
        "Result": "success",
    }


def test_systemd_unit_has_required_dependencies_and_shutdown_policy(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    unit = manager.unit()
    assert "User=" in unit
    assert "Group=" in unit
    assert "After=network-online.target mihomo.service" in unit
    assert "Wants=network-online.target mihomo.service" in unit
    assert "Requires=mihomo.service" not in unit
    assert f"WorkingDirectory={tmp_path}" in unit
    assert 'WorkingDirectory="' not in unit
    assert "Restart=on-failure" in unit
    assert "RestartSec=10s" in unit
    assert "TimeoutStopSec=90s" in unit
    assert "KillSignal=SIGTERM" in unit
    assert "UMask=0027" in unit
    assert "Environment=PYTHONUNBUFFERED=1" in unit
    assert (
        'Environment="BINANCE_MARKET_RECORDER_GIT_COMMIT=abc1234"' in unit
    )
    assert "HTTP_PROXY" not in unit
    assert "Listen" not in unit


def test_vps_unit_has_no_mihomo_or_operational_environment_authority(
    tmp_path: Path,
) -> None:
    manager = _vps_manager(tmp_path, FakeSystemctl())
    unit = manager.unit()

    assert "After=network-online.target\n" in unit
    assert "Wants=network-online.target\n" in unit
    assert "mihomo.service" not in unit
    assert "BINANCE_MARKET_RECORDER_" not in unit
    assert "EnvironmentFile" not in unit
    assert "Environment=HTTP_PROXY=" in unit
    assert "Environment=http_proxy=" in unit
    assert "Restart=on-failure" in unit
    assert "RestartSec=10s" in unit
    assert "TimeoutStopSec=90s" in unit
    assert "UMask=0027" in unit
    assert "NoNewPrivileges=true" in unit
    assert "StandardOutput=journal" in unit
    assert "StandardError=journal" in unit


def test_vps_effective_properties_are_verified_and_dropins_fail_closed(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)

    assert manager.verify_effective_properties() == manager.expected_effective_identity()

    runner.properties["DropInPaths"] = "/etc/systemd/system/service.d/override.conf"
    with pytest.raises(SystemdError, match="drop-ins"):
        manager.verify_effective_properties()


def test_vps_environment_files_uses_direct_dbus_evidence_when_systemctl_omits_it(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    del runner.properties["EnvironmentFiles"]

    assert manager.verify_effective_properties() == manager.expected_effective_identity()

    show_call = next(call for call in runner.calls if call[1] == "show")
    assert "--all" not in show_call
    assert any(call[1] == "call" for call in runner.calls)
    assert any(call[1] == "get-property" for call in runner.calls)


def test_vps_environment_files_systemctl_line_is_not_authority(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.properties["EnvironmentFiles"] = "/not/authoritative"

    assert manager.verify_effective_properties() == manager.expected_effective_identity()


@pytest.mark.parametrize(
    "property_output",
    [
        "a(sb) 1 \"/etc/default/recorder\" true",
        "a(sb) 1",
    ],
)
def test_vps_nonempty_dbus_environment_files_fail_closed(
    tmp_path: Path,
    property_output: str,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.dbus_property_output = property_output

    with pytest.raises(SystemdError, match="EnvironmentFile"):
        manager.verify_effective_properties()


def test_vps_dbus_environment_files_query_failure_fails_closed(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.dbus_returncode = 1
    runner.dbus_stderr = "org.freedesktop.DBus.Error.Failed"

    with pytest.raises(SystemdError, match="busctl call"):
        manager.verify_effective_properties()


def test_vps_dbus_environment_files_property_failure_fails_closed(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.dbus_property_returncode = 1
    runner.dbus_stderr = "org.freedesktop.DBus.Error.UnknownProperty"

    with pytest.raises(SystemdError, match="busctl get-property"):
        manager.verify_effective_properties()


@pytest.mark.parametrize(
    "property_output",
    ["s 0", "a(sb)", "a(sb) nope", "a(sb) -1", "a(sb) 0 extra"],
)
def test_vps_malformed_dbus_environment_files_evidence_fails_closed(
    tmp_path: Path,
    property_output: str,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.dbus_property_output = property_output

    with pytest.raises(SystemdError):
        manager.verify_effective_properties()


@pytest.mark.parametrize(
    "unit_output",
    [
        "",
        's "/org/freedesktop/systemd1/unit/not-a-unit"',
        'o "/org/freedesktop/systemd1/other/value"',
        'o "/org/freedesktop/systemd1/unit/valid" trailing',
    ],
)
def test_vps_malformed_dbus_unit_lookup_fails_closed(
    tmp_path: Path,
    unit_output: str,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.dbus_get_unit_output = unit_output

    with pytest.raises(SystemdError):
        manager.verify_effective_properties()


def test_vps_missing_systemctl_and_dbus_environment_files_evidence_fails_closed(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    del runner.properties["EnvironmentFiles"]
    runner.dbus_returncode = 1

    with pytest.raises(SystemdError):
        manager.verify_effective_properties()


def test_vps_effective_operational_environment_override_fails_closed(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.properties["Environment"] = (
        runner.properties["Environment"]
        + " BINANCE_MARKET_RECORDER_DATA_ROOT=/wrong"
    )

    with pytest.raises(SystemdError, match="environment mismatch"):
        manager.verify_effective_properties()


@pytest.mark.parametrize("name", ["HTTP_PROXY", "http_proxy"])
def test_vps_effective_proxy_environment_must_be_empty(
    tmp_path: Path,
    name: str,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.properties["Environment"] = runner.properties["Environment"].replace(
        f"{name}=", f"{name}=http://proxy.invalid:8080"
    )

    with pytest.raises(SystemdError, match="environment mismatch"):
        manager.verify_effective_properties()


@pytest.mark.parametrize(
    ("property_name", "value", "message"),
    [
        ("PassEnvironment", "HTTP_PROXY", "PassEnvironment"),
        ("RestartUSec", "11s", "restart_sec_usec"),
        ("TimeoutStopUSec", "91s", "timeout_stop_sec_usec"),
        ("UMask", "0022", "umask"),
        ("NoNewPrivileges", "no", "no_new_privileges"),
    ],
)
def test_vps_effective_security_properties_fail_closed(
    tmp_path: Path,
    property_name: str,
    value: str,
    message: str,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.properties[property_name] = value

    with pytest.raises(SystemdError, match=message):
        manager.verify_effective_properties()


@pytest.mark.parametrize("property_name", ["Wants", "Requires", "After"])
def test_vps_effective_mihomo_dependency_fails_closed(
    tmp_path: Path,
    property_name: str,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    runner.properties[property_name] = (
        runner.properties[property_name] + " mihomo.service"
    ).strip()

    with pytest.raises(SystemdError, match="Mihomo"):
        manager.verify_effective_properties()


def test_vps_effective_exec_start_is_exact_not_substring_based(
    tmp_path: Path,
) -> None:
    runner = ShowSystemctl()
    manager = _vps_manager(tmp_path, runner)
    runner.properties = _effective_properties(manager)
    exact = " ".join(manager.exec_start_arguments())
    runner.properties["ExecStart"] = (
        f"{{ path={manager.python_executable} ; argv[]={exact} --injected ; }}"
    )

    with pytest.raises(SystemdError, match="exec_start mismatch"):
        manager.verify_effective_properties()


def test_vps_install_ownership_contract_rejects_service_writable_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_root = tmp_path / "artifact"
    venv_root = artifact_root / "venv"
    bin_directory = venv_root / "bin"
    python = artifact_root / "venv" / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_bytes(b"python")
    config_directory = tmp_path / "config"
    config_directory.mkdir()
    config = config_directory / "recorder.toml"
    config.write_text(
        '[recorder]\ncapacity_profile="vps-production-v1"\n', encoding="utf-8"
    )
    data_root = tmp_path / "data"
    data_root.mkdir()
    unit = tmp_path / SYSTEMD_SERVICE_NAME
    unit.write_text("[Service]\n", encoding="utf-8")
    manager = SystemdManager(
        data_root=data_root,
        config_file=config,
        user="recorder",
        group="recorder",
        unit_path=unit,
        python_executable=python,
        capacity_profile_id="vps-production-v1",
        command_runner=FakeSystemctl(),
    )
    service_uid = os.getuid()
    service_gid = os.getgid()
    facts = {
        config: SimpleNamespace(
            st_uid=0, st_gid=service_gid, st_mode=stat.S_IFREG | 0o640
        ),
        config_directory: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFDIR | 0o750
        ),
        data_root: SimpleNamespace(
            st_uid=service_uid,
            st_gid=service_gid,
            st_mode=stat.S_IFDIR | 0o750,
        ),
        artifact_root: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFDIR | 0o755
        ),
        venv_root: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFDIR | 0o755
        ),
        bin_directory: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFDIR | 0o755
        ),
        python: SimpleNamespace(
            st_uid=0, st_gid=0, st_mode=stat.S_IFREG | 0o755
        ),
        unit: SimpleNamespace(st_uid=0, st_gid=0, st_mode=stat.S_IFREG | 0o644),
    }
    original_stat = Path.stat

    def fake_stat(path: Path, *, follow_symlinks: bool = True) -> object:
        return facts.get(
            path,
            original_stat(path, follow_symlinks=follow_symlinks),
        )

    def fake_lstat(path: Path) -> object:
        return facts.get(path, path.stat(follow_symlinks=False))

    monkeypatch.setattr(systemd_module, "VPS_ARTIFACT_ROOT", artifact_root)
    monkeypatch.setattr(systemd_module, "VPS_CONFIG_PATH", config)
    monkeypatch.setattr(systemd_module, "VPS_DATA_ROOT", data_root)
    monkeypatch.setattr(systemd_module, "VPS_PYTHON_PATH", python)
    monkeypatch.setattr(systemd_module, "VPS_UNIT_PATH", unit)
    monkeypatch.setattr(Path, "stat", fake_stat)
    monkeypatch.setattr(Path, "lstat", fake_lstat)

    contract = manager.verify_install_contract()
    assert contract["service_non_root"] is True
    assert contract["config_mode"] == "0640"
    assert contract["data_mode"] == "0750"
    assert contract["unit_mode"] == "0644"

    facts[python] = SimpleNamespace(
        st_uid=0,
        st_gid=service_gid,
        st_mode=stat.S_IFREG | 0o775,
    )
    with pytest.raises(SystemdError, match="must not be service-writable"):
        manager.verify_install_contract()


def test_systemd_install_start_restart_stop_and_uninstall_are_idempotent(
    tmp_path: Path,
) -> None:
    runner = FakeSystemctl()
    manager = _manager(tmp_path, runner)
    first = manager.install()
    second = manager.install()
    assert first["changed"] is True
    assert second["changed"] is False
    assert manager.start()["running"] is True
    assert manager.restart()["running"] is True
    assert manager.stop()["running"] is False
    assert manager.stop()["was_active"] is False
    data_root = manager.data_root
    assert manager.uninstall()["data_removed"] is False
    assert data_root.is_dir()
    assert manager.uninstall()["unit_removed"] is False


def test_systemd_refuses_unmanaged_unit_install(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    manager.unit_path.write_text("[Unit]\nDescription=someone else\n", encoding="utf-8")
    with pytest.raises(SystemdError, match="unmanaged"):
        manager.install()


def test_systemd_refuses_unmanaged_unit_start(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    manager.unit_path.write_text("[Unit]\nDescription=someone else\n", encoding="utf-8")
    with pytest.raises(SystemdError, match="unmanaged"):
        manager.start()


def test_systemd_refuses_unmanaged_unit_stop(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    runner = FakeSystemctl()
    manager.command_runner = runner
    manager.unit_path.write_text("[Unit]\nDescription=someone else\n", encoding="utf-8")
    with pytest.raises(SystemdError, match="unmanaged"):
        manager.stop()
    assert runner.calls == []


def test_systemd_refuses_unmanaged_unit_restart(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    manager.unit_path.write_text("[Unit]\nDescription=someone else\n", encoding="utf-8")
    with pytest.raises(SystemdError, match="unmanaged"):
        manager.restart()


def test_systemd_refuses_unmanaged_unit_uninstall(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    runner = FakeSystemctl()
    manager.command_runner = runner
    manager.unit_path.write_text("[Unit]\nDescription=someone else\n", encoding="utf-8")
    with pytest.raises(SystemdError, match="unmanaged"):
        manager.uninstall()
    assert runner.calls == []


def test_unmanaged_unit_untouched_after_rejected_uninstall(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    original = "[Unit]\nDescription=someone else\n"
    manager.unit_path.write_text(original, encoding="utf-8")
    with pytest.raises(SystemdError):
        manager.uninstall()
    assert manager.unit_path.read_text(encoding="utf-8") == original


def test_systemd_refuses_root_collector(tmp_path: Path) -> None:
    runner = FakeSystemctl()
    data_root = tmp_path / "data"
    data_root.mkdir()
    config = tmp_path / "recorder.toml"
    config.write_text("[recorder]\n", encoding="utf-8")
    config.chmod(0o600)
    manager = SystemdManager(
        data_root=data_root,
        config_file=config,
        user="root",
        group="root",
        unit_path=tmp_path / SYSTEMD_SERVICE_NAME,
        python_executable=Path(sys.executable),
        command_runner=runner,
    )
    assert os.getuid() != 0
    with pytest.raises(SystemdError, match="refusing to run the Collector as root"):
        manager.install()


def test_stop_idempotent_when_unit_absent(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    result = manager.stop()
    assert result["was_active"] is False
    assert result["running"] is False


def test_uninstall_idempotent_when_unit_absent(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    result = manager.uninstall()
    assert result["unit_removed"] is False
    assert result["data_removed"] is False


def test_status_reports_managed(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    assert manager.status()["managed"] is False
    manager.install()
    assert manager.status()["managed"] is True


def test_status_unmanaged_reports_false(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    manager.unit_path.write_text("[Unit]\nDescription=someone else\n", encoding="utf-8")
    status = manager.status()
    assert status["installed"] is True
    assert status["managed"] is False
