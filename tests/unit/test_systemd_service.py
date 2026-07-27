from __future__ import annotations

import os
import subprocess
import sys
from collections import namedtuple
from collections.abc import Sequence
from pathlib import Path

import pytest

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
