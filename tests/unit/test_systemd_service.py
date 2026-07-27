from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest

from binance_market_data_recorder.service.systemd import (
    SYSTEMD_SERVICE_NAME,
    SystemdError,
    SystemdManager,
)


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


def _manager(tmp_path: Path, runner: FakeSystemctl) -> SystemdManager:
    data_root = tmp_path / "data"
    data_root.mkdir()
    config = tmp_path / "recorder.toml"
    config.write_text("[recorder]\n", encoding="utf-8")
    config.chmod(0o640)
    return SystemdManager(
        data_root=data_root,
        config_file=config,
        user="orangepi",
        group="orangepi",
        unit_path=tmp_path / SYSTEMD_SERVICE_NAME,
        python_executable=Path(sys.executable),
        command_runner=runner,
    )


def test_systemd_unit_has_required_dependencies_and_shutdown_policy(
    tmp_path: Path,
) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    unit = manager.unit()
    assert "User=orangepi" in unit
    assert "Group=orangepi" in unit
    assert "After=network-online.target mihomo.service" in unit
    assert "Wants=network-online.target mihomo.service" in unit
    assert "Requires=mihomo.service" not in unit
    assert "Restart=on-failure" in unit
    assert "RestartSec=10s" in unit
    assert "TimeoutStopSec=90s" in unit
    assert "KillSignal=SIGTERM" in unit
    assert "UMask=0027" in unit
    assert "Environment=PYTHONUNBUFFERED=1" in unit
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


def test_systemd_refuses_unmanaged_unit(tmp_path: Path) -> None:
    manager = _manager(tmp_path, FakeSystemctl())
    manager.unit_path.write_text("[Unit]\nDescription=someone else\n", encoding="utf-8")
    with pytest.raises(SystemdError, match="unmanaged"):
        manager.install()


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
    with pytest.raises(SystemdError, match="root"):
        manager.install()
