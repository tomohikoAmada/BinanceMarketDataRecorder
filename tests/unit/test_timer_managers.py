"""Unit tests for ArchiveTimerManager and SoakTimerManager systemd units."""

from __future__ import annotations

import subprocess
from collections import namedtuple
from collections.abc import Sequence
from pathlib import Path

import pytest

from binance_market_data_recorder.service.archive_timer import (
    ARCHIVE_SERVICE_NAME,
    ARCHIVE_TIMER_NAME,
    ArchiveTimerManager,
    SystemdArchiveError,
)
from binance_market_data_recorder.service.soak_timer import (
    SOAK_SERVICE_NAME,
    SOAK_TIMER_NAME,
    SoakTimerManager,
    SystemdSoakError,
)

_FakePw = namedtuple("_FakePw", ["pw_uid", "pw_gid", "pw_name"])
_FakeGrp = namedtuple("_FakeGrp", ["gr_gid", "gr_name"])


class FakeSystemctl:
    def __init__(self, unit_name: str = ARCHIVE_TIMER_NAME) -> None:
        self.unit_name = unit_name
        self.calls: list[tuple[str, ...]] = []
        self.enabled = False
        self.running = False
        self._services: dict[str, bool] = {
            ARCHIVE_TIMER_NAME: False,
            ARCHIVE_SERVICE_NAME: False,
            SOAK_TIMER_NAME: False,
            SOAK_SERVICE_NAME: False,
        }

    def __call__(
        self, arguments: Sequence[str],
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
        rc = 0
        if action == "is-active" and not self.running:
            rc = 3
        if action == "is-enabled" and not self.enabled:
            rc = 1
        return subprocess.CompletedProcess(call, rc, "", "")


@pytest.fixture
def fake_user_group(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("pwd.getpwnam", lambda u: _FakePw(1000, 1000, u))
    monkeypatch.setattr("grp.getgrnam", lambda g: _FakeGrp(1000, g))
    monkeypatch.setattr("sys.platform", "linux")


@pytest.fixture
def archive_cfg(tmp_path: Path) -> Path:
    cfg = tmp_path / "recorder.toml"
    cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
    (tmp_path / "data").mkdir()
    return cfg


@pytest.fixture
def python_bin(tmp_path: Path) -> Path:
    p = tmp_path / "fake-python"
    p.write_text("#!/bin/sh\necho ok\n")
    p.chmod(0o755)
    return p


class TestArchiveTimerManager:
    def test_install_and_status(self, tmp_path: Path, fake_user_group: None) -> None:
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        fake = FakeSystemctl(ARCHIVE_TIMER_NAME)
        mgr = ArchiveTimerManager(
            config_file=cfg,
            user="testuser",
            group="testgroup",
            storage_id="test-storage-id",
            unit_dir=unit_dir,
            python_executable=sb,
            command_runner=fake,
        )
        result = mgr.install()
        assert result["installed"] is True
        assert result["managed"] is True

        assert (unit_dir / ARCHIVE_SERVICE_NAME).is_file()
        assert (unit_dir / ARCHIVE_TIMER_NAME).is_file()

        svc_body = (unit_dir / ARCHIVE_SERVICE_NAME).read_text()
        assert "--storage-id" in svc_body
        assert "test-storage-id" in svc_body

    def test_idempotent_install(self, tmp_path: Path, fake_user_group: None) -> None:
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        fake = FakeSystemctl(ARCHIVE_TIMER_NAME)
        mgr = ArchiveTimerManager(
            config_file=cfg, user="testuser", group="testgroup",
            storage_id="test-storage-id", unit_dir=unit_dir,
            python_executable=sb, command_runner=fake,
        )
        r1 = mgr.install()
        r2 = mgr.install()
        assert r1["installed"] is True
        assert r2["changed"] is False

    def test_reject_unmanaged_unit(self, tmp_path: Path, fake_user_group: None) -> None:
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        (unit_dir / ARCHIVE_SERVICE_NAME).write_text("# user-managed unit")
        (unit_dir / ARCHIVE_TIMER_NAME).write_text("# user-managed")
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        fake = FakeSystemctl(ARCHIVE_TIMER_NAME)
        mgr = ArchiveTimerManager(
            config_file=cfg, user="testuser", group="testgroup",
            storage_id="test-storage-id", unit_dir=unit_dir,
            python_executable=sb, command_runner=fake,
        )
        with pytest.raises(SystemdArchiveError):
            mgr.install()

    def test_reject_root_user(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("pwd.getpwnam", lambda u: _FakePw(0, 0, u))
        monkeypatch.setattr("grp.getgrnam", lambda g: _FakeGrp(0, g))
        monkeypatch.setattr("sys.platform", "linux")
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        mgr = ArchiveTimerManager(
            config_file=cfg, user="root", group="root",
            storage_id="test-storage-id", unit_dir=unit_dir,
            python_executable=sb,
        )
        with pytest.raises(SystemdArchiveError):
            mgr.install()

    def test_uninstall_then_reinstall(self, tmp_path: Path, fake_user_group: None) -> None:
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        fake = FakeSystemctl(ARCHIVE_TIMER_NAME)
        mgr = ArchiveTimerManager(
            config_file=cfg, user="testuser", group="testgroup",
            storage_id="test-storage-id", unit_dir=unit_dir,
            python_executable=sb, command_runner=fake,
        )
        mgr.install()
        mgr.uninstall()
        assert not (unit_dir / ARCHIVE_SERVICE_NAME).exists()
        assert not (unit_dir / ARCHIVE_TIMER_NAME).exists()
        mgr.install()
        assert (unit_dir / ARCHIVE_SERVICE_NAME).is_file()


class TestSoakTimerManager:
    def test_install_with_storage_id(self, tmp_path: Path, fake_user_group: None) -> None:
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        fake = FakeSystemctl(SOAK_TIMER_NAME)
        mgr = SoakTimerManager(
            config_file=cfg, user="testuser", group="testgroup",
            storage_id="soak-test-storage",
            output_path=tmp_path / "samples.jsonl",
            unit_dir=unit_dir, python_executable=sb,
            command_runner=fake,
        )
        result = mgr.install()
        assert result["installed"] is True

        svc_body = (unit_dir / SOAK_SERVICE_NAME).read_text()
        assert "--storage-id" in svc_body
        assert "soak-test-storage" in svc_body

    def test_reject_unmanaged_units(self, tmp_path: Path, fake_user_group: None) -> None:
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        (unit_dir / SOAK_SERVICE_NAME).write_text("user unit")
        (unit_dir / SOAK_TIMER_NAME).write_text("user timer")
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        fake = FakeSystemctl(SOAK_TIMER_NAME)
        mgr = SoakTimerManager(
            config_file=cfg, user="testuser", group="testgroup",
            storage_id="test", output_path=tmp_path / "samples.jsonl",
            unit_dir=unit_dir, python_executable=sb,
            command_runner=fake,
        )
        with pytest.raises(SystemdSoakError):
            mgr.install()

    def test_idempotent_uninstall(self, tmp_path: Path, fake_user_group: None) -> None:
        unit_dir = tmp_path / "systemd"
        unit_dir.mkdir()
        cfg = tmp_path / "recorder.toml"
        cfg.write_text('[recorder]\ndata_root = "' + str(tmp_path / "data") + '"')
        (tmp_path / "data").mkdir()
        sb = tmp_path / "python3"
        sb.write_text("#!/bin/sh\necho ok\n")
        sb.chmod(0o755)

        fake = FakeSystemctl(SOAK_TIMER_NAME)
        mgr = SoakTimerManager(
            config_file=cfg, user="testuser", group="testgroup",
            storage_id="test", output_path=tmp_path / "samples.jsonl",
            unit_dir=unit_dir, python_executable=sb,
            command_runner=fake,
        )
        mgr.install()
        mgr.uninstall()
        assert not (unit_dir / SOAK_SERVICE_NAME).exists()
        mgr.uninstall()
        assert not (unit_dir / SOAK_SERVICE_NAME).exists()
