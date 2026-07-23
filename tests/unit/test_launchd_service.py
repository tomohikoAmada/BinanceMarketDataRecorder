from __future__ import annotations

import json
import os
import plistlib
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import pytest

from binance_market_data_recorder.cli import main
from binance_market_data_recorder.service.launchd import (
    LaunchAgentError,
    LaunchAgentManager,
    installed_service_label,
    validate_service_label,
)
from binance_market_data_recorder.storage.layout import ensure_storage_layout

LABEL = "dev.recorderowner.BinanceMarketDataRecorder"
ROOT = Path(__file__).resolve().parents[2]


class FakeLaunchctl:
    def __init__(self) -> None:
        self.loaded = False
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
        call = tuple(arguments)
        self.calls.append(call)
        if call[:2] == ("/bin/launchctl", "print"):
            return subprocess.CompletedProcess(
                call,
                0 if self.loaded else 113,
                stdout="loaded" if self.loaded else "",
                stderr="" if self.loaded else "not found",
            )
        if call[:2] == ("/bin/launchctl", "bootstrap"):
            self.loaded = True
        elif call[:2] == ("/bin/launchctl", "bootout"):
            self.loaded = False
        return subprocess.CompletedProcess(call, 0, stdout="", stderr="")


def _manager(tmp_path: Path, runner: FakeLaunchctl) -> LaunchAgentManager:
    layout = ensure_storage_layout(tmp_path / "internal")
    return LaunchAgentManager(
        data_root=layout.root,
        label=LABEL,
        home=tmp_path / "home",
        uid=os.getuid(),
        command_runner=runner,
        python_executable=Path(sys.executable),
    )


def test_service_label_requires_author_namespace_and_project_suffix() -> None:
    assert validate_service_label(LABEL) == LABEL
    for invalid in (
        f"com.{'binance'}.BinanceMarketDataRecorder",
        f"org.{'binance'}.BinanceMarketDataRecorder",
        f"io.{'binance'}.BinanceMarketDataRecorder",
        "com.example.BinanceMarketDataRecorder",
        "dev.owner.OtherService",
        "BinanceMarketDataRecorder",
    ):
        with pytest.raises(LaunchAgentError):
            validate_service_label(invalid)


def test_install_renders_secure_user_launchagent_and_controls_lifecycle(
    tmp_path: Path,
) -> None:
    runner = FakeLaunchctl()
    manager = _manager(tmp_path, runner)
    environment = {
        "BINANCE_MARKET_RECORDER_DATA_ROOT": str(manager.data_root),
        "BINANCE_MARKET_RECORDER_PREVENT_SLEEP": "false",
    }
    result = manager.install(
        author_controls_namespace=True,
        config_file=None,
        git_commit="abc123",
        environment=environment,
    )
    assert result["loaded"] is True
    assert result["root"] is False
    assert manager.plist_path.stat().st_mode & 0o777 == 0o600
    with manager.plist_path.open("rb") as handle:
        plist = plistlib.load(handle)
    assert plist["Label"] == LABEL
    assert plist["Program"] == str(Path(sys.executable).resolve())
    assert plist["ProgramArguments"][-2:] == ["_service", "run"]
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] == {"SuccessfulExit": False}
    assert plist["LimitLoadToSessionType"] == "Aqua"
    assert plist["StandardOutPath"].endswith("/logs/launchd.stdout.log")
    assert plist["StandardErrorPath"].endswith("/logs/launchd.stderr.log")
    assert plist["EnvironmentVariables"]["BINANCE_MARKET_RECORDER_GIT_COMMIT"] == "abc123"
    assert not {"UserName", "GroupName"} & set(plist)
    assert installed_service_label(manager.data_root) == LABEL

    assert manager.start()["loaded"] is True
    assert any(call[:2] == ("/bin/launchctl", "kickstart") for call in runner.calls)
    assert manager.stop()["loaded"] is False
    assert manager.start()["loaded"] is True
    removed = manager.uninstall()
    assert removed["status"] == "UNINSTALLED"
    assert not manager.plist_path.exists()
    assert not manager.metadata_path.exists()
    assert runner.loaded is False


def test_install_requires_namespace_attestation_and_secure_config(
    tmp_path: Path,
) -> None:
    runner = FakeLaunchctl()
    manager = _manager(tmp_path, runner)
    with pytest.raises(LaunchAgentError, match="ownership confirmation"):
        manager.install(
            author_controls_namespace=False,
            config_file=None,
            git_commit=None,
            environment={},
        )

    config = tmp_path / "recorder.toml"
    config.write_text("[recorder]\n", encoding="utf-8")
    config.chmod(0o644)
    with pytest.raises(LaunchAgentError, match="mode 0600"):
        manager.install(
            author_controls_namespace=True,
            config_file=config,
            git_commit=None,
            environment={},
        )
    assert not manager.plist_path.exists()


def test_failed_bootstrap_rolls_back_new_plist(tmp_path: Path) -> None:
    class RefusedBootstrap(FakeLaunchctl):
        def __call__(
            self, arguments: Sequence[str]
        ) -> subprocess.CompletedProcess[str]:
            call = tuple(arguments)
            if call[:2] == ("/bin/launchctl", "bootstrap"):
                self.calls.append(call)
                return subprocess.CompletedProcess(
                    call, 5, stdout="", stderr="injected refusal"
                )
            return super().__call__(arguments)

    runner = RefusedBootstrap()
    manager = _manager(tmp_path, runner)
    with pytest.raises(LaunchAgentError, match="injected refusal"):
        manager.install(
            author_controls_namespace=True,
            config_file=None,
            git_commit=None,
            environment={},
        )
    assert not manager.plist_path.exists()
    assert not manager.metadata_path.exists()


def test_second_label_cannot_take_over_registered_data_root(tmp_path: Path) -> None:
    runner = FakeLaunchctl()
    manager = _manager(tmp_path, runner)
    manager.install(
        author_controls_namespace=True,
        config_file=None,
        git_commit=None,
        environment={},
    )
    conflicting = LaunchAgentManager(
        data_root=manager.data_root,
        label="net.author.other.BinanceMarketDataRecorder",
        home=manager.home,
        uid=os.getuid(),
        command_runner=runner,
        python_executable=Path(sys.executable),
    )
    with pytest.raises(LaunchAgentError, match="different LaunchAgent label"):
        conflicting.install(
            author_controls_namespace=True,
            config_file=None,
            git_commit=None,
            environment={},
        )
    with pytest.raises(LaunchAgentError, match="different LaunchAgent label"):
        conflicting.uninstall()
    assert manager.plist_path.is_file()
    assert manager.metadata_path.is_file()


def test_launchd_cli_is_structured_without_real_platform_mutation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeManager:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def install(self, **kwargs: object) -> dict[str, object]:
            assert kwargs["author_controls_namespace"] is True
            environment = kwargs["environment"]
            assert isinstance(environment, dict)
            assert "BINANCE_MARKET_RECORDER_DATA_ROOT" in environment
            return {"status": "INSTALLED", "loaded": True, "root": False}

    monkeypatch.setattr(
        "binance_market_data_recorder.cli.LaunchAgentManager",
        FakeManager,
    )
    monkeypatch.setenv(
        "BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path / "internal")
    )
    assert (
        main(
            [
                "launchd",
                "install",
                "--label",
                LABEL,
                "--author-controls-namespace",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "launchd.install"
    assert payload["status"] == "INSTALLED"


def test_service_wrapper_scripts_are_executable_and_never_use_sudo() -> None:
    for name in (
        "install-launchagent",
        "uninstall-launchagent",
        "start-recorder",
        "stop-recorder",
        "status-recorder",
    ):
        path = ROOT / "scripts" / name
        assert path.is_file()
        assert path.stat().st_mode & 0o111
        body = path.read_text(encoding="utf-8")
        assert "sudo" not in body
        assert "LaunchDaemon" not in body
