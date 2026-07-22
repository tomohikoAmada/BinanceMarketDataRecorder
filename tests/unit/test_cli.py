from __future__ import annotations

import json

import pytest

from binance_market_data_recorder.cli import main


def test_version_identifies_distribution(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["--version"])
    assert raised.value.code == 0
    assert capsys.readouterr().out.startswith("binance-market-data-recorder 0.1.0")


def test_config_show_is_structured_and_credential_free(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", "/var/lib/binance-recorder")
    assert main(["config", "show"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "config.show"
    assert payload["contains_credentials"] is False
    assert set(payload["config"]) == {
        "data_root",
        "durability_interval_seconds",
        "ingress_queue_capacity",
        "log_level",
        "max_frame_bytes",
        "rotation_bytes",
        "rotation_seconds",
    }


def test_doctor_is_structured_and_non_mutating(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", "/var/lib/binance-recorder")
    exit_code = main(["doctor"])
    payload = json.loads(capsys.readouterr().out)
    assert exit_code in {0, 1}
    assert payload["command"] == "doctor"
    assert payload["network_accessed"] is False
    assert payload["filesystem_mutated"] is False
    assert isinstance(payload["checks"], list)


def test_status_does_not_invent_a_running_collector(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["status"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "NOT_RUNNING"
    assert payload["collector_implemented"] is True
    assert payload["implemented_markets"] == ["spot", "um_perpetual"]
    assert payload["network_connected"] is False


def test_invalid_command_has_machine_readable_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["not-a-command"])
    assert raised.value.code == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"] == "argument_error"


def test_invalid_configuration_returns_two(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_API_KEY", "not-supported")
    assert main(["status"]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "configuration_error"


def test_unsafe_data_root_returns_two(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", "/tmp/persistent-data")
    assert main(["status"]) == 2
    payload = json.loads(capsys.readouterr().err)
    assert payload["error"] == "configuration_error"
    assert "forbidden_persistent_location" in payload["message"]
