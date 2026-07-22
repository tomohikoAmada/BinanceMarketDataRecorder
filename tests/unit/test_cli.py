from __future__ import annotations

import json
from pathlib import Path

import pytest

from binance_market_data_recorder.cli import main
from binance_market_data_recorder.metrics.model import MetricAggregate
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


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
    assert payload["runtime_metrics"]["cpu_percent"]["status"] == "NOT_RUNNING"


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


def test_daily_report_cli_writes_structured_json_csv(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    aggregate = MetricAggregate()
    aggregate.increment("accepted")
    with Catalog(layout.catalog) as catalog:
        catalog.record_metric_batch(
            batch_id="cli-batch",
            rows=[("2026-07-22", "spot", "agg_trade", aggregate.document())],
        )
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    assert main(["report", "daily", "--date", "2026-07-22"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "report.daily"
    assert payload["status"] == "OK"
    assert (layout.daily_reports / "2026-07-22.json").is_file()
    assert (layout.daily_reports / "2026-07-22.csv").is_file()


def test_daily_report_cli_rejects_invalid_date(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog):
        pass
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    assert main(["report", "daily", "--date", "22-07-2026"]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "report_error"


def test_status_does_not_trust_an_unimplemented_service_state_as_running(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    (layout.state / "service_state.json").write_text(
        '{"status":"RUNNING","network_connected":true}'
    )
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    assert main(["status"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "NOT_RUNNING"
    assert payload["network_connected"] is False
    assert payload["network_status"] == "UNAVAILABLE_NO_SUPERVISED_SERVICE"
