from __future__ import annotations

import json
from pathlib import Path

import pytest

from binance_market_data_recorder.cli import main
from binance_market_data_recorder.metrics.model import MetricAggregate
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from binance_market_data_recorder.storage.macos import PlatformEjectResult, VolumeInfo
from tests.archive_support import FixedVolumes, prepare_archive


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
    assert {
        "data_root",
        "durability_interval_seconds",
        "ingress_queue_capacity",
        "log_level",
        "max_frame_bytes",
        "heartbeat_seconds",
        "sleep_gap_threshold_seconds",
        "prevent_sleep",
        "rotation_bytes",
        "rotation_seconds",
    } <= set(payload["config"])
    assert all(
        forbidden not in key.casefold()
        for key in payload["config"]
        for forbidden in (
            "api_key",
            "api_secret",
            "secret_key",
            "credential",
            "private_key",
        )
    )


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
    assert payload["service_implemented"] is True
    assert payload["runtime_metrics"]["process_cpu_seconds"]["status"] == "NOT_RUNNING"


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


def test_normalize_status_is_structured_and_non_mutating(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "not-created"
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(data_root))
    assert main(["normalize", "status"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "build_count": 0,
        "builds": [],
        "command": "normalize.status",
        "dataset_version": "normalized-dataset.v1",
        "status": "NO_BUILDS",
    }
    assert not data_root.exists()


def test_normalize_run_without_raw_is_honest(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    assert main(["normalize", "run"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["command"] == "normalize.run"
    assert result["status"] == "NO_RAW_CHUNKS"
    assert result["normalized_rows"] == 0


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
    assert payload["network_status"] == "SERVICE_NOT_RUNNING"
    assert payload["service_state_error"] == "unsupported service state schema"


def test_status_rejects_a_stale_but_well_formed_running_heartbeat(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    (layout.state / "service_state.json").write_text(
        json.dumps(
            {
                "schema_version": "service-state.v1",
                "status": "RUNNING",
                "pid": 1,
                "heartbeat_at_utc_ns": 1,
                "heartbeat_interval_seconds": 1.0,
                "network_connected": True,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    assert main(["status"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "STALE"
    assert payload["network_connected"] is False
    assert payload["service_state_error"] == "service_heartbeat_stale"


def test_storage_list_is_structured_and_display_only(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    class NoExternalVolumes:
        def inventory(self) -> list[VolumeInfo]:
            return []

    monkeypatch.setattr(
        "binance_market_data_recorder.cli.DiskArbitrationAdapter",
        NoExternalVolumes,
    )
    assert main(["storage", "list"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "command": "storage.list",
        "external_volume_count": 0,
        "filesystem_mutated": False,
        "status": "OK",
        "volumes": [],
    }


def test_storage_register_status_unregister_cli(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mountpoint = tmp_path / "external"
    folder = mountpoint / "Archive" / "Recorder"
    folder.mkdir(parents=True)
    observed = VolumeInfo(
        disk_id="disk9s1",
        volume_uuid="11111111-2222-3333-4444-555555555555",
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=mountpoint,
        writable=True,
        internal=False,
        removable=True,
        total_bytes=1_000_000,
        free_bytes=900_000,
        observed_at_utc_ns=1,
    )

    class OneExternalVolume:
        def inventory(self) -> list[VolumeInfo]:
            return [observed]

    monkeypatch.setattr(
        "binance_market_data_recorder.cli.DiskArbitrationAdapter",
        OneExternalVolume,
    )
    monkeypatch.setenv(
        "BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path / "internal")
    )
    assert main(["storage", "register", str(folder)]) == 0
    registered = json.loads(capsys.readouterr().out)
    assert registered["state"] == "READY"
    storage_id = registered["storage_id"]

    assert main(["storage", "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["targets"][0]["state"] == "READY"
    assert status["targets"][0]["storage_id"] == storage_id

    assert main(["storage", "unregister", storage_id]) == 0
    unregistered = json.loads(capsys.readouterr().out)
    assert unregistered["status"] == "UNREGISTERED"
    assert unregistered["marker_deleted"] is False


def test_archive_status_retry_and_verify_cli(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_archive(tmp_path)
    volume = VolumeInfo(
        disk_id="disk9s1",
        volume_uuid=prepared.target.volume_uuid,
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=prepared.target.root.parents[1],
        writable=True,
        internal=False,
        removable=True,
        total_bytes=10**9,
        free_bytes=9 * 10**8,
        observed_at_utc_ns=1,
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.cli.DiskArbitrationAdapter",
        lambda: FixedVolumes(volume),
    )
    monkeypatch.setenv(
        "BINANCE_MARKET_RECORDER_DATA_ROOT", str(prepared.layout.root)
    )

    assert main(["archive", "status"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["backlog_files"] == 1
    assert status["transaction_count"] == 0

    assert (
        main(
            [
                "archive",
                "retry",
                "--storage-id",
                prepared.target.storage_id,
            ]
        )
        == 0
    )
    retry = json.loads(capsys.readouterr().out)
    assert retry["state"] == "LOCAL_DELETED"
    assert retry["warning"]

    assert main(["archive", "verify", prepared.target.storage_id]) == 0
    verify = json.loads(capsys.readouterr().out)
    assert verify["status"] == "VERIFIED"
    assert verify["verified_files"] == 1


def test_archive_retry_requires_a_ready_registered_target(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_archive(tmp_path)
    monkeypatch.setattr(
        "binance_market_data_recorder.cli.DiskArbitrationAdapter",
        lambda: FixedVolumes(
            VolumeInfo(
                disk_id="disk9s1",
                volume_uuid="DIFFERENT-VOLUME",
                name="Other",
                filesystem_type="apfs",
                mountpoint=tmp_path / "other",
                writable=True,
                internal=False,
                removable=True,
                total_bytes=1,
                free_bytes=1,
                observed_at_utc_ns=1,
            )
        ),
    )
    monkeypatch.setenv(
        "BINANCE_MARKET_RECORDER_DATA_ROOT", str(prepared.layout.root)
    )
    assert main(["archive", "retry"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"] == "archive_error"
    assert "no READY archive target" in error["message"]


def test_storage_eject_cli_only_succeeds_after_platform_confirmation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_archive(tmp_path)
    volume = VolumeInfo(
        disk_id="disk9s1",
        volume_uuid=prepared.target.volume_uuid,
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=prepared.target.root.parents[1],
        writable=True,
        internal=False,
        removable=True,
        total_bytes=10**9,
        free_bytes=9 * 10**8,
        observed_at_utc_ns=1,
    )

    class ConfirmedEject:
        def inventory(self) -> list[VolumeInfo]:
            return [volume]

        def request_eject(
            self, observed: VolumeInfo, *, timeout_seconds: float = 30.0
        ) -> PlatformEjectResult:
            assert observed == volume
            assert timeout_seconds == 5.0
            return PlatformEjectResult(
                disk_id=volume.disk_id,
                unmounted=True,
                ejected=True,
                failed_stage=None,
                dissenter_status=None,
                dissenter_message=None,
            )

    monkeypatch.setattr(
        "binance_market_data_recorder.cli.DiskArbitrationAdapter",
        ConfirmedEject,
    )
    monkeypatch.setenv(
        "BINANCE_MARKET_RECORDER_DATA_ROOT", str(prepared.layout.root)
    )
    assert (
        main(
            [
                "storage",
                "eject",
                prepared.target.storage_id,
                "--timeout-seconds",
                "5",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["command"] == "storage.eject"
    assert payload["status"] == "SAFE_TO_REMOVE"
    assert payload["safe_to_remove"] is True
    assert payload["forced"] is False


def test_storage_forecast_cli_persists_structured_internal_sample(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NoExternalVolumes:
        def inventory(self) -> list[VolumeInfo]:
            return []

    monkeypatch.setattr(
        "binance_market_data_recorder.cli.DiskArbitrationAdapter",
        NoExternalVolumes,
    )
    data_root = tmp_path / "internal"
    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(data_root))
    assert main(["storage", "forecast"]) == 0
    payload = json.loads(
        capsys.readouterr().out,
        parse_constant=lambda value: pytest.fail(f"non-finite JSON: {value}"),
    )
    assert payload["command"] == "storage.forecast"
    assert payload["schema_version"] == "storage-forecast.v1"
    assert payload["targets"][0]["scope_id"] == "internal"
    assert payload["targets"][0]["net_growth"]["status"] == "INSUFFICIENT_DATA"
    assert payload["storage_states"] == []
    assert (data_root / "state" / "catalog.sqlite").is_file()
