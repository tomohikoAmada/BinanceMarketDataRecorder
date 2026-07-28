from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

import binance_market_data_recorder.soak.sample as sample_module
from binance_market_data_recorder.service.state import ServiceStateStore
from binance_market_data_recorder.soak.sample import soak_sample

TEST_CONFIG = {
    "data_root": "/tmp/test",
    "log_level": "INFO",
    "proxy_url": "http://127.0.0.1:7890",
    "proxy_username": "secret_user",
    "proxy_password": "secret_pass",
    "rotation_seconds": 3600,
    "proxy_port": 7890,
    "proxy_loopback": True,
}
TEST_STORAGE = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
SAMPLED_AT = 1_700_000_100_000_000_000


def _systemd(
    *,
    active_state: str = "active",
    sub_state: str = "running",
    pid: int | None = 1234,
    error: str | None = None,
) -> dict[str, object]:
    return {
        "recorder_active_state": active_state,
        "recorder_sub_state": sub_state,
        "recorder_main_pid": pid,
        "recorder_nrestarts": 0,
        "recorder_active_enter_timestamp_monotonic": 1,
        "recorder_service_result": "success",
        "recorder_error": error,
        "archive_timer_active_state": "inactive",
        "archive_service_result": None,
    }


def _write_service_state(
    data_root: Path,
    *,
    pid: int = 1234,
    heartbeat_at: int = SAMPLED_AT,
) -> None:
    ServiceStateStore(data_root / "state" / "service_state.json").write(
        {
            "status": "RUNNING",
            "pid": pid,
            "heartbeat_at_utc_ns": heartbeat_at,
            "heartbeat_interval_seconds": 5.0,
            "network_status": "READY",
            "markets": {
                "spot": {
                    "status": "READY",
                    "last_receive_time_utc_ns": heartbeat_at,
                },
                "um_perpetual": {
                    "status": "READY",
                    "last_receive_time_utc_ns": heartbeat_at,
                },
            },
            "runtime_metrics": {
                "current_rss_bytes": 123,
                "process_cpu_seconds": 4.5,
            },
            "proxy_url": "http://proxy-user:proxy-secret@127.0.0.1:7890",
        }
    )


def _sample_with_systemd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    systemd: dict[str, object],
) -> tuple[Path, dict[str, object]]:
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    monkeypatch.setattr(sample_module, "_systemd_status", lambda: systemd)
    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "samples.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )
    return data_root, result


def test_soak_sample_writes_valid_jsonl(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    _result = soak_sample(
        data_root=data_root,
        output_path=output,
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    assert output.is_file()
    lines = output.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["schema_version"] == "m21-soak-sample.v2"
    assert isinstance(parsed["sampled_at_utc_ns"], int)
    assert "proxy_url" not in str(parsed.get("config_hash", ""))


def test_soak_sample_two_samples_do_not_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    _r1 = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )
    _r2 = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    lines = output.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    parsed1 = json.loads(lines[0])
    parsed2 = json.loads(lines[1])
    assert parsed1["sample_id"] != parsed2["sample_id"]


def test_soak_sample_no_network_no_binance(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    payload = json.dumps(result)
    assert "wss://" not in payload
    assert "binance.com" not in payload.lower()
    assert "api_key" not in payload.lower()


def test_soak_sample_recorder_not_running(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    systemd = result.get("systemd", {})
    assert isinstance(systemd, dict)
    assert "recorder_active_state" in systemd


def test_soak_sample_external_absent_no_crash(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    disk = result.get("disk", {})
    assert isinstance(disk, dict)
    assert disk.get("external_space_severity") == "ABSENT"
    assert disk.get("external_storage_id") == TEST_STORAGE


def test_soak_sample_config_hash_excludes_credentials(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    hash_val = result.get("config_hash", "")
    assert isinstance(hash_val, str)
    assert "secret_user" not in hash_val
    assert "secret_pass" not in hash_val
    assert "proxy_url" not in hash_val


def test_soak_sample_output_dir_auto_created(tmp_path: Path) -> None:
    output = tmp_path / "deep/nested/samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    _result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    assert output.is_file()


def test_soak_sample_does_not_scan_raw_directory(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    raw_dir = data_root / "data" / "active"
    raw_dir.mkdir(parents=True)
    (raw_dir / "test.bmdr.partial").write_bytes(b"secret raw data")

    _result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    assert b"secret raw data" not in output.read_bytes()


def test_soak_sample_storage_id_in_output(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    assert cast(Any, result).get("disk", {}).get("external_storage_id") == TEST_STORAGE
    assert cast(Any, result).get("archive", {}).get("storage_id") == TEST_STORAGE


def test_soak_sample_each_line_independently_parseable(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    for _ in range(3):
        soak_sample(
            data_root=data_root, output_path=output,
            storage_id=TEST_STORAGE, config_dict=TEST_CONFIG,
            recorder_version="0.1.0-test",
        )

    for line in output.read_text().strip().split("\n"):
        parsed = json.loads(line)
        assert isinstance(parsed, dict)
        assert "schema_version" in parsed


def test_soak_sample_none_storage_id_does_not_crash(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root, output_path=output,
        storage_id=None, config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    assert cast(Any, result)["archive"]["storage_id"] is None
    assert cast(Any, result)["disk"]["external_storage_id"] is None


def test_soak_stale_heartbeat_never_reports_observed_ready_as_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    _write_service_state(
        data_root,
        heartbeat_at=SAMPLED_AT - 31_000_000_000,
    )
    monkeypatch.setattr(sample_module, "_systemd_status", _systemd)

    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "samples.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )

    service = cast(dict[str, object], result["service"])
    markets = cast(dict[str, object], result["markets"])
    process = cast(dict[str, object], result["process"])
    assert service["application_status"] == "STALE"
    assert service["service_state_fresh"] is False
    assert service["service_state_error"] == "service_heartbeat_stale"
    assert markets["observed_spot_state"] == "READY"
    assert markets["observed_usdm_state"] == "READY"
    assert markets["spot_state"] == "STALE"
    assert markets["usdm_state"] == "STALE"
    assert process["runtime_metrics_status"] == "STALE"
    assert process["runtime_metrics"] == {}


def test_soak_systemd_inactive_does_not_report_ready_markets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root, _ = _sample_with_systemd(
        tmp_path,
        monkeypatch,
        _systemd(active_state="inactive", sub_state="dead", pid=None),
    )
    _write_service_state(data_root)

    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "second.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )

    assert cast(Any, result)["service"]["application_status"] == "NOT_RUNNING"
    assert cast(Any, result)["markets"]["observed_spot_state"] == "READY"
    assert cast(Any, result)["markets"]["spot_state"] == "UNTRUSTED"
    assert cast(Any, result)["markets"]["usdm_state"] == "UNTRUSTED"


def test_soak_pid_mismatch_is_untrusted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    _write_service_state(data_root, pid=111)
    monkeypatch.setattr(sample_module, "_systemd_status", lambda: _systemd(pid=222))
    monkeypatch.setattr(sample_module, "_proc_metrics", lambda _pid: {})

    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "samples.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )

    service = cast(dict[str, object], result["service"])
    process = cast(dict[str, object], result["process"])
    assert service["systemd_main_pid"] == 222
    assert service["service_state_pid"] == 111
    assert service["pid_mismatch"] is True
    assert service["application_status"] == "UNTRUSTED"
    assert process["pid"] == 222
    assert process["runtime_metrics_status"] == "UNAVAILABLE"


def test_soak_proc_sampling_uses_systemd_main_pid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    _write_service_state(data_root, pid=111)
    monkeypatch.setattr(sample_module, "_systemd_status", lambda: _systemd(pid=222))
    sampled_pids: list[int] = []

    def fake_proc_metrics(pid: int) -> dict[str, object]:
        sampled_pids.append(pid)
        return {"open_fd_count": 7}

    monkeypatch.setattr(sample_module, "_proc_metrics", fake_proc_metrics)
    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "samples.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )

    assert sampled_pids == [222]
    assert cast(Any, result)["process"]["pid_source"] == "SYSTEMD_MAIN_PID"
    assert cast(Any, result)["process"]["open_fd_count"] == 7


def test_soak_fresh_matching_pid_reports_current_ready(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    _write_service_state(data_root, pid=1234)
    monkeypatch.setattr(sample_module, "_systemd_status", _systemd)
    monkeypatch.setattr(sample_module, "_proc_metrics", lambda _pid: {})

    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "samples.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )

    service = cast(dict[str, object], result["service"])
    process = cast(dict[str, object], result["process"])
    markets = cast(dict[str, object], result["markets"])
    assert {
        "systemd_active_state",
        "systemd_sub_state",
        "systemd_main_pid",
        "application_status",
        "service_state_pid",
        "pid_mismatch",
        "heartbeat_at_utc_ns",
        "heartbeat_age_ns",
        "service_state_fresh",
        "service_state_error",
    } <= service.keys()
    assert service["application_status"] == "RUNNING"
    assert service["service_state_fresh"] is True
    assert service["pid_mismatch"] is False
    assert markets["spot_state"] == "READY"
    assert markets["usdm_state"] == "READY"
    assert process["runtime_metrics_status"] == "CURRENT"
    assert cast(Any, process)["runtime_metrics"]["current_rss_bytes"] == 123


def test_soak_systemctl_unavailable_is_explicit_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    _write_service_state(data_root)
    monkeypatch.setattr(
        sample_module,
        "_systemd_status",
        lambda: _systemd(
            active_state="UNKNOWN",
            sub_state="UNKNOWN",
            pid=None,
            error="systemctl_unavailable:FileNotFoundError",
        ),
    )

    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "samples.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )

    assert cast(Any, result)["service"]["application_status"] == "UNKNOWN"
    assert cast(Any, result)["markets"]["spot_state"] == "UNKNOWN"
    assert cast(Any, result)["process"]["runtime_metrics_status"] == "UNAVAILABLE"


def test_systemd_status_queries_recorder_main_pid_once_and_reports_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def unavailable(arguments: list[str], **_kwargs: object) -> object:
        calls.append(arguments)
        raise FileNotFoundError("systemctl")

    monkeypatch.setattr(
        "binance_market_data_recorder.soak.sample.sys.platform", "linux"
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.soak.sample.subprocess.run", unavailable
    )
    result = sample_module._systemd_status()

    assert result["recorder_active_state"] == "UNKNOWN"
    assert result["recorder_main_pid"] is None
    assert result["recorder_error"] == "systemctl_unavailable:FileNotFoundError"
    assert sum("MainPID" in call for call in calls) == 1


def test_soak_does_not_leak_proxy_from_service_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()
    _write_service_state(data_root)
    monkeypatch.setattr(sample_module, "_systemd_status", _systemd)
    monkeypatch.setattr(sample_module, "_proc_metrics", lambda _pid: {})

    result = soak_sample(
        data_root=data_root,
        output_path=tmp_path / "samples.jsonl",
        storage_id=TEST_STORAGE,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
        utc_clock_ns=lambda: SAMPLED_AT,
    )
    serialized = json.dumps(result)

    assert "proxy-user" not in serialized
    assert "proxy-secret" not in serialized
    assert "proxy_url" not in serialized
