from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

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


def test_soak_sample_writes_valid_jsonl(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    _result = soak_sample(
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    assert output.is_file()
    lines = output.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    assert parsed["schema_version"] == "m21-soak-sample.v1"
    assert isinstance(parsed["sampled_at_utc_ns"], int)
    assert "proxy_url" not in str(parsed.get("config_hash", ""))


def test_soak_sample_two_samples_do_not_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    _r1 = soak_sample(
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )
    _r2 = soak_sample(
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
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
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
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
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    systemd = cast(Any, result).get("systemd", {})
    assert isinstance(systemd, dict)
    assert "recorder_active_state" in systemd


def test_soak_sample_external_absent_no_crash(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    disk = cast(Any, result).get("disk", {})
    assert isinstance(disk, dict)
    assert disk.get("external_space_severity") == "ABSENT"


def test_soak_sample_config_hash_excludes_credentials(tmp_path: Path) -> None:
    output = tmp_path / "samples.jsonl"
    data_root = tmp_path / "data_root"
    data_root.mkdir()
    (data_root / "state").mkdir()

    result = soak_sample(
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    hash_val = cast(Any, result).get("config_hash", "")
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
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
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
        data_root=data_root,
        output_path=output,
        config_dict=TEST_CONFIG,
        recorder_version="0.1.0-test",
    )

    assert b"secret raw data" not in output.read_bytes()
