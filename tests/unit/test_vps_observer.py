"""Unit tests for the non-intrusive VPS external observer script."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from tools.observe_vps_recorder import (
    ProcessObservation,
    ProcessSampler,
    RawChunkTracker,
    StatusObservation,
    SystemdObservation,
    main,
    query_disk,
    query_status_cli,
    query_systemd,
)


def test_systemd_query_parsing_success() -> None:
    mock_output = (
        "ActiveState=active\n"
        "SubState=running\n"
        "MainPID=12345\n"
        "InvocationID=abcdef1234567890\n"
        "NRestarts=0\n"
        "ActiveEnterTimestamp=Tue 2026-11-03 01:00:00 UTC\n"
        "Result=success\n"
        "MemoryCurrent=104857600\n"
        "CPUUsageNSec=5000000000\n"
    )

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=mock_output,
            stderr="",
        )
        obs = query_systemd("binance-market-data-recorder.service", systemctl_path="systemctl")

    assert obs.available is True
    assert obs.active_state == "active"
    assert obs.sub_state == "running"
    assert obs.main_pid == 12345
    assert obs.invocation_id == "abcdef1234567890"
    assert obs.n_restarts == 0
    assert obs.active_enter_timestamp == "Tue 2026-11-03 01:00:00 UTC"
    assert obs.result == "success"
    assert obs.memory_current_bytes == 104857600
    assert obs.cpu_usage_nsec == 5000000000
    assert obs.error is None


def test_systemd_query_missing_binary() -> None:
    with patch("subprocess.run", side_effect=FileNotFoundError):
        obs = query_systemd(
            "binance-market-data-recorder.service",
            systemctl_path="nonexistent-cmd",
        )

    assert obs.available is False
    assert obs.active_state == "UNKNOWN"
    assert obs.main_pid == 0
    assert obs.error == "systemctl_not_found"


def test_systemd_query_failed_command() -> None:
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout="",
            stderr="Failed to get properties: Unit not found.",
        )
        obs = query_systemd("unknown.service", systemctl_path="systemctl")

    assert obs.available is False
    assert obs.active_state == "UNKNOWN"
    assert obs.error == "Failed to get properties: Unit not found."


def test_process_sampler_dead_pid() -> None:
    sampler = ProcessSampler()
    obs = sampler.sample(pid=99999999)
    assert obs.alive is False
    assert obs.cpu_percent == 0.0
    assert obs.rss_bytes == 0


def test_process_sampler_cputime_parsing() -> None:
    assert ProcessSampler._parse_cputime("00:15.50") == 15.50
    assert ProcessSampler._parse_cputime("01:30.00") == 90.0
    assert ProcessSampler._parse_cputime("02:10:05.50") == 7805.50
    assert ProcessSampler._parse_cputime("1-02:03:04.00") == 86400.0 + 7200.0 + 180.0 + 4.0


def test_process_sampler_with_mocked_alive(tmp_path: Path) -> None:
    sampler = ProcessSampler()
    pid = os.getpid()

    # First sample sets baseline
    with patch.object(sampler, "_is_alive", return_value=True), patch.object(
        sampler, "_read_proc_stats", return_value=(10.0, 50 * 1024 * 1024)
    ):
        obs1 = sampler.sample(pid)
        assert obs1.alive is True
        assert obs1.cpu_time_seconds == 10.0
        assert obs1.rss_bytes == 50 * 1024 * 1024
        assert obs1.cpu_percent == 0.0  # Initial sample has 0% delta

    # Second sample calculates delta CPU %
    with (
        patch.object(sampler, "_is_alive", return_value=True),
        patch.object(sampler, "_read_proc_stats", return_value=(11.0, 52 * 1024 * 1024)),
        patch("time.monotonic", return_value=sampler._prev_wall_time + 1.0),
    ):
        obs2 = sampler.sample(pid)
        assert obs2.alive is True
        assert obs2.cpu_time_seconds == 11.0
        assert obs2.cpu_percent == 100.0  # 1.0s CPU over 1.0s wall


def test_raw_chunk_tracker(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    tracker = RawChunkTracker()
    now = 1000.0

    # Initial sample: 0 chunks
    obs1 = tracker.sample(raw_dir, now)
    assert obs1.sealed_zst_count == 0
    assert obs1.active_partial_count == 0
    assert obs1.total_sealed_bytes == 0
    assert obs1.growth_rate_chunks_per_hour == 0.0

    # Create 2 sealed .zst (each 10 MiB) and 1 .partial
    chunk1 = raw_dir / "chunk1.zst"
    chunk1.write_bytes(b"x" * (10 * 1024 * 1024))
    chunk2 = raw_dir / "chunk2.zst"
    chunk2.write_bytes(b"x" * (10 * 1024 * 1024))
    (raw_dir / "active.partial").write_bytes(b"abc")
    (raw_dir / "chunk1.json").write_text("{}")

    # Sample 1800s later (0.5 hours)
    obs2 = tracker.sample(raw_dir, now + 1800.0)
    assert obs2.sealed_zst_count == 2
    assert obs2.active_partial_count == 1
    assert obs2.manifest_json_count == 1
    assert obs2.total_sealed_bytes == 20 * 1024 * 1024
    # Growth rate: 2 chunks / 0.5 hr = 4.0 chunks/hr
    assert obs2.growth_rate_chunks_per_hour == 4.0
    # MB rate: 20 MB / 0.5 hr = 40.0 MB/hr
    assert obs2.growth_rate_mb_per_hour == 40.0


def test_query_status_cli_fallback(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    state_file = state_dir / "service_state.json"
    state_file.write_text(
        json.dumps(
            {
                "status": "RUNNING",
                "pid": 4242,
                "network_connected": True,
                "network_status": "OK",
            }
        )
    )

    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="failed")
        obs = query_status_cli(None, tmp_path)

    assert obs.available is True
    assert obs.service_status == "RUNNING"
    assert obs.network_connected is True
    assert obs.network_status == "OK"


def test_query_disk() -> None:
    obs = query_disk(Path.home())
    assert obs.total_bytes > 0
    assert obs.free_bytes > 0
    assert 0.0 <= obs.used_percent <= 100.0
    assert obs.free_gib > 0


def test_take_sample_and_main_once(tmp_path: Path) -> None:
    output_file = tmp_path / "samples.jsonl"
    argv = [
        "--once",
        "--data-root",
        str(tmp_path),
        "--output",
        str(output_file),
    ]

    with patch("tools.observe_vps_recorder.query_systemd") as mock_sys:
        mock_sys.return_value = SystemdObservation(
            available=True,
            active_state="active",
            sub_state="running",
            main_pid=1111,
            invocation_id="test-inv-id",
            n_restarts=0,
            active_enter_timestamp="2026-11-03",
            result="success",
            memory_current_bytes=50000000,
            cpu_usage_nsec=1000000000,
        )
        with patch("tools.observe_vps_recorder.ProcessSampler.sample") as mock_proc:
            mock_proc.return_value = ProcessObservation(
                pid=1111,
                alive=True,
                cpu_percent=12.5,
                cpu_time_seconds=34.56,
                rss_bytes=50000000,
                rss_mib=47.684,
            )
            with patch("tools.observe_vps_recorder.query_status_cli") as mock_status:
                mock_status.return_value = StatusObservation(
                    available=True,
                    service_status="RUNNING",
                    network_connected=True,
                    network_status="CONNECTED",
                    catalog_sealed_chunks=10,
                    catalog_active_chunks=2,
                    space_severity="OK",
                    last_event_age_seconds=0.25,
                )
                exit_code = main(argv)

    assert exit_code == 0
    assert output_file.is_file()
    lines = output_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1

    sample = json.loads(lines[0])
    assert sample["sample_index"] == 1
    assert sample["systemd"]["active_state"] == "active"
    assert sample["process"]["pid"] == 1111
    assert sample["process"]["cpu_percent"] == 12.5
    assert sample["status"]["service_status"] == "RUNNING"
    assert sample["status"]["catalog_sealed_chunks"] == 10
