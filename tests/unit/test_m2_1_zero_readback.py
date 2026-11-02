from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import zstandard

from binance_market_data_recorder.config import RecorderConfig, load_config
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.service.runtime import ServiceRuntime
from binance_market_data_recorder.service.verification import verify_chunk, verify_new
from binance_market_data_recorder.spool import seal as seal_module
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def _make_envelope(ordinal: int, payload: bytes = b'{"msg": "test"}') -> EventEnvelope:
    return EventEnvelope(
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        module="binance.spot.v1",
        connection_id="conn-1",
        collector_instance_id="collector-m2-1",
        collector_version="0.1.0+test",
        receive_time_utc_ns=1_700_000_000_000_000_000 + ordinal,
        receive_monotonic_ns=10_000_000_000 + ordinal,
        exchange_event_time=1_700_000_000_000 + ordinal,
        source_sequence={"U": ordinal, "u": ordinal},
        raw_payload=payload,
    )


def test_clean_seal_zero_readback_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prove that live sealing performs ZERO readback calls on the newly created .zst."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-m2-1",
            collector_version="0.1.0+test",
        )
        for i in range(10):
            writer.append(_make_envelope(i + 1, payload=f'{{"count": {i}}}'.encode()))

        # If _artifact_identity or read_artifact is called during clean seal, fail test
        def forbidden_read_artifact(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError(
                "read_artifact / _artifact_identity must NOT be called on live seal path"
            )

        monkeypatch.setattr(seal_module, "_artifact_identity", forbidden_read_artifact)
        monkeypatch.setattr(seal_module, "read_artifact", forbidden_read_artifact)

        # Seal should succeed with ZERO readback
        manifest = writer.seal(layout=layout, catalog=catalog)

        assert manifest["complete"] is True
        assert manifest["record_count"] == 10
        chunk_id = str(manifest["chunk_id"])
        assert catalog.state(chunk_id) is ChunkState.SEALED

        sealed_file = layout.sealed / f"{UUID(chunk_id).hex}.bmdr.zst"
        assert sealed_file.exists()
        assert not writer.path.exists()

        # Restore read_artifact and verify that the sealed file is 100% valid
        monkeypatch.undo()
        decompressed_data = zstandard.ZstdDecompressor().decompress(
            sealed_file.read_bytes()
        )
        assert len(decompressed_data) == manifest["uncompressed_bytes"]

        # Full independent verification succeeds
        row = catalog.chunk(chunk_id)
        assert row is not None
        verified_evidence = verify_chunk(layout, catalog, row, {})
        assert verified_evidence["result"] == "VERIFIED"


def test_seal_partial_zero_readback_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prove that seal_partial also defaults to zero readback while validating source on the fly."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-m2-1",
            collector_version="0.1.0+test",
        )
        writer.append(_make_envelope(1, payload=b"partial-test"))
        writer.close()

        def forbidden_read_artifact(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("readback must NOT be called on seal_partial by default")

        monkeypatch.setattr(seal_module, "_artifact_identity", forbidden_read_artifact)

        manifest = seal_partial(writer.path, layout=layout, catalog=catalog)
        assert manifest["complete"] is True
        assert manifest["record_count"] == 1


def test_verify_raw_cli_offline_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prove that offline verify_new / CLI inspects frames and detects tampering."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-m2-1",
            collector_version="0.1.0+test",
        )
        writer.append(_make_envelope(1))
        manifest = writer.seal(layout=layout, catalog=catalog)
        chunk_id = str(manifest["chunk_id"])

        # Offline verification succeeds
        result = verify_new(layout, catalog)
        assert result["verified_files"] == 1
        assert result["failed_files"] == 0

        # Now tamper with sealed file and verify that verification detects it
        sealed_file = layout.sealed / f"{UUID(chunk_id).hex}.bmdr.zst"
        corrupted = bytearray(sealed_file.read_bytes())
        corrupted[-1] ^= 0x01
        sealed_file.write_bytes(corrupted)

        # Re-enroll and verify that tampering is caught
        catalog.enroll_raw_verification(chunk_id)
        result_corrupted = verify_new(layout, catalog)
        assert result_corrupted["failed_this_batch"] == 1


def test_cli_verify_raw_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test binance-market-recorder verify-raw CLI command."""
    from binance_market_data_recorder.cli import main

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-m2-1",
            collector_version="0.1.0+test",
        )
        writer.append(_make_envelope(1))
        writer.seal(layout=layout, catalog=catalog)

    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    exit_code = main(["verify-raw"])
    assert exit_code == 0
    captured = capsys.readouterr()
    output_data = json.loads(captured.out)
    assert output_data["command"] == "verify-raw"
    assert output_data["verified_files"] == 1
    assert output_data["failed_files"] == 0


def test_config_live_raw_verification_setting() -> None:
    """Verify config defaults and environment toggle for live raw verification."""
    fake_home = Path("/Users/config-test-user")
    config = load_config(environ={}, home=fake_home).config
    assert config.live_raw_verification_enabled is False

    # Enabled via environment
    env_config = load_config(
        home=fake_home,
        environ={"BINANCE_MARKET_RECORDER_LIVE_RAW_VERIFICATION_ENABLED": "1"},
    ).config
    assert env_config.live_raw_verification_enabled is True


def test_runtime_does_not_spawn_verification_worker_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify ServiceRuntime does not launch verification task during capture by default."""
    config = RecorderConfig(
        data_root=tmp_path / "data",
        spot_symbols=("BTCUSDT",),
        usdm_symbols=(),
        rotation_seconds=900.0,
        durability_interval_seconds=1.0,
    )
    assert config.live_raw_verification_enabled is False

    runtime = ServiceRuntime(config=config, logger=logging.getLogger("test.m2_1"))
    tasks_created: list[str] = []
    original_create_task = asyncio.create_task

    def track_create_task(coro: Any, *, name: str | None = None, **kwargs: Any) -> Any:
        if name:
            tasks_created.append(name)
        coro.close()
        return original_create_task(asyncio.sleep(0), name=name or "mock")

    monkeypatch.setattr(asyncio, "create_task", track_create_task)
    runtime.request_stop("TEST_STOP")

    with pytest.raises(RuntimeError):
        asyncio.run(runtime.run())

    assert "GLOBAL:raw-verification-owner" not in tasks_created
