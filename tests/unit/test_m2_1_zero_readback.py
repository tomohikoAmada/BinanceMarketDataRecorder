from __future__ import annotations

import io
import json
import os
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
import zstandard

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.service.verification import verify_chunk, verify_new
from binance_market_data_recorder.spool import seal as seal_module
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def test_compression_handles_short_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ShortWritingFile(io.FileIO):
        def write(self, data: Any) -> int:
            written = super().write(data[:7])
            assert written is not None
            return written

    monkeypatch.setattr(
        os, "fdopen",
        lambda fd, mode, **kwargs: ShortWritingFile(fd, mode, closefd=kwargs["closefd"]),
    )
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT",
            stream="diff_depth", collector_instance_id="collector-m2-1",
            collector_version="0.1.0+test",
        )
        writer.append(_make_envelope(1))
        manifest = writer.seal()
        row = catalog.chunk(str(manifest["chunk_id"]))
        assert row is not None
        assert verify_chunk(layout, catalog, row, {})["result"] == "VERIFIED"


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


def test_stream_rotation_and_stop_never_open_compressed_raw_for_reading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    original_open = Path.open

    def check_open(path: Path, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        if ".zst" in path.name and ("r" in mode or "+" in mode):
            raise AssertionError(f"live compressed Raw read: {path}")
        return original_open(path, mode, *args, **kwargs)

    manifests: list[dict[str, object]] = []
    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT",
            stream="diff_depth", collector_instance_id="collector-m2-1",
            collector_version="0.1.0+test", queue_capacity=10,
            rotation=RotationPolicy(seconds=900, bytes=1024 * 1024),
            durability_interval_seconds=1, max_frame_bytes=2 * 1024 * 1024,
            seal_observer=manifests.append,
        )
        monkeypatch.setattr(Path, "open", check_open)
        spool.enqueue(_make_envelope(1, payload=b"x" * (1024 * 1024)))
        assert spool.drain_one()  # Size rotation.
        assert len(manifests) == 1
        spool.enqueue(_make_envelope(2))
        assert spool.drain_one()
        assert len(manifests) == 1
        spool.close_and_seal()  # Graceful stop.
        assert len(manifests) == 2
        monkeypatch.undo()
        for manifest in manifests:
            row = catalog.chunk(str(manifest["chunk_id"]))
            assert row is not None
            assert verify_chunk(layout, catalog, row, {})["result"] == "VERIFIED"


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
        manifest = writer.seal(layout=layout, catalog=catalog)

    monkeypatch.setenv("BINANCE_MARKET_RECORDER_DATA_ROOT", str(tmp_path))
    exit_code = main(["verify-raw"])
    assert exit_code == 0
    captured = capsys.readouterr()
    output_data = json.loads(captured.out)
    assert output_data["command"] == "verify-raw"
    assert output_data["verified_files"] == 1
    assert output_data["failed_files"] == 0

    chunk_id = str(manifest["chunk_id"])
    artifact = layout.sealed / f"{UUID(chunk_id).hex}.bmdr.zst"
    corrupted = bytearray(artifact.read_bytes())
    corrupted[-1] ^= 1
    artifact.write_bytes(corrupted)
    assert main(["verify-raw", "--chunk-id", chunk_id]) == 2
    failure = json.loads(capsys.readouterr().out)
    assert failure["processed_files"] == 1
    assert failure["failed_this_batch"] == 1
