"""Unit and regression tests for Milestone 1.1: Spool chunk rotation tuning.

Verifies:
1. Default rotation configuration (900.0s / 128 MiB) and valid ranges (600s-900s).
2. Deterministic phase staggering across the 900s (15m) rotation window.
3. Active area disk space accounting within ADR-0016 10 GiB hard reserve.
4. Fast scan_chunk performance and truncation recovery on 128 MiB chunks.
5. Ingress queue capacity during 128 MiB seal without IngressQueueFull.
6. Actual multi-rotation files, Catalog commits and archive reservations (> 90%).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from pathlib import Path
from threading import Event
from types import SimpleNamespace
from typing import Any

import pytest

from binance_market_data_recorder.binance.usdm.websocket import UsdMStreamCollector
from binance_market_data_recorder.config import RecorderConfig, load_config
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool import stream as stream_module
from binance_market_data_recorder.spool import writer as writer_module
from binance_market_data_recorder.spool.format import FRAME_PREFIX, encode_frame, scan_chunk
from binance_market_data_recorder.spool.recovery import recover_partials
from binance_market_data_recorder.spool.seal import _seal_clean_writer
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import (
    RawChunkWriter,
    RotationPolicy,
    _rotation_deadline,
)
from binance_market_data_recorder.storage.capacity import HARD_RESERVE_BYTES, VPS_PRODUCTION_V1
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.forecast import GIB
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tools.benchmark_m1_1_rotation import replay_rotation


def test_default_rotation_policy_and_config() -> None:
    policy = RotationPolicy()
    assert policy.seconds == 900.0
    assert policy.bytes == 128 * 1024 * 1024

    assert RecorderConfig.model_fields["rotation_seconds"].default == 900.0
    assert RecorderConfig.model_fields["rotation_bytes"].default == 128 * 1024 * 1024

    cfg_600 = RecorderConfig(
        data_root=Path("/tmp"),
        spot_symbols=("BTCUSDT",),
        rotation_seconds=600.0,
    )
    assert cfg_600.rotation_seconds == 600.0

    cfg_900 = RecorderConfig(
        data_root=Path("/tmp"),
        spot_symbols=("BTCUSDT",),
        rotation_seconds=900.0,
    )
    assert cfg_900.rotation_seconds == 900.0


def test_rotation_phase_staggering_for_900s() -> None:
    opened = 1_000.0
    period = 900.0

    def deadline(market: str, symbol: str, stream: str) -> float:
        return _rotation_deadline(
            opened_monotonic=opened,
            period_seconds=period,
            market=market,
            symbol=symbol,
            stream=stream,
        )

    stream_keys = [
        ("spot", "BTCUSDT", "diff_depth"),
        ("spot", "BTCUSDT", "book_ticker"),
        ("spot", "BTCUSDT", "agg_trade"),
        ("spot", "ETHUSDT", "diff_depth"),
        ("spot", "ETHUSDT", "book_ticker"),
        ("spot", "ETHUSDT", "agg_trade"),
        ("um_perpetual", "BTCUSDT", "diff_depth"),
        ("um_perpetual", "BTCUSDT", "book_ticker"),
        ("um_perpetual", "ETHUSDT", "diff_depth"),
        ("cm_perpetual", "BTCUSD_PERP", "diff_depth"),
        ("spot", "BTCUSDT", "kline_1m"),
        ("spot", "BTCUSDT", "kline_5m"),
    ]

    deadlines = [deadline(m, s, st) for m, s, st in stream_keys]

    # Every deadline must be strictly within (opened, opened + period]
    for d in deadlines:
        assert opened < d <= opened + period

    # Distinct streams must yield distinct phase deadlines
    unique_deadlines = set(deadlines)
    assert len(unique_deadlines) == len(stream_keys)

    # Spread spans across the window
    min_offset = min(d - opened for d in deadlines)
    max_offset = max(d - opened for d in deadlines)
    assert min_offset >= 0.0
    assert max_offset <= 900.0
    assert (max_offset - min_offset) > 300.0  # Well distributed across 15 minutes


def test_active_space_accounting_within_emergency_reserve() -> None:
    assert HARD_RESERVE_BYTES == VPS_PRODUCTION_V1.hard_reserve_bytes == 10 * GIB
    # Python 3.12's default executor has at most 32 workers. Existing active
    # partials have ALREADY reduced free space; reserve covers additional output.
    # Allow an entire maximum frame past the rotation threshold and 2x expansion
    # for compression (no assumed compression benefit). No parallel-seal claim
    # is derived from hash staggering, which can collide.
    max_raw_bytes = RotationPolicy().bytes + 16 * 1024 * 1024 + FRAME_PREFIX.size
    compression_temporary_budget = 32 * 2 * max_raw_bytes
    assert compression_temporary_budget < HARD_RESERVE_BYTES


def test_128mib_scan_chunk_and_truncation_recovery(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="m1-1-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=3600, bytes=256 * 1024 * 1024),
            durability_interval_seconds=1.0,
            max_frame_bytes=16 * 1024 * 1024,
        )
        payload = b"x" * (1024 * 1024 - 1000)
        target_bytes = 128 * 1024 * 1024
        count = 0
        while writer.bytes_written < target_bytes:
            writer.append(event(count + 1, payload=payload))
            count += 1
        writer.close()

        file_size = os.path.getsize(writer.path)
        assert file_size >= 128 * 1024 * 1024

        # Measure scan_chunk execution time on full 128 MiB chunk
        t0 = time.perf_counter()
        scan_res = scan_chunk(writer.path)
        scan_duration = time.perf_counter() - t0

        assert scan_res.is_clean
        assert scan_res.statistics.record_count == count

        # Inject crash truncation at the tail (half of a valid frame appended)
        corrupted_frame = encode_frame(
            event(count + 1, payload=payload), max_frame_bytes=16 * 1024 * 1024
        )
        with open(writer.path, "ab") as f:
            f.write(corrupted_frame[: len(corrupted_frame) // 2])

        # Verify recover_partials detects tail truncation and truncates back to clean state
        t0_rec = time.perf_counter()
        actions = recover_partials(layout=layout, catalog=catalog)
        rec_duration = time.perf_counter() - t0_rec

        assert len(actions) == 1
        assert actions[0].action == "tail_truncated"
        assert catalog.state(str(writer.header.chunk_id)) is ChunkState.RECOVERED

        recovered_scan = scan_chunk(writer.path)
        assert recovered_scan.is_clean
        assert recovered_scan.statistics.record_count == count
        print(json.dumps({"scan_bytes": file_size, "scan_seconds": scan_duration,
                          "recovery_seconds": rec_duration, "records": count}))


def test_ingress_queue_preservation_during_chunk_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    started, completed = Event(), Event()
    measurements: dict[str, float | int] = {}
    real_seal = _seal_clean_writer

    def observe_seal(writer: RawChunkWriter, **kwargs: object) -> dict[str, object]:
        if writer.bytes_written < 128 * 1024 * 1024:
            return real_seal(writer, **kwargs)  # type: ignore[arg-type]
        measurements["raw_bytes"] = writer.bytes_written
        began = time.perf_counter()
        started.set()
        try:
            return real_seal(writer, **kwargs)  # type: ignore[arg-type]
        finally:
            measurements["seal_seconds"] = time.perf_counter() - began
            completed.set()

    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT",
            stream="diff_depth", collector_instance_id="m1-1-test",
            collector_version="0.1.0+test", queue_capacity=8192,
            rotation=RotationPolicy(), durability_interval_seconds=1.0,
            max_frame_bytes=16 * 1024 * 1024,
        )
        # Incompressible, different payloads: avoid measuring 128 MiB of repeated x's.
        rng = random.Random(20261102)
        for ordinal in range(127):
            spool.enqueue(event(ordinal + 1, payload=rng.randbytes(1024 * 1024)))
            spool.drain_all()
        monkeypatch.setattr(stream_module, "_seal_clean_writer", observe_seal)
        class Socket:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            async def recv(self, decode: bool | None = None) -> bytes:
                return self.payload

            async def close(self, code: int = 1000, reason: str = "") -> None:
                pass

        def envelope(**kwargs: Any) -> EventEnvelope:
            return event(payload=kwargs["raw_payload"])

        collector = UsdMStreamCollector(
            stream="diff_depth", symbol="BTCUSDT", route="public",
            wire_name="btcusdt@depth", spool=spool, collector_instance_id="m1-1-test",
            collector_version="test", logger=logging.getLogger("m1-1"),
            receipt_queue_capacity=8192, envelope_factory=envelope,
        )

        async def receive_while_sealing() -> int:
            done = asyncio.Event()
            writer_task = asyncio.create_task(collector._writer_loop(done))
            received = 0

            async def handoff(socket: Socket) -> None:
                receipt = await collector._receive_once(socket, "connection-1")
                await collector._receipts.put(receipt, writer_task=writer_task)

            try:
                await handoff(Socket(rng.randbytes(1024 * 1024)))
                async with asyncio.timeout(10):
                    while not started.is_set():
                        await asyncio.sleep(0.001)
                    socket = Socket(b"incoming during seal")
                    # Nominal 4000 frames/s. Scheduler pacing is reported as
                    # observed frames/duration, without a real network claim.
                    while not completed.is_set():
                        await handoff(socket)
                        received += 1
                        await asyncio.sleep(1 / 4000)
            finally:
                done.set()
                await asyncio.wait_for(writer_task, 30)
            return received

        received = asyncio.run(receive_while_sealing())
        stats = collector.receipt_queue_stats
        assert received > 0
        assert measurements["raw_bytes"] >= 128 * 1024 * 1024
        assert 0 < stats.high_watermark < 8192
        assert stats.wait_count == stats.depth == spool.queue.depth == 0
        assert collector.capture_progress()["persisted_frames"] == 1 + received
        measurements["received_during_seal"] = received
        measurements["receipt_high_watermark"] = stats.high_watermark
        measurements["receipt_high_watermark_bytes"] = stats.high_watermark_bytes
        measurements["ingress_queue_full"] = 0
        assert sum(json.loads(p.read_text())["record_count"]
                   for p in layout.manifests.glob("*.manifest.json")) == 128 + received
        print(json.dumps(measurements))


@pytest.mark.parametrize("seconds", [600.0, 900.0])
def test_rotation_toml_override(tmp_path: Path, seconds: float) -> None:
    config = tmp_path / "recorder.toml"
    config.write_text(f"[recorder]\nrotation_seconds = {seconds}\n")
    loaded = load_config(config_file=config, environ={}, home=tmp_path / "home")
    assert loaded.config.rotation_seconds == seconds
    assert loaded.config.durability_interval_seconds == 1.0


def test_idle_fsync_remains_independent_of_rotation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 0.0
    clock = SimpleNamespace(monotonic=lambda: now, perf_counter_ns=time.perf_counter_ns,
                            time_ns=time.time_ns)
    monkeypatch.setattr(writer_module, "time", clock)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT",
            stream="diff_depth", collector_instance_id="test", collector_version="test",
            queue_capacity=8192, rotation=RotationPolicy(),
            durability_interval_seconds=1.0, max_frame_bytes=16 * 1024 * 1024,
        )
        spool.enqueue(event())
        spool.drain_all()
        assert spool.persistence_progress().synchronized_frames == 0
        now = 0.999
        spool.drain_all()
        assert spool.persistence_progress().synchronized_frames == 0
        now = 1.0
        spool.drain_all()
        assert spool.persistence_progress().synchronized_frames == 1
        assert not list(layout.sealed.glob("*.zst"))
        spool.close_and_seal()


def test_multi_rotation_reduction_60s_vs_900s(tmp_path: Path) -> None:
    events = [event(), event().model_copy(update={"symbol": "ETHUSDT"})]
    before = replay_rotation(tmp_path / "60", events, 60.0)
    after = replay_rotation(tmp_path / "900", events, 900.0)
    assert before["content_digest"] == after["content_digest"]
    assert before["frames"] == after["frames"] == 540
    for key in ("zst_files", "manifest_files", "seal_intent_commits",
                "seal_completion_commits", "archive_reservations"):
        assert after[key] < before[key] * 0.1
    assert after["timed_seals"] == 6
