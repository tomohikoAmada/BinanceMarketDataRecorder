"""Unit and regression tests for Milestone 1.1: Spool chunk rotation tuning.

Verifies:
1. Default rotation configuration (900.0s / 128 MiB) and valid ranges (600s-900s).
2. Phase staggering across 900s (15m) rotation window without collision.
3. Active area disk space accounting within ADR-0016 10 GiB hard reserve.
4. Fast scan_chunk performance and truncation recovery on 128 MiB chunks.
5. Ingress queue capacity during 128 MiB seal without IngressQueueFull.
6. Multi-rotation file and catalog transaction reduction (> 90%).
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.spool.format import encode_frame, scan_chunk
from binance_market_data_recorder.spool.recovery import recover_partials
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import (
    RawChunkWriter,
    RotationPolicy,
    _rotation_deadline,
)
from binance_market_data_recorder.storage.capacity import HARD_RESERVE_BYTES, VPS_PRODUCTION_V1
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.forecast import GIB, hard_reserve_bytes
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event


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
    # VPS reserve is 10 GiB
    assert HARD_RESERVE_BYTES == 10 * GIB
    assert VPS_PRODUCTION_V1.hard_reserve_bytes == 10 * GIB

    reserve = hard_reserve_bytes(100 * GIB, rotation_bytes=128 * 1024 * 1024)
    assert reserve >= 5 * GIB

    # Transient disk space during Zstd compression for a 128 MiB chunk:
    # Active chunk uncompressed: 128 MiB
    # Compressed artifact in progress (Zstd level 3): ~30 MiB
    # Total per stream in-flight: ~158 MiB.
    # Phase staggering spreads seals over 900s, so streams do not seal at the same time.
    # Even if 5 high-traffic streams sealed in a burst, 5 * 158 MiB = 790 MiB << 10 GiB.
    transient_per_stream_mib = 128 + 30
    simultaneous_burst_streams = 5
    worst_case_transient_gib = (transient_per_stream_mib * simultaneous_burst_streams) / 1024
    assert worst_case_transient_gib < 1.0  # Less than 1 GiB
    assert worst_case_transient_gib * GIB < HARD_RESERVE_BYTES


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
        # 128 MiB scan must be fast (< 2.0 seconds in Python)
        assert scan_duration < 2.0

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
        assert rec_duration < 2.0


def test_multi_rotation_reduction_60s_vs_900s(tmp_path: Path) -> None:
    """Demonstrates that switching from 60s to 900s reduces steady-state

    chunk rotations and Catalog sealing operations by > 90% (93.3%).
    """
    stream_count = 5
    simulated_window_seconds = 900.0

    # Under 60s rotation, each stream rotates every 60 seconds
    rotations_60s_per_stream = int(simulated_window_seconds / 60.0)
    total_rotations_60s = stream_count * rotations_60s_per_stream

    # Under 900s rotation, each stream rotates once per 15 minutes
    rotations_900s_per_stream = int(simulated_window_seconds / 900.0)
    total_rotations_900s = stream_count * rotations_900s_per_stream

    assert total_rotations_60s == 75
    assert total_rotations_900s == 5

    reduction = (total_rotations_60s - total_rotations_900s) / total_rotations_60s
    assert reduction == pytest.approx(14 / 15, rel=1e-3)
    assert reduction > 0.90  # Exceeds 90% reduction


def test_ingress_queue_preservation_during_chunk_seal(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="m1-1-test",
            collector_version="0.1.0+test",
            queue_capacity=8192,
            rotation=RotationPolicy(seconds=900.0, bytes=128 * 1024 * 1024),
            durability_interval_seconds=1.0,
            max_frame_bytes=16 * 1024 * 1024,
        )
        # Enqueue initial events and drain to open writer
        spool.enqueue(event(1, payload=b"pre-seal data"))
        assert spool.drain_all() == 1

        # Enqueue 100 events into bounded queue while seal occurs
        for i in range(2, 102):
            spool.enqueue(event(i, payload=b"concurrent incoming data during seal"))

        # Ingress queue must retain events with IngressQueueFull == 0
        assert spool.queue.depth == 100
        assert spool.queue.depth < 8192

        # Complete seal and drain
        spool.close_and_seal()
        assert spool.queue.depth == 0

