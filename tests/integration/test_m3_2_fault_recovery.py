"""Fault injection and recovery validation for Milestone M3.2:
1. SQLite WAL tail rollback reconciliation:
   Simulates PRAGMA synchronous=NORMAL power loss where the last several seconds of
   Catalog commits are lost in WAL tail, while .zst + .json and active/partial files exist.
   Verifies recover_storage() rolls forward sealed chunks, cleans up partials, and maintains
   availability continuity without data loss.
2. Kline disconnect across close boundary automated REST backfill:
   Simulates WebSocket disconnect right before close boundary, reconnection after close,
   verifying automated REST backfill with 'closed_bar_backfill' flag and seamless
   closed-bar sequence.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.binance.public_websocket import StreamGroup
from binance_market_data_recorder.collector.availability import AvailabilityJournal
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.spool.recovery import recover_storage
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.unit.test_m1_3_kline_and_seed_retirement import DummyResponse, sample_product

BASE = 1_800_000_000_000_000_000
SECOND = 1_000_000_000


def _wal_crash_worker(root: str, state: str, retain_partial: bool) -> None:
    """Worker process that writes data and is killed to leave uncheckpointed WAL."""
    layout = ensure_storage_layout(Path(root))
    catalog = Catalog(layout.catalog)
    catalog._connection.execute("PRAGMA wal_autocheckpoint=0")
    catalog._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    journal = AvailabilityJournal("crash-instance", selection_id="selection-m32")
    journal.valid("spot/BTCUSDT/diff_depth/ws", BASE)
    while journal.pending:
        batch = journal.batch()
        catalog.persist_source_availability(batch)
        journal.acknowledge(len(batch))
    catalog.persist_source_availability_watermark("selection-m32", BASE)

    wal = Path(str(layout.catalog) + "-wal")
    retained_bytes = wal.stat().st_size

    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        collector_instance_id="crash-instance",
        collector_version="m3.2-test",
        created_at_utc_ns=BASE,
    )
    writer.append(event(1))
    writer.close()
    chunk_id = str(writer.header.chunk_id)

    if state == "SEALING":
        catalog.transition(chunk_id, ChunkState.SEALING, idempotency_key="pre-seal", evidence={})

    if state != "missing":
        retained_bytes = wal.stat().st_size

    raw = writer.path.read_bytes()
    seal_partial(writer.path, layout=layout, catalog=catalog)
    if retain_partial:
        writer.path.write_bytes(raw)

    catalog.persist_source_availability_watermark("selection-m32", BASE + 10 * SECOND)

    evidence = {
        "chunk_id": chunk_id,
        "retained_bytes": retained_bytes,
        "wal_bytes": wal.stat().st_size,
    }
    print(json.dumps(evidence), flush=True)
    sys.stdin.read(1)


@pytest.mark.parametrize("state", ["missing", "ACTIVE", "SEALING"])
@pytest.mark.parametrize("retain_partial", [False, True])
def test_sqlite_wal_tail_rollback_reconciles_sealed_and_partials(
    tmp_path: Path, state: str, retain_partial: bool
) -> None:
    """Simulate power loss with uncheckpointed WAL truncation and verify recovery."""
    child_code = (
        "from tests.integration.test_m3_2_fault_recovery import _wal_crash_worker; "
        "import sys; _wal_crash_worker(sys.argv[1], sys.argv[2], sys.argv[3] == 'True')"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, str(tmp_path), state, str(retain_partial)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        line = child.stdout.readline()
        if not line:
            _, errors = child.communicate(timeout=30)
            pytest.fail(f"Child worker failed: {errors}")
        evidence = json.loads(line)
    finally:
        child.kill()
        child.communicate(timeout=30)

    layout = ensure_storage_layout(tmp_path)
    wal = Path(str(layout.catalog) + "-wal")
    assert evidence["wal_bytes"] > evidence["retained_bytes"] > 32

    # Truncate WAL to the retained position to simulate lost tail commits
    with wal.open("r+b") as stream:
        stream.truncate(evidence["retained_bytes"])

    with Catalog(layout.catalog) as catalog:
        chunk_id = evidence["chunk_id"]
        assert catalog.state(chunk_id) == (None if state == "missing" else ChunkState(state))
        assert catalog.source_availability_watermark("selection-m32") == BASE

        actions = recover_storage(layout=layout, catalog=catalog)
        assert not [a for a in actions if a.action == "reconcile_failed"]
        assert catalog.state(chunk_id) is ChunkState.SEALED
        assert not list(layout.active.glob("*.partial"))

        row = catalog.chunk(chunk_id)
        assert row is not None and row["record_count"] == 1

        restarted = AvailabilityJournal("post-recovery")
        restarted.previous = catalog.source_availability()
        key = "spot/BTCUSDT/diff_depth/ws"
        assert restarted.ensure(key, BASE + 12 * SECOND).unavailable_since == BASE
        restarted.valid(key, BASE + 12 * SECOND)
        while restarted.pending:
            batch = restarted.batch()
            catalog.persist_source_availability(batch)
            restarted.acknowledge(len(batch))

        report = continuity_report(catalog, start=BASE, end=BASE + 12 * SECOND)
        assert report["sources"][0]["unavailable_or_unknown_ns"] == 12 * SECOND

        # Re-running recovery is idempotent
        subsequent_actions = recover_storage(layout=layout, catalog=catalog)
        assert not [a for a in subsequent_actions if a.action == "reconcile_failed"]


def test_kline_disconnect_across_close_boundary_backfills_without_gap(tmp_path: Path) -> None:
    """Simulate WS disconnect crossing close boundary and verify seamless backfill sequence."""

    def make_kline_payload(open_ms: int, close_ms: int, is_closed: bool) -> bytes:
        return json.dumps(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "E": close_ms + 1,
                    "s": "BTCUSDT",
                    "k": {
                        "t": open_ms,
                        "T": close_ms,
                        "s": "BTCUSDT",
                        "i": "1m",
                        "x": is_closed,
                        "o": "50000",
                        "c": "50100",
                        "h": "50200",
                        "l": "49900",
                        "v": "10",
                    },
                },
            }
        ).encode()

    def kline_bar(open_ms: int) -> list[Any]:
        return [
            open_ms,
            "50000",
            "50100",
            "49900",
            "50100",
            "10",
            open_ms + 59_999,
            "501000",
            100,
            "5",
            "250500",
            "0",
        ]

    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        cfg = RecorderConfig(
            data_root=tmp_path,
            coverage=CoverageConfig(underlying_assets=("BTC",), spot_intervals=("1m",)),
        )
        capture = SelectedAssetCapture(
            cfg,
            MagicMock(),
            version="m3.2",
            instance_id="m32-backfill",
            layout=layout,
            catalog=catalog,
        )
        source = next(
            s
            for s in product_sources(sample_product(), capture.coverage)
            if s.stream == "kline_1m" and s.transport == "ws"
        )
        group = StreamGroup("wss://stream.binance.com:9443", "", "ws_btcusdt", (source,))
        capture.add_source(source)
        state = capture.sources[source.key]
        state.connected = True

        mock_client = MagicMock()
        capture.clients["spot"] = mock_client

        received_envelopes: list[Any] = []
        collector = capture.make_group(
            group,
            sample_product(),
            lambda e: received_envelopes.append(e),
            lambda event: None,
        )
        assert collector.envelope_factory is not None

        # 1. Bar 0 arrives live via WS: [0 .. 59_999], x=True
        env_0 = collector.envelope_factory(
            raw_payload=make_kline_payload(0, 59_999, True),
            connection_id="ws-conn-1",
            collector_instance_id="m32-backfill",
            collector_version="m3.2",
            receive_time_utc_ns=60_000_000_000,
            receive_monotonic_ns=60_000_000,
        )
        assert env_0 is not None
        capture._observe_shared(group, env_0)
        assert state.last_closed_open_time_ms == 0

        # 2. WS disconnects at 110s (before minute 1 closes at 119_999)
        with patch("time.time_ns", return_value=110_000_000_000):
            capture._shared_lifecycle(group, "disconnected")
        assert not state.connected

        # 3. WS reconnects at 125s (after minute 1 has closed)
        mock_client.request.return_value = DummyResponse([kline_bar(60_000)])
        with patch("time.time_ns", return_value=125_000_000_000):
            capture._shared_lifecycle(group, "connected")

        # Reconnect triggers backfill detection
        assert state.backfill_start_ms == 60_000
        assert state.backfill_end_ms == 119_999

        # Run automated REST backfill
        backfill_result = await capture._run_kline_backfill(state)
        assert backfill_result.valid
        assert "closed_bar_backfill" in backfill_result.envelope.capture_flags
        assert backfill_result.envelope.receive_time_utc_ns >= 125_000_000_000

        backfill_payload = json.loads(backfill_result.envelope.raw_payload)
        assert backfill_payload["request"]["parameters"]["startTime"] == 60_000
        assert backfill_payload["request"]["parameters"]["endTime"] == 119_999
        assert state.last_closed_open_time_ms == 60_000

        # 4. Bar 2 arrives live via WS: [120_000 .. 179_999], x=True
        env_2 = collector.envelope_factory(
            raw_payload=make_kline_payload(120_000, 179_999, True),
            connection_id="ws-conn-2",
            collector_instance_id="m32-backfill",
            collector_version="m3.2",
            receive_time_utc_ns=180_000_000_000,
            receive_monotonic_ns=180_000_000,
        )
        assert env_2 is not None
        capture._observe_shared(group, env_2)
        assert state.last_closed_open_time_ms == 120_000

        # Verify the seamless sequence: Bar 0 (0), Bar 1 (60_000, backfilled), Bar 2 (120_000)
        open_times = [
            json.loads(env_0.raw_payload)["data"]["k"]["t"],
            backfill_payload["response"]["model"][0][0],
            json.loads(env_2.raw_payload)["data"]["k"]["t"],
        ]
        assert open_times == [0, 60_000, 120_000]
        # Confirm zero gap: each open_time is previous open_time + 60_000
        for i in range(len(open_times) - 1):
            assert open_times[i + 1] == open_times[i] + 60_000

    asyncio.run(scenario())
