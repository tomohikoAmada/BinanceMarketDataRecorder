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
import logging
import os
import subprocess
import sys
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.binance.public_websocket import (
    WebSocketConnection,
    stream_groups,
)
from binance_market_data_recorder.collector.availability import AvailabilityJournal
from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.spool.format import scan_chunk
from binance_market_data_recorder.spool.recovery import recover_storage
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.unit.test_m1_3_kline_and_seed_retirement import DummyResponse, sample_product

BASE = 1_800_000_000_000_000_000
SECOND = 1_000_000_000


def _wal_crash_worker(
    root: str, state: str, retain_partial: bool, artifact: str = "sealed"
) -> None:
    """Worker process that writes data and is killed to leave uncheckpointed WAL."""
    layout = ensure_storage_layout(Path(root))
    catalog = Catalog(layout.catalog)
    assert catalog._connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert catalog._connection.execute("PRAGMA synchronous").fetchone()[0] == 1
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

    if artifact == "sealed":
        raw = writer.path.read_bytes()
        seal_partial(writer.path, layout=layout, catalog=catalog)
        if retain_partial:
            writer.path.write_bytes(raw)
    elif artifact == "truncated_partial":
        with writer.path.open("ab") as stream:
            stream.write(b"\x00\x00")
            stream.flush()
            os.fsync(stream.fileno())

    catalog.persist_source_availability_watermark("selection-m32", BASE + 10 * SECOND)

    evidence = {
        "chunk_id": chunk_id,
        "retained_bytes": retained_bytes,
        "wal_bytes": wal.stat().st_size,
        "partial_path": layout.relative(writer.path),
    }
    print(json.dumps(evidence), flush=True)
    sys.stdin.read(1)


def _crash_and_rollback_wal(
    root: Path, state: str, retain_partial: bool = False, artifact: str = "sealed"
) -> dict[str, Any]:
    child_code = (
        "from tests.integration.test_m3_2_fault_recovery import _wal_crash_worker; "
        "import sys; _wal_crash_worker("
        "sys.argv[1], sys.argv[2], sys.argv[3] == 'True', sys.argv[4])"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, str(root), state, str(retain_partial), artifact],
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
        evidence: dict[str, Any] = json.loads(line)
    finally:
        child.kill()
        child.communicate(timeout=30)

    layout = ensure_storage_layout(root)
    wal = Path(str(layout.catalog) + "-wal")
    assert evidence["wal_bytes"] > evidence["retained_bytes"] > 32

    # Truncate WAL to the retained position to simulate lost tail commits
    with wal.open("r+b") as stream:
        stream.truncate(evidence["retained_bytes"])
    return evidence


@pytest.mark.parametrize("state", ["missing", "ACTIVE", "SEALING"])
@pytest.mark.parametrize("retain_partial", [False, True])
def test_sqlite_wal_tail_rollback_reconciles_sealed_and_partials(
    tmp_path: Path, state: str, retain_partial: bool
) -> None:
    """Simulate power loss with uncheckpointed WAL truncation and verify recovery."""
    evidence = _crash_and_rollback_wal(tmp_path, state, retain_partial)
    layout = ensure_storage_layout(tmp_path)

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
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        assert len(chunks) == 1
        assert [r.envelope for r in iter_source_records(chunks[0])] == [event(1)]

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


@pytest.mark.parametrize("artifact", ["partial", "truncated_partial"])
def test_wal_rollback_recovers_orphan_partial_without_claiming_complete(
    tmp_path: Path, artifact: str
) -> None:
    evidence = _crash_and_rollback_wal(tmp_path, "missing", artifact=artifact)
    layout = ensure_storage_layout(tmp_path)
    path = layout.root / evidence["partial_path"]
    original = path.read_bytes()
    with Catalog(layout.catalog) as catalog:
        chunk_id = evidence["chunk_id"]
        assert catalog.state(chunk_id) is None
        actions = recover_storage(layout=layout, catalog=catalog)
        assert not [a for a in actions if a.action == "reconcile_failed"]
        expected = ChunkState.ACTIVE if artifact == "partial" else ChunkState.RECOVERED
        assert catalog.state(chunk_id) is expected
        scan = scan_chunk(path)
        assert scan.is_clean and scan.statistics.record_count == 1
        assert not list(layout.sealed.iterdir())
        assert not list(layout.manifests.iterdir())
        # The surviving bytes remain recoverable, never sealed as a complete interval.
        raw = path.read_bytes()
        assert raw == (original if artifact == "partial" else original[:-2])
        recover_storage(layout=layout, catalog=catalog)
        assert catalog.state(chunk_id) is expected
        assert path.read_bytes() == raw


def test_kline_disconnect_across_close_boundary_backfills_without_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Use real receive/reconnect, scheduler, writer and sealed Raw reader paths."""
    # Put the synthetic bars in the past so REST still records the real receive clock.
    base_ms = (time.time_ns() // 1_000_000 // 60_000 - 3) * 60_000
    now_ns = (base_ms + 60_000) * 1_000_000
    monkeypatch.setattr(
        "binance_market_data_recorder.collector.coverage.time",
        SimpleNamespace(
            time_ns=lambda: now_ns,
            monotonic=time.monotonic,
            monotonic_ns=time.monotonic_ns,
        ),
    )

    def payload(open_offset: int) -> bytes:
        opened = base_ms + open_offset
        return json.dumps({
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline", "E": opened + 60_000, "s": "BTCUSDT",
                "k": {
                    "t": opened, "T": opened + 59_999, "s": "BTCUSDT", "i": "1m",
                    "x": True, "o": "50000", "c": "50100", "h": "50200",
                    "l": "49900", "v": "10",
                },
            },
        }).encode()

    requests: list[dict[str, Any]] = []
    request_times: list[int] = []

    class Client:
        closed = False

        def request(self, source: Any, params: dict[str, Any]) -> DummyResponse:
            assert source.endpoint == "/api/v3/klines"
            requests.append(params)
            request_times.append(time.time_ns())
            opened = base_ms + 60_000
            return DummyResponse([[
                opened, "50000", "50200", "49900", "50100", "10",
                opened + 59_999, "501000", 100, "5", "250500", "0",
            ]])

        def close(self) -> None:
            self.closed = True

    async def scenario() -> None:
        nonlocal now_ns
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            client = Client()
            capture = SelectedAssetCapture(
                RecorderConfig(
                    data_root=tmp_path,
                    coverage=CoverageConfig(underlying_assets=("BTC",), spot_intervals=("1m",)),
                ),
                logging.getLogger("m3.2.backfill"),
                version="m3.2", instance_id="m32-backfill", layout=layout, catalog=catalog,
                clients={"spot": client},
            )
            # Only this source is under test; directory discovery has its own offline tests.
            for directory in capture.sources.values():
                directory.dormant = True
            product = sample_product()
            source = next(
                s for s in product_sources(product, capture.coverage)
                if s.stream == "kline_1m" and s.transport == "ws"
            )
            capture.add_source(source)
            state = capture.sources[source.key]
            group, = stream_groups((source,), capture.coverage.streams_per_connection)
            capture.collectors[product.key] = ProductCapture(capture, product)
            stop = asyncio.Event()
            attempts = 0

            class Socket:
                delivered = False

                def __init__(self, attempt: int) -> None:
                    self.attempt = attempt

                async def recv(self, decode: bool | None = None) -> bytes:
                    nonlocal now_ns
                    assert decode is False
                    if self.attempt == 1:
                        if not self.delivered:
                            self.delivered = True
                            return payload(0)
                        while state.count < 1:
                            await asyncio.sleep(0.01)
                        now_ns = (base_ms + 110_000) * 1_000_000
                        raise OSError("disconnect before minute close")
                    if not self.delivered:
                        # The normal scheduler must finish backfill before the next live bar.
                        while not requests or state.backfill_start_ms is not None:
                            await asyncio.sleep(0.01)
                        self.delivered = True
                        now_ns = (base_ms + 180_000) * 1_000_000
                        return payload(120_000)
                    while state.count < 2:
                        await asyncio.sleep(0.01)
                    stop.set()
                    await asyncio.Event().wait()
                    raise AssertionError("unreachable")

                async def close(self, code: int = 1000, reason: str = "") -> None:
                    pass

            @asynccontextmanager
            async def opener(_url: str, **_options: Any) -> AsyncIterator[WebSocketConnection]:
                nonlocal attempts, now_ns
                attempts += 1
                assert attempts <= 2
                if attempts == 2:
                    now_ns = (base_ms + 125_000) * 1_000_000
                yield Socket(attempts)

            monkeypatch.setattr("binance_market_data_recorder.collector.coverage.connect", opener)
            owner = asyncio.create_task(capture.run(stop))
            try:
                await asyncio.wait_for(asyncio.shield(owner), timeout=10)
            finally:
                stop.set()
                await asyncio.wait_for(owner, timeout=10)

            finished_at = time.time_ns()
            assert attempts == 2 and client.closed
            assert len(requests) == 1
            assert requests[0]["startTime"] == base_ms + 60_000
            assert requests[0]["endTime"] == base_ms + 119_999
            assert state.backfill_start_ms is None
            assert state.last_closed_open_time_ms == base_ms + 120_000
            assert not list(layout.active.glob("*.partial"))

            # Inspect the actual immutable output after normal shutdown, including flags/clocks.
            restored = [
                r.envelope
                for chunk in load_source_chunks(layout=layout, catalog=catalog)
                for r in iter_source_records(chunk)
            ]
            assert len(restored) == 3
            ws = sorted(
                (e for e in restored if e.stream == group.name),
                key=lambda e: e.source_sequence["openTime"],
            )
            assert [e.raw_payload for e in ws] == [payload(0), payload(120_000)]
            assert len({e.connection_id for e in ws}) == 2
            backfill, = [e for e in restored if e.stream == "rest_observations"]
            assert "closed_bar_backfill" in backfill.capture_flags
            assert "malformed" not in backfill.capture_flags
            assert request_times[0] <= backfill.receive_time_utc_ns <= finished_at
            assert backfill.source_sequence["sourceStream"] == "kline_1m"
            document = json.loads(backfill.raw_payload)
            assert document["request"]["parameters"] == requests[0]
            open_times = sorted([
                *(json.loads(e.raw_payload)["data"]["k"]["t"] for e in ws),
                document["response"]["model"][0][0],
            ])
            assert open_times == [base_ms, base_ms + 60_000, base_ms + 120_000]

    asyncio.run(scenario())
