"""Unit and integration validation for Milestone M1.2:
- Catalog PRAGMA journal_mode=WAL, synchronous=NORMAL.
- Irreversible deletion FULL sync barriers (LOCAL_DELETED / remote authorization).
- AvailabilityJournal in-queue coalescing of healthy checkpoints.
- Shared watermark checkpointing (source_availability_watermark) reducing ~4660 writes to 1 row.
- WAL tail rollback recovery: reconcile .zst/.json to SEALED, availability honest unknown gap.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.archive.remote_authorization import RemoteAuthorizer
from binance_market_data_recorder.archive.remote_delete import RemoteDeleter
from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_sources import Source
from binance_market_data_recorder.collector.availability import AvailabilityJournal
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.spool.recovery import reconcile_sealed, recover_storage
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import (
    ArchiveState,
    Catalog,
    ChunkState,
)
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.integration.test_p2_capture import Client
from tests.remote_authorization_support import build_receipt, prepare_remote_authorization
from tests.unit.test_p3_availability import owner_at

BASE = 1_800_000_000_000_000_000
SECOND = 1_000_000_000


def flush(journal: AvailabilityJournal, catalog: Catalog) -> None:
    while journal.pending:
        batch = journal.batch()
        catalog.persist_source_availability(batch)
        journal.acknowledge(len(batch))


class ConnectionTracker:
    def __init__(self, real: sqlite3.Connection) -> None:
        self._real = real
        self.executed_pragmas: list[str] = []

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        upper = sql.strip().upper()
        if upper.startswith("PRAGMA"):
            self.executed_pragmas.append(upper)
        return self._real.execute(sql, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def test_catalog_pragma_default_wal_normal_and_barrier_full(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        # 1. Verify default connection is WAL and synchronous=NORMAL (1)
        mode = catalog._connection.execute("PRAGMA journal_mode").fetchone()[0]
        sync = catalog._connection.execute("PRAGMA synchronous").fetchone()[0]
        assert str(mode).lower() == "wal"
        assert sync == 1  # 1 == NORMAL, 2 == FULL

        # 2. Verify routine transactions keep NORMAL
        catalog.register_active(
            chunk_id="test-chunk-1",
            partial_path="active/test.partial",
            created_at_utc_ns=BASE,
        )
        sync_after_active = catalog._connection.execute("PRAGMA synchronous").fetchone()[0]
        assert sync_after_active == 1

        # 3. Verify force_sync barrier executes synchronous=FULL and wal_checkpoint(PASSIVE)
        # Register a chunk and archive transaction to test transition_archive(LOCAL_DELETED)
        catalog.transition(
            "test-chunk-1",
            ChunkState.SEALING,
            idempotency_key="sealing:1",
            evidence={},
        )
        catalog.transition(
            "test-chunk-1",
            ChunkState.SEALED,
            idempotency_key="sealed:1",
            evidence={},
            fields={
                "sealed_path": "sealed/test.zst",
                "manifest_path": "manifests/test.manifest.json",
                "record_count": 10,
                "stored_bytes": 100,
                "stored_sha256": "abc",
                "uncompressed_bytes": 200,
                "uncompressed_sha256": "def",
            },
        )
        # Setup archive storage target and transaction
        catalog.register_storage_target(
            storage_id="target-1",
            volume_uuid="vol-1",
            volume_name="ArchiveVol",
            filesystem_type="apfs",
            relative_path="archives/test",
            marker_nonce="nonce-1",
            registered_at_utc_ns=BASE,
        )
        tx = catalog.reserve_archive_transaction(
            transaction_id="tx-1",
            chunk_id="test-chunk-1",
            storage_id="target-1",
            market="spot",
            stream="test",
            source_relative_path="sealed/test.zst",
            source_manifest_relative_path="manifests/test.manifest.json",
            source_manifest_sha256="abc",
            target_relative_path="archives/test.zst",
            target_temp_relative_path="archives/test.zst.tmp",
            external_manifest_relative_path="archives/test.manifest.json",
            stored_bytes=100,
            stored_sha256="def",
        )
        catalog.transition_archive(
            str(tx["transaction_id"]),
            ArchiveState.VERIFYING,
            idempotency_key="verifying:1",
        )
        catalog.transition_archive(
            str(tx["transaction_id"]),
            ArchiveState.VERIFIED,
            idempotency_key="verified:1",
            verified_at_utc_ns=BASE + 10,
        )

        real_conn = catalog._connection
        tracker = ConnectionTracker(real_conn)
        catalog._connection = tracker  # type: ignore[assignment]

        # Transition to LOCAL_DELETE_PENDING (irreversible authorization)
        catalog.transition_archive(
            str(tx["transaction_id"]),
            ArchiveState.LOCAL_DELETE_PENDING,
            idempotency_key="pending:1",
        )
        assert any("PRAGMA SYNCHRONOUS = FULL" in p for p in tracker.executed_pragmas)
        assert any("PRAGMA WAL_CHECKPOINT(PASSIVE)" in p for p in tracker.executed_pragmas)
        assert any("PRAGMA SYNCHRONOUS = NORMAL" in p for p in tracker.executed_pragmas)

        tracker.executed_pragmas.clear()
        # Transition to LOCAL_DELETED
        catalog.transition_archive(
            str(tx["transaction_id"]),
            ArchiveState.LOCAL_DELETED,
            idempotency_key="deleted:1",
            local_deleted_at_utc_ns=BASE + 20,
        )
        assert any("PRAGMA SYNCHRONOUS = FULL" in p for p in tracker.executed_pragmas)
        assert any("PRAGMA WAL_CHECKPOINT(PASSIVE)" in p for p in tracker.executed_pragmas)
        assert any("PRAGMA SYNCHRONOUS = NORMAL" in p for p in tracker.executed_pragmas)

        # Connection synchronous setting returns to NORMAL
        sync_final = real_conn.execute("PRAGMA synchronous").fetchone()[0]
        assert sync_final == 1
        catalog._connection = real_conn


def test_availability_coalescing_and_transition_invariance(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        journal = AvailabilityJournal("test-instance")

        # 1. First valid call triggers ensure -> STARTUP_UNKNOWN gap + recovery gap closed (2 items)
        journal.valid("src-1", BASE)
        assert len(journal.pending) == 2
        # Drain the initial startup entries
        flush(journal, catalog)
        assert len(journal.pending) == 0

        # 2. Second valid call after 11s -> emits routine checkpoint
        journal.valid("src-1", BASE + 11 * SECOND)
        assert len(journal.pending) == 1
        ckpt_entry = journal.pending[0]
        assert ckpt_entry["gap"] is None
        assert ckpt_entry["state"]["observed_at_utc_ns"] == BASE + 11 * SECOND

        # 3. Third and fourth valid calls after 22s and 33s before flush ->
        # COALESCED into the 1 pending entry
        journal.valid("src-1", BASE + 22 * SECOND)
        assert len(journal.pending) == 1
        assert journal.pending[0]["state"]["observed_at_utc_ns"] == BASE + 22 * SECOND

        journal.valid("src-1", BASE + 33 * SECOND)
        assert len(journal.pending) == 1
        assert journal.pending[0]["state"]["observed_at_utc_ns"] == BASE + 33 * SECOND

        # 4. Flush the single coalesced entry
        flush(journal, catalog)
        assert len(journal.pending) == 0

        # 5. Transition invariance: transitions are never coalesced or lost
        # Failure at +35s
        journal.failure("src-1", BASE + 35 * SECOND, "TimeoutError")
        assert len(journal.pending) == 2  # unavailable gap + failure count update
        # Failure at +36s
        journal.failure("src-1", BASE + 36 * SECOND, "TimeoutError")
        assert len(journal.pending) == 3  # failure count update appended
        # Recovery at +40s
        journal.valid("src-1", BASE + 40 * SECOND)
        assert len(journal.pending) == 4  # gap closed appended

        # Flush transitions to catalog
        flush(journal, catalog)
        assert len(journal.pending) == 0

        # Check continuity report reflects exact failure and recovery
        report = continuity_report(catalog, start=BASE, end=BASE + 40 * SECOND)
        src = report["sources"][0]
        assert src["latest_state"]["failure_count"] == 2
        assert src["latest_state"]["recovery_count"] == 2
        assert src["latest_state"]["unavailable_since_utc_ns"] is None
        # Unavailable interval covers 35s to 40s
        assert (BASE + 35 * SECOND, BASE + 40 * SECOND) in src["unavailable_intervals"]


def test_shared_watermark_steady_state_reduction_and_restart(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    config = RecorderConfig(
        data_root=layout.root,
        coverage=CoverageConfig(
            underlying_assets=("BTC", "ETH"),
            capture_dated_premium_klines=False,
            capture_dated_adl_risk=False,
        ),
    )
    logger = logging.getLogger("test")

    with Catalog(layout.catalog) as catalog:
        capture = SelectedAssetCapture(
            config=config,
            logger=logger,
            version="1.0.0",
            instance_id="inst-1",
            layout=layout,
            catalog=catalog,
            clients={"spot": Client(), "usdm": Client(), "coinm": Client()},
        )
        # Exercise the planned BTC + ETH source count without network traffic.
        for i in range(4660):
            source = Source(
                family="S01",
                venue_market="spot",
                market="spot",
                symbol="BTCUSDT",
                stream=f"test_stream_{i}",
                transport="ws",
                endpoint="/ws/test",
                cadence_seconds=1.0,
            )
            capture.add_source(source)
            capture.sources[source.key].connected = True

        # First observation + flush: closes STARTUP_UNKNOWN for all sources
        capture._observe_availability(BASE)
        assert len(capture.availability.pending) > 0
        asyncio.run(capture._flush_availability(all_pending=True))
        assert len(capture.availability.pending) == 0
        assert len(catalog.source_availability()) == 4663

        # Track writes during steady-state health over 10 seconds
        persist_calls: list[int] = []
        original_persist = catalog.persist_source_availability

        def tracking_persist(batch: list[dict[str, Any]], *args: Any, **kwargs: Any) -> Any:
            persist_calls.append(len(batch))
            return original_persist(batch, *args, **kwargs)

        catalog.persist_source_availability = tracking_persist  # type: ignore[method-assign]

        statements: list[str] = []
        catalog._connection.set_trace_callback(statements.append)
        # Simulate 10 one-second observation loops with steady health
        for sec in range(1, 11):
            now = BASE + sec * SECOND
            capture._observe_availability(now)
            asyncio.run(capture._flush_availability())
            # Pending queue is drained after each cycle (steady backlog is 0)
            assert len(capture.availability.pending) == 0

        # At the 10th second, watermark flush triggered:
        # exactly 1 transaction writing 0 batch items + 1 watermark row
        assert len(persist_calls) == 1
        assert persist_calls[0] == 0  # 0 individual source rows!
        writes = [sql for sql in statements if sql.startswith(("INSERT", "UPDATE", "DELETE"))]
        assert len(writes) == 1
        assert writes[0].startswith("INSERT INTO source_availability_watermark(")
        catalog._connection.set_trace_callback(None)
        watermark = catalog.source_availability_watermark(capture.source_selection_id)
        assert watermark == BASE + 10 * SECOND

        # Verify restart semantics:
        # A new process starts on the same catalog:
        restarted_states = catalog.source_availability(capture.source_selection_id)
        # Every healthy source has observed_at advanced to the watermark!
        healthy_count = 0
        for state in restarted_states.values():
            if state["unavailable_since_utc_ns"] is None:
                assert state["observed_at_utc_ns"] == BASE + 10 * SECOND
                healthy_count += 1
        assert healthy_count == 4660

        # Restarted journal ensures sources at T + 12s (2s restart window)
        restarted_journal = AvailabilityJournal("inst-2")
        restarted_journal.previous = restarted_states
        s_state = restarted_journal.ensure("spot/BTCUSDT/test_stream_0/ws", BASE + 12 * SECOND)
        # The honest unknown interval begins at the watermark (BASE + 10s), NOT at BASE!
        assert s_state.unavailable_since == BASE + 10 * SECOND

        # Recover at +12s
        restarted_journal.valid("spot/BTCUSDT/test_stream_0/ws", BASE + 12 * SECOND)
        flush(restarted_journal, catalog)

        # Continuity report: gap is strictly (BASE + 10s, BASE + 12s), no earlier time lost
        rep = continuity_report(
            catalog,
            start=BASE,
            end=BASE + 12 * SECOND,
            source_ids=["spot/BTCUSDT/test_stream_0/ws"],
        )
        src_rep = rep["sources"][0]
        assert src_rep["unavailable_intervals"] == [(BASE + 10 * SECOND, BASE + 12 * SECOND)]
        assert src_rep["unavailable_or_unknown_ns"] == 2 * SECOND


def test_catalog_state_reversion_reconcile_and_unknown_tail(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        # Record initial baseline availability for the source at BASE
        journal = AvailabilityJournal("inst-init")
        journal.valid("spot/BTCUSDT/trade/ws", BASE)
        flush(journal, catalog)

        # 1. Setup a valid chunk and seal it on disk
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="trade",
            collector_instance_id="inst-1",
            collector_version="1.0.0",
            created_at_utc_ns=BASE,
        )
        chunk_id = str(writer.header.chunk_id)
        writer_path = writer.path
        writer.close()
        # Seal the partial cleanly
        manifest = seal_partial(writer_path, layout=layout, catalog=catalog)
        sealed_path = layout.root / str(manifest["relative_path"])
        manifest_path = layout.manifests / f"{writer.header.chunk_id.hex}.manifest.json"
        assert sealed_path.exists()
        assert manifest_path.exists()
        assert catalog.state(chunk_id) is ChunkState.SEALED

        # Record a watermark at T1 = BASE + 5s
        catalog.persist_source_availability_watermark("assets:BTC", BASE + 5 * SECOND)

        # 2. Logical stale-state model; actual WAL truncation is tested separately below.
        # Revert the chunk state in Catalog back to ACTIVE and watermark to T0 = BASE
        with catalog._transaction() as connection:
            connection.execute(
                "UPDATE chunks SET state = ?, partial_path = ? WHERE chunk_id = ?",
                (ChunkState.ACTIVE, "active/dummy.partial", chunk_id),
            )
            connection.execute(
                "UPDATE source_availability_watermark "
                "SET evaluated_through_utc_ns = ? WHERE selection_id = ?",
                (BASE, "assets:BTC"),
            )

        assert catalog.state(chunk_id) is ChunkState.ACTIVE
        assert catalog.source_availability_watermark("assets:BTC") == BASE

        # 3. Run recovery: recover_storage must roll-forward the chunk to SEALED
        actions = recover_storage(layout=layout, catalog=catalog)
        assert any(a.action == "catalog_reconciled" and a.detail == "SEALED" for a in actions)
        assert catalog.state(chunk_id) is ChunkState.SEALED

        # 4. Verify availability restart with rolled-back WAL tail:
        # Catalog has watermark at BASE (lost tail between BASE and BASE + 5s)
        avail = catalog.source_availability("assets:BTC")
        # Any source ensured after restart at BASE + 10s will have unavailable_since = BASE
        # honestly attributing the lost tail as unknown, with zero silent omission!
        restarted = AvailabilityJournal("inst-rollback")
        restarted.previous = avail
        s = restarted.ensure("spot/BTCUSDT/trade/ws", BASE + 10 * SECOND)
        assert s.unavailable_since == BASE


def test_full_barrier_restores_normal_when_begin_fails(tmp_path: Path) -> None:
    class BusyConnection(ConnectionTracker):
        def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
            if sql == "BEGIN IMMEDIATE":
                raise sqlite3.OperationalError("injected database is locked")
            return super().execute(sql, *args, **kwargs)

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        connection = catalog._connection
        catalog._connection = BusyConnection(connection)  # type: ignore[assignment]
        try:
            with (
                pytest.raises(sqlite3.OperationalError, match="injected"),
                catalog._transaction(force_sync=True),
            ):
                pytest.fail("BEGIN should have failed")
            assert connection.execute("PRAGMA synchronous").fetchone()[0] == 1
            assert not connection.in_transaction
        finally:
            catalog._connection = connection


def test_remote_authorization_and_deletion_full_barrier_order(tmp_path: Path) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    layout = fixture.prepared.layout
    receipt = build_receipt(fixture)
    with Catalog(layout.catalog) as catalog:
        statements: list[str] = []
        catalog._connection.set_trace_callback(statements.append)
        RemoteAuthorizer(layout=layout, catalog=catalog).authorize(
            receipt.canonical_bytes(), fixture.selections[0],
        )
        # The authorization is durable before any physical source deletion.
        assert fixture.selections[0].sealed_path.exists()
        for phase in ("authorize", "delete"):
            if phase == "delete":
                statements.clear()
                RemoteDeleter(layout=layout, catalog=catalog).delete_authorized(receipt.receipt_id)
            full = statements.index("PRAGMA synchronous = FULL")
            begin = statements.index("BEGIN IMMEDIATE", full)
            commit = statements.index("COMMIT", begin)
            checkpoint = statements.index("PRAGMA wal_checkpoint(PASSIVE)", commit)
            normal = statements.index("PRAGMA synchronous = NORMAL", checkpoint)
            assert full < begin < commit < checkpoint < normal
            assert catalog._connection.execute("PRAGMA synchronous").fetchone()[0] == 1
        assert not fixture.selections[0].sealed_path.exists()


@pytest.mark.parametrize("bound", ["batches", "deadline"])
def test_watermark_waits_for_all_transitions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bound: str,
) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            owner.sources.clear()
            for i in range(200):
                source = Source("S01", "spot", "spot", "BTCUSDT", f"stream-{i}", "ws", "/ws", 0)
                owner.add_source(source)
                owner.sources[source.key].connected = True
            owner._observe_availability(BASE)
            await owner._flush_availability(all_pending=True)
            owner.availability.failure("spot/BTCUSDT/stream-0/ws", BASE + SECOND, "TimeoutError")
            for sec in range(1, 11):
                owner._observe_availability(BASE + sec * SECOND)
            # Leave an outage beyond the first batch of pending checkpoints.
            key = "/".join(source.key)
            owner.availability.failure(key, BASE + 10 * SECOND, "TimeoutError")
            if bound == "batches":
                monkeypatch.setattr(
                    "binance_market_data_recorder.collector.coverage."
                    "AVAILABILITY_FLUSH_MAX_BATCHES", 1,
                )
            else:
                monkeypatch.setattr(
                    "binance_market_data_recorder.collector.coverage."
                    "AVAILABILITY_FLUSH_SECONDS", 0,
                )
            await owner._flush_availability()
            assert owner.availability.pending
            assert catalog.source_availability_watermark(owner.source_selection_id) == BASE
            assert owner._last_watermark_flush_utc_ns == BASE
            await owner._flush_availability(all_pending=True)
            assert not owner.availability.pending
            assert catalog.source_availability_watermark(owner.source_selection_id) == (
                BASE + 10 * SECOND
            )
            assert catalog.source_availability()[key]["unavailable_since_utc_ns"] == (
                BASE + 10 * SECOND
            )

    asyncio.run(scenario())


def test_failed_watermark_commit_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = owner_at(tmp_path, catalog)
            owner._observe_availability(BASE)
            persist = catalog.persist_source_availability

            def fail(batch: list[dict[str, Any]], **kwargs: Any) -> None:
                if kwargs.get("watermark") is not None:
                    raise sqlite3.OperationalError("injected write failure")
                persist(batch, **kwargs)

            monkeypatch.setattr(catalog, "persist_source_availability", fail)
            with pytest.raises(sqlite3.OperationalError, match="injected"):
                await owner._flush_availability(all_pending=True)
            assert owner._last_watermark_flush_utc_ns is None
            assert owner.availability.pending
            monkeypatch.setattr(catalog, "persist_source_availability", persist)
            await owner._flush_availability(all_pending=True)
            assert catalog.source_availability_watermark(owner.source_selection_id) == BASE

    asyncio.run(scenario())


def test_watermarks_only_cover_the_sources_observed_by_their_owner(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        selections = (("old", "selection-old"), ("new", "selection-new"), ("legacy", None))
        for key, selection in selections:
            journal = AvailabilityJournal(key, selection_id=selection)
            journal.valid(key, BASE)
            flush(journal, catalog)
        catalog.persist_source_availability_watermark("selection-old", BASE + SECOND)
        catalog.persist_source_availability_watermark("selection-new", BASE + 10 * SECOND)
        states = catalog.source_availability()
        assert states["old"]["observed_at_utc_ns"] == BASE + SECOND
        assert states["new"]["observed_at_utc_ns"] == BASE + 10 * SECOND
        assert states["legacy"]["observed_at_utc_ns"] == BASE
        # A monitor checkpoint does not manufacture a later payload receive time.
        assert states["new"]["last_valid_utc_ns"] == BASE
        report = continuity_report(catalog, start=BASE, end=BASE + 12 * SECOND)
        assert {s["source_id"]: s["unavailable_or_unknown_ns"] for s in report["sources"]} == {
            "old": 11 * SECOND, "new": 2 * SECOND, "legacy": 12 * SECOND,
        }
        restarted = AvailabilityJournal("restart", selection_id="selection-restart")
        restarted.previous = states
        assert restarted.ensure("old", BASE + 12 * SECOND).unavailable_since == BASE + SECOND


def _wal_crash_child(root: str, state: str, retain_partial: bool) -> None:
    """Leave an uncheckpointed WAL for the parent to truncate after SIGKILL."""
    layout = ensure_storage_layout(Path(root))
    catalog = Catalog(layout.catalog)
    catalog._connection.execute("PRAGMA wal_autocheckpoint=0")
    catalog._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    journal = AvailabilityJournal("before-crash", selection_id="selection-crash")
    journal.valid("spot/BTCUSDT/diff_depth/ws", BASE)
    flush(journal, catalog)
    catalog.persist_source_availability_watermark("selection-crash", BASE)
    wal = Path(str(layout.catalog) + "-wal")
    retained_bytes = wal.stat().st_size
    writer = RawChunkWriter(
        layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT", stream="diff_depth",
        collector_instance_id="before-crash", collector_version="test", created_at_utc_ns=BASE,
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
    catalog.persist_source_availability_watermark("selection-crash", BASE + 10 * SECOND)
    print(json.dumps({"chunk_id": chunk_id, "retained_bytes": retained_bytes,
                      "wal_bytes": wal.stat().st_size}), flush=True)
    sys.stdin.read(1)  # The parent kills us without closing/checkpointing SQLite.


@pytest.mark.parametrize("state", ["missing", "ACTIVE", "SEALING"])
@pytest.mark.parametrize("retain_partial", [False, True])
def test_real_wal_tail_truncation(
    tmp_path: Path, state: str, retain_partial: bool,
) -> None:
    child_code = (
        "from tests.unit.test_m1_2_catalog_and_availability import _wal_crash_child; "
        "import sys; _wal_crash_child(sys.argv[1], sys.argv[2], sys.argv[3] == 'True')"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, str(tmp_path), state, str(retain_partial)],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert child.stdout is not None
        line = child.stdout.readline()
        if not line:
            _, errors = child.communicate(timeout=30)
            pytest.fail(errors)
        evidence = json.loads(line)
    finally:
        child.kill()
        child.communicate(timeout=30)
    layout = ensure_storage_layout(tmp_path)
    wal = Path(str(layout.catalog) + "-wal")
    assert evidence["wal_bytes"] > evidence["retained_bytes"] > 32
    with wal.open("r+b") as stream:
        stream.truncate(evidence["retained_bytes"])
    with Catalog(layout.catalog) as catalog:
        chunk_id = evidence["chunk_id"]
        assert catalog.state(chunk_id) == (None if state == "missing" else ChunkState(state))
        assert catalog.source_availability_watermark("selection-crash") == BASE
        actions = recover_storage(layout=layout, catalog=catalog)
        assert not [a for a in actions if a.action == "reconcile_failed"]
        assert catalog.state(chunk_id) is ChunkState.SEALED
        assert not list(layout.active.glob("*.partial"))
        row = catalog.chunk(chunk_id)
        assert row is not None and row["record_count"] == 1
        restarted = AvailabilityJournal("after-crash")
        restarted.previous = catalog.source_availability()
        key = "spot/BTCUSDT/diff_depth/ws"
        assert restarted.ensure(key, BASE + 12 * SECOND).unavailable_since == BASE
        restarted.valid(key, BASE + 12 * SECOND)
        flush(restarted, catalog)
        report = continuity_report(catalog, start=BASE, end=BASE + 12 * SECOND)
        assert report["sources"][0]["unavailable_or_unknown_ns"] == 12 * SECOND
        # Recovery is idempotent after the roll-forward.
        assert not [a for a in recover_storage(layout=layout, catalog=catalog)
                    if a.action == "reconcile_failed"]


def test_reconcile_retained_partial_uses_raw_identity_not_catalog_path(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout, catalog=catalog, market="spot", symbol="BTCUSDT", stream="diff_depth",
            collector_instance_id="test", collector_version="test", created_at_utc_ns=BASE,
        )
        writer.append(event(1))
        writer.close()
        raw = writer.path.read_bytes()
        seal_partial(writer.path, layout=layout, catalog=catalog)
        writer.path.write_bytes(raw)
        unrelated = layout.active / "unrelated-evidence"
        unrelated.write_bytes(b"keep")
        with catalog._transaction() as connection:
            connection.execute(
                "UPDATE chunks SET state=?, partial_path=? WHERE chunk_id=?",
                (ChunkState.ACTIVE, layout.relative(unrelated), str(writer.header.chunk_id)),
            )
        reconcile_sealed(layout=layout, catalog=catalog)
        assert not writer.path.exists()
        assert unrelated.read_bytes() == b"keep"
