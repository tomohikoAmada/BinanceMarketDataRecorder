from __future__ import annotations

import asyncio
import gc
import hashlib
import json
import logging
import os
import threading
import tracemalloc
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.binance.usdm.websocket import WebSocketConnection
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from tests.integration.test_usdm_ingress_backpressure import (
    BurstSocket,
    DelayedStreamSpool,
    book_ticker,
    captured,
    make_collector,
)

pytestmark = pytest.mark.stress


def _fd_count() -> int | None:
    proc_fds = Path("/proc/self/fd")
    return len(os.listdir(proc_fds)) if proc_fds.is_dir() else None


class RotatingSocket:
    def __init__(
        self,
        payload: bytes,
        *,
        disconnect: bool,
        final_stop: asyncio.Event | None,
    ) -> None:
        self.payload = payload
        self.disconnect = disconnect
        self.final_stop = final_stop
        self.sent = False

    async def recv(self, decode: bool | None = None) -> bytes:
        if not self.sent:
            self.sent = True
            return self.payload
        if self.final_stop is not None:
            self.final_stop.set()
            await asyncio.Future[None]()
        if self.disconnect:
            raise OSError("deterministic disconnect")
        await asyncio.Future[None]()
        raise AssertionError("unreachable")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


def test_one_thousand_disconnect_reconnect_rotation_cycles_are_bounded(
    tmp_path: Path,
) -> None:
    before_threads = threading.active_count()
    before_fds = _fd_count()
    tracemalloc.start()

    async def exercise() -> tuple[int, int]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield RotatingSocket(
                book_ticker(attempts),
                disconnect=attempts % 2 == 0,
                final_stop=stop if attempts == 1_000 else None,
            )

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=64,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=1,
        )
        collector.logger.setLevel(logging.CRITICAL)
        collector.planned_rotation_seconds = 0.001
        try:
            # Each of the 1,000 boundaries now seals its generation with
            # persistent gap evidence (M21.4.11 reconnect-boundary contract);
            # observed wall-clock is ~11-52s on this host including the
            # post-run re-read of all 1,000 sealed chunks.
            await asyncio.wait_for(collector.run(stop), timeout=90)
            return attempts, collector.receipt_queue_stats.high_watermark
        finally:
            catalog.close()

    attempts, high_watermark = asyncio.run(exercise())
    gc.collect()
    current_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    after_fds = _fd_count()
    persisted, _manifests = captured(tmp_path)
    assert attempts == 1_000
    assert [event.raw_payload for event in persisted] == [
        book_ticker(value) for value in range(1, 1_001)
    ]
    assert len({event.connection_id for event in persisted}) == 1_000
    assert high_watermark <= 64
    assert current_memory < 16 * 1024 * 1024
    assert peak_memory < 64 * 1024 * 1024
    assert threading.active_count() <= before_threads + 1
    if before_fds is not None and after_fds is not None:
        assert after_fds <= before_fds + 1


@pytest.mark.parametrize("rotate_gap_boundary", [False, True], ids=["normal", "rotation"])
def test_one_hundred_backpressure_boundaries_preserve_order_and_gap_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rotate_gap_boundary: bool,
) -> None:
    before_threads = threading.active_count()
    before_fds = _fd_count()
    if rotate_gap_boundary:
        original_should_rotate = RawChunkWriter.should_rotate
        rotated = False

        def boundary_rotation(
            writer: RawChunkWriter, *, now_monotonic: float | None = None
        ) -> bool:
            nonlocal rotated
            if not rotated and "sequence_gap" in writer._statistics.capture_flags:
                rotated = True
                now_monotonic = writer._rotation_deadline_monotonic
            return original_should_rotate(writer, now_monotonic=now_monotonic)

        monkeypatch.setattr(RawChunkWriter, "should_rotate", boundary_rotation)

    async def exercise() -> tuple[int, int]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 101:
                cast(DelayedStreamSpool, collector.spool).drain_delay_seconds = 0
                messages = [book_ticker(attempts * 10_000)]
            else:
                start = attempts * 10_000
                messages = [book_ticker(start + value) for value in range(500)]
            yield BurstSocket(messages, stop=stop if attempts == 101 else None)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=1,
            drain_delay_seconds=0.005,
            put_timeout_seconds=0.0002,
            saturation_timeout_seconds=0.001,
        )
        collector.logger.setLevel(logging.CRITICAL)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=20)
            return attempts, collector.receipt_queue_stats.high_watermark
        finally:
            catalog.close()

    attempts, high_watermark = asyncio.run(exercise())
    persisted, manifests = captured(tmp_path)
    update_ids = [int(event.source_sequence["u"]) for event in persisted]
    assert attempts == 101
    assert update_ids == sorted(update_ids)
    assert len(update_ids) == len(set(update_ids))
    assert high_watermark == 1
    connection_ids = list(dict.fromkeys(event.connection_id for event in persisted))
    assert len(connection_ids) == 101
    by_connection = {
        connection_id: [event for event in persisted if event.connection_id == connection_id]
        for connection_id in connection_ids
    }
    gap_manifests = [document for document in manifests if document["gap"]]
    data_gaps = [document for document in gap_manifests if document["record_count"]]
    markers = [document for document in manifests if not document["record_count"]]
    assert all(document["complete"] is False for document in gap_manifests)
    assert all(document["capture_flags"] == ["sequence_gap"] for document in data_gaps)
    assert all(len(document["connection_ids"]) == 1 for document in data_gaps)
    assert {document["connection_ids"][0] for document in data_gaps} == set(connection_ids)
    # A phase-aligned rotation can seal the boundary frame before the reconnect
    # intent seals; the existing protocol then retains a zero-record marker.
    # Check its exact durable authority instead of assuming one file per gap.
    assert all(document["connection_ids"] == [] for document in markers)
    assert all(document["gap"] is True for document in markers)
    assert all(document["capture_flags"] == ["reconnect_gap"] for document in markers)
    if rotate_gap_boundary:
        assert rotated is True
        assert markers
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        starts = catalog.operational_events(event_type="STREAM_DISCONTINUITY_STARTED")
        completions = catalog.operational_events(
            event_type="STREAM_DISCONTINUITY_COMPLETED"
        )
        marker_intents = []
        for marker in markers:
            rows = catalog._connection.execute(
                "SELECT evidence_json FROM chunk_transitions "
                "WHERE chunk_id=? AND to_state='SEALING'",
                (marker["chunk_id"],),
            ).fetchall()
            assert len(rows) == 1
            marker_intents.append(json.loads(rows[0]["evidence_json"])["seal_intent"])
    assert len(starts) == len(completions) == 100
    start_evidence = [cast(dict[str, object], event["evidence"]) for event in starts]
    completion_evidence = [
        cast(dict[str, object], event["evidence"]) for event in completions
    ]
    assert all(event["boundary_frame_persisted"] is True for event in start_evidence)
    assert all(
        event["historical_continuity_restored"] is False
        for event in completion_evidence
    )
    starts_by_gap = {event["gap_id"]: event for event in start_evidence}
    completions_by_gap = {event["gap_id"]: event for event in completion_evidence}
    assert len(starts_by_gap) == len(completions_by_gap) == 100
    assert starts_by_gap.keys() == completions_by_gap.keys()
    for ordinal, started in enumerate(start_evidence):
        completed = completions_by_gap[started["gap_id"]]
        old_id, new_id = connection_ids[ordinal : ordinal + 2]
        assert started["original_connection_id"] == old_id
        assert started["original_generation"] == ordinal
        assert started["reason"] == completed["reason"] == "ingress_backpressure"
        assert completed["original_connection_id"] == old_id
        assert completed["new_connection_id"] == new_id
        assert completed["new_generation"] == ordinal + 1
        assert started["boundary_payload_sha256"] == hashlib.sha256(
            by_connection[old_id][-1].raw_payload
        ).hexdigest()
        assert "sequence_gap" in by_connection[old_id][-1].capture_flags
        assert "sequence_gap" in by_connection[new_id][0].capture_flags
    for intent in marker_intents:
        started = starts_by_gap[intent["gap_id"]]
        for key in (
            "original_connection_id",
            "original_generation",
            "reason",
            "boundary_payload_sha256",
            "boundary_frame_persisted",
            "gap_started_at_utc_ns",
        ):
            assert intent[key] == started[key]
    after_fds = _fd_count()
    assert threading.active_count() <= before_threads + 1
    if before_fds is not None and after_fds is not None:
        assert after_fds <= before_fds + 1
