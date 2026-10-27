from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

import pytest

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.format import ScanIssue, scan_chunk
from binance_market_data_recorder.spool.queue import BoundedEventQueue, IngressQueueFull
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_incremental_clean_seal import (
    _assert_incremental_matches_scan,
    _envelope,
    _read_envelopes,
    _writer,
)


def test_batch_exact_bytes_metadata_transitions_and_single_syscall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Test append/fsync accounting independently of host scheduling delays.
    monkeypatch.setattr("binance_market_data_recorder.spool.writer.time.monotonic", lambda: 1000.0)
    events = [
        _envelope(
            i,
            connection_id=f"connection-{i // 7}",
            flags=("sequence_gap",) if i == 15 else (),
            exchange_times=(i, i + 1, i + 2),
            payload=b"not JSON\x00\xff" if i == 13 else None,
        )
        for i in range(80)
    ]
    paths = []
    evidence = []
    calls = []
    original = os.writev

    def writev(fd: int, vectors: list[memoryview]) -> int:
        calls.append(len(vectors))
        return original(fd, vectors)

    for batched in (False, True):
        layout = ensure_storage_layout(tmp_path / str(batched))
        with Catalog(layout.catalog) as catalog:
            writer = _writer(layout, catalog, chunk_id=UUID(int=123), created_at_utc_ns=42)
            writer._rotation_deadline_monotonic = float("inf")
            paths.append(writer.path)
            if batched:
                monkeypatch.setattr(os, "writev", writev)
                assert len(writer.append_batch(events)) == len(events)
                assert calls == [80]
            else:
                for event in events:
                    writer.append(event)
            assert writer.record_count == 80
            assert writer.synced_record_count == 0
            evidence.append(writer._take_clean_seal_evidence())
            assert writer.synced_record_count == 80
    assert paths[0].read_bytes() == paths[1].read_bytes()
    assert evidence[0].statistics == evidence[1].statistics
    assert evidence[0].connection_transitions == evidence[1].connection_transitions
    assert evidence[0].uncompressed_sha256 == evidence[1].uncompressed_sha256
    assert _read_envelopes(paths[1]) == events
    assert scan_chunk(paths[1]).is_clean


@pytest.mark.parametrize("fragment", [1, 13, 500])
def test_short_vectored_writes_keep_exact_frames(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fragment: int,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        original = os.write
        interrupted = False

        def short_writev(fd: int, vectors: list[memoryview]) -> int:
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                raise InterruptedError()
            return original(fd, b"".join(vectors)[:fragment])

        monkeypatch.setattr(os, "writev", short_writev)
        events = [_envelope(i) for i in range(17)]
        assert len(writer.append_batch(events)) == 17
        _assert_incremental_matches_scan(writer)
        assert _read_envelopes(writer.path) == events


@pytest.mark.parametrize("failure", ["zero", "error", "observer", "fsync"])
def test_batch_failure_never_issues_clean_evidence_or_false_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        original_write = os.write
        calls = 0

        def failed_writev(fd: int, vectors: list[memoryview]) -> int:
            nonlocal calls
            calls += 1
            if calls == 1:
                return original_write(fd, bytes(vectors[0]) + bytes(vectors[1])[:3])
            if failure == "zero":
                return 0
            raise OSError("injected batch write failure")

        def fail(*args: object) -> None:
            raise OSError("injected observation or fsync failure")

        if failure in {"zero", "error"}:
            monkeypatch.setattr(os, "writev", failed_writev)
        elif failure == "observer":
            writer.operation_observer = fail
        else:
            monkeypatch.setattr(os, "fsync", fail)
            writer._last_sync_monotonic = 0
        with pytest.raises(OSError):
            writer.append_batch([_envelope(i) for i in range(3)])
        assert writer.poisoned
        assert writer.synced_record_count == 0
        with pytest.raises(RuntimeError, match="poisoned"):
            writer._take_clean_seal_evidence()
        if failure in {"zero", "error"}:
            assert writer.record_count == 0
            scanned = scan_chunk(writer.path)
            assert scanned.issue is ScanIssue.TRUNCATED_TAIL
            assert scanned.statistics.record_count == 1


def test_batch_snapshots_mutable_sequence_before_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        event = _envelope(7)
        original = os.writev

        def mutate(fd: int, vectors: list[memoryview]) -> int:
            event.source_sequence["u"] = 999
            return original(fd, vectors)

        monkeypatch.setattr(os, "writev", mutate)
        writer.append_batch([event])
        _assert_incremental_matches_scan(writer)
        assert _read_envelopes(writer.path)[0].source_sequence == {"U": 7, "u": 7}


def test_batch_bounds_and_sync_each_frame(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        assert len(writer.append_batch([_envelope(i) for i in range(300)])) <= 256
        writer.close()
        writer = _writer(layout, catalog, durability_interval_seconds=0)
        assert len(writer.append_batch([_envelope(i) for i in range(3)])) == 1
        assert writer.record_count == writer.synced_record_count == 1
        _assert_incremental_matches_scan(writer)


def test_spool_queue_enforces_bytes_even_with_free_slots() -> None:
    queue = BoundedEventQueue(100, byte_capacity=10000)
    event = _envelope(1, payload=b"x" * 5000)
    queue.put_nowait(event)
    before = queue.queued_bytes
    with pytest.raises(IngressQueueFull, match="byte capacity"):
        queue.put_nowait(event)
    assert queue.depth == 1 and queue.queued_bytes == before
    assert queue.get_nowait() == event
    assert queue.queued_bytes == 0
    with pytest.raises(IngressQueueFull):
        queue.put_nowait(_envelope(2, payload=b"x" * 10000))
    assert queue.depth == queue.queued_bytes == 0


def test_writer_portable_fallback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = _writer(layout, catalog)
        monkeypatch.delattr(os, "writev")
        events: list[EventEnvelope] = [_envelope(i) for i in range(3)]
        writer.append_batch(events)
        _assert_incremental_matches_scan(writer)
        assert _read_envelopes(writer.path) == events


def test_spool_rotation_and_progress_distinguish_write_from_sync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from binance_market_data_recorder.spool.stream import StreamSpool
    from binance_market_data_recorder.spool.writer import RotationPolicy

    monkeypatch.setattr("binance_market_data_recorder.spool.writer.time.monotonic", lambda: 1000.0)
    layout = ensure_storage_layout(tmp_path)
    observations = []
    with Catalog(layout.catalog) as catalog:
        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-1",
            collector_version="0.1.0+test",
            queue_capacity=20,
            rotation=RotationPolicy(seconds=3600, bytes=1024 * 1024),
            durability_interval_seconds=1,
            max_frame_bytes=1024 * 1024,
            event_observer=lambda envelope, size, depth: observations.append(size),
        )
        for i in range(5):
            spool.enqueue(_envelope(i, payload=bytes([i]) * 400000))
        assert spool.drain_all() == 5
        progress = spool.persistence_progress()
        assert progress.written_frames == 5
        assert progress.synchronized_frames == 3
        spool.close_and_seal()
        assert spool.persistence_progress().synchronized_frames == 5
        manifests = list(layout.manifests.glob("*.json"))
        assert len(manifests) == 2
        import json

        documents = [json.loads(path.read_text()) for path in manifests]
        assert sum(document["record_count"] for document in documents) == 5
        assert sum(observations) == sum(document["uncompressed_bytes"] for document in documents)
        assert spool.queue.depth == spool.queue.queued_bytes == 0
