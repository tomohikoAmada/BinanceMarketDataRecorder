from __future__ import annotations

import asyncio
import gc
import io
import json
import logging
import os
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard

import binance_market_data_recorder.spool.stream as stream_module
from binance_market_data_recorder.binance.spot.websocket import (
    ReceivedFrame,
    ReconnectBackoff,
)
from binance_market_data_recorder.binance.usdm.schema import (
    UsdMStream,
    envelope_from_websocket_frame,
)
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
    _run_owned_blocking_call,
)
from binance_market_data_recorder.collector.supervisor import MarketCollectorSupervisor
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
from binance_market_data_recorder.spool.queue import IngressGapStateConflict
from binance_market_data_recorder.spool.recovery import recover_storage
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


class BurstSocket:
    def __init__(
        self,
        messages: list[bytes],
        *,
        stop: asyncio.Event | None = None,
    ) -> None:
        self.messages = iter(messages)
        self.stop = stop
        self.close_reasons: list[str] = []

    async def recv(self, decode: bool | None = None) -> bytes:
        try:
            return next(self.messages)
        except StopIteration:
            if self.stop is not None:
                self.stop.set()
                await asyncio.Future[None]()
            raise OSError("injected disconnect") from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_reasons.append(reason)


class BlockingSocket:
    async def recv(self, decode: bool | None = None) -> bytes:
        await asyncio.Future[None]()
        raise AssertionError("unreachable")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


class BlockingCloseSocket(BlockingSocket):
    def __init__(self) -> None:
        self.close_started = asyncio.Event()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_started.set()
        await asyncio.Future[None]()


class DelayedStreamSpool(StreamSpool):
    def __init__(self, *args: Any, drain_delay_seconds: float, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.drain_delay_seconds = drain_delay_seconds

    def drain_all(self) -> int:
        time.sleep(self.drain_delay_seconds)
        return super().drain_all()


class BlockingDrainStreamSpool(StreamSpool):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.drain_started = threading.Event()
        self.release_drain = threading.Event()

    def drain_all(self) -> int:
        self.drain_started.set()
        if not self.release_drain.wait(timeout=3):
            raise TimeoutError("test did not release blocked Raw drain")
        return super().drain_all()


class BlockingWriterOperationStreamSpool(StreamSpool):
    def __init__(
        self,
        *args: Any,
        blocked_operation: str,
        fail_operation: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.blocked_operation = blocked_operation
        self.fail_operation = fail_operation
        self.operation_started = threading.Event()
        self.release_operation = threading.Event()
        self.abort_started = threading.Event()
        self.concurrent_abort = False
        self.abort_calls = 0
        self._active_operation: str | None = None
        self._test_guard = threading.Lock()

    def _enter_blocked_operation(self, operation: str) -> None:
        if self.blocked_operation != operation:
            return
        with self._test_guard:
            if self._active_operation is not None:
                raise AssertionError("writer test operation overlapped itself")
            self._active_operation = operation
        self.operation_started.set()
        if not self.release_operation.wait(timeout=3):
            raise TimeoutError(f"test did not release blocked Raw {operation}")

    def _leave_blocked_operation(self, operation: str) -> None:
        if self.blocked_operation != operation:
            return
        with self._test_guard:
            self._active_operation = None

    def drain_all(self) -> int:
        self._enter_blocked_operation("drain")
        try:
            if self.fail_operation and self.blocked_operation == "drain":
                raise OSError("injected Raw drain failure")
            return super().drain_all()
        finally:
            self._leave_blocked_operation("drain")

    def sync(self) -> None:
        self._enter_blocked_operation("sync")
        try:
            if self.fail_operation and self.blocked_operation == "sync":
                raise OSError("injected Raw sync failure")
            super().sync()
        finally:
            self._leave_blocked_operation("sync")

    def close_and_seal(self) -> dict[str, object] | None:
        self._enter_blocked_operation("seal")
        try:
            if self.fail_operation and self.blocked_operation == "seal":
                raise OSError("injected Raw seal failure")
            return super().close_and_seal()
        finally:
            self._leave_blocked_operation("seal")

    def abort_writer(self) -> None:
        with self._test_guard:
            if self._active_operation is not None:
                self.concurrent_abort = True
            self.abort_calls += 1
        self.abort_started.set()
        super().abort_writer()


async def wait_for_thread_event(event: threading.Event, *, timeout: float = 1) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not event.is_set():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("timed out waiting for worker thread")
        await asyncio.sleep(0.001)


class HealthyMarket:
    def __init__(self) -> None:
        self.stopped = False

    async def run(self, stop: asyncio.Event) -> None:
        await stop.wait()
        self.stopped = True


def book_ticker(update_id: int) -> bytes:
    return json.dumps(
        {
            "e": "bookTicker",
            "u": update_id,
            "s": "BTCUSDT",
            "b": "100.0",
            "B": "1.0",
            "a": "101.0",
            "A": "2.0",
            "T": update_id,
            "E": update_id,
        },
        separators=(",", ":"),
    ).encode()


def agg_trade(update_id: int) -> bytes:
    return json.dumps(
        {
            "e": "aggTrade",
            "E": update_id,
            "T": update_id,
            "s": "BTCUSDT",
            "a": update_id,
            "p": "100.0",
            "q": "1.0",
            "f": update_id,
            "l": update_id,
            "m": True,
        },
        separators=(",", ":"),
    ).encode()


def diff_depth(update_id: int) -> bytes:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": update_id,
            "T": update_id,
            "s": "BTCUSDT",
            "U": update_id,
            "u": update_id,
            "pu": max(0, update_id - 1),
            "b": [],
            "a": [],
        },
        separators=(",", ":"),
    ).encode()


def make_collector(
    root: Path,
    *,
    opener: Any,
    capacity: int,
    drain_delay_seconds: float,
    put_timeout_seconds: float,
    saturation_timeout_seconds: float,
    stream: UsdMStream = UsdMStream.BOOK_TICKER,
    durability_interval_seconds: float = 0,
) -> tuple[UsdMStreamCollector, Catalog]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = DelayedStreamSpool(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream=stream.value,
        collector_instance_id="m21-4-test",
        collector_version="0.1.0+test",
        queue_capacity=max(4, capacity),
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=durability_interval_seconds,
        max_frame_bytes=1024 * 1024,
        drain_delay_seconds=drain_delay_seconds,
    )
    collector = UsdMStreamCollector(
        stream=stream,
        route="market" if stream == UsdMStream.AGG_TRADE else "public",
        wire_name={
            UsdMStream.DIFF_DEPTH: "btcusdt@depth@100ms",
            UsdMStream.AGG_TRADE: "btcusdt@aggTrade",
            UsdMStream.BOOK_TICKER: "btcusdt@bookTicker",
        }[stream],
        spool=spool,
        collector_instance_id="m21-4-test",
        collector_version="0.1.0+test",
        logger=logging.getLogger("test.m21-4.usdm.backpressure"),
        receipt_queue_capacity=capacity,
        planned_rotation_seconds=60,
        backoff=ReconnectBackoff(
            initial_seconds=0.001,
            maximum_seconds=0.001,
            jitter_ratio=0,
        ),
        opener=opener,
        backpressure_put_timeout_seconds=put_timeout_seconds,
        backpressure_saturation_timeout_seconds=saturation_timeout_seconds,
        post_close_handoff_timeout_seconds=0.5,
    )
    return collector, catalog


def make_blocking_writer_collector(
    root: Path,
    *,
    blocked_operation: str,
    fail_operation: bool = False,
) -> tuple[UsdMStreamCollector, Catalog, BlockingWriterOperationStreamSpool]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = BlockingWriterOperationStreamSpool(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream="book_ticker",
        collector_instance_id="m21-4-owned-worker",
        collector_version="0.1.0+test",
        queue_capacity=4,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0.75,
        max_frame_bytes=1024 * 1024,
        blocked_operation=blocked_operation,
        fail_operation=fail_operation,
    )

    @asynccontextmanager
    async def unused_opener(_url: str) -> AsyncIterator[WebSocketConnection]:
        yield BlockingSocket()

    collector = UsdMStreamCollector(
        stream=UsdMStream.BOOK_TICKER,
        route="public",
        wire_name="btcusdt@bookTicker",
        spool=spool,
        collector_instance_id="m21-4-owned-worker",
        collector_version="0.1.0+test",
        logger=logging.getLogger("test.m21-4.owned-worker"),
        receipt_queue_capacity=2,
        opener=unused_opener,
    )
    return collector, catalog, spool


async def run_writer_with_one_frame(
    collector: UsdMStreamCollector,
    frame: ReceivedFrame,
) -> asyncio.Task[None]:
    placeholder = asyncio.create_task(asyncio.sleep(60))
    try:
        await collector._receipts.put(frame, writer_task=placeholder)
    finally:
        placeholder.cancel()
        await asyncio.gather(placeholder, return_exceptions=True)
    producer_done = asyncio.Event()
    producer_done.set()
    return asyncio.create_task(collector._writer_loop(producer_done))


def captured(root: Path) -> tuple[list[Any], list[dict[str, Any]]]:
    envelopes: list[Any] = []
    documents = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((root / "data/manifests").glob("*.json"))
    ]
    documents.sort(key=lambda item: int(item["created_at_utc_ns"]))
    for document in documents:
        raw = zstandard.ZstdDecompressor().decompress(
            (root / document["relative_path"]).read_bytes()
        )
        source = io.BytesIO(raw)
        decode_chunk_header(source)
        while prefix := source.read(FRAME_PREFIX.size):
            length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
            envelopes.append(decode_envelope(source.read(length)))
    return envelopes, documents


def record_gap_started(
    catalog: Catalog,
    *,
    gap_id: str,
    stream: str = "book_ticker",
    generation: int = 3,
) -> None:
    assert catalog.record_operational_event(
        event_id=f"stream-discontinuity-started:{gap_id}",
        event_type="STREAM_DISCONTINUITY_STARTED",
        occurred_at_utc_ns=100,
        evidence={
            "gap_id": gap_id,
            "market": "um_perpetual",
            "stream": stream,
            "reason": "ingress_backpressure",
            "interval_classification": "UNRELIABLE",
            "gap_started_at_utc_ns": 100,
            "original_connection_id": f"old-{gap_id}",
            "original_generation": generation,
            "boundary_frame_persisted": True,
        },
    )


def record_gap_completed(
    catalog: Catalog,
    *,
    gap_id: str,
    stream: str = "book_ticker",
) -> None:
    assert catalog.record_operational_event(
        event_id=f"stream-discontinuity-completed:{gap_id}",
        event_type="STREAM_DISCONTINUITY_COMPLETED",
        occurred_at_utc_ns=200,
        evidence={
            "gap_id": gap_id,
            "market": "um_perpetual",
            "stream": stream,
            "reason": "ingress_backpressure",
            "gap_ended_at_utc_ns": 200,
            "historical_continuity_restored": False,
        },
    )


def test_short_burst_above_capacity_is_lossless_and_ordered(tmp_path: Path) -> None:
    async def exercise() -> tuple[int, int]:
        stop = asyncio.Event()
        socket = BurstSocket(
            [book_ticker(value) for value in range(64)],
            stop=stop,
        )

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=4,
            drain_delay_seconds=0.005,
            put_timeout_seconds=0.2,
            saturation_timeout_seconds=1.0,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
            stats = collector.receipt_queue_stats
            return stats.high_watermark, stats.wait_count
        finally:
            catalog.close()

    high_watermark, wait_count = asyncio.run(exercise())
    envelopes, manifests = captured(tmp_path)
    assert [event.raw_payload for event in envelopes] == [
        book_ticker(value) for value in range(64)
    ]
    assert high_watermark == 4
    assert wait_count > 0
    assert all(document["complete"] is True for document in manifests)
    assert all(document["gap"] is False for document in manifests)


@pytest.mark.parametrize(
    "stream,payload_factory",
    [
        (UsdMStream.BOOK_TICKER, book_ticker),
        (UsdMStream.AGG_TRADE, agg_trade),
    ],
)
def test_sustained_overload_rotates_generation_with_persistent_gap(
    tmp_path: Path,
    stream: UsdMStream,
    payload_factory: Callable[[int], bytes],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    async def exercise() -> tuple[int, list[BurstSocket]]:
        stop = asyncio.Event()
        attempts = 0
        sockets: list[BurstSocket] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                messages = [payload_factory(0)]
            elif attempts == 2:
                messages = [payload_factory(value) for value in range(1, 501)]
            else:
                messages = [payload_factory(10_000)]
            if attempts == 3:
                cast(DelayedStreamSpool, collector.spool).drain_delay_seconds = 0
            socket = BurstSocket(messages, stop=stop if attempts == 3 else None)
            sockets.append(socket)
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0.05,
            put_timeout_seconds=0.005,
            saturation_timeout_seconds=0.02,
            stream=stream,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
            return collector.receipt_queue_stats.high_watermark, sockets
        finally:
            catalog.close()

    high_watermark, sockets = asyncio.run(exercise())
    envelopes, manifests = captured(tmp_path)
    payloads = [event.raw_payload for event in envelopes]
    first_new = payloads.index(payload_factory(10_000))
    assert payloads[first_new:] == [payload_factory(10_000)]
    assert len(payloads[:first_new]) == len(set(payloads[:first_new]))
    assert payloads[:first_new] == [
        payload_factory(value) for value in range(first_new)
    ]
    assert envelopes[first_new - 1].capture_flags == ("sequence_gap",)
    assert envelopes[first_new].capture_flags == ("sequence_gap",)
    assert all("sequence_gap" not in item.capture_flags for item in envelopes[first_new + 1 :])
    assert high_watermark == 2
    assert sockets[1].close_reasons == ["bounded ingress backpressure"]
    assert len({tuple(document["connection_ids"]) for document in manifests}) == 2
    gap_manifests = [document for document in manifests if document["gap"]]
    assert len(gap_manifests) == 2
    assert all(document["complete"] is False for document in gap_manifests)
    assert all(document["capture_flags"] == ["sequence_gap"] for document in gap_manifests)
    assert len(gap_manifests[-1]["connection_ids"]) == 1
    assert set(gap_manifests[0]["connection_ids"]).isdisjoint(
        gap_manifests[-1]["connection_ids"]
    )

    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        events = catalog.operational_events()
    discontinuities = [
        event for event in events if str(event["event_type"]).startswith("STREAM_DISCONTINUITY")
    ]
    assert [event["event_type"] for event in discontinuities] == [
        "STREAM_DISCONTINUITY_STARTED",
        "STREAM_DISCONTINUITY_COMPLETED",
    ]
    started = discontinuities[0]["evidence"]
    completed = discontinuities[1]["evidence"]
    assert isinstance(started, dict)
    assert isinstance(completed, dict)
    assert started["boundary_frame_persisted"] is True
    assert completed["historical_continuity_restored"] is False
    assert completed["raw_gap_marker"] == "sequence_gap"
    timeout_records = [
        record
        for record in caplog.records
        if getattr(record, "structured_event", "")
        == "usdm_ingress_backpressure_timeout"
    ]
    assert len(timeout_records) == 1
    fields = cast(Any, timeout_records[0]).structured_fields
    assert fields["stream"] == stream.value
    assert fields["receipt_queue_capacity"] == 2
    assert fields["receipt_queue_high_watermark"] == 2
    for name in (
        "queue_wait_p50_ns",
        "queue_wait_p95_ns",
        "queue_wait_p99_ns",
        "writer_batch_size",
        "writer_drain_ns",
        "writer_append_ns",
        "writer_fsync_ns",
        "writer_seal_ns",
    ):
        assert name in fields


def test_diff_depth_overload_ends_generation_and_requires_outer_resync(
    tmp_path: Path,
) -> None:
    async def exercise() -> tuple[list[str], int]:
        stop = asyncio.Event()
        attempts = 0
        lifecycle: list[str] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            messages = (
                [diff_depth(value) for value in range(1, 500)]
                if attempts == 1
                else [diff_depth(10_000)]
            )
            socket = BurstSocket(messages, stop=stop if attempts == 2 else None)
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0.05,
            put_timeout_seconds=0.005,
            saturation_timeout_seconds=0.02,
            stream=UsdMStream.DIFF_DEPTH,
        )
        collector.lifecycle_observer = lifecycle.append
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            assert stop.is_set() is False
            assert lifecycle[-2:] == ["disconnected", "ingress_backpressure"]
            cast(DelayedStreamSpool, collector.spool).drain_delay_seconds = 0
            await asyncio.wait_for(collector.run(stop), timeout=3)
            return lifecycle, attempts
        finally:
            catalog.close()

    lifecycle, attempts = asyncio.run(exercise())
    persisted, manifests = captured(tmp_path)
    recovered_index = [event.raw_payload for event in persisted].index(diff_depth(10_000))
    assert persisted[recovered_index].capture_flags == ("sequence_gap",)
    assert attempts == 2
    assert lifecycle.count("ingress_backpressure") == 1
    assert any(document["gap"] is True for document in manifests)


@pytest.mark.parametrize("failure_phase", ["enqueue", "drain", "seal"])
def test_writer_failure_remains_fatal_and_leaves_no_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        socket = BurstSocket([book_ticker(1)], stop=stop)

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )

        def fail(*_args: object, **_kwargs: object) -> Any:
            raise OSError(f"injected {failure_phase} failure")

        target = {
            "enqueue": "enqueue",
            "drain": "drain_all",
            "seal": "close_and_seal",
        }[failure_phase]
        monkeypatch.setattr(collector.spool, target, fail)
        baseline = set(asyncio.all_tasks())
        try:
            with pytest.raises(OSError, match=failure_phase):
                await collector.run(stop)
            await asyncio.sleep(0)
            assert [
                task
                for task in asyncio.all_tasks()
                if task not in baseline
                and task is not asyncio.current_task()
                and not task.done()
            ] == []
        finally:
            catalog.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("failure_phase", ["append", "fsync", "catalog"])
def test_raw_writer_integrity_failure_aborts_descriptor_and_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(1)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )

        def fail(*_args: object, **_kwargs: object) -> Any:
            raise OSError(f"injected {failure_phase} failure")

        if failure_phase == "append":
            monkeypatch.setattr(RawChunkWriter, "append", fail)
        elif failure_phase == "fsync":
            original_sync = RawChunkWriter.sync
            sync_calls = 0

            def fail_append_sync(writer: RawChunkWriter) -> None:
                nonlocal sync_calls
                sync_calls += 1
                if sync_calls == 2:
                    fail()
                original_sync(writer)

            monkeypatch.setattr(RawChunkWriter, "sync", fail_append_sync)
        else:
            monkeypatch.setattr(catalog, "register_active", fail)
        try:
            with pytest.raises(OSError, match=failure_phase):
                await collector.run(stop)
            assert cast(Any, collector.spool)._writer is None
            assert collector.receipt_queue_stats.depth == 0
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_actual_seal_failure_closes_writer_and_retains_recoverable_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(1)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )

        def fail_seal(*_args: object, **_kwargs: object) -> Any:
            raise OSError("injected actual seal failure")

        monkeypatch.setattr(stream_module, "seal_partial", fail_seal)
        try:
            with pytest.raises(OSError, match="actual seal"):
                await collector.run(stop)
            assert cast(Any, collector.spool)._writer is None
            partials = list((tmp_path / "data/active").glob("*.partial"))
            assert len(partials) == 1
            with partials[0].open("ab"):
                pass
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_writer_returning_early_is_fatal_and_all_tasks_are_retrieved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BlockingSocket()

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )

        async def return_early(_producer_done: asyncio.Event) -> None:
            return

        monkeypatch.setattr(collector, "_write_until_done", return_early)
        baseline = set(asyncio.all_tasks())
        try:
            with pytest.raises(RuntimeError, match="stopped unexpectedly"):
                await collector.run(asyncio.Event())
            await asyncio.sleep(0)
            assert [
                task
                for task in asyncio.all_tasks()
                if task not in baseline
                and task is not asyncio.current_task()
                and not task.done()
            ] == []
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_catalog_gap_evidence_failure_is_process_fatal_not_silent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> BurstSocket:
        socket = BurstSocket([book_ticker(value) for value in range(500)])

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0.05,
            put_timeout_seconds=0.005,
            saturation_timeout_seconds=0.02,
        )

        def fail_catalog(*_args: object, **_kwargs: object) -> Any:
            raise OSError("injected Catalog failure")

        monkeypatch.setattr(catalog, "record_operational_event", fail_catalog)
        try:
            with pytest.raises(OSError, match="Catalog"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            assert collector.receipt_queue_stats.depth == 0
            return socket
        finally:
            catalog.close()

    socket = asyncio.run(exercise())
    persisted, _manifests = captured(tmp_path)
    assert len(persisted) == len({event.raw_payload for event in persisted})
    assert "sequence_gap" in persisted[-1].capture_flags
    assert socket.close_reasons == ["bounded ingress backpressure"]


def test_recoverable_stream_overload_does_not_reach_market_supervisor(
    tmp_path: Path,
) -> None:
    async def exercise() -> tuple[dict[str, BaseException], bool, int]:
        global_stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            payloads = (
                [book_ticker(value) for value in range(500)]
                if attempts == 1
                else [book_ticker(10_000)]
            )
            if attempts == 2:
                cast(DelayedStreamSpool, collector.spool).drain_delay_seconds = 0
            yield BurstSocket(payloads, stop=global_stop if attempts == 2 else None)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0.05,
            put_timeout_seconds=0.005,
            saturation_timeout_seconds=0.02,
        )
        healthy = HealthyMarket()
        supervisor = MarketCollectorSupervisor(
            {"spot": healthy, "um_perpetual": collector}
        )
        try:
            await asyncio.wait_for(supervisor.run(global_stop), timeout=5)
            return supervisor.failures, healthy.stopped, attempts
        finally:
            catalog.close()

    failures, healthy_stopped, attempts = asyncio.run(exercise())
    assert failures == {}
    assert healthy_stopped is True
    assert attempts == 2
    persisted, _manifests = captured(tmp_path)
    assert any("sequence_gap" in event.capture_flags for event in persisted)


def test_cancel_during_websocket_recv_awaits_all_owned_tasks(tmp_path: Path) -> None:
    async def exercise() -> None:
        socket = BlockingSocket()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )
        baseline = set(asyncio.all_tasks())
        task = asyncio.create_task(collector.run(asyncio.Event()))
        await asyncio.sleep(0)
        task.cancel()
        try:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)
            await asyncio.sleep(0)
            assert [
                pending
                for pending in asyncio.all_tasks()
                if pending not in baseline
                and pending is not asyncio.current_task()
                and not pending.done()
            ] == []
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_cancel_during_planned_rotation_close_awaits_all_owned_tasks(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        socket = BlockingCloseSocket()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )
        collector.planned_rotation_seconds = 0.01
        baseline = set(asyncio.all_tasks())
        task = asyncio.create_task(collector.run(asyncio.Event()))
        try:
            await asyncio.wait_for(socket.close_started.wait(), timeout=1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)
            await asyncio.sleep(0)
            assert [
                pending
                for pending in asyncio.all_tasks()
                if pending not in baseline
                and pending is not asyncio.current_task()
                and not pending.done()
            ] == []
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_cancel_during_raw_drain_waits_for_worker_then_cleans_up(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        spool = BlockingDrainStreamSpool(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-cancel",
            collector_version="0.1.0+test",
            queue_capacity=4,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(1)])

        collector = UsdMStreamCollector(
            stream=UsdMStream.BOOK_TICKER,
            route="public",
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-cancel",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4.cancel-drain"),
            receipt_queue_capacity=2,
            opener=opener,
        )
        baseline = set(asyncio.all_tasks())
        task = asyncio.create_task(collector.run(stop))
        try:
            assert await asyncio.to_thread(spool.drain_started.wait, 1)
            task.cancel()
            await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.sleep(0.01)
            assert task.done() is False
            spool.release_drain.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)
            await asyncio.sleep(0)
            assert [
                pending
                for pending in asyncio.all_tasks()
                if pending not in baseline
                and pending is not asyncio.current_task()
                and not pending.done()
            ] == []
        finally:
            spool.release_drain.set()
            catalog.close()

    asyncio.run(exercise())


def test_stop_interrupts_reconnect_backoff(tmp_path: Path) -> None:
    async def exercise() -> float:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            asyncio.get_running_loop().call_later(0.01, stop.set)
            raise OSError("injected disconnect")
            yield BurstSocket([])  # pragma: no cover

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )
        collector.backoff = ReconnectBackoff(
            initial_seconds=60,
            maximum_seconds=60,
            jitter_ratio=0,
        )
        started = time.perf_counter()
        try:
            await asyncio.wait_for(collector.run(stop), timeout=0.5)
            assert attempts == 1
            return time.perf_counter() - started
        finally:
            catalog.close()

    assert asyncio.run(exercise()) < 0.5


def test_new_generation_marker_sync_precedes_catalog_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gap_id = "crash-order"
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as seed_catalog:
        record_gap_started(seed_catalog, gap_id=gap_id)

    async def exercise() -> list[str]:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(1)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            durability_interval_seconds=0.75,
        )
        order: list[str] = []
        original_spool_sync = collector.spool.sync
        original_record = catalog.record_operational_event

        def defer_periodic_sync(
            _writer: RawChunkWriter,
            *,
            now_monotonic: float | None = None,
        ) -> bool:
            return False

        def ordered_sync() -> None:
            order.append("raw_sync")
            original_spool_sync()

        def ordered_record(**kwargs: Any) -> bool:
            inserted = original_record(**kwargs)
            if kwargs["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                assert "raw_sync" in order
                order.append("catalog_completion")
            return inserted

        monkeypatch.setattr(RawChunkWriter, "sync_if_due", defer_periodic_sync)
        monkeypatch.setattr(collector.spool, "sync", ordered_sync)
        monkeypatch.setattr(catalog, "record_operational_event", ordered_record)
        collector.envelope_observer = lambda _envelope: order.append("observer")
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            completed = catalog.operational_events(
                event_type="STREAM_DISCONTINUITY_COMPLETED"
            )
            assert len(completed) == 1
            return order
        finally:
            catalog.close()

    order = asyncio.run(exercise())
    assert order[:3] == ["raw_sync", "catalog_completion", "observer"]
    persisted, _manifests = captured(tmp_path)
    assert persisted[0].capture_flags.count("sequence_gap") == 1


def test_gap_marker_sync_failure_leaves_started_open_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gap_id = "sync-failure"
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as seed_catalog:
        record_gap_started(seed_catalog, gap_id=gap_id, stream="diff_depth")

    async def exercise() -> None:
        stop = asyncio.Event()
        observed: list[Any] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([diff_depth(2)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            stream=UsdMStream.DIFF_DEPTH,
            durability_interval_seconds=0.75,
        )

        def fail_sync() -> None:
            raise OSError("injected recovery marker sync failure")

        monkeypatch.setattr(collector.spool, "sync", fail_sync)
        collector.envelope_observer = observed.append
        baseline = set(asyncio.all_tasks())
        try:
            with pytest.raises(OSError, match="recovery marker sync"):
                await collector.run(stop)
            assert catalog.operational_events(
                event_type="STREAM_DISCONTINUITY_COMPLETED"
            ) == []
            open_gaps = catalog.unclosed_stream_discontinuities(
                market="um_perpetual", stream="diff_depth"
            )
            assert len(open_gaps) == 1
            assert cast(dict[str, Any], open_gaps[0]["evidence"])["gap_id"] == gap_id
            assert observed == []
            await asyncio.sleep(0)
            assert [
                task
                for task in asyncio.all_tasks()
                if task not in baseline
                and task is not asyncio.current_task()
                and not task.done()
            ] == []
        finally:
            catalog.close()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "stream,payload_factory",
    [
        (UsdMStream.BOOK_TICKER, book_ticker),
        (UsdMStream.AGG_TRADE, agg_trade),
    ],
)
def test_process_restart_restores_gap_and_completes_same_identity_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stream: UsdMStream,
    payload_factory: Callable[[int], bytes],
) -> None:
    @asynccontextmanager
    async def unused_opener(_url: str) -> AsyncIterator[WebSocketConnection]:
        yield BlockingSocket()

    first, first_catalog = make_collector(
        tmp_path,
        opener=unused_opener,
        capacity=2,
        drain_delay_seconds=0,
        put_timeout_seconds=0.1,
        saturation_timeout_seconds=0.2,
        stream=stream,
        durability_interval_seconds=0.75,
    )
    boundary = ReceivedFrame(
        raw_payload=payload_factory(90),
        connection_id="old-process-connection",
        receive_time_utc_ns=100,
        receive_monotonic_ns=100,
        capture_flags=("sequence_gap",),
    )

    async def persist_started_then_exit() -> str:
        await first._persist_batch([boundary])
        await asyncio.to_thread(first.spool.close_and_seal)
        await first._record_gap_started(boundary)
        events = first_catalog.operational_events(
            event_type="STREAM_DISCONTINUITY_STARTED"
        )
        return str(cast(dict[str, Any], events[0]["evidence"])["gap_id"])

    gap_id = asyncio.run(persist_started_then_exit())
    first_catalog.close()

    async def recover() -> tuple[list[dict[str, object]], int]:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([payload_factory(100), payload_factory(101)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            stream=stream,
            durability_interval_seconds=0.75,
        )
        sync_calls = 0
        original_sync = collector.spool.sync

        def count_sync() -> None:
            nonlocal sync_calls
            sync_calls += 1
            original_sync()

        monkeypatch.setattr(collector.spool, "sync", count_sync)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            return catalog.operational_events(), sync_calls
        finally:
            catalog.close()

    events, sync_calls = asyncio.run(recover())
    starts = [event for event in events if event["event_type"] == "STREAM_DISCONTINUITY_STARTED"]
    completions = [
        event
        for event in events
        if event["event_type"] == "STREAM_DISCONTINUITY_COMPLETED"
    ]
    assert len(starts) == len(completions) == 1
    completed = cast(dict[str, Any], completions[0]["evidence"])
    assert completed["gap_id"] == gap_id
    assert completed["original_connection_id"] == "old-process-connection"
    assert completed["original_generation"] == 0
    assert completed["new_generation"] == 1
    assert completed["historical_continuity_restored"] is False
    assert sync_calls == 1
    persisted, _manifests = captured(tmp_path)
    new_events = [event for event in persisted if event.raw_payload != payload_factory(90)]
    assert new_events[0].capture_flags.count("sequence_gap") == 1
    assert "sequence_gap" not in new_events[1].capture_flags


def test_multiple_unclosed_gaps_fail_closed_without_new_evidence(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    catalog = Catalog(layout.catalog)
    record_gap_started(catalog, gap_id="conflict-one", generation=1)
    record_gap_started(catalog, gap_id="conflict-two", generation=2)
    before = catalog.operational_events()
    spool = StreamSpool(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream="book_ticker",
        collector_instance_id="m21-4-conflict",
        collector_version="0.1.0+test",
        queue_capacity=4,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0.75,
        max_frame_bytes=1024 * 1024,
    )
    try:
        with pytest.raises(
            IngressGapStateConflict,
            match="2 conflicting unclosed stream discontinuities",
        ):
            UsdMStreamCollector(
                stream=UsdMStream.BOOK_TICKER,
                route="public",
                wire_name="btcusdt@bookTicker",
                spool=spool,
                collector_instance_id="m21-4-conflict",
                collector_version="0.1.0+test",
                logger=logging.getLogger("test.m21-4.gap-conflict"),
            )
        assert catalog.operational_events() == before
    finally:
        catalog.close()


def test_completed_gap_does_not_mark_normal_startup_event(tmp_path: Path) -> None:
    gap_id = "already-completed"
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as seed_catalog:
        record_gap_started(seed_catalog, gap_id=gap_id)
        record_gap_completed(seed_catalog, gap_id=gap_id)

    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(3)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            durability_interval_seconds=0.75,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            assert len(catalog.operational_events()) == 2
        finally:
            catalog.close()

    asyncio.run(exercise())
    persisted, _manifests = captured(tmp_path)
    assert persisted[0].capture_flags == ()


def test_cancel_after_gap_sync_before_catalog_writes_no_completion_or_leaks_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gap_id = "cancel-during-sync"
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as seed_catalog:
        record_gap_started(seed_catalog, gap_id=gap_id)

    async def exercise() -> None:
        stop = asyncio.Event()
        sync_completed = asyncio.Event()
        release_completion = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(4)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            durability_interval_seconds=0.75,
        )
        original_to_thread = asyncio.to_thread
        gap_sync = collector.spool.sync

        async def hold_after_gap_sync(
            function: Callable[..., Any],
            /,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            result = await original_to_thread(function, *args, **kwargs)
            if function == gap_sync:
                sync_completed.set()
                await release_completion.wait()
            return result

        monkeypatch.setattr(asyncio, "to_thread", hold_after_gap_sync)
        baseline = set(asyncio.all_tasks())
        task = asyncio.create_task(collector.run(stop))
        try:
            await asyncio.wait_for(sync_completed.wait(), timeout=1)
            task.cancel()
            await asyncio.sleep(0.02)
            assert task.done() is False
            release_completion.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=2)
            assert catalog.operational_events(
                event_type="STREAM_DISCONTINUITY_COMPLETED"
            ) == []
            assert len(
                catalog.unclosed_stream_discontinuities(
                    market="um_perpetual", stream="book_ticker"
                )
            ) == 1
            await asyncio.sleep(0)
            assert [
                pending
                for pending in asyncio.all_tasks()
                if pending not in baseline
                and pending is not asyncio.current_task()
                and not pending.done()
            ] == []
        finally:
            release_completion.set()
            catalog.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("fail_sync", [False, True])
def test_cancel_inside_owned_gap_sync_waits_before_abort(
    tmp_path: Path,
    fail_sync: bool,
) -> None:
    gap_id = f"owned-sync-{'failure' if fail_sync else 'success'}"
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as seed_catalog:
        record_gap_started(seed_catalog, gap_id=gap_id)

    async def exercise() -> None:
        collector, catalog, spool = make_blocking_writer_collector(
            tmp_path,
            blocked_operation="sync",
            fail_operation=fail_sync,
        )
        frame = ReceivedFrame(
            raw_payload=book_ticker(401),
            connection_id="new-owned-sync",
            receive_time_utc_ns=401,
            receive_monotonic_ns=401,
            capture_flags=("sequence_gap",),
        )
        baseline = set(asyncio.all_tasks())
        loop_errors: list[dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
        task = await run_writer_with_one_frame(collector, frame)
        try:
            await wait_for_thread_event(spool.operation_started)
            writer = spool._writer
            assert writer is not None
            os.fstat(writer._descriptor)

            task.cancel()
            await asyncio.sleep(0.02)
            assert task.done() is False
            assert spool.abort_started.is_set() is False
            assert spool.concurrent_abort is False
            os.fstat(writer._descriptor)

            spool.release_operation.set()
            if fail_sync:
                with pytest.raises(OSError, match="injected Raw sync failure"):
                    await asyncio.wait_for(task, timeout=1)
            else:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=1)

            assert spool.abort_calls == 1
            assert spool.concurrent_abort is False
            assert writer.closed is True
            assert catalog.operational_events(
                event_type="STREAM_DISCONTINUITY_COMPLETED"
            ) == []
            open_gaps = catalog.unclosed_stream_discontinuities(
                market="um_perpetual", stream="book_ticker"
            )
            assert [cast(dict[str, Any], event["evidence"])["gap_id"] for event in open_gaps] == [
                gap_id
            ]
            recover_storage(layout=layout, catalog=catalog)
            await asyncio.sleep(0)
            assert loop_errors == []
            assert [
                pending
                for pending in asyncio.all_tasks()
                if pending not in baseline
                and pending is not asyncio.current_task()
                and not pending.done()
            ] == []
        finally:
            spool.release_operation.set()
            loop.set_exception_handler(previous_handler)
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            catalog.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("fail_write", [False, True])
def test_cancel_inside_owned_catalog_completion_waits_before_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fail_write: bool,
) -> None:
    gap_id = f"owned-completed-{'failure' if fail_write else 'success'}"
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as seed_catalog:
        record_gap_started(seed_catalog, gap_id=gap_id)

    async def exercise() -> None:
        @asynccontextmanager
        async def unused_opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BlockingSocket()

        collector, catalog = make_collector(
            tmp_path,
            opener=unused_opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            durability_interval_seconds=0.75,
        )
        write_started = threading.Event()
        release_write = threading.Event()
        close_started = threading.Event()
        write_active = threading.Event()
        close_raced = False
        original_record = catalog.record_operational_event
        original_close = catalog.close

        def blocking_record(
            *,
            event_id: str,
            event_type: str,
            occurred_at_utc_ns: int,
            evidence: Any,
        ) -> bool:
            if event_type != "STREAM_DISCONTINUITY_COMPLETED":
                return original_record(
                    event_id=event_id,
                    event_type=event_type,
                    occurred_at_utc_ns=occurred_at_utc_ns,
                    evidence=evidence,
                )
            write_active.set()
            write_started.set()
            try:
                if not release_write.wait(timeout=3):
                    raise TimeoutError("test did not release Catalog COMPLETED write")
                if fail_write:
                    raise OSError("injected Catalog COMPLETED failure")
                return original_record(
                    event_id=event_id,
                    event_type=event_type,
                    occurred_at_utc_ns=occurred_at_utc_ns,
                    evidence=evidence,
                )
            finally:
                write_active.clear()

        def tracked_close() -> None:
            nonlocal close_raced
            close_started.set()
            close_raced = write_active.is_set()
            original_close()

        monkeypatch.setattr(catalog, "record_operational_event", blocking_record)
        monkeypatch.setattr(catalog, "close", tracked_close)
        frame = ReceivedFrame(
            raw_payload=book_ticker(402),
            connection_id="new-owned-completed",
            receive_time_utc_ns=402,
            receive_monotonic_ns=402,
            capture_flags=("sequence_gap",),
        )
        writer_task = await run_writer_with_one_frame(collector, frame)

        async def own_writer_then_close() -> None:
            try:
                await writer_task
            finally:
                catalog.close()

        owner_task = asyncio.create_task(own_writer_then_close())
        try:
            await wait_for_thread_event(write_started)
            owner_task.cancel()
            await asyncio.sleep(0.02)
            assert owner_task.done() is False
            assert close_started.is_set() is False
            assert write_active.is_set() is True

            release_write.set()
            if fail_write:
                with pytest.raises(OSError, match="injected Catalog COMPLETED failure"):
                    await asyncio.wait_for(owner_task, timeout=1)
            else:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(owner_task, timeout=1)
            assert close_started.is_set() is True
            assert close_raced is False
        finally:
            release_write.set()
            if not owner_task.done():
                owner_task.cancel()
                await asyncio.gather(owner_task, return_exceptions=True)

    asyncio.run(exercise())
    with Catalog(layout.catalog) as reopened:
        completed = reopened.operational_events(
            event_type="STREAM_DISCONTINUITY_COMPLETED"
        )
        assert len(completed) == (0 if fail_write else 1)
        open_gaps = reopened.unclosed_stream_discontinuities(
            market="um_perpetual", stream="book_ticker"
        )
        assert len(open_gaps) == (1 if fail_write else 0)


@pytest.mark.parametrize("fail_write", [False, True])
def test_cancel_inside_owned_catalog_started_waits_and_is_restart_explainable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fail_write: bool,
) -> None:
    layout = ensure_storage_layout(tmp_path)

    async def exercise() -> str | None:
        @asynccontextmanager
        async def unused_opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BlockingSocket()

        collector, catalog = make_collector(
            tmp_path,
            opener=unused_opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            durability_interval_seconds=0.75,
        )
        write_started = threading.Event()
        release_write = threading.Event()
        close_started = threading.Event()
        write_active = threading.Event()
        close_raced = False
        original_record = catalog.record_operational_event
        original_close = catalog.close
        attempted_gap_id: str | None = None

        def blocking_record(
            *,
            event_id: str,
            event_type: str,
            occurred_at_utc_ns: int,
            evidence: Any,
        ) -> bool:
            nonlocal attempted_gap_id
            if event_type != "STREAM_DISCONTINUITY_STARTED":
                return original_record(
                    event_id=event_id,
                    event_type=event_type,
                    occurred_at_utc_ns=occurred_at_utc_ns,
                    evidence=evidence,
                )
            attempted_gap_id = str(evidence["gap_id"])
            write_active.set()
            write_started.set()
            try:
                if not release_write.wait(timeout=3):
                    raise TimeoutError("test did not release Catalog STARTED write")
                if fail_write:
                    raise OSError("injected Catalog STARTED failure")
                return original_record(
                    event_id=event_id,
                    event_type=event_type,
                    occurred_at_utc_ns=occurred_at_utc_ns,
                    evidence=evidence,
                )
            finally:
                write_active.clear()

        def tracked_close() -> None:
            nonlocal close_raced
            close_started.set()
            close_raced = write_active.is_set()
            original_close()

        monkeypatch.setattr(catalog, "record_operational_event", blocking_record)
        monkeypatch.setattr(catalog, "close", tracked_close)
        boundary = ReceivedFrame(
            raw_payload=book_ticker(403),
            connection_id="old-owned-started",
            receive_time_utc_ns=403,
            receive_monotonic_ns=403,
            capture_flags=("sequence_gap",),
        )

        async def record_then_close() -> None:
            try:
                await collector._record_gap_started(boundary)
            finally:
                catalog.close()

        task = asyncio.create_task(record_then_close())
        try:
            await wait_for_thread_event(write_started)
            task.cancel()
            await asyncio.sleep(0.02)
            assert task.done() is False
            assert close_started.is_set() is False
            assert write_active.is_set() is True
            release_write.set()
            if fail_write:
                with pytest.raises(OSError, match="injected Catalog STARTED failure"):
                    await asyncio.wait_for(task, timeout=1)
            else:
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=1)
            assert close_started.is_set() is True
            assert close_raced is False
            return attempted_gap_id
        finally:
            release_write.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    gap_id = asyncio.run(exercise())
    with Catalog(layout.catalog) as reopened:
        open_gaps = reopened.unclosed_stream_discontinuities(
            market="um_perpetual", stream="book_ticker"
        )
        assert len(open_gaps) == (0 if fail_write else 1)
        if fail_write:
            assert reopened.operational_events() == []
        else:
            assert cast(dict[str, Any], open_gaps[0]["evidence"])["gap_id"] == gap_id

            @asynccontextmanager
            async def unused_opener(_url: str) -> AsyncIterator[WebSocketConnection]:
                yield BlockingSocket()

            spool = StreamSpool(
                layout=layout,
                catalog=reopened,
                market="um_perpetual",
                symbol="BTCUSDT",
                stream="book_ticker",
                collector_instance_id="m21-4-started-restart",
                collector_version="0.1.0+test",
                queue_capacity=4,
                rotation=RotationPolicy(seconds=60),
                durability_interval_seconds=0.75,
                max_frame_bytes=1024 * 1024,
            )
            restarted = UsdMStreamCollector(
                stream=UsdMStream.BOOK_TICKER,
                route="public",
                wire_name="btcusdt@bookTicker",
                spool=spool,
                collector_instance_id="m21-4-started-restart",
                collector_version="0.1.0+test",
                logger=logging.getLogger("test.m21-4.started-restart"),
                opener=unused_opener,
            )
            assert restarted._pending_gap is not None
            assert restarted._pending_gap["gap_id"] == gap_id


@pytest.mark.parametrize("blocked_operation", ["drain", "seal"])
def test_cancel_inside_owned_raw_operation_never_overlaps_abort(
    tmp_path: Path,
    blocked_operation: str,
) -> None:
    async def exercise() -> None:
        collector, catalog, spool = make_blocking_writer_collector(
            tmp_path,
            blocked_operation=blocked_operation,
        )
        frame = ReceivedFrame(
            raw_payload=book_ticker(404),
            connection_id=f"owned-{blocked_operation}",
            receive_time_utc_ns=404,
            receive_monotonic_ns=404,
        )
        task = await run_writer_with_one_frame(collector, frame)
        try:
            await wait_for_thread_event(spool.operation_started)
            writer = spool._writer
            if blocked_operation == "seal":
                assert writer is not None
                os.fstat(writer._descriptor)
            task.cancel()
            await asyncio.sleep(0.02)
            assert task.done() is False
            assert spool.abort_started.is_set() is False
            assert spool.concurrent_abort is False
            if writer is not None:
                os.fstat(writer._descriptor)

            spool.release_operation.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=1)
            assert spool.abort_calls == 1
            assert spool.concurrent_abort is False
            if writer is not None:
                assert writer.closed is True
            recover_storage(layout=spool.layout, catalog=catalog)
        finally:
            spool.release_operation.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            catalog.close()

    asyncio.run(exercise())


def test_writer_integrity_failure_survives_cancel_during_owned_abort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        @asynccontextmanager
        async def unused_opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BlockingSocket()

        collector, catalog = make_collector(
            tmp_path,
            opener=unused_opener,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
            durability_interval_seconds=0.75,
        )
        abort_started = threading.Event()
        release_abort = threading.Event()
        original_abort = collector.spool.abort_writer

        def fail_drain() -> int:
            raise OSError("injected Writer integrity failure")

        def blocking_abort() -> None:
            abort_started.set()
            if not release_abort.wait(timeout=3):
                raise TimeoutError("test did not release owned abort")
            original_abort()

        monkeypatch.setattr(collector.spool, "drain_all", fail_drain)
        monkeypatch.setattr(collector.spool, "abort_writer", blocking_abort)
        task = await run_writer_with_one_frame(
            collector,
            ReceivedFrame(
                raw_payload=book_ticker(405),
                connection_id="writer-failure-during-cancel",
                receive_time_utc_ns=405,
                receive_monotonic_ns=405,
            ),
        )
        try:
            await wait_for_thread_event(abort_started)
            task.cancel()
            await asyncio.sleep(0.02)
            assert task.done() is False
            release_abort.set()
            with pytest.raises(OSError, match="injected Writer integrity failure"):
                await asyncio.wait_for(task, timeout=1)
        finally:
            release_abort.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            catalog.close()

    asyncio.run(exercise())


@pytest.mark.stress
def test_owned_blocking_worker_100_cancellation_races_do_not_leak(
    tmp_path: Path,
) -> None:
    fd_root = Path("/proc/self/fd")
    if not fd_root.exists():
        fd_root = Path("/dev/fd")
    fd_before = len(list(fd_root.iterdir()))
    threads_before = threading.active_count()

    async def exercise() -> None:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        loop = asyncio.get_running_loop()
        loop_errors: list[dict[str, Any]] = []
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
        baseline = set(asyncio.all_tasks())
        try:
            for iteration in range(100):
                spool = StreamSpool(
                    layout=layout,
                    catalog=catalog,
                    market="um_perpetual",
                    symbol="BTCUSDT",
                    stream="book_ticker",
                    collector_instance_id="m21-4-cancel-race",
                    collector_version="0.1.0+test",
                    queue_capacity=4,
                    rotation=RotationPolicy(seconds=60),
                    durability_interval_seconds=0.75,
                    max_frame_bytes=1024 * 1024,
                )
                operation_kind = iteration % 4
                if operation_kind in {0, 2, 3}:
                    payload_id = 5_000 + iteration
                    spool.enqueue(
                        envelope_from_websocket_frame(
                            raw_payload=book_ticker(payload_id),
                            stream=UsdMStream.BOOK_TICKER,
                            connection_id=f"cancel-race-{iteration}",
                            collector_instance_id="m21-4-cancel-race",
                            collector_version="0.1.0+test",
                            receive_time_utc_ns=payload_id,
                            receive_monotonic_ns=payload_id,
                        )
                    )
                    assert spool.drain_all() == 1

                entered = threading.Event()
                release = threading.Event()
                if operation_kind == 0:
                    blocking_operation: Callable[[], object] = spool.sync
                elif operation_kind == 1:

                    def record_event(iteration: int = iteration) -> object:
                        return catalog.record_operational_event(
                            event_id=f"owned-cancel-race:{iteration}",
                            event_type="OWNED_CANCEL_RACE",
                            occurred_at_utc_ns=iteration,
                            evidence={"iteration": iteration},
                        )

                    blocking_operation = record_event
                else:
                    blocking_operation = spool.close_and_seal

                def hold_worker(
                    entered: threading.Event = entered,
                    release: threading.Event = release,
                    operation: Callable[[], object] = blocking_operation,
                ) -> object:
                    entered.set()
                    if not release.wait(timeout=3):
                        raise TimeoutError("test did not release cancellation race")
                    return operation()

                worker_owner = asyncio.create_task(
                    _run_owned_blocking_call(hold_worker)
                )
                await wait_for_thread_event(entered)
                if operation_kind != 3:
                    worker_owner.cancel()
                    await asyncio.sleep(0)
                    assert worker_owner.done() is False
                release.set()
                if operation_kind == 3:
                    await asyncio.wait_for(worker_owner, timeout=1)
                else:
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.wait_for(worker_owner, timeout=1)

                if operation_kind == 0 or spool._writer is not None:
                    spool.abort_writer()

            recover_storage(layout=layout, catalog=catalog)
            assert len(
                catalog.operational_events(event_type="OWNED_CANCEL_RACE")
            ) == 25
            await asyncio.sleep(0)
            assert loop_errors == []
            assert [
                task
                for task in asyncio.all_tasks()
                if task not in baseline
                and task is not asyncio.current_task()
                and not task.done()
            ] == []
        finally:
            loop.set_exception_handler(previous_handler)
            catalog.close()

    asyncio.run(exercise())
    gc.collect()
    assert threading.active_count() <= threads_before + 1
    assert len(list(fd_root.iterdir())) <= fd_before + 2
