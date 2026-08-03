from __future__ import annotations

import asyncio
import io
import json
import logging
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard

import binance_market_data_recorder.spool.stream as stream_module
from binance_market_data_recorder.binance.spot.websocket import ReconnectBackoff
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.collector.supervisor import MarketCollectorSupervisor
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
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
        durability_interval_seconds=0,
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
        backpressure_persist_timeout_seconds=0.5,
    )
    return collector, catalog


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
            await asyncio.sleep(0.02)
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
