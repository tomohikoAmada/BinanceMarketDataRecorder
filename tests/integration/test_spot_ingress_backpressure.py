from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard

from binance_market_data_recorder.binance.spot.schema import SpotStream
from binance_market_data_recorder.binance.spot.websocket import (
    ReconnectBackoff,
    SpotStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
from binance_market_data_recorder.spool.queue import IngressPostCloseHandoffTimeout
from binance_market_data_recorder.spool.recovery import recover_storage
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
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


class DelayedStreamSpool(StreamSpool):
    def __init__(self, *args: Any, drain_delay_seconds: float, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.drain_delay_seconds = drain_delay_seconds

    def drain_all(self) -> int:
        if self.drain_delay_seconds:
            time.sleep(self.drain_delay_seconds)
        return super().drain_all()


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
            "M": True,
        },
        separators=(",", ":"),
    ).encode()


def diff_depth(update_id: int) -> bytes:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": update_id,
            "s": "BTCUSDT",
            "U": update_id,
            "u": update_id,
            "b": [],
            "a": [],
        },
        separators=(",", ":"),
    ).encode()


PayloadFactory = Callable[[int], bytes]


def make_collector(
    root: Path,
    *,
    opener: Any,
    stream: SpotStream,
    capacity: int,
    drain_delay_seconds: float,
    put_timeout_seconds: float,
    saturation_timeout_seconds: float,
    post_close_handoff_timeout_seconds: float = 0.5,
    lifecycle_observer: Callable[[str], None] | None = None,
) -> tuple[SpotStreamCollector, Catalog]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = DelayedStreamSpool(
        layout=layout,
        catalog=catalog,
        market="spot",
        symbol="BTCUSDT",
        stream=stream.value,
        collector_instance_id="spot-ingress-test",
        collector_version="0.1.0+test",
        queue_capacity=max(4, capacity),
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
        drain_delay_seconds=drain_delay_seconds,
    )
    wire_name = {
        SpotStream.DIFF_DEPTH: "btcusdt@depth@100ms",
        SpotStream.AGG_TRADE: "btcusdt@aggTrade",
        SpotStream.BOOK_TICKER: "btcusdt@bookTicker",
    }[stream]
    collector = SpotStreamCollector(
        stream=stream,
        wire_name=wire_name,
        spool=spool,
        collector_instance_id="spot-ingress-test",
        collector_version="0.1.0+test",
        logger=logging.getLogger("test.spot.ingress"),
        receipt_queue_capacity=capacity,
        planned_rotation_seconds=60,
        backoff=ReconnectBackoff(
            initial_seconds=0.001,
            maximum_seconds=0.001,
            jitter_ratio=0,
        ),
        opener=opener,
        lifecycle_observer=lifecycle_observer,
        backpressure_put_timeout_seconds=put_timeout_seconds,
        backpressure_saturation_timeout_seconds=saturation_timeout_seconds,
        post_close_handoff_timeout_seconds=post_close_handoff_timeout_seconds,
    )
    return collector, catalog


def manifest_envelopes(root: Path, manifest: dict[str, Any]) -> list[Any]:
    raw = zstandard.ZstdDecompressor().decompress(
        (root / manifest["relative_path"]).read_bytes()
    )
    source = io.BytesIO(raw)
    decode_chunk_header(source)
    envelopes: list[Any] = []
    while prefix := source.read(FRAME_PREFIX.size):
        length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
        envelopes.append(decode_envelope(source.read(length)))
    return envelopes


def captured(root: Path) -> tuple[list[Any], list[dict[str, Any]]]:
    documents = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (root / "data/manifests").glob("*.json")
    ]
    documents.sort(key=lambda item: int(item["created_at_utc_ns"]))
    envelopes: list[Any] = []
    for document in documents:
        envelopes.extend(manifest_envelopes(root, document))
    return envelopes, documents


def assert_old_ingress_boundary_layout(
    root: Path,
    manifests: list[dict[str, Any]],
    *,
    source_payloads: list[bytes],
    original_connection_id: str,
) -> list[bytes]:
    reconnect_manifests = [
        document
        for document in manifests
        if "reconnect_gap" in document["capture_flags"]
    ]
    assert len(reconnect_manifests) == 1
    reconnect_manifest = reconnect_manifests[0]
    reconnect_frames = manifest_envelopes(root, reconnect_manifest)
    assert reconnect_manifest["record_count"] == len(reconnect_frames)
    assert reconnect_manifest["gap"] is True
    assert reconnect_manifest["complete"] is False
    assert reconnect_manifest["record_count"] == 0 or reconnect_frames
    if reconnect_frames:
        assert all(
            frame.connection_id == original_connection_id
            for frame in reconnect_frames
        )
        assert set(reconnect_manifest["connection_ids"]) == {
            original_connection_id
        }
    else:
        assert reconnect_manifest["record_count"] == 0
        assert reconnect_manifest["connection_ids"] == []

    ordinary_manifests = [
        document for document in manifests if document is not reconnect_manifest
    ]
    assert len(ordinary_manifests) <= 1
    assert all(
        "sequence_gap" not in document["capture_flags"] for document in manifests
    )
    if ordinary_manifests:
        ordinary_manifest = ordinary_manifests[0]
        ordinary_index = next(
            index
            for index, document in enumerate(manifests)
            if document is ordinary_manifest
        )
        reconnect_index = next(
            index
            for index, document in enumerate(manifests)
            if document is reconnect_manifest
        )
        assert ordinary_index < reconnect_index
        ordinary_frames = manifest_envelopes(root, ordinary_manifest)
        assert ordinary_manifest["record_count"] == len(ordinary_frames)
        assert ordinary_manifest["record_count"] > 0
        assert "reconnect_gap" not in ordinary_manifest["capture_flags"]
        assert "sequence_gap" not in ordinary_manifest["capture_flags"]
        assert ordinary_manifest["gap"] is False
        assert ordinary_manifest["complete"] is True
        assert all(
            frame.connection_id == original_connection_id
            for frame in ordinary_frames
        )
        assert set(ordinary_manifest["connection_ids"]) == {
            original_connection_id
        }

    ordered_frames: list[Any] = []
    for document in manifests:
        frames = manifest_envelopes(root, document)
        assert document["record_count"] == len(frames)
        assert all("sequence_gap" not in frame.capture_flags for frame in frames)
        ordered_frames.extend(frames)
    old_payloads = [frame.raw_payload for frame in ordered_frames]
    assert old_payloads
    assert old_payloads == source_payloads[: len(old_payloads)]
    return old_payloads


def discontinuities(catalog: Catalog) -> list[dict[str, Any]]:
    return [
        event
        for event in catalog.operational_events()
        if str(event["event_type"]).startswith("STREAM_DISCONTINUITY")
    ]


async def wait_until_receipt_put_is_blocked(
    collector: SpotStreamCollector,
) -> None:
    while collector._receipts._saturation_started_ns is None:
        await asyncio.sleep(0)


def test_transient_saturation_is_lossless_ordered_and_gap_free(tmp_path: Path) -> None:
    async def exercise() -> int:
        stop = asyncio.Event()
        socket = BurstSocket([book_ticker(value) for value in range(64)], stop=stop)

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=4,
            drain_delay_seconds=0.005,
            put_timeout_seconds=0.2,
            saturation_timeout_seconds=1,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
            return collector.receipt_queue_stats.wait_count
        finally:
            catalog.close()

    wait_count = asyncio.run(exercise())
    envelopes, manifests = captured(tmp_path)
    assert wait_count > 0
    assert [event.raw_payload for event in envelopes] == [
        book_ticker(value) for value in range(64)
    ]
    assert all("sequence_gap" not in event.capture_flags for event in envelopes)
    assert all(document["gap"] is False for document in manifests)
    assert all(document["complete"] is True for document in manifests)


@pytest.mark.parametrize(
    "stream,payload_factory",
    [
        (SpotStream.BOOK_TICKER, book_ticker),
        (SpotStream.AGG_TRADE, agg_trade),
    ],
)
def test_sustained_saturation_preserves_boundary_and_publishes_gap(
    tmp_path: Path,
    stream: SpotStream,
    payload_factory: PayloadFactory,
) -> None:
    async def exercise() -> list[BurstSocket]:
        stop = asyncio.Event()
        attempts = 0
        sockets: list[BurstSocket] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            messages = (
                [payload_factory(value) for value in range(500)]
                if attempts == 1
                else [payload_factory(10_000)]
            )
            socket = BurstSocket(messages, stop=stop if attempts == 2 else None)
            sockets.append(socket)
            yield socket

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=stream,
            capacity=2,
            drain_delay_seconds=0.05,
            put_timeout_seconds=0.005,
            saturation_timeout_seconds=0.02,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
            return sockets
        finally:
            catalog.close()

    sockets = asyncio.run(exercise())
    envelopes, manifests = captured(tmp_path)
    payloads = [event.raw_payload for event in envelopes]
    first_new = payloads.index(payload_factory(10_000))
    assert payloads[:first_new] == [payload_factory(value) for value in range(first_new)]
    assert payloads[first_new - 1] == payload_factory(first_new - 1)
    assert envelopes[first_new - 1].capture_flags == ("sequence_gap",)
    assert envelopes[first_new].capture_flags == ("sequence_gap",)
    assert sockets[0].close_reasons == ["bounded ingress backpressure"]
    assert len({str(event.connection_id) for event in envelopes[:first_new]}) == 1
    assert len({str(event.connection_id) for event in envelopes[first_new:]}) == 1
    assert str(envelopes[first_new - 1].connection_id) != str(
        envelopes[first_new].connection_id
    )

    gap_manifests = [document for document in manifests if document["gap"]]
    assert gap_manifests
    assert all(document["complete"] is False for document in gap_manifests)
    assert all(
        "sequence_gap" in document["capture_flags"] for document in gap_manifests
    )
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        events = discontinuities(catalog)
    assert [event["event_type"] for event in events] == [
        "STREAM_DISCONTINUITY_STARTED",
        "STREAM_DISCONTINUITY_COMPLETED",
    ]
    started = cast(dict[str, Any], events[0]["evidence"])
    completed = cast(dict[str, Any], events[1]["evidence"])
    assert started["reason"] == "ingress_backpressure"
    assert started["boundary_frame_persisted"] is True
    assert started["boundary_payload_sha256"] == hashlib.sha256(
        envelopes[first_new - 1].raw_payload
    ).hexdigest()
    assert completed["raw_gap_marker"] == "sequence_gap"
    assert completed["historical_continuity_restored"] is False
    assert completed["gap_id"] == started["gap_id"]


def test_diff_depth_saturation_retains_market_resync_ownership(tmp_path: Path) -> None:
    async def exercise() -> list[str]:
        stop = asyncio.Event()
        lifecycle: list[str] = []
        opened = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal opened
            opened += 1
            yield BurstSocket([diff_depth(value) for value in range(500)])

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.DIFF_DEPTH,
            capacity=2,
            drain_delay_seconds=0.05,
            put_timeout_seconds=0.005,
            saturation_timeout_seconds=0.02,
            lifecycle_observer=lifecycle.append,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
            assert opened == 1
            return lifecycle
        finally:
            catalog.close()

    lifecycle = asyncio.run(exercise())
    assert lifecycle[-2:] == ["disconnected", "ingress_backpressure"]
    _envelopes, manifests = captured(tmp_path)
    assert any(document["gap"] and not document["complete"] for document in manifests)


def test_restart_restores_same_open_gap_and_completes_after_raw_sync(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    seed = Catalog(layout.catalog)
    gap_id = "spot-restart-gap"
    assert seed.record_operational_event(
        event_id=f"stream-discontinuity-started:{gap_id}",
        event_type="STREAM_DISCONTINUITY_STARTED",
        occurred_at_utc_ns=100,
        evidence={
            "gap_id": gap_id,
            "market": "spot",
            "stream": "book_ticker",
            "reason": "ingress_backpressure",
            "interval_classification": "UNRELIABLE",
            "gap_started_at_utc_ns": 100,
            "original_connection_id": "old-connection",
            "original_generation": 4,
            "boundary_frame_persisted": True,
        },
    )
    seed.close()

    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(10)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=1,
        )
        try:
            assert collector._pending_gap is not None
            assert collector._pending_gap["gap_id"] == gap_id
            assert collector._generation == 5
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(exercise())
    envelopes, manifests = captured(tmp_path)
    assert len(envelopes) == 1
    assert envelopes[0].capture_flags == ("sequence_gap",)
    assert manifests[0]["gap"] is True
    assert manifests[0]["complete"] is False
    with Catalog(layout.catalog, read_only=True) as catalog:
        events = discontinuities(catalog)
    assert [event["event_type"] for event in events] == [
        "STREAM_DISCONTINUITY_STARTED",
        "STREAM_DISCONTINUITY_COMPLETED",
    ]
    assert events[1]["evidence"]["gap_id"] == gap_id


def test_graceful_stop_without_overload_creates_no_false_gap(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([book_ticker(1), book_ticker(2)], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=4,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=1,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(exercise())
    _envelopes, manifests = captured(tmp_path)
    assert len(manifests) == 1
    assert manifests[0]["gap"] is False
    assert manifests[0]["complete"] is True
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        assert discontinuities(catalog) == []


def test_boundary_handoff_timeout_recovers_same_gap_without_fabricating_frame(
    tmp_path: Path,
) -> None:
    source_payloads = [book_ticker(value) for value in range(500)]

    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket(source_payloads)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=2,
            drain_delay_seconds=0.1,
            put_timeout_seconds=0.005,
            saturation_timeout_seconds=0.01,
            post_close_handoff_timeout_seconds=0.001,
        )
        try:
            with pytest.raises(IngressPostCloseHandoffTimeout):
                await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(exercise())
    old_envelopes, old_manifests = captured(tmp_path)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog, read_only=True) as catalog:
        events = discontinuities(catalog)
    assert len(events) == 1
    evidence = cast(dict[str, Any], events[0]["evidence"])
    gap_id = str(evidence["gap_id"])
    assert evidence["reason"] == "ingress_backpressure"
    assert evidence["boundary_kind"] == "last_frame_in_hand"
    assert evidence["boundary_frame_persisted"] is False
    old_payloads = assert_old_ingress_boundary_layout(
        tmp_path,
        old_manifests,
        source_payloads=source_payloads,
        original_connection_id=str(evidence["original_connection_id"]),
    )
    assert [envelope.raw_payload for envelope in old_envelopes] == old_payloads
    assert all("sequence_gap" not in envelope.capture_flags for envelope in old_envelopes)
    boundary_index = next(
        index
        for index, payload in enumerate(source_payloads)
        if hashlib.sha256(payload).hexdigest()
        == evidence["boundary_payload_sha256"]
    )
    assert boundary_index == len(old_payloads)
    assert source_payloads[boundary_index] not in old_payloads

    with Catalog(layout.catalog) as recovery_catalog:
        recover_storage(layout=layout, catalog=recovery_catalog)
        open_gaps = recovery_catalog.unclosed_stream_discontinuities(
            market="spot", stream="book_ticker"
        )
    assert len(open_gaps) == 1
    assert cast(dict[str, Any], open_gaps[0]["evidence"])["gap_id"] == gap_id

    replacement_payload = book_ticker(10_000)

    async def restart() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([replacement_payload], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )
        try:
            assert collector._pending_gap is not None
            assert collector._pending_gap["gap_id"] == gap_id
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(restart())

    all_envelopes, all_manifests = captured(tmp_path)
    assert [envelope.raw_payload for envelope in all_envelopes] == [
        *old_payloads,
        replacement_payload,
    ]
    assert all_envelopes[-1].capture_flags == ("sequence_gap",)
    assert all_manifests[-1]["gap"] is True
    assert all_manifests[-1]["complete"] is False
    with Catalog(layout.catalog, read_only=True) as catalog:
        lifecycle = discontinuities(catalog)
    assert [event["event_type"] for event in lifecycle] == [
        "STREAM_DISCONTINUITY_STARTED",
        "STREAM_DISCONTINUITY_COMPLETED",
    ]
    completed = cast(dict[str, Any], lifecycle[1]["evidence"])
    assert completed["gap_id"] == gap_id
    assert completed["raw_gap_marker"] == "sequence_gap"
    assert completed["historical_continuity_restored"] is False


def test_session_restart_post_close_timeout_recovers_same_gap_without_fabrication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_payloads = [book_ticker(value) for value in range(500)]

    async def fail_handoff() -> None:
        stop = asyncio.Event()
        session_restart = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket(source_payloads)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=2,
            drain_delay_seconds=0.1,
            put_timeout_seconds=1,
            saturation_timeout_seconds=1,
            post_close_handoff_timeout_seconds=0.001,
        )

        async def trigger_restart() -> None:
            await wait_until_receipt_put_is_blocked(collector)
            session_restart.set()
            stop.set()

        trigger = asyncio.create_task(trigger_restart())
        try:
            with pytest.raises(IngressPostCloseHandoffTimeout):
                await asyncio.wait_for(
                    collector.run(stop, session_restart), timeout=5
                )
            await asyncio.wait_for(trigger, timeout=1)
        finally:
            if not trigger.done():
                trigger.cancel()
                await asyncio.gather(trigger, return_exceptions=True)
            catalog.close()

    asyncio.run(fail_handoff())

    old_envelopes, old_manifests = captured(tmp_path)
    assert len(old_manifests) == 1
    assert old_manifests[0]["gap"] is True
    assert old_manifests[0]["complete"] is False
    assert "reconnect_gap" in old_manifests[0]["capture_flags"]
    old_payloads = [envelope.raw_payload for envelope in old_envelopes]
    assert old_payloads
    assert old_payloads == source_payloads[: len(old_payloads)]
    assert all("sequence_gap" not in envelope.capture_flags for envelope in old_envelopes)

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog, read_only=True) as catalog:
        lifecycle = discontinuities(catalog)
    assert [event["event_type"] for event in lifecycle] == [
        "STREAM_DISCONTINUITY_STARTED"
    ]
    started = cast(dict[str, Any], lifecycle[0]["evidence"])
    gap_id = str(started["gap_id"])
    assert started["reason"] == "session_restart"
    assert started["boundary_kind"] == "last_frame_in_hand"
    assert started["boundary_frame_persisted"] is False
    boundary_index = next(
        index
        for index, payload in enumerate(source_payloads)
        if hashlib.sha256(payload).hexdigest()
        == started["boundary_payload_sha256"]
    )
    assert boundary_index == len(old_payloads)
    assert source_payloads[boundary_index] not in old_payloads

    with Catalog(layout.catalog) as recovery_catalog:
        recover_storage(layout=layout, catalog=recovery_catalog)
        open_gaps = recovery_catalog.unclosed_stream_discontinuities(
            market="spot", stream="book_ticker"
        )
    assert len(open_gaps) == 1
    assert cast(dict[str, Any], open_gaps[0]["evidence"])["gap_id"] == gap_id

    replacement_payload = book_ticker(10_000)
    completed_after_raw_sync = False

    async def restart() -> None:
        nonlocal completed_after_raw_sync
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket([replacement_payload], stop=stop)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=2,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )
        original_sync = collector.spool.sync
        original_ensure = catalog.ensure_operational_event
        raw_synced = False

        def observed_sync() -> None:
            nonlocal raw_synced
            original_sync()
            raw_synced = True

        def observed_ensure(**kwargs: Any) -> bool:
            nonlocal completed_after_raw_sync
            if kwargs["event_type"] == "STREAM_DISCONTINUITY_COMPLETED":
                assert raw_synced is True
                completed_after_raw_sync = True
            return original_ensure(**kwargs)

        monkeypatch.setattr(collector.spool, "sync", observed_sync)
        monkeypatch.setattr(catalog, "ensure_operational_event", observed_ensure)
        try:
            assert collector._pending_gap is not None
            assert collector._pending_gap["gap_id"] == gap_id
            await asyncio.wait_for(collector.run(stop), timeout=5)
        finally:
            catalog.close()

    asyncio.run(restart())

    all_envelopes, all_manifests = captured(tmp_path)
    assert [envelope.raw_payload for envelope in all_envelopes] == [
        *old_payloads,
        replacement_payload,
    ]
    assert all_envelopes[-1].capture_flags == ("sequence_gap",)
    assert all_manifests[-1]["gap"] is True
    assert all_manifests[-1]["complete"] is False
    assert completed_after_raw_sync is True
    with Catalog(layout.catalog, read_only=True) as catalog:
        lifecycle = discontinuities(catalog)
    assert [event["event_type"] for event in lifecycle] == [
        "STREAM_DISCONTINUITY_STARTED",
        "STREAM_DISCONTINUITY_COMPLETED",
    ]
    completed = cast(dict[str, Any], lifecycle[1]["evidence"])
    assert completed["gap_id"] == gap_id
    assert completed["raw_gap_marker"] == "sequence_gap"
    assert completed["historical_continuity_restored"] is False


def test_global_stop_post_close_timeout_does_not_fabricate_reconnect_gap(
    tmp_path: Path,
) -> None:
    source_payloads = [book_ticker(value) for value in range(500)]

    async def exercise() -> None:
        stop = asyncio.Event()
        session_restart = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield BurstSocket(source_payloads)

        collector, catalog = make_collector(
            tmp_path,
            opener=opener,
            stream=SpotStream.BOOK_TICKER,
            capacity=2,
            drain_delay_seconds=0.1,
            put_timeout_seconds=1,
            saturation_timeout_seconds=1,
            post_close_handoff_timeout_seconds=0.001,
        )

        async def trigger_global_stop() -> None:
            await wait_until_receipt_put_is_blocked(collector)
            stop.set()

        trigger = asyncio.create_task(trigger_global_stop())
        try:
            with pytest.raises(IngressPostCloseHandoffTimeout):
                await asyncio.wait_for(
                    collector.run(stop, session_restart), timeout=5
                )
            await asyncio.wait_for(trigger, timeout=1)
        finally:
            if not trigger.done():
                trigger.cancel()
                await asyncio.gather(trigger, return_exceptions=True)
            catalog.close()

    asyncio.run(exercise())
    envelopes, manifests = captured(tmp_path)
    assert len(manifests) == 1
    assert manifests[0]["gap"] is False
    assert manifests[0]["complete"] is True
    assert [envelope.raw_payload for envelope in envelopes] == source_payloads[
        : len(envelopes)
    ]
    assert all("sequence_gap" not in envelope.capture_flags for envelope in envelopes)
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        assert discontinuities(catalog) == []
