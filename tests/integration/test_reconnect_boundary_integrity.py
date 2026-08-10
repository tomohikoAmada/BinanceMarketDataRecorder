from __future__ import annotations

import asyncio
import io
import json
import logging
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard
from websockets.exceptions import ConnectionClosedError

from binance_market_data_recorder.binance.spot.schema import SpotStream
from binance_market_data_recorder.binance.spot.websocket import (
    ReconnectBackoff,
    SpotStreamCollector,
)
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
    encode_frame,
)
from binance_market_data_recorder.spool.recovery import recover_storage
from binance_market_data_recorder.spool.seal import (
    OVERLAP_FLAG,
    RECONNECT_GAP_FLAG,
    seal_partial,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout


class ScriptedSocket:
    def __init__(
        self,
        messages: list[bytes],
        stop: asyncio.Event | None = None,
        *,
        error: Exception | None = None,
        block_on_exhaustion: bool = False,
    ) -> None:
        self.messages = iter(messages)
        self.stop = stop
        self.error = error
        self.block_on_exhaustion = block_on_exhaustion
        self.close_reasons: list[str] = []

    async def recv(self, decode: bool | None = None) -> bytes:
        try:
            return next(self.messages)
        except StopIteration:
            if self.block_on_exhaustion:
                if self.stop is not None:
                    self.stop.set()
                    await asyncio.Future[None]()
                await asyncio.Future[None]()
            if self.stop is not None:
                self.stop.set()
                await asyncio.Future[None]()
            raise (self.error or OSError("injected disconnect")) from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_reasons.append(reason)


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
    stream: UsdMStream = UsdMStream.BOOK_TICKER,
    lifecycle_observer: Callable[[str], None] | None = None,
) -> tuple[UsdMStreamCollector, Catalog, StreamSpool]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = StreamSpool(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream=stream.value,
        collector_instance_id="m21-4-11-test",
        collector_version="0.1.0+test",
        queue_capacity=32,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
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
        collector_instance_id="m21-4-11-test",
        collector_version="0.1.0+test",
        logger=logging.getLogger("test.m21-4-11"),
        receipt_queue_capacity=16,
        planned_rotation_seconds=60,
        backoff=ReconnectBackoff(
            initial_seconds=0.001,
            maximum_seconds=0.001,
            jitter_ratio=0,
        ),
        opener=opener,
        lifecycle_observer=lifecycle_observer,
    )
    return collector, catalog, spool


def manifests(root: Path) -> list[dict[str, Any]]:
    documents = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (root / "data" / "manifests").glob("*.json")
    ]
    documents.sort(key=lambda item: int(item["created_at_utc_ns"]))
    return documents


def envelopes(root: Path) -> list[Any]:
    result: list[Any] = []
    for document in manifests(root):
        raw = zstandard.ZstdDecompressor().decompress(
            (root / document["relative_path"]).read_bytes()
        )
        source = io.BytesIO(raw)
        decode_chunk_header(source)
        while prefix := source.read(FRAME_PREFIX.size):
            length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
            result.append(decode_envelope(source.read(length)))
    return result


def discontinuity_events(catalog: Catalog) -> list[dict[str, Any]]:
    return [
        event
        for event in catalog.operational_events()
        if str(event["event_type"]).startswith("STREAM_DISCONTINUITY")
    ]

def assert_boundary_contract(
    root: Path,
    catalog: Catalog,
    *,
    reason: str,
    old_frame_payloads: list[bytes],
    new_frame_payloads: list[bytes],
) -> None:
    """Shared A/B/C/D contract: generation isolation, forced gap, evidence."""
    documents = manifests(root)
    assert len(documents) == 2
    old_manifest, new_manifest = documents
    assert old_manifest["complete"] is False
    assert old_manifest["gap"] is True
    assert new_manifest["complete"] is False
    assert new_manifest["gap"] is True
    assert RECONNECT_GAP_FLAG in old_manifest["capture_flags"]
    assert "sequence_gap" in new_manifest["capture_flags"]
    assert set(old_manifest["connection_ids"]).isdisjoint(
        set(new_manifest["connection_ids"])
    )
    persisted = envelopes(root)
    assert [item.raw_payload for item in persisted] == [
        *old_frame_payloads,
        *new_frame_payloads,
    ]
    boundary = [item for item in persisted if "sequence_gap" in item.capture_flags]
    assert len(boundary) == 1
    assert boundary[0].raw_payload == new_frame_payloads[0]
    assert boundary[0].connection_id == new_manifest["connection_ids"][0]

    events = discontinuity_events(catalog)
    assert [event["event_type"] for event in events] == [
        "STREAM_DISCONTINUITY_STARTED",
        "STREAM_DISCONTINUITY_COMPLETED",
    ]
    started = cast(dict[str, Any], events[0]["evidence"])
    completed = cast(dict[str, Any], events[1]["evidence"])
    assert started["reason"] == reason
    assert started["interval_classification"] == "UNRELIABLE"
    assert started["boundary_kind"] == "no_last_frame_available"
    assert started["boundary_frame_persisted"] is False
    assert "boundary_payload_sha256" not in started
    assert started["original_connection_id"] == old_manifest["connection_ids"][0]
    assert completed["gap_id"] == started["gap_id"]
    assert completed["reason"] == reason
    assert completed["new_connection_id"] == new_manifest["connection_ids"][0]
    assert completed["new_generation"] == started["original_generation"] + 1
    assert completed["raw_gap_marker"] == "sequence_gap"
    assert completed["historical_continuity_restored"] is False


def test_unexpected_disconnect_book_ticker_gap_contract(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1), book_ticker(2)],
                    error=ConnectionClosedError(rcvd=None, sent=None, rcvd_then_sent=None),
                )
            else:
                yield ScriptedSocket([book_ticker(3)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        assert_boundary_contract(
            tmp_path,
            catalog,
            reason="unexpected_disconnect",
            old_frame_payloads=[book_ticker(1), book_ticker(2)],
            new_frame_payloads=[book_ticker(3)],
        )


def test_unexpected_disconnect_agg_trade_gap_contract(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([agg_trade(1)], error=OSError("injected disconnect"))
            else:
                yield ScriptedSocket([agg_trade(2)], stop=stop)

        collector, catalog, _spool = make_collector(
            tmp_path, opener=opener, stream=UsdMStream.AGG_TRADE
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        assert_boundary_contract(
            tmp_path,
            catalog,
            reason="unexpected_disconnect",
            old_frame_payloads=[agg_trade(1)],
            new_frame_payloads=[agg_trade(2)],
        )


def test_planned_rotation_book_ticker_gap_contract(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([book_ticker(1)], block_on_exhaustion=True)
            else:
                yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        collector.planned_rotation_seconds = 0.02
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        assert_boundary_contract(
            tmp_path,
            catalog,
            reason="planned_rotation",
            old_frame_payloads=[book_ticker(1)],
            new_frame_payloads=[book_ticker(2)],
        )


def test_planned_rotation_agg_trade_gap_contract(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([agg_trade(1)], block_on_exhaustion=True)
            else:
                yield ScriptedSocket([agg_trade(2)], stop=stop)

        collector, catalog, _spool = make_collector(
            tmp_path, opener=opener, stream=UsdMStream.AGG_TRADE
        )
        collector.planned_rotation_seconds = 0.02
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        assert_boundary_contract(
            tmp_path,
            catalog,
            reason="planned_rotation",
            old_frame_payloads=[agg_trade(1)],
            new_frame_payloads=[agg_trade(2)],
        )


def test_unexpected_disconnect_diff_depth_seals_gap_and_retires_session(
    tmp_path: Path,
) -> None:
    async def exercise() -> tuple[list[str], dict[str, Any]]:
        stop = asyncio.Event()
        attempts = 0
        lifecycle: list[str] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [diff_depth(1)], error=OSError("injected disconnect")
                )
            else:
                yield ScriptedSocket([diff_depth(2)], stop=stop)

        collector, catalog, _spool = make_collector(
            tmp_path,
            opener=opener,
            stream=UsdMStream.DIFF_DEPTH,
            lifecycle_observer=lifecycle.append,
        )
        try:
            # DIFF_DEPTH retires the session at a reconnect boundary; the
            # outer collector restarts it (second run() call, fresh snapshot
            # and bridge required before READY).
            await asyncio.wait_for(collector.run(stop), timeout=3)
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            started = cast(dict[str, Any], events[0]["evidence"])
            completed = cast(dict[str, Any], events[1]["evidence"])
            return lifecycle, {
                "started": started,
                "completed": completed,
            }
        finally:
            catalog.close()

    lifecycle, evidence = asyncio.run(exercise())
    assert "unexpected_disconnect" in lifecycle
    documents = manifests(tmp_path)
    assert len(documents) == 2
    assert documents[0]["complete"] is False and documents[0]["gap"] is True
    assert documents[1]["complete"] is False and documents[1]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
    assert "sequence_gap" in documents[1]["capture_flags"]
    assert evidence["started"]["reason"] == "unexpected_disconnect"
    assert evidence["started"]["boundary_frame_persisted"] is False
    assert evidence["completed"]["gap_id"] == evidence["started"]["gap_id"]
    assert evidence["completed"]["new_generation"] == evidence["started"][
        "original_generation"
    ] + 1
    assert evidence["completed"]["historical_continuity_restored"] is False


def test_planned_rotation_diff_depth_seals_gap_and_retires_session(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([diff_depth(1)], block_on_exhaustion=True)
            else:
                yield ScriptedSocket([diff_depth(2)], stop=stop)

        collector, catalog, _spool = make_collector(
            tmp_path, opener=opener, stream=UsdMStream.DIFF_DEPTH
        )
        collector.planned_rotation_seconds = 0.02
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()

    asyncio.run(exercise())


def test_repeated_connect_failures_before_first_new_frame_keep_one_gap(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([book_ticker(1)], error=OSError("A disconnected"))
            if attempts in (2, 3):
                raise OSError(f"connect attempt {attempts} failed")
            yield ScriptedSocket([book_ticker(4)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 4

    asyncio.run(exercise())
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        events = discontinuity_events(catalog)
        assert [event["event_type"] for event in events] == [
            "STREAM_DISCONTINUITY_STARTED",
            "STREAM_DISCONTINUITY_COMPLETED",
        ]
        started = cast(dict[str, Any], events[0]["evidence"])
        completed = cast(dict[str, Any], events[1]["evidence"])
        assert completed["gap_id"] == started["gap_id"]
        assert completed["new_generation"] == started["original_generation"] + 1
    documents = manifests(tmp_path)
    assert len(documents) == 2
    assert set(documents[0]["connection_ids"]).isdisjoint(
        set(documents[1]["connection_ids"])
    )
    assert "sequence_gap" in documents[1]["capture_flags"]


def test_crash_after_started_before_first_new_recovers_without_false_complete(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([book_ticker(1)], error=OSError("crash now"))
            # The process "crashes" before the replacement connection ever
            # delivers a frame: stop arrives while the queue is still empty.
            yield ScriptedSocket([], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED"
            ]
            assert len(
                catalog.unclosed_stream_discontinuities(
                    market="um_perpetual", stream="book_ticker"
                )
            ) == 1
        finally:
            catalog.close()

    asyncio.run(exercise())
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]

    async def recover() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            completed = cast(dict[str, Any], events[-1]["evidence"])
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    reopened_documents = manifests(tmp_path)
    assert len(reopened_documents) == 2
    assert reopened_documents[1]["complete"] is False
    assert reopened_documents[1]["gap"] is True
    assert "sequence_gap" in reopened_documents[1]["capture_flags"]
    assert completed["historical_continuity_restored"] is False
    assert completed["raw_gap_marker"] == "sequence_gap"


def test_completed_write_failure_after_first_new_is_fatal_and_recoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([book_ticker(1)], error=OSError("disconnect"))
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        original_record = catalog.record_operational_event

        def failing_record(
            *, event_id: str, event_type: str, occurred_at_utc_ns: int, evidence: Any
        ) -> bool:
            if event_type == "STREAM_DISCONTINUITY_COMPLETED":
                raise OSError("injected COMPLETED write failure")
            return original_record(
                event_id=event_id,
                event_type=event_type,
                occurred_at_utc_ns=occurred_at_utc_ns,
                evidence=evidence,
            )

        monkeypatch.setattr(catalog, "record_operational_event", failing_record)
        try:
            with pytest.raises(OSError, match="injected COMPLETED write failure"):
                await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED"
            ]
            assert len(catalog.unclosed_stream_discontinuities(
                market="um_perpetual", stream="book_ticker"
            )) == 1
            return cast(dict[str, Any], events[0]["evidence"])
        finally:
            catalog.close()

    started = asyncio.run(exercise())
    documents = manifests(tmp_path)
    # The first-new Raw frame was persisted into an active partial before the
    # COMPLETED write failed; the writer aborted without sealing, so startup
    # recovery owns the partial.
    assert len(documents) == 1
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
    layout = ensure_storage_layout(tmp_path)
    partials = list(layout.active.glob("*.bmdr.partial"))
    assert len(partials) == 1

    async def recover() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(3)], stop=stop)

        recover_storage(layout=layout, catalog=Catalog(layout.catalog))
        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
            completed = cast(dict[str, Any], events[1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    assert completed["historical_continuity_restored"] is False
    recovered_documents = manifests(tmp_path)
    # The crashed first-new Raw partial stays ACTIVE for later sealing; both
    # sealed manifests remain incomplete and carry explicit gap evidence.
    assert len(recovered_documents) == 2
    assert all(
        document["complete"] is False and document["gap"] is True
        for document in recovered_documents
    )
    assert "sequence_gap" in recovered_documents[1]["capture_flags"]


class FailingSealSpool(StreamSpool):
    def close_and_seal(
        self,
        forced_flags: frozenset[str] = frozenset(),
        seal_intent: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        raise OSError("injected seal failure during generation boundary")


def test_writer_seal_failure_during_generation_is_fatal_and_opens_no_connection(
    tmp_path: Path,
) -> None:
    async def exercise() -> int:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(1)], error=OSError("disconnect"))

        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        spool = FailingSealSpool(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-11-test",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        collector = UsdMStreamCollector(
            stream=UsdMStream.BOOK_TICKER,
            route="public",
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-11-test",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11"),
            opener=opener,
        )
        try:
            with pytest.raises(OSError, match="injected seal failure"):
                await asyncio.wait_for(collector.run(stop), timeout=3)
            return attempts
        finally:
            catalog.close()

    attempts = asyncio.run(exercise())
    assert attempts == 1


def test_graceful_shutdown_creates_no_false_reconnect_gap(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield ScriptedSocket([book_ticker(1), book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()

    asyncio.run(exercise())
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["complete"] is True
    assert documents[0]["gap"] is False
    with Catalog(tmp_path / "state/catalog.sqlite", read_only=True) as catalog:
        assert discontinuity_events(catalog) == []


def test_session_restart_boundary_is_not_a_false_gap_on_global_stop(
    tmp_path: Path,
) -> None:
    """Global stop must not create a gap even when restart signaling exists."""

    async def exercise() -> None:
        stop = asyncio.Event()
        session_restart = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield ScriptedSocket([book_ticker(1)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            # The restart latch is available but was never set: this stop is a
            # true global stop and must not produce reconnect-gap evidence.
            await asyncio.wait_for(collector.run(stop, session_restart), timeout=3)
            events = discontinuity_events(catalog)
            assert events == []
        finally:
            catalog.close()

    asyncio.run(exercise())
    documents = manifests(tmp_path)
    assert documents[0]["complete"] is True
    assert documents[0]["gap"] is False


def test_session_restart_boundary_records_gap_for_restarted_streams(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        session_restart = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket([book_ticker(1)], block_on_exhaustion=True)
            else:
                yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            def trigger_restart() -> None:
                session_restart.set()
                stop.set()

            asyncio.get_running_loop().call_later(0.05, trigger_restart)
            await asyncio.wait_for(
                collector.run(stop, session_restart), timeout=3
            )
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED"
            ]
            assert events[0]["evidence"]["reason"] == "session_restart"
            # The restarted session completes the pending gap on its first
            # reliable frame.
            stop.clear()
            session_restart.clear()
            await asyncio.wait_for(
                collector.run(stop, session_restart), timeout=3
            )
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
        finally:
            catalog.close()

    asyncio.run(exercise())
    documents = manifests(tmp_path)
    assert len(documents) == 2
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    assert documents[1]["complete"] is False
    assert documents[1]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
    assert "sequence_gap" in documents[1]["capture_flags"]


def test_blue_green_overlap_chunk_is_not_forced_incomplete(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="blue-green-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        from binance_market_data_recorder.binance.usdm.schema import (
            envelope_from_websocket_frame,
        )

        def envelope(connection_id: str, update_id: int, flags: tuple[str, ...]) -> Any:
            return envelope_from_websocket_frame(
                raw_payload=book_ticker(update_id),
                stream=UsdMStream.BOOK_TICKER,
                connection_id=connection_id,
                collector_instance_id="blue-green-test",
                collector_version="0.1.0+test",
                receive_time_utc_ns=1_000_000_000 + update_id,
                receive_monotonic_ns=update_id,
                additional_capture_flags=flags,
            )

        writer.append(envelope("active-conn", 1, (OVERLAP_FLAG, "deployment_id=x")))
        writer.append(envelope("candidate-conn", 2, (OVERLAP_FLAG, "deployment_id=x")))
        writer.close()
        manifest = cast(
            dict[str, Any], seal_partial(writer.path, layout=layout, catalog=catalog)
        )
    assert manifest["complete"] is True
    assert manifest["gap"] is False
    assert "blue_green_overlap" in manifest["capture_flags"]
    assert RECONNECT_GAP_FLAG not in manifest["capture_flags"]


def test_unmarked_connection_change_seal_fails_closed_to_incomplete(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="defense-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        from binance_market_data_recorder.binance.usdm.schema import (
            envelope_from_websocket_frame,
        )

        def envelope(connection_id: str, update_id: int) -> Any:
            return envelope_from_websocket_frame(
                raw_payload=book_ticker(update_id),
                stream=UsdMStream.BOOK_TICKER,
                connection_id=connection_id,
                collector_instance_id="defense-test",
                collector_version="0.1.0+test",
                receive_time_utc_ns=2_000_000_000 + update_id,
                receive_monotonic_ns=update_id,
            )

        writer.append(envelope("old-conn", 1))
        writer.append(envelope("new-conn", 2))
        writer.close()
        manifest = cast(
            dict[str, Any], seal_partial(writer.path, layout=layout, catalog=catalog)
        )
    assert manifest["complete"] is False
    assert manifest["gap"] is True
    assert RECONNECT_GAP_FLAG in manifest["capture_flags"]


def test_legacy_multiconnection_partial_recovers_as_forced_incomplete(
    tmp_path: Path,
) -> None:
    """Startup recovery of a legacy partial spanning connections fails closed.

    Startup coordination registers the clean partial as ACTIVE; the SEALING
    coordination path then seals it through ``seal_partial``, whose defense
    forces manifest-level reconnect_gap evidence instead of complete=true.
    """
    layout = ensure_storage_layout(tmp_path)
    catalog = Catalog(layout.catalog)
    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream="book_ticker",
        collector_instance_id="defense-recovery",
        collector_version="0.1.0+test",
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
    )
    from binance_market_data_recorder.binance.usdm.schema import (
        envelope_from_websocket_frame,
    )

    def envelope(connection_id: str, update_id: int) -> Any:
        return envelope_from_websocket_frame(
            raw_payload=book_ticker(update_id),
            stream=UsdMStream.BOOK_TICKER,
            connection_id=connection_id,
            collector_instance_id="defense-recovery",
            collector_version="0.1.0+test",
            receive_time_utc_ns=3_000_000_000 + update_id,
            receive_monotonic_ns=update_id,
        )

    writer.append(envelope("legacy-old", 1))
    writer.append(envelope("legacy-new", 2))
    writer.close()
    catalog.close()

    from binance_market_data_recorder.spool.recovery import recover_partials

    recovered_catalog = Catalog(layout.catalog)
    recover_partials(layout=layout, catalog=recovered_catalog)
    manifest = cast(
        dict[str, Any],
        seal_partial(writer.path, layout=layout, catalog=recovered_catalog),
    )
    recovered_catalog.close()
    assert manifest["complete"] is False
    assert manifest["gap"] is True
    assert RECONNECT_GAP_FLAG in manifest["capture_flags"]


class CrashDuringSealSpool(StreamSpool):
    """Enter Catalog SEALING and crash before any sealed artifact exists."""

    def close_and_seal(
        self,
        forced_flags: frozenset[str] = frozenset(),
        seal_intent: dict[str, object] | None = None,
    ) -> dict[str, object] | None:
        self.drain_all()
        writer = self._writer
        if writer is None:
            return None
        writer.close()
        self.catalog.transition(
            str(writer.header.chunk_id),
            ChunkState.SEALING,
            idempotency_key=f"sealing:{writer.header.chunk_id}",
            evidence={"verified_frames": writer.record_count},
        )
        raise OSError("injected crash during generation seal")


class FailBeforeSyncOnGapFrameWriter(RawChunkWriter):
    """Append the first-new sequence_gap frame, then crash before Raw sync."""

    def append(self, envelope: Any) -> int:
        if "sequence_gap" in envelope.capture_flags:
            frame = encode_frame(
                envelope, max_frame_bytes=self.header.max_frame_bytes
            )
            self._write_all(frame)
            self._record_count += 1
            raise OSError("injected crash before Raw sync of first-new frame")
        return super().append(envelope)


def make_usdm_crash_runner(
    root: Path,
    *,
    opener: Any,
    stream: UsdMStream = UsdMStream.BOOK_TICKER,
    spool_cls: type[StreamSpool] = CrashDuringSealSpool,
    writer_factory: Any = None,
) -> tuple[UsdMStreamCollector, Catalog]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = spool_cls(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream=stream.value,
        collector_instance_id="m21-4-11-crash",
        collector_version="0.1.0+test",
        queue_capacity=32,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
        **({"writer_factory": writer_factory} if writer_factory is not None else {}),
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
        collector_instance_id="m21-4-11-crash",
        collector_version="0.1.0+test",
        logger=logging.getLogger("test.m21-4-11-crash"),
        receipt_queue_capacity=16,
        planned_rotation_seconds=60,
        backoff=ReconnectBackoff(
            initial_seconds=0.001,
            maximum_seconds=0.001,
            jitter_ratio=0,
        ),
        opener=opener,
    )
    return collector, catalog


@pytest.mark.parametrize(
    ("stream", "frame_factory", "reason"),
    [
        (UsdMStream.BOOK_TICKER, book_ticker, "unexpected_disconnect"),
        (UsdMStream.AGG_TRADE, agg_trade, "unexpected_disconnect"),
    ],
)
def test_crash_during_seal_recovers_fail_closed_and_completes_same_gap(
    tmp_path: Path,
    stream: UsdMStream,
    frame_factory: Any,
    reason: str,
) -> None:
    """TEST-102: crash after Catalog SEALING but before the artifact/manifest.

    The durable STARTED intent recorded before the seal must survive: startup
    recovery seals the old partial with reconnect_gap (never complete=true),
    and the same gap_id completes on the replacement generation.
    """
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [frame_factory(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        collector, catalog = make_usdm_crash_runner(
            tmp_path, opener=opener, stream=stream
        )
        try:
            with pytest.raises(OSError, match="crash during generation seal"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED"
            ]
            return cast(dict[str, Any], events[0]["evidence"])
        finally:
            catalog.close()

    started = asyncio.run(crash())
    layout = ensure_storage_layout(tmp_path)
    assert len(manifests(tmp_path)) == 0
    partials = list(layout.active.glob("*.bmdr.partial"))
    assert len(partials) == 1

    async def recover() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([frame_factory(2)], stop=asyncio.Event())

        recover_storage(layout=layout, catalog=Catalog(layout.catalog))
        documents = manifests(tmp_path)
        assert len(documents) == 1
        assert documents[0]["complete"] is False
        assert documents[0]["gap"] is True
        assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
        assert (
            documents[0]["chunk_id"].replace("-", "")
            == partials[0].name.split(".")[0]
        )
        stop = asyncio.Event()
        collector, catalog = make_usdm_crash_runner(
            tmp_path,
            opener=opener,
            stream=stream,
            spool_cls=StreamSpool,
        )
        try:
            asyncio.get_running_loop().call_later(0.1, stop.set)
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
            completed = cast(dict[str, Any], events[1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    assert completed["new_generation"] == started["original_generation"] + 1
    assert completed["raw_gap_marker"] == "sequence_gap"
    assert completed["historical_continuity_restored"] is False
    reopened = manifests(tmp_path)
    assert len(reopened) == 2
    assert reopened[1]["complete"] is False
    assert "sequence_gap" in reopened[1]["capture_flags"]


def test_crash_before_seal_recovers_open_gap_and_completes_same_gap_id(
    tmp_path: Path,
) -> None:
    """TEST-101: crash after durable STARTED but before the old drain/seal."""
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        spool = FailingSealSpool(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-11-crash",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        collector = UsdMStreamCollector(
            stream=UsdMStream.BOOK_TICKER,
            route="public",
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-11-crash",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-crash"),
            opener=opener,
        )
        try:
            with pytest.raises(OSError, match="injected seal failure"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED"
            ]
            assert len(
                catalog.unclosed_stream_discontinuities(
                    market="um_perpetual", stream="book_ticker"
                )
            ) == 1
            return cast(dict[str, Any], events[0]["evidence"])
        finally:
            catalog.close()

    started = asyncio.run(crash())
    layout = ensure_storage_layout(tmp_path)
    # The old partial was drained but never sealed: Raw is preserved, nothing
    # falsely claims completeness.
    partials = list(layout.active.glob("*.bmdr.partial"))
    assert len(partials) == 1
    assert len(manifests(tmp_path)) == 0

    async def recover() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
            completed = cast(dict[str, Any], events[1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    assert completed["raw_gap_marker"] == "sequence_gap"
    reopened = manifests(tmp_path)
    assert len(reopened) == 1
    assert reopened[0]["complete"] is False
    assert "sequence_gap" in reopened[0]["capture_flags"]


def test_crash_after_artifact_before_manifest_recovers_with_forced_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-103: sealed .zst exists but the manifest write never happened."""
    import binance_market_data_recorder.spool.seal as seal_module

    def crash_manifest_write(path: Path, document: dict[str, object]) -> None:
        raise OSError("injected crash before manifest write")

    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        with monkeypatch.context() as context:
            context.setattr(seal_module, "_atomic_json", crash_manifest_write)
            collector, catalog = make_usdm_crash_runner(
                tmp_path, opener=opener, spool_cls=StreamSpool
            )
            try:
                with pytest.raises(OSError, match="before manifest write"):
                    await asyncio.wait_for(
                        collector.run(asyncio.Event()), timeout=3
                    )
                return cast(
                    dict[str, Any], discontinuity_events(catalog)[0]["evidence"]
                )
            finally:
                catalog.close()

    started = asyncio.run(crash())
    layout = ensure_storage_layout(tmp_path)
    sealed = list(layout.sealed.glob("*.bmdr.zst"))
    assert len(sealed) == 1
    assert len(list(layout.manifests.glob("*.json"))) == 0

    async def recover() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=asyncio.Event())

        recover_storage(layout=layout, catalog=Catalog(layout.catalog))
        documents = manifests(tmp_path)
        assert len(documents) == 1
        assert documents[0]["complete"] is False
        assert documents[0]["gap"] is True
        assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
        stop = asyncio.Event()
        collector, catalog = make_usdm_crash_runner(
            tmp_path, opener=opener, spool_cls=StreamSpool
        )
        try:
            asyncio.get_running_loop().call_later(0.1, stop.set)
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            completed = cast(dict[str, Any], events[-1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    assert completed["historical_continuity_restored"] is False
    reopened = manifests(tmp_path)
    assert len(reopened) == 2
    assert reopened[1]["complete"] is False
    assert "sequence_gap" in reopened[1]["capture_flags"]


def test_crash_after_manifest_before_catalog_sealed_recovers_gap_semantics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-104: manifest exists but the Catalog SEALED transition is absent."""
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        original_transition = catalog.transition

        def failing_transition(
            chunk_id: str, to_state: ChunkState, **kwargs: Any
        ) -> None:
            if to_state is ChunkState.SEALED:
                raise OSError("injected crash before Catalog SEALED")
            original_transition(chunk_id, to_state, **kwargs)

        monkeypatch.setattr(catalog, "transition", failing_transition)
        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-11-crash",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        collector = UsdMStreamCollector(
            stream=UsdMStream.BOOK_TICKER,
            route="public",
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-11-crash",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-crash"),
            opener=opener,
        )
        try:
            with pytest.raises(OSError, match="before Catalog SEALED"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            return cast(
                dict[str, Any], discontinuity_events(catalog)[0]["evidence"]
            )
        finally:
            catalog.close()

    started = asyncio.run(crash())
    layout = ensure_storage_layout(tmp_path)
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]

    async def recover() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=asyncio.Event())

        recover_storage(layout=layout, catalog=Catalog(layout.catalog))
        with Catalog(layout.catalog, read_only=True) as catalog:
            row = catalog.chunk(documents[0]["chunk_id"])
            assert row is not None
            assert ChunkState(str(row["state"])) is ChunkState.SEALED
        stop = asyncio.Event()
        collector, catalog = make_usdm_crash_runner(
            tmp_path, opener=opener, spool_cls=StreamSpool
        )
        try:
            asyncio.get_running_loop().call_later(0.1, stop.set)
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            completed = cast(dict[str, Any], events[-1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    assert completed["historical_continuity_restored"] is False
    reopened = manifests(tmp_path)
    assert len(reopened) == 2
    assert reopened[1]["complete"] is False
    assert "sequence_gap" in reopened[1]["capture_flags"]


def test_crash_after_old_seal_before_replacement_connection_recovers_same_gap(
    tmp_path: Path,
) -> None:
    """TEST-105: old generation fully sealed; crash before replacement opens."""
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise RuntimeError("injected crash before replacement connection")

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            with pytest.raises(RuntimeError, match="before replacement"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            return cast(
                dict[str, Any], discontinuity_events(catalog)[0]["evidence"]
            )
        finally:
            catalog.close()

    started = asyncio.run(crash())
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]

    async def recover() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            completed = cast(dict[str, Any], events[-1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    assert completed["raw_gap_marker"] == "sequence_gap"
    reopened = manifests(tmp_path)
    assert len(reopened) == 2
    assert reopened[1]["complete"] is False
    assert "sequence_gap" in reopened[1]["capture_flags"]


def test_crash_before_first_new_raw_sync_stays_pending_and_recovers(
    tmp_path: Path,
) -> None:
    """TEST-106: first-new Raw appended but not synced; restart stays pending."""
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            yield ScriptedSocket(
                [book_ticker(2)], error=OSError("crash before Raw sync")
            )

        collector, catalog = make_usdm_crash_runner(
            tmp_path,
            opener=opener,
            spool_cls=StreamSpool,
            writer_factory=FailBeforeSyncOnGapFrameWriter,
        )
        try:
            with pytest.raises(OSError, match="before Raw sync"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED"
            ]
            assert len(
                catalog.unclosed_stream_discontinuities(
                    market="um_perpetual", stream="book_ticker"
                )
            ) == 1
            return cast(dict[str, Any], events[0]["evidence"])
        finally:
            catalog.close()

    started = asyncio.run(crash())
    layout = ensure_storage_layout(tmp_path)
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    # The un-synced first-new partial is preserved, never falsely complete.
    assert len(list(layout.active.glob("*.bmdr.partial"))) >= 1

    async def recover() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(3)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            completed = cast(dict[str, Any], events[-1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    assert completed["raw_gap_marker"] == "sequence_gap"
    reopened = manifests(tmp_path)
    assert len(reopened) == 2
    assert reopened[1]["complete"] is False
    assert "sequence_gap" in reopened[1]["capture_flags"]


def test_restart_after_completed_gap_creates_no_duplicate_events(
    tmp_path: Path,
) -> None:
    """TEST-107/108: COMPLETED persists; restart adds no duplicate evidence."""
    async def exercise() -> list[dict[str, Any]]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("disconnect")
                )
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
            return events
        finally:
            catalog.close()

    asyncio.run(exercise())

    async def restart() -> None:
        stop = asyncio.Event()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield ScriptedSocket([book_ticker(3)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
        finally:
            catalog.close()

    asyncio.run(restart())
    persisted = envelopes(tmp_path)
    marked = [item for item in persisted if "sequence_gap" in item.capture_flags]
    assert len(marked) == 1
    documents = manifests(tmp_path)
    # The restarted generation sealed its own graceful chunk; no duplicate
    # gap evidence was created.
    assert len(documents) == 3
    assert documents[2]["complete"] is True
    assert documents[2]["gap"] is False


def test_spot_crash_during_seal_recovers_fail_closed(tmp_path: Path) -> None:
    """TEST-110: Spot crash-recovery across the reconnect-boundary seal."""
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        spool = CrashDuringSealSpool(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-11-crash",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        collector = SpotStreamCollector(
            stream=SpotStream.BOOK_TICKER,
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-11-crash",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-crash"),
            opener=opener,
        )
        try:
            with pytest.raises(OSError, match="crash during generation seal"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED"
            ]
            return cast(dict[str, Any], events[0]["evidence"])
        finally:
            catalog.close()

    started = asyncio.run(crash())
    layout = ensure_storage_layout(tmp_path)
    recover_storage(layout=layout, catalog=Catalog(layout.catalog))
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
    assert documents[0]["market"] == "spot"

    async def recover() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector = SpotStreamCollector(
            stream=SpotStream.BOOK_TICKER,
            wire_name="btcusdt@bookTicker",
            spool=StreamSpool(
                layout=layout,
                catalog=Catalog(layout.catalog),
                market="spot",
                symbol="BTCUSDT",
                stream="book_ticker",
                collector_instance_id="m21-4-11-crash",
                collector_version="0.1.0+test",
                queue_capacity=32,
                rotation=RotationPolicy(seconds=60),
                durability_interval_seconds=0,
                max_frame_bytes=1024 * 1024,
            ),
            collector_instance_id="m21-4-11-crash",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-crash"),
            opener=opener,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            with Catalog(layout.catalog, read_only=True) as catalog:
                events = discontinuity_events(catalog)
                completed = cast(dict[str, Any], events[-1]["evidence"])
                assert completed["gap_id"] == started["gap_id"]
                return completed
        finally:
            pass

    completed = asyncio.run(recover())
    assert completed["historical_continuity_restored"] is False


def test_diff_depth_crash_during_seal_recovers_and_retires_session(
    tmp_path: Path,
) -> None:
    """TEST-111: diff_depth crash/restart keeps gap evidence; session retires."""
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [diff_depth(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        collector, catalog = make_usdm_crash_runner(
            tmp_path, opener=opener, stream=UsdMStream.DIFF_DEPTH
        )
        try:
            with pytest.raises(OSError, match="crash during generation seal"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            return cast(
                dict[str, Any], discontinuity_events(catalog)[0]["evidence"]
            )
        finally:
            catalog.close()

    started = asyncio.run(crash())
    layout = ensure_storage_layout(tmp_path)
    recover_storage(layout=layout, catalog=Catalog(layout.catalog))
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["complete"] is False
    assert documents[0]["gap"] is True
    assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
    assert documents[0]["stream"] == "diff_depth"

    async def restart_session() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([diff_depth(2)], stop=stop)

        collector, catalog = make_usdm_crash_runner(
            tmp_path, opener=opener, stream=UsdMStream.DIFF_DEPTH,
            spool_cls=StreamSpool,
        )
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            with Catalog(layout.catalog, read_only=True) as catalog:
                events = discontinuity_events(catalog)
                completed = cast(dict[str, Any], events[-1]["evidence"])
                assert completed["gap_id"] == started["gap_id"]
                return completed
        finally:
            catalog.close()

    completed = asyncio.run(restart_session())
    assert completed["new_generation"] == started["original_generation"] + 1
    assert completed["historical_continuity_restored"] is False
    reopened = manifests(tmp_path)
    assert len(reopened) == 2
    assert reopened[1]["complete"] is False
    assert "sequence_gap" in reopened[1]["capture_flags"]


def test_overlap_at_one_transition_never_exempts_unmarked_third(
    tmp_path: Path,
) -> None:
    """TEST-601: A->B valid blue/green overlap, then B->C unmarked reconnect
    in the same chunk: B->C cannot seal trusted complete."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="defense-overlap",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        from binance_market_data_recorder.binance.usdm.schema import (
            envelope_from_websocket_frame,
        )

        def envelope(connection_id: str, update_id: int, flags: tuple[str, ...]) -> Any:
            return envelope_from_websocket_frame(
                raw_payload=book_ticker(update_id),
                stream=UsdMStream.BOOK_TICKER,
                connection_id=connection_id,
                collector_instance_id="defense-overlap",
                collector_version="0.1.0+test",
                receive_time_utc_ns=9_000_000_000 + update_id,
                receive_monotonic_ns=update_id,
                additional_capture_flags=flags,
            )

        writer.append(
            envelope("active-conn", 1, (OVERLAP_FLAG, "deployment_id=d"))
        )
        writer.append(
            envelope("candidate-conn", 2, (OVERLAP_FLAG, "deployment_id=d"))
        )
        writer.append(envelope("third-conn", 3, ()))
        writer.close()
        manifest = cast(
            dict[str, Any], seal_partial(writer.path, layout=layout, catalog=catalog)
        )
    assert manifest["complete"] is False
    assert manifest["gap"] is True
    assert RECONNECT_GAP_FLAG in manifest["capture_flags"]


def test_seal_rejects_existing_manifest_that_contradicts_reconnect_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REQ-107: an existing complete=true manifest must fail closed when the
    durable reconnect intent requires gap=true/complete=false; it is never
    silently adopted."""
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="defense-manifest",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        from binance_market_data_recorder.binance.usdm.schema import (
            envelope_from_websocket_frame,
        )

        def envelope(connection_id: str, update_id: int) -> Any:
            return envelope_from_websocket_frame(
                raw_payload=book_ticker(update_id),
                stream=UsdMStream.BOOK_TICKER,
                connection_id=connection_id,
                collector_instance_id="defense-manifest",
                collector_version="0.1.0+test",
                receive_time_utc_ns=9_000_000_000 + update_id,
                receive_monotonic_ns=update_id,
            )

        writer.append(envelope("only-conn", 1))
        writer.close()

        original_transition = catalog.transition

        def fail_sealed_transition(
            chunk_id: str, to_state: ChunkState, **kwargs: Any
        ) -> None:
            if to_state is ChunkState.SEALED:
                raise OSError("injected crash before Catalog SEALED")
            original_transition(chunk_id, to_state, **kwargs)

        monkeypatch.setattr(catalog, "transition", fail_sealed_transition)
        with pytest.raises(OSError, match="before Catalog SEALED"):
            seal_partial(
                writer.path,
                layout=layout,
                catalog=catalog,
                forced_flags=frozenset({RECONNECT_GAP_FLAG}),
            )
        monkeypatch.setattr(catalog, "transition", original_transition)

        manifest_path = (
            layout.manifests
            / f"{writer.header.chunk_id.hex}.manifest.json"
        )
        legacy = json.loads(manifest_path.read_text(encoding="utf-8"))
        legacy["capture_flags"] = []
        legacy["gap"] = False
        legacy["complete"] = True
        manifest_path.write_text(
            json.dumps(legacy, sort_keys=True) + "\n", encoding="utf-8"
        )
        from binance_market_data_recorder.spool.seal import SealError

        with pytest.raises(SealError, match="completeness semantics"):
            seal_partial(
                writer.path,
                layout=layout,
                catalog=catalog,
                forced_flags=frozenset({RECONNECT_GAP_FLAG}),
            )


class DrainHoldSpool(StreamSpool):
    """Hold the Raw writer drain once engaged, until the test releases it.

    Drains pass through while ``engage`` is clear, so ordinary generation
    seals are unaffected. When ``engage`` is set (before the held frame is
    accepted), every drain blocks until ``release`` is set, forcing the
    deterministic interleaving where the pending gap's first-new frame sits
    in the writer queue while the reconnect boundary decision runs.
    """

    def __init__(
        self,
        engage: threading.Event,
        release: threading.Event,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.engage = engage
        self.release = release

    def drain_all(self) -> int:
        if self.engage.is_set() and not self.release.wait(timeout=10):
            raise RuntimeError("test drain hold timed out")
        return super().drain_all()


class ErrorGateSocket(ScriptedSocket):
    """Deliver messages, then raise the error only after an external event."""

    def __init__(self, messages: list[bytes], error: Exception, gate: asyncio.Event) -> None:
        super().__init__(messages, error=error)
        self.gate = gate

    async def recv(self, decode: bool | None = None) -> bytes:
        try:
            return next(self.messages)
        except StopIteration:
            await self.gate.wait()
            raise (self.error or OSError("injected disconnect")) from None


@pytest.mark.parametrize(
    ("stream", "frame_factory"),
    [
        (UsdMStream.BOOK_TICKER, book_ticker),
        (UsdMStream.AGG_TRADE, agg_trade),
    ],
)
def test_drain_completing_gap_then_next_boundary_records_new_intent(
    tmp_path: Path,
    stream: UsdMStream,
    frame_factory: Any,
) -> None:
    """TEST-101-race: a gap completed during the boundary drain must not
    erase the next boundary's durable intent.

    Interleaving under review (M21.4.11-R1): the pre-seal intent decision
    sees the pending gap's first-new frame still in the writer queue and
    skips a new STARTED; the boundary drain then persists that frame, the
    COMPLETED pair fires, and the NEXT reconnect boundary would open a
    replacement generation with no durable intent and an unmarked first
    frame (INV-007/INV-009/INV-010). The drain is held until the boundary
    decision has run, then released; the replacement connection's first
    frame must still carry sequence_gap and a fresh STARTED/COMPLETED pair
    must exist.
    """
    engage = threading.Event()
    release = threading.Event()
    gate = asyncio.Event()

    async def scenario() -> dict[str, Any]:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        spool = DrainHoldSpool(
            engage=engage,
            release=release,
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream=stream.value,
            collector_instance_id="m21-4-11-race",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [frame_factory(1)], error=OSError("boundary-1")
                )
            if attempts == 2:
                engage.set()
                yield ErrorGateSocket(
                    [frame_factory(2)], OSError("boundary-2"), gate
                )
            yield ScriptedSocket(
                [frame_factory(3)], stop=stop, block_on_exhaustion=True
            )

        collector = UsdMStreamCollector(
            stream=stream,
            route="market" if stream == UsdMStream.AGG_TRADE else "public",
            wire_name={
                UsdMStream.AGG_TRADE: "btcusdt@aggTrade",
                UsdMStream.BOOK_TICKER: "btcusdt@bookTicker",
            }[stream],
            spool=spool,
            collector_instance_id="m21-4-11-race",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-race"),
            receipt_queue_capacity=16,
            planned_rotation_seconds=60,
            backoff=ReconnectBackoff(
                initial_seconds=0.001,
                maximum_seconds=0.001,
                jitter_ratio=0,
            ),
            opener=opener,
        )
        run_task = asyncio.create_task(collector.run(stop))
        try:
            # The second generation's first (marked) frame is now held in the
            # writer drain; the connection fails and the boundary decision
            # must run while the gap is still pending (no awaits between the
            # raise and the intent decision, so the hold release below always
            # happens after the decision).
            gate.set()
            await asyncio.sleep(0.2)
            release.set()
            deadline = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < deadline:
                if len(discontinuity_events(catalog)) >= 4:
                    break
                await asyncio.sleep(0.02)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
            started = cast(dict[str, Any], events[0]["evidence"])
            completed = cast(dict[str, Any], events[1]["evidence"])
            started_again = cast(dict[str, Any], events[2]["evidence"])
            completed_again = cast(dict[str, Any], events[3]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            assert started_again["gap_id"] != started["gap_id"]
            assert completed_again["gap_id"] == started_again["gap_id"]
            assert started_again["reason"] == "unexpected_disconnect"
            assert started_again["original_generation"] == (
                started["original_generation"] + 1
            )
            assert completed_again["new_generation"] == (
                started_again["original_generation"] + 1
            )
            return {
                "started_again": started_again,
                "completed_again": completed_again,
            }
        finally:
            stop.set()
            await asyncio.wait_for(run_task, timeout=5)
            catalog.close()

    evidence = asyncio.run(scenario())
    documents = manifests(tmp_path)
    assert len(documents) == 3
    _old, mid, new = documents
    assert mid["complete"] is False
    assert mid["gap"] is True
    assert RECONNECT_GAP_FLAG in mid["capture_flags"]
    assert "sequence_gap" in mid["capture_flags"]
    assert new["complete"] is False
    assert new["gap"] is True
    assert "sequence_gap" in new["capture_flags"]
    assert evidence["started_again"]["original_connection_id"] == mid[
        "connection_ids"
    ][0]
    assert evidence["completed_again"]["new_connection_id"] == new[
        "connection_ids"
    ][0]
    assert evidence["completed_again"]["historical_continuity_restored"] is False
    assert evidence["completed_again"]["raw_gap_marker"] == "sequence_gap"


def test_spot_drain_completing_gap_then_next_boundary_records_new_intent(
    tmp_path: Path,
) -> None:
    """Spot variant of the drain-completion race (TEST-110 companion)."""
    engage = threading.Event()
    release = threading.Event()
    gate = asyncio.Event()

    async def scenario() -> dict[str, Any]:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        spool = DrainHoldSpool(
            engage=engage,
            release=release,
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-11-race",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("boundary-1")
                )
            if attempts == 2:
                engage.set()
                yield ErrorGateSocket(
                    [book_ticker(2)], OSError("boundary-2"), gate
                )
            yield ScriptedSocket(
                [book_ticker(3)], stop=stop, block_on_exhaustion=True
            )

        collector = SpotStreamCollector(
            stream=SpotStream.BOOK_TICKER,
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-11-race",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-race"),
            opener=opener,
        )
        run_task = asyncio.create_task(collector.run(stop))
        try:
            gate.set()
            await asyncio.sleep(0.2)
            release.set()
            deadline = asyncio.get_running_loop().time() + 5
            while asyncio.get_running_loop().time() < deadline:
                if len(discontinuity_events(catalog)) >= 4:
                    break
                await asyncio.sleep(0.02)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
            started = cast(dict[str, Any], events[0]["evidence"])
            started_again = cast(dict[str, Any], events[2]["evidence"])
            completed_again = cast(dict[str, Any], events[3]["evidence"])
            assert started_again["gap_id"] != started["gap_id"]
            assert completed_again["gap_id"] == started_again["gap_id"]
            return {
                "started_again": started_again,
                "completed_again": completed_again,
            }
        finally:
            stop.set()
            await asyncio.wait_for(run_task, timeout=5)
            catalog.close()

    evidence = asyncio.run(scenario())
    documents = manifests(tmp_path)
    assert len(documents) == 3
    _old, mid, new = documents
    assert mid["complete"] is False
    assert mid["gap"] is True
    assert RECONNECT_GAP_FLAG in mid["capture_flags"]
    assert "sequence_gap" in mid["capture_flags"]
    assert new["complete"] is False
    assert new["gap"] is True
    assert "sequence_gap" in new["capture_flags"]
    assert evidence["started_again"]["original_connection_id"] == mid[
        "connection_ids"
    ][0]
    assert evidence["completed_again"]["new_connection_id"] == new[
        "connection_ids"
    ][0]


class IntentFailureCollector:
    """Build a USD-M collector whose STARTED write fails before committing.

    The durable seal intent must still be persisted into the ChunkState.SEALING
    transition evidence by the writer's seal_partial call (P1-A).
    """

    @staticmethod
    def build(
        root: Path,
        *,
        opener: Any,
        stream: UsdMStream = UsdMStream.BOOK_TICKER,
        spool_cls: type[StreamSpool] = StreamSpool,
    ) -> tuple[UsdMStreamCollector, Catalog, StreamSpool]:
        layout = ensure_storage_layout(root)
        catalog = Catalog(layout.catalog)
        spool = spool_cls(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream=stream.value,
            collector_instance_id="m21-4-11-r2",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
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
            collector_instance_id="m21-4-11-r2",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-r2"),
            receipt_queue_capacity=16,
            planned_rotation_seconds=60,
            backoff=ReconnectBackoff(
                initial_seconds=0.001,
                maximum_seconds=0.001,
                jitter_ratio=0,
            ),
            opener=opener,
        )
        return collector, catalog, spool


def fail_started_writes(
    catalog: Catalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Inject a STARTED write failure that happens BEFORE any commit."""

    original_record = catalog.record_operational_event

    def failing_record(
        *, event_id: str, event_type: str, occurred_at_utc_ns: int, evidence: Any
    ) -> bool:
        if event_type == "STREAM_DISCONTINUITY_STARTED":
            raise OSError("injected STARTED write failure")
        return original_record(
            event_id=event_id,
            event_type=event_type,
            occurred_at_utc_ns=occurred_at_utc_ns,
            evidence=evidence,
        )

    monkeypatch.setattr(catalog, "record_operational_event", failing_record)


def sealing_intent(catalog: Catalog, chunk_id: str) -> dict[str, Any]:
    evidence = catalog.latest_transition_evidence(chunk_id, ChunkState.SEALING)
    assert evidence is not None
    intent = evidence.get("seal_intent")
    assert isinstance(intent, dict)
    return intent


def assert_materialized_gap_recovery(
    root: Path,
    intent: dict[str, Any],
    *,
    expected_manifests: int,
) -> dict[str, Any]:
    """Restart with fresh Catalog/StreamSpool/collector (TEST-701/702).

    Startup recovery must reconstruct the pending discontinuity from the
    durable SEALING intent (same gap_id, never a new random one), the old
    generation must seal gap=true/complete=false, and the replacement
    generation must mark its first frame sequence_gap and close exactly one
    coherent COMPLETED pair (REQ-105/REQ-106).
    """
    layout = ensure_storage_layout(root)
    recovered_catalog = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered_catalog)
    recovered_catalog.close()
    assert any(
        action.action == "pending_discontinuity_materialized"
        for action in actions
    )
    documents = manifests(root)
    assert len(documents) == expected_manifests
    assert all(
        document["complete"] is False and document["gap"] is True
        for document in documents
    )
    assert RECONNECT_GAP_FLAG in documents[-1]["capture_flags"]

    async def recover() -> dict[str, Any]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(root, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            events = discontinuity_events(catalog)
            assert [event["event_type"] for event in events] == [
                "STREAM_DISCONTINUITY_STARTED",
                "STREAM_DISCONTINUITY_COMPLETED",
            ]
            started = cast(dict[str, Any], events[0]["evidence"])
            completed = cast(dict[str, Any], events[1]["evidence"])
            assert started["gap_id"] == intent["gap_id"]
            assert completed["gap_id"] == intent["gap_id"]
            assert completed["original_generation"] == intent[
                "original_generation"
            ]
            return completed
        finally:
            catalog.close()

    completed = asyncio.run(recover())
    reopened = manifests(root)
    assert len(reopened) == expected_manifests + 1
    assert reopened[-1]["complete"] is False
    assert reopened[-1]["gap"] is True
    assert "sequence_gap" in reopened[-1]["capture_flags"]
    assert completed["raw_gap_marker"] == "sequence_gap"
    assert completed["historical_continuity_restored"] is False
    return completed


def test_intent_failure_then_seal_success_restart_fail_closed_same_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-101: intent write fails BEFORE commit; the writer seal SUCCEEDS.

    The durable SEALING intent survives; restart reconstructs the pending
    discontinuity with the same gap_id, the old manifest stays
    gap=true/complete=false, and one coherent COMPLETED closes the gap.
    """
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        collector, catalog, _spool = IntentFailureCollector.build(
            tmp_path, opener=opener
        )
        fail_started_writes(catalog, monkeypatch)
        try:
            with pytest.raises(OSError, match="injected STARTED write failure") as raised:
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            # The writer seal itself succeeded: no writer failure to chain.
            assert raised.value.__cause__ is None
            assert discontinuity_events(catalog) == []
            documents = manifests(tmp_path)
            assert len(documents) == 1
            assert documents[0]["gap"] is True
            assert documents[0]["complete"] is False
            assert RECONNECT_GAP_FLAG in documents[0]["capture_flags"]
            return sealing_intent(catalog, documents[0]["chunk_id"])
        finally:
            catalog.close()

    intent = asyncio.run(crash())
    assert_materialized_gap_recovery(tmp_path, intent, expected_manifests=1)


def test_intent_failure_then_seal_crash_after_sealing_commit_restart_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-102 (exact blocking regression): intent write fails BEFORE commit
    AND the seal fails immediately AFTER the SEALING transition commits.

    Restart must seal the old partial gap=true/complete=false (never
    complete=true), keep one coherent gap identity, mark the replacement
    first frame sequence_gap, and later close exactly one COMPLETED.
    """
    import binance_market_data_recorder.spool.seal as seal_module

    original_compress = seal_module._compress

    def crash_after_sealing(
        source_path: Any, target_partial: Any, source_size: int
    ) -> None:
        raise OSError("injected crash after SEALING transition commit")

    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        with monkeypatch.context() as context:
            context.setattr(seal_module, "_compress", crash_after_sealing)
            collector, catalog, _spool = IntentFailureCollector.build(
                tmp_path, opener=opener
            )
            fail_started_writes(catalog, monkeypatch)
            try:
                with pytest.raises(
                    OSError, match="injected STARTED write failure"
                ) as raised:
                    await asyncio.wait_for(
                        collector.run(asyncio.Event()), timeout=3
                    )
                # REQ-109: the writer failure must not disappear; it is
                # retained as the cause of the propagated intent failure.
                assert raised.value.__cause__ is not None
                assert "SEALING transition commit" in str(raised.value.__cause__)
                assert discontinuity_events(catalog) == []
                layout = ensure_storage_layout(tmp_path)
                partials = list(layout.active.glob("*.bmdr.partial"))
                assert len(partials) == 1
                chunk_id = str(uuid.UUID(partials[0].name.split(".")[0]))
                row = catalog.chunk(chunk_id)
                assert row is not None
                assert ChunkState(str(row["state"])) is ChunkState.SEALING
                return sealing_intent(catalog, chunk_id)
            finally:
                catalog.close()

    intent = asyncio.run(crash())
    monkeypatch.setattr(seal_module, "_compress", original_compress)
    assert_materialized_gap_recovery(tmp_path, intent, expected_manifests=1)


def test_intent_failure_then_sealed_artifact_before_manifest_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-103: intent write fails; the sealed compressed artifact exists but
    the manifest write never happened. Restart reconstructs the intent."""
    import binance_market_data_recorder.spool.seal as seal_module

    original_write = seal_module._atomic_json

    def crash_manifest_write(path: Path, document: dict[str, object]) -> None:
        raise OSError("injected crash before manifest write")

    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        with monkeypatch.context() as context:
            context.setattr(seal_module, "_atomic_json", crash_manifest_write)
            collector, catalog, _spool = IntentFailureCollector.build(
                tmp_path, opener=opener
            )
            fail_started_writes(catalog, monkeypatch)
            try:
                with pytest.raises(
                    OSError, match="injected STARTED write failure"
                ):
                    await asyncio.wait_for(
                        collector.run(asyncio.Event()), timeout=3
                    )
                layout = ensure_storage_layout(tmp_path)
                sealed = list(layout.sealed.glob("*.bmdr.zst"))
                assert len(sealed) == 1
                assert len(list(layout.manifests.glob("*.json"))) == 0
                partials = list(layout.active.glob("*.bmdr.partial"))
                assert len(partials) == 1
                return sealing_intent(
                    catalog, str(uuid.UUID(partials[0].name.split(".")[0]))
                )
            finally:
                catalog.close()

    intent = asyncio.run(crash())
    monkeypatch.setattr(seal_module, "_atomic_json", original_write)
    assert_materialized_gap_recovery(tmp_path, intent, expected_manifests=1)


def test_intent_failure_then_manifest_before_catalog_sealed_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-104: intent write fails; the manifest exists but the Catalog
    SEALED transition is absent. Restart reconstructs the intent and
    completes the seal idempotently."""
    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        original_transition = catalog.transition

        def failing_transition(
            chunk_id: str, to_state: ChunkState, **kwargs: Any
        ) -> None:
            if to_state is ChunkState.SEALED:
                raise OSError("injected crash before Catalog SEALED")
            original_transition(chunk_id, to_state, **kwargs)

        monkeypatch.setattr(catalog, "transition", failing_transition)
        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-11-r2",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        collector = UsdMStreamCollector(
            stream=UsdMStream.BOOK_TICKER,
            route="public",
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-11-r2",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-r2"),
            opener=opener,
        )
        fail_started_writes(catalog, monkeypatch)
        try:
            with pytest.raises(OSError, match="injected STARTED write failure"):
                await asyncio.wait_for(collector.run(asyncio.Event()), timeout=3)
            documents = manifests(tmp_path)
            assert len(documents) == 1
            assert documents[0]["gap"] is True
            assert documents[0]["complete"] is False
            return sealing_intent(catalog, documents[0]["chunk_id"])
        finally:
            catalog.close()

    intent = asyncio.run(crash())
    assert_materialized_gap_recovery(tmp_path, intent, expected_manifests=1)


def test_intent_failure_with_conflicting_existing_started_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-105: durable SEALING intent conflicts with an existing unmatched
    STARTED (different gap_id for the same market/stream): recovery must
    fail closed hard instead of guessing a boundary identity."""
    import binance_market_data_recorder.spool.seal as seal_module

    original_compress = seal_module._compress

    def crash_after_sealing(
        source_path: Any, target_partial: Any, source_size: int
    ) -> None:
        raise OSError("injected crash after SEALING transition commit")

    async def crash() -> dict[str, Any]:
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [book_ticker(1)], error=OSError("crash boundary")
                )
            raise OSError("replacement never opens")

        with monkeypatch.context() as context:
            context.setattr(seal_module, "_compress", crash_after_sealing)
            collector, catalog, _spool = IntentFailureCollector.build(
                tmp_path, opener=opener
            )
            fail_started_writes(catalog, monkeypatch)
            try:
                with pytest.raises(OSError, match="injected STARTED write failure"):
                    await asyncio.wait_for(
                        collector.run(asyncio.Event()), timeout=3
                    )
                layout = ensure_storage_layout(tmp_path)
                partials = list(layout.active.glob("*.bmdr.partial"))
                return sealing_intent(
                    catalog, str(uuid.UUID(partials[0].name.split(".")[0]))
                )
            finally:
                catalog.close()

    intent = asyncio.run(crash())
    monkeypatch.setattr(seal_module, "_compress", original_compress)
    # A different unmatched STARTED for the same market/stream already exists.
    from binance_market_data_recorder.spool.recovery import (
        RecoveryConflictError,
    )

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        catalog.record_operational_event(
            event_id="stream-discontinuity-started:other-gap",
            event_type="STREAM_DISCONTINUITY_STARTED",
            occurred_at_utc_ns=int(intent["gap_started_at_utc_ns"]),
            evidence={
                "gap_id": "other-gap",
                "market": "um_perpetual",
                "stream": "book_ticker",
                "reason": "planned_rotation",
                "interval_classification": "UNRELIABLE",
                "gap_started_at_utc_ns": intent["gap_started_at_utc_ns"],
                "original_connection_id": "some-other-connection",
                "original_generation": 0,
            },
        )
        with pytest.raises(
            RecoveryConflictError, match="RECOVERY_SEAL_INTENT_STARTED_CONFLICT"
        ):
            recover_storage(layout=layout, catalog=catalog)
        # Fail closed: the old partial was NOT sealed complete.
        assert list(layout.manifests.glob("*.json")) == []
        assert len(list(layout.active.glob("*.bmdr.partial"))) == 1


def test_normal_sealing_crash_without_intent_does_not_fake_reconnect_gap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-106: a plain SEALING crash with no reconnect intent must NOT be
    forced to reconnect_gap on restart: blanket 'all SEALING is a gap'
    forcing is prohibited."""
    import binance_market_data_recorder.spool.seal as seal_module
    from binance_market_data_recorder.binance.usdm.schema import (
        envelope_from_websocket_frame,
    )

    original_compress = seal_module._compress

    def crash_after_sealing(
        source_path: Any, target_partial: Any, source_size: int
    ) -> None:
        raise OSError("injected crash after SEALING transition commit")

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="plain-sealing",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        writer.append(
            envelope_from_websocket_frame(
                raw_payload=book_ticker(1),
                stream=UsdMStream.BOOK_TICKER,
                connection_id="plain-conn",
                collector_instance_id="plain-sealing",
                collector_version="0.1.0+test",
                receive_time_utc_ns=4_000_000_000,
                receive_monotonic_ns=1,
            )
        )
        writer.close()
        monkeypatch.setattr(seal_module, "_compress", crash_after_sealing)
        with pytest.raises(OSError, match="SEALING transition commit"):
            seal_partial(
                writer.path,
                layout=layout,
                catalog=catalog,
                forced_flags=frozenset(),
            )
    monkeypatch.setattr(seal_module, "_compress", original_compress)

    recovered_catalog = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered_catalog)
    recovered_catalog.close()
    assert any(action.action == "seal_completed_after_crash" for action in actions)
    documents = manifests(tmp_path)
    assert len(documents) == 1
    assert documents[0]["gap"] is False
    assert documents[0]["complete"] is True
    assert RECONNECT_GAP_FLAG not in documents[0]["capture_flags"]


def test_phase_a_crash_before_started_retains_orphan_active_partial(
    tmp_path: Path,
) -> None:
    """TEST-301 (P2-A): the boundary is remembered in memory only; the process
    fails before STARTED (or any SEALING evidence) becomes durable.

    Startup recovery must deliberately retain the clean orphan ACTIVE
    partial: registered ACTIVE, never sealed complete, no manifest, no gap
    events, no quarantine. The next collector opens a fresh generation and
    no false-complete claim is ever made for the orphan interval.
    """
    from binance_market_data_recorder.binance.usdm.schema import (
        envelope_from_websocket_frame,
    )

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="phase-a",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        writer.append(
            envelope_from_websocket_frame(
                raw_payload=book_ticker(1),
                stream=UsdMStream.BOOK_TICKER,
                connection_id="phase-a-conn",
                collector_instance_id="phase-a",
                collector_version="0.1.0+test",
                receive_time_utc_ns=5_000_000_000,
                receive_monotonic_ns=1,
            )
        )
        # Simulated crash: the descriptor was closed without a boundary
        # being detected durably (no STARTED, no SEALING evidence).
        writer.close()

    partials = list(layout.active.glob("*.bmdr.partial"))
    assert len(partials) == 1
    chunk_id = str(uuid.UUID(partials[0].name.split(".")[0]))

    # Fresh recovery objects (TEST-701): the policy is durable-state driven.
    recovered_catalog = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered_catalog)
    recovered_catalog.close()
    assert partials[0].exists()
    assert list(layout.manifests.glob("*.json")) == []
    assert list(layout.quarantine.glob("*")) == []
    with Catalog(layout.catalog, read_only=True) as catalog:
        row = catalog.chunk(chunk_id)
        assert row is not None
        assert ChunkState(str(row["state"])) is ChunkState.ACTIVE
        assert discontinuity_events(catalog) == []
    assert all(
        action.action in {"unchanged", "catalog_unchanged"}
        for action in actions
    )

    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield ScriptedSocket([book_ticker(2)], stop=stop)

        collector, catalog, _spool = make_collector(tmp_path, opener=opener)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
            assert discontinuity_events(catalog) == []
        finally:
            catalog.close()

    asyncio.run(exercise())
    documents = manifests(tmp_path)
    # The orphan ACTIVE partial was never claimed complete; only the fresh
    # generation sealed its own graceful chunk (complete=true is legal: the
    # recorded interval has no transport boundary inside it).
    assert len(documents) == 1
    assert documents[0]["complete"] is True
    assert documents[0]["gap"] is False
    assert partials[0].exists()
    with Catalog(layout.catalog, read_only=True) as catalog:
        row = catalog.chunk(chunk_id)
        assert row is not None
        assert ChunkState(str(row["state"])) is ChunkState.ACTIVE


class SpotDrainBlockSpool(StreamSpool):
    """Block every Spot drain while engaged; track drain invocations."""

    def __init__(
        self, engage: threading.Event, release: threading.Event, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.engage = engage
        self.release = release
        self.drain_calls = 0
        self.blocked = threading.Event()

    def drain_all(self) -> int:
        self.drain_calls += 1
        if self.engage.is_set():
            self.blocked.set()
            try:
                if not self.release.wait(timeout=10):
                    raise RuntimeError("test drain hold timed out")
            finally:
                self.blocked.clear()
        return super().drain_all()


def test_spot_writer_cancellation_owns_blocking_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """TEST-401 (P2-B): a Spot drain blocked in a worker thread must be owned.

    Cancelling the writer task must NOT let it report complete while the
    worker still mutates the StreamSpool: the owner stays pending until the
    blocked drain finishes, then releases the descriptor (abort_writer).
    After the owner reports complete, no further writer mutation may occur
    (no unowned drain calls).
    """
    from binance_market_data_recorder.binance.spot.websocket import (
        SpotStreamCollector,
    )

    engage = threading.Event()
    release = threading.Event()
    gate = asyncio.Event()
    captured_writer_tasks: list[asyncio.Task[None]] = []
    original_create_task = asyncio.create_task

    def capturing_create_task(
        coroutine: Any, *args: Any, **kwargs: Any
    ) -> asyncio.Task[Any]:
        task = original_create_task(coroutine, *args, **kwargs)
        if "_writer_loop" in getattr(coroutine, "__qualname__", ""):
            captured_writer_tasks.append(task)
        return task

    monkeypatch.setattr(asyncio, "create_task", capturing_create_task)

    class GatedSocket(ScriptedSocket):
        async def recv(self, decode: bool | None = None) -> bytes:
            try:
                return next(self.messages)
            except StopIteration:
                await gate.wait()
                await asyncio.Future[None]()
                raise RuntimeError(
                    "test socket exhausted unreachably"
                ) from None

    async def scenario() -> dict[str, Any]:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        spool = SpotDrainBlockSpool(
            engage=engage,
            release=release,
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="book_ticker",
            collector_instance_id="m21-4-11-r2",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            yield GatedSocket([book_ticker(1), book_ticker(2)])

        collector = SpotStreamCollector(
            stream=SpotStream.BOOK_TICKER,
            wire_name="btcusdt@bookTicker",
            spool=spool,
            collector_instance_id="m21-4-11-r2",
            collector_version="0.1.0+test",
            logger=logging.getLogger("test.m21-4-11-r2"),
            opener=opener,
        )
        run_task = asyncio.create_task(collector.run(stop))
        writer_task: asyncio.Task[None] | None = None
        try:
            # Frame 1 is drained by the owned writer.
            deadline = asyncio.get_running_loop().time() + 5
            while spool.drain_calls < 1 and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.01)
            assert spool.drain_calls >= 1
            engage.set()
            # The next drain (frame 2, or an idle drain) now blocks in its
            # worker thread; wait for the deterministic blocked state.
            gate.set()
            deadline = asyncio.get_running_loop().time() + 5
            while not spool.blocked.is_set() and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.01)
            assert spool.blocked.is_set()
            assert captured_writer_tasks, "writer task was not captured"
            writer_task = captured_writer_tasks[0]
            writer_task.cancel()
            # The owner must NOT report complete while its worker is blocked:
            # cancellation is deferred behind the owned blocking call.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(
                    asyncio.shield(writer_task), timeout=0.3
                )
            assert not writer_task.done()
            return {
                "writer_task": writer_task,
                "run_task": run_task,
                "attempts_ref": attempts,
                "spool": spool,
            }
        finally:
            release.set()
            if writer_task is not None:
                await asyncio.gather(
                    run_task, writer_task, return_exceptions=True
                )
            else:
                await asyncio.gather(run_task, return_exceptions=True)
            stop.set()
            catalog.close()

    captured = asyncio.run(scenario())
    writer_task = captured["writer_task"]
    run_task = captured["run_task"]
    spool = cast(SpotDrainBlockSpool, captured["spool"])
    # The owner completed after its worker finished (deferred cancellation);
    # no drain may run after the owner reported complete.
    assert writer_task.done()
    assert run_task.done()
    assert captured["attempts_ref"] == 1
    drains_at_completion = spool.drain_calls
    deadline = time.monotonic() + 0.4
    while time.monotonic() < deadline:
        pass
    assert spool.drain_calls == drains_at_completion

    # TEST-402: the partial/descriptor state stays recoverable and no
    # replacement transport was opened.
    layout = ensure_storage_layout(tmp_path)
    partials = list(layout.active.glob("*.bmdr.partial"))
    assert len(partials) == 1
    recovered_catalog = Catalog(layout.catalog)
    actions = recover_storage(layout=layout, catalog=recovered_catalog)
    recovered_catalog.close()
    assert partials[0].exists()
    assert all(
        action.action in {"unchanged", "catalog_unchanged", "seal_completed_after_crash"}
        for action in actions
    )
