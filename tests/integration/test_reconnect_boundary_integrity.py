from __future__ import annotations

import asyncio
import io
import json
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard
from websockets.exceptions import ConnectionClosedError

from binance_market_data_recorder.binance.spot.websocket import ReconnectBackoff
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
from binance_market_data_recorder.spool.recovery import recover_storage
from binance_market_data_recorder.spool.seal import (
    OVERLAP_FLAG,
    RECONNECT_GAP_FLAG,
    seal_partial,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
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
        self, forced_flags: frozenset[str] = frozenset()
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
