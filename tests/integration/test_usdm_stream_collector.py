from __future__ import annotations

import asyncio
import io
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import zstandard
from websockets.asyncio.server import serve

from binance_market_data_recorder.binance.spot.websocket import ReconnectBackoff
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
    open_usdm_websocket,
)
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


class ScriptedSocket:
    def __init__(self, messages: list[bytes], stop: asyncio.Event | None = None) -> None:
        self.messages = iter(messages)
        self.stop = stop

    async def recv(self, decode: bool | None = None) -> bytes:
        try:
            return next(self.messages)
        except StopIteration:
            if self.stop is not None:
                self.stop.set()
                await asyncio.Future[None]()
            raise OSError("injected disconnect") from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


class BlockingSocket:
    def __init__(self) -> None:
        self.close_reasons: list[str] = []

    async def recv(self, decode: bool | None = None) -> bytes:
        await asyncio.Future[None]()
        raise AssertionError("unreachable")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_reasons.append(reason)


def depth(first: int, last: int, previous: int) -> bytes:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": last,
            "T": last,
            "s": "BTCUSDT",
            "U": first,
            "u": last,
            "pu": previous,
            "b": [],
            "a": [],
        },
        separators=(",", ":"),
    ).encode()


def make_stream(root: Path, opener: Any) -> tuple[UsdMStreamCollector, Catalog]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = StreamSpool(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream="diff_depth",
        collector_instance_id="test",
        collector_version="test",
        queue_capacity=32,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
    )
    return UsdMStreamCollector(
        stream=UsdMStream.DIFF_DEPTH,
        route="public",
        wire_name="btcusdt@depth@100ms",
        spool=spool,
        collector_instance_id="test",
        collector_version="test",
        logger=logging.getLogger("test.usdm.stream"),
        receipt_queue_capacity=16,
        planned_rotation_seconds=60,
        backoff=ReconnectBackoff(initial_seconds=0.001, maximum_seconds=0.001, jitter_ratio=0),
        opener=opener,
    ), catalog


def envelopes(root: Path) -> list[Any]:
    result: list[Any] = []
    documents = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (root / "data" / "manifests").glob("*.json")
    ]
    documents.sort(key=lambda item: int(item["created_at_utc_ns"]))
    for manifest in documents:
        raw = zstandard.ZstdDecompressor().decompress(
            (root / manifest["relative_path"]).read_bytes()
        )
        source = io.BytesIO(raw)
        decode_chunk_header(source)
        while prefix := source.read(FRAME_PREFIX.size):
            length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
            result.append(decode_envelope(source.read(length)))
    return result


def test_usdm_duplicate_out_of_order_and_reconnect_are_lossless(tmp_path: Path) -> None:
    async def exercise() -> list[dict[str, object]]:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            messages = (
                [depth(10, 10, 9), depth(10, 10, 9), depth(8, 8, 7)]
                if attempts == 1
                else [depth(11, 11, 10)]
            )
            yield ScriptedSocket(messages, stop if attempts == 2 else None)

        collector, catalog = make_stream(tmp_path, opener)
        try:
            # DIFF_DEPTH never reconnects inside one capture session: the
            # first unexpected disconnect seals the old generation with gap
            # evidence and retires the session (fresh snapshot + bridge is
            # required). The outer collector restarts the session, which is
            # the second run() call below.
            await asyncio.wait_for(collector.run(stop), timeout=3)
            await asyncio.wait_for(collector.run(stop), timeout=3)
            assert attempts == 2
            events = catalog.operational_events()
            assert [
                event["event_type"] for event in events
            ] == ["STREAM_DISCONTINUITY_STARTED", "STREAM_DISCONTINUITY_COMPLETED"]
            started = cast(dict[str, Any], events[0]["evidence"])
            assert started["reason"] == "unexpected_disconnect"
            assert started["boundary_frame_persisted"] is False
            completed = cast(dict[str, Any], events[1]["evidence"])
            assert completed["gap_id"] == started["gap_id"]
            assert completed["historical_continuity_restored"] is False
            return events
        finally:
            catalog.close()

    asyncio.run(exercise())
    assert [event.raw_payload for event in envelopes(tmp_path)] == [
        depth(10, 10, 9),
        depth(10, 10, 9),
        depth(8, 8, 7),
        depth(11, 11, 10),
    ]
    documents = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "data" / "manifests").glob("*.json")
    ]
    documents.sort(key=lambda item: int(item["created_at_utc_ns"]))
    assert len(documents) == 2
    assert documents[0]["gap"] is True and documents[0]["complete"] is False
    assert documents[1]["gap"] is True and documents[1]["complete"] is False
    assert "reconnect_gap" in documents[0]["capture_flags"]
    assert "sequence_gap" in documents[1]["capture_flags"]


def test_usdm_local_server_ping_is_ponged(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        pong = asyncio.Event()

        async def handler(websocket: Any) -> None:
            waiter = await websocket.ping(b"usdm-ping")
            await asyncio.wait_for(waiter, timeout=1)
            pong.set()
            await websocket.send(depth(1, 1, 0), text=True)
            await asyncio.sleep(0.05)
            stop.set()

        async with serve(handler, "127.0.0.1", 0) as server:
            port = next(iter(server.sockets)).getsockname()[1]
            collector, catalog = make_stream(tmp_path, open_usdm_websocket)
            collector.websocket_root = f"ws://127.0.0.1:{port}"
            try:
                await asyncio.wait_for(collector.run(stop), timeout=3)
            finally:
                catalog.close()
        assert pong.is_set()

    asyncio.run(exercise())
    assert len(envelopes(tmp_path)) == 1


def test_usdm_planned_rotation_replaces_connection_before_24_hours(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        sockets: list[BlockingSocket] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            socket = BlockingSocket()
            sockets.append(socket)
            if len(sockets) == 2:
                asyncio.get_running_loop().call_later(0.01, stop.set)
            yield socket

        collector, catalog = make_stream(tmp_path, opener)
        collector.planned_rotation_seconds = 0.02
        try:
            # DIFF_DEPTH retires its capture session at a reconnect boundary
            # (fresh snapshot required); the first rotation closes the
            # connection and seals the empty generation with gap evidence.
            await asyncio.wait_for(collector.run(stop), timeout=3)
            await asyncio.wait_for(collector.run(stop), timeout=3)
            assert len(sockets) == 2
            assert sockets[0].close_reasons == ["planned 24-hour rotation"]
            started = [
                event
                for event in catalog.operational_events()
                if event["event_type"] == "STREAM_DISCONTINUITY_STARTED"
            ]
            assert len(started) == 1
            assert cast(dict[str, Any], started[0]["evidence"])["reason"] == "planned_rotation"
            assert (
                cast(dict[str, Any], started[0]["evidence"])["boundary_frame_persisted"]
                is False
            )
        finally:
            catalog.close()

    asyncio.run(exercise())
