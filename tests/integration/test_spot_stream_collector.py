from __future__ import annotations

import asyncio
import io
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import zstandard
from websockets.asyncio.server import serve

from binance_market_data_recorder.binance.spot.schema import SpotStream
from binance_market_data_recorder.binance.spot.websocket import (
    ReconnectBackoff,
    SpotStreamCollector,
    WebSocketConnection,
    open_spot_websocket,
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


class ScriptedWebSocket:
    def __init__(
        self,
        messages: list[bytes],
        stop: asyncio.Event | None = None,
        *,
        block_on_exhaustion: bool = False,
    ) -> None:
        self.messages = iter(messages)
        self.stop = stop
        self.block_on_exhaustion = block_on_exhaustion
        self.close_reasons: list[str] = []

    async def recv(self, decode: bool | None = None) -> bytes:
        try:
            return next(self.messages)
        except StopIteration:
            if self.stop is not None:
                self.stop.set()
                await asyncio.Future()
            if self.block_on_exhaustion:
                await asyncio.Future()
            raise OSError("injected disconnect") from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_reasons.append(reason)


def make_stream(
    root: Path,
    *,
    opener: Any,
    stop: asyncio.Event,
    planned_rotation_seconds: float = 60,
) -> tuple[SpotStreamCollector, Catalog]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    spool = StreamSpool(
        layout=layout,
        catalog=catalog,
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        collector_instance_id="collector-test",
        collector_version="0.1.0+test",
        queue_capacity=32,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=0,
        max_frame_bytes=1024 * 1024,
    )
    collector = SpotStreamCollector(
        stream=SpotStream.DIFF_DEPTH,
        wire_name="btcusdt@depth@100ms",
        spool=spool,
        collector_instance_id="collector-test",
        collector_version="0.1.0+test",
        logger=logging.getLogger("test.spot"),
        receipt_queue_capacity=16,
        planned_rotation_seconds=planned_rotation_seconds,
        backoff=ReconnectBackoff(initial_seconds=0.001, maximum_seconds=0.001, jitter_ratio=0),
        opener=opener,
    )
    return collector, catalog


def manifests(root: Path) -> list[dict[str, Any]]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((root / "data" / "manifests").glob("*.json"))
    ]


def sealed_envelopes(root: Path) -> list[Any]:
    envelopes: list[Any] = []
    for document in manifests(root):
        compressed = root / document["relative_path"]
        raw = zstandard.ZstdDecompressor().decompress(compressed.read_bytes())
        source = io.BytesIO(raw)
        decode_chunk_header(source)
        while prefix := source.read(FRAME_PREFIX.size):
            body_length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
            envelopes.append(decode_envelope(source.read(body_length)))
    return envelopes


def depth(first: int, last: int) -> bytes:
    return json.dumps(
        {
            "e": "depthUpdate",
            "E": 1_700_000_000_000 + last,
            "s": "BTCUSDT",
            "U": first,
            "u": last,
            "b": [],
            "a": [],
        },
        separators=(",", ":"),
    ).encode()


def test_duplicates_and_out_of_order_messages_are_all_written(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        socket = ScriptedWebSocket([depth(10, 10), depth(10, 10), depth(8, 8)], stop)

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            yield socket

        collector, catalog = make_stream(tmp_path, opener=opener, stop=stop)
        try:
            await collector.run(stop)
        finally:
            catalog.close()

    asyncio.run(exercise())
    documents = manifests(tmp_path)
    assert sum(int(document["record_count"]) for document in documents) == 3
    assert documents[0]["sequence_ranges"] == {
        "U": {"min": 8, "max": 10},
        "u": {"min": 8, "max": 10},
    }
    assert [item.raw_payload for item in sealed_envelopes(tmp_path)] == [
        depth(10, 10),
        depth(10, 10),
        depth(8, 8),
    ]


def test_unexpected_disconnect_reconnects_with_a_new_connection_id(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            socket = ScriptedWebSocket([depth(attempts, attempts)], stop if attempts == 2 else None)
            yield socket

        collector, catalog = make_stream(tmp_path, opener=opener, stop=stop)
        try:
            await collector.run(stop)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    documents = manifests(tmp_path)
    connection_ids = {
        connection_id
        for document in documents
        for connection_id in document["connection_ids"]
    }
    assert sum(int(document["record_count"]) for document in documents) == 2
    assert len(connection_ids) == 2


def test_local_server_ping_payload_is_ponged_and_frame_is_persisted(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        pong_confirmed = asyncio.Event()
        payload = depth(1, 1)

        async def handler(websocket: Any) -> None:
            pong_waiter = await websocket.ping(b"copy-this-payload")
            await asyncio.wait_for(pong_waiter, timeout=1)
            pong_confirmed.set()
            await websocket.send(payload, text=True)
            await asyncio.sleep(0.05)
            stop.set()

        async with serve(handler, "127.0.0.1", 0) as server:
            sockets = list(server.sockets)
            assert sockets
            port = sockets[0].getsockname()[1]
            collector, catalog = make_stream(tmp_path, opener=open_spot_websocket, stop=stop)
            collector.base_url = f"ws://127.0.0.1:{port}/ws"
            try:
                await asyncio.wait_for(collector.run(stop), timeout=3)
            finally:
                catalog.close()
            assert pong_confirmed.is_set()

    asyncio.run(exercise())
    assert sum(int(document["record_count"]) for document in manifests(tmp_path)) == 1


def test_planned_rotation_replaces_connection_before_24_hours(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0
        sockets: list[ScriptedWebSocket] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            socket = ScriptedWebSocket(
                [], stop if attempts == 2 else None, block_on_exhaustion=True
            )
            sockets.append(socket)
            if attempts == 2:
                asyncio.get_running_loop().call_later(0.01, stop.set)
            yield socket

        collector, catalog = make_stream(
            tmp_path, opener=opener, stop=stop, planned_rotation_seconds=0.02
        )
        try:
            await collector.run(stop)
        finally:
            catalog.close()
        assert attempts == 2
        assert sockets[0].close_reasons == ["planned 24-hour rotation"]

    asyncio.run(exercise())


def test_server_shutdown_frame_is_persisted_before_reconnect(tmp_path: Path) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0
        shutdown = b'{"e":"serverShutdown","E":1770123456789}'

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            socket = ScriptedWebSocket(
                [shutdown] if attempts == 1 else [depth(2, 2)],
                stop if attempts == 2 else None,
                block_on_exhaustion=True,
            )
            yield socket

        collector, catalog = make_stream(tmp_path, opener=opener, stop=stop)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    captured = sealed_envelopes(tmp_path)
    assert len(captured) == 2
    assert captured[0].capture_flags == ("server_shutdown",)
