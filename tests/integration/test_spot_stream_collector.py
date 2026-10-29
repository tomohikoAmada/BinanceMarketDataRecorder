from __future__ import annotations

import asyncio
import io
import json
import logging
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import zstandard
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

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
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


class ScriptedWebSocket:
    def __init__(
        self,
        messages: list[bytes],
        stop: asyncio.Event | None = None,
        *,
        block_on_exhaustion: bool = False,
        disconnect_error: Exception | None = None,
    ) -> None:
        self.messages = iter(messages)
        self.stop = stop
        self.block_on_exhaustion = block_on_exhaustion
        self.disconnect_error = disconnect_error
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
            if self.disconnect_error is not None:
                raise self.disconnect_error from None
            raise OSError("injected disconnect") from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_reasons.append(reason)


def make_stream(
    root: Path,
    *,
    opener: Any,
    stop: asyncio.Event,
    planned_rotation_seconds: float = 60,
    rotate_before_reconnect: bool = False,
) -> tuple[SpotStreamCollector, Catalog]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    rotated = False

    class ControlledRotationWriter(RawChunkWriter):
        def should_rotate(self, *, now_monotonic: float | None = None) -> bool:
            nonlocal rotated
            # Control only the writer's rotation clock, leaving the event loop,
            # receive times, real append/seal and reconnect protocol untouched.
            if now_monotonic is None:
                now_monotonic = self._opened_monotonic
                if rotate_before_reconnect and self.record_count and not rotated:
                    rotated = True
                    now_monotonic = self._rotation_deadline_monotonic
            return super().should_rotate(now_monotonic=now_monotonic)

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
        writer_factory=ControlledRotationWriter,
    )
    collector = SpotStreamCollector(
        stream=SpotStream.DIFF_DEPTH,
        symbol="BTCUSDT",
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
    documents = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (root / "data" / "manifests").glob("*.json")
    ]
    documents.sort(key=lambda item: int(item["created_at_utc_ns"]))
    return documents


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


def assert_reconnect_layout(
    root: Path,
    *,
    old_payload: bytes,
    new_payload: bytes,
    reason: str,
    old_flags: set[str],
    rotated: bool,
) -> None:
    captured = sealed_envelopes(root)
    assert [item.raw_payload for item in captured] == [old_payload, new_payload]
    old, new = captured
    assert set(old.capture_flags) == old_flags
    assert new.capture_flags == ("sequence_gap",)
    assert old.connection_id != new.connection_id

    documents = manifests(root)
    data = [d for d in documents if d["record_count"] > 0]
    markers = [d for d in documents if d["record_count"] == 0]
    assert len(data) == 2
    assert len(markers) == 1 + int(rotated)
    assert len(documents) == len(data) + len(markers)
    assert all(d["record_count"] == 1 for d in data)
    old_data = next(d for d in data if d["connection_ids"] == [old.connection_id])
    new_data = next(d for d in data if d["connection_ids"] == [new.connection_id])
    assert set(old_data["capture_flags"]) == old_flags | (set() if rotated else {"reconnect_gap"})
    assert old_data["complete"] is rotated
    assert old_data["gap"] is not rotated
    assert new_data["capture_flags"] == ["sequence_gap"]
    assert new_data["complete"] is False and new_data["gap"] is True
    assert all(
        d["capture_flags"] == ["reconnect_gap"]
        and d["connection_ids"] == []
        and d["complete"] is False
        and d["gap"] is True
        for d in markers
    )
    assert len([d for d in markers if d["created_at_utc_ns"] < old_data["created_at_utc_ns"]]) == 1
    assert len(
        [
            d
            for d in markers
            if old_data["created_at_utc_ns"]
            < d["created_at_utc_ns"]
            < new_data["created_at_utc_ns"]
        ]
    ) == int(rotated)

    with Catalog(root / "state/catalog.sqlite", read_only=True) as catalog:
        started_events = catalog.operational_events(event_type="STREAM_DISCONTINUITY_STARTED")
        completed_events = catalog.operational_events(event_type="STREAM_DISCONTINUITY_COMPLETED")
    assert len(started_events) == len(completed_events) == 1
    started = cast(dict[str, Any], started_events[0]["evidence"])
    completed = cast(dict[str, Any], completed_events[0]["evidence"])
    assert started["reason"] == completed["reason"] == reason
    assert started["original_connection_id"] == old.connection_id
    assert completed["new_connection_id"] == new.connection_id
    assert completed["gap_id"] == started["gap_id"]
    assert completed["new_generation"] == started["original_generation"] + 1
    assert completed["raw_gap_marker"] == "sequence_gap"
    assert completed["historical_continuity_restored"] is False


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
    assert documents[0]["record_count"] == 0
    assert documents[0]["capture_flags"] == ["reconnect_gap"]
    assert documents[1]["sequence_ranges"] == {
        "U": {"min": 8, "max": 10},
        "u": {"min": 8, "max": 10},
    }
    assert [item.raw_payload for item in sealed_envelopes(tmp_path)] == [
        depth(10, 10),
        depth(10, 10),
        depth(8, 8),
    ]


@pytest.mark.parametrize(
    "disconnect_error",
    [
        ConnectionResetError(104, "injected reset"),
        ConnectionClosedError(Close(1011, "server restart"), Close(1011, "ack"), True),
    ],
    ids=["tcp-reset", "server-close"],
)
@pytest.mark.parametrize(
    "rotate_before_reconnect", [False, True], ids=["active-tail", "rotated-tail"]
)
def test_unexpected_disconnect_reconnects_with_a_new_connection_id(
    tmp_path: Path,
    disconnect_error: Exception,
    caplog: pytest.LogCaptureFixture,
    rotate_before_reconnect: bool,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            socket = ScriptedWebSocket(
                [depth(attempts, attempts)],
                stop if attempts == 2 else None,
                disconnect_error=disconnect_error,
            )
            yield socket

        collector, catalog = make_stream(
            tmp_path, opener=opener, stop=stop, rotate_before_reconnect=rotate_before_reconnect
        )
        try:
            # DIFF_DEPTH retires its capture session at a reconnect boundary;
            # the outer collector restarts the session (second run() call).
            await asyncio.wait_for(collector.run(stop), timeout=3)
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    record = next(
        r
        for r in caplog.records
        if getattr(r, "structured_event", None) == "spot_websocket_disconnected"
    )
    fields = cast(dict[str, Any], getattr(record, "structured_fields", None))
    assert fields["connection_phase"] == "established"
    assert fields["connected_duration_seconds_at_log"] >= 0
    if isinstance(disconnect_error, ConnectionClosedError):
        assert fields["close_received_code"] == 1011
        assert fields["close_received_reason"] == "server restart"
        assert fields["close_received_then_sent"] is True
    else:
        assert fields["close_received_code"] is None
        assert fields["os_errno"] == 104
    assert_reconnect_layout(
        tmp_path,
        old_payload=depth(1, 1),
        new_payload=depth(2, 2),
        reason="unexpected_disconnect",
        old_flags=set(),
        rotated=rotate_before_reconnect,
    )


def test_dns_failure_backs_off_then_reconnects_without_losing_raw(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise socket.gaierror(-2, "injected DNS failure")
            yield ScriptedWebSocket([depth(20, 20)], stop)

        collector, catalog = make_stream(tmp_path, opener=opener, stop=stop)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    record = next(
        r
        for r in caplog.records
        if getattr(r, "structured_event", None) == "spot_websocket_disconnected"
    )
    fields = cast(dict[str, Any], getattr(record, "structured_fields", None))
    assert fields["connection_phase"] == "opening"
    assert fields["connected_duration_seconds_at_log"] is None
    assert fields["os_errno"] == -2
    assert fields["close_received_code"] is None
    captured = sealed_envelopes(tmp_path)
    assert [item.raw_payload for item in captured] == [depth(20, 20)]


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
            # DIFF_DEPTH retires its capture session at a reconnect boundary.
            await asyncio.wait_for(collector.run(stop), timeout=3)
            await asyncio.wait_for(collector.run(stop), timeout=3)
            assert attempts == 2
            assert sockets[0].close_reasons == ["planned 24-hour rotation"]
            started = [
                event
                for event in catalog.operational_events()
                if event["event_type"] == "STREAM_DISCONTINUITY_STARTED"
            ]
            assert len(started) == 1
            assert cast(dict[str, Any], started[0]["evidence"])["reason"] == "planned_rotation"
            assert cast(dict[str, Any], started[0]["evidence"])["boundary_frame_persisted"] is False
        finally:
            catalog.close()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "rotate_before_reconnect", [False, True], ids=["active-tail", "rotated-tail"]
)
def test_server_shutdown_frame_is_persisted_before_reconnect(
    tmp_path: Path, rotate_before_reconnect: bool
) -> None:
    shutdown = b'{"e":"serverShutdown","E":1770123456789}'

    async def exercise() -> None:
        stop = asyncio.Event()
        attempts = 0

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

        collector, catalog = make_stream(
            tmp_path, opener=opener, stop=stop, rotate_before_reconnect=rotate_before_reconnect
        )
        try:
            # DIFF_DEPTH retires its capture session at a reconnect boundary.
            await asyncio.wait_for(collector.run(stop), timeout=3)
            await asyncio.wait_for(collector.run(stop), timeout=3)
        finally:
            catalog.close()
        assert attempts == 2

    asyncio.run(exercise())
    assert_reconnect_layout(
        tmp_path,
        old_payload=shutdown,
        new_payload=depth(2, 2),
        reason="server_shutdown",
        old_flags={"server_shutdown"},
        rotated=rotate_before_reconnect,
    )
