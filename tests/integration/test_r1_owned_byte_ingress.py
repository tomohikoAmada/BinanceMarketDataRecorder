from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from binance_market_data_recorder.binance.spot.schema import SpotStream
from binance_market_data_recorder.binance.spot.websocket import open_spot_websocket
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from binance_market_data_recorder.binance.usdm.websocket import open_usdm_websocket
from tests.integration import test_spot_ingress_backpressure as spot
from tests.integration import test_usdm_ingress_backpressure as usdm
from tests.integration.test_reconnect_boundary_integrity import (
    capture_startup_already_completed as capture_startup_already_completed,
)


@pytest.mark.parametrize("market", ["spot", "usdm"])
@pytest.mark.parametrize("sustained", [False, True])
def test_large_legal_frames_and_slow_writer_are_byte_bounded_and_loss_explicit(
    tmp_path: Path,
    market: str,
    sustained: bool,
) -> None:
    support = spot if market == "spot" else usdm

    def payload(value: int) -> bytes:
        document = json.loads(support.book_ticker(value))
        document.update(st=1, ps="BTCUSDT", padding="x" * (256 * 1024))
        return json.dumps(document, separators=(",", ":")).encode()

    originals = [payload(value) for value in range(16)]
    capacity = 2 * max(map(len, [*originals, payload(10000)]))

    async def exercise() -> tuple[Any, list[Any]]:
        stop = asyncio.Event()
        sockets: list[Any] = []

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[Any]:
            final = bool(sockets) or not sustained
            if sockets:
                collector.spool.drain_delay_seconds = 0
            socket = support.BurstSocket(
                [payload(10000)] if sockets else originals,
                stop=stop if final else None,
            )
            sockets.append(socket)
            yield socket

        parameters: dict[str, Any] = {
            "opener": opener,
            "capacity": 64,
            "byte_capacity": capacity,
            "drain_delay_seconds": 0.05 if sustained else 0.005,
            "put_timeout_seconds": 0.005 if sustained else 0.2,
            "saturation_timeout_seconds": 0.02 if sustained else 1,
        }
        if market == "spot":
            parameters["stream"] = SpotStream.BOOK_TICKER
        else:
            parameters["stream"] = UsdMStream.BOOK_TICKER
        collector, catalog = support.make_collector(tmp_path, **parameters)
        try:
            await asyncio.wait_for(collector.run(stop), timeout=5)
            return collector.receipt_queue_stats, sockets
        finally:
            catalog.close()

    stats, sockets = asyncio.run(exercise())
    frames, manifests = support.captured(tmp_path)
    payloads = [frame.raw_payload for frame in frames]
    assert stats.wait_count > 0
    assert stats.high_watermark < stats.capacity
    assert stats.high_watermark_bytes <= capacity
    assert stats.owned_bytes == stats.queued_bytes == stats.depth == 0
    if not sustained:
        assert payloads == originals
        assert all(document["complete"] and not document["gap"] for document in manifests)
    else:
        boundary = payloads.index(payload(10000))
        assert payloads[:boundary] == originals[:boundary]
        assert payloads[boundary:] == [payload(10000)]
        assert "sequence_gap" in frames[boundary - 1].capture_flags
        assert "sequence_gap" in frames[boundary].capture_flags
        assert sockets[0].close_reasons == ["bounded ingress backpressure"]
        assert any(document["gap"] and not document["complete"] for document in manifests)


@pytest.mark.parametrize("opener", [open_spot_websocket, open_usdm_websocket])
def test_actual_transport_accepts_fragmented_16mib_frame(opener: Any) -> None:
    async def exercise() -> None:
        # Real installed websockets and its assembler, rather than a mocked recv.
        payload = b"x" * (16 * 1024 * 1024)

        async def handler(socket: ServerConnection) -> None:
            await socket.send([payload[: len(payload) // 2], payload[len(payload) // 2 :]])
            await socket.wait_closed()

        async with serve(handler, "127.0.0.1", 0, compression=None) as server:
            port = next(iter(server.sockets)).getsockname()[1]
            async with opener(f"ws://127.0.0.1:{port}", proxy=None) as socket:
                assert await socket.recv(decode=False) == payload
                assert socket.protocol.max_size == len(payload)
                assert socket.recv_messages.high == 4
                assert socket.recv_messages.low == 1

    asyncio.run(exercise())
