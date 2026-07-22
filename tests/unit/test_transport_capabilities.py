from __future__ import annotations

import asyncio
from typing import Any, cast

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from tools.probe_binance_transports import offline_capability_report


def test_official_sdk_rejected_and_generic_transport_selected() -> None:
    report = offline_capability_report()
    sdk = cast(dict[str, Any], report["official_sdk_websocket"])
    generic = cast(dict[str, Any], report["generic_websocket"])

    assert report["network_accessed"] is False
    assert sdk["selected"] is False
    assert sdk["checks"]["raw_payload_bytes"]["passed"] is False
    assert sdk["checks"]["blocking_callback_backpressure"]["passed"] is False
    assert sdk["checks"]["depth_update_ids"]["passed"] is True
    assert generic["selected"] is True
    assert all(check["passed"] for check in generic["checks"].values())


def test_generic_websocket_returns_exact_text_payload_bytes() -> None:
    expected = b'{  "e" : "depthUpdate", "U": 1, "u": 2 }'

    async def exercise() -> bytes:
        async def handler(websocket: Any) -> None:
            await websocket.send(expected, text=True)

        async with serve(handler, "127.0.0.1", 0) as server:
            sockets = list(server.sockets)
            assert sockets
            port = sockets[0].getsockname()[1]
            async with connect(
                f"ws://127.0.0.1:{port}",
                proxy=None,
                compression=None,
                max_queue=(4, 1),
            ) as websocket:
                received = await websocket.recv(decode=False)
                assert isinstance(received, bytes)
                return received

    assert asyncio.run(exercise()) == expected
