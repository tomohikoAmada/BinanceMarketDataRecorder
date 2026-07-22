from __future__ import annotations

import asyncio
import os

import pytest

from binance_market_data_recorder.binance.usdm.side_data_rest import (
    REST_SIDE_DATA_SPECS,
    capture_rest_side_data,
)
from binance_market_data_recorder.binance.usdm.side_data_schema import (
    UsdMSideStream,
    envelope_from_side_stream_frame,
)
from binance_market_data_recorder.binance.usdm.websocket import open_usdm_websocket

pytestmark = [
    pytest.mark.online,
    pytest.mark.skipif(
        os.environ.get("BINANCE_MARKET_RECORDER_ONLINE") != "1",
        reason="set BINANCE_MARKET_RECORDER_ONLINE=1 for unsigned public side-data smoke",
    ),
]


def test_unsigned_public_usdm_side_rest_smoke() -> None:
    for kind in REST_SIDE_DATA_SPECS:
        envelope = capture_rest_side_data(
            kind=kind,
            collector_instance_id="online-side-smoke",
            collector_version="test",
            timeout_ms=10_000,
        )
        assert envelope.stream == kind.value
        assert "sdk_model_not_raw_http_body" in envelope.capture_flags


def test_unsigned_public_usdm_side_stream_smoke() -> None:
    async def exercise() -> None:
        mark_url = "wss://fstream.binance.com/market/ws/btcusdt@markPrice@1s"
        async with open_usdm_websocket(mark_url) as websocket:
            raw = await asyncio.wait_for(websocket.recv(decode=False), timeout=10)
            payload = raw.encode() if isinstance(raw, str) else raw
            envelope = envelope_from_side_stream_frame(
                raw_payload=payload,
                stream=UsdMSideStream.MARK_PRICE,
                connection_id="online-mark",
                collector_instance_id="online-side-smoke",
                collector_version="test",
                receive_time_utc_ns=1,
                receive_monotonic_ns=1,
            )
            assert "malformed" not in envelope.capture_flags

        liquidation_url = "wss://fstream.binance.com/market/ws/btcusdt@forceOrder"
        async with open_usdm_websocket(liquidation_url) as websocket:
            try:
                raw = await asyncio.wait_for(websocket.recv(decode=False), timeout=1.2)
            except TimeoutError:
                return
            payload = raw.encode() if isinstance(raw, str) else raw
            envelope = envelope_from_side_stream_frame(
                raw_payload=payload,
                stream=UsdMSideStream.LIQUIDATION,
                connection_id="online-liquidation",
                collector_instance_id="online-side-smoke",
                collector_version="test",
                receive_time_utc_ns=1,
                receive_monotonic_ns=1,
            )
            assert "malformed" not in envelope.capture_flags

    asyncio.run(exercise())
