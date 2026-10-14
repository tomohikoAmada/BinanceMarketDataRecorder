"""A continuous ready stream must not starve a stop or lose its boundary frame."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.spot.schema import SpotStream
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from tests.integration import test_spot_ingress_backpressure as spot
from tests.integration import test_usdm_ingress_backpressure as usdm
from tests.integration.test_reconnect_boundary_integrity import (
    # These cases target receive/stop arbitration after startup is complete.
    capture_startup_already_completed as capture_startup_already_completed,
)


@pytest.mark.parametrize("market", ["spot", "usdm"])
@pytest.mark.parametrize("stop_during_handoff", [False, True])
@pytest.mark.parametrize("session_restart", [False, True])
def test_continuous_receive_honors_stop_after_preserving_ready_frame(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: str,
    stop_during_handoff: bool,
    session_restart: bool,
) -> None:
    support = spot if market == "spot" else usdm
    payloads: list[bytes] = []

    async def exercise() -> Any:
        stop = asyncio.Event()
        restart = asyncio.Event()

        def request_stop() -> None:
            if session_restart:
                restart.set()
            stop.set()

        class ContinuousSocket:
            def __init__(self) -> None:
                self.closed = False
                self.close_reasons: list[str] = []

            async def recv(self, decode: bool | None = None) -> bytes:
                assert not self.closed
                payload: bytes = support.book_ticker(len(payloads) + 1)
                payloads.append(payload)
                # Both receive and stop can be ready in the same wait result.
                if len(payloads) == 3 and not stop_during_handoff:
                    request_stop()
                return payload

            async def close(self, code: int = 1000, reason: str = "") -> None:
                assert code == 1000
                self.closed = True
                self.close_reasons.append(reason)

        socket = ContinuousSocket()

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[Any]:
            yield socket

        stream = SpotStream.BOOK_TICKER if market == "spot" else UsdMStream.BOOK_TICKER
        collector, catalog = support.make_collector(
            tmp_path,
            opener=opener,
            stream=stream,
            capacity=8192,
            drain_delay_seconds=0,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=1,
        )
        original_accept = collector._accept

        async def accept(receipt: Any, writer: Any, selected_stop: asyncio.Event) -> Any:
            result = await original_accept(receipt, writer, selected_stop)
            # Stop may also become set after wait() but while handing off a frame.
            if len(payloads) == 3 and stop_during_handoff:
                request_stop()
            return result

        monkeypatch.setattr(collector, "_accept", accept)
        try:
            await asyncio.wait_for(collector.run(stop, restart), timeout=2)
            assert socket.close_reasons == ["collector shutdown"]
            return collector.receipt_queue_stats
        finally:
            catalog.close()

    stats = asyncio.run(exercise())
    frames, manifests = support.captured(tmp_path)
    assert len(payloads) == 3
    assert [frame.raw_payload for frame in frames] == payloads
    assert stats.owned_bytes == stats.queued_bytes == stats.depth == 0
    assert manifests
    if session_restart:
        assert any(document["gap"] and not document["complete"] for document in manifests)
    else:
        assert all(document["complete"] and not document["gap"] for document in manifests)
