"""Offline P1-R1 reproduction using the real Spot/USD-M receive methods.

Run from the repository: PYTHONPATH=. .venv/bin/python docs/milestones/P1/
reproduce_receive_retirement.py. Exit 1 means a retirement stalled. No network
or production storage is used; the writer is deliberately idle and healthy.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

from binance_market_data_recorder.binance.spot.schema import SpotStream
from tests.integration.test_spot_ingress_backpressure import (
    make_collector as make_spot,
)
from tests.integration.test_usdm_ingress_backpressure import (
    make_collector as make_usdm,
)


class BufferedThenQuietSocket:
    def __init__(self, buffered: int) -> None:
        self.buffered = buffered
        self.calls = 0
        self.close_reasons: list[str] = []

    async def recv(self, decode: bool = False) -> bytes:
        self.calls += 1
        if self.calls > self.buffered:
            await asyncio.Future[None]()
        return b"{}"

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_reasons.append(reason)


async def exercise(root: Path, market: str, boundary: str) -> dict[str, object]:
    kwargs = dict(
        opener=None, capacity=32, drain_delay_seconds=0,
        put_timeout_seconds=1, saturation_timeout_seconds=1,
    )
    if market == "spot":
        collector, catalog = make_spot(root, stream=SpotStream.BOOK_TICKER, **kwargs)
    else:
        collector, catalog = make_usdm(root, **kwargs)
    # An already-due deadline avoids a wall-clock race with a multi-hour timer.
    collector.planned_rotation_seconds = 1e-9 if boundary == "rotation" else 60
    if boundary == "server_shutdown":
        collector._server_shutdown.set()
    socket = BufferedThenQuietSocket(2 if boundary == "rotation" else 3)
    stop = asyncio.Event()
    writer = asyncio.create_task(asyncio.Event().wait())
    receive = asyncio.create_task(collector._receive_connection(socket, "review", stop, writer))
    try:
        done, _ = await asyncio.wait({receive}, timeout=0.1)
        retired = bool(done)
        outcome = receive.result() if retired else None
        stop.set()
        done, _ = await asyncio.wait({receive}, timeout=0.1)
        return {
            "market": market, "boundary": boundary, "buffered": socket.buffered,
            "recv_calls": socket.calls, "retired_without_new_message": retired,
            "finished_after_stop": bool(done), "outcome": outcome,
            "close_reasons": socket.close_reasons,
        }
    finally:
        receive.cancel()
        writer.cancel()
        await asyncio.gather(receive, writer, return_exceptions=True)
        catalog.close()


async def main(root: Path) -> bool:
    cases = [("spot", "rotation"), ("usdm", "rotation"), ("spot", "server_shutdown")]
    results = [await exercise(root / f"{m}-{b}", m, b) for m, b in cases]
    print(json.dumps(results, indent=2))
    return all(result["retired_without_new_message"] for result in results)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="recorder-p1-review-") as directory:
        raise SystemExit(0 if asyncio.run(main(Path(directory))) else 1)
