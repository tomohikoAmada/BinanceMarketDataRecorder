from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, ClassVar

from binance_market_data_recorder.binance.usdm.rest import DepthResponse
from binance_market_data_recorder.binance.usdm.websocket import WebSocketConnection
from binance_market_data_recorder.collector.usdm import UsdMCollector, UsdMCollectorSettings


class Model:
    last_update_id = 99

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": 99, "E": 1, "T": 1, "bids": [], "asks": []}


class Response:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "20"}

    def data(self) -> Model:
        return Model()


class RestApi:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("injected snapshot failure")
        return Response()


class Socket:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.sent = False

    async def recv(self, decode: bool | None = None) -> bytes:
        if not self.sent:
            self.sent = True
            return self.payload
        await asyncio.Future[None]()
        raise AssertionError("unreachable")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


def test_complete_usdm_collector_uses_routed_streams_and_snapshot(tmp_path: Path) -> None:
    payloads = {
        "btcusdt@depth@100ms": (
            b'{"e":"depthUpdate","E":1,"T":1,"s":"BTCUSDT","U":1,"u":1,"pu":0,"b":[],"a":[]}'
        ),
        "btcusdt@aggTrade": (
            b'{"e":"aggTrade","E":1,"T":1,"s":"BTCUSDT","a":1,"p":"1","q":"1","f":1,"l":1,"m":true}'
        ),
        "btcusdt@bookTicker": (
            b'{"e":"bookTicker","E":1,"T":1,"s":"BTCUSDT","u":1,"b":"1","B":"1","a":"2","A":"1"}'
        ),
    }

    async def exercise() -> None:
        stop = asyncio.Event()
        opened: set[str] = set()
        routes: dict[str, str] = {}

        @asynccontextmanager
        async def opener(url: str) -> AsyncIterator[WebSocketConnection]:
            wire_name = url.rsplit("/", 1)[-1]
            opened.add(wire_name)
            routes[wire_name] = url.split("/")[-3]
            yield Socket(payloads[wire_name])

        collector = UsdMCollector(
            UsdMCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="usdm-test",
                collector_version="0.1.0+test",
                durability_interval_seconds=0,
                snapshot_retry_initial_seconds=0.001,
                snapshot_retry_maximum_seconds=0.001,
                snapshot_retry_jitter_ratio=0,
            ),
            logger=logging.getLogger("test.usdm.complete"),
            rest_api=RestApi(failures=1),
            websocket_opener=opener,
        )
        task = asyncio.create_task(collector.run(stop))
        for _ in range(100):
            if opened == set(payloads):
                await asyncio.sleep(0.05)
                stop.set()
                break
            await asyncio.sleep(0.01)
        await asyncio.wait_for(task, timeout=3)
        assert routes == {
            "btcusdt@depth@100ms": "public",
            "btcusdt@aggTrade": "market",
            "btcusdt@bookTicker": "public",
        }

    asyncio.run(exercise())
    documents: list[dict[str, Any]] = [
        json.loads(path.read_text()) for path in (tmp_path / "data" / "manifests").glob("*.json")
    ]
    assert {document["market"] for document in documents} == {"um_perpetual"}
    assert {document["stream"] for document in documents} == {
        "diff_depth",
        "agg_trade",
        "book_ticker",
        "depth_snapshot",
    }
    assert sum(int(document["record_count"]) for document in documents) == 4
