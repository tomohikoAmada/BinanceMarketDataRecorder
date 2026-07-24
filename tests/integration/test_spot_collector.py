from __future__ import annotations

import asyncio
import json
import logging
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, ClassVar

import pytest

from binance_market_data_recorder.binance.spot.rate_limit import SpotIpRateLimiter
from binance_market_data_recorder.binance.spot.rest import (
    DepthResponse,
    SpotSnapshotRequester,
)
from binance_market_data_recorder.binance.spot.websocket import WebSocketConnection
from binance_market_data_recorder.collector.spot import SpotCollector, SpotCollectorSettings
from binance_market_data_recorder.paths import UnsafeDataRootError
from tests.integration.test_spot_stream_collector import sealed_envelopes


class Model:
    last_update_id = 100

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": 100, "bids": [], "asks": []}


class Response:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "250"}

    def data(self) -> Model:
        return Model()


class RestApi:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures

    def depth(self, symbol: str, limit: int) -> DepthResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("injected public snapshot failure")
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


def test_complete_spot_collector_assembles_three_streams_and_snapshot(tmp_path: Path) -> None:
    payloads = {
        "btcusdt@depth@100ms": (
            b'{"e":"depthUpdate","E":1,"s":"BTCUSDT",'
            b'"U":100,"u":101,"b":[],"a":[]}'
        ),
        "btcusdt@aggTrade": (
            b'{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"1","q":"1",'
            b'"f":1,"l":1,"T":1,"m":true,"M":true}'
        ),
        "btcusdt@bookTicker": b'{"u":101,"s":"BTCUSDT","b":"1","B":"1","a":"2","A":"1"}',
    }

    async def exercise() -> None:
        stop = asyncio.Event()
        opened: set[str] = set()

        @asynccontextmanager
        async def opener(url: str) -> AsyncIterator[WebSocketConnection]:
            wire_name = url.rsplit("/", 1)[-1]
            opened.add(wire_name)
            yield Socket(payloads[wire_name])

        collector = SpotCollector(
            SpotCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="collector-test",
                collector_version="0.1.0+test",
                durability_interval_seconds=0,
                snapshot_retry_initial_seconds=0.001,
                snapshot_retry_maximum_seconds=0.001,
            ),
            logger=logging.getLogger("test.spot.complete"),
            rest_api=RestApi(failures=1),
            websocket_opener=opener,
        )
        collector.set_handoff_context(
            deployment_id="spot-deployment",
            role="candidate",
            reason="UPGRADE",
        )
        task = asyncio.create_task(collector.run(stop))
        for _ in range(100):
            if opened == set(payloads) and collector.readiness_snapshot().ready:
                stop.set()
                break
            await asyncio.sleep(0.01)
        assert collector.readiness_snapshot().ready
        await asyncio.wait_for(task, timeout=3)

    asyncio.run(exercise())
    documents: list[dict[str, Any]] = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (tmp_path / "data" / "manifests").glob("*.json")
    ]
    assert {document["stream"] for document in documents} == {
        "diff_depth",
        "agg_trade",
        "book_ticker",
        "depth_snapshot",
    }
    assert sum(document["record_count"] for document in documents) >= 4
    for envelope in sealed_envelopes(tmp_path):
        assert "blue_green_overlap" in envelope.capture_flags
        assert "deployment_id=spot-deployment" in envelope.capture_flags
        assert "instance_role=candidate" in envelope.capture_flags


def test_spot_collector_refuses_repository_as_data_root() -> None:
    with pytest.raises(UnsafeDataRootError, match="unsafe data root"):
        SpotCollector(
            SpotCollectorSettings(
                data_root=Path.cwd(),
                collector_instance_id="collector-test",
                collector_version="0.1.0+test",
            ),
            logger=logging.getLogger("test.spot.unsafe"),
            rest_api=RestApi(),
        )


def test_bootstrap_buffer_overflow_restarts_connections_and_snapshot(
    tmp_path: Path,
) -> None:
    class RestartRestApi:
        def __init__(self) -> None:
            self.calls = 0
            self.first_started = threading.Event()
            self.release_first = threading.Event()

        def depth(self, symbol: str, limit: int) -> DepthResponse:
            self.calls += 1
            if self.calls == 1:
                self.first_started.set()
                assert self.release_first.wait(timeout=2)
            return Response()

    class SequenceSocket:
        def __init__(self, payloads: list[bytes]) -> None:
            self.payloads = payloads

        async def recv(self, decode: bool | None = None) -> bytes:
            if self.payloads:
                return self.payloads.pop(0)
            await asyncio.Future[None]()
            raise AssertionError("unreachable")

        async def close(self, code: int = 1000, reason: str = "") -> None:
            return None

    async def exercise() -> None:
        stop = asyncio.Event()
        rest_api = RestartRestApi()
        opens: dict[str, int] = {}

        @asynccontextmanager
        async def opener(url: str) -> AsyncIterator[WebSocketConnection]:
            wire_name = url.rsplit("/", 1)[-1]
            ordinal = opens.get(wire_name, 0) + 1
            opens[wire_name] = ordinal
            if wire_name == "btcusdt@depth@100ms":
                if ordinal == 1:
                    payloads = [
                        (
                            b'{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":'
                            + str(sequence).encode()
                            + b',"u":'
                            + str(sequence).encode()
                            + b',"b":[],"a":[]}'
                        )
                        for sequence in range(90, 94)
                    ]
                else:
                    payloads = [
                        b'{"e":"depthUpdate","E":2,"s":"BTCUSDT",'
                        b'"U":101,"u":110,"b":[],"a":[]}'
                    ]
            elif wire_name == "btcusdt@aggTrade":
                payloads = [
                    b'{"e":"aggTrade","E":1,"s":"BTCUSDT","a":1,"p":"1",'
                    b'"q":"1","f":1,"l":1,"T":1,"m":true,"M":true}'
                ]
            else:
                payloads = [
                    b'{"u":110,"s":"BTCUSDT","b":"1","B":"1","a":"2","A":"1"}'
                ]
            yield SequenceSocket(payloads)

        requester = SpotSnapshotRequester(
            rest_api=rest_api,
            rate_limiter=SpotIpRateLimiter(weight_budget_per_minute=1_000_000_000),
        )
        collector = SpotCollector(
            SpotCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="collector-overflow",
                collector_version="test",
                durability_interval_seconds=0,
                bootstrap_buffer_capacity=2,
                snapshot_retry_initial_seconds=0.001,
                snapshot_retry_maximum_seconds=0.001,
            ),
            logger=logging.getLogger("test.spot.bootstrap-overflow"),
            rest_api=rest_api,
            websocket_opener=opener,
            snapshot_requester=requester,
        )
        task = asyncio.create_task(collector.run(stop))
        assert await asyncio.to_thread(rest_api.first_started.wait, 1)
        for _ in range(100):
            if collector._bootstrap_restart.is_set():
                rest_api.release_first.set()
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("bootstrap buffer did not request a restart")
        for _ in range(200):
            if collector.readiness_snapshot().ready:
                stop.set()
                break
            await asyncio.sleep(0.01)
        assert collector.readiness_snapshot().ready
        await asyncio.wait_for(task, timeout=3)
        assert opens["btcusdt@depth@100ms"] >= 2
        assert 2 <= rest_api.calls <= 3
        assert requester.inflight_count() == 0

    asyncio.run(exercise())
