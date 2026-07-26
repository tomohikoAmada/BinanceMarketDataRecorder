from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, ClassVar, cast

from binance_market_data_recorder.binance.usdm.side_data_rest import (
    PublicResponse,
    UsdMSideRestApi,
)
from binance_market_data_recorder.binance.usdm.websocket import WebSocketConnection
from binance_market_data_recorder.collector.usdm import UsdMCollector, UsdMCollectorSettings
from binance_market_data_recorder.collector.usdm_side_data import UsdMSideDataSettings

FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance" / "usdm"


class SnapshotModel:
    last_update_id = 99

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": 99, "E": 1, "T": 1, "bids": [], "asks": []}


class SnapshotResponse:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "1"}

    def data(self) -> SnapshotModel:
        return SnapshotModel()


class SnapshotApi:
    def order_book(self, symbol: str, limit: int) -> SnapshotResponse:
        assert (symbol, limit) == ("BTCUSDT", 1000)
        return SnapshotResponse()


class Model:
    def __init__(self, value: Any) -> None:
        self.value = value

    def to_dict(self) -> Any:
        return self.value


class Response:
    status = 200
    headers: ClassVar[dict[str, object]] = {"X-MBX-USED-WEIGHT-1M": "7"}

    def __init__(self, value: Any) -> None:
        self.value = value

    def data(self) -> object:
        if isinstance(self.value, list):
            return [Model(item) for item in self.value]
        return Model(self.value)


class SideApi:
    def __init__(self) -> None:
        self.open_interest_failures = 1

    def response(self, fixture: str) -> PublicResponse:
        return Response(json.loads((FIXTURES / fixture).read_text()))

    def mark_price(self, symbol: str | None = None) -> PublicResponse:
        assert symbol == "BTCUSDT"
        return self.response("premium_index.json")

    def get_funding_rate_history(
        self,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
    ) -> PublicResponse:
        assert (symbol, start_time, end_time, limit) == ("BTCUSDT", None, None, 100)
        return self.response("funding_history.json")

    def get_funding_rate_info(self) -> PublicResponse:
        return self.response("funding_info.json")

    def open_interest(self, symbol: str | None) -> PublicResponse:
        assert symbol == "BTCUSDT"
        if self.open_interest_failures:
            self.open_interest_failures -= 1
            raise RuntimeError("injected isolated side-data failure")
        return self.response("open_interest.json")

    def exchange_information(self) -> PublicResponse:
        return self.response("exchange_info.json")


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


def test_side_data_failure_is_counted_without_stopping_core_capture(tmp_path: Path) -> None:
    core_payloads = {
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
    side_payloads = {
        "btcusdt@markPrice@1s": (FIXTURES / "mark_price.json").read_bytes().rstrip(),
        "btcusdt@forceOrder": (FIXTURES / "liquidation.json").read_bytes().rstrip(),
    }
    payloads = {**core_payloads, **side_payloads}

    def count(status: dict[str, dict[str, object]], stream: str, field: str) -> int:
        value = status[stream][field]
        assert isinstance(value, int) and not isinstance(value, bool)
        return value

    async def exercise() -> dict[str, dict[str, object]]:
        stop = asyncio.Event()
        opened: set[str] = set()

        @asynccontextmanager
        async def opener(url: str) -> AsyncIterator[WebSocketConnection]:
            wire_name = url.rsplit("/", 1)[-1]
            opened.add(wire_name)
            yield Socket(payloads[wire_name])

        side_settings = UsdMSideDataSettings(
            premium_index_interval_seconds=0.01,
            funding_history_interval_seconds=0.01,
            funding_info_interval_seconds=0.01,
            open_interest_interval_seconds=0.01,
            exchange_info_interval_seconds=0.01,
        )
        collector = UsdMCollector(
            UsdMCollectorSettings(
                data_root=tmp_path,
                collector_instance_id="m7-test",
                collector_version="0.1.0+test",
                durability_interval_seconds=0,
                side_data=side_settings,
            ),
            logger=logging.getLogger("test.usdm.side-data"),
            rest_api=SnapshotApi(),
            side_rest_api=cast(UsdMSideRestApi, SideApi()),
            websocket_opener=opener,
        )
        task = asyncio.create_task(collector.run(stop))
        for _ in range(300):
            status = collector.side_data_status()
            if (
                opened == set(payloads)
                and count(status, "open_interest", "failures") >= 1
                    and all(
                        not bool(item["enabled"])
                        or count(status, name, "accepted") >= 1
                        for name, item in status.items()
                    )
            ):
                stop.set()
                break
            await asyncio.sleep(0.01)
        await asyncio.wait_for(task, timeout=5)
        assert opened == set(payloads)
        assert collector.side_data is not None
        assert collector.side_data.supervisor.failures == {}
        assert collector.metrics.failure_count == 0
        return collector.side_data_status()

    status = asyncio.run(exercise())
    assert status["open_interest"]["failures"] == 1
    assert count(status, "open_interest", "accepted") >= 1
    manifests = [
        json.loads(path.read_text()) for path in (tmp_path / "data" / "manifests").glob("*.json")
    ]
    streams = {document["stream"] for document in manifests}
    assert {
        "diff_depth",
        "agg_trade",
        "book_ticker",
        "depth_snapshot",
        "mark_price",
        "liquidation",
        "premium_index_snapshot",
        "funding_history",
        "funding_info",
        "open_interest",
        "exchange_info",
    } <= streams
    report_paths = sorted((tmp_path / "data" / "reports" / "daily").glob("*.json"))
    assert report_paths
    report = json.loads(report_paths[-1].read_text())
    report_streams = {row["stream"]: row for row in report["streams"]}
    assert streams <= set(report_streams)
    for name in streams:
        row = report_streams[name]
        input_records = row["input"]["websocket_messages"] + row["input"]["rest_responses"]
        assert input_records == row["output"]["raw_records_written"]
