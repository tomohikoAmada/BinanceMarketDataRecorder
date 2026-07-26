from __future__ import annotations

import json
from typing import Any, ClassVar

from binance_market_data_recorder.binance.spot.exchange_info import (
    capture_spot_exchange_info,
)


class Model:
    def to_dict(self) -> dict[str, Any]:
        return {
            "serverTime": 123,
            "rateLimits": [],
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "orderTypes": ["LIMIT", "MARKET"],
                    "filters": [{"filterType": "PRICE_FILTER"}],
                }
            ],
        }


class Response:
    status = 200
    headers: ClassVar[dict[str, object]] = {
        "X-MBX-USED-WEIGHT-1M": "20",
        "Set-Cookie": "not-recorded",
    }

    def data(self) -> object:
        return Model()


class Api:
    def __init__(self) -> None:
        self.symbol: str | None = None

    def exchange_info(
        self,
        symbol: str | None = None,
        symbols: list[str] | None = None,
        permissions: object | None = None,
        show_permission_sets: bool | None = None,
        symbol_status: object | None = None,
    ) -> Response:
        self.symbol = symbol
        return Response()


def test_spot_exchange_info_preserves_rules_status_headers_and_time_source() -> None:
    api = Api()
    envelope = capture_spot_exchange_info(
        rest_api=api,
        collector_instance_id="spot-1",
        collector_version="test",
        utc_clock_ns=iter([100, 200]).__next__,
        monotonic_clock_ns=iter([300, 400]).__next__,
    )
    provenance = json.loads(envelope.raw_payload)
    symbol = provenance["response"]["model"]["symbols"][0]
    assert api.symbol == "BTCUSDT"
    assert symbol["status"] == "TRADING"
    assert symbol["orderTypes"] == ["LIMIT", "MARKET"]
    assert symbol["filters"][0]["filterType"] == "PRICE_FILTER"
    assert provenance["response"]["headers"] == {"x-mbx-used-weight-1m": "20"}
    assert envelope.exchange_event_time == 123
    assert envelope.receive_time_utc_ns == 200
    assert envelope.capture_flags == (
        "rest_snapshot",
        "official_sdk_model_no_raw_http_body",
    )
