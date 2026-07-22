from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from binance_market_data_recorder.binance.usdm.rest import (
    DepthResponse,
    capture_depth_snapshot,
)


@dataclass
class FakeModel:
    last_update_id: int | None = 42

    def to_dict(self) -> dict[str, object]:
        return {"lastUpdateId": self.last_update_id, "E": 10, "T": 9, "bids": [], "asks": []}


@dataclass
class FakeResponse:
    status: int = 200
    headers: dict[str, object] = field(
        default_factory=lambda: {
            "X-MBX-USED-WEIGHT-1M": "20",
            "Set-Cookie": "must-not-be-stored",
        }
    )
    model: FakeModel = field(default_factory=FakeModel)

    def data(self) -> FakeModel:
        return self.model


class FakeApi:
    def __init__(self, response: FakeResponse | None = None) -> None:
        self.response = response or FakeResponse()
        self.calls: list[tuple[str, int]] = []

    def order_book(self, symbol: str, limit: int) -> DepthResponse:
        self.calls.append((symbol, limit))
        return self.response


def test_usdm_snapshot_uses_unsigned_public_sdk_method_and_provenance() -> None:
    api = FakeApi()
    wall = iter([100, 200])
    monotonic = iter([300, 400])
    captured = capture_depth_snapshot(
        rest_api=api,
        collector_instance_id="collector-1",
        collector_version="test",
        utc_clock_ns=lambda: next(wall),
        monotonic_clock_ns=lambda: next(monotonic),
    )
    provenance = json.loads(captured.raw_payload)
    assert api.calls == [("BTCUSDT", 1000)]
    assert captured.market == "um_perpetual"
    assert captured.source_sequence == {"lastUpdateId": 42}
    assert provenance["request"]["path"] == "/fapi/v1/depth"
    assert provenance["response"]["headers"] == {"x-mbx-used-weight-1m": "20"}
    assert provenance["transport"]["raw_http_body_available"] is False


def test_usdm_snapshot_rejects_bad_limit_http_and_missing_id() -> None:
    with pytest.raises(ValueError, match="not supported"):
        capture_depth_snapshot(
            rest_api=FakeApi(), collector_instance_id="c", collector_version="v", limit=5000
        )
    with pytest.raises(RuntimeError, match="HTTP 429"):
        capture_depth_snapshot(
            rest_api=FakeApi(FakeResponse(status=429)),
            collector_instance_id="c",
            collector_version="v",
        )
    with pytest.raises(RuntimeError, match="no lastUpdateId"):
        capture_depth_snapshot(
            rest_api=FakeApi(FakeResponse(model=FakeModel(last_update_id=None))),
            collector_instance_id="c",
            collector_version="v",
        )
