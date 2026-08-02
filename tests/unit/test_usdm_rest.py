from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from binance_market_data_recorder.binance.usdm.rest import (
    DepthResponse,
    UsdMSnapshotHttpError,
    UsdMSnapshotResponseError,
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


def test_usdm_snapshot_rejects_bad_limit_permanent_http_and_missing_id() -> None:
    with pytest.raises(ValueError, match="not supported"):
        capture_depth_snapshot(
            rest_api=FakeApi(), collector_instance_id="c", collector_version="v", limit=5000
        )
    with pytest.raises(UsdMSnapshotHttpError, match="HTTP 400") as captured:
        capture_depth_snapshot(
            rest_api=FakeApi(FakeResponse(status=400)),
            collector_instance_id="c",
            collector_version="v",
        )
    assert captured.value.status == 400
    assert captured.value.rate_limited is False
    assert captured.value.retry_after_seconds is None
    assert captured.value.retry_at_utc_ns is None
    with pytest.raises(UsdMSnapshotResponseError, match="no lastUpdateId"):
        capture_depth_snapshot(
            rest_api=FakeApi(FakeResponse(model=FakeModel(last_update_id=None))),
            collector_instance_id="c",
            collector_version="v",
        )


def test_usdm_snapshot_http_error_retains_only_safe_retry_evidence() -> None:
    wall = iter([1_000_000_000, 2_000_000_000])
    response = FakeResponse(
        status=429,
        headers={
            "Retry-After": "1.5",
            "X-MBX-USED-WEIGHT-1M": "20",
            "Authorization": "must-not-be-retained",
            "Set-Cookie": "must-not-be-retained",
        },
    )
    with pytest.raises(UsdMSnapshotHttpError, match="HTTP 429") as captured:
        capture_depth_snapshot(
            rest_api=FakeApi(response),
            collector_instance_id="c",
            collector_version="v",
            utc_clock_ns=lambda: next(wall),
        )
    assert captured.value.status == 429
    assert captured.value.rate_limited is True
    assert captured.value.headers == {
        "retry-after": "1.5",
        "x-mbx-used-weight-1m": "20",
    }
    assert captured.value.retry_after_seconds == 1.5
    assert captured.value.retry_at_utc_ns == 3_500_000_000


def test_usdm_snapshot_http_error_uses_sdk_retry_after_evidence() -> None:
    class RateLimit:
        retryAfter = 2

    class RateLimitedResponse(FakeResponse):
        rate_limits = (RateLimit(),)

    wall = iter([1_000_000_000, 2_000_000_000])
    with pytest.raises(UsdMSnapshotHttpError, match="HTTP 418") as captured:
        capture_depth_snapshot(
            rest_api=FakeApi(RateLimitedResponse(status=418, headers={})),
            collector_instance_id="c",
            collector_version="v",
            utc_clock_ns=lambda: next(wall),
        )
    assert captured.value.retry_after_seconds == 2
    assert captured.value.retry_at_utc_ns == 4_000_000_000


def test_usdm_snapshot_model_parse_failure_is_a_fatal_response_error() -> None:
    class InvalidModelResponse(FakeResponse):
        def data(self) -> FakeModel:
            raise ValueError("invalid model")

    with pytest.raises(UsdMSnapshotResponseError, match="could not be parsed"):
        capture_depth_snapshot(
            rest_api=FakeApi(InvalidModelResponse()),
            collector_instance_id="c",
            collector_version="v",
        )


def test_usdm_snapshot_to_dict_failure_and_invalid_schema_remain_fatal() -> None:
    class ToDictFailureModel(FakeModel):
        def to_dict(self) -> dict[str, object]:
            raise ValueError("invalid document")

    with pytest.raises(UsdMSnapshotResponseError, match="could not be parsed"):
        capture_depth_snapshot(
            rest_api=FakeApi(FakeResponse(model=ToDictFailureModel())),
            collector_instance_id="c",
            collector_version="v",
        )

    class InvalidSchemaModel(FakeModel):
        def to_dict(self) -> dict[str, object]:
            return {"bids": [], "asks": []}

    with pytest.raises(UsdMSnapshotResponseError, match="schema is invalid"):
        capture_depth_snapshot(
            rest_api=FakeApi(FakeResponse(model=InvalidSchemaModel())),
            collector_instance_id="c",
            collector_version="v",
        )
