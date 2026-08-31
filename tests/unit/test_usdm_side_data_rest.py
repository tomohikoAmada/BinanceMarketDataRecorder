from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

import pytest

from binance_market_data_recorder.binance.usdm.side_data_rest import (
    REST_SIDE_DATA_SPECS,
    PublicResponse,
    RestSideDataKind,
    SideDataSchemaError,
    UsdMSideDataHttpError,
    capture_rest_side_data,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance" / "usdm"


class Model:
    def __init__(self, value: Any) -> None:
        self.value = value

    def to_dict(self) -> Any:
        return self.value


class Response:
    status = 200
    headers: ClassVar[dict[str, object]] = {
        "X-MBX-USED-WEIGHT-1M": "7",
        "Set-Cookie": "must-not-be-stored",
    }

    def __init__(self, value: Any) -> None:
        self.value = value

    def data(self) -> object:
        if isinstance(self.value, list):
            return [Model(item) for item in self.value]
        return Model(self.value)


class RateLimitedResponse(Response):
    status = 429
    headers: ClassVar[dict[str, object]] = {"Retry-After": "17"}


class Api:
    def __init__(self, fixture: str) -> None:
        self.value = json.loads((FIXTURES / fixture).read_text())
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def _response(self, name: str, *args: object) -> PublicResponse:
        self.calls.append((name, args))
        return Response(self.value)

    def mark_price(self, symbol: str | None = None) -> PublicResponse:
        return self._response("mark_price", symbol)

    def get_funding_rate_history(
        self,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
    ) -> PublicResponse:
        return self._response("funding_history", symbol, start_time, end_time, limit)

    def get_funding_rate_info(self) -> PublicResponse:
        return self._response("funding_info")

    def open_interest(self, symbol: str | None) -> PublicResponse:
        return self._response("open_interest", symbol)

    def exchange_information(self) -> PublicResponse:
        return self._response("exchange_info")

    def open_interest_statistics(self, *args: object) -> PublicResponse:
        return self._response("open_interest_statistics", *args)

    def taker_buy_sell_volume(self, *args: object) -> PublicResponse:
        return self._response("taker_buy_sell_volume", *args)

    def long_short_ratio(self, *args: object) -> PublicResponse:
        return self._response("long_short_ratio", *args)

    def top_trader_long_short_ratio_accounts(self, *args: object) -> PublicResponse:
        return self._response("top_accounts", *args)

    def top_trader_long_short_ratio_positions(self, *args: object) -> PublicResponse:
        return self._response("top_positions", *args)

    def basis(self, *args: object) -> PublicResponse:
        return self._response("basis", *args)


@pytest.mark.parametrize(
    ("kind", "fixture", "call"),
    [
        (RestSideDataKind.PREMIUM_INDEX, "premium_index.json", ("mark_price", ("BTCUSDT",))),
        (
            RestSideDataKind.FUNDING_HISTORY,
            "funding_history.json",
            ("funding_history", ("BTCUSDT", None, None, 100)),
        ),
        (RestSideDataKind.FUNDING_INFO, "funding_info.json", ("funding_info", ())),
        (RestSideDataKind.OPEN_INTEREST, "open_interest.json", ("open_interest", ("BTCUSDT",))),
        (RestSideDataKind.EXCHANGE_INFO, "exchange_info.json", ("exchange_info", ())),
    ],
)
def test_rest_side_data_uses_only_public_sdk_methods_and_records_provenance(
    kind: RestSideDataKind, fixture: str, call: tuple[str, tuple[object, ...]]
) -> None:
    api = Api(fixture)
    wall = iter([100, 200])
    monotonic = iter([300, 400])
    envelope = capture_rest_side_data(
        kind=kind,
        rest_api=api,
        collector_instance_id="collector-1",
        collector_version="test",
        utc_clock_ns=lambda: next(wall),
        monotonic_clock_ns=lambda: next(monotonic),
    )
    provenance = json.loads(envelope.raw_payload)
    assert api.calls == [call]
    assert envelope.stream == kind.value
    assert provenance["request"]["path"] == REST_SIDE_DATA_SPECS[kind].path
    assert provenance["request"]["documented_rate_limit"]
    assert provenance["response"]["headers"] == {"x-mbx-used-weight-1m": "7"}
    assert provenance["transport"]["raw_http_body_available"] is False
    assert "apiKey" not in envelope.raw_payload.decode()
    assert "secret" not in envelope.raw_payload.decode().lower()
    assert "no_forward_fill" in envelope.capture_flags


def test_funding_info_missing_btcusdt_remains_absent_without_fixed_eight_hour_fill() -> None:
    envelope = capture_rest_side_data(
        kind=RestSideDataKind.FUNDING_INFO,
        rest_api=Api("funding_info.json"),
        collector_instance_id="collector-1",
        collector_version="test",
    )
    model = json.loads(envelope.raw_payload)["response"]["model"]
    assert all(item["symbol"] != "BTCUSDT" for item in model)
    assert model[0]["fundingIntervalHours"] == 8
    assert envelope.source_sequence == {"recordCount": 1}


def test_empty_funding_responses_are_valid_and_not_forward_filled() -> None:
    api = Api("funding_history.json")
    api.value = []
    envelope = capture_rest_side_data(
        kind=RestSideDataKind.FUNDING_HISTORY,
        rest_api=api,
        collector_instance_id="collector-1",
        collector_version="test",
    )
    assert json.loads(envelope.raw_payload)["response"]["model"] == []
    assert envelope.source_sequence == {"recordCount": 0}


def test_invalid_side_data_schema_is_not_silently_accepted() -> None:
    api = Api("open_interest.json")
    api.value = {"symbol": "BTCUSDT", "time": 1}
    with pytest.raises(SideDataSchemaError, match="openInterest"):
        capture_rest_side_data(
            kind=RestSideDataKind.OPEN_INTEREST,
            rest_api=api,
            collector_instance_id="collector-1",
            collector_version="test",
        )


def test_http_rate_limit_preserves_retry_after_boundary() -> None:
    api = Api("open_interest.json")
    response = RateLimitedResponse({})
    api._response = lambda name, *args: response  # type: ignore[method-assign]
    with pytest.raises(UsdMSideDataHttpError) as raised:
        capture_rest_side_data(
            kind=RestSideDataKind.OPEN_INTEREST,
            rest_api=api,
            collector_instance_id="collector-1",
            collector_version="test",
            utc_clock_ns=iter([1_000_000_000, 2_000_000_000]).__next__,
            monotonic_clock_ns=iter([1, 2]).__next__,
        )
    assert raised.value.status == 429
    assert raised.value.retry_after_seconds == 17
    assert raised.value.retry_at_utc_ns == 19_000_000_000


@pytest.mark.parametrize(
    ("kind", "model", "call_name"),
    [
        (
            RestSideDataKind.OPEN_INTEREST_STATISTICS,
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "1",
                "sumOpenInterestValue": "2",
                "timestamp": 300_000,
            },
            "open_interest_statistics",
        ),
        (
            RestSideDataKind.TAKER_BUY_SELL_VOLUME,
            {
                "buySellRatio": "1",
                "buyVol": "2",
                "sellVol": "2",
                "timestamp": 300_000,
            },
            "taker_buy_sell_volume",
        ),
        (
            RestSideDataKind.GLOBAL_LONG_SHORT_RATIO,
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1",
                "longAccount": "0.5",
                "shortAccount": "0.5",
                "timestamp": 300_000,
            },
            "long_short_ratio",
        ),
        (
            RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO,
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1",
                "longAccount": "0.5",
                "shortAccount": "0.5",
                "timestamp": 300_000,
            },
            "top_accounts",
        ),
        (
            RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO,
            {
                "symbol": "BTCUSDT",
                "longShortRatio": "1",
                "longAccount": "0.5",
                "shortAccount": "0.5",
                "timestamp": 300_000,
            },
            "top_positions",
        ),
        (
            RestSideDataKind.BASIS,
            {
                "pair": "BTCUSDT",
                "contractType": "PERPETUAL",
                "indexPrice": "1",
                "futuresPrice": "1",
                "basis": "0",
                "basisRate": "0",
                "annualizedBasisRate": "",
                "timestamp": 300_000,
            },
            "basis",
        ),
    ],
)
def test_five_minute_statistics_capture_latest_closed_period(
    kind: RestSideDataKind, model: dict[str, object], call_name: str
) -> None:
    api = Api("funding_history.json")
    api.value = [model]
    wall = iter([600_000_000_000, 600_100_000_000])
    envelope = capture_rest_side_data(
        kind=kind,
        rest_api=api,
        collector_instance_id="collector-1",
        collector_version="test",
        utc_clock_ns=lambda: next(wall),
        monotonic_clock_ns=iter([1, 2]).__next__,
    )
    provenance = json.loads(envelope.raw_payload)
    assert api.calls[0][0] == call_name
    assert provenance["request"]["parameters"]["period"] == "5m"
    assert provenance["request"]["parameters"]["startTime"] == 300_000
    expected_end = (
        899_999
        if kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME
        else 599_999
    )
    assert provenance["request"]["parameters"]["endTime"] == expected_end
    assert envelope.source_sequence["recordCount"] == 1
    assert envelope.source_sequence["period"] == "5m"


def test_taker_volume_preserves_official_leading_overlap_but_advances_requested_range() -> None:
    api = Api("funding_history.json")
    api.value = [
        {
            "buySellRatio": "1",
            "buyVol": "2",
            "sellVol": "2",
            "timestamp": 0,
        },
        {
            "buySellRatio": "1",
            "buyVol": "3",
            "sellVol": "3",
            "timestamp": 300_000,
        },
    ]
    envelope = capture_rest_side_data(
        kind=RestSideDataKind.TAKER_BUY_SELL_VOLUME,
        rest_api=api,
        collector_instance_id="collector-1",
        collector_version="test",
        period_start_ms=300_000,
        period_end_ms=599_999,
        period_limit=1,
    )
    assert api.calls[0][1][2] == 2
    assert api.calls[0][1][4] == 899_999
    provenance = json.loads(envelope.raw_payload)
    assert provenance["request"]["parameters"]["requestedEndTime"] == 599_999
    assert envelope.source_sequence["recordCount"] == 2
    assert envelope.source_sequence["firstTimestamp"] == 0
    assert envelope.source_sequence["requestedRecordCount"] == 1
    assert envelope.source_sequence["firstRequestedTimestamp"] == 300_000
    assert envelope.source_sequence["lastRequestedTimestamp"] == 300_000
