"""Unsigned official-SDK USD-M side-data snapshots and polling provenance."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from importlib.metadata import version
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)
from binance_sdk_derivatives_trading_usds_futures.rest_api.models.enums import (
    BasisContractTypeEnum,
    BasisPeriodEnum,
    LongShortRatioPeriodEnum,
    OpenInterestStatisticsPeriodEnum,
    TakerBuySellVolumePeriodEnum,
    TopTraderLongShortRatioAccountsPeriodEnum,
    TopTraderLongShortRatioPositionsPeriodEnum,
)

from ...domain.event import EventEnvelope
from .rest import USDM_SDK_DISTRIBUTION, safe_provenance_headers


class RestSideDataKind(StrEnum):
    PREMIUM_INDEX = "premium_index_snapshot"
    FUNDING_HISTORY = "funding_history"
    FUNDING_INFO = "funding_info"
    OPEN_INTEREST = "open_interest"
    EXCHANGE_INFO = "exchange_info"
    OPEN_INTEREST_STATISTICS = "open_interest_statistics_5m"
    TAKER_BUY_SELL_VOLUME = "taker_buy_sell_volume_5m"
    GLOBAL_LONG_SHORT_RATIO = "global_long_short_ratio_5m"
    TOP_LONG_SHORT_ACCOUNT_RATIO = "top_long_short_account_ratio_5m"
    TOP_LONG_SHORT_POSITION_RATIO = "top_long_short_position_ratio_5m"
    BASIS = "basis_5m"


@dataclass(frozen=True)
class RestSideDataSpec:
    kind: RestSideDataKind
    path: str
    semantics: str
    documented_rate_limit: str


REST_SIDE_DATA_SPECS: dict[RestSideDataKind, RestSideDataSpec] = {
    RestSideDataKind.PREMIUM_INDEX: RestSideDataSpec(
        RestSideDataKind.PREMIUM_INDEX,
        "/fapi/v1/premiumIndex",
        "polling_snapshot_no_forward_fill",
        "IP weight 1 with symbol",
    ),
    RestSideDataKind.FUNDING_HISTORY: RestSideDataSpec(
        RestSideDataKind.FUNDING_HISTORY,
        "/fapi/v1/fundingRate",
        "ascending_event_history_no_fixed_cadence_assumption",
        "shared 500 requests per 5 minutes per IP with /fapi/v1/fundingInfo",
    ),
    RestSideDataKind.FUNDING_INFO: RestSideDataSpec(
        RestSideDataKind.FUNDING_INFO,
        "/fapi/v1/fundingInfo",
        "sparse_adjustment_metadata_empty_or_missing_symbol_is_valid",
        "IP weight 0; shared 500 requests per 5 minutes per IP with /fapi/v1/fundingRate",
    ),
    RestSideDataKind.OPEN_INTEREST: RestSideDataSpec(
        RestSideDataKind.OPEN_INTEREST,
        "/fapi/v1/openInterest",
        "polling_snapshot_no_forward_fill",
        "IP weight 1",
    ),
    RestSideDataKind.EXCHANGE_INFO: RestSideDataSpec(
        RestSideDataKind.EXCHANGE_INFO,
        "/fapi/v1/exchangeInfo",
        "periodic_exchange_rules_and_filters_snapshot",
        "IP weight 1",
    ),
    RestSideDataKind.OPEN_INTEREST_STATISTICS: RestSideDataSpec(
        RestSideDataKind.OPEN_INTEREST_STATISTICS,
        "/futures/data/openInterestHist",
        "latest_closed_5m_period_no_forward_fill_latest_one_month",
        "IP weight 0; IP rate limit 1000 requests per 5 minutes",
    ),
    RestSideDataKind.TAKER_BUY_SELL_VOLUME: RestSideDataSpec(
        RestSideDataKind.TAKER_BUY_SELL_VOLUME,
        "/futures/data/takerlongshortRatio",
        "latest_closed_5m_period_no_forward_fill_latest_30_days",
        "IP weight 0; IP rate limit 1000 requests per 5 minutes",
    ),
    RestSideDataKind.GLOBAL_LONG_SHORT_RATIO: RestSideDataSpec(
        RestSideDataKind.GLOBAL_LONG_SHORT_RATIO,
        "/futures/data/globalLongShortAccountRatio",
        "latest_closed_5m_period_no_forward_fill_latest_30_days",
        "IP weight 0; IP rate limit 1000 requests per 5 minutes",
    ),
    RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO: RestSideDataSpec(
        RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO,
        "/futures/data/topLongShortAccountRatio",
        "latest_closed_5m_period_no_forward_fill_latest_30_days",
        "IP weight 0; IP rate limit 1000 requests per 5 minutes",
    ),
    RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO: RestSideDataSpec(
        RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO,
        "/futures/data/topLongShortPositionRatio",
        "latest_closed_5m_period_no_forward_fill_latest_30_days",
        "IP weight 0; IP rate limit 1000 requests per 5 minutes",
    ),
    RestSideDataKind.BASIS: RestSideDataSpec(
        RestSideDataKind.BASIS,
        "/futures/data/basis",
        "latest_closed_5m_perpetual_period_no_forward_fill_latest_30_days",
        "IP weight 0",
    ),
}


@runtime_checkable
class SdkModel(Protocol):
    def to_dict(self) -> Any: ...


class PublicResponse(Protocol):
    @property
    def status(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, object]: ...

    def data(self) -> object: ...


class UsdMSideRestApi(Protocol):
    def mark_price(self, symbol: str | None = None) -> PublicResponse: ...

    def get_funding_rate_history(
        self,
        symbol: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = None,
    ) -> PublicResponse: ...

    def get_funding_rate_info(self) -> PublicResponse: ...

    def open_interest(self, symbol: str | None) -> PublicResponse: ...

    def exchange_information(self) -> PublicResponse: ...

    def open_interest_statistics(
        self,
        symbol: str | None,
        period: OpenInterestStatisticsPeriodEnum | None,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> PublicResponse: ...

    def taker_buy_sell_volume(
        self,
        symbol: str | None,
        period: TakerBuySellVolumePeriodEnum | None,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> PublicResponse: ...

    def long_short_ratio(
        self,
        symbol: str | None,
        period: LongShortRatioPeriodEnum | None,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> PublicResponse: ...

    def top_trader_long_short_ratio_accounts(
        self,
        symbol: str | None,
        period: TopTraderLongShortRatioAccountsPeriodEnum | None,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> PublicResponse: ...

    def top_trader_long_short_ratio_positions(
        self,
        symbol: str | None,
        period: TopTraderLongShortRatioPositionsPeriodEnum | None,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> PublicResponse: ...

    def basis(
        self,
        pair: str | None,
        contract_type: BasisContractTypeEnum | None,
        period: BasisPeriodEnum | None,
        limit: int | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> PublicResponse: ...


class SideDataSchemaError(RuntimeError):
    """Raised when a public response cannot satisfy its official schema."""


def _text(value: dict[str, Any], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item:
        raise SideDataSchemaError(f"{name} must be non-empty text")
    return item


def _string(value: dict[str, Any], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise SideDataSchemaError(f"{name} must be text")
    return item


def _integer(value: dict[str, Any], name: str) -> int:
    item = value.get(name)
    if not isinstance(item, int) or isinstance(item, bool) or item < 0:
        raise SideDataSchemaError(f"{name} must be a non-negative integer")
    return item


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SideDataSchemaError("response model must be an object")
    return value


def _array(value: Any) -> list[Any]:
    if not isinstance(value, list):
        raise SideDataSchemaError("response model must be an array")
    return value


def _sdk_model_value(value: object) -> Any:
    if isinstance(value, list):
        return [_sdk_model_value(item) for item in value]
    if isinstance(value, SdkModel):
        return value.to_dict()
    raise SideDataSchemaError("SDK response data is not a model or model list")


def _validate_model(kind: RestSideDataKind, model: Any) -> dict[str, int | str]:
    if kind is RestSideDataKind.PREMIUM_INDEX:
        item = _object(model)
        if _text(item, "symbol") != "BTCUSDT":
            raise SideDataSchemaError("unexpected premium-index symbol")
        for name in (
            "markPrice",
            "indexPrice",
            "estimatedSettlePrice",
            "lastFundingRate",
            "interestRate",
        ):
            _text(item, name)
        return {
            "observationTime": _integer(item, "time"),
            "nextFundingTime": _integer(item, "nextFundingTime"),
        }
    if kind is RestSideDataKind.OPEN_INTEREST:
        item = _object(model)
        if _text(item, "symbol") != "BTCUSDT":
            raise SideDataSchemaError("unexpected open-interest symbol")
        _text(item, "openInterest")
        return {"observationTime": _integer(item, "time")}
    if kind is RestSideDataKind.FUNDING_HISTORY:
        items = _array(model)
        times: list[int] = []
        for raw_item in items:
            item = _object(raw_item)
            if _text(item, "symbol") != "BTCUSDT":
                raise SideDataSchemaError("unexpected funding-history symbol")
            _text(item, "fundingRate")
            if "markPrice" in item:
                _text(item, "markPrice")
            times.append(_integer(item, "fundingTime"))
        result: dict[str, int | str] = {"recordCount": len(items)}
        if times:
            result["firstFundingTime"] = min(times)
            result["lastFundingTime"] = max(times)
        return result
    if kind is RestSideDataKind.FUNDING_INFO:
        items = _array(model)
        for raw_item in items:
            item = _object(raw_item)
            _text(item, "symbol")
            _text(item, "adjustedFundingRateCap")
            _text(item, "adjustedFundingRateFloor")
            interval = _integer(item, "fundingIntervalHours")
            if interval == 0:
                raise SideDataSchemaError("fundingIntervalHours must be positive")
        return {"recordCount": len(items)}
    if kind in {
        RestSideDataKind.OPEN_INTEREST_STATISTICS,
        RestSideDataKind.TAKER_BUY_SELL_VOLUME,
        RestSideDataKind.GLOBAL_LONG_SHORT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO,
        RestSideDataKind.BASIS,
    }:
        items = _array(model)
        timestamps: list[int] = []
        for raw_item in items:
            item = _object(raw_item)
            if kind is RestSideDataKind.BASIS:
                if _text(item, "pair") != "BTCUSDT":
                    raise SideDataSchemaError("unexpected basis pair")
                for name in (
                    "contractType",
                    "indexPrice",
                    "futuresPrice",
                    "basis",
                    "basisRate",
                ):
                    _text(item, name)
                _string(item, "annualizedBasisRate")
            else:
                if (
                    kind is not RestSideDataKind.TAKER_BUY_SELL_VOLUME
                    and _text(item, "symbol") != "BTCUSDT"
                ):
                    raise SideDataSchemaError("unexpected statistics symbol")
                required = {
                    RestSideDataKind.OPEN_INTEREST_STATISTICS: (
                        "sumOpenInterest",
                        "sumOpenInterestValue",
                    ),
                    RestSideDataKind.TAKER_BUY_SELL_VOLUME: (
                        "buySellRatio",
                        "buyVol",
                        "sellVol",
                    ),
                    RestSideDataKind.GLOBAL_LONG_SHORT_RATIO: (
                        "longShortRatio",
                        "longAccount",
                        "shortAccount",
                    ),
                    RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO: (
                        "longShortRatio",
                        "longAccount",
                        "shortAccount",
                    ),
                    RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO: (
                        "longShortRatio",
                        "longAccount",
                        "shortAccount",
                    ),
                }[kind]
                for name in required:
                    _text(item, name)
            timestamps.append(_integer(item, "timestamp"))
        output: dict[str, int | str] = {
            "recordCount": len(items),
            "period": "5m",
        }
        if timestamps:
            output["firstTimestamp"] = min(timestamps)
            output["lastTimestamp"] = max(timestamps)
        return output
    item = _object(model)
    symbols = _array(item.get("symbols"))
    rate_limits = _array(item.get("rateLimits"))
    btc = [
        symbol
        for symbol in symbols
        if isinstance(symbol, dict) and symbol.get("symbol") == "BTCUSDT"
    ]
    if len(btc) != 1 or not isinstance(btc[0].get("filters"), list):
        raise SideDataSchemaError("exchange info has no BTCUSDT filters")
    return {"symbolCount": len(symbols), "rateLimitCount": len(rate_limits)}


def _call(
    api: UsdMSideRestApi, kind: RestSideDataKind, now_ms: int
) -> tuple[PublicResponse, dict[str, object]]:
    if kind is RestSideDataKind.PREMIUM_INDEX:
        return api.mark_price("BTCUSDT"), {"symbol": "BTCUSDT"}
    if kind is RestSideDataKind.FUNDING_HISTORY:
        return api.get_funding_rate_history("BTCUSDT", None, None, 100), {
            "symbol": "BTCUSDT",
            "limit": 100,
        }
    if kind is RestSideDataKind.FUNDING_INFO:
        return api.get_funding_rate_info(), {}
    if kind is RestSideDataKind.OPEN_INTEREST:
        return api.open_interest("BTCUSDT"), {"symbol": "BTCUSDT"}
    if kind is RestSideDataKind.EXCHANGE_INFO:
        return api.exchange_information(), {}
    period_end = (now_ms // 300_000) * 300_000 - 1
    period_start = period_end - 300_000 + 1
    parameters: dict[str, object] = {
        "symbol": "BTCUSDT",
        "period": "5m",
        "limit": 1,
        "startTime": period_start,
        "endTime": period_end,
    }
    if kind is RestSideDataKind.OPEN_INTEREST_STATISTICS:
        response = api.open_interest_statistics(
            "BTCUSDT",
            OpenInterestStatisticsPeriodEnum.PERIOD_5m,
            1,
            period_start,
            period_end,
        )
    elif kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME:
        response = api.taker_buy_sell_volume(
            "BTCUSDT",
            TakerBuySellVolumePeriodEnum.PERIOD_5m,
            1,
            period_start,
            period_end,
        )
    elif kind is RestSideDataKind.GLOBAL_LONG_SHORT_RATIO:
        response = api.long_short_ratio(
            "BTCUSDT",
            LongShortRatioPeriodEnum.PERIOD_5m,
            1,
            period_start,
            period_end,
        )
    elif kind is RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO:
        response = api.top_trader_long_short_ratio_accounts(
            "BTCUSDT",
            TopTraderLongShortRatioAccountsPeriodEnum.PERIOD_5m,
            1,
            period_start,
            period_end,
        )
    elif kind is RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO:
        response = api.top_trader_long_short_ratio_positions(
            "BTCUSDT",
            TopTraderLongShortRatioPositionsPeriodEnum.PERIOD_5m,
            1,
            period_start,
            period_end,
        )
    else:
        parameters["pair"] = parameters.pop("symbol")
        parameters["contractType"] = "PERPETUAL"
        response = api.basis(
            "BTCUSDT",
            BasisContractTypeEnum.PERPETUAL,
            BasisPeriodEnum.PERIOD_5m,
            1,
            period_start,
            period_end,
        )
    return response, parameters


def capture_rest_side_data(
    *,
    kind: RestSideDataKind,
    rest_api: UsdMSideRestApi | None = None,
    collector_instance_id: str,
    collector_version: str,
    timeout_ms: int = 10_000,
    utc_clock_ns: Callable[[], int] = time.time_ns,
    monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
) -> EventEnvelope:
    """Capture one credential-free SDK response without inventing missing values."""

    if timeout_ms < 1000:
        raise ValueError("USD-M side-data REST timeout must be at least 1000 ms")
    api = (
        rest_api
        if rest_api is not None
        else DerivativesTradingUsdsFutures(
            config_rest_api=ConfigurationRestAPI(timeout=timeout_ms, retries=0)
        ).rest_api
    )
    request_utc_ns = utc_clock_ns()
    request_monotonic_ns = monotonic_clock_ns()
    response, parameters = _call(api, kind, request_utc_ns // 1_000_000)
    receive_utc_ns = utc_clock_ns()
    receive_monotonic_ns = monotonic_clock_ns()
    if response.status != 200:
        raise RuntimeError(f"USD-M {kind.value} returned HTTP {response.status}")
    model = _sdk_model_value(response.data())
    source_sequence: dict[str, int | str] = _validate_model(kind, model)
    spec = REST_SIDE_DATA_SPECS[kind]
    provenance = {
        "schema_version": "binance-usdm-side-rest-provenance.v1",
        "kind": kind.value,
        "semantics": spec.semantics,
        "request": {
            "method": "GET",
            "path": spec.path,
            "parameters": parameters,
            "request_time_utc_ns": request_utc_ns,
            "request_monotonic_ns": request_monotonic_ns,
            "timeout_ms": timeout_ms,
            "documented_rate_limit": spec.documented_rate_limit,
        },
        "response": {
            "status": response.status,
            "headers": safe_provenance_headers(response.headers),
            "model": model,
            "receive_time_utc_ns": receive_utc_ns,
            "receive_monotonic_ns": receive_monotonic_ns,
        },
        "transport": {
            "kind": "official_sdk_parsed_model",
            "package": USDM_SDK_DISTRIBUTION,
            "version": version(USDM_SDK_DISTRIBUTION),
            "raw_http_body_available": False,
        },
    }
    raw_payload = json.dumps(
        provenance, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return EventEnvelope(
        market="um_perpetual",
        symbol="BTCUSDT",
        stream=kind.value,
        module="binance.usdm.side_rest.v1",
        connection_id=f"rest-{uuid4()}",
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        source_sequence=source_sequence,
        payload_encoding="utf-8-json-provenance",
        raw_payload=raw_payload,
        capture_flags=("rest_poll", "sdk_model_not_raw_http_body", "no_forward_fill"),
    )
