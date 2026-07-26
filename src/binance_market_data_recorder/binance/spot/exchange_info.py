"""Credential-free Spot BTCUSDT exchange-information capture."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from importlib.metadata import version
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_spot.spot import Spot

from ...domain.event import EventEnvelope
from ..usdm.rest import safe_provenance_headers

SPOT_SDK_DISTRIBUTION = "binance-sdk-spot"


@runtime_checkable
class SdkModel(Protocol):
    def to_dict(self) -> Any: ...


class PublicResponse(Protocol):
    @property
    def status(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, object]: ...

    def data(self) -> object: ...


class SpotExchangeInfoApi(Protocol):
    def exchange_info(
        self,
        symbol: str | None = None,
        symbols: list[str] | None = None,
        permissions: object | None = None,
        show_permission_sets: bool | None = None,
        symbol_status: object | None = None,
    ) -> PublicResponse: ...


def capture_spot_exchange_info(
    *,
    rest_api: SpotExchangeInfoApi | None = None,
    collector_instance_id: str,
    collector_version: str,
    timeout_ms: int = 10_000,
    utc_clock_ns: Callable[[], int] = time.time_ns,
    monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
) -> EventEnvelope:
    if timeout_ms < 1000:
        raise ValueError("Spot exchangeInfo timeout must be at least 1000 ms")
    api = (
        rest_api
        if rest_api is not None
        else Spot(
            config_rest_api=ConfigurationRestAPI(timeout=timeout_ms, retries=0)
        ).rest_api
    )
    request_utc_ns = utc_clock_ns()
    request_monotonic_ns = monotonic_clock_ns()
    response = api.exchange_info(symbol="BTCUSDT")
    receive_utc_ns = utc_clock_ns()
    receive_monotonic_ns = monotonic_clock_ns()
    if response.status != 200:
        raise RuntimeError(f"Spot exchangeInfo returned HTTP {response.status}")
    data = response.data()
    if not isinstance(data, SdkModel):
        raise RuntimeError("Spot exchangeInfo SDK response has no model")
    model = data.to_dict()
    if not isinstance(model, dict):
        raise RuntimeError("Spot exchangeInfo model must be an object")
    symbols = model.get("symbols")
    if not isinstance(symbols, list) or len(symbols) != 1:
        raise RuntimeError("Spot exchangeInfo must contain exactly BTCUSDT")
    symbol = symbols[0]
    if (
        not isinstance(symbol, dict)
        or symbol.get("symbol") != "BTCUSDT"
        or not isinstance(symbol.get("filters"), list)
        or not isinstance(symbol.get("orderTypes"), list)
        or not isinstance(symbol.get("status"), str)
    ):
        raise RuntimeError("Spot exchangeInfo BTCUSDT schema is incomplete")
    provenance = {
        "schema_version": "binance-spot-exchange-info-provenance.v1",
        "request": {
            "method": "GET",
            "path": "/api/v3/exchangeInfo",
            "parameters": {"symbol": "BTCUSDT"},
            "request_weight": 20,
            "request_time_utc_ns": request_utc_ns,
            "request_monotonic_ns": request_monotonic_ns,
            "timeout_ms": timeout_ms,
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
            "package": SPOT_SDK_DISTRIBUTION,
            "version": version(SPOT_SDK_DISTRIBUTION),
            "raw_http_body_available": False,
        },
        "semantics": "periodic_rules_snapshot_no_forward_fill",
    }
    raw_server_time = model.get("serverTime")
    server_time: int | str
    if isinstance(raw_server_time, int) and not isinstance(raw_server_time, bool):
        server_time = raw_server_time
    else:
        server_time = "not_provided"
    return EventEnvelope(
        market="spot",
        symbol="BTCUSDT",
        stream="exchange_info",
        module="binance.spot.rest.exchange_info.v1",
        connection_id=f"rest-{uuid4()}",
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        exchange_event_time=(
            model.get("serverTime") if isinstance(model.get("serverTime"), int) else None
        ),
        source_sequence={"serverTime": server_time, "status": str(symbol["status"])},
        payload_encoding="utf-8-json-provenance",
        raw_payload=json.dumps(
            provenance, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode(),
        capture_flags=("rest_snapshot", "official_sdk_model_no_raw_http_body"),
    )
