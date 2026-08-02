"""Official-SDK Binance USD-M depth snapshot capture."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from importlib.metadata import version
from typing import Any, Protocol
from uuid import uuid4

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_derivatives_trading_usds_futures.derivatives_trading_usds_futures import (
    DerivativesTradingUsdsFutures,
)

from ...domain.event import EventEnvelope
from ...network import ProxyPolicy

USDM_SDK_DISTRIBUTION = "binance-sdk-derivatives-trading-usds-futures"
USDM_REST_BASE_URL = "https://fapi.binance.com"
SNAPSHOT_LIMIT = 1000
_PROVENANCE_HEADERS = {
    "content-type",
    "date",
    "retry-after",
    "x-mbx-used-weight",
    "x-mbx-used-weight-1m",
    "x-response-time",
}


class DepthModel(Protocol):
    @property
    def last_update_id(self) -> int | None: ...

    def to_dict(self) -> dict[str, Any]: ...


class DepthResponse(Protocol):
    @property
    def status(self) -> int: ...

    @property
    def headers(self) -> Mapping[str, object]: ...

    def data(self) -> DepthModel: ...


class UsdMRestApi(Protocol):
    def order_book(self, symbol: str, limit: int) -> DepthResponse: ...


class UsdMSnapshotResponseError(RuntimeError):
    """Raised when Binance returns an unusable USD-M snapshot response."""


def create_usdm_rest_api(
    *,
    timeout_ms: int,
    proxy_policy: ProxyPolicy,
) -> UsdMRestApi:
    """Build the unsigned SDK client with an explicit transport decision."""

    configuration = ConfigurationRestAPI(
        timeout=timeout_ms,
        retries=0,
        proxy=proxy_policy.sdk_proxy(USDM_REST_BASE_URL),
    )
    rest_api = DerivativesTradingUsdsFutures(config_rest_api=configuration).rest_api
    proxy_policy.configure_sdk_rest_api(rest_api)
    return rest_api


def safe_provenance_headers(headers: Mapping[str, object]) -> dict[str, str]:
    return {
        str(name).lower(): str(value)
        for name, value in headers.items()
        if str(name).lower() in _PROVENANCE_HEADERS
        or str(name).lower().startswith("x-mbx-used-weight-")
    }


def capture_depth_snapshot(
    *,
    rest_api: UsdMRestApi | None = None,
    collector_instance_id: str,
    collector_version: str,
    limit: int = SNAPSHOT_LIMIT,
    timeout_ms: int = 10_000,
    utc_clock_ns: Callable[[], int] = time.time_ns,
    monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
    additional_capture_flags: tuple[str, ...] = (),
) -> EventEnvelope:
    """Call only the unsigned public USD-M order-book method."""

    if limit not in {5, 10, 20, 50, 100, 500, 1000}:
        raise ValueError("USD-M depth snapshot limit is not supported")
    if timeout_ms < 1000:
        raise ValueError("USD-M REST timeout must be at least 1000 ms")
    api = (
        rest_api
        if rest_api is not None
        else create_usdm_rest_api(
            timeout_ms=timeout_ms,
            proxy_policy=ProxyPolicy("direct"),
        )
    )
    request_utc_ns = utc_clock_ns()
    request_monotonic_ns = monotonic_clock_ns()
    response = api.order_book("BTCUSDT", limit)
    receive_utc_ns = utc_clock_ns()
    receive_monotonic_ns = monotonic_clock_ns()
    if response.status != 200:
        raise UsdMSnapshotResponseError(
            f"USD-M depth snapshot returned HTTP {response.status}"
        )
    try:
        model = response.data()
        model_document = model.to_dict()
    except Exception as exc:
        raise UsdMSnapshotResponseError(
            "USD-M depth snapshot response could not be parsed"
        ) from exc
    if model.last_update_id is None:
        raise UsdMSnapshotResponseError("USD-M depth snapshot has no lastUpdateId")
    provenance = {
        "schema_version": "binance-usdm-depth-snapshot-provenance.v1",
        "request": {
            "method": "GET",
            "path": "/fapi/v1/depth",
            "symbol": "BTCUSDT",
            "limit": limit,
            "request_time_utc_ns": request_utc_ns,
            "request_monotonic_ns": request_monotonic_ns,
            "timeout_ms": timeout_ms,
        },
        "response": {
            "status": response.status,
            "headers": safe_provenance_headers(response.headers),
            "model": model_document,
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
        stream="depth_snapshot",
        module="binance.usdm.rest.v1",
        connection_id=f"rest-{uuid4()}",
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        source_sequence={"lastUpdateId": model.last_update_id},
        payload_encoding="utf-8-json-provenance",
        raw_payload=raw_payload,
        capture_flags=tuple(
            dict.fromkeys(
                (
                    "rest_snapshot",
                    "sdk_model_not_raw_http_body",
                    *additional_capture_flags,
                )
            )
        ),
    )
