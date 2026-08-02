"""Official-SDK Binance USD-M depth snapshot capture."""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable, Mapping
from datetime import UTC
from email.utils import parsedate_to_datetime
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


class UsdMSnapshotHttpError(UsdMSnapshotResponseError):
    """A classified USD-M HTTP response that cannot produce a snapshot."""

    def __init__(
        self,
        *,
        status: int,
        headers: Mapping[str, object],
        retry_after_seconds: float | None,
        retry_at_utc_ns: int | None,
    ) -> None:
        super().__init__(f"USD-M depth snapshot returned HTTP {status}")
        self.status = status
        self.headers = safe_provenance_headers(headers)
        self.rate_limited = status in {418, 429}
        self.retry_after_seconds = retry_after_seconds
        self.retry_at_utc_ns = retry_at_utc_ns


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


def _non_negative_seconds(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def _retry_boundary(
    response: DepthResponse,
    *,
    receive_utc_ns: int,
) -> tuple[float | None, int | None]:
    safe_headers = safe_provenance_headers(response.headers)
    retry_after = safe_headers.get("retry-after")
    if retry_after is not None:
        seconds = _non_negative_seconds(retry_after.strip())
        if seconds is not None:
            return seconds, receive_utc_ns + int(seconds * 1_000_000_000)
        try:
            boundary = parsedate_to_datetime(retry_after.strip())
        except (TypeError, ValueError, OverflowError):
            pass
        else:
            if boundary.tzinfo is None:
                boundary = boundary.replace(tzinfo=UTC)
            retry_at_utc_ns = max(
                receive_utc_ns,
                int(boundary.timestamp() * 1_000_000_000),
            )
            return (
                (retry_at_utc_ns - receive_utc_ns) / 1_000_000_000,
                retry_at_utc_ns,
            )

    # The official SDK exposes parsed rate-limit records when the transport
    # supplies them. Read only the documented retryAfter field; never inspect
    # or retain an error body or exception string.
    for rate_limit in getattr(response, "rate_limits", ()) or ():
        if isinstance(rate_limit, Mapping):
            value = rate_limit.get("retryAfter", rate_limit.get("retry_after"))
        else:
            value = getattr(
                rate_limit,
                "retryAfter",
                getattr(rate_limit, "retry_after", None),
            )
        seconds = _non_negative_seconds(value)
        if seconds is not None:
            return seconds, receive_utc_ns + int(seconds * 1_000_000_000)
    return None, None


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
        retry_after_seconds, retry_at_utc_ns = _retry_boundary(
            response,
            receive_utc_ns=receive_utc_ns,
        )
        raise UsdMSnapshotHttpError(
            status=response.status,
            headers=response.headers,
            retry_after_seconds=retry_after_seconds,
            retry_at_utc_ns=retry_at_utc_ns,
        )
    try:
        model = response.data()
        model_document = model.to_dict()
        last_update_id = model.last_update_id
    except Exception as exc:
        raise UsdMSnapshotResponseError(
            "USD-M depth snapshot response could not be parsed"
        ) from exc
    if last_update_id is None:
        raise UsdMSnapshotResponseError("USD-M depth snapshot has no lastUpdateId")
    if (
        not isinstance(last_update_id, int)
        or isinstance(last_update_id, bool)
        or not isinstance(model_document, dict)
        or model_document.get("lastUpdateId") != last_update_id
    ):
        raise UsdMSnapshotResponseError(
            "USD-M depth snapshot response schema is invalid"
        )
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
        source_sequence={"lastUpdateId": last_update_id},
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
