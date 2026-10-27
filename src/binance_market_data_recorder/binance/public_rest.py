"""Unsigned, allowlisted public REST through the installed official SDK transport.

Use send_request's parsed JSON result directly. Generated per-endpoint model
exports can erase explicit nulls (e.g. Spot referencePrice); no second HTTP
transport or duplicate body is needed to preserve the required distinction.
"""

from __future__ import annotations

import base64
import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from binance_common.configuration import ConfigurationRestAPI
from binance_common.utils import send_request
from binance_sdk_spot.spot import Spot

from ..domain.event import EventEnvelope
from ..network import ProxyPolicy
from .public_sources import PUBLIC_SDK_VERSIONS, REST_HOSTS, Source
from .recent_trades import RecentTradesCompactor
from .spot.exchange_info import PublicResponse
from .usdm.rest import safe_provenance_headers


class PublicSdkApi(Protocol):
    def send_request(
        self, endpoint: str, method: str, query_params: dict[str, Any] | None = None
    ) -> PublicResponse: ...


class PublicRestClient:
    def __init__(self, venue_market: str, proxy_policy: ProxyPolicy) -> None:
        host = REST_HOSTS[venue_market]
        configuration = ConfigurationRestAPI(
            base_path=host, timeout=10_000, retries=0, proxy=proxy_policy.sdk_proxy(host)
        )
        # This is the SDK's common unsigned transport, not its Spot schema layer.
        self.api: Any = Spot(config_rest_api=configuration).rest_api
        proxy_policy.configure_sdk_rest_api(self.api)
        # Keep environment proxy discovery without requests consulting netrc.
        self.api._session.auth = lambda request: request
        self.api._session.max_redirects = 0
        self.venue_market = venue_market
        self._received = threading.local()
        self.api._session.hooks["response"].append(self._observe_response)

    def _observe_response(self, response: Any, **kwargs: Any) -> None:
        # Complete the existing SDK request body once; requests caches it for
        # the SDK parser. Retain status/headers even when the SDK raises.
        _ = response.content
        self._received.response = response
        self._received.utc_ns = time.time_ns()
        self._received.monotonic_ns = time.monotonic_ns()

    def request(self, source: Source, parameters: dict[str, Any]) -> PublicResponse:
        if source.transport != "rest" or source.venue_market != self.venue_market:
            raise ValueError("public source does not belong to this client")
        # Sources are built only by public_sources; never accept a caller URL.
        from .public_sources import REST_PREFIX

        public_paths = {
            REST_PREFIX[self.venue_market] + "/" + path
            for path in (
                "exchangeInfo",
                "depth",
                "rpiDepth",
                "trades",
                "premiumIndex",
                "fundingRate",
                "fundingInfo",
                "openInterest",
                "klines",
                "continuousKlines",
                "markPriceKlines",
                "indexPriceKlines",
                "premiumIndexKlines",
                "constituents",
                "indexInfo",
                "assetIndex",
                "symbolAdlRisk",
                "insuranceBalance",
                "referencePrice",
                "referencePrice/calculation",
                "executionRules",
                "ticker/tradingDay",
            )
        } | {
            "/futures/data/" + path
            for path in (
                "openInterestHist",
                "takerlongshortRatio",
                "takerBuySellVol",
                "globalLongShortAccountRatio",
                "basis",
                "delivery-price",
            )
        }
        if self.venue_market == "coinm":
            public_paths.update(
                {
                    "/futures/data/topLongShortAccountRatio",
                    "/futures/data/topLongShortPositionRatio",
                }
            )
        if source.endpoint not in public_paths or any(
            key.lower() in {"apikey", "signature", "timestamp", "listenkey"} for key in parameters
        ):
            raise ValueError("request is outside the unsigned public allowlist")
        # The generated Spot 10.0.0 send_request method incorrectly subscripts a
        # function. Call its installed common implementation without that wrapper.
        self._received.response = None
        try:
            response: Any = send_request(
                self.api._session,
                self.api.configuration,
                "GET",
                source.endpoint,
                payload=parameters,
                response_model=None,
                is_signed=False,
            )
        except Exception:
            received = self._received.response
            if received is None:
                raise
            try:
                document = received.json()
            except ValueError:
                document = None
            status, headers = received.status_code, received.headers
        else:
            document = response.data()
            status, headers = response.status, response.headers
        return _ObservedResponse(
            status,
            headers,
            document,
            self._received.utc_ns,
            self._received.monotonic_ns,
            self._received.response.content if source.family == "D31" else None,
        )

    def close(self) -> None:
        self.api._session.close()


@dataclass(frozen=True)
class _ObservedResponse:
    status: int
    headers: Any
    document: Any
    receive_time_utc_ns: int
    receive_monotonic_ns: int
    raw_body: bytes | None = None

    def data(self) -> Any:
        return self.document


@dataclass(frozen=True, slots=True)
class RestObservation:
    envelope: EventEnvelope
    document: Any
    valid: bool
    status: int | None
    error: str | None


def _validate(source: Source, model: Any) -> None:
    rows = model if isinstance(model, list) else [model]
    # Kline rows are native positional arrays, preserved without float/Decimal conversion.
    if "kline" in source.stream or "bars_" in source.stream or "_seed_" in source.stream:
        for row in rows:
            if (
                not isinstance(row, list)
                or len(row) != 12
                or any(
                    not isinstance(row[index], int)
                    or isinstance(row[index], bool)
                    or not 0 <= row[index] <= 2**63 - 1
                    for index in (0, 6, 8)
                )
                or any(not isinstance(row[index], str) for index in (1, 2, 3, 4, 5, 7, 9, 10))
            ):
                raise ValueError("invalid native kline fields or decimal precision")
        return
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("public response row is not an object")
        if any(key not in row for key in source.required_fields):
            raise ValueError("public response missing a required field")
        for key in (
            "price",
            "qty",
            "quoteQty",
            "baseQty",
            "markPrice",
            "indexPrice",
            "fundingRate",
            "openInterest",
            "referencePrice",
            "basis",
            "basisRate",
        ):
            if key in row and row[key] is not None and not isinstance(row[key], str):
                raise ValueError("public decimal field must retain its decimal text")
    if source.stream == "recent_trades":
        if not isinstance(model, list):
            raise ValueError("recent trades must be a list")
        for row in model:
            if any(
                not isinstance(row.get(k), int) or isinstance(row[k], bool) or row[k] < 0
                for k in ("id", "time")
            ):
                raise ValueError("invalid native trade identity/time")
    if source.stream in {"depth_snapshot", "rpi_depth_snapshot"}:
        if not isinstance(model.get("lastUpdateId"), int):
            raise ValueError("invalid snapshot sequence")
        for side in ("bids", "asks"):
            for level in model[side]:
                if (
                    not isinstance(level, list)
                    or len(level) != 2
                    or not all(isinstance(x, str) for x in level)
                ):
                    raise ValueError("invalid snapshot price level")


def capture_public_response(
    source: Source,
    request: Callable[[Source, dict[str, Any]], PublicResponse],
    *,
    collector_instance_id: str,
    collector_version: str,
    parameters: dict[str, Any] | None = None,
    recent_trades: RecentTradesCompactor | None = None,
    utc_clock_ns: Callable[[], int] = time.time_ns,
    monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
) -> RestObservation:
    params = dict(source.parameters) if parameters is None else parameters
    requested, requested_mono = utc_clock_ns(), monotonic_clock_ns()
    document: Any = None
    status: int | None = None
    error: str | None = None
    headers: dict[str, str] = {}
    raw_body: bytes | None = None
    try:
        response = request(source, params)
        received, received_mono = utc_clock_ns(), monotonic_clock_ns()
        received = getattr(response, "receive_time_utc_ns", received)
        received_mono = getattr(response, "receive_monotonic_ns", received_mono)
        status = response.status
        raw_body = getattr(response, "raw_body", None)
        headers = safe_provenance_headers(response.headers)
        document = response.data()
        if status != 200:
            error = "HttpResponseError"
        else:
            try:
                _validate(source, document)
                if source.lifecycle == "closed_bar" and any(
                    row[6] >= requested // 1_000_000 for row in document
                ):
                    raise ValueError("native price bar is not closed at request time")
            except (TypeError, ValueError, KeyError):
                error = "RequiredFieldSchemaError"
    except Exception as exc:
        received, received_mono = utc_clock_ns(), monotonic_clock_ns()
        error = type(exc).__name__
        code = getattr(exc, "status_code", None)
        status = code if isinstance(code, int) else None
    valid = error is None
    saved_document = document
    representation = "sdk-parsed-json.v1"
    if valid and recent_trades is not None:
        saved_document = recent_trades.observe(document, received)
        representation = "recent-trades-compact.v1"
    record: dict[str, Any] = {
        "schema_version": "binance-public-rest-provenance.v1",
        "representation": representation,
        "family": source.family,
        "source_stream": source.stream,
        "scope": source.symbol,
        "venue_market": source.venue_market,
        "request": {
            "request_id": str(uuid4()),
            "method": "GET",
            "path": source.endpoint,
            "parameters": params,
            "request_time_utc_ns": requested,
            "request_monotonic_ns": requested_mono,
            "timeout_ms": 10_000,
        },
        "response": {
            "status": status,
            "headers": headers,
            "model": saved_document,
            "valid": valid,
            "error_type": error,
            "receive_time_utc_ns": received,
            "receive_monotonic_ns": received_mono,
        },
        "transport": {
            "kind": "official_sdk_unsigned_parsed_json",
            "package": "binance-sdk-spot",
            "version": PUBLIC_SDK_VERSIONS["binance-sdk-spot"],
            "common_version": PUBLIC_SDK_VERSIONS["binance-common"],
            "raw_http_body_available": False,
            "receive_boundary": "response_body_received_before_sdk_parse",
            "connection_identity": "logical_collector_venue_polling_session_not_tcp",
        },
    }
    if raw_body is not None:
        # D31 publishes numeric JSON prices. The common SDK's float parser is
        # insufficient for that precision contract; its already-received body
        # is the sole saved representation instead.
        record["representation"] = "http-body-base64.v1"
        record["response"].pop("model")
        record["response"]["raw_body_base64"] = base64.b64encode(raw_body).decode()
        record["transport"]["raw_http_body_available"] = True
        record["transport"]["kind"] = "official_sdk_unsigned_response_body"
    sequence: dict[str, int | str] = {"sourceStream": source.stream, "sourceFamily": source.family}
    if valid and isinstance(document, dict):
        for key in ("lastUpdateId", "time", "timestamp", "updateTime"):
            if isinstance(document.get(key), int) and not isinstance(document[key], bool):
                sequence[key] = document[key]
    gap_flags = (
        ("sequence_gap",)
        if (
            recent_trades is not None and valid and saved_document["poll"]["window_gap"] is not None
        )
        else ()
    )
    envelope = EventEnvelope(
        market=source.market,
        symbol=source.symbol,
        stream=source.stream,
        module="binance.public.rest.v1",
        # Requests has no reliable per-response TCP identity. Use the polling
        # owner session; request_id above identifies each individual attempt.
        # A new UUID per poll would invent WS-style reconnect gaps on sealing.
        connection_id="rest-" + collector_instance_id + "-" + source.venue_market,
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=received,
        receive_monotonic_ns=received_mono,
        source_sequence=sequence,
        payload_encoding="utf-8-json-provenance",
        raw_payload=json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode(),
        capture_flags=(
            "rest_poll",
            "exact_rest_http_body_preserved"
            if raw_body is not None
            else "sdk_model_not_raw_http_body",
        )
        + gap_flags
        + (() if valid else ("source_failed", "malformed")),
    )
    return RestObservation(envelope, document, valid, status, error)
