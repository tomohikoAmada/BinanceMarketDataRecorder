"""Official-SDK Binance Spot depth snapshot capture with explicit provenance."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from importlib.metadata import version
from typing import Any, Protocol
from uuid import uuid4

from binance_common.configuration import ConfigurationRestAPI
from binance_sdk_spot.spot import Spot

from ...domain.event import EventEnvelope

SPOT_SDK_DISTRIBUTION = "binance-sdk-spot"
SNAPSHOT_LIMIT = 5000
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


class SpotRestApi(Protocol):
    def depth(self, symbol: str, limit: int) -> DepthResponse: ...


def _safe_headers(headers: Mapping[str, object]) -> dict[str, str]:
    return {
        str(name).lower(): str(value)
        for name, value in headers.items()
        if str(name).lower() in _PROVENANCE_HEADERS
        or str(name).lower().startswith("x-mbx-used-weight-")
    }


def capture_depth_snapshot(
    *,
    rest_api: SpotRestApi | None = None,
    collector_instance_id: str,
    collector_version: str,
    limit: int = SNAPSHOT_LIMIT,
    timeout_ms: int = 10_000,
    utc_clock_ns: Callable[[], int] = time.time_ns,
    monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
) -> EventEnvelope:
    """Call only the unsigned public Spot depth method and record its provenance."""

    if not 1 <= limit <= 5000:
        raise ValueError("Spot depth snapshot limit must be between 1 and 5000")
    if timeout_ms < 1000:
        raise ValueError("Spot REST timeout must be at least 1000 ms")
    api = (
        rest_api
        if rest_api is not None
        else Spot(
            config_rest_api=ConfigurationRestAPI(
                timeout=timeout_ms,
                retries=0,
            )
        ).rest_api
    )
    request_utc_ns = utc_clock_ns()
    request_monotonic_ns = monotonic_clock_ns()
    response = api.depth("BTCUSDT", limit)
    receive_utc_ns = utc_clock_ns()
    receive_monotonic_ns = monotonic_clock_ns()
    if response.status != 200:
        raise RuntimeError(f"Spot depth snapshot returned HTTP {response.status}")
    model = response.data()
    if model.last_update_id is None:
        raise RuntimeError("Spot depth snapshot has no lastUpdateId")
    response_model = model.to_dict()
    provenance = {
        "schema_version": "binance-spot-depth-snapshot-provenance.v1",
        "request": {
            "method": "GET",
            "path": "/api/v3/depth",
            "symbol": "BTCUSDT",
            "limit": limit,
            "request_time_utc_ns": request_utc_ns,
            "request_monotonic_ns": request_monotonic_ns,
            "timeout_ms": timeout_ms,
        },
        "response": {
            "status": response.status,
            "headers": _safe_headers(response.headers),
            "model": response_model,
            "receive_time_utc_ns": receive_utc_ns,
            "receive_monotonic_ns": receive_monotonic_ns,
        },
        "transport": {
            "kind": "official_sdk_parsed_model",
            "package": SPOT_SDK_DISTRIBUTION,
            "version": version(SPOT_SDK_DISTRIBUTION),
            "raw_http_body_available": False,
        },
    }
    raw_payload = json.dumps(
        provenance, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return EventEnvelope(
        market="spot",
        symbol="BTCUSDT",
        stream="depth_snapshot",
        module="binance.spot.rest.v1",
        connection_id=f"rest-{uuid4()}",
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        source_sequence={"lastUpdateId": model.last_update_id},
        payload_encoding="utf-8-json-provenance",
        raw_payload=raw_payload,
        capture_flags=("rest_snapshot", "sdk_model_not_raw_http_body"),
    )
