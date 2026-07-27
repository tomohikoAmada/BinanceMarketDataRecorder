"""Credential-free Spot depth snapshots with byte and rate-limit provenance."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request
from uuid import uuid4

from ...domain.event import EventEnvelope
from ...network import ProxyPolicy
from .rate_limit import (
    SpotIpRateLimiter,
    SpotRateLimitBlocked,
    depth_request_weight,
    shared_spot_ip_rate_limiter,
)

SPOT_REST_BASE_URL = "https://api.binance.com"
SNAPSHOT_LIMIT = 1_000
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


@dataclass(frozen=True, slots=True)
class PublicDepthModel:
    last_update_id: int | None
    document: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.document)


@dataclass(frozen=True, slots=True)
class PublicDepthResponse:
    status: int
    headers: Mapping[str, object]
    raw_body: bytes
    model: PublicDepthModel | None

    def data(self) -> PublicDepthModel:
        if self.model is None:
            raise RuntimeError("Spot depth error response has no depth model")
        return self.model


class SpotSnapshotHttpError(RuntimeError):
    """An exact public HTTP response that cannot produce a snapshot."""

    def __init__(
        self,
        *,
        status: int,
        headers: Mapping[str, object],
        raw_body: bytes,
    ) -> None:
        super().__init__(f"Spot depth snapshot returned HTTP {status}")
        self.status = status
        self.headers = _safe_headers(headers)
        self.raw_body = raw_body


class PublicSpotRestApi:
    """Minimal unsigned transport used because SDK errors discard response headers."""

    def __init__(
        self,
        *,
        base_url: str = SPOT_REST_BASE_URL,
        timeout_ms: int = 10_000,
        opener: Callable[..., Any] | None = None,
        proxy_policy: ProxyPolicy | None = None,
    ) -> None:
        if base_url != SPOT_REST_BASE_URL:
            raise ValueError("Spot public REST base URL is frozen to api.binance.com")
        if timeout_ms < 1_000:
            raise ValueError("Spot REST timeout must be at least 1000 ms")
        self.base_url = base_url
        self.timeout_ms = timeout_ms
        policy = proxy_policy or ProxyPolicy("direct")
        self._opener = opener or policy.urllib_opener(base_url).open

    def depth(self, symbol: str, limit: int) -> PublicDepthResponse:
        if symbol != "BTCUSDT":
            raise ValueError("V1 Spot REST scope is BTCUSDT only")
        depth_request_weight(limit)
        query = urlencode({"symbol": symbol, "limit": limit})
        request = Request(
            f"{self.base_url}/api/v3/depth?{query}",
            headers={
                "Accept": "application/json",
                "User-Agent": "BinanceMarketDataRecorder/0.1 public-market-data",
            },
            method="GET",
        )
        try:
            response = cast(
                Any,
                self._opener(request, timeout=self.timeout_ms / 1_000),
            )
            with response:
                status = int(response.status)
                headers = dict(response.headers.items())
                raw_body = response.read()
        except HTTPError as exc:
            with exc:
                return PublicDepthResponse(
                    status=exc.code,
                    headers=dict(exc.headers.items()),
                    raw_body=exc.read(),
                    model=None,
                )
        if status != 200:
            return PublicDepthResponse(status, headers, raw_body, None)
        try:
            decoded = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Spot depth snapshot body is not JSON") from exc
        if not isinstance(decoded, dict):
            raise RuntimeError("Spot depth snapshot body is not an object")
        last_update_id = decoded.get("lastUpdateId")
        if not isinstance(last_update_id, int) or isinstance(last_update_id, bool):
            last_update_id = None
        return PublicDepthResponse(
            status=status,
            headers=headers,
            raw_body=raw_body,
            model=PublicDepthModel(last_update_id, decoded),
        )


def _safe_headers(headers: Mapping[str, object]) -> dict[str, str]:
    return {
        str(name).lower(): str(value)
        for name, value in headers.items()
        if str(name).lower() in _PROVENANCE_HEADERS
        or str(name).lower().startswith("x-mbx-used-weight-")
    }


def capture_depth_snapshot(
    *,
    rest_api: SpotRestApi,
    collector_instance_id: str,
    collector_version: str,
    limit: int = SNAPSHOT_LIMIT,
    timeout_ms: int = 10_000,
    utc_clock_ns: Callable[[], int] = time.time_ns,
    monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
    additional_capture_flags: tuple[str, ...] = (),
) -> EventEnvelope:
    """Capture one already-paced unsigned public depth response."""

    weight = depth_request_weight(limit)
    if timeout_ms < 1_000:
        raise ValueError("Spot REST timeout must be at least 1000 ms")
    request_utc_ns = utc_clock_ns()
    request_monotonic_ns = monotonic_clock_ns()
    response = rest_api.depth("BTCUSDT", limit)
    receive_utc_ns = utc_clock_ns()
    receive_monotonic_ns = monotonic_clock_ns()
    raw_body = getattr(response, "raw_body", None)
    if response.status != 200:
        raise SpotSnapshotHttpError(
            status=response.status,
            headers=response.headers,
            raw_body=raw_body if isinstance(raw_body, bytes) else b"",
        )
    model = response.data()
    if model.last_update_id is None:
        raise RuntimeError("Spot depth snapshot has no lastUpdateId")
    response_model = model.to_dict()
    provenance: dict[str, object] = {
        "schema_version": "binance-spot-depth-snapshot-provenance.v2",
        "request": {
            "method": "GET",
            "path": "/api/v3/depth",
            "symbol": "BTCUSDT",
            "limit": limit,
            "request_weight": weight,
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
            "kind": "stdlib_https_exact_public_response",
            "endpoint": SPOT_REST_BASE_URL,
            "credentials": False,
            "raw_http_body_available": isinstance(raw_body, bytes),
        },
    }
    if isinstance(raw_body, bytes):
        provenance["response"] = {
            **provenance["response"],  # type: ignore[dict-item]
            "raw_body_base64": base64.b64encode(raw_body).decode("ascii"),
        }
    raw_payload = json.dumps(
        provenance, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    flags = ["rest_snapshot"]
    flags.append(
        "exact_rest_http_body_preserved"
        if isinstance(raw_body, bytes)
        else "injected_model_without_raw_http_body"
    )
    return EventEnvelope(
        market="spot",
        symbol="BTCUSDT",
        stream="depth_snapshot",
        module="binance.spot.rest.v2",
        connection_id=f"rest-{uuid4()}",
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        source_sequence={"lastUpdateId": model.last_update_id},
        payload_encoding="utf-8-json-provenance",
        raw_payload=raw_payload,
        capture_flags=tuple(dict.fromkeys((*flags, *additional_capture_flags))),
    )


class SpotSnapshotRequester:
    """Single-flight snapshot requests behind the process-shared IP limiter."""

    def __init__(
        self,
        *,
        rest_api: SpotRestApi,
        rate_limiter: SpotIpRateLimiter | None = None,
    ) -> None:
        self.rest_api = rest_api
        self._rate_limiter = rate_limiter
        self._inflight_lock = asyncio.Lock()
        self._inflight: dict[tuple[str, str], asyncio.Task[EventEnvelope]] = {}

    @property
    def rate_limiter(self) -> SpotIpRateLimiter:
        if self._rate_limiter is None:
            self._rate_limiter = shared_spot_ip_rate_limiter()
        return self._rate_limiter

    async def capture(
        self,
        *,
        collector_instance_id: str,
        collector_version: str,
        limit: int,
        timeout_ms: int,
        additional_capture_flags: tuple[str, ...] = (),
    ) -> EventEnvelope:
        key = ("spot", "BTCUSDT")
        async with self._inflight_lock:
            task = self._inflight.get(key)
            if task is None:
                task = asyncio.create_task(
                    self._capture_once(
                        collector_instance_id=collector_instance_id,
                        collector_version=collector_version,
                        limit=limit,
                        timeout_ms=timeout_ms,
                        additional_capture_flags=additional_capture_flags,
                    )
                )
                self._inflight[key] = task
                task.add_done_callback(lambda completed: self._remove_inflight(key, completed))
        return await asyncio.shield(task)

    def _remove_inflight(
        self,
        key: tuple[str, str],
        completed: asyncio.Task[EventEnvelope],
    ) -> None:
        if self._inflight.get(key) is completed:
            self._inflight.pop(key, None)

    async def _capture_once(
        self,
        *,
        collector_instance_id: str,
        collector_version: str,
        limit: int,
        timeout_ms: int,
        additional_capture_flags: tuple[str, ...],
    ) -> EventEnvelope:
        await self.rate_limiter.acquire(limit=limit)
        try:
            async with self.rate_limiter.request_slot():
                envelope = await asyncio.to_thread(
                    capture_depth_snapshot,
                    rest_api=self.rest_api,
                    collector_instance_id=collector_instance_id,
                    collector_version=collector_version,
                    limit=limit,
                    timeout_ms=timeout_ms,
                    additional_capture_flags=additional_capture_flags,
                )
        except SpotSnapshotHttpError as exc:
            if exc.status in {418, 429}:
                blocked = await self.rate_limiter.observe_rejection(
                    status=exc.status,
                    limit=limit,
                    headers=exc.headers,
                    body_text=exc.raw_body.decode("utf-8", errors="replace"),
                )
                raise blocked from exc
            raise
        provenance = json.loads(envelope.raw_payload)
        headers = provenance["response"]["headers"]
        if not isinstance(headers, dict):
            raise RuntimeError("Spot snapshot provenance headers are invalid")
        await self.rate_limiter.observe_success(limit=limit, headers=headers)
        return envelope

    async def wait_for_idle(self) -> None:
        """Await any shielded worker so cancellation cannot leak snapshot work."""

        while True:
            async with self._inflight_lock:
                tasks = tuple(self._inflight.values())
            if not tasks:
                return
            await asyncio.gather(*tasks, return_exceptions=True)

    def inflight_count(self) -> int:
        return len(self._inflight)


__all__ = [
    "SNAPSHOT_LIMIT",
    "DepthResponse",
    "PublicSpotRestApi",
    "SpotRateLimitBlocked",
    "SpotRestApi",
    "SpotSnapshotHttpError",
    "SpotSnapshotRequester",
    "capture_depth_snapshot",
]
