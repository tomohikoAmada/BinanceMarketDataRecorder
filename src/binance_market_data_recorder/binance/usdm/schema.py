"""Lossless Binance USD-M WebSocket envelope extraction."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ...domain.event import EventEnvelope

USDM_DEPTH_CONTINUITY_CONTRACT = "each_event_pu_equals_previous_event_u"


class UsdMStream(StrEnum):
    DIFF_DEPTH = "diff_depth"
    AGG_TRADE = "agg_trade"
    BOOK_TICKER = "book_ticker"


@dataclass(frozen=True)
class UsdMStreamSpec:
    stream: UsdMStream
    route: str
    wire_name: str
    expected_event: str


USDM_STREAMS: tuple[UsdMStreamSpec, ...] = (
    UsdMStreamSpec(UsdMStream.DIFF_DEPTH, "public", "btcusdt@depth@100ms", "depthUpdate"),
    UsdMStreamSpec(UsdMStream.AGG_TRADE, "market", "btcusdt@aggTrade", "aggTrade"),
    UsdMStreamSpec(UsdMStream.BOOK_TICKER, "public", "btcusdt@bookTicker", "bookTicker"),
)

_SPEC_BY_STREAM = {spec.stream: spec for spec in USDM_STREAMS}


def _integer(payload: dict[str, Any], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _text(payload: dict[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be non-empty text")
    return value


def _levels(payload: dict[str, Any], name: str) -> None:
    value = payload.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    if any(
        not isinstance(level, list)
        or len(level) != 2
        or any(not isinstance(item, str) for item in level)
        for level in value
    ):
        raise ValueError(f"{name} contains an invalid price level")


def _parse_metadata(
    raw_payload: bytes, stream: UsdMStream
) -> tuple[int | None, int | None, dict[str, int | str], tuple[str, ...]]:
    try:
        decoded = json.loads(raw_payload)
        if not isinstance(decoded, dict):
            raise ValueError("payload root must be an object")
        spec = _SPEC_BY_STREAM[stream]
        if decoded.get("e") != spec.expected_event:
            raise ValueError(f"unexpected event type for {stream}")
        if _text(decoded, "s") != "BTCUSDT":
            raise ValueError("unexpected symbol")
        event_time = _integer(decoded, "E")
        transaction_time = _integer(decoded, "T")

        if stream is UsdMStream.DIFF_DEPTH:
            first = _integer(decoded, "U")
            last = _integer(decoded, "u")
            previous = _integer(decoded, "pu")
            if first > last:
                raise ValueError("depth U exceeds u")
            _levels(decoded, "b")
            _levels(decoded, "a")
            return event_time, transaction_time, {"U": first, "u": last, "pu": previous}, ()
        if stream is UsdMStream.AGG_TRADE:
            aggregate = _integer(decoded, "a")
            first_trade = _integer(decoded, "f")
            last_trade = _integer(decoded, "l")
            if first_trade > last_trade:
                raise ValueError("aggregate trade f exceeds l")
            _text(decoded, "p")
            _text(decoded, "q")
            if not isinstance(decoded.get("m"), bool):
                raise ValueError("m must be boolean")
            return (
                event_time,
                transaction_time,
                {"a": aggregate, "f": first_trade, "l": last_trade},
                (),
            )

        update_id = _integer(decoded, "u")
        for name in ("b", "B", "a", "A"):
            _text(decoded, name)
        return event_time, transaction_time, {"u": update_id}, ()
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
        return None, None, {}, ("malformed",)


def envelope_from_websocket_frame(
    *,
    raw_payload: bytes,
    stream: UsdMStream,
    connection_id: str,
    collector_instance_id: str,
    collector_version: str,
    receive_time_utc_ns: int,
    receive_monotonic_ns: int,
) -> EventEnvelope:
    """Create a USD-M Raw envelope while retaining exact transport bytes."""

    event_time, transaction_time, sequence, flags = _parse_metadata(raw_payload, stream)
    return EventEnvelope(
        market="um_perpetual",
        symbol="BTCUSDT",
        stream=stream.value,
        module="binance.usdm.websocket.v1",
        connection_id=connection_id,
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_time_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        exchange_event_time=event_time,
        exchange_trade_time=transaction_time,
        source_sequence=sequence,
        raw_payload=raw_payload,
        capture_flags=flags,
    )
