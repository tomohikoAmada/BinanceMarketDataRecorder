"""Lossless Binance Spot WebSocket envelope extraction.

Parsing enriches capture metadata but never replaces or rewrites the bytes
received from the WebSocket transport. Schema failures remain Raw records.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ...domain.event import EventEnvelope


class SpotStream(StrEnum):
    DIFF_DEPTH = "diff_depth"
    AGG_TRADE = "agg_trade"
    BOOK_TICKER = "book_ticker"


@dataclass(frozen=True)
class SpotStreamSpec:
    stream: SpotStream
    wire_name: str
    expected_event: str | None


SPOT_STREAMS: tuple[SpotStreamSpec, ...] = (
    SpotStreamSpec(SpotStream.DIFF_DEPTH, "btcusdt@depth@100ms", "depthUpdate"),
    SpotStreamSpec(SpotStream.AGG_TRADE, "btcusdt@aggTrade", "aggTrade"),
    SpotStreamSpec(SpotStream.BOOK_TICKER, "btcusdt@bookTicker", None),
)

_SPEC_BY_STREAM = {spec.stream: spec for spec in SPOT_STREAMS}


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


def _check_symbol(payload: dict[str, Any]) -> None:
    if _text(payload, "s") != "BTCUSDT":
        raise ValueError("unexpected symbol")


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


def _boolean(payload: dict[str, Any], name: str) -> None:
    if not isinstance(payload.get(name), bool):
        raise ValueError(f"{name} must be boolean")


def _parse_metadata(
    raw_payload: bytes, stream: SpotStream
) -> tuple[int | None, int | None, dict[str, int | str], tuple[str, ...]]:
    try:
        decoded = json.loads(raw_payload)
        if not isinstance(decoded, dict):
            raise ValueError("payload root must be an object")
        event_type = decoded.get("e")
        if event_type == "serverShutdown":
            return _integer(decoded, "E"), None, {}, ("server_shutdown",)

        spec = _SPEC_BY_STREAM[stream]
        if spec.expected_event is not None and event_type != spec.expected_event:
            raise ValueError(f"unexpected event type for {stream}")
        _check_symbol(decoded)

        if stream is SpotStream.DIFF_DEPTH:
            first = _integer(decoded, "U")
            last = _integer(decoded, "u")
            _integer(decoded, "E")
            if first > last:
                raise ValueError("depth U exceeds u")
            _levels(decoded, "b")
            _levels(decoded, "a")
            return decoded["E"], None, {"U": first, "u": last}, ()
        if stream is SpotStream.AGG_TRADE:
            aggregate = _integer(decoded, "a")
            first_trade = _integer(decoded, "f")
            last_trade = _integer(decoded, "l")
            event_time = _integer(decoded, "E")
            trade_time = _integer(decoded, "T")
            _text(decoded, "p")
            _text(decoded, "q")
            _boolean(decoded, "m")
            _boolean(decoded, "M")
            if first_trade > last_trade:
                raise ValueError("aggregate trade f exceeds l")
            return (
                event_time,
                trade_time,
                {"a": aggregate, "f": first_trade, "l": last_trade},
                (),
            )

        update_id = _integer(decoded, "u")
        for name in ("b", "B", "a", "A"):
            _text(decoded, name)
        return None, None, {"u": update_id}, ()
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
        return None, None, {}, ("malformed",)


def envelope_from_websocket_frame(
    *,
    raw_payload: bytes,
    stream: SpotStream,
    connection_id: str,
    collector_instance_id: str,
    collector_version: str,
    receive_time_utc_ns: int,
    receive_monotonic_ns: int,
) -> EventEnvelope:
    """Create a Raw envelope while retaining the exact transport bytes."""

    event_time, trade_time, sequence, flags = _parse_metadata(raw_payload, stream)
    return EventEnvelope(
        market="spot",
        symbol="BTCUSDT",
        stream=stream.value,
        module="binance.spot.websocket.v1",
        connection_id=connection_id,
        collector_instance_id=collector_instance_id,
        collector_version=collector_version,
        receive_time_utc_ns=receive_time_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        exchange_event_time=event_time,
        exchange_trade_time=trade_time,
        source_sequence=sequence,
        raw_payload=raw_payload,
        capture_flags=flags,
    )
