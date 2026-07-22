"""Strict adapters from immutable EventEnvelope v1 to M6 derived inputs."""

from __future__ import annotations

import json
from typing import Any

from ..domain.event import EventEnvelope
from .model import BookSnapshot, BookTicker, DepthUpdate, Level, OrderBookDataError


def _object(raw_payload: bytes) -> dict[str, Any]:
    try:
        value = json.loads(raw_payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OrderBookDataError("invalid JSON payload") from exc
    if not isinstance(value, dict):
        raise OrderBookDataError("payload must be a JSON object")
    return value


def _integer(value: dict[str, Any], name: str) -> int:
    result = value.get(name)
    if not isinstance(result, int) or isinstance(result, bool) or result < 0:
        raise OrderBookDataError(f"{name} must be a non-negative integer")
    return result


def _text(value: dict[str, Any], name: str) -> str:
    result = value.get(name)
    if not isinstance(result, str) or not result:
        raise OrderBookDataError(f"{name} must be non-empty text")
    return result


def _levels(value: object) -> tuple[Level, ...]:
    if not isinstance(value, list):
        raise OrderBookDataError("price levels must be an array")
    levels: list[Level] = []
    for level in value:
        if (
            not isinstance(level, list)
            or len(level) != 2
            or not all(isinstance(item, str) for item in level)
        ):
            raise OrderBookDataError("invalid price level")
        levels.append((level[0], level[1]))
    return tuple(levels)


def depth_update_from_envelope(envelope: EventEnvelope) -> DepthUpdate:
    if envelope.stream != "diff_depth":
        raise OrderBookDataError("envelope is not a diff-depth event")
    payload = _object(envelope.raw_payload)
    previous = _integer(payload, "pu") if envelope.market == "um_perpetual" else None
    return DepthUpdate(
        market=envelope.market,
        symbol=_text(payload, "s"),
        first_update_id=_integer(payload, "U"),
        final_update_id=_integer(payload, "u"),
        previous_final_update_id=previous,
        bids=_levels(payload.get("b")),
        asks=_levels(payload.get("a")),
        receive_time_utc_ns=envelope.receive_time_utc_ns,
    )


def snapshot_from_envelope(envelope: EventEnvelope) -> BookSnapshot:
    if envelope.stream != "depth_snapshot":
        raise OrderBookDataError("envelope is not a depth snapshot")
    provenance = _object(envelope.raw_payload)
    try:
        response = provenance["response"]
        if not isinstance(response, dict):
            raise TypeError
        model = response["model"]
        if not isinstance(model, dict):
            raise TypeError
    except (KeyError, TypeError) as exc:
        raise OrderBookDataError("invalid snapshot provenance") from exc
    return BookSnapshot(
        market=envelope.market,
        symbol=envelope.symbol,
        last_update_id=_integer(model, "lastUpdateId"),
        bids=_levels(model.get("bids")),
        asks=_levels(model.get("asks")),
    )


def book_ticker_from_envelope(envelope: EventEnvelope) -> BookTicker:
    if envelope.stream != "book_ticker":
        raise OrderBookDataError("envelope is not a book-ticker event")
    payload = _object(envelope.raw_payload)
    return BookTicker(
        market=envelope.market,
        symbol=_text(payload, "s"),
        update_id=_integer(payload, "u"),
        bid_price=_text(payload, "b"),
        bid_quantity=_text(payload, "B"),
        ask_price=_text(payload, "a"),
        ask_quantity=_text(payload, "A"),
    )
