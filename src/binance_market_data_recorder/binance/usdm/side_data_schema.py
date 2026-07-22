"""Lossless USD-M mark-price and liquidation side-stream schemas."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from ...domain.event import EventEnvelope


class UsdMSideStream(StrEnum):
    MARK_PRICE = "mark_price"
    LIQUIDATION = "liquidation"


@dataclass(frozen=True)
class UsdMSideStreamSpec:
    stream: UsdMSideStream
    route: str
    wire_name: str
    semantics: str


USDM_SIDE_STREAMS: tuple[UsdMSideStreamSpec, ...] = (
    UsdMSideStreamSpec(
        UsdMSideStream.MARK_PRICE,
        "market",
        "btcusdt@markPrice@1s",
        "periodic_1s",
    ),
    UsdMSideStreamSpec(
        UsdMSideStream.LIQUIDATION,
        "market",
        "btcusdt@forceOrder",
        "event_sparse_latest_snapshot_within_1000ms",
    ),
)


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


def _decode(raw_payload: bytes) -> dict[str, Any]:
    value = json.loads(raw_payload)
    if not isinstance(value, dict):
        raise ValueError("side-data payload root must be an object")
    return value


def _mark_price_metadata(payload: dict[str, Any]) -> tuple[int, None, dict[str, int | str]]:
    if payload.get("e") != "markPriceUpdate" or _text(payload, "s") != "BTCUSDT":
        raise ValueError("unexpected mark-price identity")
    if _integer(payload, "st") != 1:
        raise ValueError("mark-price payload is not USD-M")
    event_time = _integer(payload, "E")
    next_funding_time = _integer(payload, "T")
    for name in ("p", "i", "P", "r", "ap"):
        _text(payload, name)
    return event_time, None, {"nextFundingTime": next_funding_time}


def _liquidation_metadata(payload: dict[str, Any]) -> tuple[int, int, dict[str, int | str]]:
    if payload.get("e") != "forceOrder":
        raise ValueError("unexpected liquidation event type")
    event_time = _integer(payload, "E")
    order = payload.get("o")
    if not isinstance(order, dict) or _text(order, "s") != "BTCUSDT":
        raise ValueError("unexpected liquidation symbol")
    for name in ("S", "o", "f", "q", "p", "ap", "X", "l", "z"):
        _text(order, name)
    trade_time = _integer(order, "T")
    return event_time, trade_time, {"orderTradeTime": trade_time}


def envelope_from_side_stream_frame(
    *,
    raw_payload: bytes,
    stream: UsdMSideStream,
    connection_id: str,
    collector_instance_id: str,
    collector_version: str,
    receive_time_utc_ns: int,
    receive_monotonic_ns: int,
) -> EventEnvelope:
    """Preserve exact side-stream bytes and extract only official metadata."""

    flags: tuple[str, ...] = (
        ("event_sparse_snapshot_stream",) if stream is UsdMSideStream.LIQUIDATION else ()
    )
    event_time: int | None = None
    trade_time: int | None = None
    sequence: dict[str, int | str] = {}
    try:
        payload = _decode(raw_payload)
        if stream is UsdMSideStream.MARK_PRICE:
            event_time, trade_time, sequence = _mark_price_metadata(payload)
        else:
            event_time, trade_time, sequence = _liquidation_metadata(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError):
        flags = (*flags, "malformed")
    return EventEnvelope(
        market="um_perpetual",
        symbol="BTCUSDT",
        stream=stream.value,
        module="binance.usdm.side_stream.v1",
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
