"""Strict stream-specific conversion from EventEnvelope payloads."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from ..domain.event import EventEnvelope
from .model import SUPPORTED_STREAMS, ParsedEvent, stream_fields


class NormalizedSchemaError(ValueError):
    """A Raw payload cannot satisfy its declared normalized stream schema."""


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_json(value: object | Mapping[str, object]) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def _object(value: object, name: str = "value") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise NormalizedSchemaError(f"{name} must be an object")
    return value


def _array(value: object, name: str) -> list[Any]:
    if not isinstance(value, list):
        raise NormalizedSchemaError(f"{name} must be an array")
    return value


def _text(value: dict[str, Any], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item:
        raise NormalizedSchemaError(f"{name} must be non-empty text")
    return item


def _string(value: dict[str, Any], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise NormalizedSchemaError(f"{name} must be text")
    return item


def _integer(value: dict[str, Any], name: str) -> int:
    item = value.get(name)
    if not isinstance(item, int) or isinstance(item, bool) or item < 0:
        raise NormalizedSchemaError(f"{name} must be a non-negative integer")
    return item


def _boolean(value: dict[str, Any], name: str) -> bool:
    item = value.get(name)
    if not isinstance(item, bool):
        raise NormalizedSchemaError(f"{name} must be boolean")
    return item


def _levels(value: dict[str, Any], name: str) -> list[list[str]]:
    levels = _array(value.get(name), name)
    if any(
        not isinstance(level, list)
        or len(level) != 2
        or any(not isinstance(item, str) for item in level)
        for level in levels
    ):
        raise NormalizedSchemaError(f"{name} contains invalid price levels")
    return levels


def _decode(envelope: EventEnvelope) -> object:
    try:
        return json.loads(envelope.raw_payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NormalizedSchemaError("payload is not valid UTF-8 JSON") from exc


def _deployment_id(envelope: EventEnvelope) -> str | None:
    prefix = "deployment_id="
    return next(
        (flag[len(prefix) :] for flag in envelope.capture_flags if flag.startswith(prefix)),
        None,
    )


def _poll_identity(
    envelope: EventEnvelope, *, model_hash: str
) -> dict[str, object]:
    deployment_id = _deployment_id(envelope)
    if deployment_id is not None and "blue_green_overlap" in envelope.capture_flags:
        return {
            "kind": "blue_green_poll",
            "deployment_id": deployment_id,
            "model_sha256": model_hash,
        }
    return {
        "kind": "poll_observation",
        "receive_time_utc_ns": envelope.receive_time_utc_ns,
        "collector_instance_id": envelope.collector_instance_id,
        "connection_id": envelope.connection_id,
    }


def _core_websocket(envelope: EventEnvelope, payload: dict[str, Any]) -> list[ParsedEvent]:
    stream = envelope.stream
    if payload.get("e") == "serverShutdown":
        event_time = _integer(payload, "E")
        fields: dict[str, object] = {
            name: None
            for name in _stream_field_names(envelope.market, stream)
        }
        content = {"event_kind": "server_shutdown", "event_time": event_time}
        return [
            ParsedEvent(
                fields,
                {"kind": "server_shutdown", "event_time": event_time},
                content,
                event_kind="server_shutdown",
            )
        ]
    if _text(payload, "s") != envelope.symbol:
        raise NormalizedSchemaError("payload symbol differs from envelope")
    if stream == "diff_depth":
        first = _integer(payload, "U")
        final = _integer(payload, "u")
        if first > final:
            raise NormalizedSchemaError("depth U exceeds u")
        previous = (
            _integer(payload, "pu") if envelope.market == "um_perpetual" else None
        )
        bids = _levels(payload, "b")
        asks = _levels(payload, "a")
        fields = {
            "first_update_id": first,
            "final_update_id": final,
            "previous_final_update_id": previous,
            "bids_json": canonical_json(bids),
            "asks_json": canonical_json(asks),
        }
        identity: dict[str, object] = {
            "kind": stream,
            "market": envelope.market,
            "symbol": envelope.symbol,
            "U": first,
            "u": final,
        }
        if previous is not None:
            identity["pu"] = previous
        return [ParsedEvent(fields, identity, fields)]
    if stream == "agg_trade":
        aggregate = _integer(payload, "a")
        first_trade = _integer(payload, "f")
        last_trade = _integer(payload, "l")
        if first_trade > last_trade:
            raise NormalizedSchemaError("aggregate trade f exceeds l")
        fields = {
            "aggregate_trade_id": aggregate,
            "first_trade_id": first_trade,
            "last_trade_id": last_trade,
            "price": _text(payload, "p"),
            "quantity": _text(payload, "q"),
            "buyer_is_maker": _boolean(payload, "m"),
            "best_match": (
                _boolean(payload, "M") if envelope.market == "spot" else None
            ),
        }
        identity = {
            "kind": stream,
            "market": envelope.market,
            "symbol": envelope.symbol,
            "aggregate_trade_id": aggregate,
        }
        return [ParsedEvent(fields, identity, fields)]
    update_id = _integer(payload, "u")
    fields = {
        "update_id": update_id,
        "bid_price": _text(payload, "b"),
        "bid_quantity": _text(payload, "B"),
        "ask_price": _text(payload, "a"),
        "ask_quantity": _text(payload, "A"),
    }
    identity = {
        "kind": stream,
        "market": envelope.market,
        "symbol": envelope.symbol,
        "update_id": update_id,
    }
    return [ParsedEvent(fields, identity, fields)]


def _depth_snapshot(envelope: EventEnvelope, provenance: dict[str, Any]) -> list[ParsedEvent]:
    response = _object(provenance.get("response"), "response")
    request = _object(provenance.get("request"), "request")
    model = _object(response.get("model"), "response.model")
    last_update_id = _integer(model, "lastUpdateId")
    bids = _levels(model, "bids")
    asks = _levels(model, "asks")
    model_hash = sha256_json(model)
    fields = {
        "last_update_id": last_update_id,
        "bids_json": canonical_json(bids),
        "asks_json": canonical_json(asks),
        "response_status": _integer(response, "status"),
        "request_time_utc_ns": _integer(request, "request_time_utc_ns"),
        "response_model_sha256": model_hash,
    }
    identity = {
        "kind": "depth_snapshot",
        "market": envelope.market,
        "last_update_id": last_update_id,
        "model_sha256": model_hash,
    }
    return [ParsedEvent(fields, identity, fields, event_kind="rest_snapshot")]


def _mark_price(envelope: EventEnvelope, payload: dict[str, Any]) -> list[ParsedEvent]:
    event_time = _integer(payload, "E")
    funding_time = _integer(payload, "T")
    fields = {
        "mark_price": _text(payload, "p"),
        "index_price": _text(payload, "i"),
        "estimated_settle_price": _text(payload, "P"),
        "funding_rate": _text(payload, "r"),
        "next_funding_time_ms": funding_time,
    }
    identity = {
        "kind": "mark_price",
        "symbol": envelope.symbol,
        "event_time": event_time,
        "next_funding_time": funding_time,
    }
    return [ParsedEvent(fields, identity, fields)]


def _liquidation(envelope: EventEnvelope, payload: dict[str, Any]) -> list[ParsedEvent]:
    event_time = _integer(payload, "E")
    order = _object(payload.get("o"), "o")
    if _text(order, "s") != envelope.symbol:
        raise NormalizedSchemaError("liquidation symbol differs from envelope")
    trade_time = _integer(order, "T")
    fields = {
        "side": _text(order, "S"),
        "order_type": _text(order, "o"),
        "time_in_force": _text(order, "f"),
        "original_quantity": _text(order, "q"),
        "price": _text(order, "p"),
        "average_price": _text(order, "ap"),
        "order_status": _text(order, "X"),
        "last_filled_quantity": _text(order, "l"),
        "accumulated_filled_quantity": _text(order, "z"),
        "order_trade_time_ms": trade_time,
    }
    identity = {
        "kind": "liquidation",
        "symbol": envelope.symbol,
        "event_time": event_time,
        "trade_time": trade_time,
        "side": fields["side"],
    }
    return [ParsedEvent(fields, identity, fields)]


FIVE_MINUTE_STREAMS = frozenset(
    {
        "open_interest_statistics_5m",
        "taker_buy_sell_volume_5m",
        "global_long_short_ratio_5m",
        "top_long_short_account_ratio_5m",
        "top_long_short_position_ratio_5m",
        "basis_5m",
    }
)


def _optional_json_array(value: dict[str, Any], name: str) -> str | None:
    if name not in value:
        return None
    return canonical_json(_array(value.get(name), name))


def _spot_exchange_info(
    envelope: EventEnvelope, model: object
) -> list[ParsedEvent]:
    item = _object(model, "response.model")
    symbols = _array(item.get("symbols"), "symbols")
    matches = [
        _object(symbol, "exchange symbol")
        for symbol in symbols
        if isinstance(symbol, dict) and symbol.get("symbol") == envelope.symbol
    ]
    if len(matches) > 1:
        raise NormalizedSchemaError(
            "Spot exchange info identifies BTCUSDT more than once"
        )
    symbol = matches[0] if matches else None
    model_hash = sha256_json(item)
    fields = {
        "symbol_present": symbol is not None,
        "server_time_ms": (
            _integer(item, "serverTime") if "serverTime" in item else None
        ),
        "trading_status": (
            _text(symbol, "status") if symbol is not None else None
        ),
        "filters_json": (
            canonical_json(_array(symbol.get("filters"), "filters"))
            if symbol is not None
            else None
        ),
        "order_types_json": (
            canonical_json(_array(symbol.get("orderTypes"), "orderTypes"))
            if symbol is not None
            else None
        ),
        "rate_limits_json": (
            canonical_json(_array(item.get("rateLimits"), "rateLimits"))
            if "rateLimits" in item
            else None
        ),
        "permissions_json": (
            _optional_json_array(symbol, "permissions")
            if symbol is not None
            else None
        ),
        "permission_sets_json": (
            _optional_json_array(symbol, "permissionSets")
            if symbol is not None
            else None
        ),
        "response_model_sha256": model_hash,
    }
    identity = {
        "kind": "exchange_info",
        "market": envelope.market,
        "symbol": envelope.symbol,
        "model_sha256": model_hash,
    }
    return [ParsedEvent(fields, identity, fields, event_kind="rest_snapshot")]


def _usdm_exchange_info(
    envelope: EventEnvelope, model: object
) -> list[ParsedEvent]:
    item = _object(model, "response.model")
    symbols = _array(item.get("symbols"), "symbols")
    rate_limits = _array(item.get("rateLimits"), "rateLimits")
    matches = [
        _object(symbol, "exchange symbol")
        for symbol in symbols
        if isinstance(symbol, dict) and symbol.get("symbol") == envelope.symbol
    ]
    if len(matches) != 1:
        raise NormalizedSchemaError("exchange info does not identify BTCUSDT once")
    symbol = matches[0]
    fields = {
        "symbol_present": True,
        "server_time_ms": (
            _integer(item, "serverTime") if "serverTime" in item else None
        ),
        "contract_type": _text(symbol, "contractType"),
        "trading_status": _text(symbol, "status"),
        "filters_json": canonical_json(_array(symbol.get("filters"), "filters")),
        "rate_limits_json": canonical_json(rate_limits),
    }
    model_hash = sha256_json(item)
    identity = {
        "kind": envelope.stream,
        **_poll_identity(envelope, model_hash=model_hash),
    }
    return [ParsedEvent(fields, identity, fields, event_kind="rest_snapshot")]


def _request_period_range(provenance: dict[str, Any]) -> tuple[int, int]:
    request = _object(provenance.get("request"), "request")
    parameters = _object(request.get("parameters"), "request.parameters")
    start_name = (
        "requestedStartTime"
        if "requestedStartTime" in parameters
        else "startTime"
    )
    end_name = (
        "requestedEndTime" if "requestedEndTime" in parameters else "endTime"
    )
    return _integer(parameters, start_name), _integer(parameters, end_name)


def _five_minute_rest(
    envelope: EventEnvelope,
    provenance: dict[str, Any],
    model: object,
) -> list[ParsedEvent]:
    items = _array(model, "response.model")
    stream = envelope.stream
    if not items:
        start_ms, end_ms = _request_period_range(provenance)
        model_hash = sha256_json(items)
        fields: dict[str, object] = {
            name: None
            for name, _type, _nullable in stream_fields(
                envelope.market, stream
            )
        }
        fields["observation_empty"] = True
        identity = {
            "kind": stream,
            "market": envelope.market,
            "symbol": envelope.symbol,
            "requested_start_ms": start_ms,
            "requested_end_ms": end_ms,
            "model_sha256": model_hash,
        }
        return [
            ParsedEvent(
                fields,
                identity,
                fields,
                event_kind="empty_observation",
            )
        ]

    output: list[ParsedEvent] = []
    ratio_streams = {
        "global_long_short_ratio_5m",
        "top_long_short_account_ratio_5m",
        "top_long_short_position_ratio_5m",
    }
    for ordinal, raw_item in enumerate(items):
        item = _object(raw_item, f"{stream} item")
        timestamp = _integer(item, "timestamp")
        if stream == "open_interest_statistics_5m":
            if _text(item, "symbol") != envelope.symbol:
                raise NormalizedSchemaError("open-interest symbol differs")
            fields = {
                "observation_empty": False,
                "timestamp_ms": timestamp,
                "sum_open_interest": _text(item, "sumOpenInterest"),
                "sum_open_interest_value": _text(
                    item, "sumOpenInterestValue"
                ),
            }
        elif stream == "taker_buy_sell_volume_5m":
            fields = {
                "observation_empty": False,
                "timestamp_ms": timestamp,
                "buy_sell_ratio": _text(item, "buySellRatio"),
                "buy_volume": _text(item, "buyVol"),
                "sell_volume": _text(item, "sellVol"),
            }
        elif stream in ratio_streams:
            if _text(item, "symbol") != envelope.symbol:
                raise NormalizedSchemaError("long-short ratio symbol differs")
            fields = {
                "observation_empty": False,
                "timestamp_ms": timestamp,
                "long_short_ratio": _text(item, "longShortRatio"),
                "long_account": _text(item, "longAccount"),
                "short_account": _text(item, "shortAccount"),
            }
        else:
            pair = _text(item, "pair")
            if pair != envelope.symbol:
                raise NormalizedSchemaError("basis pair differs from envelope")
            fields = {
                "observation_empty": False,
                "timestamp_ms": timestamp,
                "pair": pair,
                "contract_type": _text(item, "contractType"),
                "index_price": _text(item, "indexPrice"),
                "futures_price": _text(item, "futuresPrice"),
                "basis": _text(item, "basis"),
                "basis_rate": _text(item, "basisRate"),
                "annualized_basis_rate": _string(
                    item, "annualizedBasisRate"
                ),
            }
        identity = {
            "kind": stream,
            "market": envelope.market,
            "symbol": envelope.symbol,
            "timestamp_ms": timestamp,
        }
        output.append(
            ParsedEvent(
                fields,
                identity,
                fields,
                subrecord_ordinal=ordinal,
                event_kind="rest_period",
            )
        )
    return output


def _side_rest(envelope: EventEnvelope, provenance: dict[str, Any]) -> list[ParsedEvent]:
    response = _object(provenance.get("response"), "response")
    model = response.get("model")
    stream = envelope.stream
    if stream == "exchange_info":
        if envelope.market == "spot":
            return _spot_exchange_info(envelope, model)
        return _usdm_exchange_info(envelope, model)
    if stream in FIVE_MINUTE_STREAMS:
        return _five_minute_rest(envelope, provenance, model)
    if stream == "premium_index_snapshot":
        item = _object(model, "response.model")
        fields = {
            "mark_price": _text(item, "markPrice"),
            "index_price": _text(item, "indexPrice"),
            "estimated_settle_price": _text(item, "estimatedSettlePrice"),
            "last_funding_rate": _text(item, "lastFundingRate"),
            "interest_rate": _text(item, "interestRate"),
            "next_funding_time_ms": _integer(item, "nextFundingTime"),
            "observation_time_ms": _integer(item, "time"),
        }
        identity = {
            "kind": stream,
            "symbol": envelope.symbol,
            "observation_time": fields["observation_time_ms"],
        }
        return [ParsedEvent(fields, identity, fields, event_kind="rest_snapshot")]
    if stream == "open_interest":
        item = _object(model, "response.model")
        fields = {
            "open_interest": _text(item, "openInterest"),
            "observation_time_ms": _integer(item, "time"),
        }
        identity = {
            "kind": stream,
            "symbol": envelope.symbol,
            "observation_time": fields["observation_time_ms"],
        }
        return [ParsedEvent(fields, identity, fields, event_kind="rest_snapshot")]
    if stream == "funding_history":
        items = _array(model, "response.model")
        if not items:
            fields = {
                "observation_empty": True,
                "funding_time_ms": None,
                "funding_rate": None,
                "mark_price": None,
            }
            identity = _poll_identity(envelope, model_hash=sha256_json(items))
            return [
                ParsedEvent(
                    fields,
                    {"kind": stream, **identity},
                    fields,
                    event_kind="empty_observation",
                )
            ]
        output: list[ParsedEvent] = []
        for ordinal, raw_item in enumerate(items):
            item = _object(raw_item, "funding history item")
            if _text(item, "symbol") != envelope.symbol:
                raise NormalizedSchemaError("funding-history symbol differs")
            funding_time = _integer(item, "fundingTime")
            fields = {
                "observation_empty": False,
                "funding_time_ms": funding_time,
                "funding_rate": _text(item, "fundingRate"),
                "mark_price": (
                    _text(item, "markPrice") if "markPrice" in item else None
                ),
            }
            identity = {
                "kind": stream,
                "symbol": envelope.symbol,
                "funding_time": funding_time,
            }
            output.append(ParsedEvent(fields, identity, fields, ordinal))
        return output
    if stream == "funding_info":
        items = _array(model, "response.model")
        matches = [
            _object(item, "funding info item")
            for item in items
            if isinstance(item, dict) and item.get("symbol") == envelope.symbol
        ]
        if len(matches) > 1:
            raise NormalizedSchemaError("multiple BTCUSDT funding-info records")
        funding_item = matches[0] if matches else None
        fields = {
            "symbol_present": funding_item is not None,
            "observation_record_count": len(items),
            "adjusted_funding_rate_cap": (
                _text(funding_item, "adjustedFundingRateCap")
                if funding_item is not None
                else None
            ),
            "adjusted_funding_rate_floor": (
                _text(funding_item, "adjustedFundingRateFloor")
                if funding_item is not None
                else None
            ),
            "funding_interval_hours": (
                _integer(funding_item, "fundingIntervalHours")
                if funding_item is not None
                else None
            ),
            "disclaimer": (
                _boolean(funding_item, "disclaimer")
                if funding_item is not None and "disclaimer" in funding_item
                else None
            ),
        }
        model_hash = sha256_json(items)
        identity = {
            "kind": stream,
            **_poll_identity(envelope, model_hash=model_hash),
        }
        return [ParsedEvent(fields, identity, fields, event_kind="rest_snapshot")]
    raise NormalizedSchemaError(f"unhandled REST side-data stream {stream}")


def _stream_field_names(market: str, stream: str) -> tuple[str, ...]:
    return tuple(name for name, _type, _nullable in stream_fields(market, stream))


def _invalid(envelope: EventEnvelope, error: str) -> list[ParsedEvent]:
    payload_hash = hashlib.sha256(envelope.raw_payload).hexdigest()
    fields: dict[str, object] = {
        name: None
        for name in _stream_field_names(envelope.market, envelope.stream)
    }
    for name, default in (
        ("observation_empty", False),
        ("symbol_present", False),
        ("observation_record_count", 0),
    ):
        if name in fields:
            fields[name] = default
    identity = {"kind": "malformed", "raw_payload_sha256": payload_hash}
    return [
        ParsedEvent(
            fields,
            identity,
            {"error_code": error, **identity},
            valid=False,
            error_code=error,
            event_kind="malformed",
        )
    ]


def parse_envelope(envelope: EventEnvelope) -> list[ParsedEvent]:
    if envelope.stream not in SUPPORTED_STREAMS.get(
        envelope.market, frozenset()
    ):
        raise NormalizedSchemaError(
            f"unsupported Raw stream {envelope.market}/{envelope.stream}"
        )
    if "malformed" in envelope.capture_flags:
        return _invalid(envelope, "UPSTREAM_MALFORMED")
    try:
        decoded = _decode(envelope)
        payload = _object(decoded, "payload")
        if envelope.stream in {"diff_depth", "agg_trade", "book_ticker"}:
            return _core_websocket(envelope, payload)
        if envelope.stream == "depth_snapshot":
            return _depth_snapshot(envelope, payload)
        if envelope.stream == "mark_price":
            return _mark_price(envelope, payload)
        if envelope.stream == "liquidation":
            return _liquidation(envelope, payload)
        return _side_rest(envelope, payload)
    except NormalizedSchemaError as exc:
        return _invalid(envelope, f"SCHEMA_ERROR:{exc}")
