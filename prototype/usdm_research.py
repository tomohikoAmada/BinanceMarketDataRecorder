"""R4 C2-aligned USD-M Perpetual research parser, exporter, and parity helper."""

from __future__ import annotations

import argparse
import base64
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize.raw import (
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import (
    LocalBookReconstructor,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter

from partition_roots import reader_lock, resolve_roots

RESEARCH_DATASET_VERSION = "normalized-dataset.v3-usdm-research.v1"
DEFAULT_PARITY_SYMBOLS: tuple[str, ...] = (
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "XRPUSDT",
)

WS_STREAMS: frozenset[str] = frozenset(
    {"diff_depth", "book_ticker", "agg_trade", "mark_price", "liquidation"}
)
REST_SOURCES: frozenset[str] = frozenset(
    {
        "open_interest",
        "top_long_short_account_ratio_5m",
        "top_long_short_position_ratio_5m",
        "global_long_short_ratio_5m",
        "taker_buy_sell_volume_5m",
        "exchange_info",
        "depth_snapshot",
    }
)
SUPPORTED_USDM_RESEARCH_SOURCES: frozenset[str] = WS_STREAMS | REST_SOURCES

SOURCE_PATH_TO_KIND: dict[str, str] = {
    "/fapi/v1/exchangeInfo": "exchange_info",
    "/fapi/v1/depth": "depth_snapshot",
    "/fapi/v1/openInterest": "open_interest",
    "/futures/data/topLongShortAccountRatio": "top_long_short_account_ratio_5m",
    "/futures/data/topLongShortPositionRatio": "top_long_short_position_ratio_5m",
    "/futures/data/globalLongShortAccountRatio": "global_long_short_ratio_5m",
    "/futures/data/takerlongshortRatio": "taker_buy_sell_volume_5m",
}

C2_SOURCE_KIND: dict[str, str] = {
    "open_interest": "OPEN_INTEREST",
    "top_long_short_account_ratio_5m": "TOP_LONG_SHORT_ACCOUNT_RATIO",
    "top_long_short_position_ratio_5m": "TOP_LONG_SHORT_POSITION_RATIO",
    "global_long_short_ratio_5m": "GLOBAL_LONG_SHORT_ACCOUNT_RATIO",
    "taker_buy_sell_volume_5m": "TAKER_BUY_SELL_VOLUME",
    "exchange_info": "INSTRUMENT",
    "depth_snapshot": "DEPTH_BASELINE",
}

RATIO_SOURCE_KINDS: frozenset[str] = frozenset(
    {
        "top_long_short_account_ratio_5m",
        "top_long_short_position_ratio_5m",
        "global_long_short_ratio_5m",
    }
)

COMMON_RESEARCH_FIELDS: tuple[tuple[str, pa.DataType, bool], ...] = (
    ("dataset_version", pa.string(), False),
    ("venue", pa.string(), False),
    ("market", pa.string(), False),
    ("symbol", pa.string(), False),
    ("stream", pa.string(), False),
    ("event_kind", pa.string(), False),
    ("receive_time_utc_ns", pa.int64(), False),
    ("receive_date", pa.string(), False),
    ("receive_hour", pa.int8(), False),
    ("receive_monotonic_ns", pa.int64(), False),
    ("exchange_event_time_ms", pa.int64(), True),
    ("exchange_transaction_time_ms", pa.int64(), True),
    ("exchange_trade_time_ms", pa.int64(), True),
    ("connection_id", pa.string(), False),
    ("collector_instance_id", pa.string(), False),
    ("collector_version", pa.string(), False),
    ("capture_flags_json", pa.string(), False),
    ("source_chunk_id", pa.string(), False),
    ("source_record_ordinal", pa.int64(), False),
    ("source_subrecord_ordinal", pa.int32(), False),
    ("semantic_key_sha256", pa.string(), False),
    ("logical_record_sha256", pa.string(), False),
    ("valid", pa.bool_(), False),
    ("error_code", pa.string(), True),
)

STREAM_RESEARCH_FIELDS: dict[str, tuple[tuple[str, pa.DataType, bool], ...]] = {
    "diff_depth": (
        ("first_update_id", pa.int64(), True),
        ("final_update_id", pa.int64(), True),
        ("previous_final_update_id", pa.int64(), True),
        ("bids_json", pa.string(), True),
        ("asks_json", pa.string(), True),
    ),
    "book_ticker": (
        ("update_id", pa.int64(), True),
        ("best_bid_price", pa.string(), True),
        ("best_bid_quantity", pa.string(), True),
        ("best_ask_price", pa.string(), True),
        ("best_ask_quantity", pa.string(), True),
    ),
    "agg_trade": (
        ("aggregate_trade_id", pa.int64(), True),
        ("first_trade_id", pa.int64(), True),
        ("last_trade_id", pa.int64(), True),
        ("price", pa.string(), True),
        ("quantity", pa.string(), True),
        ("normal_quantity", pa.string(), True),
        ("trade_time_ms", pa.int64(), True),
        ("buyer_is_maker", pa.bool_(), True),
    ),
    "mark_price": (
        ("mark_price", pa.string(), True),
        ("index_price", pa.string(), True),
        ("estimated_settle_price", pa.string(), True),
        ("funding_rate", pa.string(), True),
        ("next_funding_time_ms", pa.int64(), True),
        ("mark_price_moving_average", pa.string(), True),
    ),
    "liquidation": (
        ("side", pa.string(), True),
        ("order_type", pa.string(), True),
        ("time_in_force", pa.string(), True),
        ("original_quantity", pa.string(), True),
        ("price", pa.string(), True),
        ("average_price", pa.string(), True),
        ("order_status", pa.string(), True),
        ("last_filled_quantity", pa.string(), True),
        ("accumulated_filled_quantity", pa.string(), True),
        ("order_trade_time_ms", pa.int64(), True),
    ),
    "open_interest": (
        ("request_id", pa.string(), True),
        ("source_kind", pa.string(), False),
        ("status", pa.string(), False),
        ("source_time_ms", pa.int64(), True),
        ("open_interest", pa.string(), True),
    ),
    "top_long_short_account_ratio_5m": (
        ("request_id", pa.string(), True),
        ("source_kind", pa.string(), False),
        ("status", pa.string(), False),
        ("source_time_ms", pa.int64(), True),
        ("period", pa.string(), True),
        ("long_short_ratio", pa.string(), True),
        ("long_account", pa.string(), True),
        ("short_account", pa.string(), True),
    ),
    "top_long_short_position_ratio_5m": (
        ("request_id", pa.string(), True),
        ("source_kind", pa.string(), False),
        ("status", pa.string(), False),
        ("source_time_ms", pa.int64(), True),
        ("period", pa.string(), True),
        ("long_short_ratio", pa.string(), True),
        ("long_account", pa.string(), True),
        ("short_account", pa.string(), True),
    ),
    "global_long_short_ratio_5m": (
        ("request_id", pa.string(), True),
        ("source_kind", pa.string(), False),
        ("status", pa.string(), False),
        ("source_time_ms", pa.int64(), True),
        ("period", pa.string(), True),
        ("long_short_ratio", pa.string(), True),
        ("long_account", pa.string(), True),
        ("short_account", pa.string(), True),
    ),
    "taker_buy_sell_volume_5m": (
        ("request_id", pa.string(), True),
        ("source_kind", pa.string(), False),
        ("status", pa.string(), False),
        ("source_time_ms", pa.int64(), True),
        ("period", pa.string(), True),
        ("buy_sell_ratio", pa.string(), True),
        ("buy_volume", pa.string(), True),
        ("sell_volume", pa.string(), True),
    ),
    "exchange_info": (
        ("request_id", pa.string(), True),
        ("source_kind", pa.string(), False),
        ("status", pa.string(), False),
        ("source_time_ms", pa.int64(), True),
        ("symbol_present", pa.bool_(), False),
        ("contract_type", pa.string(), True),
        ("trading_status", pa.string(), True),
        ("base_asset", pa.string(), True),
        ("quote_asset", pa.string(), True),
        ("margin_asset", pa.string(), True),
        ("price_precision", pa.int32(), True),
        ("quantity_precision", pa.int32(), True),
        ("onboard_time_ms", pa.int64(), True),
        ("delivery_time_ms", pa.int64(), True),
        ("server_time_ms", pa.int64(), True),
        ("filters_json", pa.string(), True),
        ("rate_limits_json", pa.string(), True),
    ),
    "depth_snapshot": (
        ("request_id", pa.string(), True),
        ("source_kind", pa.string(), False),
        ("status", pa.string(), False),
        ("source_time_ms", pa.int64(), True),
        ("last_update_id", pa.int64(), True),
        ("bids_json", pa.string(), True),
        ("asks_json", pa.string(), True),
    ),
}


@dataclass(frozen=True, slots=True)
class ResearchRecord:
    stream: str
    symbol: str
    parquet_row: dict[str, Any]
    c2_event: dict[str, Any]
    semantic_key_sha256: str
    logical_record_sha256: str
    subrecord_ordinal: int = 0


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256_json(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _schema_for(stream: str) -> pa.Schema:
    if stream not in STREAM_RESEARCH_FIELDS:
        raise ValueError(f"unsupported research stream: {stream}")
    fields = [
        pa.field(name, dtype, nullable=nullable)
        for name, dtype, nullable in (
            *COMMON_RESEARCH_FIELDS,
            *STREAM_RESEARCH_FIELDS[stream],
        )
    ]
    return pa.schema(
        fields,
        metadata={
            b"bmdr.dataset_version": RESEARCH_DATASET_VERSION.encode("utf-8"),
            b"bmdr.stream": stream.encode("utf-8"),
        },
    )


def _receive_date_hour(receive_time_utc_ns: int) -> tuple[str, int]:
    dt = datetime.fromtimestamp(receive_time_utc_ns / 1_000_000_000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d"), dt.hour


def _req_str(obj: Mapping[str, Any], key: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing or non-text field {key!r}")
    return value


def _opt_str(obj: Mapping[str, Any], key: str) -> str | None:
    if key not in obj or obj[key] is None:
        return None
    value = obj[key]
    if not isinstance(value, str):
        raise ValueError(f"field {key!r} must be string when present")
    return value


def _req_int(obj: Mapping[str, Any], key: str) -> int:
    value = obj.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"field {key!r} must be non-negative int")
    return value


def _opt_int(obj: Mapping[str, Any], key: str) -> int | None:
    if key not in obj or obj[key] is None:
        return None
    return _req_int(obj, key)


def _req_bool(obj: Mapping[str, Any], key: str) -> bool:
    value = obj.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"field {key!r} must be bool")
    return value


def _levels(obj: Mapping[str, Any], key: str) -> list[list[str]]:
    raw = obj.get(key)
    if not isinstance(raw, list):
        raise ValueError(f"field {key!r} must be list")
    out: list[list[str]] = []
    for level in raw:
        if (
            not isinstance(level, list)
            or len(level) != 2
            or not isinstance(level[0], str)
            or not isinstance(level[1], str)
        ):
            raise ValueError(f"invalid price level in {key!r}")
        out.append([level[0], level[1]])
    return out


def validate_export_request(
    *,
    market: str = "um_perpetual",
    symbols: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    fields_by_source: Mapping[str, Sequence[str]] | None = None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Fail upfront on unsupported markets, symbols, sources, or fields."""
    if market not in {"um_perpetual", "MARKET_USD_M_PERPETUAL"}:
        raise ValueError(f"unsupported research market: {market}")
    selected_symbols = tuple(symbols) if symbols is not None else DEFAULT_PARITY_SYMBOLS
    if not selected_symbols or len(selected_symbols) > 20:
        raise ValueError("research symbols must contain 1..20 symbols")
    seen_sym: set[str] = set()
    for sym in selected_symbols:
        if (
            not isinstance(sym, str)
            or not sym
            or sym == "__GLOBAL__"
            or not sym.isascii()
            or not sym.isalnum()
            or sym != sym.upper()
            or sym in seen_sym
        ):
            raise ValueError(f"invalid or duplicate research symbol: {sym!r}")
        seen_sym.add(sym)

    selected_sources = (
        tuple(sources)
        if sources is not None
        else tuple(sorted(SUPPORTED_USDM_RESEARCH_SOURCES))
    )
    if not selected_sources:
        raise ValueError("research sources must not be empty")
    for source in selected_sources:
        if source not in SUPPORTED_USDM_RESEARCH_SOURCES:
            raise ValueError(f"unsupported research source: {source}")

    if fields_by_source is not None:
        common_names = {name for name, _t, _n in COMMON_RESEARCH_FIELDS}
        for source, requested_fields in fields_by_source.items():
            if source not in STREAM_RESEARCH_FIELDS:
                raise ValueError(f"unsupported research source in fields: {source}")
            allowed = common_names | {
                name for name, _t, _n in STREAM_RESEARCH_FIELDS[source]
            }
            for field_name in requested_fields:
                if field_name not in allowed:
                    raise ValueError(
                        f"unsupported research field {field_name!r} for {source}"
                    )
    return selected_symbols, selected_sources


def load_profile(profile_path: Path) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    """Load and upfront-validate a research profile JSON file."""
    doc = json.loads(profile_path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        raise ValueError("profile JSON must be an object")
    market = str(
        (doc.get("market_mapping") or {}).get("recorder")
        or doc.get("market")
        or "um_perpetual"
    )
    symbols = doc.get("symbols")
    sources: list[str] = []
    for section in ("ws_sources", "periodic_rest_sources", "reference_sources"):
        for item in doc.get(section, []):
            if isinstance(item, dict) and isinstance(item.get("source"), str):
                sources.append(item["source"])
    validated_symbols, validated_sources = validate_export_request(
        market=market,
        symbols=symbols,
        sources=sources or None,
    )
    return market, validated_symbols, validated_sources


def _map_quality_flags(flags: Sequence[str]) -> list[str]:
    mapped = {
        "duplicate": "DUPLICATE",
        "out_of_order": "OUT_OF_ORDER",
        "sequence_gap": "SEQUENCE_GAP",
        "ingress_drop": "SEQUENCE_GAP",
        "malformed": "MALFORMED_PAYLOAD",
        "recovered": "RECOVERED_TAIL",
    }
    result: list[str] = []
    for flag in flags:
        token = mapped.get(flag)
        if token and token not in result:
            result.append(token)
        elif flag.isupper() and flag not in result:
            result.append(flag)
    return result


def _normalize_filter(raw: Mapping[str, Any]) -> dict[str, Any]:
    filter_type = _req_str(raw, "filterType")
    out: dict[str, Any] = {"filter_type": filter_type}
    mapping = {
        "price": "price",
        "quantity": "quantity",
        "notional": "notional",
        "minNotional": "notional",
        "limit": "limit",
        "minPrice": "min_price",
        "maxPrice": "max_price",
        "tickSize": "tick_size",
        "minQty": "min_quantity",
        "maxQty": "max_quantity",
        "stepSize": "step_size",
        "multiplierUp": "multiplier_up",
        "multiplierDown": "multiplier_down",
        "multiplierDecimal": "multiplier_decimal",
        "positionControlSide": "position_control_side",
    }
    for src_key, dst_key in mapping.items():
        if src_key in raw and raw[src_key] is not None and dst_key not in out:
            out[dst_key] = raw[src_key]
    return out


def _normalize_rate_limit(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "rate_limit_type": _req_str(raw, "rateLimitType"),
        "interval": _req_str(raw, "interval"),
        "interval_num": _req_int(raw, "intervalNum"),
        "limit": _req_int(raw, "limit"),
    }


def _common_row(
    envelope: EventEnvelope,
    *,
    symbol: str,
    stream: str,
    event_kind: str,
    exchange_times: Mapping[str, int | None],
    source_chunk_id: str,
    source_record_ordinal: int,
    subrecord_ordinal: int,
    semantic_key_sha256: str,
    logical_record_sha256: str,
    valid: bool,
    error_code: str | None,
) -> dict[str, Any]:
    date_str, hour = _receive_date_hour(envelope.receive_time_utc_ns)
    return {
        "dataset_version": RESEARCH_DATASET_VERSION,
        "venue": "binance",
        "market": "um_perpetual",
        "symbol": symbol,
        "stream": stream,
        "event_kind": event_kind,
        "receive_time_utc_ns": envelope.receive_time_utc_ns,
        "receive_date": date_str,
        "receive_hour": hour,
        "receive_monotonic_ns": envelope.receive_monotonic_ns,
        "exchange_event_time_ms": exchange_times.get("exchange_event_time_ms"),
        "exchange_transaction_time_ms": exchange_times.get(
            "exchange_transaction_time_ms"
        ),
        "exchange_trade_time_ms": exchange_times.get("exchange_trade_time_ms"),
        "connection_id": envelope.connection_id,
        "collector_instance_id": envelope.collector_instance_id,
        "collector_version": envelope.collector_version,
        "capture_flags_json": canonical_json(list(envelope.capture_flags)),
        "source_chunk_id": source_chunk_id,
        "source_record_ordinal": source_record_ordinal,
        "source_subrecord_ordinal": subrecord_ordinal,
        "semantic_key_sha256": semantic_key_sha256,
        "logical_record_sha256": logical_record_sha256,
        "valid": valid,
        "error_code": error_code,
    }


def _unwrap_ws_payload(envelope: EventEnvelope) -> dict[str, Any]:
    decoded = json.loads(envelope.raw_payload.decode("utf-8"))
    if not isinstance(decoded, dict):
        raise ValueError("WS payload must be a JSON object")
    if (
        envelope.payload_encoding == "utf-8-json-combined.v1"
        or ("stream" in decoded and isinstance(decoded.get("data"), dict))
    ):
        data = decoded.get("data")
        if not isinstance(data, dict):
            raise ValueError("combined WS payload missing data object")
        return data
    return decoded


def _parse_ws_usdm(
    envelope: EventEnvelope,
    *,
    source_chunk_id: str,
    source_record_ordinal: int,
) -> list[ResearchRecord]:
    stream = envelope.stream
    payload = _unwrap_ws_payload(envelope)
    quality_flags = _map_quality_flags(envelope.capture_flags)
    base_meta = {
        "venue": "BINANCE",
        "market": "USD_M_PERPETUAL",
        "symbol": envelope.symbol,
        "producer": "binance-market-data-recorder",
        "producer_version": envelope.collector_version,
        "connection_id": envelope.connection_id,
        "receive_time_utc_ns": envelope.receive_time_utc_ns,
        "receive_monotonic_ns": envelope.receive_monotonic_ns,
        "quality_flags": quality_flags,
    }
    if stream == "diff_depth":
        if _req_str(payload, "s") != envelope.symbol:
            raise ValueError("diff_depth symbol mismatch")
        first = _req_int(payload, "U")
        final = _req_int(payload, "u")
        if first > final:
            raise ValueError("diff_depth U exceeds u")
        pu = _req_int(payload, "pu")
        bids = _levels(payload, "b")
        asks = _levels(payload, "a")
        event_time = _req_int(payload, "E")
        tx_time = _opt_int(payload, "T")
        exchange_times = {
            "exchange_event_time_ms": event_time,
            "exchange_transaction_time_ms": tx_time,
            "exchange_trade_time_ms": None,
        }
        stream_fields = {
            "first_update_id": first,
            "final_update_id": final,
            "previous_final_update_id": pu,
            "bids_json": canonical_json(bids),
            "asks_json": canonical_json(asks),
        }
        semantic_sha = _sha256_json(
            {"kind": stream, "symbol": envelope.symbol, "U": first, "u": final, "pu": pu}
        )
        logical_sha = _sha256_json(stream_fields)
        c2_event = {
            "metadata": {
                **base_meta,
                **exchange_times,
                "stream": "DIFF_DEPTH",
                "schema_version": "depth-update.v1",
            },
            "first_update_id": first,
            "final_update_id": final,
            "previous_final_update_id": pu,
            "bids": [{"price": p, "quantity": q} for p, q in bids],
            "asks": [{"price": p, "quantity": q} for p, q in asks],
        }
    elif stream == "book_ticker":
        if _req_str(payload, "s") != envelope.symbol:
            raise ValueError("book_ticker symbol mismatch")
        update_id = _req_int(payload, "u")
        event_time = _opt_int(payload, "E")
        tx_time = _opt_int(payload, "T")
        exchange_times = {
            "exchange_event_time_ms": event_time,
            "exchange_transaction_time_ms": tx_time,
            "exchange_trade_time_ms": None,
        }
        stream_fields = {
            "update_id": update_id,
            "best_bid_price": _req_str(payload, "b"),
            "best_bid_quantity": _req_str(payload, "B"),
            "best_ask_price": _req_str(payload, "a"),
            "best_ask_quantity": _req_str(payload, "A"),
        }
        semantic_sha = _sha256_json(
            {"kind": stream, "symbol": envelope.symbol, "update_id": update_id}
        )
        logical_sha = _sha256_json(stream_fields)
        c2_event = {
            "metadata": {
                **base_meta,
                **exchange_times,
                "stream": "BOOK_TICKER",
                "schema_version": "book-ticker.v1",
            },
            **stream_fields,
        }
    elif stream == "agg_trade":
        if _req_str(payload, "s") != envelope.symbol:
            raise ValueError("agg_trade symbol mismatch")
        agg_id = _req_int(payload, "a")
        first_tid = _req_int(payload, "f")
        last_tid = _req_int(payload, "l")
        if first_tid > last_tid:
            raise ValueError("agg_trade f exceeds l")
        event_time = _req_int(payload, "E")
        trade_time = _req_int(payload, "T")
        normal_qty = _opt_str(payload, "nq")
        exchange_times = {
            "exchange_event_time_ms": event_time,
            "exchange_transaction_time_ms": None,
            "exchange_trade_time_ms": trade_time,
        }
        stream_fields = {
            "aggregate_trade_id": agg_id,
            "first_trade_id": first_tid,
            "last_trade_id": last_tid,
            "price": _req_str(payload, "p"),
            "quantity": _req_str(payload, "q"),
            "normal_quantity": normal_qty,
            "trade_time_ms": trade_time,
            "buyer_is_maker": _req_bool(payload, "m"),
        }
        semantic_sha = _sha256_json(
            {"kind": stream, "symbol": envelope.symbol, "aggregate_trade_id": agg_id}
        )
        logical_sha = _sha256_json(stream_fields)
        c2_event = {
            "metadata": {
                **base_meta,
                **exchange_times,
                "stream": "AGG_TRADE",
                "schema_version": "agg-trade.v1",
            },
            **stream_fields,
        }
    elif stream == "mark_price":
        if _req_str(payload, "s") != envelope.symbol:
            raise ValueError("mark_price symbol mismatch")
        event_time = _req_int(payload, "E")
        next_funding = _req_int(payload, "T")
        ap = _opt_str(payload, "ap")
        # T in Binance markPriceUpdate is strictly next funding time, never tx/trade time.
        exchange_times = {
            "exchange_event_time_ms": event_time,
            "exchange_transaction_time_ms": None,
            "exchange_trade_time_ms": None,
        }
        stream_fields = {
            "mark_price": _req_str(payload, "p"),
            "index_price": _req_str(payload, "i"),
            "estimated_settle_price": _req_str(payload, "P"),
            "funding_rate": _req_str(payload, "r"),
            "next_funding_time_ms": next_funding,
            "mark_price_moving_average": ap,
        }
        semantic_sha = _sha256_json(
            {
                "kind": stream,
                "symbol": envelope.symbol,
                "event_time": event_time,
                "next_funding_time_ms": next_funding,
            }
        )
        logical_sha = _sha256_json(stream_fields)
        c2_event = {
            "metadata": {
                **base_meta,
                **exchange_times,
                "stream": "MARK_PRICE",
                "schema_version": "mark-price.v1",
            },
            **stream_fields,
        }
    elif stream == "liquidation":
        order = payload.get("o")
        if not isinstance(order, dict):
            raise ValueError("liquidation payload missing o object")
        if _req_str(order, "s") != envelope.symbol:
            raise ValueError("liquidation symbol mismatch")
        event_time = _req_int(payload, "E")
        trade_time = _req_int(order, "T")
        exchange_times = {
            "exchange_event_time_ms": event_time,
            "exchange_transaction_time_ms": None,
            "exchange_trade_time_ms": trade_time,
        }
        stream_fields = {
            "side": _req_str(order, "S"),
            "order_type": _req_str(order, "o"),
            "time_in_force": _req_str(order, "f"),
            "original_quantity": _req_str(order, "q"),
            "price": _req_str(order, "p"),
            "average_price": _opt_str(order, "ap"),
            "order_status": _req_str(order, "X"),
            "last_filled_quantity": _req_str(order, "l"),
            "accumulated_filled_quantity": _req_str(order, "z"),
            "order_trade_time_ms": trade_time,
        }
        semantic_sha = _sha256_json(
            {
                "kind": stream,
                "symbol": envelope.symbol,
                "event_time": event_time,
                "trade_time": trade_time,
                "side": stream_fields["side"],
                "price": stream_fields["price"],
                "qty": stream_fields["original_quantity"],
            }
        )
        logical_sha = _sha256_json(stream_fields)
        c2_event = {
            "metadata": {
                **base_meta,
                **exchange_times,
                "stream": "LIQUIDATION",
                "schema_version": "liquidation.v1",
            },
            **stream_fields,
        }
    else:
        raise ValueError(f"unsupported WS stream: {stream}")

    row = {
        **_common_row(
            envelope,
            symbol=envelope.symbol,
            stream=stream,
            event_kind="market_event",
            exchange_times=exchange_times,
            source_chunk_id=source_chunk_id,
            source_record_ordinal=source_record_ordinal,
            subrecord_ordinal=0,
            semantic_key_sha256=semantic_sha,
            logical_record_sha256=logical_sha,
            valid=True,
            error_code=None,
        ),
        **stream_fields,
    }
    return [
        ResearchRecord(
            stream=stream,
            symbol=envelope.symbol,
            parquet_row=row,
            c2_event=c2_event,
            semantic_key_sha256=semantic_sha,
            logical_record_sha256=logical_sha,
        )
    ]


def _decode_rest_provenance(
    envelope: EventEnvelope,
) -> tuple[str, dict[str, Any], dict[str, Any], dict[str, Any], Any, bool, str | None]:
    provenance = json.loads(envelope.raw_payload.decode("utf-8"))
    if not isinstance(provenance, dict):
        raise ValueError("REST payload must be an object")
    if provenance.get("schema_version") != "binance-public-rest-provenance.v1":
        raise ValueError("unsupported REST provenance schema_version")
    request = provenance.get("request")
    response = provenance.get("response")
    if not isinstance(request, dict) or not isinstance(response, dict):
        raise ValueError("REST provenance missing request/response objects")
    path = request.get("path")
    source_stream = (
        provenance.get("source_stream")
        or envelope.source_sequence.get("sourceStream")
        or (SOURCE_PATH_TO_KIND.get(str(path)) if path else None)
        or envelope.stream
    )
    if source_stream not in REST_SOURCES:
        raise ValueError(f"unsupported REST source_stream: {source_stream!r}")

    status_code = response.get("status")
    valid_flag = response.get("valid")
    error_type = response.get("error_type")
    if (
        "malformed" in envelope.capture_flags
        or valid_flag is False
        or not isinstance(status_code, int)
        or status_code < 200
        or status_code >= 300
    ):
        err = (
            str(error_type)
            if error_type
            else (
                f"HTTP_{status_code}"
                if isinstance(status_code, int)
                else "MALFORMED_RESPONSE"
            )
        )
        return source_stream, provenance, request, response, None, False, err

    representation = provenance.get("representation")
    if representation == "http-body-base64.v1":
        raw_b64 = response.get("raw_body_base64")
        if not isinstance(raw_b64, str):
            return (
                source_stream,
                provenance,
                request,
                response,
                None,
                False,
                "MISSING_RAW_BODY_BASE64",
            )
        try:
            body_bytes = base64.b64decode(raw_b64, validate=True)
            model = json.loads(body_bytes.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return (
                source_stream,
                provenance,
                request,
                response,
                None,
                False,
                "INVALID_JSON_BODY",
            )
    elif representation == "sdk-parsed-json.v1":
        model = response.get("model")
    else:
        raise ValueError(f"unsupported REST representation: {representation!r}")
    return source_stream, provenance, request, response, model, True, None


def _build_reference_record(
    envelope: EventEnvelope,
    *,
    stream: str,
    symbol: str,
    request_id: str,
    obs_status: str,
    error_code: str | None,
    source_time_ms: int | None,
    stream_fields: Mapping[str, Any],
    c2_payload_key: str | None,
    c2_payload: Any,
    source_chunk_id: str,
    source_record_ordinal: int,
    subrecord_ordinal: int,
    exchange_times: Mapping[str, int | None] | None = None,
    identity_extra: Mapping[str, Any] | None = None,
) -> ResearchRecord:
    source_kind = C2_SOURCE_KIND[stream]
    null_fields = {
        name: None
        for name, _dtype, _nullable in STREAM_RESEARCH_FIELDS[stream]
        if name not in {"request_id", "source_kind", "status", "source_time_ms"}
    }
    merged_stream = {
        **null_fields,
        **stream_fields,
        "request_id": request_id,
        "source_kind": source_kind,
        "status": obs_status,
        "source_time_ms": source_time_ms,
    }
    semantic_obj = {
        "kind": stream,
        "symbol": symbol,
        "status": obs_status,
        "request_id": request_id,
        **(dict(identity_extra) if identity_extra else {}),
    }
    semantic_sha = _sha256_json(semantic_obj)
    logical_sha = _sha256_json({"status": obs_status, **merged_stream})
    ex_times = exchange_times or {
        "exchange_event_time_ms": None,
        "exchange_transaction_time_ms": None,
        "exchange_trade_time_ms": None,
    }
    valid = obs_status != "ERROR"
    row = {
        **_common_row(
            envelope,
            symbol=symbol,
            stream=stream,
            event_kind="reference_observation",
            exchange_times=ex_times,
            source_chunk_id=source_chunk_id,
            source_record_ordinal=source_record_ordinal,
            subrecord_ordinal=subrecord_ordinal,
            semantic_key_sha256=semantic_sha,
            logical_record_sha256=logical_sha,
            valid=valid,
            error_code=error_code,
        ),
        **merged_stream,
    }
    c2_event: dict[str, Any] = {
        "schema_version": "reference-observation.v1",
        "venue": "BINANCE",
        "market": "USD_M_PERPETUAL",
        "symbol": symbol,
        "request_id": request_id,
        "source_kind": source_kind,
        "receive_time_utc_ns": envelope.receive_time_utc_ns,
        "source_time_ms": source_time_ms,
        "status": obs_status,
        "error_code": error_code,
    }
    if c2_payload_key is not None and c2_payload is not None:
        c2_event[c2_payload_key] = c2_payload
    return ResearchRecord(
        stream=stream,
        symbol=symbol,
        parquet_row=row,
        c2_event=c2_event,
        semantic_key_sha256=semantic_sha,
        logical_record_sha256=logical_sha,
        subrecord_ordinal=subrecord_ordinal,
    )


def _parse_rest_usdm(
    envelope: EventEnvelope,
    *,
    configured_symbols: tuple[str, ...],
    source_chunk_id: str,
    source_record_ordinal: int,
) -> list[ResearchRecord]:
    (
        stream,
        provenance,
        request,
        _response,
        model,
        ok,
        err_code,
    ) = _decode_rest_provenance(envelope)
    request_id = str(request.get("request_id") or f"rest-{envelope.receive_monotonic_ns}")
    params = request.get("parameters") if isinstance(request.get("parameters"), dict) else {}

    if stream == "exchange_info":
        target_symbols = configured_symbols
        if not ok:
            return [
                _build_reference_record(
                    envelope,
                    stream=stream,
                    symbol=sym,
                    request_id=request_id,
                    obs_status="ERROR",
                    error_code=err_code,
                    source_time_ms=None,
                    stream_fields={"symbol_present": False},
                    c2_payload_key=None,
                    c2_payload=None,
                    source_chunk_id=source_chunk_id,
                    source_record_ordinal=source_record_ordinal,
                    subrecord_ordinal=idx,
                    identity_extra={"error_code": err_code},
                )
                for idx, sym in enumerate(target_symbols)
            ]
        if not isinstance(model, dict):
            raise ValueError("exchangeInfo model must be object")
        server_time_ms = _opt_int(model, "serverTime")
        raw_rate_limits = model.get("rateLimits", [])
        if not isinstance(raw_rate_limits, list):
            raise ValueError("exchangeInfo rateLimits must be list")
        rate_limits = [_normalize_rate_limit(item) for item in raw_rate_limits]
        raw_symbols = model.get("symbols")
        if not isinstance(raw_symbols, list):
            raise ValueError("exchangeInfo symbols must be list")
        by_symbol = {
            str(entry["symbol"]): entry
            for entry in raw_symbols
            if isinstance(entry, dict) and isinstance(entry.get("symbol"), str)
        }
        records: list[ResearchRecord] = []
        for idx, sym in enumerate(target_symbols):
            entry = by_symbol.get(sym)
            if entry is None:
                records.append(
                    _build_reference_record(
                        envelope,
                        stream=stream,
                        symbol=sym,
                        request_id=request_id,
                        obs_status="ERROR",
                        error_code="SYMBOL_MISSING_IN_EXCHANGE_INFO",
                        source_time_ms=server_time_ms,
                        stream_fields={
                            "symbol_present": False,
                            "server_time_ms": server_time_ms,
                            "rate_limits_json": canonical_json(raw_rate_limits),
                        },
                        c2_payload_key=None,
                        c2_payload=None,
                        source_chunk_id=source_chunk_id,
                        source_record_ordinal=source_record_ordinal,
                        subrecord_ordinal=idx,
                    )
                )
                continue
            raw_filters = entry.get("filters")
            if not isinstance(raw_filters, list):
                raise ValueError(f"exchangeInfo filters missing for {sym}")
            filters = [_normalize_filter(item) for item in raw_filters]
            instrument = {
                "symbol_present": True,
                "contract_type": _req_str(entry, "contractType"),
                "trading_status": _req_str(entry, "status"),
                "base_asset": _req_str(entry, "baseAsset"),
                "quote_asset": _req_str(entry, "quoteAsset"),
                "margin_asset": _req_str(entry, "marginAsset"),
                "price_precision": _req_int(entry, "pricePrecision"),
                "quantity_precision": _req_int(entry, "quantityPrecision"),
                "onboard_time_ms": _opt_int(entry, "onboardDate"),
                "delivery_time_ms": _opt_int(entry, "deliveryDate"),
                "filters": filters,
                "rate_limits": rate_limits,
                "server_time_ms": server_time_ms,
            }
            stream_fields = {
                "symbol_present": True,
                "contract_type": instrument["contract_type"],
                "trading_status": instrument["trading_status"],
                "base_asset": instrument["base_asset"],
                "quote_asset": instrument["quote_asset"],
                "margin_asset": instrument["margin_asset"],
                "price_precision": instrument["price_precision"],
                "quantity_precision": instrument["quantity_precision"],
                "onboard_time_ms": instrument["onboard_time_ms"],
                "delivery_time_ms": instrument["delivery_time_ms"],
                "server_time_ms": server_time_ms,
                "filters_json": canonical_json(raw_filters),
                "rate_limits_json": canonical_json(raw_rate_limits),
            }
            records.append(
                _build_reference_record(
                    envelope,
                    stream=stream,
                    symbol=sym,
                    request_id=request_id,
                    obs_status="VALID",
                    error_code=None,
                    source_time_ms=server_time_ms,
                    stream_fields=stream_fields,
                    c2_payload_key="instrument",
                    c2_payload=instrument,
                    source_chunk_id=source_chunk_id,
                    source_record_ordinal=source_record_ordinal,
                    subrecord_ordinal=idx,
                    identity_extra={"server_time_ms": server_time_ms},
                )
            )
        return records

    scope_symbol = (
        str(params.get("symbol"))
        if envelope.symbol == "__GLOBAL__" and params.get("symbol")
        else envelope.symbol
    )
    if scope_symbol == "__GLOBAL__":
        raise ValueError(f"symbol-scoped REST stream {stream} missing symbol")

    if not ok:
        return [
            _build_reference_record(
                envelope,
                stream=stream,
                symbol=scope_symbol,
                request_id=request_id,
                obs_status="ERROR",
                error_code=err_code,
                source_time_ms=None,
                stream_fields={},
                c2_payload_key=None,
                c2_payload=None,
                source_chunk_id=source_chunk_id,
                source_record_ordinal=source_record_ordinal,
                subrecord_ordinal=0,
                identity_extra={"error_code": err_code},
            )
        ]

    if stream == "depth_snapshot":
        if not isinstance(model, dict):
            raise ValueError("depth_snapshot model must be object")
        last_update_id = _req_int(model, "lastUpdateId")
        bids = _levels(model, "bids")
        asks = _levels(model, "asks")
        event_time = _opt_int(model, "E")
        tx_time = _opt_int(model, "T")
        source_time = event_time if event_time is not None else tx_time
        depth_baseline = {
            "venue": "BINANCE",
            "market": "USD_M_PERPETUAL",
            "symbol": scope_symbol,
            "schema_version": "exchange-depth-snapshot.v1",
            "producer": "binance-market-data-recorder",
            "producer_version": envelope.collector_version,
            "request_id": request_id,
            "last_update_id": last_update_id,
            "bids": [{"price": p, "quantity": q} for p, q in bids],
            "asks": [{"price": p, "quantity": q} for p, q in asks],
            "exchange_transaction_time_ms": tx_time,
            "exchange_event_time_ms": event_time,
            "receive_time_utc_ns": envelope.receive_time_utc_ns,
            "receive_monotonic_ns": envelope.receive_monotonic_ns,
            "quality_flags": _map_quality_flags(envelope.capture_flags),
        }
        return [
            _build_reference_record(
                envelope,
                stream=stream,
                symbol=scope_symbol,
                request_id=request_id,
                obs_status="VALID",
                error_code=None,
                source_time_ms=source_time,
                stream_fields={
                    "last_update_id": last_update_id,
                    "bids_json": canonical_json(bids),
                    "asks_json": canonical_json(asks),
                },
                c2_payload_key="depth_baseline",
                c2_payload=depth_baseline,
                source_chunk_id=source_chunk_id,
                source_record_ordinal=source_record_ordinal,
                subrecord_ordinal=0,
                exchange_times={
                    "exchange_event_time_ms": event_time,
                    "exchange_transaction_time_ms": tx_time,
                    "exchange_trade_time_ms": None,
                },
                identity_extra={"last_update_id": last_update_id},
            )
        ]

    if stream == "open_interest":
        if not isinstance(model, dict):
            raise ValueError("open_interest model must be object")
        if _req_str(model, "symbol") != scope_symbol:
            raise ValueError("open_interest symbol mismatch")
        oi = _req_str(model, "openInterest")
        source_time = _opt_int(model, "time")
        return [
            _build_reference_record(
                envelope,
                stream=stream,
                symbol=scope_symbol,
                request_id=request_id,
                obs_status="VALID",
                error_code=None,
                source_time_ms=source_time,
                stream_fields={"open_interest": oi},
                c2_payload_key="open_interest",
                c2_payload={"open_interest": oi, "source_time_ms": source_time},
                source_chunk_id=source_chunk_id,
                source_record_ordinal=source_record_ordinal,
                subrecord_ordinal=0,
                identity_extra={"source_time_ms": source_time},
            )
        ]

    if not isinstance(model, list):
        raise ValueError(f"{stream} model must be JSON array")
    period = str(params.get("period") or "5m")
    if not model:
        return [
            _build_reference_record(
                envelope,
                stream=stream,
                symbol=scope_symbol,
                request_id=request_id,
                obs_status="EMPTY",
                error_code=None,
                source_time_ms=None,
                stream_fields={"period": period},
                c2_payload_key=None,
                c2_payload=None,
                source_chunk_id=source_chunk_id,
                source_record_ordinal=source_record_ordinal,
                subrecord_ordinal=0,
                identity_extra={"empty": True, "period": period},
            )
        ]

    records = []
    for idx, item in enumerate(model):
        if not isinstance(item, dict):
            raise ValueError(f"{stream} item must be object")
        if "symbol" in item and _req_str(item, "symbol") != scope_symbol:
            raise ValueError(f"{stream} item symbol mismatch")
        ts = _opt_int(item, "timestamp")
        if stream in RATIO_SOURCE_KINDS:
            ls_ratio = _req_str(item, "longShortRatio")
            l_acc = _req_str(item, "longAccount")
            s_acc = _req_str(item, "shortAccount")
            c2_key = {
                "top_long_short_account_ratio_5m": "top_long_short_account_ratio",
                "top_long_short_position_ratio_5m": "top_long_short_position_ratio",
                "global_long_short_ratio_5m": "global_long_short_account_ratio",
            }[stream]
            c2_payload = {
                "long_short_ratio": ls_ratio,
                "long_account": l_acc,
                "short_account": s_acc,
                "source_time_ms": ts,
                "period": period,
            }
            stream_fields = {
                "period": period,
                "long_short_ratio": ls_ratio,
                "long_account": l_acc,
                "short_account": s_acc,
            }
        else:
            bs_ratio = _req_str(item, "buySellRatio")
            buy_vol = _req_str(item, "buyVol")
            sell_vol = _req_str(item, "sellVol")
            c2_key = "taker_buy_sell_volume"
            c2_payload = {
                "buy_sell_ratio": bs_ratio,
                "buy_volume": buy_vol,
                "sell_volume": sell_vol,
                "source_time_ms": ts,
                "period": period,
            }
            stream_fields = {
                "period": period,
                "buy_sell_ratio": bs_ratio,
                "buy_volume": buy_vol,
                "sell_volume": sell_vol,
            }
        records.append(
            _build_reference_record(
                envelope,
                stream=stream,
                symbol=scope_symbol,
                request_id=request_id,
                obs_status="VALID",
                error_code=None,
                source_time_ms=ts,
                stream_fields=stream_fields,
                c2_payload_key=c2_key,
                c2_payload=c2_payload,
                source_chunk_id=source_chunk_id,
                source_record_ordinal=source_record_ordinal,
                subrecord_ordinal=idx,
                identity_extra={"source_time_ms": ts, "ordinal": idx},
            )
        )
    return records


def parse_usdm_envelope(
    envelope: EventEnvelope,
    *,
    configured_symbols: tuple[str, ...] = DEFAULT_PARITY_SYMBOLS,
    source_chunk_id: str = "fixture",
    source_record_ordinal: int = 0,
) -> list[ResearchRecord]:
    """Parse one Raw EventEnvelope into C2-aligned USD-M ResearchRecords."""
    if envelope.market != "um_perpetual":
        raise ValueError(f"expected um_perpetual envelope, got {envelope.market}")
    if envelope.stream == "rest_observations" or envelope.stream in REST_SOURCES:
        return _parse_rest_usdm(
            envelope,
            configured_symbols=configured_symbols,
            source_chunk_id=source_chunk_id,
            source_record_ordinal=source_record_ordinal,
        )
    return _parse_ws_usdm(
        envelope,
        source_chunk_id=source_chunk_id,
        source_record_ordinal=source_record_ordinal,
    )


def _external_roots_for(catalog: Catalog) -> dict[str, Path]:
    if not catalog.storage_targets():
        return {}
    registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
    return {
        s["storage_id"]: Path(s["resolved_path"])
        for s in registry.observe_statuses()
        if s.get("state") in {"READY", "LOW_SPACE"} and s.get("resolved_path")
    }


def _reconstruct_books(
    records_by_stream: Mapping[str, list[ResearchRecord]],
    symbols: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    books: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        reconstructor = LocalBookReconstructor("um_perpetual", sym)
        events: list[tuple[int, int, str, ResearchRecord]] = []
        for rec in records_by_stream.get("depth_snapshot", []):
            if rec.symbol == sym and rec.parquet_row["valid"]:
                events.append(
                    (
                        rec.parquet_row["receive_time_utc_ns"],
                        0,
                        "snapshot",
                        rec,
                    )
                )
        for rec in records_by_stream.get("diff_depth", []):
            if rec.symbol == sym and rec.parquet_row["valid"]:
                events.append(
                    (
                        rec.parquet_row["receive_time_utc_ns"],
                        1,
                        "depth",
                        rec,
                    )
                )
        events.sort(
            key=lambda item: (
                item[0],
                item[1],
                item[3].parquet_row["source_record_ordinal"],
            )
        )
        sync_results: list[str] = []
        for _rx, _rank, kind, rec in events:
            row = rec.parquet_row
            if kind == "snapshot":
                bids = tuple((p, q) for p, q in json.loads(row["bids_json"]))
                asks = tuple((p, q) for p, q in json.loads(row["asks_json"]))
                snap = BookSnapshot(
                    market="um_perpetual",
                    symbol=sym,
                    last_update_id=row["last_update_id"],
                    bids=bids,
                    asks=asks,
                )
                sync_results.append(reconstructor.synchronize(snap).value)
            else:
                bids = tuple((p, q) for p, q in json.loads(row["bids_json"]))
                asks = tuple((p, q) for p, q in json.loads(row["asks_json"]))
                update = DepthUpdate(
                    market="um_perpetual",
                    symbol=sym,
                    first_update_id=row["first_update_id"],
                    final_update_id=row["final_update_id"],
                    previous_final_update_id=row["previous_final_update_id"],
                    bids=bids,
                    asks=asks,
                    receive_time_utc_ns=row["receive_time_utc_ns"],
                )
                reconstructor.offer(update)
        if reconstructor.is_reliable:
            book = reconstructor.book
            canonical = book.canonical_mapping()
            books[sym] = {
                "symbol": sym,
                "synchronized": True,
                "state": reconstructor.state.value,
                "last_update_id": book.update_id,
                "book_hash": _sha256_json(canonical),
                "bids": canonical["bids"],
                "asks": canonical["asks"],
                "sync_results": sync_results,
            }
        else:
            books[sym] = {
                "symbol": sym,
                "synchronized": False,
                "state": reconstructor.state.value,
                "last_update_id": None,
                "book_hash": None,
                "bids": [],
                "asks": [],
                "sync_results": sync_results,
            }
    return books


def export_usdm_research(
    root: Path,
    output_dir: Path,
    *,
    cold_root: Path | None = None,
    profile_path: Path | None = None,
    symbols: Sequence[str] | None = None,
    sources: Sequence[str] | None = None,
    fields_by_source: Mapping[str, Sequence[str]] | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    reconstruct_books: bool = True,
) -> dict[str, Any]:
    """Export C2-aligned USD-M Parquet tables + JSONL events across hot & cold partitions."""
    if profile_path is not None:
        market, prof_symbols, prof_sources = load_profile(profile_path)
        selected_symbols, selected_sources = validate_export_request(
            market=market,
            symbols=symbols if symbols is not None else prof_symbols,
            sources=sources if sources is not None else prof_sources,
            fields_by_source=fields_by_source,
        )
    else:
        selected_symbols, selected_sources = validate_export_request(
            market="um_perpetual",
            symbols=symbols,
            sources=sources,
            fields_by_source=fields_by_source,
        )

    allowed_symbols = set(selected_symbols)
    allowed_sources = set(selected_sources)
    collected: dict[str, list[ResearchRecord]] = {}
    seen_logical: dict[tuple[str, str, str], ResearchRecord] = {}
    resolved_roots: list[Path] = []

    with reader_lock(root):
        resolved_roots = resolve_roots(
            root, cold_root=cold_root, start_ns=start_ns, end_ns=end_ns
        )
        for physical_root in resolved_roots:
            layout = StorageLayout.from_root(physical_root)
            with Catalog(layout.catalog, read_only=True) as catalog:
                external = _external_roots_for(catalog)
                chunks = load_source_chunks(
                    layout=layout, catalog=catalog, external_roots=external
                )
            for chunk in sorted(
                chunks, key=lambda c: (c.manifest["created_at_utc_ns"], c.chunk_id)
            ):
                for record in iter_source_records(chunk):
                    envelope = record.envelope
                    if envelope.market != "um_perpetual":
                        continue
                    rx_ns = envelope.receive_time_utc_ns
                    if start_ns is not None and rx_ns < start_ns:
                        continue
                    if end_ns is not None and rx_ns >= end_ns:
                        continue
                    if (
                        envelope.stream not in allowed_sources
                        and envelope.stream != "rest_observations"
                    ):
                        continue
                    parsed = parse_usdm_envelope(
                        envelope,
                        configured_symbols=selected_symbols,
                        source_chunk_id=chunk.chunk_id,
                        source_record_ordinal=record.ordinal,
                    )
                    for item in parsed:
                        if item.stream not in allowed_sources:
                            continue
                        if item.symbol not in allowed_symbols:
                            continue
                        dedup_key = (
                            item.stream,
                            item.symbol,
                            item.semantic_key_sha256,
                        )
                        existing = seen_logical.get(dedup_key)
                        if existing is None or (
                            item.parquet_row["receive_time_utc_ns"],
                            item.parquet_row["source_chunk_id"],
                        ) < (
                            existing.parquet_row["receive_time_utc_ns"],
                            existing.parquet_row["source_chunk_id"],
                        ):
                            seen_logical[dedup_key] = item

    for rec in seen_logical.values():
        collected.setdefault(rec.stream, []).append(rec)
    for stream_rows in collected.values():
        stream_rows.sort(
            key=lambda item: (
                item.parquet_row["receive_time_utc_ns"],
                item.symbol,
                item.parquet_row["source_chunk_id"],
                item.parquet_row["source_record_ordinal"],
                item.subrecord_ordinal,
            )
        )

    parquet_dir = output_dir / "parquet"
    jsonl_dir = output_dir / "c2_events"
    parquet_dir.mkdir(parents=True, exist_ok=True)
    jsonl_dir.mkdir(parents=True, exist_ok=True)

    stream_counts: dict[str, int] = {}
    parquet_paths: dict[str, str] = {}
    jsonl_paths: dict[str, str] = {}
    for stream in sorted(collected):
        rows = collected[stream]
        stream_counts[stream] = len(rows)
        schema = _schema_for(stream)
        table = pa.Table.from_pylist(
            [item.parquet_row for item in rows], schema=schema
        )
        pq_path = parquet_dir / f"{stream}.parquet"
        pq.write_table(table, pq_path, compression="zstd")
        parquet_paths[stream] = str(pq_path)

        jl_path = jsonl_dir / f"{stream}.jsonl"
        jl_path.write_text(
            "\n".join(canonical_json(item.c2_event) for item in rows) + "\n",
            encoding="utf-8",
        )
        jsonl_paths[stream] = str(jl_path)

    books: dict[str, dict[str, Any]] = {}
    if reconstruct_books:
        books = _reconstruct_books(collected, selected_symbols)
        books_path = output_dir / "reconstructed_books.json"
        books_path.write_text(
            json.dumps(books, indent=2, sort_keys=True), encoding="utf-8"
        )

    manifest = {
        "dataset_version": RESEARCH_DATASET_VERSION,
        "market": "um_perpetual",
        "symbols": list(selected_symbols),
        "sources": list(selected_sources),
        "resolved_roots": [str(path) for path in resolved_roots],
        "stream_counts": stream_counts,
        "parquet_files": parquet_paths,
        "c2_jsonl_files": jsonl_paths,
        "books": books,
    }
    (output_dir / "export_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return manifest


def build_usdm_parity_fixtures() -> dict[str, Any]:
    """Return canonical Binance USD-M raw payloads for R4 vs Gateway G7 same-input tests."""
    return {
        "symbols": ["BTCUSDT", "ETHUSDT"],
        "ws_payloads": [
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1762992000100,
                    "s": "BTCUSDT",
                    "a": 5001,
                    "p": "90123.40",
                    "q": "0.250",
                    "nq": "0.125",
                    "f": 1001,
                    "l": 1002,
                    "T": 1762992000095,
                    "m": False,
                },
            },
            {
                "stream": "btcusdt@aggTrade",
                "data": {
                    "e": "aggTrade",
                    "E": 1762992000200,
                    "s": "BTCUSDT",
                    "a": 5002,
                    "p": "90124.00",
                    "q": "0.100",
                    "nq": "0",
                    "f": 1003,
                    "l": 1003,
                    "T": 1762992000195,
                    "m": True,
                },
            },
            {
                "stream": "btcusdt@markPrice@1s",
                "data": {
                    "e": "markPriceUpdate",
                    "E": 1762992001000,
                    "s": "BTCUSDT",
                    "p": "90120.50",
                    "i": "90118.25",
                    "P": "90119.00",
                    "r": "0.00010000",
                    "T": 1763020800000,
                    "ap": "90115.10",
                },
            },
            {
                "stream": "btcusdt@bookTicker",
                "data": {
                    "e": "bookTicker",
                    "E": 1762992001100,
                    "T": 1762992001099,
                    "s": "BTCUSDT",
                    "u": 9001,
                    "b": "90120.00",
                    "B": "1.500",
                    "a": "90121.00",
                    "A": "2.000",
                },
            },
            {
                "stream": "btcusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1762992001200,
                    "T": 1762992001195,
                    "s": "BTCUSDT",
                    "U": 99,
                    "u": 101,
                    "pu": 98,
                    "b": [["90120.00", "2.000"]],
                    "a": [["90121.00", "2.500"]],
                },
            },
            {
                "stream": "btcusdt@depth@100ms",
                "data": {
                    "e": "depthUpdate",
                    "E": 1762992001300,
                    "T": 1762992001295,
                    "s": "BTCUSDT",
                    "U": 102,
                    "u": 103,
                    "pu": 101,
                    "b": [["90119.50", "1.000"], ["90120.00", "0"]],
                    "a": [["90122.00", "3.000"]],
                },
            },
        ],
        "depth_snapshot": {
            "lastUpdateId": 100,
            "E": 1762992001150,
            "T": 1762992001148,
            "bids": [["90120.00", "1.200"], ["90119.00", "4.000"]],
            "asks": [["90121.00", "1.800"]],
        },
        "exchange_info": {
            "serverTime": 1762992000000,
            "rateLimits": [
                {
                    "rateLimitType": "REQUEST_WEIGHT",
                    "interval": "MINUTE",
                    "intervalNum": 1,
                    "limit": 2400,
                }
            ],
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                    "filters": [
                        {
                            "filterType": "PRICE_FILTER",
                            "minPrice": "556.80",
                            "maxPrice": "4529764",
                            "tickSize": "0.10",
                        },
                        {
                            "filterType": "LOT_SIZE",
                            "minQty": "0.001",
                            "maxQty": "1000",
                            "stepSize": "0.001",
                        },
                    ],
                },
                {
                    "symbol": "ETHUSDT",
                    "contractType": "PERPETUAL",
                    "status": "TRADING",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                    "marginAsset": "USDT",
                    "pricePrecision": 2,
                    "quantityPrecision": 3,
                    "onboardDate": 1569398400000,
                    "deliveryDate": 4133404800000,
                    "filters": [],
                },
            ],
        },
        "rest_observations": {
            "open_interest": {
                "symbol": "BTCUSDT",
                "openInterest": "85432.123",
                "time": 1762992000000,
            },
            "top_long_short_account_ratio_5m": [
                {
                    "symbol": "BTCUSDT",
                    "longShortRatio": "1.2345",
                    "longAccount": "0.5525",
                    "shortAccount": "0.4475",
                    "timestamp": 1762991700000,
                }
            ],
            "top_long_short_position_ratio_5m": [],
            "global_long_short_ratio_5m": [
                {
                    "symbol": "BTCUSDT",
                    "longShortRatio": "1.1111",
                    "longAccount": "0.5263",
                    "shortAccount": "0.4737",
                    "timestamp": 1762991700000,
                }
            ],
            "taker_buy_sell_volume_5m": [
                {
                    "buySellRatio": "0.9500",
                    "buyVol": "123.450",
                    "sellVol": "129.950",
                    "timestamp": 1762991700000,
                }
            ],
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cold-root", type=Path)
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--symbol", action="append", dest="symbols")
    parser.add_argument("--source", action="append", dest="sources")
    parser.add_argument("--start-ns", type=int)
    parser.add_argument("--end-ns", type=int)
    args = parser.parse_args(argv)
    report = export_usdm_research(
        args.root,
        args.output,
        cold_root=args.cold_root,
        profile_path=args.profile,
        symbols=args.symbols,
        sources=args.sources,
        start_ns=args.start_ns,
        end_ns=args.end_ns,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
