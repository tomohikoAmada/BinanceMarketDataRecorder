"""Versioned Arrow schemas and parsed normalized event model."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa  # type: ignore[import-untyped]

DATASET_VERSION = "normalized-dataset.v1"
DEDUP_VERSION = "normalized-dedup.v1"
PARQUET_PROFILE = "bmdr-parquet.v1"

SUPPORTED_STREAMS: dict[str, frozenset[str]] = {
    "spot": frozenset(
        {"diff_depth", "agg_trade", "book_ticker", "depth_snapshot"}
    ),
    "um_perpetual": frozenset(
        {
            "diff_depth",
            "agg_trade",
            "book_ticker",
            "depth_snapshot",
            "mark_price",
            "liquidation",
            "premium_index_snapshot",
            "funding_history",
            "funding_info",
            "open_interest",
            "exchange_info",
        }
    ),
}


@dataclass(frozen=True, slots=True)
class ParsedEvent:
    fields: Mapping[str, object]
    semantic_identity: Mapping[str, object]
    logical_content: Mapping[str, object]
    subrecord_ordinal: int = 0
    valid: bool = True
    error_code: str | None = None
    event_kind: str = "market_event"


COMMON_FIELDS: tuple[tuple[str, pa.DataType, bool], ...] = (
    ("dataset_version", pa.string(), False),
    ("schema_version", pa.string(), False),
    ("dedup_version", pa.string(), False),
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
    ("module", pa.string(), False),
    ("connection_id", pa.string(), False),
    ("collector_instance_id", pa.string(), False),
    ("collector_version", pa.string(), False),
    ("source_sequence_json", pa.string(), False),
    ("capture_flags_json", pa.string(), False),
    ("source_chunk_id", pa.string(), False),
    ("source_chunk_sha256", pa.string(), False),
    ("source_record_ordinal", pa.int64(), False),
    ("source_subrecord_ordinal", pa.int32(), False),
    ("raw_payload_sha256", pa.string(), False),
    ("semantic_key_sha256", pa.string(), False),
    ("logical_record_sha256", pa.string(), False),
    ("duplicate_count", pa.int32(), False),
    ("duplicate_sources_json", pa.string(), False),
    ("identity_conflict", pa.bool_(), False),
    ("valid", pa.bool_(), False),
    ("error_code", pa.string(), True),
    ("source_complete", pa.bool_(), False),
    ("source_gap", pa.bool_(), False),
    ("source_resync", pa.bool_(), False),
    ("source_recovered", pa.bool_(), False),
    ("source_capture_flags_json", pa.string(), False),
)

STREAM_FIELDS: dict[str, tuple[tuple[str, pa.DataType, bool], ...]] = {
    "diff_depth": (
        ("first_update_id", pa.int64(), True),
        ("final_update_id", pa.int64(), True),
        ("previous_final_update_id", pa.int64(), True),
        ("bids_json", pa.string(), True),
        ("asks_json", pa.string(), True),
    ),
    "agg_trade": (
        ("aggregate_trade_id", pa.int64(), True),
        ("first_trade_id", pa.int64(), True),
        ("last_trade_id", pa.int64(), True),
        ("price", pa.string(), True),
        ("quantity", pa.string(), True),
        ("buyer_is_maker", pa.bool_(), True),
        ("best_match", pa.bool_(), True),
    ),
    "book_ticker": (
        ("update_id", pa.int64(), True),
        ("bid_price", pa.string(), True),
        ("bid_quantity", pa.string(), True),
        ("ask_price", pa.string(), True),
        ("ask_quantity", pa.string(), True),
    ),
    "depth_snapshot": (
        ("last_update_id", pa.int64(), True),
        ("bids_json", pa.string(), True),
        ("asks_json", pa.string(), True),
        ("response_status", pa.int32(), True),
        ("request_time_utc_ns", pa.int64(), True),
        ("response_model_sha256", pa.string(), True),
    ),
    "mark_price": (
        ("mark_price", pa.string(), True),
        ("index_price", pa.string(), True),
        ("estimated_settle_price", pa.string(), True),
        ("funding_rate", pa.string(), True),
        ("next_funding_time_ms", pa.int64(), True),
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
    "premium_index_snapshot": (
        ("mark_price", pa.string(), True),
        ("index_price", pa.string(), True),
        ("estimated_settle_price", pa.string(), True),
        ("last_funding_rate", pa.string(), True),
        ("interest_rate", pa.string(), True),
        ("next_funding_time_ms", pa.int64(), True),
        ("observation_time_ms", pa.int64(), True),
    ),
    "funding_history": (
        ("observation_empty", pa.bool_(), False),
        ("funding_time_ms", pa.int64(), True),
        ("funding_rate", pa.string(), True),
        ("mark_price", pa.string(), True),
    ),
    "funding_info": (
        ("symbol_present", pa.bool_(), False),
        ("observation_record_count", pa.int32(), False),
        ("adjusted_funding_rate_cap", pa.string(), True),
        ("adjusted_funding_rate_floor", pa.string(), True),
        ("funding_interval_hours", pa.int32(), True),
        ("disclaimer", pa.bool_(), True),
    ),
    "open_interest": (
        ("open_interest", pa.string(), True),
        ("observation_time_ms", pa.int64(), True),
    ),
    "exchange_info": (
        ("symbol_present", pa.bool_(), False),
        ("server_time_ms", pa.int64(), True),
        ("contract_type", pa.string(), True),
        ("trading_status", pa.string(), True),
        ("filters_json", pa.string(), True),
        ("rate_limits_json", pa.string(), True),
    ),
}


def schema_version(market: str, stream: str) -> str:
    market_name = "usdm" if market == "um_perpetual" else market
    return f"normalized-binance-{market_name}-{stream.replace('_', '-')}.v1"


def schema_for(market: str, stream: str) -> pa.Schema:
    if stream not in SUPPORTED_STREAMS.get(market, frozenset()):
        raise ValueError(f"unsupported normalized stream {market}/{stream}")
    fields = [
        pa.field(name, data_type, nullable=nullable)
        for name, data_type, nullable in (*COMMON_FIELDS, *STREAM_FIELDS[stream])
    ]
    metadata = {
        b"bmdr.dataset_version": DATASET_VERSION.encode(),
        b"bmdr.schema_version": schema_version(market, stream).encode(),
        b"bmdr.dedup_version": DEDUP_VERSION.encode(),
        b"bmdr.parquet_profile": PARQUET_PROFILE.encode(),
    }
    return pa.schema(fields, metadata=metadata)
