"""Bounded selected-product daily trade supplement; never a live receive clock."""

from __future__ import annotations

import csv
import io
import re
import zipfile
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from ..domain.product import ProductKey
from .planner import BASE_URL, BackfillPlan, PlanEntry

MARKETS = frozenset({"um_perpetual", "um_delivery", "cm_perpetual", "cm_delivery"})
DAILY_SCHEMA = pa.schema(
    [
        pa.field("archive_event_time_utc_ns", pa.int64(), nullable=False),
        pa.field("source_row_ordinal", pa.int64(), nullable=False),
        pa.field("trade_id", pa.int64(), nullable=False),
        pa.field("price", pa.string(), nullable=False),
        pa.field("qty", pa.string(), nullable=False),
        pa.field("quote_qty", pa.string()),
        pa.field("base_qty", pa.string()),
        pa.field("is_buyer_maker", pa.bool_(), nullable=False),
        pa.field("is_best_match", pa.bool_()),
    ]
)


def build_daily_trade_plan(products: Sequence[ProductKey], start: date, end: date) -> BackfillPlan:
    """Explicit products, daily only, at most 31 UTC dates and 256 identities."""
    if not products or len(products) > 256 or len(set(products)) != len(products):
        raise ValueError("daily trades require 1..256 distinct explicit products")
    if not 0 <= (end - start).days < 31 or end >= datetime.now(UTC).date():
        raise ValueError("daily trades require 1..31 completed UTC dates")
    entries = []
    for p in sorted(products):
        if p.market not in MARKETS or not re.fullmatch(r"[A-Z0-9_]{1,64}", p.symbol):
            raise ValueError("invalid physical market or symbol")
        dated = re.search(r"_\d{6}$", p.symbol) is not None
        if p.market.endswith("delivery") != dated or (
            p.market == "cm_perpetual" and not p.symbol.endswith("_PERP")
        ):
            raise ValueError("declared contract kind does not match the native symbol")
        route = (
            "spot"
            if p.market == "spot"
            else "futures/" + ("cm" if p.market.startswith("cm_") else "um")
        )
        for offset in range((end - start).days + 1):
            day = start + timedelta(days=offset)
            period = day.isoformat()
            url = f"{BASE_URL}/{route}/daily/trades/{p.symbol}/{p.symbol}-trades-{period}.zip"
            entries.append(
                PlanEntry(
                    p.market,
                    p.symbol,
                    "trades",
                    None,
                    "daily",
                    period,
                    period,
                    period,
                    "microseconds"
                    if p.market == "spot" and day >= date(2025, 1, 1)
                    else "milliseconds",
                    url,
                    url + ".CHECKSUM",
                    1_000_000_000,
                )
            )
    return BackfillPlan("selected-daily-trades", start.isoformat(), end.isoformat(), tuple(entries))


def _boolean(value: str) -> bool:
    if value.lower() not in {"true", "false"}:
        raise ValueError("invalid daily trade boolean")
    return value.lower() == "true"


def daily_trade_rows(entry: PlanEntry, zip_path: Path) -> Iterator[dict[str, Any]]:
    """Strict supplied-field validation; exact decimal text and native IDs retained."""
    coin = entry.market.startswith("cm_")
    spot = entry.market == "spot"
    expected = ["id", "price", "qty", "base_qty" if coin else "quote_qty", "time", "is_buyer_maker"]
    if spot:
        expected.append("is_best_match")
    first_ns = int(datetime.fromisoformat(entry.start_date).replace(tzinfo=UTC).timestamp()) * 10**9
    last_ns = first_ns + 86400 * 10**9
    multiplier = 1000 if entry.timestamp_unit == "microseconds" else 1_000_000
    previous_id = -1
    with zipfile.ZipFile(zip_path) as archive:
        expected_name = entry.zip_url.rsplit("/", 1)[1].removesuffix(".zip") + ".csv"
        if archive.namelist() != [expected_name]:
            raise ValueError("daily trade ZIP member does not match product/date")
        with archive.open(expected_name) as raw:
            for ordinal, values in enumerate(
                csv.reader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            ):
                if ordinal == 0 and values == expected:
                    continue
                if len(values) != len(expected):
                    raise ValueError(
                        "unknown daily trade fields; source ZIP retained for inspection"
                    )
                trade_id = int(values[0])
                event_ns = int(values[4]) * multiplier
                if trade_id <= previous_id or not first_ns <= event_ns < last_ns:
                    raise ValueError("daily trade identity/order/date mismatch")
                previous_id = trade_id
                try:
                    for text in values[1:4]:
                        value = Decimal(text)
                        if not value.is_finite() or value < 0:
                            raise ValueError("invalid daily trade decimal")
                except InvalidOperation as exc:
                    raise ValueError("invalid daily trade decimal") from exc
                yield {
                    "archive_event_time_utc_ns": event_ns,
                    "source_row_ordinal": ordinal,
                    "trade_id": trade_id,
                    "price": values[1],
                    "qty": values[2],
                    "quote_qty": None if coin else values[3],
                    "base_qty": values[3] if coin else None,
                    "is_buyer_maker": _boolean(values[5]),
                    "is_best_match": _boolean(values[6]) if spot else None,
                }


def iter_daily_trade_facts(path: Path) -> Iterator[dict[str, Any]]:
    """Published bounded reader: file acquisition time is provenance, never receive time."""
    parquet = pq.ParquetFile(path)
    metadata = parquet.schema_arrow.metadata or {}
    if metadata.get(b"bmdr.schema_version") != b"official-daily-trades.v1":
        raise ValueError("unsupported daily trade schema")
    shared = {
        key: metadata[key.encode()].decode()
        for key in (
            "market",
            "symbol",
            "source_revision",
            "source_zip_sha256",
            "downloaded_at_utc_ns",
        )
    }
    for batch in parquet.iter_batches(batch_size=50_000):
        for row in batch.to_pylist():
            yield {
                **row,
                **shared,
                "clock_semantics": "archive_source",
                "is_rpi_trade_available": False,
                "live_receive_time_utc_ns": None,
                "trade_time_utc_ns": row["archive_event_time_utc_ns"],
            }


def reconcile_trade_facts(live: Mapping[str, Any], daily: Mapping[str, Any]) -> dict[str, Any]:
    """One matched trade in a consumer fact view; preserve conflicting source records.

    Live inputs use the reader's canonical names, with any REST-only fields left
    intact. Original live observations and their timings are never modified.
    """
    identity = ("market", "symbol", "trade_id")
    if any(live[k] != daily[k] for k in identity):
        raise ValueError("reconciliation requires the same product/native trade ID")
    conflicts = []
    for key in (
        "price",
        "qty",
        "quote_qty",
        "base_qty",
        "is_buyer_maker",
        "is_best_match",
        "trade_time_utc_ns",
    ):
        left, right = live.get(key), daily.get(key)
        if left is None or right is None:
            continue
        equal = (
            Decimal(left) == Decimal(right)
            if key in {"price", "qty", "quote_qty", "base_qty"}
            else left == right
        )
        if not equal:
            conflicts.append(key)
    return {
        "status": "CONFLICT" if conflicts else "MATCHED_COMMON_FACTS",
        "conflicting_fields": conflicts,
        "fact_count": 1,
        "preferred_live_fact": dict(live) if not conflicts else None,
        "daily_provenance": {
            k: daily[k] for k in ("source_revision", "source_zip_sha256", "downloaded_at_utc_ns")
        },
        "conflicting_sources": [dict(live), dict(daily)] if conflicts else [],
        "live_gap_repaired": False,
    }
