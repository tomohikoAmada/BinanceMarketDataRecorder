"""Deterministic data.binance.vision plan construction."""

from __future__ import annotations

import calendar
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from enum import StrEnum

BASE_URL = "https://data.binance.vision/data"


class BackfillProfile(StrEnum):
    BASELINE_BARS = "baseline-bars"
    MICROSTRUCTURE_TRADES = "microstructure-trades"


@dataclass(frozen=True, slots=True)
class Dataset:
    market: str
    data_type: str
    interval: str | None
    estimated_daily_bytes: int


BASELINE_DATASETS = (
    Dataset("spot", "klines", "1m", 3_000_000),
    Dataset("futures/um", "klines", "1m", 3_000_000),
    Dataset("futures/um", "markPriceKlines", "1m", 2_000_000),
    Dataset("futures/um", "indexPriceKlines", "1m", 2_000_000),
    Dataset("futures/um", "premiumIndexKlines", "1m", 2_000_000),
    Dataset("futures/um", "fundingRate", None, 100_000),
)
MICROSTRUCTURE_DATASETS = (
    Dataset("spot", "trades", None, 1_000_000_000),
    Dataset("spot", "aggTrades", None, 500_000_000),
    Dataset("futures/um", "trades", None, 1_000_000_000),
    Dataset("futures/um", "aggTrades", None, 500_000_000),
)


@dataclass(frozen=True, slots=True)
class PlanEntry:
    market: str
    symbol: str
    data_type: str
    interval: str | None
    granularity: str
    period: str
    start_date: str
    end_date: str
    timestamp_unit: str
    zip_url: str
    checksum_url: str
    estimated_bytes: int

    def public_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BackfillPlan:
    profile: str
    start_date: str
    end_date: str
    entries: tuple[PlanEntry, ...]

    def public_dict(self) -> dict[str, object]:
        return {
            "profile": self.profile,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "estimated_bytes": sum(item.estimated_bytes for item in self.entries),
            "file_count": len(self.entries),
            "concurrency_limit": 1,
            "entries": [item.public_dict() for item in self.entries],
        }


def _month_end(day: date) -> date:
    return date(day.year, day.month, calendar.monthrange(day.year, day.month)[1])


def _next_month(day: date) -> date:
    return date(day.year + (day.month == 12), day.month % 12 + 1, 1)


def _filename(dataset: Dataset, period: str) -> str:
    interval = f"-{dataset.interval}" if dataset.interval else ""
    return f"BTCUSDT{interval}-{period}.zip"


def _entry(
    dataset: Dataset, granularity: str, first: date, last: date
) -> PlanEntry:
    period = first.strftime("%Y-%m" if granularity == "monthly" else "%Y-%m-%d")
    interval_path = f"/{dataset.interval}" if dataset.interval else ""
    filename = _filename(dataset, period)
    zip_url = (
        f"{BASE_URL}/{dataset.market}/{granularity}/{dataset.data_type}/"
        f"BTCUSDT{interval_path}/{filename}"
    )
    days = (last - first).days + 1
    timestamp_unit = (
        "microseconds"
        if dataset.market == "spot" and first >= date(2025, 1, 1)
        else "milliseconds"
    )
    return PlanEntry(
        market=dataset.market,
        symbol="BTCUSDT",
        data_type=dataset.data_type,
        interval=dataset.interval,
        granularity=granularity,
        period=period,
        start_date=first.isoformat(),
        end_date=last.isoformat(),
        timestamp_unit=timestamp_unit,
        zip_url=zip_url,
        checksum_url=f"{zip_url}.CHECKSUM",
        estimated_bytes=dataset.estimated_daily_bytes * days,
    )


def build_plan(profile: str, start: date, end: date) -> BackfillPlan:
    if end < start:
        raise ValueError("backfill end date must not precede start date")
    try:
        selected = BackfillProfile(profile)
    except ValueError as exc:
        raise ValueError(f"unsupported backfill profile: {profile}") from exc
    datasets = (
        BASELINE_DATASETS
        if selected is BackfillProfile.BASELINE_BARS
        else MICROSTRUCTURE_DATASETS
    )
    entries: list[PlanEntry] = []
    for dataset in datasets:
        cursor = start
        while cursor <= end:
            month_last = _month_end(cursor)
            if cursor.day == 1 and month_last <= end:
                entries.append(_entry(dataset, "monthly", cursor, month_last))
                cursor = _next_month(cursor)
            else:
                entries.append(_entry(dataset, "daily", cursor, cursor))
                cursor += timedelta(days=1)
    return BackfillPlan(
        profile=selected.value,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        entries=tuple(entries),
    )
