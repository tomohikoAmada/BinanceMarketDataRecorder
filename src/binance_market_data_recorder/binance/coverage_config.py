"""Explicit scope and admission limits for selected-asset public capture."""

from __future__ import annotations

import math
import re

from pydantic import BaseModel, ConfigDict, Field, field_validator

SPOT_INTERVALS = (
    "1s",
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
)
FUTURES_INTERVALS = SPOT_INTERVALS[1:]


class CoverageConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    underlying_assets: tuple[str, ...]
    spot_quote_assets: tuple[str, ...] = ()  # Empty means every base-asset quote pair.
    max_products: int = Field(default=256, ge=1, le=4096)
    max_streams: int = Field(default=32768, ge=1, le=262144)
    streams_per_connection: int = Field(default=128, ge=1, le=1024)
    rest_concurrency: int = Field(default=4, ge=1, le=16)
    directory_interval_seconds: float = Field(default=3600.0, ge=60)
    # Full native interval sets are the default. Explicit subsets disclose a reduction.
    spot_intervals: tuple[str, ...] = SPOT_INTERVALS
    futures_intervals: tuple[str, ...] = FUTURES_INTERVALS
    # False explicitly selects the owner-approved ADR-0055 initial-release omission.
    # Keep the complete target for existing configurations unless they opt in.
    capture_dated_premium_klines: bool = True
    recent_trades_compact: bool = True
    recent_trades_interval_seconds: float = Field(default=1.0, ge=1)
    recent_trades_intervals: dict[str, float] = Field(default_factory=dict)
    max_steady_derivative_weight_per_minute: float | None = Field(default=None, gt=0, le=1200)

    @field_validator("recent_trades_intervals")
    @classmethod
    def _trade_intervals(cls, value: dict[str, float]) -> dict[str, float]:
        for key, seconds in value.items():
            if not re.fullmatch(r"(?:um|cm)_(?:perpetual|delivery):[A-Z0-9_]{1,64}", key):
                raise ValueError("recent trade cadence requires a derivative market:symbol")
            if not math.isfinite(seconds) or not 1 <= seconds <= 3600:
                raise ValueError("recent trade cadence must be finite, between 1 and 3600 seconds")
        return value

    @field_validator("underlying_assets", "spot_quote_assets", mode="before")
    @classmethod
    def _assets(cls, value: object) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)):
            raise ValueError("assets must be a list")
        if any(not isinstance(x, str) or not x.isascii() or not x.isalnum() for x in value):
            raise ValueError("asset names must be ASCII letters and digits")
        result = tuple(str(x).upper() for x in value)
        if len(set(result)) != len(result):
            raise ValueError("duplicate assets")
        return result

    @field_validator("underlying_assets")
    @classmethod
    def _nonempty_assets(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("at least one underlying asset is required")
        return value

    @field_validator("spot_intervals", "futures_intervals", mode="before")
    @classmethod
    def _intervals(cls, value: object) -> tuple[str, ...]:
        if not isinstance(value, (list, tuple)) or not value:
            raise ValueError("native kline intervals must be a nonempty list")
        if any(x not in SPOT_INTERVALS for x in value) or len(set(value)) != len(value):
            raise ValueError("unknown or duplicate native interval")
        return tuple(value)

    @field_validator("futures_intervals")
    @classmethod
    def _futures_intervals(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(x not in FUTURES_INTERVALS for x in value):
            raise ValueError("futures do not publish the Spot 1s interval")
        return value
