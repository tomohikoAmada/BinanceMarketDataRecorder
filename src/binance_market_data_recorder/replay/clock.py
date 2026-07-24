"""Versioned event-clock resolution for normalized stream rows."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from .model import (
    MissingExchangeTimeError,
    MissingExchangeTimePolicy,
    ReplayClock,
)

_EXCHANGE_FIELDS: dict[str, tuple[str, ...]] = {
    "diff_depth": ("exchange_transaction_time_ms", "exchange_event_time_ms"),
    "agg_trade": ("exchange_trade_time_ms", "exchange_event_time_ms"),
    "book_ticker": ("exchange_transaction_time_ms", "exchange_event_time_ms"),
    "depth_snapshot": (),
    "mark_price": ("exchange_event_time_ms",),
    "liquidation": ("exchange_event_time_ms", "order_trade_time_ms"),
    "premium_index_snapshot": ("observation_time_ms",),
    "funding_history": ("funding_time_ms",),
    "funding_info": (),
    "open_interest": ("observation_time_ms",),
    "exchange_info": ("server_time_ms",),
}


@dataclass(frozen=True, slots=True)
class ClockReading:
    event_time_ns: int
    used_receive_time_fallback: bool


def _non_negative_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise MissingExchangeTimeError(f"{name} is not a non-negative integer")
    return value


class EventClock:
    """Resolve the selected public event clock without silent fallback."""

    def __init__(
        self,
        clock: ReplayClock,
        missing_exchange_time: MissingExchangeTimePolicy = (
            MissingExchangeTimePolicy.ERROR
        ),
    ) -> None:
        if not isinstance(clock, ReplayClock):
            raise TypeError("clock must be ReplayClock")
        if not isinstance(missing_exchange_time, MissingExchangeTimePolicy):
            raise TypeError(
                "missing_exchange_time must be MissingExchangeTimePolicy"
            )
        self.clock = clock
        self.missing_exchange_time = missing_exchange_time

    def resolve(self, row: Mapping[str, object]) -> ClockReading | None:
        receive_time = _non_negative_int(
            row.get("receive_time_utc_ns"), "receive_time_utc_ns"
        )
        if self.clock is ReplayClock.RECEIVE_TIME:
            return ClockReading(receive_time, False)
        stream = row.get("stream")
        if not isinstance(stream, str) or stream not in _EXCHANGE_FIELDS:
            raise MissingExchangeTimeError("unsupported stream event clock")
        for field in _EXCHANGE_FIELDS[stream]:
            value = row.get(field)
            if value is not None:
                milliseconds = _non_negative_int(value, field)
                return ClockReading(milliseconds * 1_000_000, False)
        if self.missing_exchange_time is MissingExchangeTimePolicy.EXCLUDE:
            return None
        if (
            self.missing_exchange_time
            is MissingExchangeTimePolicy.FALLBACK_RECEIVE
        ):
            return ClockReading(receive_time, True)
        raise MissingExchangeTimeError(
            f"{row.get('market')}/{stream} has no documented exchange time"
        )
