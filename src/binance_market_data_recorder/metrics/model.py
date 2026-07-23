"""Versioned aggregate model; no market-event payloads enter SQLite."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from ..domain.event import EventEnvelope

AGGREGATE_SCHEMA_VERSION = "operational-metric-aggregate.v1"

INPUT_COUNTERS = (
    "websocket_messages",
    "websocket_payload_bytes",
    "rest_responses",
    "depth_bid_level_updates",
    "depth_ask_level_updates",
    "agg_trade_records",
    "book_ticker_records",
)
QUALITY_COUNTERS = (
    "accepted",
    "duplicate",
    "malformed",
    "out_of_order",
    "sequence_gap",
    "orderbook_resync",
    "planned_reconnect",
    "unexpected_disconnect",
    "server_shutdown",
    "checksum_failure",
)
OUTPUT_COUNTERS = (
    "raw_records_written",
    "raw_bytes_written",
    "sealed_chunks",
    "compressed_bytes",
    "archived_files",
    "archived_bytes",
    "deleted_local_bytes",
    "archive_backlog_bytes",
)
COUNTER_NAMES = INPUT_COUNTERS + QUALITY_COUNTERS + OUTPUT_COUNTERS

HISTOGRAM_NAMES = ("receive_lag_ns", "write_latency_ns", "fsync_latency_ns")
GAUGE_NAMES = (
    "queue_depth",
    "cpu_percent",
    "rss_memory_bytes",
    "internal_free_bytes",
)

# Deterministic logarithmic-ish nanosecond bounds: 1 us through 60 s.
HISTOGRAM_BOUNDS_NS = (
    1_000,
    10_000,
    100_000,
    1_000_000,
    5_000_000,
    10_000_000,
    50_000_000,
    100_000_000,
    500_000_000,
    1_000_000_000,
    5_000_000_000,
    10_000_000_000,
    60_000_000_000,
)


def histogram_bucket(value: int) -> str:
    if value < 0:
        raise ValueError("histogram value must be non-negative")
    for bound in HISTOGRAM_BOUNDS_NS:
        if value <= bound:
            return str(bound)
    return "overflow"


def _decoded_object(envelope: EventEnvelope) -> dict[str, Any] | None:
    try:
        value = json.loads(envelope.raw_payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


@dataclass
class MetricAggregate:
    counters: dict[str, int] = field(default_factory=dict)
    histograms: dict[str, dict[str, int]] = field(default_factory=dict)
    gauges: dict[str, dict[str, int | float]] = field(default_factory=dict)
    first_event_time_utc_ns: int | None = None
    last_event_time_utc_ns: int | None = None

    def increment(self, name: str, amount: int = 1) -> None:
        if name not in COUNTER_NAMES or amount < 0:
            raise ValueError(f"invalid metric counter {name}")
        self.counters[name] = self.counters.get(name, 0) + amount

    def observe_histogram(self, name: str, value: int) -> None:
        if name not in HISTOGRAM_NAMES:
            raise ValueError(f"invalid metric histogram {name}")
        bucket = histogram_bucket(value)
        counts = self.histograms.setdefault(name, {})
        counts[bucket] = counts.get(bucket, 0) + 1

    def observe_gauge(self, name: str, value: int | float) -> None:
        if name not in GAUGE_NAMES:
            raise ValueError(f"invalid metric gauge {name}")
        current = self.gauges.setdefault(name, {"last": value, "max": value})
        current["last"] = value
        current["max"] = max(current["max"], value)

    def observe_envelope(self, envelope: EventEnvelope, *, raw_frame_bytes: int) -> None:
        is_rest = ".rest." in envelope.module or ".side_rest." in envelope.module
        if is_rest:
            self.increment("rest_responses")
        else:
            self.increment("websocket_messages")
            self.increment("websocket_payload_bytes", len(envelope.raw_payload))

        malformed = "malformed" in envelope.capture_flags
        self.increment("malformed" if malformed else "accepted")
        for flag, counter in (
            ("duplicate", "duplicate"),
            ("out_of_order", "out_of_order"),
            ("sequence_gap", "sequence_gap"),
            ("orderbook_resync", "orderbook_resync"),
            ("server_shutdown", "server_shutdown"),
            ("checksum_failure", "checksum_failure"),
        ):
            if flag in envelope.capture_flags:
                self.increment(counter)

        if envelope.stream == "diff_depth" and not malformed:
            payload = _decoded_object(envelope)
            if payload is not None:
                bids = payload.get("b")
                asks = payload.get("a")
                if isinstance(bids, list):
                    self.increment("depth_bid_level_updates", len(bids))
                if isinstance(asks, list):
                    self.increment("depth_ask_level_updates", len(asks))
        elif envelope.stream == "agg_trade" and not malformed:
            self.increment("agg_trade_records")
        elif envelope.stream == "book_ticker" and not malformed:
            self.increment("book_ticker_records")

        self.increment("raw_records_written")
        self.increment("raw_bytes_written", raw_frame_bytes)
        self.observe_gauge("queue_depth", 0)
        event_time = envelope.receive_time_utc_ns
        self.first_event_time_utc_ns = (
            event_time
            if self.first_event_time_utc_ns is None
            else min(self.first_event_time_utc_ns, event_time)
        )
        self.last_event_time_utc_ns = (
            event_time
            if self.last_event_time_utc_ns is None
            else max(self.last_event_time_utc_ns, event_time)
        )
        if envelope.exchange_event_time is not None:
            exchange_ns = envelope.exchange_event_time * 1_000_000
            if event_time >= exchange_ns:
                self.observe_histogram("receive_lag_ns", event_time - exchange_ns)

    def merge(self, other: MetricAggregate) -> None:
        for name, value in other.counters.items():
            self.counters[name] = self.counters.get(name, 0) + value
        for name, buckets in other.histograms.items():
            destination = self.histograms.setdefault(name, {})
            for bucket, count in buckets.items():
                destination[bucket] = destination.get(bucket, 0) + count
        for name, values in other.gauges.items():
            current = self.gauges.get(name)
            if current is None:
                self.gauges[name] = dict(values)
            else:
                current["last"] = values["last"]
                current["max"] = max(current["max"], values["max"])
        if other.first_event_time_utc_ns is not None:
            self.first_event_time_utc_ns = (
                other.first_event_time_utc_ns
                if self.first_event_time_utc_ns is None
                else min(self.first_event_time_utc_ns, other.first_event_time_utc_ns)
            )
        if other.last_event_time_utc_ns is not None:
            self.last_event_time_utc_ns = (
                other.last_event_time_utc_ns
                if self.last_event_time_utc_ns is None
                else max(self.last_event_time_utc_ns, other.last_event_time_utc_ns)
            )

    def document(self) -> dict[str, object]:
        return {
            "schema_version": AGGREGATE_SCHEMA_VERSION,
            "counters": dict(sorted(self.counters.items())),
            "histograms": {
                name: dict(sorted(values.items()))
                for name, values in sorted(self.histograms.items())
            },
            "gauges": {
                name: dict(values) for name, values in sorted(self.gauges.items())
            },
            "first_event_time_utc_ns": self.first_event_time_utc_ns,
            "last_event_time_utc_ns": self.last_event_time_utc_ns,
        }

    @classmethod
    def from_document(cls, document: object) -> MetricAggregate:
        if (
            not isinstance(document, dict)
            or document.get("schema_version") != AGGREGATE_SCHEMA_VERSION
        ):
            raise ValueError("unsupported metric aggregate")
        aggregate = cls()
        counters = document.get("counters")
        histograms = document.get("histograms")
        gauges = document.get("gauges")
        if (
            not isinstance(counters, dict)
            or not isinstance(histograms, dict)
            or not isinstance(gauges, dict)
        ):
            raise ValueError("invalid metric aggregate collections")
        for name, value in counters.items():
            if not isinstance(name, str) or not isinstance(value, int) or isinstance(value, bool):
                raise ValueError("invalid metric counter document")
            aggregate.increment(name, value)
        for name, buckets in histograms.items():
            if name not in HISTOGRAM_NAMES or not isinstance(buckets, dict):
                raise ValueError("invalid metric histogram document")
            parsed: dict[str, int] = {}
            for bucket, count in buckets.items():
                if not isinstance(bucket, str) or not isinstance(count, int) or count < 0:
                    raise ValueError("invalid metric histogram bucket")
                parsed[bucket] = count
            aggregate.histograms[name] = parsed
        for name, values in gauges.items():
            if name not in GAUGE_NAMES or not isinstance(values, dict):
                raise ValueError("invalid metric gauge document")
            last = values.get("last")
            maximum = values.get("max")
            if not isinstance(last, (int, float)) or not isinstance(maximum, (int, float)):
                raise ValueError("invalid metric gauge value")
            aggregate.gauges[name] = {"last": last, "max": maximum}
        first = document.get("first_event_time_utc_ns")
        last_event = document.get("last_event_time_utc_ns")
        if first is not None and not isinstance(first, int):
            raise ValueError("invalid first event time")
        if last_event is not None and not isinstance(last_event, int):
            raise ValueError("invalid last event time")
        aggregate.first_event_time_utc_ns = first
        aggregate.last_event_time_utc_ns = last_event
        return aggregate


def histogram_percentile(buckets: dict[str, int], percentile: float) -> int | None:
    if not 0 < percentile <= 1:
        raise ValueError("percentile must be in (0, 1]")
    total = sum(buckets.values())
    if total == 0:
        return None
    target = max(1, int(total * percentile + 0.999999999))
    cumulative = 0
    ordered = [str(bound) for bound in HISTOGRAM_BOUNDS_NS] + ["overflow"]
    for bucket in ordered:
        cumulative += buckets.get(bucket, 0)
        if cumulative >= target:
            return HISTOGRAM_BOUNDS_NS[-1] if bucket == "overflow" else int(bucket)
    raise ValueError("histogram counts are inconsistent")
