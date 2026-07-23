"""Deterministic JSON/CSV UTC daily reports from Catalog aggregate batches."""

from __future__ import annotations

import csv
import io
import json
import os
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path
from threading import RLock

from ..storage.catalog import Catalog
from ..storage.layout import fsync_directory
from .model import (
    INPUT_COUNTERS,
    OUTPUT_COUNTERS,
    QUALITY_COUNTERS,
    MetricAggregate,
    histogram_percentile,
)

REPORT_SCHEMA_VERSION = "daily-operational-report.v1"
_REPORT_LOCK = RLock()
NOT_IMPLEMENTED_OUTPUTS = (
    "normalized_rows",
    "normalized_bytes",
)


def _validate_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("UTC date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("UTC date must be canonical YYYY-MM-DD")
    return value


def _percentiles(aggregate: MetricAggregate, name: str) -> dict[str, object]:
    buckets = aggregate.histograms.get(name, {})
    if not buckets:
        return {
            "p50": None,
            "p95": None,
            "p99": None,
            "status": "INSUFFICIENT_DATA",
        }
    return {
        "p50": histogram_percentile(buckets, 0.50),
        "p95": histogram_percentile(buckets, 0.95),
        "p99": histogram_percentile(buckets, 0.99),
        "status": "AVAILABLE",
    }


def _gauge(aggregate: MetricAggregate, name: str, field: str = "last") -> dict[str, object]:
    values = aggregate.gauges.get(name)
    if values is None:
        return {"value": None, "status": "INSUFFICIENT_DATA"}
    return {"value": values[field], "status": "AVAILABLE"}


def _stream_document(
    market: str,
    stream: str,
    aggregate: MetricAggregate,
    *,
    generated_at_utc_ns: int,
    archive_backlog_bytes: int,
    oldest_unarchived_time_utc_ns: int | None,
) -> dict[str, object]:
    inputs: dict[str, object] = {
        name: aggregate.counters.get(name, 0) for name in INPUT_COUNTERS
    }
    if aggregate.counters.get("rest_responses", 0):
        inputs["rest_bytes"] = None
        rest_bytes_status = "UNAVAILABLE_SDK_RAW_BODY"
    else:
        inputs["rest_bytes"] = 0
        rest_bytes_status = "NOT_APPLICABLE"
    quality = {name: aggregate.counters.get(name, 0) for name in QUALITY_COUNTERS}
    output: dict[str, object] = {
        name: aggregate.counters.get(name, 0) for name in OUTPUT_COUNTERS
    }
    output["archive_backlog_bytes"] = archive_backlog_bytes
    output.update({name: None for name in NOT_IMPLEMENTED_OUTPUTS})
    last_event = aggregate.last_event_time_utc_ns
    last_event_age = (
        None if last_event is None else max(0, generated_at_utc_ns - last_event)
    )
    performance: dict[str, object] = {
        "receive_lag_ns": _percentiles(aggregate, "receive_lag_ns"),
        "queue_depth_max": _gauge(aggregate, "queue_depth", "max"),
        "write_latency_ns": _percentiles(aggregate, "write_latency_ns"),
        "fsync_latency_ns": _percentiles(aggregate, "fsync_latency_ns"),
        "cpu_percent": _gauge(aggregate, "cpu_percent"),
        "rss_memory_bytes": _gauge(aggregate, "rss_memory_bytes"),
        "internal_free_bytes": _gauge(aggregate, "internal_free_bytes"),
        "external_free_bytes": {
            "value": None,
            "status": "NO_REGISTERED_TARGET_SAMPLE",
        },
        "oldest_unarchived_age_ns": {
            "value": (
                None
                if oldest_unarchived_time_utc_ns is None
                else max(0, generated_at_utc_ns - oldest_unarchived_time_utc_ns)
            ),
            "status": (
                "INSUFFICIENT_DATA"
                if oldest_unarchived_time_utc_ns is None
                else "AVAILABLE"
            ),
        },
        "last_event_age_ns": {
            "value": last_event_age,
            "status": "INSUFFICIENT_DATA" if last_event_age is None else "AVAILABLE",
        },
    }
    unavailable = {"input.rest_bytes": rest_bytes_status}
    unavailable.update(
        {f"output.{name}": "NOT_IMPLEMENTED" for name in NOT_IMPLEMENTED_OUTPUTS}
    )
    return {
        "market": market,
        "stream": stream,
        "input": inputs,
        "quality": quality,
        "output": output,
        "performance": performance,
        "availability": unavailable,
    }


def _flatten(prefix: str, value: object, output: dict[str, object]) -> None:
    if isinstance(value, dict):
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else key
            _flatten(child, value[key], output)
    else:
        output[prefix] = value


class DailyReporter:
    def __init__(
        self,
        *,
        catalog: Catalog,
        daily_directory: Path,
        utc_clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.catalog = catalog
        self.daily_directory = daily_directory
        self.utc_clock_ns = utc_clock_ns

    def build(self, utc_date: str, *, generated_at_utc_ns: int | None = None) -> dict[str, object]:
        selected_date = _validate_date(utc_date)
        generated_at = self.utc_clock_ns() if generated_at_utc_ns is None else generated_at_utc_ns
        aggregates: dict[tuple[str, str], MetricAggregate] = {}
        historical: dict[tuple[str, str], MetricAggregate] = {}
        batch_ids: set[str] = set()
        for row in self.catalog.metric_batches(selected_date):
            market = row["market"]
            stream = row["stream"]
            batch_id = row["batch_id"]
            if (
                not isinstance(market, str)
                or not isinstance(stream, str)
                or not isinstance(batch_id, str)
            ):
                raise ValueError("invalid Catalog metric row identity")
            batch_ids.add(batch_id)
            aggregate = aggregates.setdefault((market, stream), MetricAggregate())
            aggregate.merge(MetricAggregate.from_document(row["aggregate"]))
        for row in self.catalog.metric_batches_through(selected_date):
            market = row["market"]
            stream = row["stream"]
            if not isinstance(market, str) or not isinstance(stream, str):
                raise ValueError("invalid historical metric row identity")
            aggregate = historical.setdefault((market, stream), MetricAggregate())
            aggregate.merge(MetricAggregate.from_document(row["aggregate"]))
        streams = []
        for (market, stream), aggregate in sorted(aggregates.items()):
            lifetime = historical[(market, stream)]
            archive_backlog_bytes = max(
                0,
                lifetime.counters.get("archive_backlog_bytes", 0)
                - lifetime.counters.get("archived_bytes", 0),
            )
            streams.append(
                _stream_document(
                    market,
                    stream,
                    aggregate,
                    generated_at_utc_ns=generated_at,
                    archive_backlog_bytes=archive_backlog_bytes,
                    oldest_unarchived_time_utc_ns=(
                        lifetime.first_event_time_utc_ns
                        if archive_backlog_bytes
                        else None
                    ),
                )
            )
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "utc_date": selected_date,
            "generated_at_utc_ns": generated_at,
            "status": "OK" if streams else "NO_DATA",
            "batch_count": len(batch_ids),
            "stream_count": len(streams),
            "streams": streams,
        }

    def _atomic_write(self, path: Path, body: bytes) -> None:
        self.daily_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        partial = path.with_name(f".{path.name}.{os.getpid()}.partial")
        descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            view = memoryview(body)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("daily report write returned no progress")
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(partial, path)
        fsync_directory(self.daily_directory)

    def write(self, utc_date: str, *, generated_at_utc_ns: int | None = None) -> dict[str, object]:
        with _REPORT_LOCK:
            return self._write_locked(utc_date, generated_at_utc_ns=generated_at_utc_ns)

    def _write_locked(
        self, utc_date: str, *, generated_at_utc_ns: int | None = None
    ) -> dict[str, object]:
        document = self.build(utc_date, generated_at_utc_ns=generated_at_utc_ns)
        json_body = (
            json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            + "\n"
        ).encode()
        json_path = self.daily_directory / f"{utc_date}.json"
        self._atomic_write(json_path, json_body)

        rows: list[dict[str, object]] = []
        stream_documents = document["streams"]
        if not isinstance(stream_documents, list):
            raise ValueError("invalid daily report streams")
        for stream in stream_documents:
            if not isinstance(stream, dict):
                raise ValueError("invalid daily stream document")
            flattened: dict[str, object] = {
                "schema_version": document["schema_version"],
                "utc_date": document["utc_date"],
                "generated_at_utc_ns": document["generated_at_utc_ns"],
            }
            _flatten("", stream, flattened)
            rows.append(flattened)
        fieldnames = sorted({name for row in rows for name in row})
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, lineterminator="\n")
        if fieldnames:
            writer.writeheader()
            writer.writerows(rows)
        self._atomic_write(
            self.daily_directory / f"{utc_date}.csv",
            buffer.getvalue().encode(),
        )
        return document
