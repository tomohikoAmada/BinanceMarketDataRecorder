"""Thread-safe batching of operational summaries into the Catalog."""

from __future__ import annotations

import logging
import resource
import shutil
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from uuid import uuid4

from ..domain.event import EventEnvelope
from ..logging import log_event
from ..storage.catalog import Catalog
from ..storage.forecast import StorageForecaster
from .model import MetricAggregate

MetricKey = tuple[str, str, str]
CAPACITY_SAMPLE_BUCKET_NS = 60_000_000_000


def utc_date_from_ns(value: int) -> str:
    return datetime.fromtimestamp(value / 1_000_000_000, tz=UTC).date().isoformat()


class MetricsRecorder:
    """Accumulate summaries in memory and commit retry-idempotent batches."""

    def __init__(
        self,
        *,
        catalog: Catalog,
        data_root: Path,
        collector_instance_id: str,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock_ns: Callable[[], int] = time.monotonic_ns,
        process_time_ns: Callable[[], int] = time.process_time_ns,
        sample_interval_ns: int = 60_000_000_000,
        daily_directory: Path | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if not collector_instance_id or sample_interval_ns < 0:
            raise ValueError("invalid metrics recorder configuration")
        self.catalog = catalog
        self.data_root = data_root
        self.collector_instance_id = collector_instance_id
        self.utc_clock_ns = utc_clock_ns
        self.monotonic_clock_ns = monotonic_clock_ns
        self.process_time_ns = process_time_ns
        self.sample_interval_ns = sample_interval_ns
        self.daily_directory = (
            data_root / "data" / "reports" / "daily"
            if daily_directory is None
            else daily_directory
        )
        self.logger = logger or logging.getLogger("binance_market_data_recorder.metrics")
        self.failure_count = 0
        self.last_error_type: str | None = None
        self._lock = RLock()
        self._aggregates: dict[MetricKey, MetricAggregate] = {}
        self._batch_id = self._new_batch_id()
        self._last_sample_monotonic_ns: int | None = None
        self._last_process_time_ns: int | None = None
        self._latest_utc_date: str | None = None

    def _new_batch_id(self) -> str:
        return f"{self.collector_instance_id}:{uuid4()}"

    def _aggregate(self, utc_date: str, market: str, stream: str) -> MetricAggregate:
        return self._aggregates.setdefault((utc_date, market, stream), MetricAggregate())

    def _rollover_if_needed(self, utc_date: str) -> None:
        if self._latest_utc_date is None:
            self._latest_utc_date = utc_date
            return
        if utc_date <= self._latest_utc_date:
            return
        completed_days = sorted({day for day, _market, _stream in self._aggregates})
        self._flush_locked()
        from .report import DailyReporter

        reporter = DailyReporter(catalog=self.catalog, daily_directory=self.daily_directory)
        for day in completed_days:
            reporter.write(day)
        self._latest_utc_date = utc_date

    def observe_written(
        self,
        envelope: EventEnvelope,
        *,
        raw_frame_bytes: int,
        queue_depth: int,
    ) -> None:
        with self._lock:
            key = utc_date_from_ns(envelope.receive_time_utc_ns)
            self._rollover_if_needed(key)
            aggregate = self._aggregate(key, envelope.market, envelope.stream)
            aggregate.observe_envelope(envelope, raw_frame_bytes=raw_frame_bytes)
            aggregate.observe_gauge("queue_depth", queue_depth)
            self._sample_runtime(key)

    def observe_operation(
        self,
        *,
        market: str,
        stream: str,
        name: str,
        duration_ns: int,
        occurred_at_utc_ns: int | None = None,
    ) -> None:
        with self._lock:
            at = self.utc_clock_ns() if occurred_at_utc_ns is None else occurred_at_utc_ns
            utc_date = utc_date_from_ns(at)
            self._rollover_if_needed(utc_date)
            self._aggregate(utc_date, market, stream).observe_histogram(
                name, duration_ns
            )

    def observe_seal(self, manifest: dict[str, object]) -> None:
        sealed_at = manifest.get("sealed_at_utc_ns")
        market = manifest.get("market")
        stream = manifest.get("stream")
        stored_bytes = manifest.get("stored_bytes")
        if (
            not isinstance(sealed_at, int)
            or not isinstance(market, str)
            or not isinstance(stream, str)
        ):
            raise ValueError("manifest lacks metrics identity")
        if not isinstance(stored_bytes, int) or isinstance(stored_bytes, bool):
            raise ValueError("manifest stored bytes are invalid")
        with self._lock:
            utc_date = utc_date_from_ns(sealed_at)
            self._rollover_if_needed(utc_date)
            aggregate = self._aggregate(utc_date, market, stream)
            aggregate.increment("sealed_chunks")
            aggregate.increment("compressed_bytes", stored_bytes)
            aggregate.increment("archive_backlog_bytes", stored_bytes)

    def observe_lifecycle(
        self,
        *,
        market: str,
        stream: str,
        event: str,
        occurred_at_utc_ns: int | None = None,
    ) -> None:
        counter = {
            "planned_rotation": "planned_reconnect",
            "unexpected_disconnect": "unexpected_disconnect",
        }.get(event)
        if counter is None:
            raise ValueError("unsupported connection lifecycle metric")
        with self._lock:
            at = self.utc_clock_ns() if occurred_at_utc_ns is None else occurred_at_utc_ns
            utc_date = utc_date_from_ns(at)
            self._rollover_if_needed(utc_date)
            self._aggregate(utc_date, market, stream).increment(counter)

    def observe_quality(
        self,
        *,
        market: str,
        stream: str,
        event: str,
        occurred_at_utc_ns: int,
    ) -> None:
        counter = {
            "duplicate_or_stale_depth": "duplicate",
            "out_of_order": "out_of_order",
            "sequence_gap": "sequence_gap",
            "orderbook_resync": "orderbook_resync",
            "checksum_failure": "checksum_failure",
        }.get(event)
        if counter is None:
            return
        with self._lock:
            utc_date = utc_date_from_ns(occurred_at_utc_ns)
            self._rollover_if_needed(utc_date)
            self._aggregate(utc_date, market, stream).increment(counter)

    def _sample_runtime(self, utc_date: str) -> None:
        now_mono = self.monotonic_clock_ns()
        if (
            self._last_sample_monotonic_ns is not None
            and now_mono - self._last_sample_monotonic_ns < self.sample_interval_ns
        ):
            return
        process_now = self.process_time_ns()
        cpu_percent: float | None = None
        if self._last_sample_monotonic_ns is not None and self._last_process_time_ns is not None:
            elapsed = now_mono - self._last_sample_monotonic_ns
            if elapsed > 0:
                cpu_percent = max(0.0, (process_now - self._last_process_time_ns) / elapsed * 100)
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_bytes = int(rss) if __import__("sys").platform == "darwin" else int(rss) * 1024
        usage = shutil.disk_usage(self.data_root)
        free_bytes = usage.free
        total_bytes = usage.total
        observed_at_utc_ns = self.utc_clock_ns()
        observed_at_utc_ns -= observed_at_utc_ns % CAPACITY_SAMPLE_BUCKET_NS
        StorageForecaster(catalog=self.catalog, data_root=self.data_root).observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=total_bytes,
            free_bytes=free_bytes,
            observed_at_utc_ns=observed_at_utc_ns,
        )
        for (day, _market, _stream), aggregate in self._aggregates.items():
            if day != utc_date:
                continue
            aggregate.observe_gauge("rss_memory_bytes", rss_bytes)
            aggregate.observe_gauge("internal_free_bytes", free_bytes)
            if cpu_percent is not None:
                aggregate.observe_gauge("cpu_percent", cpu_percent)
        self._last_sample_monotonic_ns = now_mono
        self._last_process_time_ns = process_now

    def flush(self) -> str | None:
        with self._lock:
            return self._flush_locked()

    def _flush_locked(self) -> str | None:
        if not self._aggregates:
            return None
        batch_id = self._batch_id
        rows = [
            (day, market, stream, aggregate.document())
            for (day, market, stream), aggregate in sorted(self._aggregates.items())
        ]
        self.catalog.record_metric_batch(batch_id=batch_id, rows=rows)
        self._aggregates.clear()
        self._batch_id = self._new_batch_id()
        return batch_id

    def pending_keys(self) -> tuple[MetricKey, ...]:
        with self._lock:
            return tuple(sorted(self._aggregates))

    def _record_isolated_failure(self, operation: str, exc: Exception) -> None:
        self.failure_count += 1
        self.last_error_type = type(exc).__name__
        log_event(
            self.logger,
            logging.ERROR,
            "metrics_operation_failed",
            "operational metrics failed; Raw capture remains active",
            operation=operation,
            error_type=type(exc).__name__,
            failure_count=self.failure_count,
        )

    def safely_observe_written(
        self,
        envelope: EventEnvelope,
        *,
        raw_frame_bytes: int,
        queue_depth: int,
    ) -> bool:
        try:
            self.observe_written(
                envelope, raw_frame_bytes=raw_frame_bytes, queue_depth=queue_depth
            )
        except Exception as exc:
            self._record_isolated_failure("observe_written", exc)
            return False
        return True

    def safely_observe_operation(
        self, *, market: str, stream: str, name: str, duration_ns: int
    ) -> bool:
        try:
            self.observe_operation(
                market=market, stream=stream, name=name, duration_ns=duration_ns
            )
        except Exception as exc:
            self._record_isolated_failure("observe_operation", exc)
            return False
        return True

    def safely_observe_seal(self, manifest: dict[str, object]) -> bool:
        try:
            self.observe_seal(manifest)
        except Exception as exc:
            self._record_isolated_failure("observe_seal", exc)
            return False
        return True

    def safely_observe_lifecycle(self, *, market: str, stream: str, event: str) -> bool:
        try:
            self.observe_lifecycle(market=market, stream=stream, event=event)
        except Exception as exc:
            self._record_isolated_failure("observe_lifecycle", exc)
            return False
        return True

    def safely_observe_quality(
        self,
        *,
        market: str,
        stream: str,
        event: str,
        occurred_at_utc_ns: int,
    ) -> bool:
        try:
            self.observe_quality(
                market=market,
                stream=stream,
                event=event,
                occurred_at_utc_ns=occurred_at_utc_ns,
            )
        except Exception as exc:
            self._record_isolated_failure("observe_quality", exc)
            return False
        return True

    def safely_flush(self) -> str | None:
        try:
            return self.flush()
        except Exception as exc:
            self._record_isolated_failure("flush", exc)
            return None
