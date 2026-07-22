"""Binance Spot BTCUSDT Collector assembly for M4."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from binance_common.errors import Error as BinanceSdkError

from ..binance.spot.rest import SpotRestApi, capture_depth_snapshot
from ..binance.spot.schema import SPOT_STREAMS
from ..binance.spot.websocket import (
    ConnectionOpener,
    ReconnectBackoff,
    SpotStreamCollector,
    open_spot_websocket,
)
from ..domain.event import EventEnvelope
from ..logging import log_event
from ..metrics.recorder import MetricsRecorder
from ..metrics.report import DailyReporter
from ..paths import validate_data_root
from ..spool.stream import StreamSpool
from ..spool.writer import RotationPolicy
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, ensure_storage_layout


@dataclass(frozen=True)
class SpotCollectorSettings:
    data_root: Path
    collector_instance_id: str
    collector_version: str
    queue_capacity: int = 8192
    receipt_queue_capacity: int = 1024
    rotation_seconds: float = 60.0
    rotation_bytes: int = 128 * 1024 * 1024
    durability_interval_seconds: float = 1.0
    max_frame_bytes: int = 16 * 1024 * 1024
    planned_connection_rotation_seconds: float = 23 * 60 * 60 + 50 * 60
    snapshot_limit: int = 5000
    snapshot_timeout_ms: int = 10_000
    snapshot_retry_initial_seconds: float = 1.0
    snapshot_retry_maximum_seconds: float = 60.0
    snapshot_retry_jitter_ratio: float = 0.2


class SnapshotUnavailableError(RuntimeError):
    """Raised on shutdown when no required Spot depth snapshot was captured."""


class SpotCollector:
    """Own the three independent Spot streams and one public REST snapshot."""

    def __init__(
        self,
        settings: SpotCollectorSettings,
        *,
        logger: logging.Logger,
        rest_api: SpotRestApi | None = None,
        websocket_opener: ConnectionOpener = open_spot_websocket,
    ) -> None:
        self.settings = settings
        self.logger = logger
        self.rest_api = rest_api
        self.snapshot_backoff = ReconnectBackoff(
            initial_seconds=settings.snapshot_retry_initial_seconds,
            maximum_seconds=settings.snapshot_retry_maximum_seconds,
            jitter_ratio=settings.snapshot_retry_jitter_ratio,
        )
        safe_data_root = validate_data_root(settings.data_root)
        self.layout: StorageLayout = ensure_storage_layout(safe_data_root)
        self.catalog = Catalog(self.layout.catalog)
        self.metrics = MetricsRecorder(
            catalog=self.catalog,
            data_root=self.layout.root,
            collector_instance_id=settings.collector_instance_id,
            logger=logger,
        )
        rotation = RotationPolicy(
            seconds=settings.rotation_seconds, bytes=settings.rotation_bytes
        )

        def spool(stream: str) -> StreamSpool:
            def observe_event(
                envelope: EventEnvelope, frame_bytes: int, queue_depth: int
            ) -> None:
                self.metrics.safely_observe_written(
                    envelope, raw_frame_bytes=frame_bytes, queue_depth=queue_depth
                )

            def observe_operation(name: str, duration: int) -> None:
                self.metrics.safely_observe_operation(
                    market="spot", stream=stream, name=name, duration_ns=duration
                )

            def observe_seal(manifest: dict[str, object]) -> None:
                self.metrics.safely_observe_seal(manifest)

            return StreamSpool(
                layout=self.layout,
                catalog=self.catalog,
                market="spot",
                symbol="BTCUSDT",
                stream=stream,
                collector_instance_id=settings.collector_instance_id,
                collector_version=settings.collector_version,
                queue_capacity=settings.queue_capacity,
                rotation=rotation,
                durability_interval_seconds=settings.durability_interval_seconds,
                max_frame_bytes=settings.max_frame_bytes,
                event_observer=observe_event,
                operation_observer=observe_operation,
                seal_observer=observe_seal,
            )

        def lifecycle_observer(stream: str) -> Callable[[str], None]:
            def observe(event: str) -> None:
                self.metrics.safely_observe_lifecycle(
                    market="spot", stream=stream, event=event
                )

            return observe

        self.streams = tuple(
            SpotStreamCollector(
                stream=spec.stream,
                wire_name=spec.wire_name,
                spool=spool(spec.stream.value),
                collector_instance_id=settings.collector_instance_id,
                collector_version=settings.collector_version,
                logger=logger,
                receipt_queue_capacity=settings.receipt_queue_capacity,
                planned_rotation_seconds=settings.planned_connection_rotation_seconds,
                opener=websocket_opener,
                lifecycle_observer=lifecycle_observer(spec.stream.value),
            )
            for spec in SPOT_STREAMS
        )
        self.snapshot_spool = spool("depth_snapshot")

    async def _capture_snapshot(self, stop: asyncio.Event) -> None:
        failures = 0
        while not stop.is_set():
            try:
                envelope = await asyncio.to_thread(
                    capture_depth_snapshot,
                    rest_api=self.rest_api,
                    collector_instance_id=self.settings.collector_instance_id,
                    collector_version=self.settings.collector_version,
                    limit=self.settings.snapshot_limit,
                    timeout_ms=self.settings.snapshot_timeout_ms,
                )
            except (BinanceSdkError, RuntimeError) as exc:
                failures += 1
                log_event(
                    self.logger,
                    logging.WARNING,
                    "spot_snapshot_failed",
                    "public Spot depth snapshot failed; core streams remain active",
                    error_type=type(exc).__name__,
                    retry=failures,
                )
                delay = self.snapshot_backoff.delay(failures)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    continue
                break
            self.snapshot_spool.enqueue(envelope)
            await asyncio.to_thread(self.snapshot_spool.drain_all)
            return
        raise SnapshotUnavailableError(
            "Collector stopped before a required Spot depth snapshot was captured"
        )

    async def run(self, stop: asyncio.Event) -> None:
        """Start streams before snapshot, then gracefully seal when stopped."""

        try:
            async with asyncio.TaskGroup() as tasks:
                for stream in self.streams:
                    tasks.create_task(stream.run(stop))
                tasks.create_task(self._capture_snapshot(stop))
                await stop.wait()
        finally:
            stop.set()
            await asyncio.to_thread(self.snapshot_spool.close_and_seal)
            days = {day for day, _market, _stream in self.metrics.pending_keys()}
            batch_id = await asyncio.to_thread(self.metrics.safely_flush)
            reporter = DailyReporter(
                catalog=self.catalog, daily_directory=self.layout.daily_reports
            )
            if batch_id is not None:
                for day in sorted(days):
                    try:
                        await asyncio.to_thread(reporter.write, day)
                    except Exception as exc:
                        log_event(
                            self.logger,
                            logging.ERROR,
                            "daily_report_failed",
                            "daily report write failed; Raw remains sealed",
                            utc_date=day,
                            error_type=type(exc).__name__,
                        )
            self.catalog.close()
