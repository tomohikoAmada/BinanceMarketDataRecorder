"""Binance USD-M BTCUSDT Collector assembly for M5."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from binance_common.errors import Error as BinanceSdkError

from ..binance.spot.websocket import ReconnectBackoff
from ..binance.usdm.rest import UsdMRestApi, capture_depth_snapshot
from ..binance.usdm.schema import USDM_STREAMS
from ..binance.usdm.side_data_rest import UsdMSideRestApi
from ..binance.usdm.websocket import ConnectionOpener, UsdMStreamCollector, open_usdm_websocket
from ..domain.event import EventEnvelope
from ..logging import log_event
from ..metrics.recorder import MetricsRecorder
from ..metrics.report import DailyReporter
from ..paths import validate_data_root
from ..spool.stream import StreamSpool
from ..spool.writer import RotationPolicy
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, ensure_storage_layout
from ..supervisor.readiness import CollectorReadiness, ReadinessSnapshot
from .usdm_side_data import UsdMSideDataManager, UsdMSideDataSettings


@dataclass(frozen=True)
class UsdMCollectorSettings:
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
    snapshot_limit: int = 1000
    snapshot_timeout_ms: int = 10_000
    snapshot_retry_initial_seconds: float = 1.0
    snapshot_retry_maximum_seconds: float = 60.0
    snapshot_retry_jitter_ratio: float = 0.2
    side_data: UsdMSideDataSettings | None = None


class SnapshotUnavailableError(RuntimeError):
    """Raised on shutdown when no required USD-M snapshot was captured."""


class UsdMCollector:
    """Own three independent USD-M streams and one public REST snapshot."""

    def __init__(
        self,
        settings: UsdMCollectorSettings,
        *,
        logger: logging.Logger,
        rest_api: UsdMRestApi | None = None,
        side_rest_api: UsdMSideRestApi | None = None,
        websocket_opener: ConnectionOpener = open_usdm_websocket,
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
        self.readiness = CollectorReadiness(
            market="um_perpetual",
            collector_instance_id=settings.collector_instance_id,
            collector_version=settings.collector_version,
        )
        self._capture_flags: tuple[str, ...] = ()
        self._candidate_handoff = False
        rotation = RotationPolicy(seconds=settings.rotation_seconds, bytes=settings.rotation_bytes)

        def spool(stream: str) -> StreamSpool:
            def observe_event(
                envelope: EventEnvelope, frame_bytes: int, queue_depth: int
            ) -> None:
                self.metrics.safely_observe_written(
                    envelope, raw_frame_bytes=frame_bytes, queue_depth=queue_depth
                )

            def observe_operation(name: str, duration: int) -> None:
                self.metrics.safely_observe_operation(
                    market="um_perpetual",
                    stream=stream,
                    name=name,
                    duration_ns=duration,
                )

            def observe_seal(manifest: dict[str, object]) -> None:
                self.metrics.safely_observe_seal(manifest)

            return StreamSpool(
                layout=self.layout,
                catalog=self.catalog,
                market="um_perpetual",
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
                if event == "connected":
                    self.readiness.observe_connected(stream)
                    return
                if event == "disconnected":
                    self.readiness.observe_disconnected(stream)
                    return
                self.metrics.safely_observe_lifecycle(
                    market="um_perpetual", stream=stream, event=event
                )

            return observe

        self.streams = tuple(
            UsdMStreamCollector(
                stream=spec.stream,
                route=spec.route,
                wire_name=spec.wire_name,
                spool=spool(spec.stream.value),
                collector_instance_id=settings.collector_instance_id,
                collector_version=settings.collector_version,
                logger=logger,
                receipt_queue_capacity=settings.receipt_queue_capacity,
                planned_rotation_seconds=settings.planned_connection_rotation_seconds,
                opener=websocket_opener,
                lifecycle_observer=lifecycle_observer(spec.stream.value),
                envelope_observer=self.readiness.observe_persisted,
            )
            for spec in USDM_STREAMS
        )
        self.snapshot_spool = spool("depth_snapshot")
        self.side_data: UsdMSideDataManager | None = None
        if settings.side_data is not None:
            self.side_data = UsdMSideDataManager(
                settings=settings.side_data,
                layout=self.layout,
                catalog=self.catalog,
                collector_instance_id=settings.collector_instance_id,
                collector_version=settings.collector_version,
                logger=logger,
                queue_capacity=settings.queue_capacity,
                receipt_queue_capacity=settings.receipt_queue_capacity,
                rotation=rotation,
                durability_interval_seconds=settings.durability_interval_seconds,
                max_frame_bytes=settings.max_frame_bytes,
                planned_rotation_seconds=settings.planned_connection_rotation_seconds,
                rest_timeout_ms=settings.snapshot_timeout_ms,
                rest_api=side_rest_api,
                websocket_opener=websocket_opener,
                metrics=self.metrics,
            )

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
                    additional_capture_flags=self._capture_flags,
                )
            except (BinanceSdkError, RuntimeError) as exc:
                failures += 1
                log_event(
                    self.logger,
                    logging.WARNING,
                    "usdm_snapshot_failed",
                    "public USD-M depth snapshot failed; core streams remain active",
                    error_type=type(exc).__name__,
                    retry=failures,
                )
                try:
                    await asyncio.wait_for(
                        stop.wait(), timeout=self.snapshot_backoff.delay(failures)
                    )
                except TimeoutError:
                    continue
                break
            self.snapshot_spool.enqueue(envelope)
            await asyncio.to_thread(self.snapshot_spool.drain_all)
            self.readiness.observe_snapshot_persisted(envelope)
            if self.readiness.snapshot().orderbook_synchronized:
                return
            if not self._candidate_handoff:
                return
            failures += 1
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.snapshot_backoff.delay(failures)
                )
            except TimeoutError:
                continue
            break
        raise SnapshotUnavailableError(
            "Collector stopped before a required USD-M depth snapshot was captured"
        )

    async def run(self, stop: asyncio.Event) -> None:
        try:
            async with asyncio.TaskGroup() as tasks:
                for stream in self.streams:
                    tasks.create_task(stream.run(stop))
                tasks.create_task(self._capture_snapshot(stop))
                if self.side_data is not None:
                    tasks.create_task(self.side_data.run(stop))
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

    def side_data_status(self) -> dict[str, dict[str, object]]:
        return self.side_data.status() if self.side_data is not None else {}

    def readiness_snapshot(self) -> ReadinessSnapshot:
        return self.readiness.snapshot()

    def set_handoff_context(
        self,
        *,
        deployment_id: str | None,
        role: str | None,
        reason: str | None,
    ) -> None:
        flags = _handoff_flags(deployment_id, role, reason)
        self._capture_flags = flags
        self._candidate_handoff = role == "candidate"
        for stream in self.streams:
            stream.set_capture_flags(flags)


def _handoff_flags(
    deployment_id: str | None, role: str | None, reason: str | None
) -> tuple[str, ...]:
    if deployment_id is None or role is None or reason is None:
        return ()
    return (
        "blue_green_overlap",
        f"deployment_id={deployment_id}",
        f"instance_role={role}",
        f"handoff_reason={reason.lower()}",
    )
