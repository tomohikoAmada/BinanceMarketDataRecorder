"""Binance USD-M BTCUSDT Collector assembly for M5."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from binance_common.errors import Error as BinanceSdkError

from ..binance.spot.websocket import ReconnectBackoff
from ..binance.usdm.rest import UsdMRestApi, capture_depth_snapshot
from ..binance.usdm.schema import USDM_STREAMS
from ..binance.usdm.websocket import ConnectionOpener, UsdMStreamCollector, open_usdm_websocket
from ..logging import log_event
from ..paths import validate_data_root
from ..spool.stream import StreamSpool
from ..spool.writer import RotationPolicy
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, ensure_storage_layout


class UsdMSideDataExtension(Protocol):
    """M7 extension boundary; M5 does not instantiate or execute extensions."""

    @property
    def name(self) -> str: ...

    async def run(self, stop: asyncio.Event) -> None: ...


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


class SnapshotUnavailableError(RuntimeError):
    """Raised on shutdown when no required USD-M snapshot was captured."""


class UsdMCollector:
    """Own three independent USD-M streams and one public REST snapshot."""

    side_data_extensions: tuple[UsdMSideDataExtension, ...] = ()

    def __init__(
        self,
        settings: UsdMCollectorSettings,
        *,
        logger: logging.Logger,
        rest_api: UsdMRestApi | None = None,
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
        rotation = RotationPolicy(seconds=settings.rotation_seconds, bytes=settings.rotation_bytes)

        def spool(stream: str) -> StreamSpool:
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
            )

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
            )
            for spec in USDM_STREAMS
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
            return
        raise SnapshotUnavailableError(
            "Collector stopped before a required USD-M depth snapshot was captured"
        )

    async def run(self, stop: asyncio.Event) -> None:
        try:
            async with asyncio.TaskGroup() as tasks:
                for stream in self.streams:
                    tasks.create_task(stream.run(stop))
                tasks.create_task(self._capture_snapshot(stop))
                await stop.wait()
        finally:
            stop.set()
            await asyncio.to_thread(self.snapshot_spool.close_and_seal)
            self.catalog.close()
