"""Failure-isolated USD-M auxiliary public market-data tasks."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from functools import partial
from typing import Protocol

from binance_common.errors import Error as BinanceSdkError

from ..binance.usdm.side_data_rest import (
    REST_SIDE_DATA_SPECS,
    RestSideDataKind,
    UsdMSideRestApi,
    capture_rest_side_data,
)
from ..binance.usdm.side_data_schema import (
    USDM_SIDE_STREAMS,
    UsdMSideStream,
    envelope_from_side_stream_frame,
)
from ..binance.usdm.websocket import ConnectionOpener, UsdMStreamCollector
from ..domain.event import EventEnvelope
from ..logging import log_event
from ..spool.stream import StreamSpool
from ..spool.writer import RotationPolicy
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout


@dataclass(frozen=True)
class UsdMSideDataSettings:
    mark_price_enabled: bool = True
    liquidation_enabled: bool = True
    premium_index_enabled: bool = True
    funding_history_enabled: bool = True
    funding_info_enabled: bool = True
    open_interest_enabled: bool = True
    exchange_info_enabled: bool = True
    premium_index_interval_seconds: float = 60.0
    funding_history_interval_seconds: float = 300.0
    funding_info_interval_seconds: float = 3600.0
    open_interest_interval_seconds: float = 60.0
    exchange_info_interval_seconds: float = 3600.0

    def __post_init__(self) -> None:
        intervals = (
            self.premium_index_interval_seconds,
            self.funding_history_interval_seconds,
            self.funding_info_interval_seconds,
            self.open_interest_interval_seconds,
            self.exchange_info_interval_seconds,
        )
        if any(interval <= 0 for interval in intervals):
            raise ValueError("USD-M side-data polling intervals must be positive")

    def rest_enabled(self, kind: RestSideDataKind) -> bool:
        return {
            RestSideDataKind.PREMIUM_INDEX: self.premium_index_enabled,
            RestSideDataKind.FUNDING_HISTORY: self.funding_history_enabled,
            RestSideDataKind.FUNDING_INFO: self.funding_info_enabled,
            RestSideDataKind.OPEN_INTEREST: self.open_interest_enabled,
            RestSideDataKind.EXCHANGE_INFO: self.exchange_info_enabled,
        }[kind]

    def rest_interval(self, kind: RestSideDataKind) -> float:
        return {
            RestSideDataKind.PREMIUM_INDEX: self.premium_index_interval_seconds,
            RestSideDataKind.FUNDING_HISTORY: self.funding_history_interval_seconds,
            RestSideDataKind.FUNDING_INFO: self.funding_info_interval_seconds,
            RestSideDataKind.OPEN_INTEREST: self.open_interest_interval_seconds,
            RestSideDataKind.EXCHANGE_INFO: self.exchange_info_interval_seconds,
        }[kind]

    def stream_enabled(self, stream: UsdMSideStream) -> bool:
        return {
            UsdMSideStream.MARK_PRICE: self.mark_price_enabled,
            UsdMSideStream.LIQUIDATION: self.liquidation_enabled,
        }[stream]


@dataclass
class SideDataStats:
    enabled: bool
    attempts: int = 0
    accepted: int = 0
    malformed: int = 0
    failures: int = 0
    last_error_type: str | None = None

    def observe_envelope(self, envelope: EventEnvelope) -> None:
        self.attempts += 1
        if "malformed" in envelope.capture_flags:
            self.malformed += 1
        else:
            self.accepted += 1

    def observe_failure(self, error_type: str) -> None:
        self.failures += 1
        self.last_error_type = error_type

    def public_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "attempts": self.attempts,
            "accepted": self.accepted,
            "malformed": self.malformed,
            "failures": self.failures,
            "last_error_type": self.last_error_type,
        }


class SideDataExtension(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...


class RestSideDataPoller:
    def __init__(
        self,
        *,
        kind: RestSideDataKind,
        interval_seconds: float,
        spool: StreamSpool,
        stats: SideDataStats,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        rest_api: UsdMSideRestApi | None = None,
        timeout_ms: int = 10_000,
        request_lock: asyncio.Lock | None = None,
    ) -> None:
        self.kind = kind
        self.interval_seconds = interval_seconds
        self.spool = spool
        self.stats = stats
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.logger = logger
        self.rest_api = rest_api
        self.timeout_ms = timeout_ms
        self.request_lock = request_lock or asyncio.Lock()

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                self.stats.attempts += 1
                try:
                    async with self.request_lock:
                        envelope = await asyncio.to_thread(
                            capture_rest_side_data,
                            kind=self.kind,
                            rest_api=self.rest_api,
                            collector_instance_id=self.collector_instance_id,
                            collector_version=self.collector_version,
                            timeout_ms=self.timeout_ms,
                        )
                    self.spool.enqueue(envelope)
                    await asyncio.to_thread(self.spool.drain_all)
                    self.stats.accepted += 1
                except (BinanceSdkError, RuntimeError, OSError, TimeoutError, ValueError) as exc:
                    self.stats.observe_failure(type(exc).__name__)
                    log_event(
                        self.logger,
                        logging.WARNING,
                        "usdm_side_rest_failed",
                        "USD-M side-data poll failed; core collectors remain active",
                        stream=self.kind.value,
                        error_type=type(exc).__name__,
                        failures=self.stats.failures,
                    )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self.interval_seconds)
                except TimeoutError:
                    continue
        finally:
            await asyncio.to_thread(self.spool.close_and_seal)


class SideDataSupervisor:
    """Contain terminal side-task failures without setting the core stop event."""

    def __init__(
        self,
        extensions: dict[str, SideDataExtension],
        stats: dict[str, SideDataStats],
        logger: logging.Logger,
    ) -> None:
        self.extensions = extensions
        self.stats = stats
        self.logger = logger
        self.failures: dict[str, BaseException] = {}

    async def _run_one(self, name: str, extension: SideDataExtension, stop: asyncio.Event) -> None:
        try:
            await extension.run(stop)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failures[name] = exc
            self.stats[name].observe_failure(type(exc).__name__)
            log_event(
                self.logger,
                logging.ERROR,
                "usdm_side_task_stopped",
                "USD-M side-data task stopped; core collectors remain active",
                stream=name,
                error_type=type(exc).__name__,
            )

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._run_one(name, extension, stop))
            for name, extension in self.extensions.items()
        ]
        try:
            await stop.wait()
        finally:
            await asyncio.gather(*tasks, return_exceptions=True)


class UsdMSideDataManager:
    """Build independently switchable side streams and REST polling tasks."""

    def __init__(
        self,
        *,
        settings: UsdMSideDataSettings,
        layout: StorageLayout,
        catalog: Catalog,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        queue_capacity: int,
        receipt_queue_capacity: int,
        rotation: RotationPolicy,
        durability_interval_seconds: float,
        max_frame_bytes: int,
        planned_rotation_seconds: float,
        rest_timeout_ms: int,
        rest_api: UsdMSideRestApi | None,
        websocket_opener: ConnectionOpener,
    ) -> None:
        enabled = {
            **{kind.value: settings.rest_enabled(kind) for kind in REST_SIDE_DATA_SPECS},
            **{
                spec.stream.value: settings.stream_enabled(spec.stream)
                for spec in USDM_SIDE_STREAMS
            },
        }
        self.stats = {name: SideDataStats(is_enabled) for name, is_enabled in enabled.items()}

        def spool(stream: str) -> StreamSpool:
            return StreamSpool(
                layout=layout,
                catalog=catalog,
                market="um_perpetual",
                symbol="BTCUSDT",
                stream=stream,
                collector_instance_id=collector_instance_id,
                collector_version=collector_version,
                queue_capacity=queue_capacity,
                rotation=rotation,
                durability_interval_seconds=durability_interval_seconds,
                max_frame_bytes=max_frame_bytes,
            )

        extensions: dict[str, SideDataExtension] = {}
        rest_request_lock = asyncio.Lock()
        for spec in USDM_SIDE_STREAMS:
            if not settings.stream_enabled(spec.stream):
                continue
            stream_stats = self.stats[spec.stream.value]
            extensions[spec.stream.value] = UsdMStreamCollector(
                stream=spec.stream.value,
                route=spec.route,
                wire_name=spec.wire_name,
                spool=spool(spec.stream.value),
                collector_instance_id=collector_instance_id,
                collector_version=collector_version,
                logger=logger,
                receipt_queue_capacity=receipt_queue_capacity,
                planned_rotation_seconds=planned_rotation_seconds,
                opener=websocket_opener,
                envelope_factory=partial(envelope_from_side_stream_frame, stream=spec.stream),
                envelope_observer=stream_stats.observe_envelope,
                failure_observer=stream_stats.observe_failure,
            )
        for kind in REST_SIDE_DATA_SPECS:
            if not settings.rest_enabled(kind):
                continue
            extensions[kind.value] = RestSideDataPoller(
                kind=kind,
                interval_seconds=settings.rest_interval(kind),
                spool=spool(kind.value),
                stats=self.stats[kind.value],
                collector_instance_id=collector_instance_id,
                collector_version=collector_version,
                logger=logger,
                rest_api=rest_api,
                timeout_ms=rest_timeout_ms,
                request_lock=rest_request_lock,
            )
        self.supervisor = SideDataSupervisor(extensions, self.stats, logger)

    async def run(self, stop: asyncio.Event) -> None:
        await self.supervisor.run(stop)

    def status(self) -> dict[str, dict[str, object]]:
        return {name: stats.public_dict() for name, stats in sorted(self.stats.items())}
