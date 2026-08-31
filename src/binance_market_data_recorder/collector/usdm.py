"""M5 的 Binance USD-M BTCUSDT Collector 组装。

UsdMCollector 镜像 SpotCollector 的生命周期(三条独立流、snapshot resync、优雅关闭),
但增加以下 USD-M 特定不变量:

- USD-M 深度使用 U/u/pu 连续性(ADR-0011);Spot 使用 lastUpdateId+1。
  重建器强制执行此差异;Collector 自身不感知。
- Side data 任务(mark price、liquidation、REST 轮询)由 UsdMSideDataManager 管理,
  在核心关闭前被等待,确保 side Raw 在核心 Collector 的 Catalog 关闭之前持久化。
- Snapshot 循环对 Binance SDK 错误和运行时错误执行有上限的退避重试,
  同时保持核心 streams 活跃。
- Finally 块在关闭时排出 side data;side-task 错误被记录日志但不掩盖核心异常。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from binance_common.errors import Error as BinanceSdkError
from binance_common.errors import RateLimitBanError, TooManyRequestsError

from ..binance.spot.websocket import ReconnectBackoff
from ..binance.usdm.rest import (
    UsdMRestApi,
    UsdMSnapshotHttpError,
    UsdMSnapshotResponseError,
    capture_depth_snapshot,
)
from ..binance.usdm.schema import USDM_STREAMS
from ..binance.usdm.side_data_rest import UsdMSideRestApi
from ..binance.usdm.websocket import ConnectionOpener, UsdMStreamCollector, open_usdm_websocket
from ..domain.event import EventEnvelope
from ..logging import log_event
from ..metrics.recorder import MetricsRecorder
from ..metrics.report import DailyReporter
from ..orderbook.reconstructor import QualityAudit
from ..paths import validate_data_root
from ..spool.stream import StreamSpool
from ..spool.writer import RotationPolicy
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout, ensure_storage_layout
from ..supervisor.readiness import CollectorReadiness, ReadinessSnapshot
from .resync import DepthResyncCoordinator
from .usdm_side_data import (
    UsdMRestCooldown,
    UsdMSideDataManager,
    UsdMSideDataSettings,
)


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
    bootstrap_buffer_capacity: int = 8192
    side_data: UsdMSideDataSettings | None = None


class SnapshotUnavailableError(RuntimeError):
    """Raised when a USD-M capture session terminates without a control signal."""


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
        self.public_rest_request_lock = asyncio.Lock()
        self.public_rest_cooldown = UsdMRestCooldown()
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
        self.resync = DepthResyncCoordinator(
            market="um_perpetual", catalog=self.catalog
        )
        def observe_quality(audit: QualityAudit, occurred_at_utc_ns: int | None) -> None:
            if occurred_at_utc_ns is None:
                return
            self.metrics.safely_observe_quality(
                market="um_perpetual",
                stream="diff_depth",
                event=audit.kind,
                occurred_at_utc_ns=occurred_at_utc_ns,
            )
            if audit.kind == "sequence_gap":
                self.readiness.record_failure("sequence_gap")
                self.resync.request("sequence_gap", occurred_at_utc_ns)

        self.readiness = CollectorReadiness(
            market="um_perpetual",
            collector_instance_id=settings.collector_instance_id,
            collector_version=settings.collector_version,
            audit_observer=observe_quality,
            bootstrap_buffer_capacity=settings.bootstrap_buffer_capacity,
            bootstrap_overflow_observer=lambda: self.resync.request(
                "bootstrap_buffer_overflow"
            ),
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
                if event == "ingress_backpressure":
                    if stream == "diff_depth":
                        self.readiness.record_failure(event)
                        self.resync.request(event)
                    return
                self.metrics.safely_observe_lifecycle(
                    market="um_perpetual", stream=stream, event=event
                )
                if stream == "diff_depth" and event in {
                    "unexpected_disconnect",
                    "planned_rotation",
                    "server_shutdown",
                }:
                    self.readiness.record_failure(event)
                    self.resync.request(event)

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
                envelope_observer=self._observe_persisted,
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
                request_lock=self.public_rest_request_lock,
                cooldown=self.public_rest_cooldown,
            )

    async def _capture_snapshot(self, stop: asyncio.Event) -> None:
        failures = 0
        while not stop.is_set():
            try:
                await self.public_rest_cooldown.wait(stop)
                if stop.is_set():
                    return
                async with self.public_rest_request_lock:
                    await self.public_rest_cooldown.wait(stop)
                    if stop.is_set():
                        return
                    request_task = asyncio.create_task(
                        asyncio.to_thread(
                            capture_depth_snapshot,
                            rest_api=self.rest_api,
                            collector_instance_id=self.settings.collector_instance_id,
                            collector_version=self.settings.collector_version,
                            limit=self.settings.snapshot_limit,
                            timeout_ms=self.settings.snapshot_timeout_ms,
                            additional_capture_flags=self._capture_flags,
                        )
                    )
                    try:
                        envelope = await asyncio.shield(request_task)
                    except asyncio.CancelledError:
                        # asyncio cannot cancel an SDK call already running in a
                        # worker thread. Keep ownership of the Task until the call
                        # finishes, retrieve its outcome, then preserve cancellation.
                        await asyncio.gather(request_task, return_exceptions=True)
                        raise
            except UsdMSnapshotHttpError as exc:
                if not exc.rate_limited and not 500 <= exc.status < 600:
                    raise
                failures += 1
                if exc.rate_limited:
                    _, retry_at_utc_ns, reason = self.public_rest_cooldown.install(
                        status=exc.status,
                        retry_after_seconds=exc.retry_after_seconds,
                        retry_at_utc_ns=exc.retry_at_utc_ns,
                    )
                    log_event(
                        self.logger,
                        logging.WARNING,
                        "usdm_snapshot_rate_limited",
                        "USD-M public REST is rate limited; shared cooldown installed",
                        http_status=exc.status,
                        retry_at_utc_ns=retry_at_utc_ns,
                        response_headers=exc.headers,
                        reason_source=reason,
                        retry=failures,
                    )
                    await self.public_rest_cooldown.wait(stop)
                    if stop.is_set():
                        return
                    continue
                delay = self.snapshot_backoff.delay(failures)
                log_event(
                    self.logger,
                    logging.WARNING,
                    "usdm_snapshot_server_error",
                    "USD-M public depth snapshot returned a transient server error",
                    http_status=exc.status,
                    retry_at_utc_ns=exc.retry_at_utc_ns,
                    response_headers=exc.headers,
                    retry=failures,
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    continue
                return
            except RateLimitBanError:
                failures += 1
                _, retry_at_utc_ns, reason = self.public_rest_cooldown.install(
                    status=418
                )
                log_event(
                    self.logger,
                    logging.WARNING,
                    "usdm_snapshot_rate_limited",
                    "USD-M public REST is rate limited; shared cooldown installed",
                    http_status=418,
                    retry_at_utc_ns=retry_at_utc_ns,
                    reason_source=reason,
                    error_type="RateLimitBanError",
                    retry=failures,
                )
                await self.public_rest_cooldown.wait(stop)
                if stop.is_set():
                    return
                continue
            except TooManyRequestsError:
                failures += 1
                _, retry_at_utc_ns, reason = self.public_rest_cooldown.install(
                    status=429
                )
                log_event(
                    self.logger,
                    logging.WARNING,
                    "usdm_snapshot_rate_limited",
                    "USD-M public REST is rate limited; shared cooldown installed",
                    http_status=429,
                    retry_at_utc_ns=retry_at_utc_ns,
                    reason_source=reason,
                    error_type="TooManyRequestsError",
                    retry=failures,
                )
                await self.public_rest_cooldown.wait(stop)
                if stop.is_set():
                    return
                continue
            except UsdMSnapshotResponseError:
                raise
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
            if stop.is_set():
                # The blocking SDK request completed after this capture
                # session was retired. Its snapshot belongs to the old stream
                # generation and must not participate in a new diff bridge.
                return
            self.public_rest_cooldown.observe_success()
            self.snapshot_spool.enqueue(envelope)
            await asyncio.to_thread(self.snapshot_spool.drain_all)
            result = self.readiness.observe_snapshot_persisted(envelope)
            if self.readiness.snapshot().orderbook_synchronized:
                recovered_update_id = self.readiness.reliable_update_id
                if recovered_update_id is None:
                    raise RuntimeError(
                        "USD-M readiness reported synchronized without a local update ID"
                    )
                self.resync.complete(envelope, recovered_update_id)
                return
            failures += 1
            log_event(
                self.logger,
                logging.WARNING,
                "usdm_snapshot_bridge_pending",
                "snapshot did not bridge buffered depth; retry remains backoff bounded",
                synchronize_result=result.value,
                candidate_handoff=self._candidate_handoff,
                retry=failures,
            )
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.snapshot_backoff.delay(failures)
                )
            except TimeoutError:
                continue
            break
        # Session retirement is normal control flow. A resync (including
        # disconnect, planned rotation, or server shutdown) deliberately stops
        # this snapshot attempt and the outer loop starts a fresh snapshot +
        # diff bridge. Global shutdown follows the same cleanup path. Genuine
        # REST/response/integrity failures still escape above.
        return

    async def _run_capture_session(self, external_stop: asyncio.Event) -> None:
        session_stop = asyncio.Event()
        restarting = asyncio.Event()

        async def control_session() -> None:
            external = asyncio.create_task(external_stop.wait())
            restart = asyncio.create_task(self.resync.requested.wait())
            try:
                done, _pending = await asyncio.wait(
                    {external, restart}, return_when=asyncio.FIRST_COMPLETED
                )
                if restart in done and external not in done:
                    # A resync-requested session retirement reopens every
                    # stream connection in a fresh session. That close/open
                    # boundary is a transport reconnect boundary for every
                    # stream and must carry persistent gap evidence; a true
                    # global stop must not.
                    restarting.set()
            finally:
                session_stop.set()
                for task in (external, restart):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(external, restart, return_exceptions=True)

        async with asyncio.TaskGroup() as tasks:
            for stream in self.streams:
                tasks.create_task(stream.run(session_stop, session_restart=restarting))
            tasks.create_task(self._capture_snapshot(session_stop))
            tasks.create_task(control_session())

    async def run(self, stop: asyncio.Event) -> None:
        side_task = (
            asyncio.create_task(self.side_data.run(stop))
            if self.side_data is not None
            else None
        )
        try:
            restart_failures = 0
            while not stop.is_set():
                self.resync.requested.clear()
                await self._run_capture_session(stop)
                if stop.is_set():
                    break
                if not self.resync.requested.is_set():
                    raise SnapshotUnavailableError(
                        "USD-M capture session ended without shutdown or resync"
                    )
                restart_failures += 1
                log_event(
                    self.logger,
                    logging.CRITICAL,
                    "usdm_depth_resync_restart",
                    "depth continuity invalidated; restarting USD-M capture session",
                    buffered_capacity=self.settings.bootstrap_buffer_capacity,
                    reason=(
                        self.resync.active.reason
                        if self.resync.active is not None
                        else "unknown"
                    ),
                    restart=restart_failures,
                )
                self.readiness.restart_bootstrap()
                self.resync.prepare_restart()
                try:
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=self.snapshot_backoff.delay(restart_failures),
                    )
                except TimeoutError:
                    continue
        finally:
            stop.set()
            if side_task is not None:
                side_result = await asyncio.gather(side_task, return_exceptions=True)
                if side_result and isinstance(side_result[0], BaseException):
                    log_event(
                        self.logger,
                        logging.ERROR,
                        "usdm_side_shutdown_failed",
                        "USD-M side-data cleanup failed after stop; preserving core cause",
                        error_type=type(side_result[0]).__name__,
                    )
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

    def _observe_persisted(self, envelope: EventEnvelope) -> None:
        if envelope.stream == "diff_depth":
            self.resync.observe_depth(envelope)
        self.readiness.observe_persisted(envelope)

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
