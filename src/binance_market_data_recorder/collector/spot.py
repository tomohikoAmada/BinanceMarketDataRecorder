"""M4 的 Binance Spot BTCUSDT Collector 组装。

SpotCollector 拥有三条独立的 Spot WebSocket 流(diff_depth、agg_trade、book_ticker)
和一个公共 REST depth snapshot。生命周期不变量:

- 流在 snapshot 循环之前启动;这确保 diff-depth 事件在 snapshot 桥接前被缓冲,
  降低 SNAPSHOT_TOO_OLD 的概率。
- _capture_snapshot 在 capture session 的 TaskGroup 内运行,由监控外部 stop 事件
  和 resync-coordinator 请求事件的控制任务监督。
- 任何非干净全局停止的会话退出后,run() 循环以全新的 snapshot+stream 周期
  重新进入 _run_capture_session。这是 ADR-0023 描述的完整 resync 重启路径。
- 优雅关闭(stop.is_set())时,finally 块设置 stop 以确保 side-data 任务感知、
  密封 snapshot spool、刷新 metrics、写入日报告并关闭 Catalog。side data 在密封
  之前被等待,以确保 side Raw 在核心 Collector 退出前已持久化。
- 接收时间在 WebSocket 回调的 JSON 解析之前捕获(参见 binance.spot.websocket),
  保证解析异常不影响墙上时钟记录。
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from ..binance.spot.exchange_info import SpotExchangeInfoApi
from ..binance.spot.rate_limit import FullJitterBackoff, SpotRateLimitBlocked
from ..binance.spot.rest import (
    PublicSpotRestApi,
    SpotRestApi,
    SpotSnapshotHttpError,
    SpotSnapshotRequester,
)
from ..binance.spot.schema import SPOT_STREAMS
from ..binance.spot.websocket import (
    ConnectionOpener,
    SpotStreamCollector,
    open_spot_websocket,
)
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
from .spot_side_data import SpotExchangeInfoPoller
from .usdm_side_data import SideDataStats, SideDataSupervisor


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
    snapshot_limit: int = 1000
    snapshot_timeout_ms: int = 10_000
    snapshot_retry_initial_seconds: float = 1.0
    snapshot_retry_maximum_seconds: float = 60.0
    bootstrap_buffer_capacity: int = 8192
    exchange_info_enabled: bool = False
    exchange_info_interval_seconds: float = 3600.0
    side_data_degraded_after_seconds: float = 900.0


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
        snapshot_requester: SpotSnapshotRequester | None = None,
        exchange_info_api: SpotExchangeInfoApi | None = None,
    ) -> None:
        self.settings = settings
        self.logger = logger
        self.rest_api = rest_api or PublicSpotRestApi(
            timeout_ms=settings.snapshot_timeout_ms
        )
        self.snapshot_requester = snapshot_requester or SpotSnapshotRequester(
            rest_api=self.rest_api
        )
        self.snapshot_backoff = FullJitterBackoff(
            initial_seconds=settings.snapshot_retry_initial_seconds,
            maximum_seconds=settings.snapshot_retry_maximum_seconds,
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
        self.resync = DepthResyncCoordinator(market="spot", catalog=self.catalog)
        self._bootstrap_restart = self.resync.requested
        def observe_quality(audit: QualityAudit, occurred_at_utc_ns: int | None) -> None:
            if occurred_at_utc_ns is None:
                return
            self.metrics.safely_observe_quality(
                market="spot",
                stream="diff_depth",
                event=audit.kind,
                occurred_at_utc_ns=occurred_at_utc_ns,
            )
            if audit.kind == "sequence_gap":
                self.readiness.record_failure("sequence_gap")
                self.resync.request("sequence_gap", occurred_at_utc_ns)

        self.readiness = CollectorReadiness(
            market="spot",
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
                if event == "connected":
                    self.readiness.observe_connected(stream)
                    return
                if event == "disconnected":
                    self.readiness.observe_disconnected(stream)
                    return
                self.metrics.safely_observe_lifecycle(
                    market="spot", stream=stream, event=event
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
                envelope_observer=self._observe_persisted,
            )
            for spec in SPOT_STREAMS
        )
        self.snapshot_spool = spool("depth_snapshot")
        self._side_stats = {
            "exchange_info": SideDataStats(settings.exchange_info_enabled)
        }
        self._side_degraded_after_seconds = settings.side_data_degraded_after_seconds
        side_factories = {}
        if settings.exchange_info_enabled:
            side_factories["exchange_info"] = lambda: SpotExchangeInfoPoller(
                interval_seconds=settings.exchange_info_interval_seconds,
                spool=spool("exchange_info"),
                stats=self._side_stats["exchange_info"],
                collector_instance_id=settings.collector_instance_id,
                collector_version=settings.collector_version,
                rest_api=exchange_info_api,
                timeout_ms=settings.snapshot_timeout_ms,
                logger=logger,
            )
        self._side_supervisor = SideDataSupervisor(
            side_factories,
            self._side_stats,
            logger,
            retry_initial_seconds=settings.snapshot_retry_initial_seconds,
            retry_maximum_seconds=settings.snapshot_retry_maximum_seconds,
        )

    async def _capture_snapshot(self, stop: asyncio.Event) -> None:
        failures = 0
        while not stop.is_set():
            request_task = asyncio.create_task(
                self.snapshot_requester.capture(
                    collector_instance_id=self.settings.collector_instance_id,
                    collector_version=self.settings.collector_version,
                    limit=self.settings.snapshot_limit,
                    timeout_ms=self.settings.snapshot_timeout_ms,
                    additional_capture_flags=self._capture_flags,
                )
            )
            stop_task = asyncio.create_task(stop.wait())
            try:
                done, _pending = await asyncio.wait(
                    {request_task, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done and stop_task.result():
                    request_task.cancel()
                    await asyncio.gather(request_task, return_exceptions=True)
                    await self.snapshot_requester.wait_for_idle()
                    return
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
                envelope = await request_task
            except SpotRateLimitBlocked as exc:
                failures += 1
                log_event(
                    self.logger,
                    logging.WARNING,
                    "spot_snapshot_rate_limited",
                    "Spot public REST is blocked until the official retry boundary",
                    http_status=exc.status,
                    retry_at_utc_ns=exc.retry_at_utc_ns,
                    response_headers=exc.headers,
                    retry=failures,
                )
                delay = max(
                    0.0,
                    (exc.retry_at_utc_ns - time.time_ns()) / 1_000_000_000,
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    continue
                return
            except SpotSnapshotHttpError as exc:
                if not 500 <= exc.status < 600:
                    raise
                failures += 1
                log_event(
                    self.logger,
                    logging.WARNING,
                    "spot_snapshot_server_error",
                    "Spot public depth snapshot returned a transient server error",
                    http_status=exc.status,
                    response_headers=exc.headers,
                    retry=failures,
                )
                delay = self.snapshot_backoff.delay(failures)
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                continue
            except (OSError, RuntimeError, TimeoutError) as exc:
                failures += 1
                log_event(
                    self.logger,
                    logging.WARNING,
                    "spot_snapshot_transport_error",
                    "Spot public depth snapshot transport failed",
                    error_type=type(exc).__name__,
                    retry=failures,
                )
                delay = self.snapshot_backoff.delay(failures)
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                continue
            finally:
                if not stop_task.done():
                    stop_task.cancel()
                    await asyncio.gather(stop_task, return_exceptions=True)
            self.snapshot_spool.enqueue(envelope)
            await asyncio.to_thread(self.snapshot_spool.drain_all)
            result = self.readiness.observe_snapshot_persisted(envelope)
            if self.readiness.snapshot().orderbook_synchronized:
                recovered_update_id = self.readiness.reliable_update_id
                if recovered_update_id is None:
                    raise RuntimeError(
                        "Spot readiness reported synchronized without a local update ID"
                    )
                self.resync.complete(envelope, recovered_update_id)
                return
            failures += 1
            log_event(
                self.logger,
                logging.WARNING,
                "spot_snapshot_bridge_pending",
                "snapshot did not bridge buffered depth; retry remains rate limited",
                synchronize_result=result.value,
                retry=failures,
            )
            try:
                await asyncio.wait_for(
                    stop.wait(), timeout=self.snapshot_backoff.delay(failures)
                )
            except TimeoutError:
                continue
            return

    async def _run_capture_session(
        self,
        external_stop: asyncio.Event,
    ) -> None:
        session_stop = asyncio.Event()

        async def control_session() -> None:
            external = asyncio.create_task(external_stop.wait())
            overflow = asyncio.create_task(self.resync.requested.wait())
            try:
                await asyncio.wait(
                    {external, overflow},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                session_stop.set()
                for task in (external, overflow):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(external, overflow, return_exceptions=True)

        async with asyncio.TaskGroup() as tasks:
            for stream in self.streams:
                tasks.create_task(stream.run(session_stop))
            tasks.create_task(self._capture_snapshot(session_stop))
            tasks.create_task(control_session())

    async def run(self, stop: asyncio.Event) -> None:
        """Start streams before snapshot, then gracefully seal when stopped."""

        try:
            side_task = asyncio.create_task(self._side_supervisor.run(stop))
            restart_failures = 0
            while not stop.is_set():
                self.resync.requested.clear()
                await self._run_capture_session(stop)
                if stop.is_set():
                    break
                if not self.resync.requested.is_set():
                    raise SnapshotUnavailableError(
                        "Spot capture session ended without shutdown or bootstrap restart"
                    )
                restart_failures += 1
                log_event(
                    self.logger,
                    logging.CRITICAL,
                    "spot_depth_resync_restart",
                    "depth continuity invalidated; restarting Spot capture session",
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
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=self.snapshot_backoff.delay(restart_failures),
                    )
        finally:
            stop.set()
            if "side_task" in locals():
                await asyncio.gather(side_task, return_exceptions=True)
            await self.snapshot_requester.wait_for_idle()
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

    def readiness_snapshot(self) -> ReadinessSnapshot:
        return self.readiness.snapshot()

    def side_data_status(self) -> dict[str, dict[str, object]]:
        return {
            name: stats.public_dict(
                degraded_after_seconds=self._side_degraded_after_seconds
            )
            for name, stats in sorted(self._side_stats.items())
        }

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
