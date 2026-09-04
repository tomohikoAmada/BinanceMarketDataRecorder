"""故障隔离的 USD-M 辅助公共市场数据任务。

Side-data 任务(mark price WebSocket、liquidation WebSocket、以及 premium index、
funding、open interest、exchange info 和六种 5 分钟统计的 REST 轮询)遵循以下
不变量:

- 每个任务拥有自己的 StreamSpool,因此 REST 限流或 WebSocket 故障不会阻塞
  核心 L2 diff_depth/agg_trade/book_ticker。
- SideDataSupervisor 以有上限的指数完全抖动回退重启失败的 REST 任务,保留
  尝试计数和连续失败计数。它永不设置核心 stop 事件,因此 side-data 故障
  不能终止核心采集。
- WebSocket side 任务(mark_price/liquidation)的 transport-integrity 语义与
  核心流一致(M21.4.11-R4):网络断连在 collector 内部通过 Reconnect
  Boundary 状态机恢复;任何逃逸 collector 的终态完整性/存储故障都
  fail closed —— 任务进入 FAILED 且绝不自动打开替代连接,直到服务重启
  运行启动恢复。
- RestSideDataPoller._catch_up_five_minute 为 M19 5 分钟统计实现有界追赶。
  它从 Cursor + 5 分钟查询到最后一个已关闭的 UTC 周期,以可配置批次进行。
  Raw 在 Cursor 推进前完成排空和 fsync。空响应不推进 Cursor 并创建
  SIDE_DATA_EMPTY_RESPONSE 事件。超出官方保留窗口的缺失周期变为
  SIDE_DATA_UNRECOVERABLE_GAP 事件。
- Cursor 通过 Catalog.side_data_cursor/advance_side_data_cursor 实现持久化,
  因此停机后重启从最后持久化的周期恢复。
- USD-M 5 分钟 REST 调用共享单个 asyncio.Lock 进行串行化
  (ADR-0012;用于避免初始请求突发,而非严格 weight 核算)。
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from typing import Protocol

from binance_common.errors import Error as BinanceSdkError
from binance_common.errors import RateLimitBanError, TooManyRequestsError

from ..binance.usdm.side_data_rest import (
    FIVE_MINUTE_KINDS,
    FIVE_MINUTE_PERIOD_MS,
    FIVE_MINUTE_RETENTION,
    REST_SIDE_DATA_SPECS,
    RestSideDataKind,
    UsdMSideDataHttpError,
    UsdMSideRestApi,
    capture_rest_side_data,
)
from ..binance.usdm.side_data_schema import (
    USDM_SIDE_STREAMS,
    UsdMSideStream,
    UsdMSideStreamSpec,
    envelope_from_side_stream_frame,
)
from ..binance.usdm.websocket import ConnectionOpener, UsdMStreamCollector
from ..domain.event import EventEnvelope
from ..logging import log_event
from ..metrics.recorder import MetricsRecorder
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
    open_interest_statistics_enabled: bool = False
    taker_buy_sell_volume_enabled: bool = False
    global_long_short_ratio_enabled: bool = False
    top_long_short_account_ratio_enabled: bool = False
    top_long_short_position_ratio_enabled: bool = False
    basis_enabled: bool = False
    premium_index_interval_seconds: float = 60.0
    funding_history_interval_seconds: float = 300.0
    funding_info_interval_seconds: float = 3600.0
    open_interest_interval_seconds: float = 60.0
    exchange_info_interval_seconds: float = 3600.0
    open_interest_statistics_interval_seconds: float = 300.0
    taker_buy_sell_volume_interval_seconds: float = 300.0
    global_long_short_ratio_interval_seconds: float = 300.0
    top_long_short_account_ratio_interval_seconds: float = 300.0
    top_long_short_position_ratio_interval_seconds: float = 300.0
    basis_interval_seconds: float = 300.0
    degraded_after_seconds: float = 900.0
    retry_initial_seconds: float = 1.0
    retry_maximum_seconds: float = 60.0

    def __post_init__(self) -> None:
        intervals = (
            self.premium_index_interval_seconds,
            self.funding_history_interval_seconds,
            self.funding_info_interval_seconds,
            self.open_interest_interval_seconds,
            self.exchange_info_interval_seconds,
            self.open_interest_statistics_interval_seconds,
            self.taker_buy_sell_volume_interval_seconds,
            self.global_long_short_ratio_interval_seconds,
            self.top_long_short_account_ratio_interval_seconds,
            self.top_long_short_position_ratio_interval_seconds,
            self.basis_interval_seconds,
            self.degraded_after_seconds,
            self.retry_initial_seconds,
            self.retry_maximum_seconds,
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
            RestSideDataKind.OPEN_INTEREST_STATISTICS: (
                self.open_interest_statistics_enabled
            ),
            RestSideDataKind.TAKER_BUY_SELL_VOLUME: self.taker_buy_sell_volume_enabled,
            RestSideDataKind.GLOBAL_LONG_SHORT_RATIO: (
                self.global_long_short_ratio_enabled
            ),
            RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO: (
                self.top_long_short_account_ratio_enabled
            ),
            RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO: (
                self.top_long_short_position_ratio_enabled
            ),
            RestSideDataKind.BASIS: self.basis_enabled,
        }[kind]

    def rest_interval(self, kind: RestSideDataKind) -> float:
        return {
            RestSideDataKind.PREMIUM_INDEX: self.premium_index_interval_seconds,
            RestSideDataKind.FUNDING_HISTORY: self.funding_history_interval_seconds,
            RestSideDataKind.FUNDING_INFO: self.funding_info_interval_seconds,
            RestSideDataKind.OPEN_INTEREST: self.open_interest_interval_seconds,
            RestSideDataKind.EXCHANGE_INFO: self.exchange_info_interval_seconds,
            RestSideDataKind.OPEN_INTEREST_STATISTICS: (
                self.open_interest_statistics_interval_seconds
            ),
            RestSideDataKind.TAKER_BUY_SELL_VOLUME: (
                self.taker_buy_sell_volume_interval_seconds
            ),
            RestSideDataKind.GLOBAL_LONG_SHORT_RATIO: (
                self.global_long_short_ratio_interval_seconds
            ),
            RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO: (
                self.top_long_short_account_ratio_interval_seconds
            ),
            RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO: (
                self.top_long_short_position_ratio_interval_seconds
            ),
            RestSideDataKind.BASIS: self.basis_interval_seconds,
        }[kind]

    def stream_enabled(self, stream: UsdMSideStream) -> bool:
        return {
            UsdMSideStream.MARK_PRICE: self.mark_price_enabled,
            UsdMSideStream.LIQUIDATION: self.liquidation_enabled,
        }[stream]


@dataclass
class SideDataStats:
    enabled: bool
    expected_interval_seconds: float | None = None
    status: str = "STOPPED"
    running: bool = False
    attempts: int = 0
    accepted: int = 0
    malformed: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    last_success_at_utc_ns: int | None = None
    last_error_type: str | None = None
    next_retry_at_utc_ns: int | None = None

    def observe_envelope(self, envelope: EventEnvelope) -> None:
        if "malformed" in envelope.capture_flags:
            self.malformed += 1
        else:
            self.accepted += 1
            self.observe_success()

    def observe_success(self) -> None:
        self.consecutive_failures = 0
        self.last_success_at_utc_ns = time.time_ns()
        self.last_error_type = None
        self.next_retry_at_utc_ns = None
        self.status = "RUNNING"

    def observe_failure(self, error_type: str) -> None:
        self.failures += 1
        self.consecutive_failures += 1
        self.last_error_type = error_type
        self.status = "RETRYING"

    def public_dict(self, *, degraded_after_seconds: float) -> dict[str, object]:
        status = self.status
        stale_after_seconds = degraded_after_seconds
        if self.expected_interval_seconds is not None:
            stale_after_seconds += self.expected_interval_seconds
        if (
            self.enabled
            and self.last_success_at_utc_ns is not None
            and time.time_ns() - self.last_success_at_utc_ns
            > int(stale_after_seconds * 1_000_000_000)
        ):
            status = "STALE"
        return {
            "status": status,
            "enabled": self.enabled,
            "running": self.running,
            "attempts": self.attempts,
            "accepted": self.accepted,
            "malformed": self.malformed,
            "failures": self.failures,
            "consecutive_failures": self.consecutive_failures,
            "last_success_at_utc_ns": self.last_success_at_utc_ns,
            "last_error_type": self.last_error_type,
            "next_retry_at_utc_ns": self.next_retry_at_utc_ns,
        }


class UsdMRestCooldown:
    """One process-local no-request gate for USD-M public REST."""

    FALLBACK_429_SECONDS = 60.0
    FALLBACK_418_SECONDS = 24.0 * 60.0 * 60.0
    MAX_418_SECONDS = 3.0 * 24.0 * 60.0 * 60.0

    def __init__(
        self,
        *,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._utc_clock_ns = utc_clock_ns
        self._monotonic_clock = monotonic_clock
        self._until_monotonic = 0.0
        self._until_utc_ns: int | None = None
        self._reason: str | None = None
        self._status: int | None = None
        self._strikes_418 = 0
        self._changed = asyncio.Event()

    @property
    def retry_at_utc_ns(self) -> int | None:
        return self._until_utc_ns

    @property
    def status(self) -> int | None:
        return self._status

    @property
    def reason(self) -> str | None:
        return self._reason

    def _deadline(self) -> float:
        return self._until_monotonic

    def install(
        self,
        *,
        status: int,
        retry_after_seconds: float | None = None,
        retry_at_utc_ns: int | None = None,
    ) -> tuple[float, int, str]:
        now_mono = self._monotonic_clock()
        now_utc = self._utc_clock_ns()
        if status == 418:
            self._strikes_418 += 1
            fallback = min(
                self.MAX_418_SECONDS,
                self.FALLBACK_418_SECONDS * (2 ** min(self._strikes_418 - 1, 2)),
            )
        else:
            self._strikes_418 = 0
            fallback = self.FALLBACK_429_SECONDS
        if retry_at_utc_ns is not None:
            duration = max(0.0, (retry_at_utc_ns - now_utc) / 1_000_000_000)
            reason = "retry_after"
        elif retry_after_seconds is not None and retry_after_seconds >= 0:
            duration = retry_after_seconds
            reason = "retry_after"
        else:
            duration = fallback
            reason = "fallback"
        candidate_mono = now_mono + duration
        candidate_utc = now_utc + int(duration * 1_000_000_000)
        if candidate_mono >= self._until_monotonic:
            self._until_monotonic = candidate_mono
            self._until_utc_ns = max(self._until_utc_ns or 0, candidate_utc)
            self._reason = reason
            self._status = status
        else:
            candidate_mono = self._until_monotonic
            candidate_utc = self._until_utc_ns or candidate_utc
        self._changed.set()
        return candidate_mono, candidate_utc, reason

    def observe_success(self) -> None:
        if self._monotonic_clock() >= self._until_monotonic:
            self._strikes_418 = 0
            self._until_utc_ns = None
            self._reason = None
            self._status = 200

    async def wait(self, stop: asyncio.Event) -> None:
        while True:
            delay = self._deadline() - self._monotonic_clock()
            if delay <= 0:
                return
            stop_task = asyncio.create_task(stop.wait())
            changed_task = asyncio.create_task(self._changed.wait())
            try:
                done, _ = await asyncio.wait(
                    (stop_task, changed_task),
                    timeout=delay,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    return
                if stop_task in done and stop.is_set():
                    return
                self._changed.clear()
            finally:
                for task in (stop_task, changed_task):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(stop_task, changed_task, return_exceptions=True)


class SideDataExtension(Protocol):
    """One side-data task owned by the supervisor.

    ``terminal_on_failure`` marks transport-integrity extensions (WebSocket
    collectors): an exception escaping such a task means the old writer and
    connection cannot be proven safely reconciled, so the supervisor must
    fail closed and never silently open a replacement connection (INV-015,
    M21.4.11-R4). REST pollers are stateless per request and keep the
    retryable default.
    """

    terminal_on_failure: bool = False

    async def run(self, stop: asyncio.Event) -> None: ...


class SideWebSocketExtension:
    """Fail-closed transport wrapper around ``UsdMStreamCollector``.

    The collector handles its own network reconnect boundaries internally
    with durable gap evidence. Any exception that escapes it is a terminal
    integrity/storage failure: the supervisor must not restart the task,
    because a replacement WebSocket could receive frames without a durable
    reconnect boundary covering the old connection's storage state.
    """

    terminal_on_failure = True

    def __init__(self, collector: UsdMStreamCollector) -> None:
        self.collector = collector

    async def run(self, stop: asyncio.Event) -> None:
        await self.collector.run(stop)


class RestSideDataPoller:
    """Stateless REST poller: retryable by the supervisor (no transport).

    Each request mints its own connection_id; there is no WebSocket
    continuity to protect, so ``terminal_on_failure`` stays False.
    """

    terminal_on_failure = False

    def __init__(
        self,
        *,
        kind: RestSideDataKind,
        symbol: str,
        interval_seconds: float,
        spool: StreamSpool,
        stats: SideDataStats,
        collector_instance_id: str,
        collector_version: str,
        logger: logging.Logger,
        catalog: Catalog,
        rest_api: UsdMSideRestApi | None = None,
        timeout_ms: int = 10_000,
        request_lock: asyncio.Lock | None = None,
        cooldown: UsdMRestCooldown | None = None,
        catchup_batch_limit: int = 500,
        catchup_batches_per_attempt: int = 2,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        cursor_observer: Callable[[str, dict[str, object]], None] | None = None,
    ) -> None:
        if not symbol or spool.symbol != symbol:
            raise ValueError("USD-M side-data symbol must match its spool")
        self.kind = kind
        self.symbol = symbol
        self.interval_seconds = interval_seconds
        self.spool = spool
        self.stats = stats
        self.collector_instance_id = collector_instance_id
        self.collector_version = collector_version
        self.logger = logger
        self.catalog = catalog
        self.rest_api = rest_api
        self.timeout_ms = timeout_ms
        self.request_lock = (
            request_lock if request_lock is not None else asyncio.Lock()
        )
        self.cooldown = (
            cooldown
            if cooldown is not None
            else UsdMRestCooldown(utc_clock_ns=utc_clock_ns)
        )
        if not 1 <= catchup_batch_limit <= 500:
            raise ValueError("USD-M catch-up batch limit must be between 1 and 500")
        if catchup_batches_per_attempt < 1:
            raise ValueError("USD-M catch-up batches per attempt must be positive")
        self.catchup_batch_limit = catchup_batch_limit
        self.catchup_batches_per_attempt = catchup_batches_per_attempt
        self.utc_clock_ns = utc_clock_ns
        self.cursor_observer = cursor_observer

    async def run(self, stop: asyncio.Event) -> None:
        self._active_stop = stop
        try:
            while not stop.is_set():
                caught_up = True
                try:
                    if self.kind in FIVE_MINUTE_KINDS:
                        caught_up = await self._catch_up_five_minute(stop)
                    else:
                        await self._capture_and_persist()
                except (BinanceSdkError, RuntimeError, OSError, TimeoutError, ValueError) as exc:
                    caught_up = False
                    self.stats.observe_failure(type(exc).__name__)
                    rate_limit = self._rate_limit_details(exc)
                    if rate_limit is not None:
                        _, retry_at_utc_ns, reason = self.cooldown.install(
                            status=rate_limit[0],
                            retry_after_seconds=rate_limit[1],
                            retry_at_utc_ns=rate_limit[2],
                        )
                        self.stats.next_retry_at_utc_ns = retry_at_utc_ns
                        log_event(
                            self.logger,
                            logging.WARNING,
                            "usdm_rest_shared_cooldown",
                            "USD-M REST shared rate-limit cooldown entered or extended",
                            stream=self.kind.value,
                            status=rate_limit[0],
                            cooldown_deadline_utc_ns=retry_at_utc_ns,
                            reason_source=reason,
                        )
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
                    delay = (
                        self.interval_seconds
                        if caught_up
                        else min(self.interval_seconds, 5.0)
                    )
                    if self.cooldown.retry_at_utc_ns is not None:
                        delay = max(
                            delay,
                            (self.cooldown.retry_at_utc_ns - time.time_ns())
                            / 1_000_000_000,
                        )
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    continue
        finally:
            await asyncio.to_thread(self.spool.close_and_seal)

    async def _capture_and_persist(self) -> EventEnvelope:
        envelope = await self._request()
        await self._persist(envelope)
        self.stats.accepted += 1
        self.stats.observe_success()
        self.cooldown.observe_success()
        return envelope

    @staticmethod
    def _rate_limit_details(
        exc: BaseException,
    ) -> tuple[int, float | None, int | None] | None:
        if isinstance(exc, UsdMSideDataHttpError) and exc.rate_limited:
            return exc.status, exc.retry_after_seconds, exc.retry_at_utc_ns
        if isinstance(exc, RateLimitBanError):
            return 418, None, None
        if isinstance(exc, TooManyRequestsError):
            return 429, None, None
        return None

    async def _request(
        self,
        *,
        period_start_ms: int | None = None,
        period_end_ms: int | None = None,
        period_limit: int = 1,
    ) -> EventEnvelope:
        stop = getattr(self, "_active_stop", None)
        if stop is None:
            stop = asyncio.Event()
        await self.cooldown.wait(stop)
        if stop.is_set():
            raise asyncio.CancelledError
        async with self.request_lock:
            await self.cooldown.wait(stop)
            if stop.is_set():
                raise asyncio.CancelledError
            envelope = await asyncio.to_thread(
                capture_rest_side_data,
                kind=self.kind,
                symbol=self.symbol,
                rest_api=self.rest_api,
                collector_instance_id=self.collector_instance_id,
                collector_version=self.collector_version,
                timeout_ms=self.timeout_ms,
                period_start_ms=period_start_ms,
                period_end_ms=period_end_ms,
                period_limit=period_limit,
            )
        return envelope

    async def _persist(self, envelope: EventEnvelope) -> None:
        self.spool.enqueue(envelope)
        await asyncio.to_thread(self.spool.drain_all)
        await asyncio.to_thread(self.spool.sync)

    async def _catch_up_five_minute(self, stop: asyncio.Event) -> bool:
        retention_name, retention_ms = FIVE_MINUTE_RETENTION[self.kind]
        now_ms = self.utc_clock_ns() // 1_000_000
        last_closed = (
            now_ms // FIVE_MINUTE_PERIOD_MS
        ) * FIVE_MINUTE_PERIOD_MS - FIVE_MINUTE_PERIOD_MS
        if last_closed < 0:
            return True
        # The public statistics routes enforce retention against request time,
        # not merely against the aligned period boundary. Starting exactly one
        # nominal retention window behind ``last_closed`` can therefore be a
        # few minutes too old while the current period is in progress. Keep one
        # additional fully closed period inside the published window.
        earliest_recoverable = max(
            0, last_closed - retention_ms + 2 * FIVE_MINUTE_PERIOD_MS
        )
        cursor = self.catalog.side_data_cursor(self.kind.value, self.symbol)
        if cursor is not None:
            persisted_value = cursor["last_persisted_period_timestamp"]
            if not isinstance(persisted_value, int):
                raise RuntimeError("side-data cursor timestamp is not an integer")
            next_period = persisted_value + FIVE_MINUTE_PERIOD_MS
        else:
            unresolved_empty_starts = [
                evidence["requested_start_timestamp"]
                for event in self.catalog.operational_events(
                    event_type="SIDE_DATA_EMPTY_RESPONSE"
                )
                if (
                    isinstance((evidence := event.get("evidence")), dict)
                    and evidence.get("kind") == self.kind.value
                    and evidence.get("symbol") == self.symbol
                    and isinstance(
                        evidence.get("requested_start_timestamp"), int
                    )
                )
            ]
            next_period = (
                min(unresolved_empty_starts)
                if unresolved_empty_starts
                else earliest_recoverable
            )
        if next_period < earliest_recoverable:
            gap_end = earliest_recoverable - FIVE_MINUTE_PERIOD_MS
            self.catalog.record_operational_event(
                event_id=(
                    f"side-data-unrecoverable-gap:{self.kind.value}:"
                    f"{self.symbol}:{next_period}:{gap_end}"
                ),
                event_type="SIDE_DATA_UNRECOVERABLE_GAP",
                occurred_at_utc_ns=self.utc_clock_ns(),
                    evidence={
                        "kind": self.kind.value,
                        "symbol": self.symbol,
                        "gap_start_timestamp": next_period,
                        "gap_end_timestamp": gap_end,
                        "source_retention_window": retention_name,
                        "retention_window_ms": retention_ms,
                    },
                    symbol=self.symbol,
                )
            next_period = earliest_recoverable
        for _batch in range(self.catchup_batches_per_attempt):
            if stop.is_set() or next_period > last_closed:
                return next_period > last_closed
            remaining = (
                (last_closed - next_period) // FIVE_MINUTE_PERIOD_MS
            ) + 1
            requested_count = min(self.catchup_batch_limit, remaining)
            request_end = (
                next_period
                + requested_count * FIVE_MINUTE_PERIOD_MS
                - 1
            )
            envelope = await self._request(
                period_start_ms=next_period,
                period_end_ms=request_end,
                period_limit=requested_count,
            )
            await self._persist(envelope)
            record_count = int(envelope.source_sequence["requestedRecordCount"])
            if record_count == 0:
                self.catalog.record_operational_event(
                    event_id=(
                        f"side-data-empty-response:{self.kind.value}:"
                        f"{self.symbol}:{next_period}"
                    ),
                    event_type="SIDE_DATA_EMPTY_RESPONSE",
                    occurred_at_utc_ns=envelope.receive_time_utc_ns,
                    evidence={
                        "kind": self.kind.value,
                        "symbol": self.symbol,
                        "requested_start_timestamp": next_period,
                        "requested_end_timestamp": request_end,
                        "source_retention_window": retention_name,
                    },
                    symbol=self.symbol,
                )
                raise RuntimeError("EMPTY_RESPONSE")
            last_timestamp = int(
                envelope.source_sequence["lastRequestedTimestamp"]
            )
            updated_at_utc_ns = envelope.receive_time_utc_ns
            self.catalog.advance_side_data_cursor(
                kind=self.kind.value,
                symbol=self.symbol,
                last_persisted_period_timestamp=last_timestamp,
                updated_at_utc_ns=updated_at_utc_ns,
                source_retention_window=retention_name,
                retention_window_ms=retention_ms,
            )
            if self.cursor_observer is not None:
                self.cursor_observer(
                    self.kind.value,
                    {
                        "kind": self.kind.value,
                        "symbol": self.symbol,
                        "last_persisted_period_timestamp": last_timestamp,
                        "updated_at_utc_ns": updated_at_utc_ns,
                        "source_retention_window": retention_name,
                        "retention_window_ms": retention_ms,
                    },
                )
            self.stats.accepted += 1
            self.stats.observe_success()
            self.cooldown.observe_success()
            next_period = last_timestamp + FIVE_MINUTE_PERIOD_MS
        return next_period > last_closed


class SideDataSupervisor:
    """Restart terminal side-task failures without setting the core stop event."""

    def __init__(
        self,
        factories: Mapping[str, Callable[[], SideDataExtension] | SideDataExtension],
        stats: dict[str, SideDataStats],
        logger: logging.Logger,
        *,
        retry_initial_seconds: float = 1.0,
        retry_maximum_seconds: float = 60.0,
    ) -> None:
        self.factories = {
            name: (
                candidate
                if callable(candidate)
                else (lambda extension=candidate: extension)
            )
            for name, candidate in factories.items()
        }
        self.stats = stats
        self.logger = logger
        self.failures: dict[str, BaseException] = {}
        self.retry_initial_seconds = retry_initial_seconds
        self.retry_maximum_seconds = retry_maximum_seconds

    async def _run_one(
        self, name: str, factory: Callable[[], SideDataExtension], stop: asyncio.Event
    ) -> None:
        stats = self.stats[name]
        while not stop.is_set():
            stats.attempts += 1
            stats.running = True
            stats.status = "RUNNING"
            extension = factory()
            try:
                await extension.run(stop)
                if not stop.is_set():
                    raise RuntimeError("side-data task returned before service stop")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.failures[name] = exc
                stats.observe_failure(type(exc).__name__)
                if getattr(extension, "terminal_on_failure", False):
                    # Transport-integrity task: the old WebSocket and writer
                    # cannot be proven safely reconciled, so a replacement
                    # connection must not receive frames without a durable
                    # reconnect boundary. Fail closed: the side stream stays
                    # FAILED (recoverable only by a service restart that runs
                    # startup recovery) while the core continues (INV-015).
                    stats.status = "FAILED"
                    log_event(
                        self.logger,
                        logging.CRITICAL,
                        "usdm_side_task_terminal",
                        "USD-M side-data transport task failed closed; "
                        "no automatic reconnect without a durable boundary",
                        stream=name,
                        error_type=type(exc).__name__,
                        attempts=stats.attempts,
                        outcome="FAILED",
                    )
                    break
                delay = min(
                    self.retry_maximum_seconds,
                    self.retry_initial_seconds
                    * (2 ** min(stats.consecutive_failures - 1, 16)),
                )
                delay = random.uniform(0.0, delay)
                stats.next_retry_at_utc_ns = time.time_ns() + int(
                    delay * 1_000_000_000
                )
                log_event(
                    self.logger,
                    logging.ERROR,
                    "usdm_side_task_retry",
                    "USD-M side-data task stopped; retry scheduled while core remains active",
                    stream=name,
                    error_type=type(exc).__name__,
                    consecutive_failures=stats.consecutive_failures,
                    retry_seconds=delay,
                )
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                except TimeoutError:
                    continue
            finally:
                stats.running = False
        if stop.is_set():
            stats.status = "STOPPED"

    async def run(self, stop: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._run_one(name, factory, stop))
            for name, factory in self.factories.items()
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
        symbol: str,
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
        metrics: MetricsRecorder | None = None,
        request_lock: asyncio.Lock | None = None,
        cooldown: UsdMRestCooldown | None = None,
    ) -> None:
        if not symbol:
            raise ValueError("USD-M side-data symbol must be non-empty")
        enabled = {
            **{kind.value: settings.rest_enabled(kind) for kind in REST_SIDE_DATA_SPECS},
            **{
                spec.stream.value: settings.stream_enabled(spec.stream)
                for spec in USDM_SIDE_STREAMS
            },
        }
        rest_intervals = {
            kind.value: settings.rest_interval(kind)
            for kind in REST_SIDE_DATA_SPECS
        }
        self.stats = {
            name: SideDataStats(
                is_enabled,
                expected_interval_seconds=rest_intervals.get(name),
            )
            for name, is_enabled in enabled.items()
        }
        self.degraded_after_seconds = settings.degraded_after_seconds
        self.catalog = catalog
        self.cursor_state = {
            kind.value: catalog.side_data_cursor(kind.value, symbol)
            for kind in FIVE_MINUTE_KINDS
        }

        def observe_cursor(kind: str, cursor: dict[str, object]) -> None:
            self.cursor_state[kind] = dict(cursor)

        def spool(stream: str) -> StreamSpool:
            def observe_event(
                envelope: EventEnvelope, frame_bytes: int, queue_depth: int
            ) -> None:
                if metrics is not None:
                    metrics.safely_observe_written(
                        envelope, raw_frame_bytes=frame_bytes, queue_depth=queue_depth
                    )

            def observe_operation(name: str, duration: int) -> None:
                if metrics is not None:
                    metrics.safely_observe_operation(
                        market="um_perpetual",
                        stream=stream,
                        name=name,
                        duration_ns=duration,
                    )

            def observe_seal(manifest: dict[str, object]) -> None:
                if metrics is not None:
                    metrics.safely_observe_seal(manifest)

            return StreamSpool(
                layout=layout,
                catalog=catalog,
                market="um_perpetual",
                symbol=symbol,
                stream=stream,
                collector_instance_id=collector_instance_id,
                collector_version=collector_version,
                queue_capacity=queue_capacity,
                rotation=rotation,
                durability_interval_seconds=durability_interval_seconds,
                max_frame_bytes=max_frame_bytes,
                event_observer=None if metrics is None else observe_event,
                operation_observer=None if metrics is None else observe_operation,
                seal_observer=None if metrics is None else observe_seal,
            )

        def lifecycle_observer(stream: str) -> Callable[[str], None] | None:
            if metrics is None:
                return None

            def observe(event: str) -> None:
                if event in {"connected", "disconnected"}:
                    return
                metrics.safely_observe_lifecycle(
                    market="um_perpetual", stream=stream, event=event
                )

            return observe

        factories: dict[str, Callable[[], SideDataExtension]] = {}
        self.rest_request_lock = (
            request_lock if request_lock is not None else asyncio.Lock()
        )
        self.rest_cooldown = (
            cooldown if cooldown is not None else UsdMRestCooldown()
        )
        for spec in USDM_SIDE_STREAMS:
            if not settings.stream_enabled(spec.stream):
                continue
            stream_stats = self.stats[spec.stream.value]

            def stream_factory(
                stream_spec: UsdMSideStreamSpec = spec,
                stats: SideDataStats = stream_stats,
            ) -> SideDataExtension:
                return SideWebSocketExtension(
                    UsdMStreamCollector(
                        stream=stream_spec.stream.value,
                        symbol=symbol,
                        route=stream_spec.route,
                        wire_name=stream_spec.wire_name,
                        spool=spool(stream_spec.stream.value),
                        collector_instance_id=collector_instance_id,
                        collector_version=collector_version,
                        logger=logger,
                        receipt_queue_capacity=receipt_queue_capacity,
                        planned_rotation_seconds=planned_rotation_seconds,
                        opener=websocket_opener,
                        envelope_factory=partial(
                            envelope_from_side_stream_frame, stream=stream_spec.stream
                        ),
                        envelope_observer=stats.observe_envelope,
                        failure_observer=stats.observe_failure,
                        lifecycle_observer=lifecycle_observer(
                            stream_spec.stream.value
                        ),
                    )
                )

            factories[spec.stream.value] = stream_factory
        for kind in REST_SIDE_DATA_SPECS:
            if not settings.rest_enabled(kind):
                continue

            def rest_factory(
                rest_kind: RestSideDataKind = kind,
            ) -> SideDataExtension:
                return RestSideDataPoller(
                    kind=rest_kind,
                    symbol=symbol,
                    interval_seconds=settings.rest_interval(rest_kind),
                    spool=spool(rest_kind.value),
                    stats=self.stats[rest_kind.value],
                    collector_instance_id=collector_instance_id,
                    collector_version=collector_version,
                    logger=logger,
                    catalog=catalog,
                    rest_api=rest_api,
                    timeout_ms=rest_timeout_ms,
                    request_lock=self.rest_request_lock,
                    cooldown=self.rest_cooldown,
                    cursor_observer=observe_cursor,
                )

            factories[kind.value] = rest_factory
        self.supervisor = SideDataSupervisor(
            factories,
            self.stats,
            logger,
            retry_initial_seconds=settings.retry_initial_seconds,
            retry_maximum_seconds=settings.retry_maximum_seconds,
        )

    async def run(self, stop: asyncio.Event) -> None:
        await self.supervisor.run(stop)

    def status(self) -> dict[str, dict[str, object]]:
        result = {
            name: stats.public_dict(
                degraded_after_seconds=self.degraded_after_seconds
            )
            for name, stats in sorted(self.stats.items())
        }
        for kind in FIVE_MINUTE_KINDS:
            result[kind.value]["cursor"] = self.cursor_state[kind.value]
        return result
