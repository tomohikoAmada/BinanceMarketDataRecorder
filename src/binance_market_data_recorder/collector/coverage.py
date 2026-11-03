"""Selected-asset capture using the existing bounded ingress, Raw and Catalog.

One product owner per ProductKey, plus explicit shared-scope owners. Polling is
process-owned and concurrency/weight bounded; no worker is created per poll.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import ChainMap
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from functools import partial
from typing import Any, cast
from uuid import uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from ..binance.coverage_config import legacy_symbols_to_coverage_config
from ..binance.products import TERMINAL_STATUSES, ProductDirectory
from ..binance.public_rest import PublicRestClient, RestObservation, capture_public_response
from ..binance.public_sources import (
    Source,
    bar_period_seconds,
    directory_source,
    nominal_derivative_weight_per_minute,
    source_plan,
    source_selection_report,
)
from ..binance.public_websocket import (
    CombinedWebSocketStream,
    DepthSequenceObserver,
    StreamGroup,
    envelope_for_group,
    logical_envelope,
    stream_groups,
)
from ..binance.recent_trades import RecentTradesCompactor
from ..binance.request_budget import observe_websocket_rejection, shared_request_budget
from ..binance.websocket_common import run_owned_blocking_call
from ..config import RecorderConfig
from ..domain.event import EventEnvelope
from ..domain.product import Product, ProductKey
from ..metrics.recorder import MetricsRecorder
from ..orderbook.reconstructor import SynchronizeResult
from ..spool.stream import StreamSpool
from ..spool.writer import RotationPolicy
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from ..supervisor.depth_continuity import DepthContinuity
from ..supervisor.readiness import CORE_STREAMS, CollectorReadiness, ReadinessSnapshot
from .availability import AvailabilityJournal
from .explicit import explicit_source_plan
from .supervisor import CoreMarketTerminalFailure

AVAILABILITY_FLUSH_MAX_BATCHES = 32
AVAILABILITY_FLUSH_SECONDS = 0.1


@dataclass(slots=True)
class SourceState:
    source: Source
    first_due: int
    next_due: float = 0
    last_success: int | None = None
    last_attempt: int | None = None
    error: str | None = None
    count: int = 0
    connected: bool = False
    failures: int = 0
    dormant: bool = False
    response_deadline_utc_ns: int | None = None
    publication_status: str = "NOT_APPLICABLE"
    publication_time_ms: int | None = None
    recent_trades: RecentTradesCompactor | None = None
    sequence_reliable: bool | None = None
    completed_deliveries: set[int] = field(default_factory=set)
    availability: dict[str, object] = field(default_factory=dict)
    forming_filtered_count: int = 0
    last_closed_open_time_ms: int | None = None
    first_kline_open_time_ms: int | None = None
    backfill_start_ms: int | None = None
    backfill_end_ms: int = 0
    backfill_due: float = 0
    next_period_ms: int | None = None
    period_limit: int = 499
    degraded_after_seconds: float | None = None

    def public_dict(self, now: int) -> dict[str, object]:
        source = self.source
        deadline = self.response_deadline_utc_ns or (
            (self.last_success or self.first_due) + int((source.cadence_seconds + 10) * 1e9)
        )
        if self.degraded_after_seconds is not None:
            deadline = (self.last_success or self.first_due) + int(
                (self.degraded_after_seconds + source.cadence_seconds) * 1e9
            )
        status = (
            "DORMANT_LIFECYCLE"
            if self.dormant
            else ("CONNECTED" if self.connected else "CONNECTING")
            if source.transport == "ws"
            else (
                "FAILED"
                if self.error
                else "PENDING"
                if self.last_success is None
                else "STALE"
                if source.cadence_seconds and now > deadline
                else "VALID"
            )
        )
        return {
            "enabled": not self.dormant,
            "family": source.family,
            "scope": source.symbol,
            "stream": source.stream,
            "status": status,
            "last_success_utc_ns": self.last_success,
            "last_attempt_utc_ns": self.last_attempt,
            "error_type": self.error,
            "observations": self.count,
            "forming_filtered_count": self.forming_filtered_count,
            "last_closed_open_time_ms": self.last_closed_open_time_ms,
            "backfill_start_ms": self.backfill_start_ms,
            "backfill_end_ms": self.backfill_end_ms if self.backfill_start_ms is not None else None,
            "success_deadline_utc_ns": deadline,
            "cadence_seconds": source.cadence_seconds,
            "priority": source.priority,
            "data_freshness_evaluated": source.lifecycle in {"closed_bar", "after_delivery"}
            or source.endpoint.startswith("/futures/data/"),
            "publication_status": self.publication_status,
            "publication_time_ms": self.publication_time_ms,
            "continuity_qualified": False,
            "sequence_reliable": self.sequence_reliable,
            "availability": self.availability,
            "consecutive_failures": self.failures,
        }


class ProductCapture:
    def __init__(self, owner: SelectedAssetCapture, product: Product) -> None:
        self.owner = owner
        self.product = product
        self.readiness = CollectorReadiness(
            market=product.key.market,
            symbol=product.key.symbol,
            collector_instance_id=owner.instance_id,
            collector_version=owner.version,
            bootstrap_buffer_capacity=owner.config.ingress_queue_capacity,
            bootstrap_overflow_observer=self._reset_depth,
        )
        self.snapshot: EventEnvelope | None = None
        self.rpi_snapshot: EventEnvelope | None = None
        self.rpi = DepthContinuity(
            product.key.market, capacity=owner.config.ingress_queue_capacity, audit_observer=None
        )
        self.groups: list[CombinedWebSocketStream] = []
        self.source_states = {
            s.source.stream: s
            for s in owner.sources.values()
            if s.source.market == product.key.market and s.source.symbol == product.key.symbol
        }

    def _reset_depth(self) -> None:
        self.snapshot = None
        self.readiness.invalidate_depth(time.time_ns())
        self.owner.request_snapshot(self.product)

    def readiness_snapshot(self) -> ReadinessSnapshot:
        return self.readiness.snapshot()

    def side_data_status(self) -> dict[str, dict[str, object]]:
        if self.owner.config.coverage is None:
            return {
                name: state.public_dict(time.time_ns())
                for name, state in self.source_states.items()
                if name not in {"diff_depth", "agg_trade", "book_ticker", "depth_snapshot"}
                and not (self.product.venue_market == "usdm"
                         and name in {"funding_info", "exchange_info"})
            }
        return {
            "/".join(state.source.key): state.public_dict(time.time_ns())
            for state in self.source_states.values()
        }

    def persisted(self, envelope: EventEnvelope) -> None:
        stream = envelope.source_sequence.get("sourceStream", envelope.stream)
        state = self.source_states.get(stream) if isinstance(stream, str) else None
        if state is not None:
            state.count += 1
            if "malformed" not in envelope.capture_flags:
                state.last_success = envelope.receive_time_utc_ns
            state.error = (
                "MalformedWebSocketPayload" if "malformed" in envelope.capture_flags else None
            )
        if state is not None and state.error is not None:
            self.owner.availability.failure(
                "/".join(state.source.key), envelope.receive_time_utc_ns, state.error
            )
        if isinstance(stream, str) and stream in CORE_STREAMS:
            self.readiness.observe_persisted(envelope)
            if stream == "diff_depth" and not self.readiness.snapshot().orderbook_synchronized:
                if self.snapshot is not None:
                    result = self.readiness.observe_snapshot_persisted(self.snapshot)
                    if result in {
                        SynchronizeResult.SNAPSHOT_TOO_OLD, SynchronizeResult.SEQUENCE_GAP,
                    }:
                        self.snapshot = None
                # A snapshot ahead of persisted WS events needs those events,
                # not a continuously newer REST target that they can never reach.
                if self.snapshot is None:
                    self.owner.request_snapshot(self.product)
            if (
                stream == "diff_depth"
                and state is not None
                and not self.readiness.snapshot().orderbook_synchronized
            ):
                self.owner.availability.unavailable(
                    "/".join(state.source.key),
                    envelope.receive_time_utc_ns,
                    "DEPTH_RESYNC_PENDING",
                )
        if stream == "rpi_depth":
            if "malformed" in envelope.capture_flags:
                self.rpi.invalidate(envelope.receive_time_utc_ns)
            else:
                self.rpi.offer(envelope)
            # Observe the known fault before a cached/new snapshot can bridge it.
            if not self.rpi.is_reliable and state is not None:
                self.owner.availability.unavailable(
                    "/".join(state.source.key), envelope.receive_time_utc_ns, "DEPTH_RESYNC_PENDING"
                )
            if self.rpi.bootstrap_buffer_overflowed:
                self.rpi = DepthContinuity(
                    self.product.key.market,
                    capacity=self.owner.config.ingress_queue_capacity,
                    audit_observer=None,
                )
                self.rpi_snapshot = None
            if not self.rpi.is_reliable and self.rpi_snapshot is not None:
                result = self.rpi.synchronize(self.rpi_snapshot)
                if result in {
                    SynchronizeResult.SNAPSHOT_TOO_OLD, SynchronizeResult.SEQUENCE_GAP,
                }:
                    self.rpi_snapshot = None
            if not self.rpi.is_reliable and self.rpi_snapshot is None:
                self.owner.request_snapshot(self.product, rpi=True)
            self._observe_rpi_health(envelope.receive_time_utc_ns)
        if envelope.source_sequence.get("sourceStream") == "server_shutdown":
            self.owner.request_snapshot(self.product)

    def snapshot_persisted(self, envelope: EventEnvelope) -> None:
        if envelope.stream == "rpi_depth_snapshot":
            self.rpi_snapshot = envelope.model_copy(update={"stream": "depth_snapshot"})
            self.rpi.synchronize(self.rpi_snapshot)
            self._observe_rpi_health(envelope.receive_time_utc_ns)
            return
        self.snapshot = logical_envelope(envelope)
        self.readiness.observe_snapshot_persisted(self.snapshot)

    def _observe_rpi_health(self, received_at: int) -> None:
        state = self.source_states.get("rpi_depth")
        if state is None:
            return
        state.sequence_reliable = self.rpi.is_reliable
        # A bridge repairs sequence continuity, not other source unavailability.
        if self.rpi.is_reliable and not state.dormant:
            key = "/".join(state.source.key)
            now = time.time_ns()
            # A frame received before a clock reversal may persist afterwards.
            # Its receive time cannot end uncertainty in the current wall clock.
            if now < self.owner._clock_unreliable_until:
                return
            completed_at = max(received_at, now)
            health = self.owner.availability.sources.get(key)
            if health is not None and (
                completed_at < health.observed_at or health.unavailable_since is None
            ):
                return
            # Do not scan writer groups for every healthy persisted frame.
            stalled = self.owner._writer_stalls(completed_at)
            reason, since = self.owner._ws_unavailability(state, completed_at, stalled)
            if reason is None:
                self.owner.availability.valid(key, completed_at)
            else:
                self.owner.availability.unavailable(key, completed_at, reason, since=since)

    def lifecycle(self, group: StreamGroup, event: str) -> None:
        for source in group.sources:
            state = self.owner.sources[source.key]
            state.connected = event == "connected"
            if event != "connected":
                self.owner.availability.failure("/".join(source.key), time.time_ns(), event)
            else:
                self.owner._check_reconnect_kline_backfill(source, state)
            if source.stream in CORE_STREAMS:
                if event == "connected":
                    self.readiness.observe_connected(source.stream)
                else:
                    self.readiness.observe_disconnected(source.stream)
            if source.stream == "diff_depth" and event != "connected":
                self.snapshot = None
                # Do not reuse a pre-disconnect bridge or retain a reliable stale sequence.
                self.readiness.invalidate_depth(time.time_ns())
                self.owner.request_snapshot(self.product)
            if source.stream == "rpi_depth" and event != "connected":
                self.rpi_snapshot = None
                self.rpi.invalidate(time.time_ns())
                state.sequence_reliable = False
                self.owner.request_snapshot(self.product, rpi=True)

    async def run(self, stop: asyncio.Event) -> None:
        sources = tuple(
            s.source
            for s in self.owner.sources.values()
            if s.source.symbol == self.product.key.symbol
            and s.source.market == self.product.key.market
            and s.source.transport == "ws"
        )
        async with asyncio.TaskGroup() as tasks:
            for group in stream_groups(sources, self.owner.coverage.streams_per_connection):
                collector = self.owner.make_group(
                    group, self.product, self.persisted, partial(self.lifecycle, group)
                )
                self.groups.append(collector)
                tasks.create_task(self.owner.run_group(collector, stop))
            await stop.wait()


class SelectedAssetCapture:
    def __init__(
        self,
        config: RecorderConfig,
        logger: logging.Logger,
        version: str,
        instance_id: str,
        layout: StorageLayout,
        catalog: Catalog,
        *,
        clients: dict[str, Any] | None = None,
        terminal_failure_observer: Callable[[ProductKey, BaseException], None] | None = None,
    ) -> None:
        self.config, self.logger, self.version, self.instance_id = (
            config,
            logger,
            version,
            instance_id,
        )
        self.layout, self.catalog = layout, catalog
        self._empty_stats_starts: dict[tuple[str, str], int] = {}
        if config.coverage is None:
            for row in catalog.operational_events(event_type="SIDE_DATA_EMPTY_RESPONSE"):
                evidence = cast(dict[str, Any], row["evidence"])
                identity = str(evidence["kind"]), str(evidence["symbol"])
                start = int(evidence["requested_start_timestamp"])
                self._empty_stats_starts[identity] = min(
                    start, self._empty_stats_starts.get(identity, start)
                )
        self.metrics = MetricsRecorder(
            catalog=catalog,
            data_root=layout.root,
            collector_instance_id=instance_id,
            logger=logger,
        )
        self._last_metrics_flush = time.monotonic()
        self.coverage = config.coverage or legacy_symbols_to_coverage_config(config)
        self.terminal_failure_observer = terminal_failure_observer
        self.selection_started_utc_ns = time.time_ns()
        self._source_selection = source_selection_report((), self.coverage, ())
        self.directory = ProductDirectory(self.coverage)
        self.collectors: dict[ProductKey, ProductCapture] = {}
        self.failures: dict[ProductKey, BaseException] = {}
        self._active_sources: dict[tuple[str, str, str, str], SourceState] = {}
        self._completed_seed_sources: dict[tuple[str, str, str, str], SourceState] = {}
        self.sources: ChainMap[tuple[str, str, str, str], SourceState] = ChainMap(
            self._active_sources, self._completed_seed_sources
        )
        self.spools: dict[tuple[str, str, str], StreamSpool] = {}
        self._spool_locks: dict[tuple[str, str, str], asyncio.Lock] = {}
        self.product_tasks: dict[ProductKey, asyncio.Task[None]] = {}
        self.product_stops: dict[ProductKey, asyncio.Event] = {}
        self.shared_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self.shared_stops: dict[tuple[str, str], asyncio.Event] = {}
        self.shared_members: dict[tuple[str, str], tuple[tuple[str, str, str, str], ...]] = {}
        self.admission: dict[ProductKey, str] = {}
        self.polls: dict[tuple[str, str, str, str], asyncio.Task[RestObservation | None]] = {}
        self._changed = asyncio.Event()
        self._stop: asyncio.Event | None = None
        self._discovery_lock = asyncio.Lock()
        self.availability = AvailabilityJournal(instance_id, selection_id=self.source_selection_id)
        self._pending_watermark_utc_ns: int | None = None
        self._last_watermark_flush_utc_ns: int | None = None
        self._last_monitor_utc_ns: int | None = None
        self._clock_unreliable_until: int = 0
        self._group_sources: dict[CombinedWebSocketStream, StreamGroup] = {}
        self._wire_started: set[tuple[str, str, str, str]] = set()
        self._snapshot_after: dict[tuple[str, str, str, str], float] = {}
        self._rest_drain_waiters: dict[tuple[str, str, str], list[asyncio.Future[None]]] = {}
        self._rest_drain_in_progress: set[tuple[str, str, str]] = set()
        if config.coverage is None:
            products, sources = explicit_source_plan(config)
            for source in sources:
                self.add_source(source)
            for product in products:
                key = product.key
                self.directory.products[key] = product
                self.collectors[key] = ProductCapture(self, product)
                self.admission[key] = "ADMITTED"
            self._source_selection = {
                **source_selection_report(products, self.coverage, sources),
                "policy": "explicit_symbols",
                "directory_source_count": sum(s.stream == "exchange_info" for s in sources),
            }
        else:
            for venue in ("spot", "usdm", "coinm"):
                self.add_source(directory_source(venue, self.coverage))
        venues = {state.source.venue_market for state in self.sources.values()}
        self.clients: dict[str, Any] = clients if clients is not None else {
            v: PublicRestClient(v, config.proxy_policy()) for v in sorted(venues)
        }

    @property
    def source_selection_id(self) -> str:
        assets = ",".join(sorted(self.coverage.underlying_assets))
        # A new owner must not extend health for sources left by an older run.
        return f"assets:{assets}:{self.instance_id}"

    def add_source(self, source: Source) -> None:
        if source.key in self.sources:
            state = self.sources[source.key]
            if source.family == "D31":
                source = replace(
                    source, products=tuple(sorted(set(source.products + state.source.products)))
                )
            state.source = source
        else:
            state = SourceState(
                source,
                time.time_ns(),
                recent_trades=RecentTradesCompactor()
                if source.family == "D06" and self.coverage.recent_trades_compact
                else None,
            )
            self.sources[source.key] = state
            if self.config.coverage is None and source.stream not in CORE_STREAMS:
                state.degraded_after_seconds = self.config.side_degraded_after_seconds
                if source.stream.endswith("_5m"):
                    cursor = self.catalog.side_data_cursor(source.stream, source.symbol)
                    saved_period = (
                        cursor["last_persisted_period_timestamp"] if cursor is not None
                        else self._empty_stats_starts.get((source.stream, source.symbol))
                    )
                    if saved_period is not None:
                        # Re-evaluate the saved period once, including sparse OI.
                        assert isinstance(saved_period, int)
                        if cursor is not None and source.family != "D15":
                            saved_period += 300_000
                        state.next_period_ms = saved_period
                    last_closed = time.time_ns() // 1_000_000 // 300_000 * 300_000 - 300_000
                    earliest = max(0, last_closed - 30 * 86400_000 + 600_000)
                    if saved_period is None:
                        state.next_period_ms = earliest
                    else:
                        if saved_period < earliest:
                            self.catalog.record_operational_event(
                                event_id=f"side-data-retention:{self.instance_id}:{source.key}",
                                event_type="SIDE_DATA_UNRECOVERABLE_GAP",
                                occurred_at_utc_ns=time.time_ns(),
                                evidence={
                                    "kind": source.stream, "symbol": source.symbol,
                                    "gap_start_timestamp": saved_period,
                                    "gap_end_timestamp": earliest - 300_000,
                                    "source_retention_window": "latest_1_month"
                                    if source.family == "D15" else "latest_30_days",
                                    "retention_window_ms": 30 * 86400_000,
                                },
                                symbol=source.symbol,
                            )
                            state.next_period_ms = earliest
                    assert state.next_period_ms is not None
                    if state.next_period_ms > last_closed:
                        state.next_due = time.monotonic() + (
                            state.next_period_ms - last_closed
                        ) / 1000
        if source.key not in self._completed_seed_sources:
            self._active_sources[source.key] = state
        self._changed.set()

    def spool(self, market: str, symbol: str, stream: str) -> StreamSpool:
        key = market, symbol, stream
        if key not in self.spools:

            def observe_event(event: EventEnvelope, frame_bytes: int, queue_depth: int) -> None:
                # Aggregate the physical Raw stream once; per-source health remains
                # in the availability ledger. Do not parse combined JSON again.
                self.metrics.safely_observe_written(
                    event,
                    raw_frame_bytes=frame_bytes,
                    queue_depth=queue_depth,
                )

            def observe_operation(name: str, duration: int) -> None:
                self.metrics.safely_observe_operation(
                    market=market,
                    stream=stream,
                    name=name,
                    duration_ns=duration,
                )

            def observe_seal(manifest: dict[str, object]) -> None:
                self.metrics.safely_observe_seal(manifest)

            self.spools[key] = StreamSpool(
                layout=self.layout,
                catalog=self.catalog,
                market=market,
                symbol=symbol,
                stream=stream,
                collector_instance_id=self.instance_id,
                collector_version=self.version,
                queue_capacity=self.config.ingress_queue_capacity,
                rotation=RotationPolicy(self.config.rotation_seconds, self.config.rotation_bytes),
                durability_interval_seconds=self.config.durability_interval_seconds,
                max_frame_bytes=self.config.max_frame_bytes,
                event_observer=observe_event,
                operation_observer=observe_operation,
                seal_observer=observe_seal,
            )
            self._spool_locks[key] = asyncio.Lock()
        return self.spools[key]

    async def persist_rest(self, envelope: EventEnvelope) -> None:
        sequence = {**envelope.source_sequence, "sourceStream": envelope.stream}
        grouped = envelope.model_copy(
            update={"stream": "rest_observations", "source_sequence": sequence}
        )
        spool_key = (grouped.market, grouped.symbol, grouped.stream)
        spool = self.spool(*spool_key)
        spool.enqueue(grouped)

        def drain_batch() -> None:
            spool.drain_all()
            if self.config.coverage is None:
                progress = spool.persistence_progress()
                if progress.synchronized_frames < progress.written_frames:
                    spool.sync()

        if spool_key not in self._rest_drain_in_progress:
            self._rest_drain_in_progress.add(spool_key)
            try:
                while True:
                    batch_waiters = self._rest_drain_waiters.pop(spool_key, [])
                    try:
                        async with self._spool_locks[spool_key]:
                            await run_owned_blocking_call(drain_batch)
                        for w in batch_waiters:
                            if not w.done():
                                w.set_result(None)
                    except BaseException as exc:
                        for w in batch_waiters:
                            if not w.done():
                                w.set_exception(exc)
                        for remaining in self._rest_drain_waiters.pop(spool_key, []):
                            if not remaining.done():
                                remaining.set_exception(exc)
                        raise
                    if spool_key not in self._rest_drain_waiters:
                        break
            finally:
                self._rest_drain_in_progress.discard(spool_key)
        else:
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            self._rest_drain_waiters.setdefault(spool_key, []).append(waiter)
            await waiter

        source_stream = envelope.stream
        if (
            source_stream in {"depth_snapshot", "rpi_depth_snapshot"}
            and "malformed" not in envelope.capture_flags
        ):
            product = self.collectors.get(ProductKey(envelope.market, envelope.symbol))
            if product is not None:
                product.snapshot_persisted(envelope)

    def request_snapshot(self, product: Product, *, rpi: bool = False) -> None:
        key = (
            product.venue_market,
            product.key.symbol,
            "rpi_depth_snapshot" if rpi else "depth_snapshot",
            "rest",
        )
        state = self.sources.get(key)
        if state is not None:
            # Bound repeated stale-snapshot repair requests; retain sequence failure meanwhile.
            when = max(time.monotonic(), self._snapshot_after.get(key, 0))
            if state.next_due <= when or (
                state.error is not None and state.next_due != float("inf")
            ):
                return
            state.next_due = when
            self._completed_seed_sources.pop(key, None)
            self._active_sources[key] = state
            self._snapshot_after[key] = when + 1
            self._changed.set()

    def make_group(
        self,
        group: StreamGroup,
        product: Product | None,
        observer: Callable[[EventEnvelope], None],
        lifecycle: Callable[[str], None],
    ) -> CombinedWebSocketStream:
        source = group.sources[0]
        market = product.key.market if product else source.market
        symbol = product.key.symbol if product else source.symbol
        policy = self.config.proxy_policy()

        from collections.abc import AsyncIterator
        from contextlib import asynccontextmanager

        from ..binance.public_websocket import WebSocketConnection

        @asynccontextmanager
        async def opener(url: str) -> AsyncIterator[WebSocketConnection]:
            pool = "spot" if source.venue_market == "spot" else "usdm"
            await shared_request_budget().acquire({pool + "_connections": 1})
            try:
                async with connect(
                    url,
                    proxy=policy.websocket_proxy(group.root),
                    compression=None,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=(4, 1),
                    max_size=self.config.max_frame_bytes,
                    close_timeout=10,
                ) as ws:
                    yield ws
            except InvalidStatus as exc:
                observe_websocket_rejection(pool, exc.response.status_code, exc.response.headers)
                raise

        def observed(envelope: EventEnvelope) -> None:
            observer(envelope)
            if envelope.source_sequence.get("sourceStream") == "server_shutdown":
                collector.request_rotation()

        sequence = DepthSequenceObserver()

        def capture(**kwargs: Any) -> EventEnvelope | None:
            envelope = sequence.observe(envelope_for_group(group, product, **kwargs))
            if (
                "klineClosed" in envelope.source_sequence
                and "malformed" not in envelope.capture_flags
            ):
                state = self._find_source_state_for_envelope(group, envelope)
                is_closed = envelope.source_sequence["klineClosed"] == 1
                t = int(envelope.source_sequence["openTime"])
                if state is not None:
                    if state.first_kline_open_time_ms is None:
                        state.first_kline_open_time_ms = t
                    if is_closed:
                        interval = self._extract_kline_interval(state.source)
                        expected_t = state.first_kline_open_time_ms
                        if interval and state.last_closed_open_time_ms is not None:
                            expected_t = self._next_kline_open_time_ms(
                                state.last_closed_open_time_ms,
                                interval,
                                utc8=state.source.family == "S12",
                            )
                        if interval and expected_t is not None and t > expected_t:
                            self._schedule_kline_backfill(state.source, expected_t, t - 1, interval)
                        state.last_closed_open_time_ms = max(state.last_closed_open_time_ms or 0, t)
                if not is_closed and self.coverage.kline_forming_policy == "closed_only":
                    if state is not None:
                        state.last_success = envelope.receive_time_utc_ns
                        state.error = None
                        state.forming_filtered_count += 1
                        state.count += 1
                        now = envelope.receive_time_utc_ns
                        key = "/".join(state.source.key)
                        if now >= self._clock_unreliable_until:
                            self.availability.valid(key, now, checkpoint=False)
                        state.availability = self.availability.ensure(key, now).public_dict(now)
                    return None
            return envelope

        collector = CombinedWebSocketStream(
            market=market,
            stream=group.name,
            symbol=symbol,
            route=group.route,
            wire_name=group.wire_names[0],
            combined_streams=group.wire_names,
            websocket_root=group.root,
            spool=self.spool(market, symbol, group.name),
            collector_instance_id=self.instance_id,
            collector_version=self.version,
            logger=self.logger,
            receipt_queue_capacity=self.config.ingress_queue_capacity,
            opener=opener,
            envelope_factory=capture,
            envelope_observer=observed,
            lifecycle_observer=lifecycle,
        )
        self._group_sources[collector] = group
        return collector

    def _find_source_state_for_envelope(
        self, group: StreamGroup, envelope: EventEnvelope
    ) -> SourceState | None:
        wire = envelope.source_sequence.get("wireStream")
        if isinstance(wire, str) and wire in group.by_wire:
            source = group.by_wire[wire]
            return self.sources.get(source.key)
        stream = envelope.source_sequence.get("sourceStream")
        if isinstance(stream, str):
            for s in group.sources:
                if s.stream == stream:
                    return self.sources.get(s.key)
        return None

    @staticmethod
    def _extract_kline_interval(source: Source) -> str | None:
        import re

        match = re.search(r"(?:kline_|price_kline_)([0-9]+[smhdMwy])(?:_utc8)?$", source.stream)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def _next_kline_open_time_ms(open_time_ms: int, interval: str, *, utc8: bool = False) -> int:
        if interval != "1M":
            period = 1 if interval == "1s" else bar_period_seconds(interval)
            return open_time_ms + period * 1000
        from datetime import UTC, datetime, timedelta

        # UTC+8 monthly opens fall at 16:00 on the previous UTC date.
        offset = timedelta(hours=8 if utc8 else 0)
        opened = datetime.fromtimestamp(open_time_ms / 1000, UTC) + offset
        following = (
            opened.replace(
                year=opened.year + (opened.month == 12), month=opened.month % 12 + 1, day=1
            )
            - offset
        )
        return int(following.timestamp() * 1000)

    def _schedule_kline_backfill(
        self, source: Source, start_time_ms: int, end_time_ms: int, interval: str
    ) -> None:
        if start_time_ms > end_time_ms:
            return
        state = self.sources[source.key]
        state.backfill_start_ms = min(
            state.backfill_start_ms if state.backfill_start_ms is not None else start_time_ms,
            start_time_ms,
        )
        state.backfill_end_ms = max(state.backfill_end_ms, end_time_ms)

    async def _run_kline_backfill(self, state: SourceState) -> RestObservation:
        source = state.source
        interval = self._extract_kline_interval(source)
        assert interval is not None and state.backfill_start_ms is not None
        start = state.backfill_start_ms
        # One small page per scheduler turn; normal live polls have priority.
        end = start
        page_size = (
            5
            if interval == "1M"
            else min(100, 200 * 86400 // (1 if interval == "1s" else bar_period_seconds(interval)))
        )
        for _ in range(page_size - 1):
            following = self._next_kline_open_time_ms(end, interval, utc8=source.family == "S12")
            if following > state.backfill_end_ms:
                break
            end = following
        end = self._next_kline_open_time_ms(end, interval, utc8=source.family == "S12") - 1
        end = min(end, state.backfill_end_ms)
        rest_source = self._build_kline_backfill_source(source, interval, start, end)
        await shared_request_budget().acquire(rest_source.charges)
        self._wire_started.add(source.key)
        result = await run_owned_blocking_call(
            capture_public_response,
            rest_source,
            self.clients[source.venue_market].request,
            collector_instance_id=self.instance_id,
            collector_version=self.version,
            parameters=dict(rest_source.parameters),
            additional_capture_flags=("closed_bar_backfill",),
        )
        self._observe_rest_rejection(source, result)
        # Keep failure evidence too. Storage failures propagate to the supervisor.
        await self.persist_rest(result.envelope)
        cursor = start
        latest_open = start
        if result.valid:
            for row in result.document:
                if row[0] != cursor or row[6] >= result.envelope.receive_time_utc_ns // 1_000_000:
                    break
                latest_open = cursor
                cursor = self._next_kline_open_time_ms(
                    cursor, interval, utc8=source.family == "S12"
                )
        if cursor > start:
            state.last_closed_open_time_ms = max(state.last_closed_open_time_ms or 0, latest_open)
            state.backfill_start_ms = cursor if cursor <= state.backfill_end_ms else None
        state.backfill_due = time.monotonic() + (5 if cursor == start else 0)
        return result

    def _build_kline_backfill_source(
        self, source: Source, interval: str, start_time_ms: int, end_time_ms: int
    ) -> Source:
        from ..binance.public_sources import REST_PREFIX

        limit = 100

        venue = source.venue_market
        prefix = REST_PREFIX[venue]
        params: dict[str, Any] = {
            "interval": interval,
            "startTime": start_time_ms,
            "endTime": end_time_ms,
            "limit": limit,
        }
        if venue == "spot":
            params["symbol"] = source.symbol
            if source.family == "S12":
                params["timeZone"] = "8"
            endpoint = prefix + "/klines"
        else:
            if source.stream.startswith("continuous_kline_"):
                pair = source.symbol.split("_", 1)[0] if "_" in source.symbol else source.symbol
                contract_type = (
                    source.symbol.split("_", 1)[1] if "_" in source.symbol else "PERPETUAL"
                )
                params["pair"] = pair
                params["contractType"] = contract_type
                endpoint = prefix + "/continuousKlines"
            elif source.stream.startswith("mark_price_kline_"):
                params["symbol"] = source.symbol
                endpoint = prefix + "/markPriceKlines"
            elif source.stream.startswith("index_price_kline_"):
                params["pair"] = source.symbol
                endpoint = prefix + "/indexPriceKlines"
            elif source.stream.startswith("premium_price_kline_"):
                params["symbol"] = source.symbol
                endpoint = prefix + "/premiumIndexKlines"
            else:
                params["symbol"] = source.symbol
                endpoint = prefix + "/klines"

        return Source(
            family=source.family,
            venue_market=venue,
            market=source.market,
            symbol=source.symbol,
            stream=source.stream,
            transport="rest",
            endpoint=endpoint,
            cadence_seconds=0,
            parameters=tuple(params.items()),
            weight=2,
            priority="bar_repair",
            lifecycle="closed_bar",
        )

    def _check_reconnect_kline_backfill(self, source: Source, state: SourceState) -> None:
        interval = self._extract_kline_interval(source)
        if interval is None:
            return
        start = state.first_kline_open_time_ms
        if state.last_closed_open_time_ms is not None:
            start = self._next_kline_open_time_ms(
                state.last_closed_open_time_ms, interval, utc8=source.family == "S12"
            )
        if start is None:
            return
        now_ms = time.time_ns() // 1_000_000
        end = start
        if interval != "1M":
            period = (1 if interval == "1s" else bar_period_seconds(interval)) * 1000
            end += max(0, (now_ms - start) // period) * period
        else:
            while (
                following := self._next_kline_open_time_ms(
                    end, interval, utc8=source.family == "S12"
                )
            ) <= now_ms:
                end = following
        if end > start:
            self._schedule_kline_backfill(source, start, end - 1, interval)

    async def run_group(self, collector: CombinedWebSocketStream, stop: asyncio.Event) -> None:
        try:
            await collector.run(stop)
        finally:
            self._group_sources.pop(collector, None)

    def _observe_shared(self, group: StreamGroup, envelope: EventEnvelope) -> None:
        stream = envelope.source_sequence.get("sourceStream")
        for source in group.sources:
            if stream == source.stream:
                state = self.sources[source.key]
                if "malformed" not in envelope.capture_flags:
                    state.last_success = envelope.receive_time_utc_ns
                state.error = (
                    "MalformedWebSocketPayload" if "malformed" in envelope.capture_flags else None
                )
                state.count += 1
                if state.error is not None:
                    self.availability.failure(
                        "/".join(source.key), envelope.receive_time_utc_ns, state.error
                    )
        if stream == "contract_info":
            # Lifecycle events trigger an authoritative directory refresh; never guess rules.
            for venue in ("usdm", "coinm"):
                self.sources[(venue, "__GLOBAL__", "exchange_info", "rest")].next_due = 0
            self._changed.set()

    def _shared_lifecycle(self, group: StreamGroup, event: str) -> None:
        for source in group.sources:
            self.sources[source.key].connected = event == "connected"
            if event != "connected":
                self.availability.failure("/".join(source.key), time.time_ns(), event)
            else:
                self._check_reconnect_kline_backfill(source, self.sources[source.key])

    async def reconcile(self, venue: str, document: dict[str, Any]) -> None:
        async with self._discovery_lock:
            self.directory.apply(venue, document)
            now_ms = time.time_ns() // 1_000_000
            for key, product in sorted(self.directory.products.items()):
                expired = (
                    not product.perpetual
                    and product.delivery_time_ms is not None
                    and product.delivery_time_ms <= now_ms
                )
                if key in self.collectors:
                    old_product = self.collectors[key].product
                    self.collectors[key].product = product
                    if expired or product.status in TERMINAL_STATUSES:
                        self.admission[key] = "EXPIRED" if expired else product.status
                        self.product_stops[key].set()
                        await self.product_tasks.pop(key)
                        self.collectors.pop(key)
                    elif product.status == "MISSING_FROM_DIRECTORY":
                        self.admission[key] = "MISSING_FROM_DIRECTORY"
                        continue
                    elif replace(old_product, status=product.status) != product:
                        self.product_stops[key].set()
                        await self.product_tasks.pop(key)
                        self.collectors.pop(key)
                    else:
                        self.admission[key] = "ADMITTED"
                        continue
                if expired or product.status in TERMINAL_STATUSES:
                    self.admission[key] = "EXPIRED" if expired else product.status
                    continue
                if product.status != "TRADING":
                    self.admission[key] = "INACTIVE_AT_DISCOVERY:" + product.status
                    continue
                if self._stop is not None and self._stop.is_set():
                    continue
                if len(self.collectors) >= self.coverage.max_products:
                    self.admission[key] = "NOT_ADMITTED_PRODUCT_BUDGET"
                    continue
                # Admission counts actual shared stream scope, not a guessed symbols multiplier.
                candidates = (*(c.product for c in self.collectors.values()), product)
                plan = source_plan(candidates, self.coverage)
                if sum(s.transport == "ws" for s in plan) > self.coverage.max_streams:
                    self.admission[key] = "NOT_ADMITTED_STREAM_BUDGET"
                    continue
                rest_limit = self.coverage.max_steady_derivative_weight_per_minute
                steady = nominal_derivative_weight_per_minute(plan, self.coverage)
                if rest_limit is not None and steady > rest_limit:
                    self.admission[key] = "NOT_ADMITTED_REST_BUDGET"
                    continue
                for source in plan:
                    self.add_source(source)
                self.collectors[key] = ProductCapture(self, product)
                self.admission[key] = "ADMITTED"
                self.product_stops[key] = asyncio.Event()
                self.product_tasks[key] = asyncio.create_task(
                    self.collectors[key].run(self.product_stops[key]),
                    name=f"collector:{key.market}:{key.symbol}",
                )
            admitted_products = tuple(c.product for c in self.collectors.values())
            current_plan = source_plan(admitted_products, self.coverage)
            self._source_selection = source_selection_report(
                admitted_products, self.coverage, current_plan
            )
            for source in current_plan:
                self.add_source(source)
            active_keys = {source.key for source in current_plan}
            for state in self.sources.values():
                source = state.source
                if source.lifecycle == "after_delivery":
                    state.dormant = not any(
                        p.key.symbol in source.products
                        and p.delivery_time_ms is not None
                        and p.delivery_time_ms <= now_ms
                        and p.delivery_time_ms not in state.completed_deliveries
                        for p in self.directory.products.values()
                    )
                elif source.stream != "exchange_info":
                    state.dormant = source.key not in active_keys
            await self._reconcile_shared()
            self.catalog.record_operational_event(
                event_id="product-directory:" + str(uuid4()),
                event_type="PRODUCT_DIRECTORY_OBSERVED",
                occurred_at_utc_ns=time.time_ns(),
                evidence={
                    **self.status(include_sources=False),
                    "source_plan": [state.source.public_dict() for state in self.sources.values()],
                },
            )

    async def _reconcile_shared(self) -> None:
        if self._stop is not None and self._stop.is_set():
            return
        groups: dict[tuple[str, str], list[Source]] = {}
        for state in self.sources.values():
            source = state.source
            if (
                state.dormant
                or source.transport != "ws"
                or ProductKey(source.market, source.symbol) in self.collectors
            ):
                continue
            groups.setdefault((source.market, source.symbol), []).append(source)
        for key, sources in groups.items():
            members = tuple(sorted(source.key for source in sources))
            if key in self.shared_tasks:
                if self.shared_members[key] == members:
                    continue
                self.shared_stops[key].set()
                await self.shared_tasks.pop(key)
            self.shared_members[key] = members
            stop = self.shared_stops[key] = asyncio.Event()

            async def run_shared(members: tuple[Source, ...], child_stop: asyncio.Event) -> None:
                async with asyncio.TaskGroup() as tasks:
                    for group in stream_groups(members, self.coverage.streams_per_connection):
                        collector = self.make_group(
                            group,
                            None,
                            partial(self._observe_shared, group),
                            partial(self._shared_lifecycle, group),
                        )
                        tasks.create_task(self.run_group(collector, child_stop))
                    await child_stop.wait()

            self.shared_tasks[key] = asyncio.create_task(run_shared(tuple(sources), stop))
        for key in set(self.shared_tasks) - set(groups):
            self.shared_stops[key].set()
            await self.shared_tasks.pop(key)

    def _parameters(self, source: Source) -> dict[str, Any]:
        params: dict[str, Any] = dict(source.parameters)
        if source.endpoint.startswith("/futures/data/") and source.family != "D31":
            end = (time.time_ns() // 1_000_000 // 300_000) * 300_000 - 1
            params.update(startTime=end - 600_000 + 1, endTime=end)
            if self.config.coverage is None:
                state = self.sources[source.key]
                # Resume each original 5m source independently, in bounded pages.
                # Stay one closed period inside the published 30-day retention.
                start = max(state.next_period_ms if state.next_period_ms is not None
                            else end - 299_999,
                            end - 30 * 86400_000 + 600_001)
                state.next_period_ms = start
                params.update(startTime=start,
                              endTime=min(end, start + state.period_limit * 300_000 - 1),
                              limit=500)
            if source.family == "D16":
                # Match the existing side_data_rest taker boundary policy. The
                # endpoint can include the preceding period and needs the next
                # boundary to return both requested closed periods.
                params.update(endTime=params["endTime"] + 300_000,
                              limit=500 if self.config.coverage is None else 3)
        if source.family == "D23":
            from datetime import UTC, datetime

            interval = str(params["interval"])
            now_ms = time.time_ns() // 1_000_000
            if interval == "1M":
                now = datetime.fromtimestamp(now_ms / 1000, UTC)
                params["endTime"] = (
                    int(
                        now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()
                        * 1000
                    )
                    - 1
                )
            else:
                units = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}
                params["endTime"] = now_ms - int(interval[:-1]) * units[interval[-1]]
        return params

    def _observe_rest_rejection(self, source: Source, result: RestObservation) -> None:
        if result.error in {"TooManyRequestsError", "RateLimitBanError"} or result.status in {
            418,
            429,
        }:
            from ..binance.spot.rate_limit import _retry_at_utc_ns

            received = result.envelope.receive_time_utc_ns
            status = result.status or (418 if result.error == "RateLimitBanError" else 429)
            body_text = json.dumps(result.document) if result.document is not None else ""
            try:
                until = _retry_at_utc_ns(
                    status=status,
                    headers=result.response_headers,
                    body_text=body_text,
                    now_utc_ns=received,
                )
            except (ValueError, TypeError, OverflowError):
                # A malformed directive must not skip failure persistence now
                # that admission closes first. Reuse the missing-header/ban
                # fallback, retaining the original header in the saved record.
                until = _retry_at_utc_ns(
                    status=status,
                    headers={},
                    body_text=body_text,
                    now_utc_ns=received,
                )
            shared_request_budget().block(
                "spot" if source.venue_market == "spot" else "usdm",
                max(0.0, (until - time.time_ns()) / 1e9),
            )

    async def _poll_if_ready(self, state: SourceState) -> None:
        # Other tasks may consume the budget before this task starts. Leave
        # the source due so a throttled poll cannot occupy a concurrency slot.
        if shared_request_budget().delay(state.source.charges) <= 0:
            await self.poll_once(state)

    async def poll_once(self, state: SourceState) -> RestObservation:
        source = state.source
        await shared_request_budget().acquire(source.charges)
        state.last_attempt = time.time_ns()
        eligible_since = state.first_due
        if source.lifecycle == "after_delivery":
            delivery_times = [
                p.delivery_time_ms * 1_000_000
                for p in self.directory.products.values()
                if p.key.symbol in source.products
                and p.delivery_time_ms is not None
                and p.delivery_time_ms * 1_000_000 <= state.last_attempt
                and p.delivery_time_ms not in state.completed_deliveries
            ]
            eligible_since = max(eligible_since, min(delivery_times, default=state.last_attempt))
        self.availability.ensure("/".join(source.key), eligible_since)
        self._wire_started.add(source.key)
        parameters = self._parameters(source)
        evaluated_period = (
            {
                "start_time_ms": parameters["startTime"],
                "end_time_ms": parameters["endTime"] - 300_000,
                "allowed_preceding_overlap_ms": 300_000,
            }
            if source.family == "D16"
            else None
        )
        result = await run_owned_blocking_call(
            capture_public_response,
            source,
            self.clients[source.venue_market].request,
            collector_instance_id=self.instance_id,
            collector_version=self.version,
            parameters=parameters,
            recent_trades=state.recent_trades,
            evaluated_period=evaluated_period,
        )
        self._observe_rest_rejection(source, result)
        # Persistence exceptions propagate; they are never reclassified as network retries.
        await self.persist_rest(result.envelope)
        state.count += 1
        state.error = result.error
        if result.error is not None:
            self.availability.failure(
                "/".join(source.key),
                time.time_ns(),
                result.error,
                since=state.last_attempt,
            )
        if result.valid:
            previous_success = state.last_success
            if "sequence_gap" in result.envelope.capture_flags:
                self.availability.failure(
                    "/".join(source.key),
                    result.envelope.receive_time_utc_ns,
                    "OBSERVED_SEQUENCE_GAP",
                    since=previous_success or state.first_due,
                )
            state.last_success = result.envelope.receive_time_utc_ns
            if self.config.coverage is not None:
                state.failures = 0
            state.next_due = (
                time.monotonic() + source.cadence_seconds
                if source.cadence_seconds
                else float("inf")
            )
            if source.lifecycle == "closed_bar":
                rows = result.document
                interval = str(dict(source.parameters)["interval"])
                now_ms = time.time_ns() // 1_000_000
                state.publication_status = "PENDING"
                if rows:
                    latest_close = max(row[6] for row in rows)
                    state.publication_time_ms = latest_close
                    # Query endTime names the native period requested by the existing
                    # source adapter. Do not credit an older closed bar as that period.
                    if latest_close >= parameters["endTime"]:
                        state.publication_status = "CURRENT"
                    else:
                        state.error = "DataPublicationStale"
                    if interval == "1M":
                        from datetime import UTC, datetime

                        opened = datetime.fromtimestamp((latest_close + 1) / 1000, UTC)
                        following = opened.replace(
                            year=opened.year + (opened.month == 12),
                            month=opened.month % 12 + 1,
                            day=1,
                        )
                        next_close = int(following.timestamp() * 1000)
                    else:
                        next_close = latest_close + 1 + bar_period_seconds(interval) * 1000
                    delay = (next_close - now_ms) / 1000 + 1
                    state.next_due = time.monotonic() + (max(1, delay) if delay >= 0 else 60)
                else:
                    state.error = "DataPublicationPending"
                    state.next_due = time.monotonic() + 60
            if source.endpoint.startswith("/futures/data/") and source.family != "D31":
                rows = result.document
                timestamps = (
                    [row.get("timestamp") for row in rows] if isinstance(rows, list) else []
                )
                period_start, period_end = parameters["startTime"], parameters["endTime"]
                if source.family == "D16":
                    period_end -= 300_000
                if not isinstance(rows, list):
                    state.error = "DataPeriodShapeUnknown"
                    state.publication_status = "UNKNOWN"
                elif any(
                    not isinstance(value, int)
                    or isinstance(value, bool)
                    or not (
                        period_start <= value <= period_end
                        or (source.family == "D16" and value == period_start - 300_000)
                    )
                    for value in timestamps
                ):
                    state.error = "DataOutsideRequestedPeriod"
                    state.publication_status = "STALE"
                else:
                    evaluated = [t for t in timestamps if period_start <= t <= period_end]
                    if rows and not evaluated:
                        state.error = "DataPublicationStale"
                        state.publication_status = "STALE"
                    else:
                        state.publication_status = (
                            "EMPTY_EVALUATED_QUERY_WINDOW" if not rows else "CURRENT"
                        )
                    state.publication_time_ms = max(evaluated, default=None)
                    if self.config.coverage is None and state.error is None:
                        if source.family == "D15" and len(rows) >= parameters["limit"]:
                            state.error = "DataResponseLimitReached"
                            state.period_limit = max(1, state.period_limit // 2)
                        elif source.family != "D15" and not evaluated:
                            state.error = "DataPublicationPending"
                        elif source.family != "D15" and sorted(evaluated) != list(
                            range(period_start, period_end + 1, 300_000)
                        ):
                            state.error = "DataPeriodGap"
                        else:
                            evaluated_through = period_end - 299_999
                            await run_owned_blocking_call(
                                self.catalog.advance_side_data_cursor,
                                kind=source.stream, symbol=source.symbol,
                                last_persisted_period_timestamp=evaluated_through,
                                updated_at_utc_ns=result.envelope.receive_time_utc_ns,
                                source_retention_window="latest_1_month"
                                if source.family == "D15" else "latest_30_days",
                                retention_window_ms=30 * 86400_000,
                            )
                            state.next_period_ms = evaluated_through + 300_000
                            now_ms = time.time_ns() // 1_000_000
                            if state.next_period_ms < (now_ms // 300_000) * 300_000:
                                state.next_due = time.monotonic()
                            else:
                                state.next_due = time.monotonic() + max(
                                    source.cadence_seconds,
                                    (state.next_period_ms + 300_000 - now_ms) / 1000,
                                )
                if self.config.coverage is None and source.family == "D15":
                    await run_owned_blocking_call(
                        self.catalog.record_operational_event,
                        event_id=(f"side-data-range:{source.stream}:{source.symbol}:"
                                  f"{result.envelope.receive_time_utc_ns}"),
                        event_type="SIDE_DATA_RANGE_EVALUATED",
                        occurred_at_utc_ns=result.envelope.receive_time_utc_ns,
                        evidence={
                            "kind": source.stream, "symbol": source.symbol,
                            "requested_start_timestamp": period_start,
                            "requested_end_timestamp": period_end,
                            "response_record_count": len(rows) if isinstance(rows, list) else 0,
                            "response_limit": parameters["limit"],
                            "range_complete": state.error is None,
                            "cursor_semantics": "evaluated_closed_query_windows.v1",
                            "raw_connection_id": result.envelope.connection_id,
                        },
                        symbol=source.symbol,
                    )
                if self.config.coverage is None and rows == []:
                    await run_owned_blocking_call(
                        self.catalog.record_operational_event,
                        event_id=(f"side-data-empty:{source.stream}:{source.symbol}:"
                                  f"{result.envelope.receive_time_utc_ns}"),
                        event_type="SIDE_DATA_EMPTY_RESPONSE",
                        occurred_at_utc_ns=result.envelope.receive_time_utc_ns,
                        evidence={
                            "kind": source.stream, "symbol": source.symbol,
                            "requested_start_timestamp": period_start,
                            "requested_end_timestamp": period_end,
                        },
                        symbol=source.symbol,
                    )
            if source.cadence_seconds:
                state.response_deadline_utc_ns = (
                    time.time_ns() + int((max(0, state.next_due - time.monotonic()) + 10) * 1e9)
                    if source.lifecycle == "closed_bar"
                    else result.envelope.receive_time_utc_ns
                    + int((source.cadence_seconds + 10) * 1e9)
                )
            if source.family == "D31":
                rows = result.document if isinstance(result.document, list) else [result.document]
                state.completed_deliveries.update(row["deliveryTime"] for row in rows)
                state.dormant = not any(
                    p.key.symbol in source.products
                    and p.delivery_time_ms is not None
                    and p.delivery_time_ms <= time.time_ns() // 1_000_000
                    and p.delivery_time_ms not in state.completed_deliveries
                    for p in self.directory.products.values()
                )
                if not state.dormant:
                    state.last_success = previous_success
                    state.error = "DeliveryPublicationPending"
                    state.publication_status = "PENDING"
                else:
                    state.publication_status = "CURRENT"
            completed_at = time.time_ns()
            if state.error is None and (
                state.response_deadline_utc_ns is None
                or completed_at <= state.response_deadline_utc_ns
            ):
                if completed_at >= self._clock_unreliable_until:
                    self.availability.valid("/".join(source.key), completed_at)
                if state.dormant:
                    self.availability.suspend("/".join(source.key), completed_at)
            elif state.error is not None:
                self.availability.failure(
                    "/".join(source.key),
                    completed_at,
                    state.error,
                    since=previous_success or state.first_due,
                )
            if self.config.coverage is None:
                if state.error is None:
                    state.failures = 0
                else:
                    state.failures += 1
                    state.next_due = time.monotonic() + min(60, 2 ** min(state.failures, 6))
            if source.stream == "exchange_info" and self.config.coverage is not None:
                await self.reconcile(source.venue_market, result.document)
            if source.cadence_seconds == 0 and state.error is None:
                self._active_sources.pop(source.key, None)
                self._completed_seed_sources[source.key] = state
                if "_seed_" in source.stream and isinstance(result.document, list):
                    valid_opens = [
                        row[0]
                        for row in result.document
                        if row[6] < result.envelope.receive_time_utc_ns // 1_000_000
                    ]
                    if valid_opens:
                        latest_open = max(valid_opens)
                        ws_stream = source.stream.replace(
                            "continuous_seed_", "continuous_kline_"
                        ).replace("_seed_", "_")
                        ws_key = (source.venue_market, source.symbol, ws_stream, "ws")
                        ws_state = self.sources.get(ws_key)
                        if ws_state is not None:
                            ws_state.last_closed_open_time_ms = max(
                                ws_state.last_closed_open_time_ms or 0, latest_open
                            )
        else:
            state.failures += 1
            state.next_due = time.monotonic() + min(60, 2 ** min(state.failures, 6))
        return result

    def shared_source_status(self) -> dict[str, dict[str, object]]:
        return {
            "/".join(state.source.key): state.public_dict(time.time_ns())
            for state in self.sources.values()
            if ProductKey(state.source.market, state.source.symbol) not in self.collectors
        }

    def status(self, *, include_sources: bool = True) -> dict[str, Any]:
        return {
            "schema_version": "selected-asset-coverage.v1",
            "underlying_assets": list(self.coverage.underlying_assets),
            "directories_observed": sorted(self.directory.observed_markets),
            "products": [
                {**p.public_dict(), "capture_status": self.admission.get(k, "PENDING")}
                for k, p in sorted(self.directory.products.items())
            ],
            "not_offered": self.directory.not_offered(),
            "source_selection": {
                **self._source_selection,
                "effective_from_utc_ns": self.selection_started_utc_ns,
                "collector_instance_id": self.instance_id,
            },
            "sources": [s.public_dict(time.time_ns()) for s in self.sources.values()]
            if include_sources
            else [],
            "config": self.coverage.model_dump(mode="json"),
            "continuity_qualified": False,
            "availability_journal_pending": len(self.availability.pending),
            "capture_metrics": {
                "failure_count": self.metrics.failure_count,
                "last_error_type": self.metrics.last_error_type,
                "forming_filtered_count": sum(
                    s.forming_filtered_count for s in self.sources.values()
                ),
            },
            "transport_progress": [
                {"group_id": group.name, **collector.capture_progress()}
                for collector, group in self._group_sources.items()
            ],
        }

    async def _flush_availability(self, *, all_pending: bool = False) -> None:
        deadline = time.monotonic() + AVAILABILITY_FLUSH_SECONDS
        batches = 0
        watermark_param: tuple[str, int] | None = None
        if self._pending_watermark_utc_ns is not None and (
            all_pending
            or self._last_watermark_flush_utc_ns is None
            or (
                self._pending_watermark_utc_ns - self._last_watermark_flush_utc_ns >= 10_000_000_000
            )
        ):
            watermark_param = (self.source_selection_id, self._pending_watermark_utc_ns)

        while self.availability.pending:
            # One handoff/transaction for a burst, rather than up to 128
            # round trips competing with the WS writers for the same executor.
            batch = self.availability.batch(
                4096 if len(self.availability.pending) > 2048 else 128
            )
            # A monitor-backed journal uses the shared watermark for health.
            # Standalone journal users retain ordinary per-source checkpoints.
            to_persist = (
                [entry for entry in batch if entry.get("is_transition", False)]
                if self._pending_watermark_utc_ns is not None
                else batch
            )
            current_watermark = (
                watermark_param if len(batch) == len(self.availability.pending) else None
            )
            if to_persist or current_watermark is not None:
                if current_watermark is not None:
                    await run_owned_blocking_call(
                        self.catalog.persist_source_availability,
                        to_persist,
                        watermark=current_watermark,
                    )
                    self._last_watermark_flush_utc_ns = current_watermark[1]
                    watermark_param = None
                else:
                    await run_owned_blocking_call(
                        self.catalog.persist_source_availability,
                        to_persist,
                    )
                batches += 1
            self.availability.acknowledge(len(batch))
            if not all_pending:
                if len(self.availability.pending) > 2048:
                    if batches >= 128:
                        break
                elif (
                    batches >= AVAILABILITY_FLUSH_MAX_BATCHES
                    or time.monotonic() >= deadline
                ):
                    break

        # A bounded flush may leave transitions pending. Never certify their
        # monitoring time until those transitions have committed.
        if watermark_param is not None and not self.availability.pending:
            await run_owned_blocking_call(
                self.catalog.persist_source_availability, [], watermark=watermark_param
            )
            self._last_watermark_flush_utc_ns = watermark_param[1]

    def _writer_stalls(self, now: int) -> dict[tuple[str, str, str, str], int]:
        stalled: dict[tuple[str, str, str, str], int] = {}
        for collector, group in self._group_sources.items():
            progress = collector.capture_progress()
            pending = progress["pending_since_utc_ns"]
            last_write = progress["last_write_utc_ns"]
            boundary = (
                max(pending, last_write)
                if pending is not None and last_write is not None
                else pending
            )
            if pending is not None and boundary is not None and now - boundary > 10_000_000_000:
                stalled.update((source.key, boundary) for source in group.sources)
        return stalled

    def _ws_unavailability(
        self, state: SourceState, now: int, stalled: dict[tuple[str, str, str, str], int]
    ) -> tuple[str | None, int | None]:
        """Use the same current blockers for monitor and immediate RPI recovery."""
        source = state.source
        if now < self._clock_unreliable_until:
            return "CLOCK_DISCONTINUITY", None
        if not state.connected:
            return "DISCONNECTED", None
        if source.key in stalled:
            return "WRITER_STALLED", stalled[source.key]
        if state.error is not None:
            return state.error, None
        if source.stream == "diff_depth":
            product = self.collectors.get(ProductKey(source.market, source.symbol))
            if product is None or not product.readiness_snapshot().orderbook_synchronized:
                return "DEPTH_RESYNC_PENDING", None
        elif source.stream == "rpi_depth" and state.sequence_reliable is not True:
            return "DEPTH_RESYNC_PENDING", None
        return None, None

    def _observe_availability(self, now: int) -> None:
        stalled = self._writer_stalls(now)
        previous = self._last_monitor_utc_ns
        if previous is not None and now < previous:
            self._clock_unreliable_until = max(self._clock_unreliable_until, previous)
        self._last_monitor_utc_ns = now
        for state in self._active_sources.values():
            source = state.source
            key = "/".join(source.key)
            if (
                state.dormant
                and key not in self.availability.sources
                and (
                    key not in self.availability.previous
                    or self.availability.previous[key].get("dormant")
                )
            ):
                state.availability = {"lifecycle": "DORMANT", "source_id": key}
                continue
            health = self.availability.ensure(
                key, now if source.lifecycle == "after_delivery" else state.first_due
            )
            if state.dormant:
                self.availability.suspend(key, now)
                # End eligibility explicitly; do not reinterpret earlier gaps as health.
                state.availability = {**health.public_dict(now), "lifecycle": "DORMANT"}
                continue
            self.availability.resume(key, now)
            if now < self._clock_unreliable_until:
                self.availability.unavailable(key, now, "CLOCK_DISCONTINUITY")
                state.availability = health.public_dict(now)
                continue
            if previous is not None and (now < previous or now - previous > 5_000_000_000):
                self.availability.unavailable(key, now, "MONITORING_UNKNOWN", since=previous)
            reason = None
            since = None
            if source.transport == "ws":
                reason, since = self._ws_unavailability(state, now, stalled)
            else:
                deadline = state.response_deadline_utc_ns or (
                    (state.last_success or state.first_due)
                    + int((source.cadence_seconds + 10) * 1e9)
                )
                if state.error:
                    reason = state.error
                elif state.last_success is None:
                    reason = "FIRST_RESPONSE_PENDING"
                elif source.cadence_seconds and now > deadline:
                    reason, since = "RESPONSE_DEADLINE_EXCEEDED", deadline
            if reason is not None:
                self.availability.unavailable(key, now, reason, since=since)
            else:
                self.availability.valid(key, now)
            state.availability = health.public_dict(now)
        self._pending_watermark_utc_ns = now

    async def _monitor_availability(self, stop: asyncio.Event) -> None:
        # SQLite/REST waits in the scheduling loop must not stop in-memory
        # health observations and manufacture a full-source UNKNOWN storm.
        while not stop.is_set():
            self._observe_availability(time.time_ns())
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=1.0)

    async def run(self, stop: asyncio.Event) -> None:
        self._stop = stop
        terminal_error: BaseException | None = None
        monitor: asyncio.Task[None] | None = None
        try:
            self.availability.previous = await run_owned_blocking_call(
                self.catalog.source_availability
            )
            monitor = asyncio.create_task(self._monitor_availability(stop))
            for initial_key, collector in self.collectors.items():
                if initial_key in self.product_tasks:
                    continue
                self.product_stops[initial_key] = asyncio.Event()
                self.product_tasks[initial_key] = asyncio.create_task(
                    collector.run(self.product_stops[initial_key]),
                    name=f"collector:{initial_key.market}:{initial_key.symbol}",
                )
            while not stop.is_set():
                self._changed.clear()
                if monitor.done():
                    monitor.result()
                    if stop.is_set():
                        break
                    raise RuntimeError("availability monitor returned before stop")
                await self._flush_availability()
                if time.monotonic() - self._last_metrics_flush >= 10:
                    await run_owned_blocking_call(self.metrics.safely_flush)
                    self._last_metrics_flush = time.monotonic()
                for key, task in list(self.polls.items()):
                    if task.done():
                        task.result()
                        self._wire_started.discard(key)
                        del self.polls[key]
                for product_key, product_task in self.product_tasks.items():
                    if product_task.done() and not self.product_stops[product_key].is_set():
                        try:
                            product_task.result()
                        except BaseException as exc:
                            self.failures[product_key] = exc
                        else:
                            self.failures[product_key] = RuntimeError(
                                "product capture returned before retirement"
                            )
                        failure = self.failures[product_key]
                        if self.terminal_failure_observer is not None:
                            self.terminal_failure_observer(product_key, failure)
                        raise CoreMarketTerminalFailure(
                            f"core market Collector terminated: {product_key}"
                        ) from failure
                for shared_task in self.shared_tasks.values():
                    if shared_task.done():
                        shared_task.result()
                        raise RuntimeError("shared capture returned before stop")
                due = sorted(
                    (
                        s
                        for k, s in self._active_sources.items()
                        if s.source.transport == "rest" and k not in self.polls and not s.dormant
                    ),
                    key=lambda s: s.next_due,
                )
                for state in due:
                    if (
                        len(self.polls) >= self.coverage.rest_concurrency
                        or state.next_due > time.monotonic()
                    ):
                        break
                    if shared_request_budget().delay(state.source.charges) <= 0:
                        self.polls[state.source.key] = asyncio.create_task(
                            self._poll_if_ready(state)
                        )
                for state in self._active_sources.values():
                    if len(self.polls) >= self.coverage.rest_concurrency:
                        break
                    if (
                        state.backfill_start_ms is not None
                        and state.source.key not in self.polls
                        and time.monotonic() >= state.backfill_due
                    ):
                        self.polls[state.source.key] = asyncio.create_task(
                            self._run_kline_backfill(state)
                        )
                # Cheap memory checks retain 1s durability without empty worker jobs.
                for spool_key, spool in tuple(self.spools.items()):
                    if (
                        spool_key[2] == "rest_observations"
                        and not self._spool_locks[spool_key].locked()
                        and spool_key not in self._rest_drain_in_progress
                        and spool.maintenance_due()
                    ):
                        async with self._spool_locks[spool_key]:
                            await run_owned_blocking_call(spool.drain_all)
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=0.1)
        except BaseException as exc:
            terminal_error = exc
            raise
        finally:
            for child_stop in (*self.product_stops.values(), *self.shared_stops.values()):
                child_stop.set()
            cleanup_error: BaseException | None = None
            if monitor is not None:
                monitor.cancel()
                monitor_result = (await asyncio.gather(monitor, return_exceptions=True))[0]
                if isinstance(monitor_result, BaseException) and not isinstance(
                    monitor_result, asyncio.CancelledError
                ):
                    cleanup_error = monitor_result
            stopped_at = time.time_ns()
            try:
                for state in self.sources.values():
                    if not state.dormant:
                        self.availability.unavailable(
                            "/".join(state.source.key),
                            stopped_at,
                            "SERVICE_STOPPED"
                            if terminal_error is None
                            else "SERVICE_FAILED:" + type(terminal_error).__name__,
                        )
            except BaseException as exc:
                cleanup_error = exc
            for source_key, poll_task in self.polls.items():
                if source_key not in self._wire_started:
                    poll_task.cancel()
            results = await asyncio.gather(
                *self.product_tasks.values(),
                *self.shared_tasks.values(),
                *self.polls.values(),
                return_exceptions=True,
            )
            for spool in self.spools.values():
                try:
                    await run_owned_blocking_call(spool.close_and_seal)
                except BaseException as exc:
                    cleanup_error = exc
            try:
                await self._flush_availability(all_pending=True)
                await run_owned_blocking_call(self.metrics.safely_flush)
            except BaseException as exc:
                cleanup_error = exc
            for client in self.clients.values():
                client.close()
            product_keys = tuple(self.product_tasks)
            for index, result in enumerate(results):
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    if index < len(product_keys):
                        failed_product = product_keys[index]
                        self.failures[failed_product] = result
                        if terminal_error is None:
                            if self.terminal_failure_observer is not None:
                                self.terminal_failure_observer(failed_product, result)
                            raise CoreMarketTerminalFailure(
                                f"core market Collector failed while draining: {failed_product}"
                            ) from result
                    elif cleanup_error is None:
                        cleanup_error = result
            if cleanup_error is not None and terminal_error is None:
                raise cleanup_error


CoverageRuntimeSupervisor = SelectedAssetCapture
