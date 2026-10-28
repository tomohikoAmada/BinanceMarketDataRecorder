"""Selected-asset capture using the existing bounded ingress, Raw and Catalog.

One product owner per ProductKey, plus explicit shared-scope owners. Polling is
process-owned and concurrency/weight bounded; no worker is created per poll.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from functools import partial
from typing import Any
from uuid import uuid4

from websockets.asyncio.client import connect
from websockets.exceptions import InvalidStatus

from ..binance.products import TERMINAL_STATUSES, ProductDirectory
from ..binance.public_rest import PublicRestClient, RestObservation, capture_public_response
from ..binance.public_sources import Source, bar_period_seconds, directory_source, source_plan
from ..binance.public_websocket import (
    DepthSequenceObserver,
    StreamGroup,
    envelope_for_group,
    logical_envelope,
    stream_groups,
)
from ..binance.recent_trades import RecentTradesCompactor
from ..binance.request_budget import observe_websocket_rejection, shared_request_budget
from ..binance.usdm.websocket import UsdMStreamCollector
from ..binance.websocket_common import run_owned_blocking_call
from ..config import RecorderConfig
from ..domain.event import EventEnvelope
from ..domain.product import Product, ProductKey
from ..spool.stream import StreamSpool
from ..spool.writer import RotationPolicy
from ..storage.catalog import Catalog
from ..storage.layout import StorageLayout
from ..supervisor.depth_continuity import DepthContinuity
from ..supervisor.readiness import CORE_STREAMS, CollectorReadiness, ReadinessSnapshot


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
    recent_trades: RecentTradesCompactor | None = None
    sequence_reliable: bool | None = None
    completed_deliveries: set[int] = field(default_factory=set)

    def public_dict(self, now: int) -> dict[str, object]:
        source = self.source
        deadline = (self.last_success or self.first_due) + int((source.cadence_seconds + 10) * 1e9)
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
            "success_deadline_utc_ns": deadline,
            "cadence_seconds": source.cadence_seconds,
            "priority": source.priority,
            "data_freshness_evaluated": False,
            "continuity_qualified": False,
            "sequence_reliable": self.sequence_reliable,
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
        self.groups: list[UsdMStreamCollector] = []
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
        return {
            "/".join(state.source.key): state.public_dict(time.time_ns())
            for state in self.source_states.values()
        }

    def persisted(self, envelope: EventEnvelope) -> None:
        stream = envelope.source_sequence.get("sourceStream", envelope.stream)
        logical = (
            logical_envelope(envelope)
            if stream in CORE_STREAMS or stream == "rpi_depth"
            else envelope
        )
        state = self.source_states.get(stream) if isinstance(stream, str) else None
        if state is not None:
            state.count += 1
            if "malformed" not in logical.capture_flags:
                state.last_success = logical.receive_time_utc_ns
            state.error = (
                "MalformedWebSocketPayload" if "malformed" in logical.capture_flags else None
            )
        if logical.stream in CORE_STREAMS:
            self.readiness.observe_persisted(logical)
            if (
                logical.stream == "diff_depth"
                and not self.readiness.snapshot().orderbook_synchronized
            ):
                if self.snapshot is not None:
                    self.readiness.observe_snapshot_persisted(self.snapshot)
                if not self.readiness.snapshot().orderbook_synchronized:
                    self.owner.request_snapshot(self.product)
        if logical.stream == "rpi_depth":
            if "malformed" in logical.capture_flags:
                self.rpi.invalidate(logical.receive_time_utc_ns)
            else:
                self.rpi.offer(logical.model_copy(update={"stream": "diff_depth"}))
            if self.rpi.bootstrap_buffer_overflowed:
                self.rpi = DepthContinuity(
                    self.product.key.market,
                    capacity=self.owner.config.ingress_queue_capacity,
                    audit_observer=None,
                )
                self.rpi_snapshot = None
            if not self.rpi.is_reliable and self.rpi_snapshot is not None:
                self.rpi.synchronize(self.rpi_snapshot)
            if not self.rpi.is_reliable:
                self.owner.request_snapshot(self.product, rpi=True)
            if state is not None:
                state.sequence_reliable = self.rpi.is_reliable
        if logical.source_sequence.get("sourceStream") == "server_shutdown":
            self.owner.request_snapshot(self.product)

    def snapshot_persisted(self, envelope: EventEnvelope) -> None:
        if envelope.stream == "rpi_depth_snapshot":
            self.rpi_snapshot = envelope.model_copy(update={"stream": "depth_snapshot"})
            self.rpi.synchronize(self.rpi_snapshot)
            return
        self.snapshot = logical_envelope(envelope)
        self.readiness.observe_snapshot_persisted(self.snapshot)

    def lifecycle(self, group: StreamGroup, event: str) -> None:
        for source in group.sources:
            state = self.owner.sources[source.key]
            state.connected = event == "connected"
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
                tasks.create_task(collector.run(stop))
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
    ) -> None:
        assert config.coverage is not None
        self.config, self.logger, self.version, self.instance_id = (
            config,
            logger,
            version,
            instance_id,
        )
        self.layout, self.catalog = layout, catalog
        self.coverage = config.coverage
        self.directory = ProductDirectory(self.coverage)
        self.clients = clients or {
            v: PublicRestClient(v, config.proxy_policy()) for v in ("spot", "usdm", "coinm")
        }
        self.collectors: dict[ProductKey, ProductCapture] = {}
        self.failures: dict[ProductKey, BaseException] = {}
        self.sources: dict[tuple[str, str, str, str], SourceState] = {}
        self.spools: dict[tuple[str, str, str], StreamSpool] = {}
        self._spool_locks: dict[tuple[str, str, str], asyncio.Lock] = {}
        self.product_tasks: dict[ProductKey, asyncio.Task[None]] = {}
        self.product_stops: dict[ProductKey, asyncio.Event] = {}
        self.shared_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self.shared_stops: dict[tuple[str, str], asyncio.Event] = {}
        self.shared_members: dict[tuple[str, str], tuple[tuple[str, str, str, str], ...]] = {}
        self.admission: dict[ProductKey, str] = {}
        self.polls: dict[tuple[str, str, str, str], asyncio.Task[RestObservation]] = {}
        self._changed = asyncio.Event()
        self._stop: asyncio.Event | None = None
        self._discovery_lock = asyncio.Lock()
        self._wire_started: set[tuple[str, str, str, str]] = set()
        self._snapshot_after: dict[tuple[str, str, str, str], float] = {}
        for venue in ("spot", "usdm", "coinm"):
            self.add_source(directory_source(venue, self.coverage))

    def add_source(self, source: Source) -> None:
        if source.key not in self.sources:
            self.sources[source.key] = SourceState(
                source,
                time.time_ns(),
                recent_trades=RecentTradesCompactor()
                if source.family == "D06" and self.coverage.recent_trades_compact
                else None,
            )
        else:
            if source.family == "D31":
                source = replace(
                    source,
                    products=tuple(
                        sorted(set(source.products + self.sources[source.key].source.products))
                    ),
                )
            self.sources[source.key].source = source
        self._changed.set()

    def spool(self, market: str, symbol: str, stream: str) -> StreamSpool:
        key = market, symbol, stream
        if key not in self.spools:
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
            )
            self._spool_locks[key] = asyncio.Lock()
        return self.spools[key]

    async def persist_rest(self, envelope: EventEnvelope) -> None:
        sequence = {**envelope.source_sequence, "sourceStream": envelope.stream}
        grouped = envelope.model_copy(
            update={"stream": "rest_observations", "source_sequence": sequence}
        )
        spool = self.spool(grouped.market, grouped.symbol, grouped.stream)
        async with self._spool_locks[(grouped.market, grouped.symbol, grouped.stream)]:

            def write() -> None:
                spool.enqueue(grouped)
                spool.drain_all()

            await run_owned_blocking_call(write)
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
            self._snapshot_after[key] = when + 1
            self._changed.set()

    def make_group(
        self,
        group: StreamGroup,
        product: Product | None,
        observer: Callable[[EventEnvelope], None],
        lifecycle: Callable[[str], None],
    ) -> UsdMStreamCollector:
        source = group.sources[0]
        market = product.key.market if product else source.market
        symbol = product.key.symbol if product else source.symbol
        policy = self.config.proxy_policy()

        from collections.abc import AsyncIterator
        from contextlib import asynccontextmanager

        from ..binance.usdm.websocket import WebSocketConnection

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

        def capture(**kwargs: Any) -> EventEnvelope:
            return sequence.observe(envelope_for_group(group, product, **kwargs))

        collector = UsdMStreamCollector(
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
        return collector

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
        if stream == "contract_info":
            # Lifecycle events trigger an authoritative directory refresh; never guess rules.
            for venue in ("usdm", "coinm"):
                self.sources[(venue, "__GLOBAL__", "exchange_info", "rest")].next_due = 0
            self._changed.set()

    def _shared_lifecycle(self, group: StreamGroup, event: str) -> None:
        for source in group.sources:
            self.sources[source.key].connected = event == "connected"

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
                steady = (
                    sum(
                        s.weight * 60 / s.cadence_seconds
                        for s in plan
                        if s.transport == "rest"
                        and s.venue_market != "spot"
                        and s.cadence_seconds > 0
                        and s.lifecycle != "after_delivery"
                    )
                    + 120 / self.coverage.directory_interval_seconds
                )
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
            current_plan = source_plan(
                tuple(c.product for c in self.collectors.values()), self.coverage
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
                        tasks.create_task(collector.run(child_stop))
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

    async def poll_once(self, state: SourceState) -> RestObservation:
        source = state.source
        await shared_request_budget().acquire(source.charges)
        state.last_attempt = time.time_ns()
        self._wire_started.add(source.key)
        result = await run_owned_blocking_call(
            capture_public_response,
            source,
            self.clients[source.venue_market].request,
            collector_instance_id=self.instance_id,
            collector_version=self.version,
            parameters=self._parameters(source),
            recent_trades=state.recent_trades,
        )
        # A known rejection closes new admission before any storage await.
        # Existing wire attempts retain their normal completion/drain boundary.
        if result.error in {"TooManyRequestsError", "RateLimitBanError"} or result.status in {
            418,
            429,
        }:
            from ..binance.spot.rate_limit import _retry_at_utc_ns

            saved = json.loads(result.envelope.raw_payload)
            received = result.envelope.receive_time_utc_ns
            until = _retry_at_utc_ns(
                status=result.status or (418 if result.error == "RateLimitBanError" else 429),
                headers=saved["response"]["headers"],
                body_text=json.dumps(result.document),
                now_utc_ns=received,
            )
            shared_request_budget().block(
                "spot" if source.venue_market == "spot" else "usdm",
                max(0.0, (until - time.time_ns()) / 1e9),
            )
        # Persistence exceptions propagate; they are never reclassified as network retries.
        await self.persist_rest(result.envelope)
        state.count += 1
        state.error = result.error
        if result.valid:
            previous_success = state.last_success
            state.last_success = result.envelope.receive_time_utc_ns
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
                if rows:
                    latest_close = max(row[6] for row in rows)
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
                    state.next_due = time.monotonic() + 60
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
            if source.stream == "exchange_info":
                await self.reconcile(source.venue_market, result.document)
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
            "sources": [s.public_dict(time.time_ns()) for s in self.sources.values()]
            if include_sources
            else [],
            "config": self.coverage.model_dump(mode="json"),
            "continuity_qualified": False,
        }

    async def run(self, stop: asyncio.Event) -> None:
        self._stop = stop
        try:
            while not stop.is_set():
                self._changed.clear()
                for key, task in list(self.polls.items()):
                    if task.done():
                        task.result()
                        self._wire_started.discard(key)
                        del self.polls[key]
                for product_key, product_task in self.product_tasks.items():
                    if product_task.done() and not self.product_stops[product_key].is_set():
                        product_task.result()
                        raise RuntimeError("product capture returned before retirement")
                for shared_task in self.shared_tasks.values():
                    if shared_task.done():
                        shared_task.result()
                        raise RuntimeError("shared capture returned before stop")
                due = sorted(
                    (
                        s
                        for k, s in self.sources.items()
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
                    self.polls[state.source.key] = asyncio.create_task(self.poll_once(state))
                # Idle REST spools need the same periodic sync/rotation as active ones.
                for spool_key, spool in tuple(self.spools.items()):
                    if (
                        spool_key[2] == "rest_observations"
                        and not self._spool_locks[spool_key].locked()
                    ):
                        async with self._spool_locks[spool_key]:
                            await run_owned_blocking_call(spool.drain_all)
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=0.1)
        finally:
            for child_stop in (*self.product_stops.values(), *self.shared_stops.values()):
                child_stop.set()
            for source_key, poll_task in self.polls.items():
                if source_key not in self._wire_started:
                    poll_task.cancel()
            results = await asyncio.gather(
                *self.product_tasks.values(),
                *self.shared_tasks.values(),
                *self.polls.values(),
                return_exceptions=True,
            )
            cleanup_error: BaseException | None = None
            for spool in self.spools.values():
                try:
                    await run_owned_blocking_call(spool.close_and_seal)
                except BaseException as exc:
                    cleanup_error = exc
            for client in self.clients.values():
                client.close()
            for result in results:
                if isinstance(result, BaseException) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    cleanup_error = result
            if cleanup_error is not None:
                raise cleanup_error
