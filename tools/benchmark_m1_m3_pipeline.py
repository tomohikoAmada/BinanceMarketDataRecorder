"""Offline fixed-input SelectedAssetCapture baseline (M1.0).

This is a short burst benchmark, including startup and graceful shutdown. It
cannot measure steady-state rotation rates; those require M3's multi-rotation run.
Run with the project's Python 3.12 environment; no network is used.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import platform
import random
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.products import discover_products
from binance_market_data_recorder.binance.public_rest import RestObservation
from binance_market_data_recorder.binance.public_sources import (
    Source,
    bar_period_seconds,
    source_plan,
)
from binance_market_data_recorder.binance.public_websocket import (
    StreamGroup,
    stream_groups,
)
from binance_market_data_recorder.binance.request_budget import RECORDER_WINDOWS, RequestBudget
from binance_market_data_recorder.collector.availability import Availability, AvailabilityJournal
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture, SourceState
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.domain.product import Product
from binance_market_data_recorder.normalize.raw import (
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.service.resources import current_rss_bytes, peak_rss_bytes
from binance_market_data_recorder.spool.queue import BoundedEventQueue
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

if TYPE_CHECKING:
    from binance_market_data_recorder.binance.public_websocket import (
        CombinedWebSocketStream,
        WebSocketConnection,
    )


REPO = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPO / "tests/fixtures/binance/p2"
EPOCH_MS = 1767571200000  # 2026-01-05 00:00 UTC; Monday.
SourceKey = tuple[str, str, str, str]


def directory(venue: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / f"{venue}-directory.json").read_bytes())  # type: ignore[no-any-return]


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def distribution(values: list[float]) -> dict[str, int | float]:
    ordered = sorted(values)
    return {
        "count": len(values),
        "p50": round(ordered[int((len(ordered) - 1) * 0.50)], 3) if ordered else 0,
        "p99": round(ordered[int((len(ordered) - 1) * 0.99)], 3) if ordered else 0,
        "max": round(max(values, default=0), 3),
        "total": round(sum(values), 3),
    }


class StatsCollector:
    def __init__(self) -> None:
        self.latencies: dict[str, list[float]] = {
            name: []
            for name in (
                "queue_wait",
                "persist_lag",
                "append_batch",
                "write",
                "fsync",
                "seal",
                "availability_flush",
                "sql_statement",
                "sql_batch",
                "sql_transaction",
            )
        }
        self.sql_statement_count = 0
        self.pending_peak = 0
        self.peak_wal_bytes = 0
        self.peak_rss = 0
        self.seals: list[dict[str, object]] = []
        self.total_frames = 0
        self.event_sink: Callable[[EventEnvelope], None] | None = None

    def record(self, name: str, started: int) -> None:
        self.latencies[name].append((time.perf_counter_ns() - started) / 1e6)


class InstrumentedJournal(AvailabilityJournal):
    def __init__(self, instance_id: str, stats: StatsCollector) -> None:
        super().__init__(instance_id)
        self.stats = stats

    def _emit(
        self,
        state: Availability,
        gap: dict[str, object] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super()._emit(state, gap, *args, **kwargs)
        self.stats.pending_peak = max(self.stats.pending_peak, len(self.pending))


class ConnectionProxy:
    """Time SQL statements separately from successful committed transactions.

    Catalog uses isolation_level=None and explicit BEGIN/COMMIT. A standalone
    DML statement is an autocommit transaction; DML inside BEGIN is not.
    Catalog's existing lock serializes access to this connection.
    """

    def __init__(self, connection: sqlite3.Connection, stats: StatsCollector) -> None:
        self._real = connection
        self.stats = stats
        self.transaction_started: int | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    def execute(self, sql: str, *args: Any) -> sqlite3.Cursor:
        verb = sql.lstrip().split()[0].upper()
        started = time.perf_counter_ns()
        in_transaction = self._real.in_transaction
        result = self._real.execute(sql, *args)
        if verb == "BEGIN":
            self.transaction_started = started
        elif verb in {"COMMIT", "END"}:
            if self.transaction_started is not None:
                self.stats.record("sql_transaction", self.transaction_started)
            self.transaction_started = None
        elif verb == "ROLLBACK":
            self.transaction_started = None
        elif verb in {"INSERT", "UPDATE", "DELETE", "REPLACE"}:
            self.stats.sql_statement_count += 1
            self.stats.record("sql_statement", started)
            if not in_transaction:
                self.stats.record("sql_transaction", started)
        return result

    def executemany(self, sql: str, parameters: Sequence[Any]) -> sqlite3.Cursor:
        # Catalog batches metric rows inside an explicit transaction. Preserve
        # its real executemany operation and time the batch separately.
        started = time.perf_counter_ns()
        result = self._real.executemany(sql, parameters)
        self.stats.sql_statement_count += len(parameters)
        self.stats.record("sql_batch", started)
        return result


class InstrumentedCatalog(Catalog):
    def __init__(self, path: Path, stats: StatsCollector) -> None:
        super().__init__(path)
        self._connection = ConnectionProxy(self._connection, stats)  # type: ignore[assignment]


class InstrumentedQueue(BoundedEventQueue):
    def __init__(self, capacity: int, *, byte_capacity: int, stats: StatsCollector) -> None:
        super().__init__(capacity, byte_capacity=byte_capacity)
        self.stats = stats
        self.enqueued_at: dict[int, int] = {}

    def put_nowait(self, envelope: EventEnvelope) -> None:
        self.enqueued_at[id(envelope)] = time.perf_counter_ns()
        super().put_nowait(envelope)

    def get_nowait(self) -> EventEnvelope | None:
        event = super().get_nowait()
        if event is not None:
            self.stats.record("queue_wait", self.enqueued_at.pop(id(event)))
        return event


class InstrumentedWriter(RawChunkWriter):
    def __init__(self, *args: Any, stats: StatsCollector, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.stats = stats
        # Keep the real period/size cap, but give every burst stream a full first
        # period. Host uptime must not randomly split an otherwise identical run.
        self._rotation_deadline_monotonic = self._opened_monotonic + self.rotation.seconds

    def append_batch(self, envelopes: Sequence[EventEnvelope]) -> tuple[int, ...]:
        started = time.perf_counter_ns()
        try:
            return super().append_batch(envelopes)
        finally:
            self.stats.record("append_batch", started)


class Response:
    status = 200

    def __init__(self, model: Any) -> None:
        self.model = model
        self.headers = {"X-MBX-USED-WEIGHT-1M": "12"}

    def data(self) -> Any:
        return self.model


class MockRestClient:
    def __init__(self, venue: str) -> None:
        self.venue = venue

    def request(self, source: Source, params: dict[str, Any]) -> Response:
        stream = source.stream
        if stream == "exchange_info":
            return Response(directory(self.venue))
        if stream in {"depth_snapshot", "rpi_depth_snapshot"}:
            return Response(
                {"lastUpdateId": 100, "bids": [["50000", "1"]], "asks": [["50001", "1"]]}
            )
        if "interval" in params:
            end = int(params.get("endTime", EPOCH_MS - 1))
            period = bar_period_seconds(str(params["interval"])) * 1000
            return Response(
                [
                    [
                        end + 1 - period,
                        "100",
                        "101",
                        "99",
                        "100",
                        "10",
                        end,
                        "1000",
                        100,
                        "5",
                        "500",
                        "0",
                    ]
                ]
            )
        if stream == "recent_trades":
            return Response(
                [{"id": 100, "price": "100", "qty": "1", "time": EPOCH_MS, "isBuyerMaker": False}]
            )
        # Populate the existing adapter's required schema rather than feeding {}
        # into healthy paths. Fields with semantic checks keep their real types.
        row: dict[str, Any] = dict.fromkeys(source.required_fields, "1.0")
        row.update(symbol=source.symbol)
        for key in ("time", "timestamp", "updateTime", "openTime", "closeTime", "fundingTime"):
            if key in source.required_fields:
                row[key] = EPOCH_MS
        for key in ("symbols", "assets", "symbolRules", "constituents"):
            if key in source.required_fields:
                row[key] = []
        if source.endpoint.startswith("/futures/data/"):
            row["timestamp"] = params["startTime"]
        model = (
            [row]
            if source.endpoint.startswith("/futures/data/")
            or stream in {"funding_info", "funding_history"}
            else row
        )
        return Response(model)

    def close(self) -> None:
        pass


def fixture_groups(products: list[Product], config: CoverageConfig) -> tuple[StreamGroup, ...]:
    scopes: dict[tuple[str, str], list[Source]] = {}
    for source in source_plan(tuple(products), config):
        if source.transport == "ws":
            scopes.setdefault((source.market, source.symbol), []).append(source)
    return tuple(
        g
        for members in scopes.values()
        for g in stream_groups(tuple(members), config.streams_per_connection, isolate_hot=True)
    )


def bar_bounds(interval: str, ordinal: int, utc8: bool) -> tuple[int, int]:
    offset = 8 * 3600_000 if utc8 else 0
    if interval == "1M":
        opened = datetime(2026 + ordinal // 12, ordinal % 12 + 1, 1, tzinfo=UTC)
        following = (opened + timedelta(days=32)).replace(day=1)
        return int(opened.timestamp() * 1000) - offset, int(
            following.timestamp() * 1000
        ) - offset - 1
    period = bar_period_seconds(interval) * 1000
    # Weekly streams anchor on Monday; other fixed intervals use the UTC epoch.
    anchor = 4 * 86400_000 if interval.endswith("w") else 0
    opened_ms = ((EPOCH_MS + offset - anchor) // period + ordinal) * period + anchor - offset
    return opened_ms, opened_ms + period - 1


def build_fixture_frames(
    products: list[Product],
    config: CoverageConfig,
    events_per_stream: int,
    seed: int,
) -> tuple[dict[str, list[bytes]], dict[str, Any]]:
    rng = random.Random(seed)
    feed: dict[str, list[bytes]] = {}
    manifest: dict[str, Any] = {}
    by_key = {(p.key.market, p.key.symbol): p for p in products}
    for group in fixture_groups(products, config):
        frames = []
        for idx in range(events_per_stream):
            for wire in group.wire_names:
                source = group.by_wire[wire]
                product = by_key.get((source.market, source.symbol))
                s, stream, t = source.symbol, source.stream, EPOCH_MS + idx * 100
                price = str(50000 + rng.randrange(100))
                data: Any = {
                    "e": "24hrTicker",
                    "E": t,
                    "s": s,
                    "o": "49000",
                    "h": "51000",
                    "l": "48000",
                    "c": price,
                    "v": "1000",
                    "n": 10,
                }
                if stream in {"diff_depth", "rpi_depth"}:
                    data = {
                        "e": "depthUpdate",
                        "E": t,
                        "s": s,
                        "U": 100 + idx * 2,
                        "u": 101 + idx * 2,
                        "b": [[price, "1"]],
                        "a": [["50100", "1"]],
                    }
                    if source.venue_market != "spot":
                        data.update(pu=99 + idx * 2, T=t)
                elif stream == "book_ticker":
                    data = {"s": s, "u": 100 + idx, "b": price, "B": "1", "a": "50100", "A": "2"}
                    if source.venue_market != "spot":
                        data.update(e="bookTicker", E=t, T=t)
                elif stream in {"agg_trade", "trade", "block_trade"}:
                    data = {
                        "e": "aggTrade" if stream == "agg_trade" else "trade",
                        "E": t,
                        "s": s,
                        "a": 100 + idx,
                        "t": 100 + idx,
                        "p": price,
                        "q": "1",
                        "f": 100 + idx,
                        "l": 100 + idx,
                        "T": t,
                        "m": True,
                    }
                elif "kline" in stream:
                    interval = wire.rsplit("_", 1)[1].split("@")[0]
                    opened, closed = bar_bounds(interval, idx // 4, wire.endswith("@+08:00"))
                    is_closed = idx % 4 == 3
                    event = "continuous_kline" if stream.startswith("continuous_") else "kline"
                    data = {
                        "e": event,
                        "E": closed + 1 if is_closed else opened + idx % 4,
                        "k": {
                            "t": opened,
                            "T": closed,
                            "s": s,
                            "i": interval,
                            "f": 100,
                            "L": 109,
                            "o": "50000",
                            "c": price,
                            "h": "50100",
                            "l": "49900",
                            "v": "100",
                            "n": 10,
                            "x": is_closed,
                            "q": "5000000",
                            "V": "50",
                            "Q": "2500000",
                            "B": "0",
                        },
                    }
                    if event == "continuous_kline":
                        data.update(ps=s.rsplit("_", 1)[0], ct=s.rsplit("_", 1)[1])
                    else:
                        data["s"] = s
                elif stream == "reference_price":
                    data = {"e": "referencePrice", "s": s, "r": price, "t": t}
                elif stream == "avg_price":
                    data = {"e": "avgPrice", "E": t, "s": s, "i": "5m", "w": price, "T": t}
                elif stream == "mark_price":
                    data = {
                        "e": "markPriceUpdate",
                        "E": t,
                        "s": s,
                        "p": price,
                        "i": price,
                        "P": price,
                        "r": "0.0001",
                        "T": t + 3600_000,
                    }
                elif stream == "index_price":
                    data = {"e": "indexPriceUpdate", "E": t, "i": s, "p": price}
                elif stream == "liquidation":
                    data = {"e": "forceOrder", "E": t, "o": {"s": s, "p": price, "q": "1"}}
                elif stream == "asset_index":
                    data = [{"e": "assetIndexUpdate", "E": t, "s": "BTC", "i": price}]
                elif stream == "contract_info":
                    # Valid synthetic lifecycle observation, without changing the directory.
                    data = {"e": "contractInfo", "E": t}
                if product is not None and source.venue_market != "spot":
                    data.update(st=product.symbol_type, ps=product.pair)
                frames.append(
                    json.dumps({"stream": wire, "data": data}, separators=(",", ":")).encode()
                )
        feed[group.name] = frames
        manifest[group.name] = {"frames": len(frames), "sha256": digest([f.hex() for f in frames])}
    return feed, manifest


class MockConnection:
    def __init__(self, frames: list[bytes]) -> None:
        self.frames = iter(frames)

    async def recv(self, decode: bool | None = None) -> bytes:
        frame = next(self.frames, None)
        if frame is not None:
            return frame
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def close(self, code: int = 1000, reason: str = "") -> None:
        pass


class BenchmarkCapture(SelectedAssetCapture):
    def __init__(
        self, *args: Any, stats: StatsCollector, feed: dict[str, list[bytes]], **kwargs: Any
    ) -> None:
        super().__init__(*args, **kwargs)
        self.stats, self.feed = stats, feed
        self.availability = InstrumentedJournal(self.instance_id, stats)
        self.rest_remaining: dict[SourceKey, int] = {}
        self.rest_completed: Counter[SourceKey] = Counter()
        self.groups: list[CombinedWebSocketStream] = []

    def _parameters(self, source: Source) -> dict[str, Any]:
        # Freeze query windows; receive times remain real in Raw.
        params: dict[str, Any] = dict(source.parameters)
        if source.endpoint.startswith("/futures/data/") and source.family != "D31":
            params.update(startTime=EPOCH_MS - 600_000, endTime=EPOCH_MS - 1)
            if source.family == "D16":
                params.update(endTime=EPOCH_MS + 300_000 - 1, limit=3)
        if source.family == "D23":
            params["endTime"] = EPOCH_MS - 1
        return params

    async def poll_once(self, state: SourceState) -> RestObservation:
        key = state.source.key
        if self.rest_remaining.get(key, 0) <= 0:
            state.next_due = float("inf")
            # No-op tasks are allowed by the real scheduler, but never wire/persist.
            return None  # type: ignore[return-value]
        self.rest_remaining[key] -= 1
        result = await super().poll_once(state)
        if not result.valid:
            raise AssertionError(f"invalid REST fixture: {key}: {result.error}")
        self.rest_completed[key] += 1
        state.next_due = time.monotonic() + 0.1 if self.rest_remaining[key] else float("inf")
        return result

    async def _flush_availability(self, *, all_pending: bool = False) -> None:
        self.stats.pending_peak = max(self.stats.pending_peak, len(self.availability.pending))
        started = time.perf_counter_ns()
        await super()._flush_availability(all_pending=all_pending)
        self.stats.record("availability_flush", started)

    def spool(self, market: str, symbol: str, stream: str) -> StreamSpool:
        key = market, symbol, stream
        existing = key in self.spools
        spool = super().spool(market, symbol, stream)
        if existing:
            return spool
        spool._writer_factory = partial(InstrumentedWriter, stats=self.stats)
        spool.queue = InstrumentedQueue(
            spool.queue.capacity, byte_capacity=spool.queue.byte_capacity, stats=self.stats
        )
        event_observer = spool._event_observer
        operation_observer = spool._operation_observer
        seal_observer = spool._seal_observer
        seal_current = spool._seal_current

        def event(event: EventEnvelope, size: int, depth: int) -> None:
            if self.stats.event_sink is not None:
                self.stats.event_sink(event)
            if event_observer:
                event_observer(event, size, depth)
            self.stats.total_frames += 1
            self.stats.latencies["persist_lag"].append(
                (time.monotonic_ns() - event.receive_monotonic_ns) / 1e6
            )

        def operation(name: str, duration: int) -> None:
            if operation_observer:
                operation_observer(name, duration)
            metric = {"write_latency_ns": "write", "fsync_latency_ns": "fsync"}.get(name)
            if metric:
                self.stats.latencies[metric].append(duration / 1e6)

        def seal(manifest: dict[str, object]) -> None:
            if seal_observer:
                seal_observer(manifest)
            self.stats.seals.append(manifest)

        def timed_seal(*args: Any, **kwargs: Any) -> Any:
            started = time.perf_counter_ns()
            result = seal_current(*args, **kwargs)
            if result is not None:  # Exclude close calls on already sealed spools.
                self.stats.record("seal", started)
            return result

        spool._event_observer, spool._operation_observer, spool._seal_observer = (
            event,
            operation,
            seal,
        )
        spool._seal_current = timed_seal  # type: ignore[method-assign]
        return spool

    def make_group(
        self,
        group: StreamGroup,
        product: Product | None,
        observer: Callable[[EventEnvelope], None],
        lifecycle: Callable[[str], None],
    ) -> CombinedWebSocketStream:
        collector = super().make_group(group, product, observer, lifecycle)

        @asynccontextmanager
        async def opener(url: str) -> AsyncIterator[WebSocketConnection]:
            yield MockConnection(self.feed[group.name])

        collector.opener = opener
        self.groups.append(collector)
        return collector


def logical_record(event: EventEnvelope) -> str:
    """Hash real decoded contents, omitting only volatile local identities/clocks."""
    payload: Any = event.raw_payload.hex()
    if event.module == "binance.public.rest.v1":
        payload = json.loads(event.raw_payload)
        for key in ("request_id", "request_time_utc_ns", "request_monotonic_ns"):
            payload["request"].pop(key, None)
        for key in ("receive_time_utc_ns", "receive_monotonic_ns"):
            payload["response"].pop(key, None)
        model = payload["response"].get("model")
        if isinstance(model, dict) and model.get("schema_version") == "recent-trades-compact.v1":
            model["poll"].pop("received_utc_ns")
            for change in model["changes"]:
                change.pop("first_observed_utc_ns")
                change.pop("observed_utc_ns")
    return digest(
        [
            event.market,
            event.symbol,
            event.stream,
            event.module,
            event.source_sequence,
            event.exchange_event_time,
            event.exchange_transaction_time,
            event.exchange_trade_time,
            event.payload_encoding,
            event.capture_flags,
            payload,
        ]
    )


async def run_benchmark(
    *,
    seed: int = 20261102,
    events_per_stream: int = 20,
    rotation_seconds: float = 60.0,
    rest_rounds: int = 2,
    timeout_seconds: float = 120.0,
    monitor_seconds: float = 12.0,
    event_sink: Callable[[EventEnvelope], None] | None = None,
) -> dict[str, Any]:
    if (
        events_per_stream < 4
        or rest_rounds < 2
        or rotation_seconds <= 0
        or timeout_seconds <= 0
        or monitor_seconds < 0
    ):
        raise ValueError("need >=4 events, >=2 REST rounds, positive rotation and timeout")
    cfg = CoverageConfig(
        underlying_assets=("BTC", "ETH"),
        spot_intervals=("1m", "5m"),
        futures_intervals=("1m", "5m"),
        spot_utc8_intervals=("1m", "5m"),
        kline_forming_policy="all",
    )
    products = [
        p
        for v in ("spot", "usdm", "coinm")
        for p in discover_products(v, directory(v), cfg)
        if p.status == "TRADING"
    ]
    feed, fixture_manifest = build_fixture_frames(products, cfg, events_per_stream, seed)
    expected_ws = Counter(hashlib.sha256(f).hexdigest() for frames in feed.values() for f in frames)
    stats = StatsCollector()
    stats.event_sink = event_sink
    # This is synthetic traffic, not requests to an IP. Keep limiter code, but
    # remove real-time network quotas so offline bursts do not sleep for minutes.
    budget = RequestBudget(
        {name: (1_000_000, seconds) for name, (_, seconds) in RECORDER_WINDOWS.items()}
    )
    with tempfile.TemporaryDirectory(prefix="bmdr-m1-baseline-") as tmp:
        layout = ensure_storage_layout(Path(tmp))
        with InstrumentedCatalog(layout.catalog, stats) as catalog:
            owner = BenchmarkCapture(
                RecorderConfig(
                    data_root=layout.root, coverage=cfg, rotation_seconds=rotation_seconds
                ),
                logging.getLogger("benchmark"),
                "m1-baseline",
                "bench-inst",
                layout,
                catalog,
                stats=stats,
                feed=feed,
                clients={v: MockRestClient(v) for v in ("spot", "usdm", "coinm")},
            )
            stop = asyncio.Event()
            wal = Path(str(layout.catalog) + "-wal")
            start_wal_bytes = wal.stat().st_size if wal.exists() else 0
            start_rss = current_rss_bytes() or 0
            stats.peak_rss = start_rss
            started, cpu_started = time.perf_counter(), time.process_time()
            with patch(
                "binance_market_data_recorder.collector.coverage.shared_request_budget",
                return_value=budget,
            ):
                for venue in ("spot", "usdm", "coinm"):
                    await owner.reconcile(venue, directory(venue))
                # Freeze representative REST sources by market + stream, plus every
                # product's depth snapshot. No race to a global first-N counter.
                representatives: dict[tuple[str, str], SourceState] = {}
                for key, state in sorted(owner.sources.items()):
                    state.next_due = float("inf")
                    if state.source.transport == "rest" and not state.dormant:
                        representatives.setdefault(
                            (state.source.market, state.source.stream), state
                        )
                        if state.source.stream in {"depth_snapshot", "rpi_depth_snapshot"}:
                            owner.rest_remaining[key] = 1
                for state in representatives.values():
                    owner.rest_remaining[state.source.key] = (
                        rest_rounds if state.source.cadence_seconds else 1
                    )
                quotas = dict(owner.rest_remaining)
                for key in quotas:
                    owner.sources[key].next_due = 0

                async def finish() -> None:
                    while True:
                        stats.peak_rss = max(stats.peak_rss, current_rss_bytes() or 0)
                        if wal.exists():
                            stats.peak_wal_bytes = max(stats.peak_wal_bytes, wal.stat().st_size)
                        progress = [g.capture_progress() for g in owner.groups]
                        ws_complete = len(progress) == len(feed) and all(
                            p["received_frames"]
                            == p["persisted_frames"]
                            == len(feed[g.stream_name])
                            for g, p in zip(owner.groups, progress, strict=True)
                        )
                        rest_complete = all(owner.rest_completed[k] == n for k, n in quotas.items())
                        if (
                            ws_complete
                            and rest_complete
                            and time.perf_counter() - started >= monitor_seconds
                        ):
                            stop.set()
                            return
                        await asyncio.sleep(0.01)

                async def capture() -> None:
                    async with asyncio.TaskGroup() as tasks:
                        tasks.create_task(owner.run(stop))
                        tasks.create_task(finish())

                try:
                    await asyncio.wait_for(capture(), timeout_seconds)
                finally:
                    stop.set()
            wall, cpu = time.perf_counter() - started, time.process_time() - cpu_started
            stats.peak_rss = max(stats.peak_rss, current_rss_bytes() or 0)
            # Explicit offline audit AFTER timing. No readback added to production.
            chunks = load_source_chunks(layout=layout, catalog=catalog)
            logical_hashes: list[str] = []
            actual_ws: Counter[str] = Counter()
            actual_rest: Counter[SourceKey] = Counter()
            chunk_items = []
            for chunk in chunks:
                count = 0
                for record in iter_source_records(chunk):
                    event = record.envelope
                    if "malformed" in event.capture_flags:
                        raise AssertionError("unexpected malformed fixture in Raw")
                    logical_hashes.append(logical_record(event))
                    if event.module == "binance.public.websocket.v1":
                        actual_ws[hashlib.sha256(event.raw_payload).hexdigest()] += 1
                    else:
                        saved = json.loads(event.raw_payload)
                        actual_rest[
                            (saved["venue_market"], saved["scope"], saved["source_stream"], "rest")
                        ] += 1
                    count += 1
                if count != chunk.manifest["record_count"]:
                    raise AssertionError("Raw frame count differs from manifest")
                chunk_items.append(
                    {
                        "chunk_id": chunk.chunk_id,
                        "market": chunk.manifest["market"],
                        "symbol": chunk.manifest["symbol"],
                        "stream": chunk.manifest["stream"],
                        "record_count": count,
                        "stored_sha256": chunk.manifest["stored_sha256"],
                        "uncompressed_sha256": chunk.uncompressed_sha256,
                    }
                )
            if actual_ws != expected_ws or actual_rest != Counter(quotas):
                raise AssertionError("persisted workload differs from fixed WS/REST input")
            if len(logical_hashes) != stats.total_frames or any(layout.active.glob("*.partial")):
                raise AssertionError("incomplete final drain")
            if len(chunks) != len(stats.seals):
                raise AssertionError("sealed artifact inventory differs from observer")
            pragmas = {
                name: catalog._connection.execute(f"PRAGMA {name}").fetchone()[0]
                for name in ("journal_mode", "synchronous")
            }
    count = len(logical_hashes)
    return {
        "configuration": {
            "seed": seed,
            "events_per_stream": events_per_stream,
            "rest_rounds": rest_rounds,
            "monitor_seconds": monitor_seconds,
            "coverage": cfg.model_dump(mode="json"),
            "rotation_seconds": rotation_seconds,
            "rotation_bytes": owner.config.rotation_bytes,
            "first_rotation": "full period from writer open (benchmark only)",
            "sqlite_pragmas": pragmas,
            "offline_budget_capacity": 1_000_000,
        },
        "workload": {
            "products": len(products),
            "ws_groups": len(feed),
            "ws_frames": sum(expected_ws.values()),
            "rest_polls": sum(quotas.values()),
            "rest_sources": [{"key": key, "polls": n} for key, n in sorted(quotas.items())],
            "ws_fixture_manifest": fixture_manifest,
            "input_digest": digest([fixture_manifest, sorted(quotas.items())]),
        },
        "summary": {
            "wall_seconds": wall,
            "cpu_seconds": cpu,
            "total_frames": count,
            "cpu_us_per_frame": cpu * 1e6 / count,
            "wall_throughput_fps": count / wall,
        },
        "metrics": {
            "sealed_chunks_count": len(chunks),
            "manifest_files_count": len(chunks),
            "empty_chunks_count": sum(c["record_count"] == 0 for c in chunk_items),
            "sealed_chunks_per_hour": len(chunks) * 3600 / wall,
            "manifest_files_per_hour": len(chunks) * 3600 / wall,
            "rate_note": "startup/shutdown rates normalized per hour; not steady-state rotation",
            "sqlite_catalog_write_transactions_per_min": len(stats.latencies["sql_transaction"])
            * 60
            / wall,
            "sqlite_catalog_write_statements": stats.sql_statement_count,
            "sqlite_catalog_wal_start_bytes": start_wal_bytes,
            "sqlite_catalog_wal_peak_bytes": stats.peak_wal_bytes,
            "sqlite_catalog_wal_growth_peak_bytes": max(0, stats.peak_wal_bytes - start_wal_bytes),
            "availability_journal_pending_peak": stats.pending_peak,
            "latencies_ms": {k: distribution(v) for k, v in stats.latencies.items()},
            "start_rss_bytes": start_rss,
            "peak_rss_bytes": stats.peak_rss,
            "process_lifetime_peak_rss_bytes": peak_rss_bytes(),
            "raw_chunk_count": len(chunks),
            "total_persisted_frames": count,
            "logical_content_digest": digest(sorted(logical_hashes)),
        },
        "chunks": sorted(
            chunk_items,
            key=lambda c: (
                str(c["market"]),
                str(c["symbol"]),
                str(c["stream"]),
                str(c["chunk_id"]),
            ),
        ),
        "measurement_scope": {
            "timed": "reconcile/startup + WS/REST capture + availability monitor + drain/seal/stop",
            "excluded": "imports, fixture generation, Catalog schema creation, offline Raw audit",
            "queue_wait": "Spool enqueue to dequeue, monotonic clock, one sample per frame",
            "persist_lag": "receive to append completion, monotonic clock, one sample per frame",
            "fsync": "Raw writer fsync only; directory/compression fsync included in seal time",
            "seal": "successful _seal_current calls only, including startup empty boundaries",
            "rss": "current RSS sampled every 10ms and at boundaries; lifetime peak separate",
            "sqlite": "DML statement time vs BEGIN-to-COMMIT/autocommit transaction time",
            "limitations": "synthetic burst; 1m/5m intervals; no steady-state backlog claim; "
            "background verification worker is not started",
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=20261102)
    parser.add_argument("--events-per-stream", type=int, default=20)
    parser.add_argument("--rest-rounds", type=int, default=2)
    parser.add_argument("--rotation-seconds", type=float, default=60.0)
    parser.add_argument("--timeout-seconds", type=float, default=120)
    parser.add_argument("--monitor-seconds", type=float, default=12)
    parser.add_argument("--output", type=Path, default=Path("docs/benchmarks/baseline-main.json"))
    parser.add_argument(
        "--verify-repeatability", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args()
    results = [
        asyncio.run(
            run_benchmark(
                seed=args.seed,
                events_per_stream=args.events_per_stream,
                rest_rounds=args.rest_rounds,
                rotation_seconds=args.rotation_seconds,
                timeout_seconds=args.timeout_seconds,
                monitor_seconds=args.monitor_seconds,
            )
        )
        for _ in range(2 if args.verify_repeatability else 1)
    ]
    repeats = [
        {
            "frames": r["summary"]["total_frames"],
            "input_digest": r["workload"]["input_digest"],
            "content_digest": r["metrics"]["logical_content_digest"],
        }
        for r in results
    ]
    if any(r != repeats[0] for r in repeats):
        raise SystemExit("FAIL: fixed input or persisted content changed between runs")
    report = {
        "schema_version": "bmdr-m1-baseline.v2",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "source_git_sha": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip(),
        "runtime_matches_pre_m1": subprocess.run(
            ["git", "diff", "--quiet", "f109ffc", "--", "src"], cwd=REPO
        ).returncode
        == 0,
        "runtime_source_tree_sha": subprocess.check_output(
            ["git", "rev-parse", "HEAD:src"], cwd=REPO, text=True
        ).strip(),
        "benchmark_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "environment": {"python": sys.version, "platform": platform.platform()},
        "repeatability": {"verified": len(repeats) == 2, "runs": repeats},
        **results[0],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "summary": report["summary"],
                "repeatability": report["repeatability"],
                "output": str(args.output),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
