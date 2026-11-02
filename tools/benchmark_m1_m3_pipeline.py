"""Multi-stream pipeline benchmark for M1-M3 capture validation.

Runs SelectedAssetCapture offline across BTC + ETH full product families
(Spot, USD-M perpetual/delivery, COIN-M perpetual/delivery) against deterministic
synthetic and fixture feeds. Collects the 8 core baseline metrics required by M1.0.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import random
import sys
import tempfile
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.products import discover_products
from binance_market_data_recorder.binance.public_sources import Source, product_sources
from binance_market_data_recorder.binance.public_websocket import (
    DepthSequenceObserver,
    StreamGroup,
    envelope_for_group,
    stream_groups,
)
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.domain.product import Product
from binance_market_data_recorder.service.resources import current_rss_bytes, peak_rss_bytes
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "tests/fixtures/binance/p2"


def directory(venue: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / f"{venue}-directory.json").read_bytes())  # type: ignore[no-any-return]


class Response:
    status = 200

    def __init__(self, model: Any) -> None:
        self.model = model
        self.headers: dict[str, object] = {"X-MBX-USED-WEIGHT-1M": "12", "Set-Cookie": "exclude"}

    def data(self) -> Any:
        return self.model


class StatsCollector:
    """Accumulate in-memory latency distributions and operational metrics."""

    def __init__(self) -> None:
        self.queue_wait_ms: list[float] = []
        self.append_batch_ms: list[float] = []
        self.fsync_ms: list[float] = []
        self.seal_ms: list[float] = []
        self.pending_availability_counts: list[int] = []
        self.availability_flush_ms: list[float] = []
        self.sql_write_durations_ms: list[float] = []
        self.sql_write_count: int = 0
        self.peak_wal_bytes: int = 0
        self.peak_rss: int = 0
        self.start_rss: int = 0
        self.start_time: float = 0
        self.end_time: float = 0
        self.cpu_start: float = 0
        self.cpu_end: float = 0
        self.total_frames: int = 0
        self.sealed_manifests: list[dict[str, Any]] = []

    def record_queue_wait(self, wait_ms: float) -> None:
        self.queue_wait_ms.append(wait_ms)

    def record_operation(self, name: str, duration_ns: int) -> None:
        ms = duration_ns / 1_000_000.0
        if "append" in name or "write" in name:
            self.append_batch_ms.append(ms)
        elif "fsync" in name:
            self.fsync_ms.append(ms)
        elif "seal" in name:
            self.seal_ms.append(ms)

    def record_pending_availability(self, count: int) -> None:
        self.pending_availability_counts.append(count)

    def record_availability_flush(self, duration_ns: int) -> None:
        self.availability_flush_ms.append(duration_ns / 1_000_000.0)

    def record_sql_write(self, duration_ns: int) -> None:
        self.sql_write_count += 1
        self.sql_write_durations_ms.append(duration_ns / 1_000_000.0)

    def record_seal_manifest(self, manifest: dict[str, Any]) -> None:
        self.sealed_manifests.append(manifest)


def percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int((len(sorted_data) - 1) * pct)
    return round(sorted_data[idx], 3)


class ConnectionProxy:
    """Proxy SQLite connection to track transaction counts and write durations."""

    def __init__(self, real_con: Any, stats: StatsCollector) -> None:
        self._real = real_con
        self._stats = stats

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        is_write = any(
            sql.lstrip().upper().startswith(p)
            for p in ("INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT")
        )
        start = time.perf_counter_ns()
        try:
            return self._real.execute(sql, *args, **kwargs)
        finally:
            if is_write:
                self._stats.record_sql_write(time.perf_counter_ns() - start)

    def executemany(self, sql: str, *args: Any, **kwargs: Any) -> Any:
        is_write = any(
            sql.lstrip().upper().startswith(p)
            for p in ("INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT")
        )
        start = time.perf_counter_ns()
        try:
            return self._real.executemany(sql, *args, **kwargs)
        finally:
            if is_write:
                self._stats.record_sql_write(time.perf_counter_ns() - start)


class InstrumentedCatalog(Catalog):
    """Catalog subclass with connection-level SQLite write tracing."""

    def __init__(self, path: Path, stats: StatsCollector, **kwargs: Any) -> None:
        super().__init__(path, **kwargs)
        self.stats = stats
        self._connection = ConnectionProxy(self._connection, stats)  # type: ignore[assignment]


class MockConnection:
    """Deterministic in-memory WebSocket connection feeding pre-generated frames."""

    def __init__(self, frames: list[bytes], done_event: asyncio.Event) -> None:
        self.frames = frames
        self.pos = 0
        self.done_event = done_event

    async def recv(self, decode: bool | None = None) -> bytes:
        if self.pos < len(self.frames):
            frame = self.frames[self.pos]
            self.pos += 1
            if self.pos == len(self.frames):
                self.done_event.set()
            return frame
        self.done_event.set()
        await asyncio.Event().wait()
        return b""

    async def close(self, code: int = 1000, reason: str = "") -> None:
        pass


class MockRestClient:
    """Deterministic REST client returning static fixture or compliant data."""

    def __init__(self, venue: str) -> None:
        self.venue = venue

    def request(self, source: Source, params: dict[str, Any]) -> Any:
        stream = source.stream
        if stream == "exchange_info":
            return Response(directory(self.venue))
        if stream in {"depth_snapshot", "rpi_depth_snapshot"}:
            return Response(
                {"lastUpdateId": 100, "bids": [["1000", "1.0"]], "asks": [["1001", "1.0"]]}
            )
        if "kline" in stream or "_seed_" in stream:
            return Response(
                [
                    [
                        1793000000000,
                        "100",
                        "101",
                        "99",
                        "100",
                        "10",
                        1793000059999,
                        "1000",
                        100,
                        "5",
                        "500",
                        "0",
                    ]
                ]
            )
        if stream == "funding_info":
            return Response([{"symbol": source.symbol, "fundingIntervalHours": 8}])
        if stream == "execution_rules":
            return Response({"symbolRules": []})
        return Response({})

    def close(self) -> None:
        pass


class BenchmarkCapture(SelectedAssetCapture):
    """SelectedAssetCapture wired to in-memory feeds and metrics instrumentation."""

    def __init__(
        self,
        *args: Any,
        stats: StatsCollector,
        feed_map: dict[str, list[bytes]],
        done_events: dict[str, asyncio.Event],
        target_rest_polls: int = 40,
        rotation_seconds: float = 3600.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.stats = stats
        self.feed_map = feed_map
        self.done_events = done_events
        self.target_rest_polls = target_rest_polls
        self.rotation_seconds = rotation_seconds
        self.rest_polled_count = 0

    async def _flush_availability(self, *, all_pending: bool = False) -> None:
        self.stats.record_pending_availability(len(self.availability.pending))
        start = time.perf_counter_ns()
        await super()._flush_availability(all_pending=all_pending)
        dur = time.perf_counter_ns() - start
        self.stats.record_availability_flush(dur)

    async def poll_once(self, state: Any) -> Any:
        if self.rest_polled_count >= self.target_rest_polls:
            state.next_due = float("inf")
            return None
        self.rest_polled_count += 1
        res = await super().poll_once(state)
        state.next_due = float("inf")
        return res

    def spool(self, market: str, symbol: str, stream: str) -> StreamSpool:
        key = market, symbol, stream
        if key not in self.spools:

            def observe_event(event: EventEnvelope, frame_bytes: int, queue_depth: int) -> None:
                self.metrics.safely_observe_written(
                    event,
                    raw_frame_bytes=frame_bytes,
                    queue_depth=queue_depth,
                )
                wait_ms = (time.time_ns() - event.receive_time_utc_ns) / 1_000_000.0
                self.stats.record_queue_wait(max(0.0, wait_ms))
                self.stats.total_frames += 1

            def observe_operation(name: str, duration: int) -> None:
                self.metrics.safely_observe_operation(
                    market=market,
                    stream=stream,
                    name=name,
                    duration_ns=duration,
                )
                self.stats.record_operation(name, duration)

            def observe_seal(manifest: dict[str, object]) -> None:
                self.metrics.safely_observe_seal(manifest)
                self.stats.record_seal_manifest(manifest)

            spool = StreamSpool(
                layout=self.layout,
                catalog=self.catalog,
                market=market,
                symbol=symbol,
                stream=stream,
                collector_instance_id=self.instance_id,
                collector_version=self.version,
                queue_capacity=self.config.ingress_queue_capacity,
                rotation=RotationPolicy(
                    self.rotation_seconds, self.config.rotation_bytes
                ),
                durability_interval_seconds=self.config.durability_interval_seconds,
                max_frame_bytes=self.config.max_frame_bytes,
                event_observer=observe_event,
                operation_observer=observe_operation,
                seal_observer=observe_seal,
            )
            orig_close_and_seal = spool.close_and_seal

            def traced_close_and_seal(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter_ns()
                try:
                    return orig_close_and_seal(*args, **kwargs)
                finally:
                    dur = time.perf_counter_ns() - start
                    self.stats.record_operation("seal_latency_ns", dur)

            spool.close_and_seal = traced_close_and_seal  # type: ignore[assignment]
            self.spools[key] = spool
            self._spool_locks[key] = asyncio.Lock()
        return self.spools[key]

    def make_group(
        self,
        group: StreamGroup,
        product: Product | None,
        observer: Any,
        lifecycle: Any,
    ) -> UsdMStreamCollector:
        source = group.sources[0]
        market = product.key.market if product else source.market
        symbol = product.key.symbol if product else source.symbol

        frames = self.feed_map.get(group.name, [])
        done = self.done_events.setdefault(group.name, asyncio.Event())

        @asynccontextmanager
        async def mock_opener(url: str) -> AsyncIterator[WebSocketConnection]:
            yield MockConnection(frames, done)

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
            opener=mock_opener,
            envelope_factory=capture,
            envelope_observer=observed,
            lifecycle_observer=lifecycle,
        )
        self._group_sources[collector] = group
        return collector


def build_fixture_frames(
    products: list[Product],
    config: CoverageConfig,
    events_per_stream: int,
    seed: int,
) -> tuple[dict[str, list[bytes]], dict[str, Any]]:
    """Generate deterministic combined WebSocket frames across all streams."""
    random.Random(seed)
    epoch_ms = 1793000000000
    feed_map: dict[str, list[bytes]] = {}
    manifest: dict[str, Any] = {}

    for product in products:
        p_sources = product_sources(product, config)
        ws_sources = [s for s in p_sources if s.transport == "ws"]
        groups = stream_groups(ws_sources, config.streams_per_connection)
        for group in groups:
            frames: list[bytes] = []
            for idx in range(events_per_stream):
                for wire in group.wire_names:
                    source = group.by_wire[wire]
                    s = product.key.symbol
                    st = product.symbol_type
                    ps = product.pair
                    t = epoch_ms + idx * 100
                    stream = source.stream

                    data: dict[str, Any]
                    if stream in {"diff_depth", "rpi_depth"}:
                        data = {
                            "e": "depthUpdate",
                            "E": t,
                            "s": s,
                            "U": 100 + idx * 2,
                            "u": 101 + idx * 2,
                            "b": [["50000.00", "1.0"]],
                            "a": [["50000.10", "1.0"]],
                        }
                        if source.venue_market != "spot":
                            data.update(pu=99 + idx * 2, T=t, st=st, ps=ps)
                    elif stream == "book_ticker":
                        data = {
                            "s": s,
                            "u": 100 + idx,
                            "b": "50000.00",
                            "B": "1.5",
                            "a": "50000.10",
                            "A": "2.0",
                        }
                        if source.venue_market != "spot":
                            data.update(e="bookTicker", E=t, T=t, st=st, ps=ps)
                    elif stream == "agg_trade":
                        data = {
                            "e": "aggTrade",
                            "E": t,
                            "s": s,
                            "a": 1000 + idx,
                            "p": "50000.00",
                            "q": "0.5",
                            "f": 2000 + idx,
                            "l": 2000 + idx,
                            "T": t,
                            "m": True,
                        }
                        if source.venue_market == "spot":
                            data["M"] = True
                        else:
                            data["st"] = st
                    elif "kline" in stream:
                        is_closed = idx % 4 == 3
                        bar_open = epoch_ms + (idx // 4) * 60_000
                        data = {
                            "e": "kline",
                            "E": t,
                            "s": s,
                            "k": {
                                "t": bar_open,
                                "T": bar_open + 59_999,
                                "s": s,
                                "i": "1m",
                                "f": 100 + idx * 10,
                                "L": 109 + idx * 10,
                                "o": "50000.00",
                                "c": "50050.00",
                                "h": "50100.00",
                                "l": "49950.00",
                                "v": "100.0",
                                "n": 10,
                                "x": is_closed,
                                "q": "5002500.00",
                                "V": "50.0",
                                "Q": "2501250.00",
                                "B": "0",
                            },
                        }
                    else:
                        data = {
                            "e": "24hrTicker",
                            "E": t,
                            "s": s,
                            "o": "49000.00",
                            "h": "51000.00",
                            "l": "48500.00",
                            "c": "50050.00",
                            "v": "1000.0",
                            "n": 5000,
                        }
                    wire_payload = json.dumps(
                        {"stream": wire, "data": data}, separators=(",", ":")
                    ).encode("utf-8")
                    frames.append(wire_payload)

            feed_map[group.name] = frames
            manifest[group.name] = {
                "frames": len(frames),
                "bytes": sum(len(f) for f in frames),
                "sha256": hashlib.sha256(b"\n".join(frames)).hexdigest(),
            }

    return feed_map, manifest


async def run_benchmark(
    *,
    seed: int = 20261102,
    events_per_stream: int = 20,
    kline_intervals: tuple[str, ...] = ("1m", "5m"),
    target_rest_polls: int = 40,
    data_root: Path | None = None,
    rotation_seconds: float = 3600.0,
) -> dict[str, Any]:
    """Execute one complete deterministic benchmark run and collect all 8 metrics."""
    stats = StatsCollector()
    cfg = CoverageConfig(
        underlying_assets=("BTC", "ETH"),
        spot_intervals=kline_intervals,
        futures_intervals=kline_intervals,
    )
    products: list[Product] = []
    for v in ("spot", "usdm", "coinm"):
        discovered = discover_products(v, directory(v), cfg)
        products.extend([p for p in discovered if p.status == "TRADING"])

    feed_map, _fixture_manifest = build_fixture_frames(products, cfg, events_per_stream, seed)
    done_events: dict[str, asyncio.Event] = {}

    with tempfile.TemporaryDirectory(prefix="bmdr-m1-baseline-") as tmp_dir:
        root_path = Path(tmp_dir) if data_root is None else data_root
        layout = ensure_storage_layout(root_path)
        wal_path = root_path / "state/catalog.sqlite-wal"

        with InstrumentedCatalog(layout.catalog, stats=stats) as catalog:
            config = RecorderConfig(data_root=root_path, coverage=cfg)
            clients = {v: MockRestClient(v) for v in ("spot", "usdm", "coinm")}

            owner = BenchmarkCapture(
                config,
                logging.getLogger("bench"),
                "v1",
                "bench-inst",
                layout,
                catalog,
                clients=clients,
                stats=stats,
                feed_map=feed_map,
                done_events=done_events,
                target_rest_polls=target_rest_polls,
                rotation_seconds=rotation_seconds,
            )

            # Reconcile product directories
            for v in ("spot", "usdm", "coinm"):
                await owner.reconcile(v, directory(v))

            stop = asyncio.Event()

            async def monitor() -> None:
                while not stop.is_set():
                    rss = current_rss_bytes() or 0
                    stats.peak_rss = max(stats.peak_rss, rss)
                    if wal_path.exists():
                        stats.peak_wal_bytes = max(stats.peak_wal_bytes, wal_path.stat().st_size)
                    await asyncio.sleep(0.05)
                stats.peak_rss = max(stats.peak_rss, current_rss_bytes() or 0, peak_rss_bytes())

            async def feed_watcher() -> None:
                await asyncio.gather(*(evt.wait() for evt in done_events.values()))
                while owner.rest_polled_count < owner.target_rest_polls:
                    await asyncio.sleep(0.01)
                while owner.polls and any(not t.done() for t in owner.polls.values()):
                    await asyncio.sleep(0.01)
                while any(s.queue.depth > 0 for s in owner.spools.values()):
                    await asyncio.sleep(0.01)
                stop.set()

            stats.start_rss = current_rss_bytes() or 0
            stats.peak_rss = stats.start_rss
            stats.start_time = time.perf_counter()
            stats.cpu_start = time.process_time()

            monitor_task = asyncio.create_task(monitor())
            watcher_task = asyncio.create_task(feed_watcher())

            await owner.run(stop)
            await watcher_task
            await monitor_task

            stats.end_time = time.perf_counter()
            stats.cpu_end = time.process_time()

            cursor = catalog._connection.cursor()
            rows = cursor.execute(
                "SELECT chunk_id, state, sealed_path, manifest_path, record_count, stored_bytes, "
                "uncompressed_bytes, stored_sha256 FROM chunks ORDER BY chunk_id"
            ).fetchall()
            chunks = [
                {
                    "chunk_id": str(r[0]),
                    "state": str(r[1]),
                    "sealed_path": str(r[2]),
                    "manifest_path": str(r[3]),
                    "record_count": int(r[4] or 0),
                    "stored_bytes": int(r[5] or 0),
                    "uncompressed_bytes": int(r[6] or 0),
                    "stored_sha256": str(r[7] or ""),
                }
                for r in rows
            ]

    wall_seconds = stats.end_time - stats.start_time
    cpu_seconds = stats.cpu_end - stats.cpu_start
    total_frames = stats.total_frames
    cpu_us_per_frame = (
        round((cpu_seconds * 1_000_000.0) / total_frames, 2) if total_frames else 0.0
    )
    wall_throughput_fps = round(total_frames / wall_seconds, 2) if wall_seconds else 0.0

    sealed_count = len(chunks)
    sealed_per_hour = round((sealed_count / wall_seconds) * 3600.0, 1) if wall_seconds else 0.0

    tx_per_min = round((stats.sql_write_count / wall_seconds) * 60.0, 1) if wall_seconds else 0.0
    tx_total_ms = round(sum(stats.sql_write_durations_ms), 2)

    manifest_items = sorted(
        (str(m["market"]), str(m["symbol"]), str(m["stream"]), int(m["record_count"]))
        for m in stats.sealed_manifests
    )
    chunks_digest = hashlib.sha256(json.dumps(manifest_items).encode("utf-8")).hexdigest()

    return {
        "summary": {
            "wall_seconds": round(wall_seconds, 3),
            "cpu_seconds": round(cpu_seconds, 3),
            "total_frames": total_frames,
            "cpu_us_per_frame": cpu_us_per_frame,
            "wall_throughput_fps": wall_throughput_fps,
        },
        "metrics": {
            # ① cpu_us_per_frame & wall throughput
            "cpu_us_per_frame": cpu_us_per_frame,
            "wall_throughput_fps": wall_throughput_fps,
            # ② hourly sealed chunks & manifests
            "sealed_chunks_count": sealed_count,
            "sealed_chunks_per_hour": sealed_per_hour,
            "manifest_files_per_hour": sealed_per_hour,
            # ③ SQLite Catalog write transactions, duration, WAL growth
            "sqlite_catalog_write_transactions_per_min": tx_per_min,
            "sqlite_catalog_write_total_ms": tx_total_ms,
            "sqlite_catalog_wal_bytes": stats.peak_wal_bytes,
            # ④ AvailabilityJournal pending peak length & flush duration
            "availability_journal_pending_peak": max(
                stats.pending_availability_counts, default=0
            ),
            "availability_journal_flush_p50_ms": percentile(
                stats.availability_flush_ms, 0.50
            ),
            "availability_journal_flush_p99_ms": percentile(
                stats.availability_flush_ms, 0.99
            ),
            "availability_journal_flush_max_ms": round(
                max(stats.availability_flush_ms, default=0.0), 3
            ),
            # ⑤ Spool queue oldest event wait time (ms)
            "oldest_queue_wait_ms_p50": percentile(stats.queue_wait_ms, 0.50),
            "oldest_queue_wait_ms_p99": percentile(stats.queue_wait_ms, 0.99),
            "oldest_queue_wait_ms_max": round(max(stats.queue_wait_ms, default=0.0), 3),
            # ⑥ Write batch duration and fsync / seal duration distribution (ms)
            "append_batch_ms_p50": percentile(stats.append_batch_ms, 0.50),
            "append_batch_ms_p99": percentile(stats.append_batch_ms, 0.99),
            "append_batch_ms_max": round(max(stats.append_batch_ms, default=0.0), 3),
            "fsync_ms_p50": percentile(stats.fsync_ms, 0.50),
            "fsync_ms_p99": percentile(stats.fsync_ms, 0.99),
            "fsync_ms_max": round(max(stats.fsync_ms, default=0.0), 3),
            "seal_ms_p50": percentile(stats.seal_ms, 0.50),
            "seal_ms_p99": percentile(stats.seal_ms, 0.99),
            "seal_ms_max": round(max(stats.seal_ms, default=0.0), 3),
            # ⑦ Process peak resident memory (peak_rss_bytes)
            "start_rss_bytes": stats.start_rss,
            "peak_rss_bytes": stats.peak_rss,
            # ⑧ Raw chunks count, frame count, logical digest
            "raw_chunk_count": sealed_count,
            "total_persisted_frames": total_frames,
            "logical_chunks_digest": chunks_digest,
        },
        "chunks": chunks,
        "manifest_items": manifest_items,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=20261102, help="Fixed random seed")
    parser.add_argument(
        "--events-per-stream",
        type=int,
        default=20,
        help="Events generated per WebSocket stream",
    )
    parser.add_argument(
        "--target-rest-polls",
        type=int,
        default=40,
        help="Target REST poll count before completing run",
    )
    parser.add_argument(
        "--rotation-seconds",
        type=float,
        default=3600.0,
        help="Rotation period in seconds (default: 3600.0 for stable burst runs)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/benchmarks/baseline-main.json"),
        help="Output path for baseline JSON report",
    )
    parser.add_argument(
        "--verify-repeatability",
        action="store_true",
        default=True,
        help="Verify two consecutive runs produce 100%% identical frame counts and logical digests",
    )
    args = parser.parse_args()

    print(
        f"Starting Pre-M1 Baseline Pipeline Benchmark (seed={args.seed}, "
        f"events_per_stream={args.events_per_stream}, "
        f"target_rest_polls={args.target_rest_polls})..."
    )

    res1 = asyncio.run(
        run_benchmark(
            seed=args.seed,
            events_per_stream=args.events_per_stream,
            target_rest_polls=args.target_rest_polls,
            rotation_seconds=args.rotation_seconds,
        )
    )

    m = res1["metrics"]
    s = res1["summary"]
    print("\n--- Baseline Run Summary ---")
    print(f"Wall-clock elapsed: {s['wall_seconds']}s")
    print(f"CPU process elapsed: {s['cpu_seconds']}s")
    print(f"Total persisted frames: {s['total_frames']}")
    print(f"CPU us per frame: {s['cpu_us_per_frame']} us")
    print(f"Wall throughput: {s['wall_throughput_fps']} frames/s")
    print(f"Sealed chunks count: {m['sealed_chunks_count']}")
    print(f"SQLite Catalog writes/min: {m['sqlite_catalog_write_transactions_per_min']}")
    print(f"AvailabilityJournal peak pending: {m['availability_journal_pending_peak']} rows")
    print(
        f"Oldest queue wait p50/p99: {m['oldest_queue_wait_ms_p50']} / "
        f"{m['oldest_queue_wait_ms_p99']} ms"
    )
    print(f"Append batch p50/p99: {m['append_batch_ms_p50']} / {m['append_batch_ms_p99']} ms")
    print(f"Fsync p50/p99: {m['fsync_ms_p50']} / {m['fsync_ms_p99']} ms")
    print(f"Seal p50/p99: {m['seal_ms_p50']} / {m['seal_ms_p99']} ms")
    print(f"Peak RSS: {m['peak_rss_bytes'] / (1024 * 1024):.2f} MiB")
    print(f"Logical chunks digest: {m['logical_chunks_digest']}")

    if args.verify_repeatability:
        print("\nVerifying repeatability with identical-input second run...")
        res2 = asyncio.run(
            run_benchmark(
                seed=args.seed,
                events_per_stream=args.events_per_stream,
                target_rest_polls=args.target_rest_polls,
                rotation_seconds=args.rotation_seconds,
            )
        )
        frames_match = (
            res1["metrics"]["total_persisted_frames"] == res2["metrics"]["total_persisted_frames"]
        )
        digest_match = (
            res1["metrics"]["logical_chunks_digest"] == res2["metrics"]["logical_chunks_digest"]
        )
        print(f"Round 1 frames: {res1['metrics']['total_persisted_frames']}")
        print(f"Round 1 digest: {res1['metrics']['logical_chunks_digest']}")
        print(f"Round 2 frames: {res2['metrics']['total_persisted_frames']}")
        print(f"Round 2 digest: {res2['metrics']['logical_chunks_digest']}")

        if not frames_match or not digest_match:
            print("ERROR: Consecutive runs did not produce identical results!", file=sys.stderr)
            sys.exit(1)
        print("PASS: Consecutive runs produced 100% identical frame count and logical digest.")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": "bmdr-m1-baseline.v1",
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "configuration": {
            "seed": args.seed,
            "events_per_stream": args.events_per_stream,
            "target_rest_polls": args.target_rest_polls,
            "underlying_assets": ["BTC", "ETH"],
        },
        "summary": res1["summary"],
        "metrics": res1["metrics"],
        "manifest_items_sample": res1["manifest_items"][:20],
    }

    with args.output.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"\nSaved baseline report to: {args.output}")


if __name__ == "__main__":
    main()
