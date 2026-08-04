"""Deterministic offline USD-M ingress/backpressure benchmark.

The benchmark uses fake WebSockets and a temporary Raw/Catalog root. It never
contacts Binance and refuses to reuse an existing output directory.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import math
import os
import shutil
import tempfile
import threading
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

import zstandard

from binance_market_data_recorder.binance.spot.websocket import ReconnectBackoff
from binance_market_data_recorder.binance.usdm.schema import UsdMStream
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.service.resources import current_rss_bytes
from binance_market_data_recorder.spool.format import (
    FRAME_PREFIX,
    decode_chunk_header,
    decode_envelope,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    events: int
    receipt_capacity: int
    batch_capacity: int
    reconnect_at: int | None = None
    append_delay_seconds: float = 0
    fsync_delay_seconds: float = 0
    drain_delay_seconds: float = 0
    seal_delay_seconds: float = 0
    durability_interval_seconds: float = 1


class BenchmarkWriter(RawChunkWriter):
    def __init__(
        self,
        *args: Any,
        append_delay_seconds: float,
        fsync_delay_seconds: float,
        append_samples_ns: list[int],
        fsync_samples_ns: list[int],
        **kwargs: Any,
    ) -> None:
        self.append_delay_seconds = append_delay_seconds
        self.fsync_delay_seconds = fsync_delay_seconds
        self.append_samples_ns = append_samples_ns
        self.fsync_samples_ns = fsync_samples_ns
        super().__init__(*args, **kwargs)

    def append(self, envelope: EventEnvelope) -> int:
        started = time.perf_counter_ns()
        try:
            if self.append_delay_seconds:
                time.sleep(self.append_delay_seconds)
            return super().append(envelope)
        finally:
            self.append_samples_ns.append(time.perf_counter_ns() - started)

    def sync(self) -> None:
        started = time.perf_counter_ns()
        try:
            if self.fsync_delay_seconds:
                time.sleep(self.fsync_delay_seconds)
            super().sync()
        finally:
            self.fsync_samples_ns.append(time.perf_counter_ns() - started)


class InstrumentedSpool(StreamSpool):
    def __init__(
        self,
        *args: Any,
        drain_delay_seconds: float,
        seal_delay_seconds: float,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.drain_delay_seconds = drain_delay_seconds
        self.seal_delay_seconds = seal_delay_seconds
        self.drain_durations_ns: list[int] = []
        self.seal_durations_ns: list[int] = []

    def drain_all(self) -> int:
        started = time.perf_counter_ns()
        if self.drain_delay_seconds:
            time.sleep(self.drain_delay_seconds)
        try:
            return super().drain_all()
        finally:
            self.drain_durations_ns.append(time.perf_counter_ns() - started)

    def close_and_seal(self) -> dict[str, object] | None:
        started = time.perf_counter_ns()
        if self.seal_delay_seconds:
            time.sleep(self.seal_delay_seconds)
        try:
            return super().close_and_seal()
        finally:
            self.seal_durations_ns.append(time.perf_counter_ns() - started)


class FakeSocket:
    def __init__(
        self,
        messages: Sequence[bytes],
        *,
        stop: asyncio.Event | None,
        send_times_ns: list[int],
    ) -> None:
        self.messages = iter(messages)
        self.stop = stop
        self.send_times_ns = send_times_ns

    async def recv(self, decode: bool | None = None) -> bytes:
        try:
            message = next(self.messages)
        except StopIteration:
            if self.stop is not None:
                self.stop.set()
                await asyncio.Future[None]()
            raise OSError("deterministic reconnect") from None
        self.send_times_ns.append(time.perf_counter_ns())
        return message

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


def _book_ticker(update_id: int) -> bytes:
    return json.dumps(
        {
            "e": "bookTicker",
            "E": update_id,
            "T": update_id,
            "s": "BTCUSDT",
            "u": update_id,
            "b": "100.0",
            "B": "1.0",
            "a": "101.0",
            "A": "1.0",
        },
        separators=(",", ":"),
    ).encode()


def _percentile(samples: Sequence[int], fraction: float) -> int | None:
    if not samples:
        return None
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _fd_count() -> int | None:
    path = Path("/proc/self/fd")
    return len(os.listdir(path)) if path.is_dir() else None


def _read_raw(root: Path) -> list[Any]:
    events: list[Any] = []
    documents = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in (root / "data/manifests").glob("*.json")
    ]
    documents.sort(key=lambda item: int(item["created_at_utc_ns"]))
    for document in documents:
        raw = zstandard.ZstdDecompressor().decompress(
            (root / str(document["relative_path"])).read_bytes()
        )
        source = io.BytesIO(raw)
        decode_chunk_header(source)
        while prefix := source.read(FRAME_PREFIX.size):
            length, _flags, _reserved, _checksum = FRAME_PREFIX.unpack(prefix)
            events.append(decode_envelope(source.read(length)))
    return events


async def _run_scenario(scenario: Scenario, root: Path) -> dict[str, Any]:
    layout = ensure_storage_layout(root)
    catalog = Catalog(layout.catalog)
    operation_samples: dict[str, list[int]] = {
        "write_latency_ns": [],
        "fsync_latency_ns": [],
    }
    append_samples_ns: list[int] = []
    fsync_samples_ns: list[int] = []

    def observe_operation(name: str, duration_ns: int) -> None:
        operation_samples.setdefault(name, []).append(duration_ns)

    writer_factory = partial(
        BenchmarkWriter,
        append_delay_seconds=scenario.append_delay_seconds,
        fsync_delay_seconds=scenario.fsync_delay_seconds,
        append_samples_ns=append_samples_ns,
        fsync_samples_ns=fsync_samples_ns,
    )
    spool = InstrumentedSpool(
        layout=layout,
        catalog=catalog,
        market="um_perpetual",
        symbol="BTCUSDT",
        stream="book_ticker",
        collector_instance_id="m21-4-benchmark",
        collector_version="0.1.0+benchmark",
        queue_capacity=scenario.batch_capacity,
        rotation=RotationPolicy(seconds=60),
        durability_interval_seconds=scenario.durability_interval_seconds,
        max_frame_bytes=1024 * 1024,
        writer_factory=writer_factory,
        operation_observer=observe_operation,
        drain_delay_seconds=scenario.drain_delay_seconds,
        seal_delay_seconds=scenario.seal_delay_seconds,
    )
    stop = asyncio.Event()
    send_times_ns: list[int] = []
    payloads = [_book_ticker(value) for value in range(scenario.events)]
    reconnect_at = scenario.reconnect_at
    message_groups = (
        [payloads]
        if reconnect_at is None
        else [payloads[:reconnect_at], payloads[reconnect_at:]]
    )
    opened = 0

    @asynccontextmanager
    async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
        nonlocal opened
        group = message_groups[opened]
        opened += 1
        yield FakeSocket(
            group,
            stop=stop if opened == len(message_groups) else None,
            send_times_ns=send_times_ns,
        )

    collector = UsdMStreamCollector(
        stream=UsdMStream.BOOK_TICKER,
        route="public",
        wire_name="btcusdt@bookTicker",
        spool=spool,
        collector_instance_id="m21-4-benchmark",
        collector_version="0.1.0+benchmark",
        logger=logging.getLogger(f"m21-4.benchmark.{scenario.name}"),
        receipt_queue_capacity=scenario.receipt_capacity,
        planned_rotation_seconds=60,
        backoff=ReconnectBackoff(
            initial_seconds=0.0001,
            maximum_seconds=0.0001,
            jitter_ratio=0,
        ),
        opener=opener,
        backpressure_put_timeout_seconds=0.05,
        backpressure_saturation_timeout_seconds=10,
        post_close_handoff_timeout_seconds=1,
    )
    collector.logger.setLevel(logging.CRITICAL)
    peak_rss = current_rss_bytes() or 0
    peak_fds = _fd_count()
    peak_threads = threading.active_count()
    monitor_stop = asyncio.Event()

    async def monitor() -> None:
        nonlocal peak_rss, peak_fds, peak_threads
        while not monitor_stop.is_set():
            peak_rss = max(peak_rss, current_rss_bytes() or 0)
            current_fds = _fd_count()
            if current_fds is not None:
                peak_fds = max(peak_fds or 0, current_fds)
            peak_threads = max(peak_threads, threading.active_count())
            await asyncio.sleep(0.001)

    monitor_task = asyncio.create_task(monitor())
    started_ns = time.perf_counter_ns()
    try:
        await collector.run(stop)
    finally:
        elapsed_ns = time.perf_counter_ns() - started_ns
        monitor_stop.set()
        await monitor_task
        catalog.close()
    events = _read_raw(root)
    update_ids = [int(event.source_sequence["u"]) for event in events]
    producer_elapsed_ns = max(1, send_times_ns[-1] - send_times_ns[0])
    stats = collector.receipt_queue_stats
    writes = operation_samples["write_latency_ns"]
    return {
        "scenario": scenario.name,
        "configured_events": scenario.events,
        "received_events": len(send_times_ns),
        "persisted_events": len(events),
        "producer_rate_per_second": len(send_times_ns) * 1_000_000_000 / producer_elapsed_ns,
        "persisted_rate_per_second": len(events) * 1_000_000_000 / max(1, elapsed_ns),
        "elapsed_ns": elapsed_ns,
        "connections": opened,
        "receipt_queue_capacity": stats.capacity,
        "receipt_queue_depth": stats.depth,
        "receipt_queue_high_watermark": stats.high_watermark,
        "queue_wait_count": stats.wait_count,
        "queue_wait_p50_ns": stats.wait_p50_ns,
        "queue_wait_p95_ns": stats.wait_p95_ns,
        "queue_wait_p99_ns": stats.wait_p99_ns,
        "writer_batch_capacity": scenario.batch_capacity,
        "append_p50_ns": _percentile(append_samples_ns, 0.50),
        "append_p95_ns": _percentile(append_samples_ns, 0.95),
        "append_p99_ns": _percentile(append_samples_ns, 0.99),
        "write_p50_ns": _percentile(writes, 0.50),
        "write_p95_ns": _percentile(writes, 0.95),
        "write_p99_ns": _percentile(writes, 0.99),
        "drain_p50_ns": _percentile(spool.drain_durations_ns, 0.50),
        "drain_p95_ns": _percentile(spool.drain_durations_ns, 0.95),
        "drain_p99_ns": _percentile(spool.drain_durations_ns, 0.99),
        "fsync_p50_ns": _percentile(fsync_samples_ns, 0.50),
        "fsync_p95_ns": _percentile(fsync_samples_ns, 0.95),
        "fsync_p99_ns": _percentile(fsync_samples_ns, 0.99),
        "seal_p50_ns": _percentile(spool.seal_durations_ns, 0.50),
        "seal_p95_ns": _percentile(spool.seal_durations_ns, 0.95),
        "seal_p99_ns": _percentile(spool.seal_durations_ns, 0.99),
        "peak_rss_bytes": peak_rss,
        "peak_fds": peak_fds,
        "peak_threads": peak_threads,
        "lost_events": scenario.events - len(events),
        "duplicate_events": len(update_ids) - len(set(update_ids)),
        "ordering_ok": update_ids == list(range(scenario.events)),
    }


def _scenarios(events: int) -> tuple[Scenario, ...]:
    small = max(100, events // 4)
    return (
        Scenario("baseline", events, 64, 256),
        Scenario("reconnect_burst", events, 8, 64, reconnect_at=small, drain_delay_seconds=0.002),
        Scenario("slow_append", small, 16, 64, append_delay_seconds=0.0001),
        Scenario(
            "slow_fsync",
            small,
            16,
            64,
            fsync_delay_seconds=0.0001,
            durability_interval_seconds=0,
        ),
        Scenario("slow_seal", small, 16, 64, seal_delay_seconds=0.02),
        Scenario("batch_8", small, 16, 8),
        Scenario("batch_64", small, 16, 64),
        Scenario("batch_256", small, 16, 256),
        Scenario("near_capacity", small, 8, 16, drain_delay_seconds=0.003),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=int, default=4_000)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.events < 100:
        raise SystemExit("--events must be at least 100")
    parent = Path(tempfile.mkdtemp(prefix="bmdr-m21-4-benchmark-"))
    try:
        results = [
            asyncio.run(_run_scenario(scenario, parent / scenario.name))
            for scenario in _scenarios(args.events)
        ]
        document = {
            "benchmark": "M21.4 deterministic offline USD-M ingress",
            "fake_websocket": True,
            "production_paths_used": False,
            "results": results,
            "passed": all(
                result["lost_events"] == 0
                and result["duplicate_events"] == 0
                and result["ordering_ok"] is True
                and result["receipt_queue_high_watermark"]
                <= result["receipt_queue_capacity"]
                for result in results
            ),
        }
        rendered = json.dumps(document, indent=2, sort_keys=True) + "\n"
        if args.output is None:
            print(rendered, end="")
        else:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered, encoding="utf-8")
        return 0 if document["passed"] else 1
    finally:
        shutil.rmtree(parent)


if __name__ == "__main__":
    raise SystemExit(main())
