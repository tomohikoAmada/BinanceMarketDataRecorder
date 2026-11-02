"""M3.1 real-time, offline three-way capture benchmark (sequential fresh processes).

Default: A=f109ffc, B=current source with original scope, C=production scope;
45 wall-clock minutes EACH. Only network peers are synthetic. No clock patches,
manual health updates, emulated baseline, or production data directories.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import subprocess
import sys
import tarfile
import tempfile
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

from binance_market_data_recorder.binance.coverage_config import SPOT_INTERVALS, CoverageConfig
from binance_market_data_recorder.binance.products import discover_products
from binance_market_data_recorder.binance.public_rest import RestObservation
from binance_market_data_recorder.binance.public_sources import Source
from binance_market_data_recorder.binance.public_websocket import (
    StreamGroup,
)
from binance_market_data_recorder.binance.request_budget import RECORDER_WINDOWS, RequestBudget
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture, SourceState
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.domain.product import Product
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.paths import default_data_root
from binance_market_data_recorder.service.resources import current_rss_bytes
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tools.benchmark_m1_m3_pipeline import (
    EPOCH_MS,
    BenchmarkCapture,
    InstrumentedCatalog,
    MockRestClient,
    StatsCollector,
    bar_bounds,
    build_fixture_frames,
    directory,
    logical_record,
)

if TYPE_CHECKING:
    from binance_market_data_recorder.binance.public_websocket import (
        CombinedWebSocketStream,
        WebSocketConnection,
    )


REPO = Path(__file__).resolve().parents[1]
DISTINCT_UTC8 = ("6h", "12h", "1d", "3d", "1w", "1M")


def distribution(counts: Counter[int]) -> dict[str, int | float]:
    """Exact distribution after quantizing observations to integer microseconds."""
    n = sum(counts.values())
    result: dict[str, int | float] = {"count": n}
    for label, pct in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99)):
        accumulated = 0
        result[label] = 0.0
        for value, count in sorted(counts.items()):
            accumulated += count
            if accumulated > int((n - 1) * pct):
                result[label] = value / 1000
                break
    result["max"] = max(counts, default=0) / 1000
    result["total"] = sum(k * v for k, v in counts.items()) / 1000
    return result


class Fingerprint:
    """Bounded multiset fingerprint: frame count + sum(SHA256) modulo 2**256.

    Independent of thread ordering and chunk boundaries. Compare actual decoded
    Raw to the expected input as well as comparing arms; this is not a file hash.
    """

    def __init__(self) -> None:
        self.count = 0
        self.value = 0

    def add(self, value: bytes) -> None:
        hashed = int.from_bytes(hashlib.sha256(value).digest())
        self.count += 1
        self.value = (self.value + hashed) % (1 << 256)

    def public(self) -> dict[str, int | str]:
        return {"count": self.count, "sha256_sum_mod_2_256": f"{self.value:064x}"}


def frame_at(template: bytes, tick: int, seed: int) -> bytes:
    """Valid, deterministic native payload; bar boundaries follow exchange time.

    The exchange clock starts at a fixed Monday. Receive clocks remain real.
    One update per wire per second is a declared synthetic load, not a measured
    Binance rate. Every common wire has identical bytes in A/B/C.
    """
    frame = json.loads(template)
    data = frame["data"]
    now = EPOCH_MS + (tick + 1) * 1000
    if isinstance(data, dict):
        for name in ("E", "T", "t"):
            if name in data:
                data[name] = now
        if "p" in data:
            data["p"] = str(50000 + (seed + tick) % 100)
        if "U" in data:
            data.update(U=100 + tick * 2, u=101 + tick * 2)
            if "pu" in data:
                data["pu"] = 99 + tick * 2
        elif "u" in data:
            data["u"] = 100 + tick
        if "a" in data and isinstance(data["a"], int):
            data.update(a=100 + tick, f=100 + tick, l=100 + tick)
        if "k" in data:
            k = data["k"]
            interval = k["i"]
            first, following = bar_bounds(interval, 0, frame["stream"].endswith("@+08:00"))
            # During a 45-minute run only fixed sub-hour periods advance.
            period = following - first + 1
            boundary = now > first and (now - first) % period == 0
            opened = first + ((now - first - int(boundary)) // period) * period
            k.update(t=opened, T=opened + period - 1, x=boundary)
    return json.dumps(frame, separators=(",", ":")).encode()


class PacedConnection:
    def __init__(self, owner: TimedCapture, group: StreamGroup) -> None:
        self.owner = owner
        self.group = group
        self.templates = owner.templates[group.name]
        self.wires = len(group.wire_names)
        self.index = 0

    async def recv(self, decode: bool | None = None) -> bytes:
        tick, ordinal = divmod(self.index, self.wires)
        if tick >= self.owner.seconds:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        await asyncio.sleep(max(0, self.owner.started + tick - time.monotonic()))
        value = frame_at(self.templates[ordinal], tick, self.owner.seed)
        self.index += 1
        if (
            tick == 119
            and self.group.sources[0].market == "spot"
            and json.loads(value)["stream"] == "btcusdt@kline_1m"
        ):
            # Same missed close in every arm. C must automatically REST-repair it.
            return await self.recv(decode)
        self.owner.received.add(value)
        data = json.loads(value)["data"]
        if not (
            self.owner.closed_only and isinstance(data, dict) and "k" in data and not data["k"]["x"]
        ):
            self.owner.expected_ws.add(value)
        return value

    async def close(self, code: int = 1000, reason: str = "") -> None:
        pass


class TimedCapture(BenchmarkCapture):
    def __init__(self, *args: Any, seconds: int, seed: int, arm: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.seconds, self.seed, self.arm = seconds, seed, arm
        self.closed_only = arm == "C"
        self.started = time.monotonic()
        self.templates = self.feed
        self.received, self.expected_ws = Fingerprint(), Fingerprint()
        self.ws_persisted = self.rest_persisted = self.backfilled = 0
        self.expected_logical = Fingerprint()
        self.expected_rest = Fingerprint()
        self.pending_before_flush = 0
        self.last_pending_after = 0
        self.flush_after: list[int] = []
        self.stats.event_sink = self.on_persisted

    def on_persisted(self, event: EventEnvelope) -> None:
        if "malformed" in event.capture_flags:
            raise AssertionError("invalid fixture reached Raw")
        self.expected_logical.add(logical_record(event).encode())
        if event.module == "binance.public.websocket.v1":
            self.ws_persisted += 1
        else:
            self.rest_persisted += 1
            if "closed_bar_backfill" in event.capture_flags:
                rows = json.loads(event.raw_payload)["response"]["model"]
                assert rows[0][0] == EPOCH_MS + 60000
                assert rows[0][6] == EPOCH_MS + 119999
                assert event.receive_time_utc_ns // 1_000_000 > rows[0][6]
                self.backfilled += 1
            self.expected_rest.add(logical_record(event).encode())

    def spool(self, market: str, symbol: str, stream: str) -> StreamSpool:
        spool = super().spool(market, symbol, stream)
        # Keep real hash-based phase staggering, not M1.0's full-first-period writer.
        spool._writer_factory = RawChunkWriter
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
            yield PacedConnection(self, group)

        collector.opener = opener
        return collector

    def _schedule_kline_backfill(
        self,
        source: Source,
        start_time_ms: int,
        end_time_ms: int,
        interval: str,
    ) -> None:
        # B must preserve A's persistence scope, including the deliberately missed
        # close. Automatic closed-bar repair is part of D1, enabled in C only.
        if self.arm != "B":
            super()._schedule_kline_backfill(source, start_time_ms, end_time_ms, interval)

    async def poll_once(self, state: SourceState) -> RestObservation:
        # Run real request representation + validation + persistence. Fixed quotas
        # and deadlines avoid request-count differences caused by faster engines.
        if self.rest_completed[state.source.key] >= self.rest_remaining.get(state.source.key, 0):
            state.next_due = float("inf")
            return None  # type: ignore[return-value]
        result = await SelectedAssetCapture.poll_once(self, state)
        if not result.valid:
            raise AssertionError(f"invalid REST fixture: {state.source.key}: {result.error}")
        key = state.source.key
        self.rest_completed[key] += 1
        interval = max(1.0, state.source.cadence_seconds)
        number = self.rest_completed[key]
        state.next_due = (
            self.started + number * interval if number < self.rest_remaining[key] else float("inf")
        )
        return result

    async def _flush_availability(self, *, all_pending: bool = False) -> None:
        self.pending_before_flush = max(self.pending_before_flush, len(self.availability.pending))
        await super()._flush_availability(all_pending=all_pending)
        self.last_pending_after = len(self.availability.pending)
        self.flush_after.append(self.last_pending_after)


def no_network(*args: Any, **kwargs: Any) -> None:
    raise AssertionError("offline benchmark attempted a network connection")


async def run_arm(arm: str, seconds: int, seed: int, root: Path) -> dict[str, Any]:
    options: dict[str, Any] = {"underlying_assets": ("BTC", "ETH")}
    if arm != "A":
        options.update(
            spot_utc8_intervals=DISTINCT_UTC8 if arm == "C" else SPOT_INTERVALS,
            kline_forming_policy="closed_only" if arm == "C" else "all",
        )
    cfg = CoverageConfig(**options)
    products = [
        p
        for venue in ("spot", "usdm", "coinm")
        for p in discover_products(venue, directory(venue), cfg)
        if p.status == "TRADING"
    ]
    feed, _ = build_fixture_frames(products, cfg, 4, seed)
    stats = StatsCollector()
    layout = ensure_storage_layout(root)
    counters = {"fsync": 0, "raw_bytes": 0}
    original_fsync, original_write, original_writev = os.fsync, os.write, os.writev

    def fsync(fd: int) -> None:
        original_fsync(fd)
        counters["fsync"] += 1

    def write(fd: int, value: Any) -> int:
        count = original_write(fd, value)
        counters["raw_bytes"] += count
        return count

    def writev(fd: int, values: Any) -> int:
        count = original_writev(fd, values)
        counters["raw_bytes"] += count
        return count

    budget = RequestBudget({k: (1_000_000, s) for k, (_, s) in RECORDER_WINDOWS.items()})
    with InstrumentedCatalog(layout.catalog, stats) as catalog:
        owner = TimedCapture(
            RecorderConfig(
                data_root=root, coverage=cfg, rotation_seconds=60.0 if arm == "A" else 900.0
            ),
            logging.getLogger("m3"),
            "m3-benchmark",
            "m3-benchmark",
            layout,
            catalog,
            stats=stats,
            feed=feed,
            seconds=seconds,
            seed=seed,
            arm=arm,
            clients={v: MockRestClient(v) for v in ("spot", "usdm", "coinm")},
        )
        stop = asyncio.Event()
        samples: list[dict[str, Any]] = []
        minutes: list[dict[str, Any]] = []
        histograms: dict[str, Counter[int]] = {k: Counter() for k in stats.latencies}
        previous: Counter[str] = Counter()
        start_wall, start_cpu = time.perf_counter(), time.process_time()
        start_rss = current_rss_bytes() or 0
        previous_wall, previous_cpu = start_wall, start_cpu
        sealed_bytes = 0
        total_seals = 0

        def sample() -> None:
            nonlocal previous_wall, previous_cpu, sealed_bytes, total_seals
            wall, cpu = time.perf_counter(), time.process_time()
            sealed_through = len(stats.seals)
            for manifest in stats.seals[:sealed_through]:
                # Manifests already pass through the instrumented os.write.
                # Compression writes via FileIO, so add that separate output once.
                sealed_bytes += int(str(manifest["stored_bytes"]))
            total_seals += sealed_through
            del stats.seals[:sealed_through]
            row = {
                "elapsed_seconds": round(wall - start_wall, 3),
                "cpu_percent": round(100 * (cpu - previous_cpu) / (wall - previous_wall), 3),
                "rss_mib": round((current_rss_bytes() or 0) / 1024**2, 3),
                "received_frames": owner.received.count,
                "persisted_ws_frames": owner.ws_persisted,
                "persisted_rest_frames": owner.rest_persisted,
                "forming_filtered_count": sum(
                    getattr(s, "forming_filtered_count", 0) for s in owner.sources.values()
                ),
                "sealed_chunks": total_seals,
                "seal_window": sealed_through > 0,
                "raw_artifact_write_bytes": counters["raw_bytes"] + sealed_bytes,
                "python_fsync_calls": counters["fsync"],
                "pending_before_flush_max": owner.pending_before_flush,
                "pending_after_flush": owner.last_pending_after,
                "pending_after_flush_max": max(owner.flush_after, default=0),
                "flushes_returned_to_zero": owner.flush_after.count(0),
            }
            samples.append(row)
            previous_wall, previous_cpu = wall, cpu
            owner.pending_before_flush = 0
            owner.flush_after.clear()

        def end_minute() -> None:
            current: Counter[str] = Counter(
                {
                    k: samples[-1][k]
                    for k in (
                        "received_frames",
                        "persisted_ws_frames",
                        "persisted_rest_frames",
                        "forming_filtered_count",
                        "sealed_chunks",
                        "raw_artifact_write_bytes",
                        "python_fsync_calls",
                    )
                }
            )
            row: dict[str, Any] = {
                "elapsed_seconds": samples[-1]["elapsed_seconds"],
                **{k: current[k] - previous[k] for k in current},
            }
            for name, values in stats.latencies.items():
                counts = Counter(round(v * 1000) for v in values)
                row[name + "_ms"] = distribution(counts)
                histograms[name].update(counts)
                values.clear()
            previous.clear()
            previous.update(current)
            minutes.append(row)

        verifier_stop = asyncio.Event()
        verification = {"processed_files": 0, "failed_files": 0}

        async def verify_baseline() -> None:
            from binance_market_data_recorder.service.verification import verify_new

            while True:
                result = await asyncio.to_thread(verify_new, layout, catalog)
                verification["processed_files"] += int(str(result["processed_files"]))
                verification["failed_files"] += int(str(result["failed_this_batch"]))
                assert verification["failed_files"] == 0
                if verifier_stop.is_set() and result["processed_files"] == 0:
                    return
                await asyncio.sleep(1)

        async def capture() -> None:
            try:
                await owner.run(stop)
            finally:
                verifier_stop.set()

        async def monitor() -> None:
            for second in range(1, seconds + 1):
                await asyncio.sleep(max(0, owner.started + second - time.monotonic()))
                sample()
                if second % 60 == 0:
                    end_minute()
                    print(json.dumps({"arm": arm, **samples[-1]}), flush=True)
            # Every input has a fixed count. Finish the real bounded queues/polls
            # before asking the supervisor to perform its normal shutdown.
            async with asyncio.timeout(120):
                while (
                    any(
                        g.capture_progress()["received_frames"]
                        != (
                            len(owner.templates[g.stream_name]) // 4 * seconds
                            - int(
                                seconds >= 120
                                and g.market == "spot"
                                and g.symbol == "BTCUSDT"
                                and "btcusdt@kline_1m" in g.combined_streams
                            )
                        )
                        for g in owner.groups
                    )
                    or any(owner.rest_completed[k] != n for k, n in owner.rest_remaining.items())
                    or any(
                        getattr(state, "backfill_start_ms", None) is not None
                        for state in owner.sources.values()
                    )
                ):
                    if time.monotonic() - owner.started > seconds + 110:
                        print(
                            "Incomplete REST quotas:",
                            [
                                (k, owner.rest_completed[k], n)
                                for k, n in owner.rest_remaining.items()
                                if owner.rest_completed[k] != n
                            ],
                            flush=True,
                        )
                    await asyncio.sleep(0.05)
            stop.set()

        with (
            patch("socket.socket.connect", no_network),
            patch("os.fsync", fsync),
            patch("os.write", write),
            patch("os.writev", writev),
            patch(
                "binance_market_data_recorder.collector.coverage.shared_request_budget",
                return_value=budget,
            ),
        ):
            for venue in ("spot", "usdm", "coinm"):
                await owner.reconcile(venue, directory(venue))
            # Each concrete REST source is included, at its declared cadence.
            # Freeze finite slots and parameters; no live network quotas apply.
            for key, state in owner.sources.items():
                if state.source.transport == "rest" and not state.dormant:
                    interval = max(1.0, state.source.cadence_seconds)
                    owner.rest_remaining[key] = (
                        max(1, int((seconds - 1) // interval) + 1)
                        if state.source.cadence_seconds
                        else 1
                    )
                    state.next_due = 0
            owner.started = time.monotonic()
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(capture())
                if arm == "A":
                    tasks.create_task(verify_baseline())
                tasks.create_task(monitor())
            sample()
            end_minute()
        wall, cpu = time.perf_counter() - start_wall, time.process_time() - start_cpu
        pragmas = {
            name: catalog._connection.execute(f"PRAGMA {name}").fetchone()[0]
            for name in ("journal_mode", "synchronous")
        }
        assert pragmas["synchronous"] == (2 if arm == "A" else 1)
        # Audit only after capture/metrics stop. This readback is an offline check.
        actual_ws, actual_rest, actual_logical = Fingerprint(), Fingerprint(), Fingerprint()
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        inventory = []
        for chunk in chunks:
            count = 0
            for record in iter_source_records(chunk):
                event = record.envelope
                if "malformed" in event.capture_flags:
                    raise AssertionError("malformed Raw")
                encoded = logical_record(event).encode()
                actual_logical.add(encoded)
                if event.module == "binance.public.websocket.v1":
                    actual_ws.add(event.raw_payload)
                else:
                    actual_rest.add(encoded)
                count += 1
            assert count == chunk.manifest["record_count"]
            inventory.append(
                {
                    k: chunk.manifest[k]
                    for k in (
                        "chunk_id",
                        "market",
                        "symbol",
                        "stream",
                        "record_count",
                        "stored_bytes",
                        "stored_sha256",
                        "uncompressed_bytes",
                        "uncompressed_sha256",
                    )
                }
            )
        assert actual_ws.public() == owner.expected_ws.public(), (
            "WS content",
            actual_ws.public(),
            owner.expected_ws.public(),
        )
        assert actual_rest.public() == owner.expected_rest.public()
        expected_backfill = int(arm == "C" and seconds >= 180)
        assert owner.backfilled == expected_backfill, (
            "backfill count",
            owner.backfilled,
            expected_backfill,
        )
        assert actual_rest.count == sum(owner.rest_remaining.values()) + expected_backfill
        assert owner.received.count - owner.expected_ws.count == sum(
            getattr(state, "forming_filtered_count", 0) for state in owner.sources.values()
        )
        assert actual_logical.public() == owner.expected_logical.public()
        assert not list(layout.active.glob("*.partial"))
        assert len(chunks) == total_seals
        if arm == "A":
            assert verification["processed_files"] == len(chunks)
        result = {
            "arm": arm,
            "platform": platform.platform(),
            "python": sys.version,
            "runtime_module": __import__("binance_market_data_recorder").__file__,
            "coverage": cfg.model_dump(mode="json"),
            "sqlite_pragmas": pragmas,
            "duration_seconds": seconds,
            "wall_seconds": wall,
            "cpu_seconds": cpu,
            "cpu_average_percent": cpu / wall * 100,
            "cpu_peak_seal_window_percent": max(
                (r["cpu_percent"] for r in samples if r["seal_window"]), default=0
            ),
            "start_rss_mib": start_rss / 1024**2,
            "peak_rss_mib": max(r["rss_mib"] for r in samples),
            "input_ws": owner.received.public(),
            "expected_ws": owner.expected_ws.public(),
            "actual_ws": actual_ws.public(),
            "actual_rest": actual_rest.public(),
            "closed_bar_backfill_frames": owner.backfilled,
            "actual_logical": actual_logical.public(),
            "rest_quotas": [
                {"key": k, "count": v} for k, v in sorted(owner.rest_remaining.items())
            ],
            "availability_pending_peak_rows": stats.pending_peak,
            "availability_steady_pending_max": max(
                (r["pending_after_flush_max"] for r in samples[300:-1]), default=0
            ),
            "summary": samples[-1],
            "latencies_ms": {k: distribution(v) for k, v in histograms.items()},
            "samples_seconds": samples,
            "timeline_minutes": minutes,
            "chunks": inventory,
            "audit": "PASS",
            "baseline_background_verification": verification,
        }
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-seconds", type=int, default=2700)
    parser.add_argument("--seed", type=int, default=20261102)
    parser.add_argument("--arm", choices=("A", "B", "C"))
    parser.add_argument("--data-root", type=Path)
    parser.add_argument(
        "--output-json", type=Path, default=REPO / "docs/benchmarks/m3.1-three-way-benchmark.json"
    )
    args = parser.parse_args()
    if args.duration_seconds < 1:
        parser.error("duration must be positive")
    if args.arm:
        if args.data_root is None:
            parser.error("worker needs --data-root")
        result = asyncio.run(run_arm(args.arm, args.duration_seconds, args.seed, args.data_root))
        args.output_json.write_text(json.dumps(result, indent=2) + "\n")
        return
    scratch = default_data_root() / "benchmarks"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="m3-review-", dir=scratch) as tmp:
        work = Path(tmp)
        baseline = work / "baseline"
        baseline.mkdir()
        archive = work / "source.tar"
        subprocess.run(
            ["git", "archive", "f109ffc", "src", "-o", str(archive)], cwd=REPO, check=True
        )
        with tarfile.open(archive) as tar:
            tar.extractall(baseline, filter="data")
        arms = {}
        for arm in ("A", "B", "C"):
            output = work / f"{arm}.json"
            env = dict(os.environ)
            runtime = baseline if arm == "A" else REPO
            env["PYTHONPATH"] = os.pathsep.join((str(runtime / "src"), str(REPO)))
            subprocess.run(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "--arm",
                    arm,
                    "--duration-seconds",
                    str(args.duration_seconds),
                    "--seed",
                    str(args.seed),
                    "--data-root",
                    str(work / f"data-{arm}"),
                    "--output-json",
                    str(output),
                ],
                env=env,
                cwd=REPO,
                check=True,
            )
            arms[arm] = json.loads(output.read_text())
            # Save progress after each successful, fully audited arm.
            args.output_json.write_text(
                json.dumps({"status": "IN_PROGRESS", "arms": arms}, indent=2) + "\n"
            )
        assert arms["A"]["input_ws"] == arms["B"]["input_ws"]
        assert arms["A"]["actual_ws"] == arms["B"]["actual_ws"]
        assert arms["A"]["actual_rest"] == arms["B"]["actual_rest"]
        report = {
            "schema_version": "bmdr-m3.1-three-way.v2",
            "status": "PASS" if args.duration_seconds >= 2700 else "SMOKE_PASS",
            "qualification_duration_met": args.duration_seconds >= 2700,
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "seed": args.seed,
            "candidate_src_tree": subprocess.check_output(
                ["git", "rev-parse", "HEAD:src"], cwd=REPO, text=True
            ).strip(),
            "baseline_src_tree": subprocess.check_output(
                ["git", "rev-parse", "f109ffc:src"], cwd=REPO, text=True
            ).strip(),
            "candidate_git_sha": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
            ).strip(),
            "baseline_git_sha": subprocess.check_output(
                ["git", "rev-parse", "f109ffc"], cwd=REPO, text=True
            ).strip(),
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "helper_sha256": hashlib.sha256(
                (REPO / "tools/benchmark_m1_m3_pipeline.py").read_bytes()
            ).hexdigest(),
            "measurement_scope": {
                "clock": "real wall and monotonic; sequential fresh subprocesses",
                "load": "BTC/ETH fixture sources; one WS update/wire/second; native REST cadence",
                "fault": "omit BTCUSDT Spot 1m close at 120s in all arms; C REST-repairs at 180s",
                "io": "Python write/writev + compression bytes; excludes SQLite/physical IO",
                "fsync": "Python os.fsync calls incl. directories; excludes SQLite C calls",
                "cpu": "process CPU / real elapsed, 100% = one core; 1s samples for seal windows",
                "rss": "fresh process; 1s samples; includes harness overhead; no VPS extrapolation",
                "audit": "offline after timing; count + modular SHA256 multiset fingerprint",
                "baseline": "f109ffc capture/seal + verify_new thread; combined process CPU",
            },
            "arms": arms,
        }
        args.output_json.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
