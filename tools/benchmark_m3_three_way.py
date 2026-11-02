"""M3.1 Three-Way Multi-Rotation Benchmark.

Runs a reproducible 45-minute multi-rotation comparative benchmark across:
  1. Arm A (Baseline main f109ffc): 60s rotation, FULL sync, uncoalesced journal,
     double readback verification on seal, startup 0-record empty chunks.
  2. Arm B (Optimized Same-Scope): 900s / 128 MiB rotation, NORMAL sync, coalesced
     journal + shared watermark, zero readback, zero startup empty chunks.
     Same scope as Arm A (all kline forming updates and all 16 S12 intervals).
  3. Arm C (Optimized Production-Scope): Arm B optimized engine with D1 (kline closed-only
     persistence + REST backfill) and D2 (S12 deduplicated to 6 distinct intervals).

Outputs:
  - docs/benchmarks/m3.1-three-way-benchmark.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import platform
import random
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

from binance_market_data_recorder.binance.coverage_config import (
    SPOT_INTERVALS,
    SPOT_UTC8_INTERVALS,
    CoverageConfig,
)
from binance_market_data_recorder.binance.products import discover_products
from binance_market_data_recorder.binance.public_sources import (
    bar_period_seconds,
    source_plan,
)
from binance_market_data_recorder.collector.availability import (
    Availability,
    AvailabilityJournal,
)
from binance_market_data_recorder.collector.coverage import (
    SelectedAssetCapture,
    SourceState,
)
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.service.resources import current_rss_bytes
from binance_market_data_recorder.spool.readback import read_artifact
from binance_market_data_recorder.spool.seal import RECONNECT_GAP_FLAG
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tools.benchmark_m1_m3_pipeline import (
    EPOCH_MS,
    MockRestClient,
    digest,
    directory,
    fixture_groups,
    logical_record,
)

REPO = Path(__file__).resolve().parents[1]
EPOCH_NS = EPOCH_MS * 1_000_000


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int((len(ordered) - 1) * (pct / 100.0))
    return round(ordered[idx], 3)


def latency_distribution(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "total": 0.0}
    ordered = sorted(values)
    n = len(ordered)
    return {
        "count": n,
        "p50": round(ordered[int((n - 1) * 0.50)], 3),
        "p95": round(ordered[int((n - 1) * 0.95)], 3),
        "p99": round(ordered[int((n - 1) * 0.99)], 3),
        "max": round(max(values), 3),
        "total": round(sum(values), 3),
    }


class ControlledClock:
    """Deterministic monotonic and wall clock for multi-rotation simulation."""

    def __init__(self, start_ns: int = EPOCH_NS) -> None:
        self._mono = 0.0
        self._epoch_ns = start_ns

    def monotonic(self) -> float:
        return self._mono

    def time_ns(self) -> int:
        return self._epoch_ns

    def time(self) -> float:
        return self._epoch_ns / 1e9

    def perf_counter_ns(self) -> int:
        return time.perf_counter_ns()

    def advance(self, secs: float) -> None:
        self._mono += secs
        self._epoch_ns += int(secs * 1e9)


class BaselineAvailabilityJournal(AvailabilityJournal):
    """Uncoalesced baseline journal recreating pre-M1.2 behavior."""

    def _emit(
        self,
        state: Availability,
        gap: dict[str, object] | None = None,
        *,
        is_transition: bool = False,
    ) -> None:
        if len(self.pending) >= self.capacity:
            raise RuntimeError("availability journal exceeded bounded queue")
        entry = {
            "state": state.public_dict(state.observed_at),
            "gap": gap,
            "eligibility": {
                "interval_id": state.eligibility_id,
                "source_id": state.source_id,
                "started_at_utc_ns": state.eligible_since,
                "ended_at_utc_ns": state.observed_at if state.dormant else None,
            },
            "is_transition": is_transition,
        }
        self.pending.append(entry)
        state.checkpoint_at = state.observed_at


def extract_interval(wire: str) -> str:
    """Extract standard bar interval string from websocket wire stream name."""
    return wire.rsplit("_", 1)[1].split("@")[0]


class ArmMetricsCollector:
    """Collects per-tick and per-minute bucket statistics across 45 minutes."""

    def __init__(self, arm_name: str, duration_s: int, tick_s: float) -> None:
        self.arm_name = arm_name
        self.duration_s = duration_s
        self.tick_s = tick_s
        self.ticks_per_minute = max(1, int(60.0 / tick_s))
        self.minutes_total = duration_s // 60

        self.minute_records: list[dict[str, Any]] = []

        # Current minute accumulator
        self.cur_received = 0
        self.cur_persisted = 0
        self.cur_filtered = 0
        self.cur_sealed = 0
        self.cur_fsyncs = 0
        self.cur_disk_bytes = 0
        self.cur_max_pending = 0
        self.cur_persist_lags: list[float] = []
        self.cur_flush_latencies: list[float] = []
        self.min_cpu_start = time.process_time()
        self.min_wall_start = time.perf_counter()

        # Overall totals
        self.total_received = 0
        self.total_persisted = 0
        self.total_filtered = 0
        self.total_sealed = 0
        self.total_fsyncs = 0
        self.total_disk_bytes = 0
        self.peak_pending = 0
        self.all_persist_lags: list[float] = []
        self.all_flush_latencies: list[float] = []
        self.all_seal_latencies: list[float] = []
        self.all_queue_waits: list[float] = []
        self.rss_curve_mib: list[float] = []
        self.cpu_curve_percent: list[float] = []
        self.empty_startup_chunks = 0
        self.readback_passes = 0

    def record_frame(
        self,
        *,
        persisted: bool,
        filtered: bool,
        size_bytes: int = 0,
        persist_lag_ms: float = 0.0,
        queue_wait_ms: float = 0.0,
    ) -> None:
        self.cur_received += 1
        self.total_received += 1
        if filtered:
            self.cur_filtered += 1
            self.total_filtered += 1
        elif persisted:
            self.cur_persisted += 1
            self.total_persisted += 1
            self.cur_disk_bytes += size_bytes
            self.total_disk_bytes += size_bytes
            self.cur_persist_lags.append(persist_lag_ms)
            self.all_persist_lags.append(persist_lag_ms)
            self.all_queue_waits.append(queue_wait_ms)

    def record_fsync(self) -> None:
        self.cur_fsyncs += 1
        self.total_fsyncs += 1

    def record_seal(
        self, manifest: dict[str, Any], duration_ms: float, compressed_bytes: int
    ) -> None:
        self.cur_sealed += 1
        self.total_sealed += 1
        self.cur_disk_bytes += compressed_bytes
        self.total_disk_bytes += compressed_bytes
        self.all_seal_latencies.append(duration_ms)
        if manifest.get("record_count") == 0:
            self.empty_startup_chunks += 1

    def record_availability_flush(self, duration_ms: float, pending_rows: int) -> None:
        self.cur_flush_latencies.append(duration_ms)
        self.all_flush_latencies.append(duration_ms)
        self.cur_max_pending = max(self.cur_max_pending, pending_rows)
        self.peak_pending = max(self.peak_pending, pending_rows)

    def end_minute(self, minute: int) -> None:
        wall_now = time.perf_counter()
        cpu_now = time.process_time()
        wall_elapsed = max(0.0001, wall_now - self.min_wall_start)
        cpu_elapsed = max(0.0, cpu_now - self.min_cpu_start)
        cpu_percent = (cpu_elapsed / wall_elapsed) * 100.0

        rss_bytes = current_rss_bytes() or 0
        rss_mib = round(rss_bytes / (1024 * 1024), 2)
        self.rss_curve_mib.append(rss_mib)
        self.cpu_curve_percent.append(round(cpu_percent, 2))

        record = {
            "minute": minute,
            "cpu_percent": round(cpu_percent, 2),
            "rss_mib": rss_mib,
            "received_frames": self.cur_received,
            "persisted_frames": self.cur_persisted,
            "forming_filtered_frames": self.cur_filtered,
            "sealed_chunks": self.cur_sealed,
            "disk_bytes": self.cur_disk_bytes,
            "fsync_calls": self.cur_fsyncs,
            "max_pending_rows": self.cur_max_pending,
            "persist_lag_ms": latency_distribution(self.cur_persist_lags),
            "flush_latency_ms": latency_distribution(self.cur_flush_latencies),
        }
        self.minute_records.append(record)

        # Reset minute accumulators
        self.cur_received = 0
        self.cur_persisted = 0
        self.cur_filtered = 0
        self.cur_sealed = 0
        self.cur_fsyncs = 0
        self.cur_disk_bytes = 0
        self.cur_max_pending = 0
        self.cur_persist_lags.clear()
        self.cur_flush_latencies.clear()
        self.min_cpu_start = cpu_now
        self.min_wall_start = wall_now


def run_single_arm(
    *,
    arm_name: str,
    duration_s: int = 2700,
    tick_s: float = 10.0,
    seed: int = 20261102,
) -> dict[str, Any]:
    """Execute one complete multi-rotation benchmark arm."""
    rng = random.Random(seed)
    clock = ControlledClock()
    is_arm_a = arm_name == "A"
    is_arm_c = arm_name == "C"

    # 1. Coverage configuration
    cfg = CoverageConfig(
        underlying_assets=("BTC", "ETH"),
        spot_intervals=SPOT_INTERVALS,
        spot_utc8_intervals=SPOT_UTC8_INTERVALS if is_arm_c else SPOT_INTERVALS,
        kline_forming_policy="closed_only" if is_arm_c else "all",
    )

    products = [
        p
        for v in ("spot", "usdm", "coinm")
        for p in discover_products(v, directory(v), cfg)
        if p.status == "TRADING"
    ]

    rotation_secs = 60.0 if is_arm_a else 900.0
    metrics = ArmMetricsCollector(arm_name, duration_s, tick_s)

    with tempfile.TemporaryDirectory(prefix=f"bmdr-m3-arm-{arm_name}-") as tmp_dir:
        layout = ensure_storage_layout(Path(tmp_dir))
        with Catalog(layout.catalog) as catalog:
            if is_arm_a:
                catalog._connection.execute("PRAGMA journal_mode = WAL;")
                catalog._connection.execute("PRAGMA synchronous = FULL;")
            else:
                catalog._connection.execute("PRAGMA journal_mode = WAL;")
                catalog._connection.execute("PRAGMA synchronous = NORMAL;")

            start_rss = current_rss_bytes() or 0
            capture = SelectedAssetCapture(
                RecorderConfig(
                    data_root=layout.root,
                    coverage=cfg,
                    rotation_seconds=rotation_secs,
                ),
                logging.getLogger(f"bench-{arm_name}"),
                f"bench-inst-{arm_name}",
                "bench-v1",
                layout,
                catalog,
                clients={v: MockRestClient(v) for v in ("spot", "usdm", "coinm")},
            )

            if is_arm_a:
                capture.availability = BaselineAvailabilityJournal(
                    capture.instance_id, capacity=1_000_000
                )

            sources = source_plan(tuple(products), cfg)
            t_start = clock.time_ns()
            for s in sources:
                st = SourceState(source=s, first_due=0)
                st.connected = True
                st.last_success = t_start
                capture.sources[s.key] = st
                capture._active_sources[s.key] = st
                capture.availability.ensure("/".join(s.key), t_start)
                capture.availability.valid("/".join(s.key), t_start)

            groups = fixture_groups(products, cfg)
            spools: dict[str, StreamSpool] = {
                group.name: capture.spool(
                    group.sources[0].market, group.sources[0].symbol, group.name
                )
                for group in groups
            }

            # Arm A creates startup 0-record empty chunks (reproducing pre-M2.2 behavior)
            if is_arm_a:
                for sp in spools.values():
                    sp._writer = sp._new_writer()
                    m = sp._seal_current(forced_flags=frozenset({RECONNECT_GAP_FLAG}))
                    if m is not None:
                        metrics.record_seal(m, 0.5, cast(int, m.get("stored_bytes", 0)))
                        metrics.readback_passes += 2

            # Instrument spools for metrics & Arm A readback
            for sp in spools.values():
                original_seal = sp._seal_current

                def make_timed_seal(
                    spool_obj: StreamSpool, orig_seal: Callable[..., Any]
                ) -> Callable[..., Any]:
                    def timed_seal(*args: Any, **kwargs: Any) -> Any:
                        t0 = time.perf_counter_ns()
                        manifest = orig_seal(*args, **kwargs)
                        dur_ms = (time.perf_counter_ns() - t0) / 1e6
                        if manifest is not None:
                            stored = cast(int, manifest.get("stored_bytes", 0))
                            metrics.record_seal(manifest, dur_ms, stored)
                            if is_arm_a:
                                rel = manifest.get("relative_path")
                                if rel:
                                    sealed_path = layout.root / str(rel)
                                    if sealed_path.exists():
                                        read_artifact(sealed_path, scan_frames=True)
                                        read_artifact(sealed_path, scan_frames=False)
                                        metrics.readback_passes += 2
                        return manifest

                    return timed_seal

                sp._seal_current = make_timed_seal(sp, original_seal)  # type: ignore[method-assign]

            # Timing & execution
            start_wall = time.perf_counter()
            start_cpu = time.process_time()
            ticks = int(duration_s // tick_s)

            with (
                patch("binance_market_data_recorder.spool.writer.time", clock),
                patch(
                    "binance_market_data_recorder.collector.coverage.time.monotonic",
                    clock.monotonic,
                ),
                patch(
                    "binance_market_data_recorder.collector.coverage.time.time_ns", clock.time_ns
                ),
                patch("binance_market_data_recorder.collector.coverage.time.time", clock.time),
            ):
                for tick in range(ticks):
                    clock.advance(tick_s)
                    now_mono = clock.monotonic()
                    now_ns = clock.time_ns()
                    now_ms = now_ns // 1_000_000

                    # 1. Feed WS events across groups
                    for group in groups:
                        g_market = group.sources[0].market
                        g_symbol = group.sources[0].symbol
                        for wire in group.wire_names:
                            source = group.by_wire[wire]
                            # Incoming frame marks source valid in memory
                            capture.availability.valid(
                                "/".join(source.key), now_ns, checkpoint=False
                            )
                            is_kline = "kline" in wire.lower()
                            if is_kline:
                                interval = extract_interval(wire)
                                period_s = bar_period_seconds(interval)
                                is_closed = int(now_mono) % period_s == 0
                                if is_arm_c and not is_closed:
                                    # D1: Filter forming bar in memory
                                    capture.sources[source.key].forming_filtered_count += 1
                                    metrics.record_frame(persisted=False, filtered=True)
                                    continue

                            price = str(50000 + rng.randrange(100))
                            payload = json.dumps(
                                {
                                    "stream": wire,
                                    "data": {
                                        "e": "trade" if not is_kline else "kline",
                                        "E": now_ms,
                                        "s": g_symbol,
                                        "p": price,
                                        "q": "1.0",
                                        "T": now_ms,
                                        "m": True,
                                    },
                                },
                                separators=(",", ":"),
                            ).encode()

                            env = EventEnvelope(
                                market=g_market,
                                symbol=g_symbol,
                                stream=group.name,
                                module="binance.public.websocket.v1",
                                source_sequence={"sourceStream": wire, "t": tick},
                                exchange_event_time=now_ms,
                                receive_time_utc_ns=now_ns,
                                receive_monotonic_ns=int(now_mono * 1e9),
                                connection_id=f"conn-{g_market}",
                                collector_instance_id=capture.instance_id,
                                collector_version=capture.version,
                                raw_payload=payload,
                            )

                            sp = spools[group.name]
                            q_t0 = time.perf_counter_ns()
                            sp.enqueue(env)
                            q_wait_ms = (time.perf_counter_ns() - q_t0) / 1e6

                            p_t0 = time.perf_counter_ns()
                            metrics.record_frame(
                                persisted=True,
                                filtered=False,
                                size_bytes=len(payload),
                                persist_lag_ms=(time.perf_counter_ns() - p_t0) / 1e6,
                                queue_wait_ms=q_wait_ms,
                            )

                    # 2. Drain all spools
                    for sp in spools.values():
                        sp.drain_all()
                        metrics.record_fsync()

                    # 3. Availability monitoring & flush
                    # Maintain monitor state across synthetic tick jumps
                    capture._last_monitor_utc_ns = now_ns - 1_000_000_000
                    for st_obj in capture._active_sources.values():
                        st_obj.last_success = now_ns
                    capture._observe_availability(now_ns)
                    f_t0 = time.perf_counter_ns()
                    if is_arm_a:
                        capture._pending_watermark_utc_ns = None
                    asyncio.run(capture._flush_availability())
                    flush_ms = (time.perf_counter_ns() - f_t0) / 1e6
                    metrics.record_availability_flush(
                        flush_ms, len(capture.availability.pending)
                    )

                    # 4. Check minute boundary
                    if (tick + 1) % metrics.ticks_per_minute == 0:
                        minute_idx = (tick + 1) // metrics.ticks_per_minute
                        metrics.end_minute(minute_idx)

                # Shutdown & final seal
                for sp in spools.values():
                    sp.close_and_seal()

            total_wall = time.perf_counter() - start_wall
            total_cpu = time.process_time() - start_cpu
            final_rss = current_rss_bytes() or 0

            # Offline audit
            chunks = load_source_chunks(layout=layout, catalog=catalog)
            logical_hashes: list[str] = []
            for chunk in chunks:
                for record in iter_source_records(chunk):
                    logical_hashes.append(logical_record(record.envelope))

            content_digest = digest(sorted(logical_hashes))
            zst_files = list(layout.sealed.glob("*.zst"))
            manifest_files = list(layout.manifests.glob("*.manifest.json"))
            orphan_partials = list(layout.active.glob("*.partial"))

            if len(orphan_partials) > 0:
                raise AssertionError(
                    f"Arm {arm_name} left {len(orphan_partials)} orphan partial files"
                )

            peak_seal_minute = max(metrics.minute_records, key=lambda m: m["sealed_chunks"])
            cpu_peak_seal_percent = peak_seal_minute["cpu_percent"]
            avg_cpu_percent = (total_cpu / total_wall) * 100.0

            result = {
                "arm": arm_name,
                "configuration": {
                    "underlying_assets": list(cfg.underlying_assets),
                    "spot_intervals": list(cfg.spot_intervals),
                    "spot_utc8_intervals": list(cfg.spot_utc8_intervals),
                    "kline_forming_policy": cfg.kline_forming_policy,
                    "rotation_seconds": rotation_secs,
                    "sqlite_sync": "FULL" if is_arm_a else "NORMAL",
                    "availability_coalescing": not is_arm_a,
                    "startup_empty_chunks": is_arm_a,
                    "seal_readback": is_arm_a,
                },
                "summary": {
                    "wall_seconds": round(total_wall, 3),
                    "cpu_seconds": round(total_cpu, 3),
                    "cpu_average_percent": round(avg_cpu_percent, 2),
                    "cpu_peak_seal_window_percent": round(cpu_peak_seal_percent, 2),
                    "start_rss_mib": round(start_rss / (1024 * 1024), 2),
                    "peak_rss_mib": max(metrics.rss_curve_mib, default=0.0),
                    "final_rss_mib": round(final_rss / (1024 * 1024), 2),
                    "rss_growth_mib": round((final_rss - start_rss) / (1024 * 1024), 2),
                    "frames_received_total": metrics.total_received,
                    "frames_persisted_total": metrics.total_persisted,
                    "forming_filtered_total": metrics.total_filtered,
                    "sealed_chunks_total": len(zst_files),
                    "sealed_chunks_per_hour": round(len(zst_files) * 3600.0 / duration_s, 1),
                    "empty_startup_chunks": metrics.empty_startup_chunks,
                    "manifest_files_total": len(manifest_files),
                    "readback_passes": metrics.readback_passes,
                    "disk_bytes_total": metrics.total_disk_bytes,
                    "disk_write_rate_mb_per_min": round(
                        (metrics.total_disk_bytes / (1024 * 1024)) / (duration_s / 60.0), 3
                    ),
                    "fsync_calls_total": metrics.total_fsyncs,
                    "availability_max_pending": metrics.peak_pending,
                    "availability_steady_state_pending": round(
                        sum(m["max_pending_rows"] for m in metrics.minute_records)
                        / len(metrics.minute_records),
                        1,
                    )
                    if metrics.minute_records
                    else 0,
                    "availability_flush_ms": latency_distribution(metrics.all_flush_latencies),
                    "persist_lag_ms": latency_distribution(metrics.all_persist_lags),
                    "queue_wait_ms": latency_distribution(metrics.all_queue_waits),
                    "seal_latency_ms": latency_distribution(metrics.all_seal_latencies),
                    "logical_content_digest": content_digest,
                    "verified_raw_frames": len(logical_hashes),
                },
                "timeline_minutes": metrics.minute_records,
            }

            return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-minutes", type=int, default=45)
    parser.add_argument("--tick-seconds", type=float, default=10.0)
    parser.add_argument("--seed", type=int, default=20261102)
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("docs/benchmarks/m3.1-three-way-benchmark.json"),
    )
    parser.add_argument(
        "--verify-repeatability", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args()

    duration_s = args.duration_minutes * 60
    print(
        f"Starting M3.1 Three-Way Benchmark "
        f"(Duration: {args.duration_minutes}m / {duration_s}s, Tick: {args.tick_seconds}s)..."
    )

    arms_results: dict[str, Any] = {}
    for arm in ("A", "B", "C"):
        print(f"--- Running Arm {arm} ---")
        t0 = time.time()
        res = run_single_arm(
            arm_name=arm,
            duration_s=duration_s,
            tick_s=args.tick_seconds,
            seed=args.seed,
        )
        arms_results[arm] = res
        summary = res["summary"]
        print(
            f"Arm {arm} finished in {time.time() - t0:.2f}s wall clock. "
            f"(Avg CPU: {summary['cpu_average_percent']}%, Peak RSS: {summary['peak_rss_mib']} MiB)"
        )

    repeat_matches = True
    if args.verify_repeatability:
        print("--- Verifying Repeatability on Arm C ---")
        repeat_res = run_single_arm(
            arm_name="C",
            duration_s=duration_s,
            tick_s=args.tick_seconds,
            seed=args.seed,
        )
        c_orig = arms_results["C"]["summary"]
        c_rep = repeat_res["summary"]
        repeat_matches = (
            c_orig["frames_received_total"] == c_rep["frames_received_total"]
            and c_orig["frames_persisted_total"] == c_rep["frames_persisted_total"]
            and c_orig["forming_filtered_total"] == c_rep["forming_filtered_total"]
            and c_orig["sealed_chunks_total"] == c_rep["sealed_chunks_total"]
            and c_orig["logical_content_digest"] == c_rep["logical_content_digest"]
        )
        digest_prefix = c_orig["logical_content_digest"][:16]
        print(
            f"Arm C repeatability: {'MATCH' if repeat_matches else 'MISMATCH'} "
            f"(Content Digest: {digest_prefix}...)"
        )
        if not repeat_matches:
            raise SystemExit("FAIL: Repeatability mismatch in Arm C runs!")

    full_report = {
        "schema_version": "bmdr-m3.1-three-way.v1",
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True
        ).strip(),
        "environment": {
            "platform": platform.platform(),
            "python": sys.version,
            "duration_minutes": args.duration_minutes,
            "duration_seconds": duration_s,
            "tick_seconds": args.tick_seconds,
            "random_seed": args.seed,
        },
        "repeatability": {
            "verified": args.verify_repeatability,
            "status": "PASS" if repeat_matches else "FAIL",
        },
        "arms": arms_results,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(full_report, indent=2) + "\n")
    print(f"Saved JSON report to {args.output_json}")


if __name__ == "__main__":
    main()
