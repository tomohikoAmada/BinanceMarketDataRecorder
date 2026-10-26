"""Offline, finite core-capture baseline; never opens a real exchange connection.

Runs unchanged Spot/USD-M collectors, metrics, order books, Raw sealing and
SQLite against deterministic fixture feeds. Verification is timed separately.
This is not all-feature production capacity or a live-network benchmark.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import logging
import os
import platform
import resource
import sys
import tempfile
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import ClassVar

from binance_market_data_recorder.collector.spot import SpotCollector, SpotCollectorSettings
from binance_market_data_recorder.collector.usdm import UsdMCollector, UsdMCollectorSettings
from binance_market_data_recorder.collector.usdm_side_data import UsdMRestCooldown
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.service.resources import current_rss_bytes
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
)
EPOCH_MS = 1793000000000


def payload(market, symbol, stream, index):
    d = {"s": symbol, "E": EPOCH_MS + index, "T": EPOCH_MS + index}
    if market == "um_perpetual":
        d.update(st=1, ps=symbol)
    if stream == "book_ticker":
        d.update(u=101 + index, b="1000", B="2", a="1001", A="3")
        if market == "um_perpetual":
            d["e"] = "bookTicker"
    elif stream == "agg_trade":
        d.update(e="aggTrade", a=index, p="1000", q="1", f=index, l=index, m=True, M=True)
    else:
        d.update(
            e="depthUpdate",
            U=100 if index == 0 else 101 + index,
            u=101 + index,
            pu=99 if index == 0 else 100 + index,
            b=[["1000", str(1 + index % 7)]],
            a=[["1001", str(2 + index % 7)]],
        )
    return json.dumps(d, separators=(",", ":"), sort_keys=True).encode()


class Snapshot:
    last_update_id = 100

    def to_dict(self):
        return {
            "lastUpdateId": 100,
            "E": EPOCH_MS,
            "T": EPOCH_MS,
            "bids": [[str(1000 - i), "2"] for i in range(100)],
            "asks": [[str(1001 + i), "3"] for i in range(100)],
        }


class Response:
    status = 200
    headers: ClassVar = {"Content-Type": "application/json", "X-MBX-USED-WEIGHT-1M": "20"}

    def data(self):
        return Snapshot()


class Rest:
    def order_book(self, symbol, limit):
        return Response()

    def depth(self, symbol, limit):
        return Response()


class Feed:
    def __init__(self, frames, done):
        self.frames = frames
        self.position = 0
        self.done = done

    async def recv(self, decode=None):
        if self.position == len(self.frames):
            self.done.set()
            await asyncio.Future()
        result = self.frames[self.position]
        self.position += 1
        return result

    async def close(self, code=1000, reason=""):
        pass


def proc_counters():
    result = {}
    for name in ("stat", "self/io", "pressure/cpu", "pressure/io", "pressure/memory"):
        p = Path("/proc") / name
        if p.exists():
            data = p.read_text()
            result[name] = data.splitlines()[0] if name == "stat" else data
    return result


async def capture(args, root):
    collectors, inputs, finished, connections = [], {}, {}, Counter()
    sizes = {
        "book_ticker": args.events,
        "agg_trade": max(1, args.events // 25),
        "diff_depth": max(2, args.events // 40),
    }
    logger = logging.getLogger("preplan.baseline")
    logger.setLevel(logging.ERROR)
    lock, cooldown = asyncio.Lock(), UsdMRestCooldown()
    for market in ("spot", "um_perpetual"):
        for symbol in SYMBOLS[: args.symbols]:
            for stream, count in sizes.items():
                key = (market, symbol, stream)
                inputs[key] = tuple(payload(*key, i) for i in range(count))
                finished[key] = asyncio.Event()

            @asynccontextmanager
            async def opener(url, m=market):
                wire = url.rsplit("/", 1)[-1]
                sym, channel = wire.split("@", 1)
                stream = {
                    "bookTicker": "book_ticker",
                    "aggTrade": "agg_trade",
                    "depth@100ms": "diff_depth",
                }[channel]
                key = (m, sym.upper(), stream)
                connections[key] += 1
                if connections[key] != 1:
                    raise RuntimeError(f"unexpected fixture reconnect: {key}")
                yield Feed(inputs[key], finished[key])

            common = dict(
                data_root=root,
                symbol=symbol,
                collector_instance_id=f"baseline-{market}-{symbol}",
                collector_version="preplan-baseline",
                rotation_seconds=60,
                durability_interval_seconds=1.0,
            )
            if market == "spot":
                collector = SpotCollector(
                    SpotCollectorSettings(**common),
                    logger=logger,
                    rest_api=Rest(),
                    websocket_opener=opener,
                )
            else:
                collector = UsdMCollector(
                    UsdMCollectorSettings(**common),
                    logger=logger,
                    rest_api=Rest(),
                    websocket_opener=opener,
                    request_lock=lock,
                    cooldown=cooldown,
                )
            collectors.append(collector)

    # All fixture allocation/import/setup happens before the measured interval.
    fixture_manifest = {
        "/".join(k): {
            "frames": len(v),
            "bytes": sum(map(len, v)),
            "sha256": hashlib.sha256(b"\n".join(v)).hexdigest(),
        }
        for k, v in inputs.items()
    }
    stop = asyncio.Event()
    rss_peak, lags = current_rss_bytes() or 0, []

    async def monitor():
        nonlocal rss_peak
        while not stop.is_set():
            target = time.perf_counter() + 0.1
            await asyncio.sleep(0.1)
            lags.append(max(0, time.perf_counter() - target))
            rss_peak = max(rss_peak, current_rss_bytes() or 0)

    before = proc_counters()
    cpu_start, wall_start = time.process_time(), time.perf_counter()
    tasks = [asyncio.create_task(c.run(stop)) for c in collectors]
    monitor_task = asyncio.create_task(monitor())
    try:
        async with asyncio.timeout(args.timeout):
            while True:
                for task in tasks:
                    if task.done():
                        task.result()
                        raise RuntimeError("collector exited before fixture completion")
                counts = [c.readiness_snapshot().event_count for c in collectors]
                ready = all(c.readiness_snapshot().ready for c in collectors)
                if (
                    all(e.is_set() for e in finished.values())
                    and ready
                    and all(n >= sum(sizes.values()) for n in counts)
                ):
                    break
                await asyncio.sleep(0.02)
            ready_at_end = [c.readiness_snapshot().public_dict() for c in collectors]
            feed_wall = time.perf_counter() - wall_start
            feed_cpu = time.process_time() - cpu_start
    finally:
        stop.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=60)
        await monitor_task
    wall, cpu = time.perf_counter() - wall_start, time.process_time() - cpu_start
    after = proc_counters()
    lags.sort()
    result = {
        "wall_seconds_including_drain_seal": wall,
        "cpu_seconds": cpu,
        "capture_wall_seconds": feed_wall,
        "capture_cpu_seconds": feed_cpu,
        "cpu_cores_used": cpu / wall,
        "rss_peak_sampled_bytes": rss_peak,
        "rss_includes_preallocated_fixture": True,
        "event_loop_lag_p99_seconds": lags[int((len(lags) - 1) * 0.99)] if lags else None,
        "event_loop_lag_max_seconds": max(lags, default=0),
        "readiness_before_stop": ready_at_end,
        "fixture": fixture_manifest,
        "operations": [
            {
                "market": c.readiness_snapshot().market,
                "symbol": c.settings.symbol,
                "stream": str(s.stream),
                "queue": asdict(s.receipt_queue_stats),
                "spool": asdict(s.spool.operation_stats()),
            }
            for c in collectors
            for s in c.streams
        ],
        "proc_before": before,
        "proc_after": after,
    }
    expected = sum(len(v) for v in inputs.values())
    result.update(
        expected_ws_frames=expected,
        ws_frames_per_second=expected / wall,
        cpu_us_per_ws_frame=cpu * 1e6 / expected,
    )
    if args.pause_after_capture:
        print("CAPTURE_COMPLETE", flush=True)
        await asyncio.sleep(args.pause_after_capture)

    # Outside capture timing: full generated-input/Raw equality, CRC and hashes.
    verify_wall, verify_cpu = time.perf_counter(), time.process_time()
    catalog = Catalog(ensure_storage_layout(root).catalog)
    chunks = load_source_chunks(layout=ensure_storage_layout(root), catalog=catalog)
    actual, flags = {}, Counter()
    for chunk in chunks:
        for record in iter_source_records(chunk):
            e = record.envelope
            flags.update(e.capture_flags)
            if e.stream == "depth_snapshot":
                continue
            key = (e.market, e.symbol, e.stream)
            data = json.loads(e.raw_payload)
            seq = data["a"] if e.stream == "agg_trade" else data["u"] - 101
            identity = (*key, seq)
            if identity in actual:
                raise RuntimeError(f"duplicate frame: {identity}")
            if e.raw_payload != inputs[key][seq]:
                raise RuntimeError(f"payload mismatch: {identity}")
            actual[identity] = True
    catalog.close()
    if len(actual) != expected or flags.get("malformed", 0):
        raise RuntimeError(f"bad fixture result: {len(actual)}/{expected}, {flags}")
    result["verification"] = {
        "saved_ws_frames": len(actual),
        "expected_ws_frames": expected,
        "exact_payloads_and_crc_and_artifact_hashes_pass": True,
        "capture_flags": dict(flags),
        "wall_seconds": time.perf_counter() - verify_wall,
        "cpu_seconds": time.process_time() - verify_cpu,
        "chunks": len(chunks),
        "stored_bytes": sum(int(c.manifest["stored_bytes"]) for c in chunks),
        "decoded_bytes": sum(int(c.manifest["uncompressed_bytes"]) for c in chunks),
    }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols", type=int, choices=range(1, 11), default=1)
    parser.add_argument("--events", type=int, default=15000)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--pause-after-capture", type=float, default=0)
    parser.add_argument("--temp-parent", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.events < 80 or args.output.exists():
        parser.error("events must be >=80 and output must be new")
    parent = args.temp_parent or Path(tempfile.gettempdir())
    with tempfile.TemporaryDirectory(prefix="bmdr-offline-baseline-", dir=parent) as tmp:
        result = asyncio.run(capture(args, Path(tmp)))
    result.update(
        schema="preplan-core-baseline.v1",
        utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        symbols=SYMBOLS[: args.symbols],
        python=sys.version,
        platform=platform.platform(),
        logical_cpus=os.cpu_count(),
        ru_maxrss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        ru_maxrss_unit="bytes" if sys.platform == "darwin" else "KiB",
        source_package_path=__import__("binance_market_data_recorder").__file__,
        dependencies={d.metadata["Name"]: d.version for d in importlib.metadata.distributions()},
        excluded=[
            "real sockets/TLS/network",
            "auxiliary streams and REST polling",
            "service manager and report worker",
            "archive timer",
            "COIN-M and delivery adapters (not implemented)",
            "live capacity, long-run stability, real traffic distribution",
        ],
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                k: result[k]
                for k in [
                    "symbols",
                    "expected_ws_frames",
                    "wall_seconds_including_drain_seal",
                    "cpu_seconds",
                    "cpu_us_per_ws_frame",
                    "verification",
                ]
            }
        )
    )


if __name__ == "__main__":
    main()
