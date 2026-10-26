"""P0 finite, paced, all-current-runtime-feature benchmark in a fresh temporary root.

Only network boundaries and temporary archive-volume discovery are injected.
Never starts a production unit, opens production data, or changes installed code.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import logging
import platform
import resource
import struct
import subprocess
import sys
import tempfile
import time
from collections import Counter
from contextlib import ExitStack, asynccontextmanager, suppress
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research/2026-10-26"))
import preplan_baseline as core

from binance_market_data_recorder.archive import archive_drain
from binance_market_data_recorder.binance.usdm.side_data_rest import (
    FIVE_MINUTE_RETENTION,
    RestSideDataKind,
)
from binance_market_data_recorder.collector.usdm_side_data import RestSideDataPoller
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.normalize.pipeline import Normalizer
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.service import runtime as runtime_module
from binance_market_data_recorder.service.resources import current_rss_bytes
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.forecast import StorageForecaster
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.archive_support import FixedVolumes, prepare_archive


def _cursor_model(kind: RestSideDataKind, timestamp: int) -> dict[str, object]:
    common: dict[str, object] = {"timestamp": timestamp}
    if kind is RestSideDataKind.OPEN_INTEREST_STATISTICS:
        return {
            **common,
            "symbol": "BTCUSDT",
            "sumOpenInterest": "1",
            "sumOpenInterestValue": "2",
        }
    if kind is RestSideDataKind.TAKER_BUY_SELL_VOLUME:
        return {
            **common,
            "buySellRatio": "1",
            "buyVol": "2",
            "sellVol": "2",
        }
    if kind in {
        RestSideDataKind.GLOBAL_LONG_SHORT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO,
        RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO,
    }:
        return {
            **common,
            "symbol": "BTCUSDT",
            "longShortRatio": "1",
            "longAccount": "0.5",
            "shortAccount": "0.5",
        }
    return {
        **common,
        "pair": "BTCUSDT",
        "contractType": "PERPETUAL",
        "indexPrice": "1",
        "futuresPrice": "1",
        "basis": "0",
        "basisRate": "0",
        "annualizedBasisRate": "",
    }


def digest_frame(digest, frame):
    digest.update(struct.pack("!I", len(frame)))
    digest.update(frame)


class Model:
    def __init__(self, data):
        self.value = data

    def to_dict(self):
        return self.value


class Response:
    status = 200
    headers: ClassVar = {"Content-Type": "application/json", "X-MBX-USED-WEIGHT-1M": "1"}

    def __init__(self, data):
        self.value = data

    def data(self):
        if isinstance(self.value, list):
            return [Model(row) for row in self.value]
        return Model(self.value)


class Api(core.Rest):
    def __init__(self, fixtures):
        self.fixtures = fixtures
        self.calls = Counter()

    def response(self, name, symbol=None):
        self.calls[name] += 1
        value = json.loads((self.fixtures / f"{name}.json").read_text())
        if symbol:
            value = json.loads(json.dumps(value).replace("BTCUSDT", symbol))
        return Response(value)

    def exchange_info(self, symbol=None, **kwargs):
        self.calls["spot_exchange_info"] += 1
        return Response(
            {
                "serverTime": core.EPOCH_MS,
                "rateLimits": [],
                "symbols": [
                    {
                        "symbol": symbol,
                        "baseAsset": symbol[:-4],
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                        "orderTypes": ["LIMIT", "MARKET"],
                        "filters": [],
                    }
                ],
            }
        )

    def mark_price(self, symbol=None):
        return self.response("premium_index", symbol)

    def get_funding_rate_history(self, symbol=None, *args):
        return self.response("funding_history", symbol)

    def get_funding_rate_info(self):
        return self.response("funding_info")

    def open_interest(self, symbol=None):
        return self.response("open_interest", symbol)

    def exchange_information(self):
        return self.response("exchange_info")

    def periods(self, kind, *args):
        self.calls[kind.value] += 1
        symbol, limit, start, end = args[0], args[-3], args[-2], args[-1]
        rows = [_cursor_model(kind, stamp) for stamp in range(start, end + 1, 300000)][:limit]
        return Response(json.loads(json.dumps(rows).replace("BTCUSDT", symbol)))

    def open_interest_statistics(self, *args):
        return self.periods(RestSideDataKind.OPEN_INTEREST_STATISTICS, *args)

    def taker_buy_sell_volume(self, *args):
        return self.periods(RestSideDataKind.TAKER_BUY_SELL_VOLUME, *args)

    def long_short_ratio(self, *args):
        return self.periods(RestSideDataKind.GLOBAL_LONG_SHORT_RATIO, *args)

    def top_trader_long_short_ratio_accounts(self, *args):
        return self.periods(RestSideDataKind.TOP_LONG_SHORT_ACCOUNT_RATIO, *args)

    def top_trader_long_short_ratio_positions(self, *args):
        return self.periods(RestSideDataKind.TOP_LONG_SHORT_POSITION_RATIO, *args)

    def basis(self, *args):
        return self.periods(RestSideDataKind.BASIS, *args)


def counters():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    children = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "wall": time.perf_counter(),
        "cpu": time.process_time(),
        "child_cpu": children.ru_utime + children.ru_stime,
        "voluntary_switches": usage.ru_nvcsw,
        "involuntary_switches": usage.ru_nivcsw,
        "proc": core.proc_counters(),
    }


async def measure(args, parent):
    prepared = prepare_archive(parent, chunk_count=0)
    root, layout = prepared.layout.root, prepared.layout
    fixtures = args.repo / "tests/fixtures/binance/usdm"
    api = Api(fixtures)
    # A steady-state fixture starts at the previous closed period, rather than
    # synthesizing a month of cold-start catch-up inside the measured interval.
    last_closed = core.EPOCH_MS // 300000 * 300000 - 300000
    with Catalog(layout.catalog) as catalog:
        for symbol in core.SYMBOLS[: args.symbols]:
            for kind, (retention, window) in FIVE_MINUTE_RETENTION.items():
                catalog.advance_side_data_cursor(
                    kind=kind.value,
                    symbol=symbol,
                    last_persisted_period_timestamp=last_closed - 300000,
                    updated_at_utc_ns=core.EPOCH_MS * 1_000_000,
                    source_retention_window=retention,
                    retention_window_ms=window,
                )
    duration = args.warmup + args.seconds
    inputs, expected, sent, opened = {}, {}, Counter(), Counter()
    intervals = {
        "book_ticker": 0.02,
        "agg_trade": 0.5,
        "diff_depth": 0.2,
        "mark_price": 1.0,
        "liquidation": 20.0,
    }
    for market in ("spot", "um_perpetual"):
        for symbol in core.SYMBOLS[: args.symbols]:
            for stream, interval in intervals.items():
                if market == "spot" and stream in ("mark_price", "liquidation"):
                    continue
                key = (market, symbol, stream)
                count = max(2 if stream == "diff_depth" else 1, int(duration / interval))
                if stream in ("mark_price", "liquidation"):
                    frame = (fixtures / f"{stream}.json").read_bytes().strip()
                    frames = tuple(frame.replace(b"BTCUSDT", symbol.encode()) for _ in range(count))
                else:
                    frames = tuple(core.payload(*key, index) for index in range(count))
                inputs[key] = frames
                digest = hashlib.sha256()
                for frame in frames:
                    digest_frame(digest, frame)
                expected["/".join(key)] = {"frames": count, "sha256": digest.hexdigest()}
    anchor = None

    class Socket:
        def __init__(self, key):
            self.key, self.position = key, 0

        async def recv(self, decode=None):
            if self.position == len(inputs[self.key]):
                await asyncio.Future()
            due = anchor + self.position * intervals[self.key[2]]
            await asyncio.sleep(max(0, due - time.perf_counter()))
            frame = inputs[self.key][self.position]
            self.position += 1
            sent[self.key] += 1
            return frame

        async def close(self, code=1000, reason=""):
            pass

    def opener(market):
        @asynccontextmanager
        async def connect(url, **kwargs):
            symbol, wire = url.rsplit("/", 1)[-1].split("@", 1)
            stream = {
                "bookTicker": "book_ticker",
                "aggTrade": "agg_trade",
                "depth@100ms": "diff_depth",
                "markPrice@1s": "mark_price",
                "forceOrder": "liquidation",
            }[wire]
            key = (market, symbol.upper(), stream)
            opened[key] += 1
            if opened[key] != 1:
                raise RuntimeError(f"unexpected reconnect: {key}")
            yield Socket(key)

        return connect

    config = RecorderConfig(
        data_root=root,
        spot_symbols=core.SYMBOLS[: args.symbols],
        usdm_symbols=core.SYMBOLS[: args.symbols],
    )
    logger = logging.getLogger("P0.baseline")
    logger.setLevel(logging.ERROR)
    runtime = runtime_module.ServiceRuntime(config=config, logger=logger)
    archived, forecasts, samples = [], [], []
    stop_background = asyncio.Event()

    def background_once():
        result = subprocess.run(
            [
                sys.executable,
                __file__,
                "--background-worker",
                str(root),
                prepared.target.storage_id,
                str(prepared.target.root),
            ],
            capture_output=True,
            text=True,
            timeout=90,
        )
        if result.returncode:
            raise RuntimeError(result.stderr)
        document = json.loads(result.stdout)
        forecasts.append(document.pop("forecast"))
        return document

    async def background():
        while not stop_background.is_set():
            archived.append(await asyncio.to_thread(background_once))
            with suppress(TimeoutError):
                await asyncio.wait_for(stop_background.wait(), 10)

    async def sample():
        while not stop_background.is_set():
            target = time.perf_counter() + 0.1
            await asyncio.sleep(0.1)
            samples.append(
                {
                    "elapsed": time.perf_counter() - anchor,
                    "lag": max(0, time.perf_counter() - target),
                    "rss": current_rss_bytes(),
                    "tasks": len(asyncio.all_tasks()),
                }
            )

    poller_init = RestSideDataPoller.__init__

    def fixed_source_clock(instance, **kwargs):
        kwargs["utc_clock_ns"] = lambda: core.EPOCH_MS * 1_000_000
        poller_init(instance, **kwargs)

    with ExitStack() as stack:
        stack.enter_context(patch.object(RestSideDataPoller, "__init__", fixed_source_clock))
        for name in (
            "PublicSpotRestApi",
            "create_spot_exchange_info_api",
            "create_usdm_rest_api",
            "create_usdm_side_rest_api",
        ):
            stack.enter_context(patch.object(runtime_module, name, lambda *a, **kw: api))
        stack.enter_context(patch.object(runtime_module, "open_spot_websocket", opener("spot")))
        stack.enter_context(
            patch.object(runtime_module, "open_usdm_websocket", opener("um_perpetual"))
        )
        anchor = time.perf_counter()
        (root / "state/p0-start-clock.json").write_text(json.dumps({"anchor": anchor}))
        task = asyncio.create_task(runtime.run())
        monitor = asyncio.create_task(sample())
        worker = asyncio.create_task(background())
        try:
            await asyncio.sleep(args.warmup)
            if task.done():
                task.result()
                raise RuntimeError("runtime ended in warmup")
            before = counters()
            print("STEADY_BEGIN", flush=True)
            await asyncio.sleep(args.seconds)
            after = counters()
            print("STEADY_END", flush=True)
            async with asyncio.timeout(60):
                while sum(sent.values()) != sum(len(v) for v in inputs.values()):
                    if task.done():
                        task.result()
                    await asyncio.sleep(0.05)
            if task.done():
                task.result()
                raise RuntimeError("runtime ended before normal stop")
            readiness = [c.readiness_snapshot().public_dict() for c in runtime._collectors.values()]
            side_status = {
                f"{p.market}/{p.symbol}": c.side_data_status()
                for p, c in runtime._collectors.items()
            }
        finally:
            runtime.request_stop("P0_FIXED_INPUT_COMPLETE")
            await asyncio.wait_for(task, 90)
            stop_background.set()
            await asyncio.gather(worker, monitor)
    archived.append(await asyncio.to_thread(background_once))
    actual, flags, stream_counts = {}, Counter(), Counter()
    with Catalog(layout.catalog) as catalog:
        chunks = load_source_chunks(
            layout=layout,
            catalog=catalog,
            external_roots={prepared.target.storage_id: prepared.target.root},
        )
        # load_source_chunks is chronological per stream; sort frames by capture sequence.
        records = [r for c in chunks for r in iter_source_records(c)]
        records.sort(
            key=lambda r: (
                r.envelope.market,
                r.envelope.symbol,
                r.envelope.stream,
                r.envelope.receive_monotonic_ns,
            )
        )
        for record in records:
            e = record.envelope
            stream_counts[f"{e.market}/{e.symbol}/{e.stream}"] += 1
            flags.update(e.capture_flags)
            key = (e.market, e.symbol, e.stream)
            if key in inputs:
                label = "/".join(key)
                item = actual.setdefault(label, {"frames": 0, "digest": hashlib.sha256()})
                item["frames"] += 1
                digest_frame(item["digest"], e.raw_payload)
        actual = {
            k: {"frames": v["frames"], "sha256": v["digest"].hexdigest()} for k, v in actual.items()
        }
        if actual != expected or flags.get("malformed", 0):
            raise RuntimeError(f"Raw input mismatch/malformed: {actual != expected}, {flags}")
        backlog = catalog.archive_aggregate(prepared.target.storage_id)
    steady = [s for s in samples if args.warmup <= s["elapsed"] <= duration]
    lag = sorted(s["lag"] for s in steady)
    return {
        "schema": "p0-steady-baseline.v2",
        "source_commit": args.commit,
        "python": sys.version,
        "platform": platform.platform(),
        "dependencies": {
            name: importlib.metadata.version(name)
            for name in (
                "binance-sdk-spot",
                "binance-sdk-derivatives-trading-usds-futures",
                "binance-common",
                "pydantic",
                "websockets",
                "cbor2",
                "google-crc32c",
                "zstandard",
                "pyarrow",
            )
        },
        "config": config.model_dump(mode="json") | {"data_root": "<fresh-temporary-root>"},
        "source_period_epoch_ms": core.EPOCH_MS,
        "initial_fixture_cursor_ms": last_closed - 300000,
        "input_manifest": expected,
        "raw_verification": actual,
        "all_ws_exact_bytes_order_count_crc_hashes": True,
        "capture_flags": dict(flags),
        "streams_saved": dict(stream_counts),
        "rest_calls": dict(api.calls),
        "side_status_before_stop": side_status,
        "readiness_before_stop": readiness,
        "measurement_before": before,
        "measurement_after": after,
        "steady_seconds": after["wall"] - before["wall"],
        "steady_cpu_seconds": after["cpu"] - before["cpu"],
        "rss_min_bytes": min(s["rss"] for s in steady),
        "rss_max_bytes": max(s["rss"] for s in steady),
        "loop_lag_p99_seconds": lag[int((len(lag) - 1) * 0.99)],
        "loop_lag_max_seconds": max(lag),
        "samples": steady,
        "archive_runs": archived,
        "archive_final": backlog,
        "forecast_runs": len(forecasts),
        "report_files": len(list(layout.daily_reports.rglob("*.json"))),
        "unsupported": [
            "COIN-M",
            "dated futures",
            "new P2 families",
            "real network/TLS",
            "real archive-device throughput",
            "remote archive transport",
            "power failure",
            "production capacity profile identity",
        ],
        "offline_consumers": "normalization and replay exercised in archive child after sealing",
    }


def background_worker():
    from binance_market_data_recorder.replay import ManifestCatalog, ReplayQuery
    from binance_market_data_recorder.storage.macos import VolumeInfo

    root, storage_id, archive_root = Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4])
    layout = ensure_storage_layout(root)
    start = counters()
    with Catalog(layout.catalog) as catalog:
        target_row = catalog.storage_targets()[0]
        volume = VolumeInfo(
            disk_id="disk9s1",
            volume_uuid=str(target_row["volume_uuid"]),
            name="Test Archive",
            filesystem_type="apfs",
            mountpoint=root.parent / "external-volume",
            writable=True,
            internal=False,
            removable=True,
            total_bytes=100 * 1024**3,
            free_bytes=90 * 1024**3,
            observed_at_utc_ns=1,
        )
        result = archive_drain(
            layout=layout,
            catalog=catalog,
            storage_id=storage_id,
            max_runtime_seconds=10,
            max_files=200,
            volumes=FixedVolumes(volume),
        )
        forecaster = StorageForecaster(data_root=root, catalog=catalog)
        forecaster.observe_internal()
        result["forecast"] = forecaster.forecast("internal")
        # Run the optional consumers once after the first nonempty sealed batch.
        marker = root / "state/p0-consumers.json"
        anchor = json.loads((root / "state/p0-start-clock.json").read_text())["anchor"]
        if time.perf_counter() >= anchor + 65 and not marker.exists():
            normalized = Normalizer(
                layout=layout, catalog=catalog, external_roots={storage_id: archive_root}
            ).run()
            replayed = 0
            if normalized.build_id:
                dataset = ManifestCatalog(root).open_build(normalized.build_id)
                replayed = sum(1 for _ in dataset.replay(ReplayQuery(streams=("agg_trade",))))
            consumers = {"normalization": normalized.public_dict(), "replayed_agg_trades": replayed}
            if not normalized.normalized_rows or not replayed:
                raise RuntimeError("consumers must process nonempty core data")
            report = DailyReporter(catalog=catalog, daily_directory=layout.daily_reports).write(
                time.strftime("%Y-%m-%d", time.gmtime())
            )
            consumers["daily_report"] = report
            marker.write_text(json.dumps(consumers))
            result["consumers"] = consumers
    result["resources_before"] = start
    result["resources_after"] = counters()
    result["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
        1 if sys.platform == "darwin" else 1024
    )
    print(json.dumps(result, default=str))


def main():
    if sys.argv[1:2] == ["--background-worker"]:
        background_worker()
        return
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", type=Path, required=True)
    p.add_argument("--commit", required=True)
    p.add_argument("--symbols", type=int, choices=(1, 2), required=True)
    p.add_argument("--warmup", type=float, default=20)
    p.add_argument("--seconds", type=float, default=120)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    if args.output.exists() or args.seconds < 5 or args.warmup < 1:
        p.error("new output and positive bounded durations required")
    with tempfile.TemporaryDirectory(prefix="bmdr-p0-steady-", dir="/var/tmp") as temporary:
        result = asyncio.run(measure(args, Path(temporary).resolve()))
    args.output.write_text(json.dumps(result, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
