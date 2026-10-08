"""Q6 identical-input derived-work comparison; isolated fixtures, no Formal credit.

Supply a trusted JSON source bundle (baseline/candidate pipeline, replay,
checkpoint, Catalog modules). This tool executes that reviewed local source,
never downloads code. All writes occur in a new child of the selected disposable
parent. No production roots, network, service control or recursive cleanup.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import shutil
import sys
import tempfile
import time
import types
from pathlib import Path
from typing import Any

from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import LocalBookReconstructor
from binance_market_data_recorder.replay import GapPolicy, ManifestCatalog, ReplayQuery
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

MODULES = {"pipeline": "normalize", "replay": "replay", "checkpoint": "orderbook",
           "catalog": "storage"}
SYMBOLS = ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
           "ADAUSDT", "AVAXUSDT", "LINKUSDT", "LTCUSDT")
MARKETS: tuple[Market, ...] = ("spot", "um_perpetual")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def load(source: str, key: str, variant: str) -> Any:
    name = f"binance_market_data_recorder.{MODULES[key]}._q6_{key}_{variant}"
    module = types.ModuleType(name)
    module.__package__ = f"binance_market_data_recorder.{MODULES[key]}"
    sys.modules[name] = module
    exec(compile(source, name, "exec"), module.__dict__)
    return module


def usage() -> dict[str, Any]:
    used = resource.getrusage(resource.RUSAGE_SELF)
    return {"rss_peak_native": used.ru_maxrss, "input_blocks": used.ru_inblock,
            "output_blocks": used.ru_oublock, "minor_faults": used.ru_minflt,
            "major_faults": used.ru_majflt}


def measure(function: Any) -> tuple[Any, dict[str, Any]]:
    wall, cpu, before = time.perf_counter(), time.process_time(), usage()
    result = function()
    after = usage()
    return result, {"wall_seconds": time.perf_counter() - wall,
                    "cpu_seconds": time.process_time() - cpu,
                    "usage_before": before, "usage_after": after}


def fixture(root: Path, products: int, frames: int) -> dict[str, Any]:
    layout = ensure_storage_layout(root)
    symbols = SYMBOLS[:products // 2]
    totals = []
    with Catalog(layout.catalog) as catalog:
        for product, (market, symbol) in enumerate(
            (m, s) for m in MARKETS for s in symbols
        ):
            # Same bytes are cloned for every variant/repetition. Twenty percent
            # repeated logical IDs exercise full duplicate provenance retention.
            writer = RawChunkWriter(layout=layout, catalog=catalog, market=market,
                                    symbol=symbol, stream="agg_trade",
                                    collector_instance_id="q6-isolated-fixture",
                                    collector_version="q6-fixture",
                                    durability_interval_seconds=1)
            for index in range(frames // products):
                identity = index - 1 if index % 5 == 4 else index
                exchange_time = 1791158400000 + identity
                payload = {"e": "aggTrade", "E": exchange_time,
                           "s": symbol, "a": identity, "p": "100.00", "q": "0.010",
                           "f": identity, "l": identity, "T": exchange_time,
                           "m": False, "M": True}
                writer.append(EventEnvelope(
                    market=market, symbol=symbol, stream="agg_trade",
                    module=f"binance.{'spot' if market == 'spot' else 'usdm'}.websocket.v1",
                    connection_id=f"fixture-{product}", collector_instance_id="q6-fixture",
                    collector_version="q6-fixture", receive_time_utc_ns=(
                        1791158400000000000 + (index * products + product) * 1000000
                    ), receive_monotonic_ns=index * 1000000,
                    exchange_event_time=exchange_time, exchange_trade_time=exchange_time,
                    source_sequence={"a": identity, "f": identity, "l": identity},
                    raw_payload=canonical(payload),
                ))
            writer.close()
            totals.append(seal_partial(writer.path, layout=layout, catalog=catalog))
    return {"products": products, "frames": frames, "chunks": len(totals),
            "stored_bytes": sum(m["stored_bytes"] for m in totals),
            "uncompressed_bytes": sum(m["uncompressed_bytes"] for m in totals),
            "manifest_identity": digest(totals)}


def normalized(modules: dict[str, Any], root: Path, symbols: tuple[str, ...]) -> dict[str, Any]:
    layout = ensure_storage_layout(root)
    with modules["catalog"].Catalog(layout.catalog) as catalog:
        result = modules["pipeline"].Normalizer(layout=layout, catalog=catalog).run()
    normalized_root = root / "data" / "normalized"
    # Exact persisted bytes, including build ID, all provenance and Parquet hash.
    identities = {str(p.relative_to(normalized_root)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in normalized_root.rglob("*") if p.is_file()}

    def replay() -> dict[str, Any]:
        opened = ManifestCatalog(root).open_build(result.build_id)._opened
        dataset = modules["replay"].ReplayDataset(opened)
        sha, count = hashlib.sha256(), 0
        for symbol in symbols:
            for event in dataset.replay(ReplayQuery(symbol=symbol, gap_policy=GapPolicy.INCLUDE)):
                sha.update(canonical({"row": dict(event.row), "time": event.event_time_ns,
                                      "unreliable": event.is_unreliable}) + b"\n")
                count += 1
        return {"sha256": sha.hexdigest(), "count": count}

    replay_result, replay_time = measure(replay)
    return {"identity": digest(identities), "build_id": result.build_id,
            "normalized_rows": result.normalized_rows,
            "duplicates": result.duplicate_rows_removed,
            "replay": replay_result, "replay_time": replay_time,
            "published_bytes": sum(p.stat().st_size for p in normalized_root.rglob("*")
                                   if p.is_file())}


def checkpoint(modules: dict[str, Any], root: Path, products: int, levels: int) -> dict[str, Any]:
    layout = ensure_storage_layout(root)
    outputs = []
    with modules["catalog"].Catalog(layout.catalog) as catalog:
        store = modules["checkpoint"].OrderBookCheckpointStore(layout, catalog)
        # Fixed UUID is confined to this isolated module and unique per product.
        for product, (market, symbol) in enumerate(
            (m, s) for m in MARKETS for s in SYMBOLS[:products // 2]
        ):
            book = LocalBookReconstructor(market, symbol)
            book.offer(DepthUpdate(market, symbol, 10, 11,
                                   10 if market == "um_perpetual" else None, (), ()))
            book.synchronize(BookSnapshot(market, symbol, 10,
                tuple((str(100000 - i), "1.20") for i in range(levels)),
                tuple((str(100001 + i), "2.30") for i in range(levels))))
            modules["checkpoint"].uuid4 = lambda product=product: f"q6-fixture-{product}"
            path = store.save(book, collector_version="q6-fixture",
                              source_chunk_hashes=("a" * 64,), utc_clock_ns=lambda: 123)
            document = json.loads(path.read_bytes())
            row = catalog.orderbook_checkpoint(document["checkpoint_id"])
            restored = store.restore(path)
            assert row["book_hash"] == restored.book.logical_hash() == document["book_hash"]
            outputs.append({"file": hashlib.sha256(path.read_bytes()).hexdigest(), "row": row})
    return {"identity": digest(outputs), "products": products, "levels_per_side": levels}


def catalog_comparison(modules: dict[str, Any], root: Path) -> dict[str, Any]:
    root.mkdir()
    path = root / "old.sqlite"
    with modules["baseline"]["catalog"].Catalog(path) as catalog:
        catalog._connection.execute("BEGIN")
        catalog._connection.executemany(
            "INSERT INTO chunks(chunk_id,state,created_at_utc_ns,updated_at_utc_ns) "
            "VALUES(?,?,?,?)",
            ((f"retained-{i}", "LOCAL_DELETED", i, i) for i in range(200000)))
        catalog._connection.executemany(
            "INSERT INTO chunks(chunk_id,state,created_at_utc_ns,updated_at_utc_ns) "
            "VALUES(?,?,?,?)",
            ((f"sealed-{i:04}", "SEALED", 200001, 200001) for i in range(50)))
        catalog._connection.execute("COMMIT")
    output = {}
    for name in ("baseline", "candidate"):
        copy = root / f"{name}.sqlite"
        shutil.copyfile(path, copy)
        catalog, migration = measure(
            lambda name=name, copy=copy: modules[name]["catalog"].Catalog(copy))
        with catalog:
            steps = [0]
            catalog._connection.set_progress_handler(lambda steps=steps:
                                                     steps.__setitem__(0, steps[0] + 100)
                                                     or 0, 100)
            selected = catalog.oldest_unowned_sealed_chunk()
            catalog._connection.set_progress_handler(None, 0)
            comparisons = []
            for _ in range(3):
                _, elapsed = measure(lambda catalog=catalog: [catalog.oldest_unowned_sealed_chunk()
                                              for _ in range(50)])
                comparisons.append(elapsed)
            catalog._connection.execute("BEGIN")
            _, insertion = measure(lambda catalog=catalog: catalog._connection.executemany(
                "INSERT INTO chunks(chunk_id,state,created_at_utc_ns,updated_at_utc_ns) "
                "VALUES(?,?,?,?)", ((f"new-{i}", "SEALED", 300000 + i, 300000 + i)
                                     for i in range(1000))))
            catalog._connection.execute("COMMIT")
            query_plan = [tuple(row) for row in catalog._connection.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM chunks WHERE state='SEALED' "
                "AND NOT EXISTS (SELECT 1 FROM archive_transactions AS local "
                "WHERE local.chunk_id=chunks.chunk_id) "
                "ORDER BY created_at_utc_ns,chunk_id LIMIT 1")]
        output[name] = {"selected": selected, "vm_steps": steps[0], "migration": migration,
                        "selection_50_calls": comparisons, "insert_1000_rows": insertion,
                        "bytes": copy.stat().st_size, "query_plan": query_plan}
    assert output["baseline"]["selected"] == output["candidate"]["selected"]
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--parent", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--frames", type=int, default=40000)
    parser.add_argument("--burst-frames", type=int, default=120000)
    args = parser.parse_args()
    if args.frames < 100 or args.frames % 20 or args.burst_frames % 20:
        parser.error("frames must be positive multiples of20 (ordinary>=100)")
    if not 1 <= args.repeats <= 5:
        parser.error("repeats must be1-5")
    parent = args.parent.resolve(strict=True)
    root = Path(tempfile.mkdtemp(prefix="q6-derived-", dir=parent))
    bundle_bytes = args.bundle.read_bytes()
    bundle = json.loads(bundle_bytes)
    modules = {name: {key: load(code, key, name) for key, code in sources.items()}
               for name, sources in bundle.items()}
    modules["candidate_sorted"] = dict(modules["candidate"])
    modules["candidate_sorted"]["pipeline"] = load(bundle["candidate"]["pipeline"],
                                                  "pipeline", "candidate_sorted")
    modules["candidate_sorted"]["pipeline"]._winner = modules["baseline"]["pipeline"]._winner
    report: dict[str, Any] = {"kind": "ISOLATED_DERIVED_WORK_NOT_FORMAL", "root": str(root),
              "utc_start_ns": time.time_ns(), "host": platform.node(),
              "machine": platform.machine(),
              "python": platform.python_version(), "logical_cpus": os.cpu_count(),
              "bundle_sha256": hashlib.sha256(bundle_bytes).hexdigest(),
              "sources": {name: {key: hashlib.sha256(code.encode()).hexdigest()
                                 for key, code in sources.items()}
                          for name, sources in bundle.items()},
              "cache_policy": "observed first then warm; alternating order; no cache dropping",
              "rss_units": "bytes on macOS, KiB on Linux; cumulative process high-water mark",
              "runs": [], "catalog": None}
    output = root / "results.json"
    output.write_bytes(canonical(report))
    print(root, flush=True)
    for products, frames in [(4, args.frames), (20, args.frames), (20, args.burst_frames)]:
        seed = root / f"seed-{products}-{frames}"
        metadata = fixture(seed, products, frames)
        expected = None
        for repeat in range(args.repeats):
            order = ["baseline", "candidate", "candidate_sorted"]
            if repeat % 2:
                order.reverse()
            for name in order:
                cloned = root / f"normal-{products}-{frames}-{repeat}-{name}"
                shutil.copytree(seed, cloned)
                result, elapsed = measure(
                    lambda name=name, cloned=cloned, products=products: normalized(
                        modules[name], cloned, SYMBOLS[:products // 2]))
                identity = {k: v for k, v in result.items() if k != "replay_time"}
                if expected is not None and expected != identity:
                    raise RuntimeError("complete normalization/replay bytes or identity differ")
                expected = identity
                report["runs"].append({"workload": "normalization_and_verified_replay",
                    "fixture": metadata, "repeat": repeat, "variant": name,
                    "timing": elapsed, "result": result})
                output.write_bytes(canonical(report))
                print(f"normal {products}/{frames}/{repeat}/{name}: "
                      f"{elapsed['wall_seconds']:.3f}s", flush=True)
    for products in (4, 20):
        for levels in (1000, 5000):
            expected = None
            for repeat in range(args.repeats):
                order = ["baseline", "candidate"] if repeat % 2 == 0 else ["candidate", "baseline"]
                for name in order:
                    path = root / f"checkpoint-{products}-{levels}-{repeat}-{name}"
                    result, elapsed = measure(lambda name=name, path=path,
                                               products=products, levels=levels: checkpoint(
                        modules[name], path, products, levels))
                    if expected is not None and result != expected:
                        raise RuntimeError("complete checkpoint file or Catalog identity differs")
                    expected = result
                    report["runs"].append({"workload": "checkpoint_save_restore",
                        "repeat": repeat, "variant": name, "timing": elapsed, "result": result})
                    output.write_bytes(canonical(report))
    report["catalog"] = catalog_comparison(modules, root / "catalog-selection")
    report["utc_finish_ns"] = time.time_ns()
    report["retained_directory_bytes"] = sum(
        p.stat().st_size for p in root.rglob("*") if p.is_file())
    output.write_bytes(canonical(report))
    print(json.dumps({"results": str(output),
                      "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                      "normal_runs": len(report["runs"]), "complete": True}), flush=True)


if __name__ == "__main__":
    main()
