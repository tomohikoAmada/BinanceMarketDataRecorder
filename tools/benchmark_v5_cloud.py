"""Read-only comparison of installed V5 and an in-memory optimization candidate.

Run on the qualification VPS using its installed Python. Supply candidate source
with --candidate-bundle (JSON) or an injected CANDIDATE_BUNDLE mapping when sending
this script over SSH stdin. No Catalog migration, evidence publication or service
control occurs. Reads a bounded existing audit shard, its Raw and the live Catalog.
This microbenchmark is neither a full audit nor Formal acceptance evidence.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import platform
import statistics
import time
from pathlib import Path
from typing import Any

from binance_market_data_recorder import cli
from binance_market_data_recorder.service import acceptance_v5_raw as raw
from binance_market_data_recorder.service.acceptance_v5_io import open_exact
from binance_market_data_recorder.storage import catalog as catalog_module
from binance_market_data_recorder.storage.catalog import Catalog


def measure(function: Any) -> tuple[Any, dict[str, float]]:
    wall, cpu = time.perf_counter(), time.process_time()
    result = function()
    return result, {
        "wall_seconds": time.perf_counter() - wall,
        "cpu_seconds": time.process_time() - cpu,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--archive-root", type=Path, required=True)
    parser.add_argument("--audit-shard", type=Path, required=True)
    parser.add_argument("--candidate-bundle", type=Path)
    parser.add_argument("--repeats", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.repeats <= 4:
        parser.error("repeats must be 1-4")
    bundle = (
        json.loads(args.candidate_bundle.read_text())
        if args.candidate_bundle
        else globals()["CANDIDATE_BUNDLE"]
    )
    candidate_raw = dict(vars(raw))
    exec(compile(bundle["raw"], "<candidate-raw>", "exec"), candidate_raw)
    candidate_catalog = dict(vars(catalog_module))
    exec(compile(bundle["summary"], "<candidate-summary>", "exec"), candidate_catalog)
    candidate_cli = dict(vars(cli))
    exec(compile(bundle["status"], "<candidate-status>", "exec"), candidate_cli)

    class ReadOnlyCandidate(Catalog):
        archive_transaction_summary = candidate_catalog["archive_transaction_summary"]

    with open_exact(args.audit_shard.parent, args.audit_shard.name) as source:
        body = source.read(1024 * 1024 + 1)
    if len(body) > 1024 * 1024:
        parser.error("audit shard exceeds existing 1 MiB bound")
    records = json.loads(body)["records"]
    manifests = [
        record["authority"]["manifest"] for record in records if record["family"] == "manifest"
    ]
    # At most two large book-ticker chunks and one per remaining market/stream.
    chosen: list[dict[str, Any]] = []
    groups: set[tuple[str, str]] = set()
    for manifest in sorted(manifests, key=lambda m: m["record_count"], reverse=True):
        group = manifest["market"], manifest["stream"]
        if group not in groups or (manifest["stream"] == "book_ticker" and len(chosen) == 1):
            chosen.append(manifest)
            groups.add(group)
        if len(chosen) == 6:
            break

    runs: list[dict[str, Any]] = []
    stored = uncompressed = events = 0
    with ReadOnlyCandidate(args.data_root / "state" / "catalog.sqlite", read_only=True) as catalog:
        for manifest in chosen:
            transaction = catalog._connection.execute(
                "SELECT target_relative_path, state FROM archive_transactions WHERE chunk_id=?",
                (manifest["chunk_id"],),
            ).fetchone()
            if transaction is None or transaction["state"] != "LOCAL_DELETED":
                raise RuntimeError("benchmark requires an existing archived chunk")
            relative = str(transaction["target_relative_path"])
            reference = raw.scan_raw(args.archive_root, relative, manifest)
            if candidate_raw["scan_raw"](args.archive_root, relative, manifest) != reference:
                raise RuntimeError("candidate Raw proof differs")
            stored += manifest["stored_bytes"]
            uncompressed += manifest["uncompressed_bytes"]
            events += manifest["record_count"]
            measurements: dict[str, list[dict[str, float]]] = {"installed": [], "candidate": []}
            for repeat in range(args.repeats):
                order = (
                    ("installed", "candidate") if repeat % 2 == 0 else ("candidate", "installed")
                )
                for name in order:
                    scanner = raw.scan_raw if name == "installed" else candidate_raw["scan_raw"]
                    proof, timing = measure(
                        lambda scanner=scanner, relative=relative, manifest=manifest: scanner(
                            args.archive_root, relative, manifest
                        )
                    )
                    if proof != reference:
                        raise RuntimeError("measured Raw proof differs")
                    measurements[name].append(timing)
            runs.append(
                {
                    "chunk_id": manifest["chunk_id"],
                    "market": manifest["market"],
                    "stream": manifest["stream"],
                    "record_count": manifest["record_count"],
                    "stored_bytes": manifest["stored_bytes"],
                    "uncompressed_bytes": manifest["uncompressed_bytes"],
                    "measurements": measurements,
                }
            )

        status: dict[str, Any] = {}
        common = None
        for name, function in (
            ("installed", cli._archive_status),
            ("candidate", candidate_cli["_archive_status"]),
        ):
            gc.collect()

            def serialize(function: Any = function) -> tuple[dict[str, Any], int]:
                document = function(catalog)
                return {
                    key: value
                    for key, value in document.items()
                    if key
                    not in {
                        "transactions",
                        "transactions_included",
                        "transaction_limit",
                        "transaction_offset",
                    }
                }, len(json.dumps(document, sort_keys=True).encode())

            (totals, output_size), timing = measure(serialize)
            if common is not None and common != totals:
                raise RuntimeError("archive status totals differ")
            common = totals
            status[name] = {**timing, "output_bytes": output_size, "totals": totals}

    times = {
        name: sum(
            statistics.median(t["wall_seconds"] for t in run["measurements"][name]) for run in runs
        )
        for name in ("installed", "candidate")
    }
    report = {
        "kind": "READ_ONLY_CLOUD_MICROBENCHMARK_NOT_FORMAL_CREDIT",
        "utc_ns": time.time_ns(),
        "host": platform.node(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "candidate_source_sha256": {
            key: hashlib.sha256(value.encode()).hexdigest() for key, value in bundle.items()
        },
        "cache_policy": "warm both; alternate order; no OS cache dropping",
        "sample_stored_bytes": stored,
        "sample_uncompressed_bytes": uncompressed,
        "sample_events": events,
        "raw_proofs_equal": True,
        "raw_runs": runs,
        "median_sum_wall_seconds": times,
        "raw_speedup": times["installed"] / times["candidate"],
        "archive_status": status,
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
