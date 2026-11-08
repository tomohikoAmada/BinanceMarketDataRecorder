"""Use frozen Python archive transactions and independent Raw reader for C++ roots."""

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path

from binance_market_data_recorder.archive.drain import archive_drain
from binance_market_data_recorder.normalize.raw import (
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import (
    StorageLayout,
    ensure_storage_layout,
)
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["register", "archive", "verify"])
    parser.add_argument("root", type=Path)
    parser.add_argument("--target", type=Path)
    args = parser.parse_args()
    if args.action == "register":
        layout = ensure_storage_layout(args.root)
        if args.target is None:
            parser.error("--target required")
        args.target.mkdir(parents=True, exist_ok=True)
    else:
        layout = StorageLayout.from_root(args.root)
    with Catalog(layout.catalog, read_only=args.action == "verify") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
        if args.action == "register":
            print(json.dumps(registry.register(args.target), indent=2))
            return
        if args.action == "archive":
            targets = catalog.storage_targets()
            if len(targets) != 1:
                raise ValueError("exactly one archive target required")
            report = archive_drain(
                layout=layout,
                catalog=catalog,
                storage_id=targets[0]["storage_id"],
                max_runtime_seconds=50,
                max_files=1000,
            )
            print(json.dumps(report, indent=2))
            if report.get("failed_transactions", 0):
                raise SystemExit(1)
            return
        with sqlite3.connect(f"file:{layout.catalog}?mode=ro", uri=True) as db:
            tables = {
                row[0]
                for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            archive_states = (
                dict(
                    db.execute(
                        "SELECT state,count(*) FROM archive_transactions GROUP BY state"
                    )
                )
                if "archive_transactions" in tables
                else {}
            )
        external = {
            s["storage_id"]: Path(s["resolved_path"])
            for s in (
                registry.observe_statuses() if "storage_targets" in tables else []
            )
            if s.get("state") in {"READY", "LOW_SPACE"} and s.get("resolved_path")
        }
        chunks = load_source_chunks(
            layout=layout, catalog=catalog, external_roots=external
        )
        counts = Counter()
        flags = Counter()
        statuses = Counter()
        total = 0
        connections = {}
        bridge_connections = {}
        stored_bytes = sum(c.manifest["stored_bytes"] for c in chunks)
        for chunk in chunks:
            seen = 0
            for record in iter_source_records(chunk):
                e = record.envelope
                seen += 1
                total += 1
                key = f"{e.market}/{e.symbol}/{e.stream}"
                counts[key] += 1
                connections.setdefault(key, set()).add(e.connection_id)
                if "depth_bridge" in e.capture_flags:
                    bridge_connections.setdefault(key, set()).add(e.connection_id)
                for flag in e.capture_flags:
                    flags[f"{key}/{flag}"] += 1
                if e.module == "binance.public.rest.v1":
                    provenance = json.loads(e.raw_payload)
                    statuses[
                        f"{e.market}/{e.symbol}/{e.source_sequence.get('sourceStream')}/{provenance['response']['status']}/{provenance['response']['valid']}"
                    ] += 1
            assert seen == chunk.manifest["record_count"]
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "chunks": len(chunks),
                    "records": total,
                    "stored_bytes": stored_bytes,
                    "archive_states": archive_states,
                    "connections": {k: sorted(v) for k, v in connections.items()},
                    "bridge_connections": {
                        k: sorted(v) for k, v in bridge_connections.items()
                    },
                    "source_counts": dict(counts),
                    "capture_flags": dict(flags),
                    "rest_statuses": dict(statuses),
                    "archive_roots": {k: str(v) for k, v in external.items()},
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
