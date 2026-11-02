"""M1.1 offline rotation comparison using M1.0's captured WS/REST input.

Only the writer's monotonic clock advances synthetically. Actual Raw encoding,
fsync, compression, manifests, Catalog commits and archive reservations run.
This checks rotation counts, not sustained CPU/latency or M3 qualification.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tools.benchmark_m1_m3_pipeline import digest, logical_record, run_benchmark


def replay_rotation(
    root: Path, events: list[EventEnvelope], seconds: float, *, duration: int = 2700
) -> dict[str, Any]:
    """Replay every input, keeping sparse streams active at ten-second ticks."""
    grouped: dict[tuple[str, str, str], list[EventEnvelope]] = defaultdict(list)
    for event in events:
        grouped[event.market, event.symbol, event.stream].append(event)
    for records in grouped.values():
        records.sort(key=logical_record)
    layout = ensure_storage_layout(root)
    ticks = duration // 10
    expected: Counter[str] = Counter()
    now = 0.0
    clock = SimpleNamespace(monotonic=lambda: now, perf_counter_ns=time.perf_counter_ns,
                            time_ns=time.time_ns)
    with Catalog(layout.catalog) as catalog, patch(
        "binance_market_data_recorder.spool.writer.time", clock
    ):
        spools = {
            key: StreamSpool(
                layout=layout, catalog=catalog, market=key[0], symbol=key[1], stream=key[2],
                collector_instance_id="m1-rotation", collector_version="benchmark",
                queue_capacity=8192, rotation=RotationPolicy(seconds=seconds),
                durability_interval_seconds=1.0, max_frame_bytes=16 * 1024 * 1024,
            ) for key in grouped
        }
        started = time.perf_counter()
        for tick in range(ticks):
            now = tick * 10.0
            for key, records in grouped.items():
                # Preserve every original input at least once; repeat sparse
                # observations explicitly to exercise the full rotation window.
                batch = records[tick * len(records) // ticks:(tick + 1) * len(records) // ticks]
                if not batch:
                    batch = [records[tick % len(records)]]
                for event in batch:
                    spools[key].enqueue(event)
                    expected[logical_record(event)] += 1
                spools[key].drain_all()
        now = float(duration)
        for spool in spools.values():
            spool.drain_all()
        timed_seals = catalog._connection.execute(
            "SELECT COUNT(*) FROM chunk_transitions WHERE to_state = 'SEALED'"
        ).fetchone()[0]
        for spool in spools.values():
            spool.close_and_seal()
        wall = time.perf_counter() - started
        # Manual offline audit, outside the timed replay.
        actual: Counter[str] = Counter()
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        for chunk in chunks:
            for record in iter_source_records(chunk):
                actual[logical_record(record.envelope)] += 1
        assert actual == expected
        assert not list(layout.active.glob("*.partial"))
        transitions = dict(catalog._connection.execute(
            "SELECT to_state, COUNT(*) FROM chunk_transitions GROUP BY to_state"
        ).fetchall())
        # Exercise actual Catalog archive allocation, retaining all local Raw.
        # No external volume or production archive is touched.
        catalog.register_storage_target(
            storage_id="offline", volume_uuid="offline", volume_name=None,
            filesystem_type=None, relative_path="archive", marker_nonce="offline",
            registered_at_utc_ns=1,
        )
        for chunk in chunks:
            m = chunk.manifest
            manifest_path = layout.manifests / f"{chunk.chunk_id.replace('-', '')}.manifest.json"
            catalog.reserve_archive_transaction(
                transaction_id=chunk.chunk_id, chunk_id=chunk.chunk_id, storage_id="offline",
                market=str(m["market"]), stream=str(m["stream"]),
                source_relative_path=str(m["relative_path"]),
                source_manifest_relative_path=layout.relative(manifest_path),
                source_manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                target_relative_path=f"archive/{chunk.chunk_id}.zst",
                target_temp_relative_path=f"archive/{chunk.chunk_id}.partial",
                external_manifest_relative_path=f"archive/{chunk.chunk_id}.json",
                stored_bytes=cast(int, m["stored_bytes"]), stored_sha256=str(m["stored_sha256"]),
            )
        return {
            "rotation_seconds": seconds, "logical_duration_seconds": duration,
            "tick_seconds": 10, "physical_streams": len(spools),
            "frames": sum(actual.values()), "content_digest": digest(sorted(actual.items())),
            "timed_seals": timed_seals, "shutdown_seals": len(chunks) - timed_seals,
            "zst_files": len(list(layout.sealed.glob("*.zst"))),
            "manifest_files": len(list(layout.manifests.glob("*.manifest.json"))),
            "seal_intent_commits": transitions.get("SEALING", 0),
            "seal_completion_commits": transitions.get("SEALED", 0),
            "archive_reservations": catalog._connection.execute(
                "SELECT COUNT(*) FROM archive_transactions"
            ).fetchone()[0],
            "execution_wall_seconds": round(wall, 3),
        }


async def compare() -> dict[str, Any]:
    events: list[EventEnvelope] = []
    baseline = await run_benchmark(event_sink=events.append)
    with tempfile.TemporaryDirectory(prefix="bmdr-m1-rotation-") as tmp:
        results = [replay_rotation(Path(tmp) / str(s), events, s) for s in (60.0, 900.0)]
    before, after = results
    assert before["frames"] == after["frames"]
    assert before["content_digest"] == after["content_digest"]
    reductions = {key: 1 - after[key] / before[key] for key in (
        "zst_files", "manifest_files", "seal_intent_commits", "seal_completion_commits",
        "archive_reservations",
    )}
    assert all(value > 0.90 for value in reductions.values())
    return {
        "schema": "m1.1-rotation-probe.v1",
        "environment": {"python": sys.version, "platform": platform.platform()},
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip(),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "measurement_boundary": "2700s logical writer time; real I/O; not a 45m wall-clock run",
        "archive_boundary": "actual COPYING reservations only; no copy/delete performance claim",
        "m1_0_input_digest": baseline["workload"]["input_digest"],
        "m1_0_frames": baseline["summary"]["total_frames"],
        "runs": results, "reductions_including_shutdown": reductions,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = asyncio.run(compare())
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
