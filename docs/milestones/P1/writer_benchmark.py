"""Finite local P1 append comparison worker; run separately for each source tree.

PYTHONPATH selects the before/after package. All input allocation, imports and
independent scanning are outside the append timer. Uses an isolated temporary
root and never opens production data. Compare alternating fresh processes.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import tempfile
import time
from collections import Counter
from pathlib import Path
from unittest.mock import patch
from uuid import UUID

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.format import scan_chunk
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--events", type=int, default=20000)
    args = parser.parse_args()
    if os.path.lexists(args.output) or args.events < 1:
        parser.error("output must be new and events positive")
    events = [
        EventEnvelope(
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            module="binance.spot.v1",
            connection_id="connection-1",
            collector_instance_id="collector-1",
            collector_version="p1-writer-comparison",
            receive_time_utc_ns=1700000000000000000 + i,
            receive_monotonic_ns=5000000000 + i,
            exchange_event_time=1700000000000 + i,
            source_sequence={"U": i, "u": i},
            raw_payload=(b'{"fixture":' + str(i).encode() + b"}"),
        )
        for i in range(args.events)
    ]
    calls = Counter()
    original_write = os.write
    original_writev = os.writev

    def write(fd, data):
        calls["write"] += 1
        return original_write(fd, data)

    def writev(fd, vectors):
        calls["writev"] += 1
        return original_writev(fd, vectors)

    with tempfile.TemporaryDirectory(prefix="bmdr-p1-writer-", dir="/var/tmp") as temporary:
        layout = ensure_storage_layout(Path(temporary))
        with Catalog(layout.catalog) as catalog:
            writer = RawChunkWriter(
                layout=layout,
                catalog=catalog,
                market="spot",
                symbol="BTCUSDT",
                stream="diff_depth",
                collector_instance_id="collector-1",
                collector_version="p1-writer-comparison",
                chunk_id=UUID(int=1234),
                created_at_utc_ns=42,
                rotation=RotationPolicy(seconds=3600),
            )
            # Fixed non-rotating workload; each side retains periodic fsync.
            writer._rotation_deadline_monotonic = float("inf")
            with patch.object(os, "write", write), patch.object(os, "writev", writev):
                wall, cpu = time.perf_counter(), time.process_time()
                if hasattr(writer, "append_batch"):
                    for start in range(0, len(events), 256):
                        pending = events[start : start + 256]
                        while pending:
                            sizes = writer.append_batch(pending)
                            pending = pending[len(sizes) :]
                else:
                    for envelope in events:
                        writer.append(envelope)
                writer.close()
                elapsed, cpu_elapsed = time.perf_counter() - wall, time.process_time() - cpu
            scanned = scan_chunk(writer.path)
            if not scanned.is_clean or scanned.statistics.record_count != len(events):
                raise RuntimeError("independent scan failed")
            result = {
                "schema": "p1-local-writer-comparison.v1",
                "events": len(events),
                "source_package_path": __import__("binance_market_data_recorder").__file__,
                "platform": platform.platform(),
                "wall_seconds": elapsed,
                "cpu_seconds": cpu_elapsed,
                "syscalls": dict(calls),
                "uncompressed_sha256": scanned.uncompressed_sha256,
                "bytes": scanned.file_size,
                "independent_scan_passed": True,
            }
    with args.output.open("x") as output:
        output.write(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
