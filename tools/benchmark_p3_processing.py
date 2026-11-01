"""Local identical-input P3 readback/hash comparison; no network or production roots."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import random
import statistics
import tempfile
import time
import tracemalloc
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import Any

import zstandard

from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.readback import read_artifact
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def old_readback(path: Path) -> tuple[int, str, int, str]:
    expanded_size = 0
    expanded_hash = hashlib.sha256()
    with (
        path.open("rb", buffering=0) as compressed,
        zstandard.ZstdDecompressor().stream_reader(compressed) as reader,
    ):
        while block := reader.read(1024 * 1024):
            expanded_size += len(block)
            expanded_hash.update(block)
    stored_hash = hashlib.sha256()
    stored_size = 0
    with path.open("rb", buffering=0) as source:
        while block := source.read(1024 * 1024):
            stored_size += len(block)
            stored_hash.update(block)
    return stored_size, stored_hash.hexdigest(), expanded_size, expanded_hash.hexdigest()


def combined_readback(path: Path) -> tuple[int, str, int, str]:
    result = read_artifact(path)
    return (
        result.stored_bytes,
        result.stored_sha256,
        result.uncompressed_bytes,
        result.uncompressed_sha256,
    )


def timed(operation: Callable[[], object]) -> dict[str, float]:
    wall, cpu = time.perf_counter(), time.process_time()
    operation()
    return {"wall_seconds": time.perf_counter() - wall, "cpu_seconds": time.process_time() - cpu}


def peak(operation: Callable[[], object]) -> int:
    tracemalloc.start()
    try:
        operation()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--rounds", type=int, default=8)
    args = parser.parse_args()
    if args.rounds < 4:
        parser.error("at least four matched rounds are required")
    if args.output is not None and args.output.exists():
        parser.error("refusing to overwrite an existing benchmark result")
    with tempfile.TemporaryDirectory(prefix="bmdr-p3-benchmark-") as temporary:
        layout = ensure_storage_layout(Path(temporary))
        with Catalog(layout.catalog) as catalog:
            writer = RawChunkWriter(
                layout=layout,
                catalog=catalog,
                market="spot",
                symbol="BTCUSDT",
                stream="diff_depth",
                collector_instance_id="p3-local-benchmark",
                collector_version="p3-benchmark",
                durability_interval_seconds=1,
            )
            generator = random.Random(20261030)
            # Deterministic opaque received bytes are legitimate Raw even when malformed JSON.
            for number in range(64):
                writer.append(
                    EventEnvelope(
                        market="spot",
                        symbol="BTCUSDT",
                        stream="diff_depth",
                        module="p3-fixture",
                        connection_id="fixture",
                        collector_instance_id="p3-local-benchmark",
                        collector_version="p3-benchmark",
                        receive_time_utc_ns=1_790_000_000_000_000_000 + number,
                        receive_monotonic_ns=number,
                        source_sequence={"U": number, "u": number},
                        raw_payload=generator.randbytes(256 * 1024),
                        capture_flags=("malformed",),
                    )
                )
            writer.close()
            manifest = seal_partial(writer.path, layout=layout, catalog=catalog)
            artifact = layout.root / str(manifest["relative_path"])
            expected = old_readback(artifact)
            assert combined_readback(artifact) == expected
            rounds: dict[str, list[dict[str, float]]] = {"before": [], "after": []}
            operations = {"before": old_readback, "after": combined_readback}
            for number in range(args.rounds):
                order = ("before", "after") if number % 2 == 0 else ("after", "before")
                for name in order:
                    rounds[name].append(timed(partial(operations[name], artifact)))
            readback = {
                name: {
                    "median_wall_seconds": statistics.median(r["wall_seconds"] for r in values),
                    "median_cpu_seconds": statistics.median(r["cpu_seconds"] for r in values),
                    "peak_python_allocated_bytes": peak(partial(operations[name], artifact)),
                    "compressed_opens": 2 if name == "before" else 1,
                    "compressed_bytes_read": expected[0] * (2 if name == "before" else 1),
                    "rounds": values,
                }
                for name, values in rounds.items()
            }
        # Existing published Raw digest is reused across children of one immutable response.
        payload = bytes(range(256)) * 4096
        children = 128

        def repeated() -> list[str]:
            return [hashlib.sha256(payload).hexdigest() for _ in range(children)]

        def shared() -> list[str]:
            digest = hashlib.sha256(payload).hexdigest()
            return [digest] * children

        assert repeated() == shared()
        result: dict[str, Any] = {
            "schema_version": "p3-local-processing-comparison.v1",
            "platform": platform.platform(),
            "python": platform.python_version(),
            "scope": "local warm-cache microcomparison; not all-feature CPU or VPS capacity",
            "identical_results": True,
            "input": {
                "records": 64,
                "stored_bytes": expected[0],
                "stored_sha256": expected[1],
                "uncompressed_bytes": expected[2],
                "uncompressed_sha256": expected[3],
            },
            "readback": readback,
            "raw_digest_reuse": {
                "payload_bytes": len(payload),
                "children": children,
                "before": timed(repeated),
                "after": timed(shared),
                "before_sha_calls": children,
                "after_sha_calls": 1,
            },
        }
        body = json.dumps(result, indent=2) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x") as output:
                output.write(body)
        else:
            print(body, end="")


if __name__ == "__main__":
    main()
