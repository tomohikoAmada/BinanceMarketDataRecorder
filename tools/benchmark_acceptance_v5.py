"""Non-CI V5 synthetic benchmark; temporary offline data only, no wall-clock gate.

Run: python -m tools.benchmark_acceptance_v5 --counts 10000 50000 100000
"""

from __future__ import annotations

import argparse
import hashlib
import json
import resource
import subprocess
import sys
import tempfile
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from binance_market_data_recorder.service import acceptance_v5_raw as raw
from binance_market_data_recorder.service.acceptance import sha256_bytes
from binance_market_data_recorder.service.acceptance_v5_finalize import baseline, finalize
from binance_market_data_recorder.service.acceptance_v5_io import open_exact
from binance_market_data_recorder.service.acceptance_v5_raw import cancellable_unit
from tests.unit.test_acceptance_v5_io import stopped
from tests.unit.test_acceptance_v5_online import advance, observer_fixture
from tests.v5_support import synthetic_history


def benchmark(count: int) -> dict[str, Any]:
    fingerprint = source_fingerprint()
    root = Path(tempfile.mkdtemp(prefix="bmdr-v5-synthetic-", dir="/private/var/tmp")).resolve()
    with nullcontext(root):
        observer, clock, _evaluator = observer_fixture(root)
        synthetic_history(observer.data_root, count)
        before = time.perf_counter()
        path, _sha, base = baseline(
            data_root=observer.data_root,
            evidence_root=root / "baseline-full",
            identity=observer.identity,
            products=[["um_perpetual", "BTCUSDT"]],
            archive_roots={},
            probe=stopped,
            identity_verifier=lambda _identity: None,
            boot_id="boot-a",
        )
        baseline_seconds = time.perf_counter() - before
        if base["result"] != "PASS_CANDIDATE":
            raise RuntimeError(f"invalid synthetic baseline: {base['blocking_findings']}")
        observer.predecessor_path = path
        observer.snapshot_unit = cancellable_unit
        observer.raw_unit = cancellable_unit
        observer.start()
        reads = hashes = 0
        real_hash = sha256_bytes

        def counted_open(root: Path, relative: str) -> Any:
            nonlocal reads
            if relative.endswith(".manifest.json"):
                reads += 1
            return open_exact(root, relative)

        def counted_hash(value: bytes) -> str:
            nonlocal hashes
            if b'"manifest_schema_version"' in value:
                hashes += 1
            return real_hash(value)

        raw.open_exact = counted_open  # type: ignore[attr-defined]
        raw.sha256_bytes = counted_hash  # type: ignore[attr-defined]
        samples = []
        for _ in range(3):
            advance(clock, 300)
            before = time.perf_counter()
            sample_path, _sha, sample = observer.sample()
            samples.append(
                {
                    "duration_seconds": time.perf_counter() - before,
                    "manifest_content_reads": reads,
                    "manifest_hashes": hashes,
                    "catalog_rows_consumed": sum(
                        len(page) for page in sample["delta_pages"].values()
                    ),
                    "bytes_serialized": sample_path.stat().st_size,
                }
            )
        for _ in range(20):
            advance(clock, 300)
            observer.sample()
        advance(clock, 300)
        observer.finalize()
        before = time.perf_counter()
        _path, _sha, final = finalize(
            evidence_root=observer.evidence_root,
            identity=observer.identity,
            archive_roots={},
            probe=stopped,
            identity_verifier=lambda _identity: None,
        )
        terminal_seconds = time.perf_counter() - before
        if not final["eligible_for_next_stage"]:
            raise RuntimeError(f"invalid synthetic terminal: {final['blocking_findings']}")
        if source_fingerprint() != fingerprint:
            raise RuntimeError("benchmark implementation changed during measurement")
        return {
            "implementation_source_sha256": fingerprint,
            "synthetic_evidence_root": str(root),
            "manifest_count": count,
            "delta_count": 0,
            "samples": samples,
            "baseline_seconds": baseline_seconds,
            "terminal_seconds": terminal_seconds,
            "qualified_raw_bytes": base["raw_bytes_verified"],
            "baseline_raw_bytes_per_second": base["raw_bytes_verified"] / baseline_seconds,
            "terminal_raw_bytes_per_second": base["raw_bytes_verified"] / terminal_seconds,
            "qualified_archive_bytes": 0,
            "archive_bytes_per_second": 0,
            "peak_rss_native_units": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "worker_peak_rss_native_units": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
            "rss_units": "bytes" if sys.platform == "darwin" else "KiB",
        }


def source_fingerprint() -> str:
    source = Path(__file__).resolve().parents[1] / "src"
    digest = hashlib.sha256()
    for path in sorted(source.rglob("*.py")):
        digest.update(str(path.relative_to(source)).encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--counts", type=int, nargs="+", default=[10_000, 50_000, 100_000])
    parser.add_argument("--child-count", type=int)
    args = parser.parse_args()
    if args.child_count is not None:
        print(json.dumps(benchmark(args.child_count), sort_keys=True), flush=True)
        return
    for count in args.counts:
        subprocess.run(
            [sys.executable, "-m", "tools.benchmark_acceptance_v5", "--child-count", str(count)],
            check=True,
        )


if __name__ == "__main__":
    main()
