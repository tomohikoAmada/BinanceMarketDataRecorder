from __future__ import annotations

import json
import os
import resource
import sys
import time
import tracemalloc
from datetime import UTC, datetime
from pathlib import Path

import pytest

from binance_market_data_recorder.spool.format import scan_chunk
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event

pytestmark = pytest.mark.stress


def test_one_million_synthetic_frames_use_bounded_python_memory(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    envelope = event(1, payload=b"{}")
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="million-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(bytes=1024 * 1024 * 1024),
            durability_interval_seconds=1,
        )
        tracemalloc.start()
        write_start = time.perf_counter()
        for _ in range(1_000_000):
            writer.append(envelope)
        write_duration_s = time.perf_counter() - write_start
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        writer.close()

        assert writer.record_count == 1_000_000
        assert peak < 64 * 1024 * 1024

        scan_start = time.perf_counter()
        result = scan_chunk(writer.path)
        scan_duration_s = time.perf_counter() - scan_start

        assert result.is_clean
        assert result.statistics.record_count == 1_000_000

        ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_rss_bytes = ru if sys.platform == "darwin" else ru * 1024
        peak_rss_mib = peak_rss_bytes / (1024 * 1024)
        tracemalloc_peak_mib = peak / (1024 * 1024)
        write_fps = 1_000_000 / write_duration_s if write_duration_s > 0 else 0.0
        scan_fps = 1_000_000 / scan_duration_s if scan_duration_s > 0 else 0.0

        metrics = {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "record_count": 1_000_000,
            "write_duration_seconds": round(write_duration_s, 4),
            "write_frames_per_second": round(write_fps, 2),
            "scan_duration_seconds": round(scan_duration_s, 4),
            "scan_frames_per_second": round(scan_fps, 2),
            "tracemalloc_peak_bytes": peak,
            "tracemalloc_peak_mib": round(tracemalloc_peak_mib, 4),
            "peak_rss_bytes": peak_rss_bytes,
            "peak_rss_mib": round(peak_rss_mib, 4),
            "uncompressed_bytes": writer.path.stat().st_size,
        }

        print(
            f"\n[1M Stress] write: {write_duration_s:.3f}s ({write_fps:,.1f} fps), "
            f"scan: {scan_duration_s:.3f}s ({scan_fps:,.1f} fps), "
            f"tracemalloc peak: {tracemalloc_peak_mib:.2f} MiB, "
            f"peak RSS: {peak_rss_mib:.2f} MiB"
        )

        output_path_env = os.environ.get("STRESS_METRICS_PATH")
        if output_path_env:
            out_file = Path(output_path_env)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_text(json.dumps(metrics, indent=2))
        else:
            default_out = Path("docs/benchmarks/m3.2-stress-million-20261103.json")
            if default_out.parent.is_dir():
                default_out.write_text(json.dumps(metrics, indent=2))
