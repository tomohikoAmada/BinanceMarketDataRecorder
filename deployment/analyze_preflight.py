"""Summarize F7 preflight status logs: queue, seal handoff, RSS and 900 s windows."""

import argparse
import json
from pathlib import Path


def window(rows, start, seconds=900):
    selected = [
        row
        for row in rows
        if start <= row.get("elapsed_seconds", 0) < start + seconds
    ]
    if not selected:
        return None
    return {
        "start_seconds": start,
        "samples": len(selected),
        "entry_queue_peak_bytes": max(
            (row.get("queued_payload_bytes", 0) for row in selected), default=0
        ),
        "entry_queue_peak_records": max(
            (row.get("queued_records", 0) for row in selected), default=0
        ),
        "seal_queue_peak": max(
            (row.get("seal_queue_peak", 0) for row in selected), default=0
        ),
        "seal_queue_depth_max": max(
            (row.get("seal_queue_depth", 0) for row in selected), default=0
        ),
        "seal_wait_ns_max": max(
            (row.get("seal_enqueue_wait_ns_max", 0) for row in selected), default=0
        ),
        "seal_pending_raw_bytes_max": max(
            (row.get("seal_pending_raw_bytes", 0) for row in selected), default=0
        ),
        "rss_peak_bytes": max(
            (row.get("rss_bytes", 0) for row in selected), default=0
        ),
        "cpu_percent_max": max(
            (row.get("cpu_percent", 0) for row in selected), default=0
        ),
    }


def analyze(path, queue_limit_mib):
    rows = [
        json.loads(line)
        for line in Path(path).read_text().splitlines()
        if line.strip()
    ]
    if not rows:
        raise ValueError(f"no status samples in {path}")
    limit = queue_limit_mib * 1024**2
    longest = 0.0
    run_start = None
    above = 0
    for row in rows:
        elapsed = row.get("elapsed_seconds", 0)
        if row.get("queued_payload_bytes", 0) >= 0.9 * limit:
            if run_start is None:
                run_start = elapsed
            longest = max(longest, elapsed - run_start + 1)
            above += 1
        else:
            run_start = None
    last_elapsed = max(row.get("elapsed_seconds", 0) for row in rows)
    windows = []
    start = 0
    while start <= last_elapsed:
        summary = window(rows, start)
        if summary:
            windows.append(summary)
        start += 900
    seal_wait_max = max(
        (row.get("seal_enqueue_wait_ns_max", 0) for row in rows), default=0
    )
    seal_peak = max((row.get("seal_queue_peak", 0) for row in rows), default=0)
    rss = [row["rss_bytes"] for row in rows if "rss_bytes" in row]
    result = {
        "status_path": str(path),
        "samples": len(rows),
        "duration_seconds": last_elapsed,
        "entry_queue_limit_bytes": limit,
        "entry_queue_peak_bytes": max(
            (row.get("queued_payload_bytes", 0) for row in rows), default=0
        ),
        "entry_queue_peak_records": max(
            (row.get("queued_records", 0) for row in rows), default=0
        ),
        "entry_queue_samples_above_90_percent": above,
        "entry_queue_longest_run_above_90_percent_seconds": longest,
        "seal_queue_peak": seal_peak,
        "seal_queue_depth_max": max(
            (row.get("seal_queue_depth", 0) for row in rows), default=0
        ),
        "seal_enqueue_wait_ns_max": seal_wait_max,
        "seal_pending_raw_bytes_max": max(
            (row.get("seal_pending_raw_bytes", 0) for row in rows), default=0
        ),
        "rss_first_bytes": rss[0] if rss else 0,
        "rss_last_bytes": rss[-1] if rss else 0,
        "rss_min_bytes": min(rss, default=0),
        "rss_peak_bytes": max(rss, default=0),
        "cpu_percent_max": max(
            (row.get("cpu_percent", 0) for row in rows), default=0
        ),
        "windows_900s": windows,
    }
    result["sustained_entry_queue_full"] = longest >= 900
    result["seal_handoff_blocked"] = (
        seal_wait_max > 1_000_000_000 or (seal_peak >= 250 and seal_wait_max > 100_000_000)
    )
    result["status"] = (
        "PASS"
        if not result["sustained_entry_queue_full"]
        and not result["seal_handoff_blocked"]
        else "FAIL"
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("status_log", type=Path)
    parser.add_argument("--queue-limit-mib", type=int, default=128)
    args = parser.parse_args()
    print(
        json.dumps(
            analyze(args.status_log, args.queue_limit_mib), indent=2
        )
    )


if __name__ == "__main__":
    main()
