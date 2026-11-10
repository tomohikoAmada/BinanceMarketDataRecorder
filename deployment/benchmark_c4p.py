"""Bounded same-scope Python Recorder baseline for the C4-P comparison."""

import argparse
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

CLK_TCK = os.sysconf("SC_CLK_TCK")


def proc_sample(pid):
    stat = Path(f"/proc/{pid}/stat").read_text()
    rest = stat.rsplit(")", 1)[1].split()
    cpu_seconds = (int(rest[11]) + int(rest[12])) / CLK_TCK
    status = Path(f"/proc/{pid}/status").read_text()
    rss_kib = int(status.split("VmRSS:")[1].split()[0])
    threads = int(status.split("Threads:")[1].split()[0])
    return cpu_seconds, rss_kib * 1024, threads


def host_sample():
    values = [
        int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]
    ]
    return sum(values), sum(values) - values[3] - values[4], values[7]


def catalog_metrics(root):
    path = root / "state/catalog.sqlite"
    if not path.exists():
        return {"chunks": 0, "records": 0, "stored_bytes": 0, "states": {}}
    with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as db:
        states = dict(
            db.execute("SELECT state,count(*) FROM chunks GROUP BY state").fetchall()
        )
        row = db.execute(
            "SELECT count(*),coalesce(sum(record_count),0),coalesce(sum(stored_bytes),0) "
            "FROM chunks"
        ).fetchone()
    return {
        "chunks": row[0],
        "records": row[1],
        "stored_bytes": row[2],
        "states": states,
    }


def parse_logs(path):
    summary = {
        "json_lines": 0,
        "reconnect_events": 0,
        "writer_seal_max_ns": 0,
        "writer_fsync_max_ns": 0,
        "writer_append_max_ns": 0,
        "queue_wait_max_ns": 0,
        "receipt_queue_high_watermark": 0,
    }
    if not path.exists():
        return summary
    for line in path.read_text(errors="replace").splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        summary["json_lines"] += 1
        fields = event.get("fields")
        merged = {**event, **fields} if isinstance(fields, dict) else event
        for key in (
            "writer_seal_max_ns",
            "writer_fsync_max_ns",
            "writer_append_max_ns",
            "queue_wait_max_ns",
        ):
            value = merged.get(key)
            if isinstance(value, int):
                summary[key] = max(summary[key], value)
        value = merged.get("receipt_queue_high_watermark")
        if isinstance(value, int):
            summary["receipt_queue_high_watermark"] = max(
                summary["receipt_queue_high_watermark"], value
            )
        name = str(event.get("event", "")).lower()
        if "disconnect" in name or "reconnect" in name or "recovery" in name:
            summary["reconnect_events"] += 1
    return summary


def c4b_window(evidence, seconds):
    report = {}
    combined_records = 0
    combined_cpu = 0.0
    combined_rss = 0
    combined_queue = 0
    combined_seal = 0.0
    combined_sync = 0.0
    elapsed = 0.0
    for market in ("spot", "usdm"):
        rows = [
            json.loads(line)
            for line in (evidence / f"{market}.jsonl").read_text().splitlines()
            if line.strip()
        ]
        rows = [
            row
            for row in rows
            if isinstance(row.get("elapsed_seconds"), (int, float))
            and not isinstance(row.get("elapsed_seconds"), bool)
            and isinstance(row.get("records"), int)
            and isinstance(row.get("cpu_seconds"), (int, float))
            and float(row["elapsed_seconds"]) <= seconds
        ]
        if not rows:
            continue
        first, last = rows[0], rows[-1]
        elapsed = max(elapsed, float(last.get("elapsed_seconds", 0)))
        cpu = float(last.get("cpu_seconds", 0)) - float(first.get("cpu_seconds", 0))
        records = int(last.get("records", 0)) - int(first.get("records", 0))
        report[market] = {
            "records": records,
            "cpu_seconds": cpu,
            "peak_rss_bytes": max(int(row.get("rss_bytes", 0)) for row in rows),
            "peak_queue_bytes": max(
                int(row.get("queued_payload_bytes", 0)) for row in rows
            ),
            "writer_seal_ms_max": max(
                float(row.get("writer_seal_ms", 0)) for row in rows
            ),
            "writer_sync_ms_max": max(
                float(row.get("writer_sync_ms", 0)) for row in rows
            ),
        }
        combined_records += records
        combined_cpu += cpu
        combined_rss += report[market]["peak_rss_bytes"]
        combined_queue = max(combined_queue, report[market]["peak_queue_bytes"])
        combined_seal = max(combined_seal, report[market]["writer_seal_ms_max"])
        combined_sync = max(combined_sync, report[market]["writer_sync_ms_max"])
    report["combined"] = {
        "elapsed_seconds": elapsed,
        "records": combined_records,
        "cpu_seconds": combined_cpu,
        "cpu_mean_percent": 100 * combined_cpu / elapsed if elapsed else 0,
        "records_per_sec": combined_records / elapsed if elapsed else 0,
        "cpu_sec_per_10k_records": (
            10000 * combined_cpu / combined_records if combined_records else 0
        ),
        "peak_rss_sum_bytes": combined_rss,
        "peak_queue_bytes": combined_queue,
        "writer_seal_ms_max": combined_seal,
        "writer_sync_ms_max": combined_sync,
    }
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=960)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--c4b-evidence", type=Path, required=True)
    parser.add_argument("--keep-root", action="store_true")
    args = parser.parse_args()

    if args.root.exists():
        shutil.rmtree(args.root)
    args.root.mkdir(parents=True)
    stdout_path = args.output.with_suffix(".stdout.jsonl")
    stderr_path = args.output.with_suffix(".stderr.jsonl")
    env = {
        **os.environ,
        "PYTHONPATH": str((args.source / "vendor/python-baseline/src").resolve()),
    }
    command = [
        str(args.source / "venv/bin/python"),
        "-m",
        "binance_market_data_recorder",
        "--config",
        str(args.config),
        "_service",
        "run",
    ]
    started = time.monotonic()
    with stdout_path.open("w") as stdout, stderr_path.open("w") as stderr:
        process = subprocess.Popen(
            command,
            cwd=args.source,
            env=env,
            stdout=stdout,
            stderr=stderr,
        )
        first_cpu = None
        last_cpu = 0.0
        peak_rss = 0
        rss_at_60 = 0
        rss_at_900 = 0
        threads_peak = 0
        samples = []
        host_start = host_sample()
        while time.monotonic() - started < args.seconds:
            if process.poll() is not None:
                break
            try:
                cpu, rss, threads = proc_sample(process.pid)
            except (OSError, IndexError, ValueError):
                break
            elapsed = time.monotonic() - started
            if first_cpu is None:
                first_cpu = cpu
            last_cpu = cpu
            peak_rss = max(peak_rss, rss)
            threads_peak = max(threads_peak, threads)
            if elapsed >= 60 and not rss_at_60:
                rss_at_60 = rss
            if elapsed >= 900 and not rss_at_900:
                rss_at_900 = rss
            samples.append((elapsed, cpu, rss, threads))
            time.sleep(1)
        survived = time.monotonic() - started
        early_exit = process.poll() is not None
        if not early_exit:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=180)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        host_end = host_sample()
        returncode = process.returncode

    metrics = catalog_metrics(args.root)
    logs = parse_logs(stderr_path)
    cpu_seconds = max(0.0, last_cpu - (first_cpu or 0.0))
    peak_1s = 0.0
    peak_30s = 0.0
    for index in range(1, len(samples)):
        elapsed, cpu, _, _ = samples[index]
        previous = samples[index - 1]
        delta = elapsed - previous[0]
        if delta > 0:
            peak_1s = max(peak_1s, 100 * (cpu - previous[1]) / delta)
        window = [sample for sample in samples[: index + 1] if sample[0] >= elapsed - 30]
        if len(window) > 1 and elapsed - window[0][0] > 0:
            peak_30s = max(
                peak_30s,
                100 * (cpu - window[0][1]) / (elapsed - window[0][0]),
            )
    result = {
        "status": "PASS" if not early_exit and returncode == 0 else "EARLY_EXIT",
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "survived_seconds": survived,
        "early_exit": early_exit,
        "returncode": returncode,
        "config": str(args.config),
        "root": str(args.root),
        "records": metrics["records"],
        "records_per_sec": metrics["records"] / survived if survived else 0,
        "stored_bytes": metrics["stored_bytes"],
        "chunks": metrics["chunks"],
        "chunk_states": metrics["states"],
        "cpu_seconds": cpu_seconds,
        "cpu_mean_percent": 100 * cpu_seconds / survived if survived else 0,
        "cpu_peak_1s_percent": peak_1s,
        "cpu_peak_30s_percent": peak_30s,
        "cpu_sec_per_10k_records": (
            10000 * cpu_seconds / metrics["records"] if metrics["records"] else 0
        ),
        "peak_rss_bytes": peak_rss,
        "rss_at_60s_bytes": rss_at_60,
        "rss_at_900s_bytes": rss_at_900,
        "rss_growth_60s_to_900s_bytes": rss_at_900 - rss_at_60
        if rss_at_900 and rss_at_60
        else None,
        "threads_peak": threads_peak,
        "host": {
            "busy_percent": (
                100
                * (host_end[1] - host_start[1])
                / (host_end[0] - host_start[0])
                if host_end[0] != host_start[0]
                else 0
            ),
            "stolen_percent": (
                100
                * (host_end[2] - host_start[2])
                / (host_end[0] - host_start[0])
                if host_end[0] != host_start[0]
                else 0
            ),
        },
        "logs": logs,
        "cpp_c4b_window": c4b_window(args.c4b_evidence, args.seconds),
    }
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if not args.keep_root:
        shutil.rmtree(args.root)


if __name__ == "__main__":
    main()
