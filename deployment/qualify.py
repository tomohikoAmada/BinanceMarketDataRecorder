"""Bounded real-time qualification; completion requires stopped-process full Raw readback."""

import argparse
import hashlib
import json
import signal
import subprocess
import sys
import time
from collections import Counter, deque
from datetime import UTC, datetime
from pathlib import Path


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


class SystemdChild:
    def __init__(self, market):
        self.unit = f"sbmdr-recorder@{market}.service"
        subprocess.run(["systemctl", "start", self.unit], check=True)
        self.pid = int(self.property("MainPID"))
        if not self.pid:
            raise RuntimeError(f"{self.unit} did not start")
        self.returncode = None

    def property(self, name):
        return subprocess.check_output(
            ["systemctl", "show", self.unit, "--property", name, "--value"], text=True
        ).strip()

    def poll(self):
        if self.property("ActiveState") == "active":
            if (
                int(self.property("MainPID")) == self.pid
                and int(self.property("NRestarts")) == 0
            ):
                return None
            self.returncode = 1
        else:
            self.returncode = (
                0
                if self.property("Result") == "success"
                and self.property("ExecMainStatus") == "0"
                else 1
            )
        return self.returncode

    def send_signal(self, sig):
        subprocess.run(["systemctl", "kill", f"--signal={sig}", self.unit], check=True)

    def wait(self, timeout=180):
        deadline = time.monotonic() + timeout
        while self.poll() is None:
            if time.monotonic() >= deadline:
                raise subprocess.TimeoutExpired(self.unit, timeout)
            time.sleep(0.2)
        return self.returncode

    def kill(self):
        self.send_signal(signal.SIGKILL)


def host_sample():
    values = [
        int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]
    ]
    memory = {
        line.split(":", 1)[0]: int(line.split()[1]) * 1024
        for line in Path("/proc/meminfo").read_text().splitlines()
        if line.startswith(("MemTotal:", "MemAvailable:"))
    }
    return {
        "cpu_total": sum(values),
        "cpu_busy": sum(values) - values[3] - values[4],
        "cpu_steal": values[7],
        **memory,
    }


def check_drops(market, recorder, readback, max_drop_percent):
    """Reconcile the recorder split with Catalog events using accepted WS records."""
    ws_accepted = readback.get("ws_records")
    assert isinstance(ws_accepted, int) and ws_accepted > 0, (
        market,
        "no accepted WS records; an empty run cannot pass the drop ratio",
        ws_accepted,
    )
    ws_dropped = recorder.get("ws_dropped_total")
    rest_dropped = recorder.get("rest_dropped_total")
    assert isinstance(ws_dropped, int) and isinstance(rest_dropped, int), (
        market,
        "recorder did not report the WS/REST drop split",
        recorder.get("ws_dropped_total"),
        recorder.get("rest_dropped_total"),
    )
    assert recorder["dropped_total"] == ws_dropped + rest_dropped, (
        market,
        recorder["dropped_total"],
        ws_dropped,
        rest_dropped,
    )
    catalog_drops = readback["ingress_drops"]
    assert catalog_drops["dropped_ws"] == ws_dropped, (
        market,
        "Catalog WS drops differ from the recorder",
        catalog_drops["dropped_ws"],
        ws_dropped,
    )
    assert catalog_drops["dropped_rest"] == rest_dropped, (
        market,
        "Catalog REST drops differ from the recorder",
        catalog_drops["dropped_rest"],
        rest_dropped,
    )
    dropped = recorder["dropped_total"]
    drop_percent = 100 * dropped / ws_accepted
    assert drop_percent <= max_drop_percent, (
        market,
        "dropped_total",
        dropped,
        "ws_accepted",
        ws_accepted,
        "drop_percent",
        drop_percent,
        "limit",
        max_drop_percent,
    )
    return {
        "ws_accepted_records": ws_accepted,
        "dropped_total": dropped,
        "ws_dropped_total": ws_dropped,
        "rest_dropped_total": rest_dropped,
        "catalog_drop_totals": catalog_drops,
        "drop_percent": drop_percent,
    }


def check_connection_groups(report, symbols, evidence):
    """Check each public/market connection group and truthful renewal reasons."""
    renewal = []
    for market in ("spot", "usdm"):
        elapsed = 0
        for line in (evidence / f"{market}.jsonl").read_text().splitlines():
            event = json.loads(line)
            elapsed = event.get("elapsed_seconds", elapsed)
            if event.get("event") == "WS_RECONNECT":
                renewal.append(
                    {"market": market, "elapsed_seconds": elapsed, **event}
                )
    report["reconnections"] = renewal
    required_groups = {
        "spot": ("spot-public",),
        "usdm": ("usdm-public", "usdm-market"),
    }
    groups = {}
    for market, names in required_groups.items():
        readback = report["markets"][market]["readback"]
        ranges = readback["connection_monotonic_ranges_ns"]
        for name in names:
            suffix = "-ws-public" if name.endswith("public") else "-ws-market"
            spans = {
                connection: span
                for connection, span in ranges.items()
                if suffix in connection
            }
            longest = max(
                ((span[1] - span[0]) / 1e9 for span in spans.values()),
                default=0,
            )
            events = [
                event
                for event in renewal
                if event["market"] == market and event.get("group") == name
            ]
            info = {
                "market": market,
                "group": name,
                "connections": len(spans),
                "max_connection_seconds": longest,
                "reconnect_events": len(events),
                "reason_counts": dict(
                    Counter(event.get("reason", "unknown") for event in events)
                ),
            }
            if longest >= 82790:
                assert any(
                    event.get("reason") == "23h_renewal"
                    and event["elapsed_seconds"] >= 82740
                    and event.get("accepted", 0) > 0
                    for event in events
                ), (market, name, "23h connection renewal not observed", events)
                assert len(spans) >= 2, (market, name, "renewal connection missing")
                info["renewal_triggered"] = True
            else:
                info["renewal_triggered"] = False
                info["note"] = (
                    "connection lifetime did not reach the scheduled 23h trigger"
                )
            groups[f"{market}:{name}"] = info
    report["connection_groups"] = groups
    for market in ("spot", "usdm"):
        readback = report["markets"][market]["readback"]
        for symbol in symbols[market]:
            key = f"{'spot' if market == 'spot' else 'um_perpetual'}/{symbol}/diff_depth"
            connections = readback["connections"][key]
            assert len(connections) >= 2, (key, "no depth reconnection")
            latest = max(
                connections,
                key=lambda c: readback["connection_monotonic_ranges_ns"][c][1],
            )
            assert latest in readback["bridge_connections"].get(key, []), (
                key,
                "latest depth connection unbridged",
            )


def archive_state():
    return subprocess.check_output(
        [
            "systemctl",
            "show",
            "sbmdr-archive.service",
            "--property",
            "ActiveState",
            "--value",
        ],
        text=True,
    ).strip()


def stop_archive(timeout_seconds):
    """Stop the timer, let an in-flight oneshot pass finish, then stop if needed."""
    subprocess.run(["systemctl", "stop", "sbmdr-archive.timer"], check=False)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        state = archive_state()
        if state not in {"active", "activating", "deactivating"}:
            return
        time.sleep(1)
    # The in-flight pass exceeded the budget; request a bounded stop.
    subprocess.run(["systemctl", "stop", "sbmdr-archive.service"], check=False)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        state = archive_state()
        if state not in {"active", "activating", "deactivating"}:
            return
        time.sleep(1)
    raise RuntimeError("archive worker did not quiesce")


def verify_stopped(a, report, progress_path=None):
    """Verify the exact completed capture, without acquiring any additional data."""
    assert (
        hashlib.sha256((a.release / "release/sbmdr-c0").read_bytes()).hexdigest()
        == report["binary_sha256"]
    )
    assert a.seconds == report["seconds_required"]
    symbols = report["config"]
    for market in ("spot", "usdm"):
        assert Path(report["markets"][market]["root"]) == a.root / market
        result = json.loads((a.evidence / f"{market}.json").read_text())
        assert result["wall_seconds"] >= a.seconds
        assert result["stop_reason"] == "SIGNAL" and result["interrupted"] is True
        report["markets"][market]["recorder"] = result
    if a.services:
        assert report["host"]["busy_percent_of_machine"] <= 50
        assert (
            report["host"]["minimum_available_bytes"]
            >= report["host"]["total_memory_bytes"] / 2
        )
        assert report["slice_cpu_peak_percent"] <= 200
        assert report["slice_cpu_60s_peak_percent"] <= a.cpu_limit_percent
    report["status"] = "VERIFYING"
    save(progress_path or (a.evidence / "progress.json"), report)
    for market in ("spot", "usdm"):
        target = a.evidence / f"{market}-readback.json"
        with target.open("w") as out:
            subprocess.run(
                [
                    sys.executable,
                    a.release / "prototype/operations.py",
                    "verify",
                    a.root / market,
                ],
                stdout=out,
                check=True,
            )
        readback = json.loads(target.read_text())
        assert readback["records"] == report["markets"][market]["recorder"]["records"]
        if not a.services:
            assert readback["ws_receive_span_seconds"] >= a.seconds - 5
        assert not list((a.root / market).rglob("*.partial")), (
            market,
            "partial artifacts remain after stop",
        )
        status = check_drops(
            market,
            report["markets"][market]["recorder"],
            readback,
            a.max_drop_percent,
        )
        status_rows = [
            json.loads(line)
            for line in (a.evidence / f"{market}.jsonl").read_text().splitlines()
            if line.strip()
        ]
        queue_peak = max(
            (row.get("queued_payload_bytes", 0) for row in status_rows), default=0
        )
        sync = [row["writer_sync_ms"] for row in status_rows if "writer_sync_ms" in row]
        seal = [row["writer_seal_ms"] for row in status_rows if "writer_seal_ms" in row]
        status.update({
            "queue_peak_bytes": queue_peak,
            "writer_sync_ms_mean": sum(sync) / len(sync) if sync else 0,
            "writer_sync_ms_max": max(sync, default=0),
            "writer_seal_ms_mean": sum(seal) / len(seal) if seal else 0,
            "writer_seal_ms_max": max(seal, default=0),
        })
        report["markets"][market]["status"] = status
        if a.queue_limit_mib is not None:
            assert queue_peak <= a.queue_limit_mib * 1024**2, (
                market,
                "queue peak",
                queue_peak,
                "limit",
                a.queue_limit_mib * 1024**2,
            )
        for symbol in symbols[market]:
            prefix = f"{'spot' if market == 'spot' else 'um_perpetual'}/{symbol}/"
            for stream in (
                ("diff_depth", "book_ticker", "agg_trade")
                if market == "spot"
                else ("diff_depth", "book_ticker", "agg_trade", "mark_price")
            ):
                assert readback["source_counts"].get(prefix + stream, 0) > 0, (
                    market,
                    symbol,
                    stream,
                )
            assert any(
                key.startswith(prefix) and key.endswith("/depth_bridge") and value
                for key, value in readback["capture_flags"].items()
            )
            if market == "usdm":
                for source in (
                    "open_interest",
                    "top_long_short_account_ratio_5m",
                    "top_long_short_position_ratio_5m",
                    "global_long_short_ratio_5m",
                    "taker_buy_sell_volume_5m",
                ):
                    assert (
                        readback["rest_statuses"].get(
                            f"um_perpetual/{symbol}/{source}/200/True", 0
                        )
                        > 0
                    ), (symbol, source, "no successful REST observation")
        report["markets"][market]["readback"] = readback
    report["cpu_mean_percent"] = sum(
        100 * m["recorder"]["cpu_seconds"] / m["recorder"]["wall_seconds"]
        for m in report["markets"].values()
    )
    report["peak_rss_sum_bytes"] = sum(
        m["recorder"]["peak_rss_bytes"] for m in report["markets"].values()
    )
    report["status_cpu_peak_sum_upper_bound_percent"] = sum(
        m["recorder"]["max_status_cpu_percent"] for m in report["markets"].values()
    )
    import shutil

    report["storage"] = {
        "stored_bytes": sum(
            m["readback"]["stored_bytes"] for m in report["markets"].values()
        ),
        "archive_available_bytes": shutil.disk_usage("/srv/recorder-data").free,
        "system_available_bytes": shutil.disk_usage(a.root).free,
    }
    daily = report["storage"]["stored_bytes"] * 86400 / a.seconds
    report["storage"]["compressed_gb_per_day"] = daily / 1e9
    report["storage"]["additional_retention_days_at_observed_rate"] = (
        report["storage"]["archive_available_bytes"] / daily
    )
    assert report["system_minimum_free_bytes"] >= 13 * 1024**3
    assert report["cpu_mean_percent"] <= a.cpu_limit_percent
    assert report["peak_rss_sum_bytes"] <= a.rss_limit_mib * 1024**2
    if a.services:
        import shutil

        archive_log = Path("/var/log/sbmdr/archive.jsonl")
        archived = (
            [json.loads(line) for line in archive_log.read_text().splitlines()]
            if archive_log.exists()
            else []
        )
        shutil.copyfile(archive_log, a.evidence / "archive.jsonl")
        report["archival"] = {
            "passes": len(archived),
            "failed_passes": sum(row["failed"] for row in archived),
            "cpu_seconds": sum(row["cpu_seconds"] for row in archived),
            "peak_rss_upper_bound_bytes": max(
                (row["peak_rss_upper_bound_bytes"] for row in archived), default=0
            ),
        }
        report["cpu_mean_with_archive_percent"] = (
            report["cpu_mean_percent"]
            + 100 * report["archival"]["cpu_seconds"] / a.seconds
        )
        report["rss_with_archive_upper_bound_bytes"] = (
            report["peak_rss_sum_bytes"]
            + report["archival"]["peak_rss_upper_bound_bytes"]
        )
        assert report["cpu_mean_with_archive_percent"] <= a.cpu_limit_percent
        assert report["rss_with_archive_upper_bound_bytes"] <= a.rss_limit_mib * 1024**2
        assert archived and any(not row["failed"] for row in archived)
        for market in ("spot", "usdm"):
            assert (
                report["markets"][market]["readback"]["archive_states"].get(
                    "LOCAL_DELETED", 0
                )
                > 0
            ), "no verified production archival"
    if a.services and a.seconds >= 86400:
        check_connection_groups(report, symbols, a.evidence)
    if a.services:
        report["cpu_budget_results"] = {
            "applied_limit_percent": a.cpu_limit_percent,
            "frozen_limit_percent": 120,
            "frozen_rolling_60s_pass": report["slice_cpu_60s_peak_percent"] <= 120,
            "frozen_recorder_mean_pass": report["cpu_mean_percent"] <= 120,
            "frozen_recorder_and_archive_mean_pass": report["cpu_mean_with_archive_percent"] <= 120,
        }
    report["limits"] = {
        "seconds": a.seconds,
        "rss_limit_mib": a.rss_limit_mib,
        "cpu_limit_percent": a.cpu_limit_percent,
        "max_drop_percent": a.max_drop_percent,
        "queue_limit_mib": a.queue_limit_mib,
    }
    report["status"] = "PASS"


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--release", type=Path, required=True)
    p.add_argument("--config", type=Path, required=True)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--evidence", type=Path, required=True)
    p.add_argument("--seconds", type=int, required=True)
    p.add_argument("--services", action="store_true")
    p.add_argument(
        "--verify-only",
        action="store_true",
        help="Re-verify an already stopped capture from its evidence; never starts a recorder",
    )
    p.add_argument("--rss-limit-mib", type=int, default=768)
    p.add_argument("--cpu-limit-percent", type=float, default=120)
    p.add_argument(
        "--max-drop-percent",
        type=float,
        default=0.0,
        help="Maximum dropped_total as percent of accepted WS records; 0 requires none",
    )
    p.add_argument(
        "--queue-limit-mib",
        type=int,
        default=None,
        help="Optional billed queue peak limit in MiB; omit for C4-C",
    )
    a = p.parse_args()
    a.evidence.mkdir(parents=True, exist_ok=True)
    symbols = json.loads(a.config.read_text())
    if a.verify_only:
        progress = a.evidence / "progress.json"
        assert progress.is_file(), f"missing stopped run report: {progress}"
        report = json.loads(progress.read_text())
        assert report["binary_sha256"] == hashlib.sha256(
            (a.release / "release/sbmdr-c0").read_bytes()
        ).hexdigest(), "release identity differs from the stopped run"
        assert report["config"] == symbols, "config identity differs from the stopped run"
        assert report["seconds_required"] == a.seconds, "duration differs from the stopped run"
        for market in ("spot", "usdm"):
            assert Path(report["markets"][market]["root"]) == a.root / market, (
                market,
                "root identity differs from the stopped run",
            )
            assert (a.evidence / f"{market}.json").is_file(), (
                market,
                "stopped recorder summary is missing",
            )
            assert (a.evidence / f"{market}.jsonl").is_file(), (
                market,
                "status log is missing",
            )
        started = report.get("start_utc")
        required = report.get("seconds_required")
        try:
            verify_stopped(a, report, a.evidence / "verify-only-progress.json")
        except BaseException as error:
            report["status"] = "FAILED"
            report["error"] = repr(error)
            save(a.evidence / "verify-only.json", report)
            raise
        report["verification"] = {
            "mode": "verify-only",
            "original_start_utc": started,
            "seconds_required": required,
            "verified_utc": datetime.now(UTC).isoformat(),
        }
        save(a.evidence / "verify-only.json", report)
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "evidence": str(a.evidence / "verify-only.json"),
                    "original_start_utc": started,
                    "seconds_required": required,
                },
                indent=2,
            )
        )
        return
    children = {}
    handles = []
    report = {
        "status": "RUNNING",
        "start_utc": datetime.now(UTC).isoformat(),
        "seconds_required": a.seconds,
        "config": symbols,
        "binary_sha256": hashlib.sha256(
            (a.release / "release/sbmdr-c0").read_bytes()
        ).hexdigest(),
        "markets": {},
    }
    save(a.evidence / "progress.json", report)
    try:
        for market in ("spot", "usdm"):
            root = a.root / market
            root.mkdir(parents=True, exist_ok=True)
            if a.services:
                child = SystemdChild(market)
            else:
                out = (a.evidence / f"{market}.json").open("w")
                err = (a.evidence / f"{market}.jsonl").open("w")
                handles.extend([out, err])
                child = subprocess.Popen(
                    [
                        a.release / "release/sbmdr-c0",
                        "record",
                        "--public",
                        root,
                        "9223372036854775807",
                        "/etc/ssl/certs/ca-certificates.crt",
                        market,
                        a.config,
                    ],
                    stdout=out,
                    stderr=err,
                )
            children[market] = child
            report["markets"][market] = {"pid": child.pid, "root": str(root)}
            if a.services:
                report["markets"][market]["cpu_weight"] = int(child.property("CPUWeight"))
        start = time.monotonic()
        cpu_samples = deque()
        slice_cpu = None
        if a.services:
            group = subprocess.check_output(
                [
                    "systemctl",
                    "show",
                    "sbmdr.slice",
                    "--property",
                    "ControlGroup",
                    "--value",
                ],
                text=True,
            ).strip()
            slice_cpu = Path("/sys/fs/cgroup") / group.lstrip("/") / "cpu.stat"

        def cpu_sample():
            values = dict(line.split() for line in slice_cpu.read_text().splitlines())
            return time.monotonic(), int(values["usage_usec"]) / 1e6

        previous_sample = cpu_sample() if slice_cpu else None
        report["slice_cpu_peak_percent"] = 0
        report["slice_cpu_60s_peak_percent"] = 0
        host_start = host_sample()
        minimum_available = host_start["MemAvailable"]
        next_check = start
        while time.monotonic() - start < a.seconds:
            report["elapsed_seconds"] = time.monotonic() - start
            if slice_cpu:
                current = cpu_sample()
                percent = (
                    100
                    * (current[1] - previous_sample[1])
                    / (current[0] - previous_sample[0])
                )
                if current[0] - previous_sample[0] >= 0.9:
                    report["slice_cpu_peak_percent"] = max(
                        report["slice_cpu_peak_percent"], percent
                    )
                cpu_samples.append(current)
                while len(cpu_samples) > 1 and current[0] - cpu_samples[1][0] >= 60:
                    cpu_samples.popleft()
                if current[0] - cpu_samples[0][0] >= 60:
                    rolling = (
                        100
                        * (current[1] - cpu_samples[0][1])
                        / (current[0] - cpu_samples[0][0])
                    )
                    report["slice_cpu_60s_peak_percent"] = max(
                        report["slice_cpu_60s_peak_percent"], rolling
                    )
                previous_sample = current
            if time.monotonic() >= next_check:
                for market, child in children.items():
                    if child.poll() is not None:
                        raise RuntimeError(
                            f"{market} recorder exited early: {child.returncode}"
                        )
                    status = Path(f"/proc/{child.pid}/status").read_text()
                    report["markets"][market]["rss_kib"] = int(
                        status.split("VmRSS:")[1].split()[0]
                    )
                    report["markets"][market]["threads_peak"] = max(
                        report["markets"][market].get("threads_peak", 0),
                        int(status.split("Threads:")[1].split()[0]),
                    )
                    wal = a.root / market / "state/catalog.sqlite-wal"
                    report["markets"][market]["wal_peak_bytes"] = max(
                        report["markets"][market].get("wal_peak_bytes", 0),
                        wal.stat().st_size if wal.exists() else 0,
                    )
                import shutil

                free = shutil.disk_usage(a.root).free
                report["system_minimum_free_bytes"] = min(
                    report.get("system_minimum_free_bytes", free), free
                )
                minimum_available = min(
                    minimum_available, host_sample()["MemAvailable"]
                )
                next_check = time.monotonic() + 30
                save(a.evidence / "progress.json", report)
            time.sleep(
                min(
                    1 if a.services else 30,
                    max(0, a.seconds - report["elapsed_seconds"]),
                )
            )
        for child in children.values():
            child.send_signal(signal.SIGTERM)
        if a.services:
            stop_archive(600)
        for market, child in children.items():
            child.wait(timeout=180)
            if a.services:
                import shutil

                shutil.copyfile(
                    f"/var/log/sbmdr/{market}.stdout.jsonl",
                    a.evidence / f"{market}.json",
                )
                shutil.copyfile(
                    f"/var/log/sbmdr/{market}.status.jsonl",
                    a.evidence / f"{market}.jsonl",
                )
            if child.returncode != 0:
                raise RuntimeError(f"{market} exit {child.returncode}")
            result = json.loads((a.evidence / f"{market}.json").read_text())
            report["markets"][market]["recorder"] = result
        host_end = host_sample()
        report["host"] = {
            "busy_percent_of_machine": 100
            * (host_end["cpu_busy"] - host_start["cpu_busy"])
            / (host_end["cpu_total"] - host_start["cpu_total"]),
            "stolen_percent_of_machine": 100
            * (host_end["cpu_steal"] - host_start["cpu_steal"])
            / (host_end["cpu_total"] - host_start["cpu_total"]),
            "minimum_available_bytes": minimum_available,
            "total_memory_bytes": host_start["MemTotal"],
        }
        verify_stopped(a, report)
    except BaseException as error:
        report["status"] = "FAILED"
        report["error"] = repr(error)
        raise
    finally:
        if a.services:
            stop_archive(600)
        for child in children.values():
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)
        for child in children.values():
            try:
                child.wait(timeout=180)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for handle in handles:
            handle.close()
        report["finish_utc"] = datetime.now(UTC).isoformat()
        save(a.evidence / "progress.json", report)


if __name__ == "__main__":
    main()
