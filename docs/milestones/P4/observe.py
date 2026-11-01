"""Bounded operator for the single P4 BTC run, not a qualification framework.

Run as a native, independent systemd job. It never changes capture configuration.
Results require review of resource samples and actual exercised paths.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any

from binance_market_data_recorder.metrics.continuity import continuity_report
from binance_market_data_recorder.storage.catalog import Catalog

ROOT = Path("/var/lib/binance-market-data-recorder/p4-20261101-btc-b")
EVIDENCE = ROOT / "evidence"
UNIT = "binance-market-data-p4b-btc.service"
ARCHIVE = "binance-market-data-p4b-archive.service"


def publish(name: str, value: Any) -> None:
    with (EVIDENCE / name).open("x") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))


def systemctl(*args: str) -> str:
    result = subprocess.run(
        ["systemctl", *args], text=True, capture_output=True, timeout=200, check=False
    )
    if result.returncode:
        raise RuntimeError(f"systemctl {args}: {result.stderr}")
    return result.stdout


def sample() -> dict[str, Any]:
    state = json.loads((ROOT / "state/service_state.json").read_text())
    # Catalog keeps source-local evidence. Do not replicate the static 2MiB
    # inventory and every healthy source in each 30-second resource sample.
    coverage = state["selected_asset_coverage"]
    state["selected_asset_coverage"] = {
        k: coverage[k]
        for k in (
            "products",
            "directories_observed",
            "transport_progress",
            "availability_journal_pending",
        )
    }
    for products in state["products"].values():
        for product in products.values():
            product.pop("side_data", None)
    state.pop("shared_public_sources", None)
    with Catalog.open_live_read_only(ROOT / "state/catalog.sqlite") as catalog:
        availability = catalog.source_availability()
        verification = catalog.raw_verification_status()
    unit = dict(
        line.split("=", 1)
        for line in systemctl(
            "show",
            UNIT,
            "-p",
            "MainPID",
            "-p",
            "InvocationID",
            "-p",
            "ActiveState",
            "-p",
            "NRestarts",
            "-p",
            "MemoryCurrent",
            "-p",
            "MemoryPeak",
            "-p",
            "CPUUsageNSec",
            "-p",
            "TasksCurrent",
        ).splitlines()
    )
    return {
        "utc_ns": time.time_ns(),
        "boottime_ns": time.clock_gettime_ns(int(vars(time)["CLOCK_BOOTTIME"])),
        "unit": unit,
        "service": state,
        "verification": verification,
        "disk_free": shutil.disk_usage(ROOT).free,
        "archive_disk_free": shutil.disk_usage("/srv/recorder-data").free,
        "host_cpu": Path("/proc/stat").read_text().splitlines()[0],
        "host_memory": Path("/proc/meminfo").read_text(),
        "availability_count": len(availability),
        "unavailable": {
            k: v
            for k, v in availability.items()
            if not v.get("dormant") and v["unavailable_since_utc_ns"] is not None
        },
    }


def checkpoint(freeze: dict[str, Any], start: int, end: int, label: str) -> dict[str, Any]:
    with Catalog.open_live_read_only(ROOT / "state/catalog.sqlite") as catalog:
        high_water = catalog.raw_verification_status()["enrollment_high_water_utc_ns"]
        assert isinstance(high_water, int)
    publish(f"{label}-boundary.json", {"start": start, "end": end, "high_water": high_water})
    # Let normal workers finish precisely this sealed boundary while capture continues.
    deadline = time.monotonic() + 900
    while True:
        with Catalog.open_live_read_only(ROOT / "state/catalog.sqlite") as catalog:
            verification = catalog.raw_verification_status(through_utc_ns=high_water)
            states = catalog.source_availability()
        if (
            verification["pending_files"] == 0
            and all(s["observed_at_utc_ns"] >= end or s.get("dormant") for s in states.values())
        ) or time.monotonic() >= deadline:
            break
        time.sleep(5)
    expected = {
        "/".join((s["venue_market"], s["symbol"], s["stream"], s["transport"])): s
        for s in freeze["sources"]
    }
    with Catalog.open_live_read_only(ROOT / "state/catalog.sqlite") as catalog:
        report = continuity_report(
            catalog, start=start, end=end, source_ids=sorted(set(expected) | set(states))
        )
    dormant = {
        key
        for key, s in expected.items()
        if s["lifecycle"] == "after_delivery"
        and not any(
            p["delivery_time_ms"] is not None
            and p["key"]["symbol"] in s["products"]
            and p["delivery_time_ms"] * 1_000_000 < end
            for p in freeze["products"]
        )
    }
    failed = [
        s["source_id"]
        for s in report["sources"]
        if s["source_id"] not in dormant and (s["status"] != "OBSERVED" or s["coverage"] < 0.995)
    ]
    enrolled = verification["enrolled_files"]
    assert isinstance(enrolled, int)
    result = {
        "continuity": report,
        "predeclared_dormant_sources": sorted(dormant),
        "verification": verification,
        "failed_sources": failed,
        "continuity_and_verification_pass": not failed
        and enrolled > 0
        and verification["pending_files"] == 0
        and verification["failed_files"] == 0,
        "resource_and_actual_paths_review_required": True,
        "production_qualified": False,
    }
    publish(f"{label}-result.json", result)
    return result


def main() -> None:
    freeze = json.loads((EVIDENCE / "freeze.json").read_text())
    config_path = Path("/opt/binance-market-data-recorder/p4-20261101-btc-b/recorder.toml")
    if hashlib.sha256(config_path.read_bytes()).hexdigest() != freeze["config_sha256"]:
        raise RuntimeError("P4 configuration changed after freeze")
    initial_boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    # Preparation deadline is independently armed before capture starts.
    expected = {
        "/".join((s["venue_market"], s["symbol"], s["stream"], s["transport"]))
        for s in freeze["sources"]
        if s["lifecycle"] != "after_delivery"
    }
    start: int | None = None
    start_boot: int | None = None
    identity = None
    checkpoints = iter((7200, 86400))
    target = next(checkpoints)
    with (EVIDENCE / "samples.jsonl").open("x") as log:
        while True:
            try:
                current = sample()
            except (OSError, ValueError):
                time.sleep(1)
                continue
            log.write(json.dumps(current, separators=(",", ":")) + "\n")
            log.flush()
            publish_status = {
                "utc_ns": current["utc_ns"],
                "phase": "PREPARING" if start is None else "RUNNING",
                "unavailable_count": len(current["unavailable"]),
                "reasons": dict(Counter(s["reason"] for s in current["unavailable"].values())),
            }
            print(json.dumps(publish_status), flush=True)
            state, unit = current["service"], current["unit"]
            if unit["ActiveState"] != "active" or state["status"] == "FAILED":
                publish("terminated.json", {"reason": "CAPTURE_NOT_RUNNING", "sample": current})
                return
            if start is None:
                with Catalog.open_live_read_only(ROOT / "state/catalog.sqlite") as catalog:
                    states = catalog.source_availability()
                actual_products = state["selected_asset_coverage"]["products"]
                ready = (
                    {
                        (p["key"]["market"], p["key"]["symbol"])
                        for p in actual_products
                        if p["capture_status"] == "ADMITTED"
                    }
                    == {(p["key"]["market"], p["key"]["symbol"]) for p in freeze["products"]}
                    and all(
                        p["orderbook_synchronized"]
                        for products in state["products"].values()
                        for p in products.values()
                    )
                    and expected <= states.keys()
                    and not (expected & current["unavailable"].keys())
                )
                if ready:
                    start, start_boot = current["utc_ns"], current["boottime_ns"]
                    identity = (unit["MainPID"], unit["InvocationID"], state["service_instance_id"])
                    publish(
                        "start.json",
                        {
                            "start_utc_ns": start,
                            "start_boottime_ns": start_boot,
                            "identity": identity,
                            "boot_id": initial_boot,
                        },
                    )
                    systemctl("stop", "binance-market-data-p4b-prep-stop.timer")
                time.sleep(10)
                continue
            if (
                identity != (unit["MainPID"], unit["InvocationID"], state["service_instance_id"])
                or Path("/proc/sys/kernel/random/boot_id").read_text().strip() != initial_boot
            ):
                publish("terminated.json", {"reason": "IDENTITY_CHANGED", "sample": current})
                systemctl("stop", UNIT)
                return
            assert start_boot is not None
            if current["boottime_ns"] - start_boot >= target * 1_000_000_000:
                result = checkpoint(
                    freeze, start, start + target * 1_000_000_000, "2h" if target == 7200 else "24h"
                )
                if target == 86400 or not result["continuity_and_verification_pass"]:
                    systemctl("stop", UNIT)
                    publish(
                        "stopped.json",
                        {
                            "reason": "24H_OBSERVED_REVIEW_REQUIRED"
                            if target == 86400
                            else "2H_CHECK_FAILED",
                            "stopped_at_utc_ns": time.time_ns(),
                            "unit_state": systemctl(
                                "show", UNIT, "-p", "ActiveState", "-p", "UnitFileState"
                            ),
                        },
                    )
                    return
                target = next(checkpoints)
            time.sleep(30)


if __name__ == "__main__":
    main()
