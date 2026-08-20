"""Persisted capacity observations, robust growth rates, threshold state, and ETAs."""

from __future__ import annotations

import math
import shutil
import statistics
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Any, cast

from .capacity import CapacityProfile, evaluate_capacity
from .catalog import Catalog

GIB = 1024**3
WINDOWS_SECONDS = {
    "1h": 60 * 60,
    "6h": 6 * 60 * 60,
    "24h": 24 * 60 * 60,
    "7d": 7 * 24 * 60 * 60,
}
FORECAST_SCHEMA_VERSION = "storage-forecast.v1"
MAX_DATETIME_UTC_NS = 253_402_300_799_999_999_999


class SpaceSeverity(StrEnum):
    OK = "OK"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


def threshold_bytes(total_bytes: int) -> dict[str, int]:
    if total_bytes <= 0:
        raise ValueError("total bytes must be positive")
    return {
        "warning_40_percent": total_bytes * 40 // 100,
        "critical_15_percent": total_bytes * 15 // 100,
        "emergency": max(10 * GIB, total_bytes * 5 // 100),
        "exhausted": 0,
    }


def hard_reserve_bytes(total_bytes: int, *, rotation_bytes: int) -> int:
    if total_bytes <= 0 or rotation_bytes <= 0:
        raise ValueError("capacity and rotation bytes must be positive")
    return max(5 * GIB, total_bytes * 2 // 100, rotation_bytes * 2)


def space_severity(total_bytes: int, free_bytes: int) -> SpaceSeverity:
    if total_bytes <= 0 or free_bytes < 0 or free_bytes > total_bytes:
        raise ValueError("invalid capacity observation")
    limits = threshold_bytes(total_bytes)
    if free_bytes <= limits["emergency"]:
        return SpaceSeverity.EMERGENCY
    if free_bytes * 100 <= total_bytes * 15:
        return SpaceSeverity.CRITICAL
    if free_bytes * 100 <= total_bytes * 40:
        return SpaceSeverity.WARNING
    return SpaceSeverity.OK


def _nearest_existing(path: Path) -> Path:
    candidate = path.resolve()
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def _sample_int(row: dict[str, object], key: str) -> int:
    value = row.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"invalid space sample {key}")
    return value


def _window_samples(
    samples: Sequence[dict[str, object]], *, now_utc_ns: int, window_seconds: int
) -> list[dict[str, object]]:
    cutoff = now_utc_ns - window_seconds * 1_000_000_000
    tolerance_ns = int(window_seconds * 0.2 * 1_000_000_000)
    eligible = [
        row
        for row in samples
        if _sample_int(row, "observed_at_utc_ns") <= now_utc_ns
    ]
    before = [
        row for row in eligible if _sample_int(row, "observed_at_utc_ns") <= cutoff
    ]
    inside = [
        row for row in eligible if _sample_int(row, "observed_at_utc_ns") > cutoff
    ]
    anchor = (
        [before[-1]]
        if before
        and cutoff - _sample_int(before[-1], "observed_at_utc_ns") <= tolerance_ns
        else []
    )
    selected = anchor + inside
    return selected


def _window_rate(
    samples: Sequence[dict[str, object]], *, now_utc_ns: int, window_seconds: int
) -> dict[str, object]:
    selected = _window_samples(
        samples, now_utc_ns=now_utc_ns, window_seconds=window_seconds
    )
    if len(selected) < 2:
        return {
            "status": "INSUFFICIENT_DATA",
            "bytes_per_second": None,
            "sample_count": len(selected),
            "covered_seconds": 0,
        }
    first_at = _sample_int(selected[0], "observed_at_utc_ns")
    last_at = _sample_int(selected[-1], "observed_at_utc_ns")
    covered_seconds = (last_at - first_at) / 1_000_000_000
    if (
        covered_seconds < window_seconds * 0.8
        or now_utc_ns - last_at > window_seconds * 0.2 * 1_000_000_000
    ):
        return {
            "status": "INSUFFICIENT_DATA",
            "bytes_per_second": None,
            "sample_count": len(selected),
            "covered_seconds": round(covered_seconds, 6),
        }
    rates: list[float] = []
    for previous, current in pairwise(selected):
        if _sample_int(previous, "total_bytes") != _sample_int(
            current, "total_bytes"
        ):
            continue
        elapsed_ns = _sample_int(current, "observed_at_utc_ns") - _sample_int(
            previous, "observed_at_utc_ns"
        )
        if elapsed_ns <= 0:
            continue
        consumed = _sample_int(previous, "free_bytes") - _sample_int(
            current, "free_bytes"
        )
        rates.append(consumed * 1_000_000_000 / elapsed_ns)
    if not rates:
        return {
            "status": "INSUFFICIENT_DATA",
            "bytes_per_second": None,
            "sample_count": len(selected),
            "covered_seconds": round(covered_seconds, 6),
        }
    rate = float(statistics.median(rates))
    if not math.isfinite(rate):
        raise ValueError("non-finite storage growth rate")
    return {
        "status": "AVAILABLE",
        "bytes_per_second": round(rate, 6),
        "sample_count": len(selected),
        "covered_seconds": round(covered_seconds, 6),
    }


def _eta(
    *,
    now_utc_ns: int,
    free_bytes: int,
    threshold: int,
    rate_bytes_per_second: float | None,
    rate_status: str,
) -> dict[str, object]:
    if free_bytes <= threshold:
        return {
            "status": "REACHED",
            "utc_ns": now_utc_ns,
            "utc": datetime.fromtimestamp(
                now_utc_ns / 1_000_000_000, tz=UTC
            ).isoformat(),
        }
    if rate_status == "INSUFFICIENT_DATA" or rate_bytes_per_second is None:
        return {"status": "INSUFFICIENT_DATA", "utc_ns": None, "utc": None}
    if rate_bytes_per_second <= 0:
        return {"status": "NOT_APPROACHING", "utc_ns": None, "utc": None}
    seconds = (free_bytes - threshold) / rate_bytes_per_second
    if not math.isfinite(seconds) or seconds < 0:
        raise ValueError("invalid threshold ETA")
    eta_ns = now_utc_ns + int(seconds * 1_000_000_000)
    if eta_ns > MAX_DATETIME_UTC_NS:
        return {"status": "BEYOND_SUPPORTED_RANGE", "utc_ns": None, "utc": None}
    return {
        "status": "FORECAST",
        "utc_ns": eta_ns,
        "utc": datetime.fromtimestamp(eta_ns / 1_000_000_000, tz=UTC).isoformat(),
    }


class StorageForecaster:
    def __init__(
        self,
        *,
        catalog: Catalog,
        data_root: Path,
        utc_clock_ns: Callable[[], int] = time.time_ns,
        disk_usage: Callable[[Path], Any] = shutil.disk_usage,
    ) -> None:
        self.catalog = catalog
        self.data_root = data_root
        self.utc_clock_ns = utc_clock_ns
        self.disk_usage = disk_usage

    def observe_internal(self, *, observed_at_utc_ns: int | None = None) -> bool:
        observed_at = (
            self.utc_clock_ns() if observed_at_utc_ns is None else observed_at_utc_ns
        )
        usage = self.disk_usage(_nearest_existing(self.data_root))
        return self.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=int(usage.total),
            free_bytes=int(usage.free),
            observed_at_utc_ns=observed_at,
        )

    def observe(
        self,
        *,
        scope_id: str,
        storage_id: str | None,
        total_bytes: int,
        free_bytes: int,
        observed_at_utc_ns: int,
    ) -> bool:
        backlog_bytes = 0
        oldest: int | None = None
        if scope_id == "internal":
            lifecycle = self.catalog.source_lifecycle_aggregate()
            backlog_bytes = cast(int, lifecycle["unarchived_backlog_bytes"])
            oldest_value = lifecycle["oldest_unarchived_at_utc_ns"]
            oldest = int(oldest_value) if isinstance(oldest_value, int) else None
        severity = space_severity(total_bytes, free_bytes)
        return self.catalog.record_space_sample(
            sample_id=f"space:{scope_id}:{observed_at_utc_ns}",
            scope_id=scope_id,
            storage_id=storage_id,
            observed_at_utc_ns=observed_at_utc_ns,
            total_bytes=total_bytes,
            free_bytes=free_bytes,
            archive_backlog_bytes=backlog_bytes,
            oldest_unarchived_at_utc_ns=oldest,
            severity=severity,
        )

    def forecast(
        self,
        scope_id: str,
        *,
        now_utc_ns: int | None = None,
        capacity_profile: CapacityProfile | None = None,
    ) -> dict[str, object]:
        if capacity_profile is not None:
            capacity_profile.validate_scope(scope_id)
        now = self.utc_clock_ns() if now_utc_ns is None else now_utc_ns
        samples = self.catalog.space_samples(scope_id)
        if not samples:
            return {
                "scope_id": scope_id,
                "status": "INSUFFICIENT_DATA",
                "sample_count": 0,
            }
        current = max(
            (
                row
                for row in samples
                if _sample_int(row, "observed_at_utc_ns") <= now
            ),
            key=lambda row: _sample_int(row, "observed_at_utc_ns"),
            default=None,
        )
        if current is None:
            return {
                "scope_id": scope_id,
                "status": "INSUFFICIENT_DATA",
                "sample_count": 0,
            }
        total = _sample_int(current, "total_bytes")
        free = _sample_int(current, "free_bytes")
        windows = {
            name: _window_rate(
                samples, now_utc_ns=now, window_seconds=window_seconds
            )
            for name, window_seconds in WINDOWS_SECONDS.items()
        }
        available_rates = [
            float(value["bytes_per_second"])
            for value in windows.values()
            if value["status"] == "AVAILABLE"
            and isinstance(value["bytes_per_second"], (int, float))
        ]
        selected_rate = max(available_rates) if available_rates else None
        rate_status = (
            "INSUFFICIENT_DATA"
            if selected_rate is None
            else ("NOT_APPROACHING" if selected_rate <= 0 else "AVAILABLE")
        )
        thresholds = threshold_bytes(total)
        oldest_unarchived = current.get("oldest_unarchived_at_utc_ns")
        result: dict[str, object] = {
            "scope_id": scope_id,
            "storage_id": current.get("storage_id"),
            "status": space_severity(total, free),
            "observed_at_utc_ns": _sample_int(current, "observed_at_utc_ns"),
            "total_bytes": total,
            "free_bytes": free,
            "free_percent": round(free / total * 100, 6),
            "threshold_bytes": thresholds,
            "net_growth": {
                "status": rate_status,
                "selected_bytes_per_second": (
                    None if selected_rate is None else round(selected_rate, 6)
                ),
                "selection": "MAX_AVAILABLE_WINDOW_MEDIAN",
                "windows": windows,
            },
            "eta": {
                name: _eta(
                    now_utc_ns=now,
                    free_bytes=free,
                    threshold=threshold,
                    rate_bytes_per_second=selected_rate,
                    rate_status=rate_status,
                )
                for name, threshold in thresholds.items()
            },
            "archive_backlog_bytes": _sample_int(
                current, "archive_backlog_bytes"
            ),
            "oldest_unarchived_at_utc_ns": oldest_unarchived,
            "oldest_unarchived_age_ns": (
                max(0, now - oldest_unarchived)
                if isinstance(oldest_unarchived, int)
                and not isinstance(oldest_unarchived, bool)
                else None
            ),
            "sample_count": len(samples),
        }
        if capacity_profile is not None:
            hard_reserve_eta = _eta(
                now_utc_ns=now,
                free_bytes=free,
                threshold=capacity_profile.hard_reserve_bytes,
                rate_bytes_per_second=selected_rate,
                rate_status=rate_status,
            )
            decision = evaluate_capacity(
                profile=capacity_profile,
                scope_id=scope_id,
                total_bytes=total,
                free_bytes=free,
                hard_reserve_eta=hard_reserve_eta,
                now_utc_ns=now,
            )
            result.update(
                {
                    "capacity_profile": decision.profile_id,
                    "capacity_state": decision.state.value,
                    "hard_reserve_eta": hard_reserve_eta,
                }
            )
        return result

    def document(
        self,
        scope_ids: Sequence[str],
        *,
        now_utc_ns: int | None = None,
        capacity_profile: CapacityProfile | None = None,
    ) -> dict[str, object]:
        now = self.utc_clock_ns() if now_utc_ns is None else now_utc_ns
        targets = [
            self.forecast(
                scope,
                now_utc_ns=now,
                capacity_profile=(
                    capacity_profile
                    if capacity_profile is not None and scope == capacity_profile.scope
                    else None
                ),
            )
            for scope in scope_ids
        ]
        return {
            "schema_version": FORECAST_SCHEMA_VERSION,
            "generated_at_utc_ns": now,
            "status": "OK",
            "targets": targets,
        }
