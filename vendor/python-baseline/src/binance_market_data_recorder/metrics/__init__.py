"""Durable operational metrics and deterministic UTC daily reports."""

from __future__ import annotations

from typing import Any

from .recorder import MetricsRecorder

__all__ = ["DailyReporter", "MetricsRecorder"]


def __getattr__(name: str) -> Any:
    if name == "DailyReporter":
        from .report import DailyReporter

        return DailyReporter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
