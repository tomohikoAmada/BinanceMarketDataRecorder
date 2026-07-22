"""Honest status before the supervised service milestone."""

from __future__ import annotations

from typing import Any


def service_status() -> dict[str, Any]:
    """Report implemented capture libraries without inventing a service."""

    return {
        "command": "status",
        "status": "NOT_RUNNING",
        "service_implemented": False,
        "collector_implemented": True,
        "implemented_markets": ["spot", "um_perpetual"],
        "network_connected": False,
        "detail": (
            "M5 implements Spot and USD-M Collector libraries, but no supervised service "
            "is running or inspected by this command."
        ),
    }
