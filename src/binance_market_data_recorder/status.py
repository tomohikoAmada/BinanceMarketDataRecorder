"""Honest status before the supervised service milestone."""

from __future__ import annotations

from typing import Any


def service_status() -> dict[str, Any]:
    """Report implemented Spot capture without inventing a running service."""

    return {
        "command": "status",
        "status": "NOT_RUNNING",
        "service_implemented": False,
        "collector_implemented": True,
        "implemented_markets": ["spot"],
        "network_connected": False,
        "detail": (
            "M4 implements the Spot Collector library, but no supervised service "
            "is running or inspected by this command."
        ),
    }
