"""Honest M1 service status."""

from __future__ import annotations

from typing import Any


def service_status() -> dict[str, Any]:
    """Report that runtime collection does not exist in M1."""

    return {
        "command": "status",
        "status": "NOT_IMPLEMENTED",
        "service_implemented": False,
        "collector_implemented": False,
        "network_connected": False,
        "detail": "M1 provides the service skeleton only; collectors begin in later milestones.",
    }
