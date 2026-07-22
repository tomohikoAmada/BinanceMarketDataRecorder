"""Honest pre-service status."""

from __future__ import annotations

from typing import Any


def service_status() -> dict[str, Any]:
    """Report that runtime collection is still unimplemented."""

    return {
        "command": "status",
        "status": "NOT_IMPLEMENTED",
        "service_implemented": False,
        "collector_implemented": False,
        "network_connected": False,
        "detail": (
            "M3 provides durable Raw/Catalog foundations only; the service and "
            "collectors remain unimplemented."
        ),
    }
