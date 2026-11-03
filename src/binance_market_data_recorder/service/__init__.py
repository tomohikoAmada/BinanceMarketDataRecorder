"""Native macOS launchd and Linux systemd lifecycle support."""

from typing import TYPE_CHECKING, Any

from .launchd import LaunchAgentError, LaunchAgentManager, validate_service_label
from .lock import ServiceAlreadyRunning, ServiceProcessLock
from .power import CaffeinateAssertion, ClockDiscontinuityDetector, MacSleepObserver
from .state import ServiceStateStore
from .systemd import SystemdError, SystemdManager

if TYPE_CHECKING:
    from .runtime import ServiceRuntime, run_service


def __getattr__(name: str) -> Any:
    if name in {"ServiceRuntime", "run_service"}:
        from . import runtime

        return getattr(runtime, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CaffeinateAssertion",
    "ClockDiscontinuityDetector",
    "LaunchAgentError",
    "LaunchAgentManager",
    "MacSleepObserver",
    "ServiceAlreadyRunning",
    "ServiceProcessLock",
    "ServiceRuntime",
    "ServiceStateStore",
    "SystemdError",
    "SystemdManager",
    "run_service",
    "validate_service_label",
]
