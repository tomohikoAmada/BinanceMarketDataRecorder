"""Native macOS launchd and Linux systemd lifecycle support."""

from .launchd import LaunchAgentError, LaunchAgentManager, validate_service_label
from .lock import ServiceAlreadyRunning, ServiceProcessLock
from .power import CaffeinateAssertion, ClockDiscontinuityDetector, MacSleepObserver
from .runtime import ServiceRuntime, run_service
from .state import ServiceStateStore
from .systemd import SystemdError, SystemdManager

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
