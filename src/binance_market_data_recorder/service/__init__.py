"""Native macOS service lifecycle support."""

from .launchd import LaunchAgentError, LaunchAgentManager, validate_service_label
from .lock import ServiceAlreadyRunning, ServiceProcessLock
from .power import CaffeinateAssertion, ClockDiscontinuityDetector, MacSleepObserver
from .runtime import ServiceRuntime, run_service
from .state import ServiceStateStore

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
    "run_service",
    "validate_service_label",
]
