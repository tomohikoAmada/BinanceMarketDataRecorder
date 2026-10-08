"""Pure state check for a pinned native qualification observer."""

from collections.abc import Mapping


def observer_finished(state: Mapping[str, str], invocation: str) -> bool:
    """Return success only after exit; propagate a failed observer immediately."""
    if not invocation or state.get("InvocationID") != invocation:
        raise RuntimeError("observer replaced or lost")
    if state.get("ActiveState") == "failed":
        raise RuntimeError(f"owned Formal observer failed: {state.get('Result')}")
    if state.get("ActiveState") == "inactive" and state.get("MainPID") == "0":
        if state.get("Result") != "success":
            raise RuntimeError(
                f"owned Formal observer exited unsuccessfully: {state.get('Result')}"
            )
        return True
    return False
