from __future__ import annotations

import pytest

from tools.qualification_observer_state import observer_finished


@pytest.mark.parametrize("active", ["active", "activating", "deactivating"])
def test_running_owned_observer_does_not_grant_completion(active: str) -> None:
    assert not observer_finished({"ActiveState": active, "MainPID": "123",
                                  "Result": "success", "InvocationID": "owned"}, "owned")


@pytest.mark.parametrize("active", ["failed", "inactive"])
def test_failed_owned_observer_is_propagated_without_waiting(active: str) -> None:
    with pytest.raises(RuntimeError, match=r"observer (failed|exited unsuccessfully)"):
        observer_finished({"ActiveState": active, "MainPID": "0",
                           "Result": "exit-code", "InvocationID": "owned"}, "owned")


def test_only_stopped_successful_same_invocation_is_complete() -> None:
    state = {"ActiveState": "inactive", "MainPID": "0",
             "Result": "success", "InvocationID": "owned"}
    assert observer_finished(state, "owned")
    for expected in ("", "other"):
        with pytest.raises(RuntimeError, match="replaced or lost"):
            observer_finished(state, expected)
    state["MainPID"] = "123"
    assert not observer_finished(state, "owned")
