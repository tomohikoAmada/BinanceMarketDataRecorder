from __future__ import annotations

import importlib
import os
import sys
from threading import Event

import pytest

from binance_market_data_recorder.service.power import (
    CaffeinateAssertion,
    MacSleepObserver,
)


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS platform integration")
def test_nsworkspace_sleep_observer_registers_and_cleans_up() -> None:
    appkit = importlib.import_module("AppKit")
    observed: list[str] = []
    delivered = Event()

    def callback(event: str, _occurred_at: int) -> None:
        observed.append(event)
        if len(observed) == 2:
            delivered.set()

    observer = MacSleepObserver(callback, run_loop_seconds=0.01)
    observer.start()
    center = appkit.NSWorkspace.sharedWorkspace().notificationCenter()
    center.postNotificationName_object_(
        appkit.NSWorkspaceWillSleepNotification,
        None,
    )
    center.postNotificationName_object_(
        appkit.NSWorkspaceDidWakeNotification,
        None,
    )
    assert delivered.wait(timeout=2.0)
    observer.stop()
    assert observed == ["will_sleep", "did_wake"]


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS platform integration")
def test_real_scoped_idle_sleep_assertion_starts_and_releases() -> None:
    assertion = CaffeinateAssertion(enabled=True, service_pid=os.getpid())
    assertion.start()
    assert assertion.active is True
    assertion.stop()
    assert assertion.active is False
