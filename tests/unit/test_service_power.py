from __future__ import annotations

import subprocess
from collections.abc import Sequence

import pytest

from binance_market_data_recorder.service.power import (
    CaffeinateAssertion,
    ClockDiscontinuityDetector,
)


class FakeProcess:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def wait(self, timeout: float | None = None) -> int:
        assert timeout is None or timeout > 0
        if self.returncode is None:
            raise subprocess.TimeoutExpired("fake", timeout or 0)
        return self.returncode


def test_clock_discontinuity_marks_explicit_gap() -> None:
    detector = ClockDiscontinuityDetector(threshold_seconds=5)
    assert detector.observe(100_000_000_000, 50_000_000_000) is None
    gap = detector.observe(200_000_000_000, 60_000_000_000)
    assert gap is not None
    assert gap.started_at_utc_ns == 110_000_000_000
    assert gap.ended_at_utc_ns == 200_000_000_000
    assert gap.duration_ns == 90_000_000_000
    assert gap.source == "wall_monotonic_discontinuity"


def test_small_clock_drift_does_not_invent_sleep() -> None:
    detector = ClockDiscontinuityDetector(threshold_seconds=5)
    detector.observe(10_000_000_000, 10_000_000_000)
    assert detector.observe(11_100_000_000, 11_000_000_000) is None


def test_scoped_caffeinate_assertion_uses_service_pid_and_cleans_up() -> None:
    process = FakeProcess()
    calls: list[tuple[str, ...]] = []

    def factory(arguments: Sequence[str]) -> FakeProcess:
        calls.append(tuple(arguments))
        return process

    assertion = CaffeinateAssertion(
        enabled=True,
        process_factory=factory,
        service_pid=4321,
    )
    assertion.start()
    assert calls == [("/usr/bin/caffeinate", "-i", "-w", "4321")]
    assert assertion.active is True
    assertion.stop()
    assert process.terminated is True
    assert process.killed is False
    assert assertion.active is False


def test_disabled_power_assertion_never_starts_process() -> None:
    def unexpected(_arguments: Sequence[str]) -> FakeProcess:
        pytest.fail("disabled assertion started a process")

    assertion = CaffeinateAssertion(enabled=False, process_factory=unexpected)
    assertion.start()
    assertion.stop()
    assert assertion.active is False
