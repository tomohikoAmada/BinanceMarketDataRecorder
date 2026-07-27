"""Sleep-gap observation and scoped macOS idle-sleep prevention."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from threading import Event, Thread
from types import ModuleType
from typing import Protocol


@dataclass(frozen=True, slots=True)
class SleepGap:
    started_at_utc_ns: int
    ended_at_utc_ns: int
    duration_ns: int
    source: str

    def public_dict(self) -> dict[str, object]:
        return {
            "started_at_utc_ns": self.started_at_utc_ns,
            "ended_at_utc_ns": self.ended_at_utc_ns,
            "duration_ns": self.duration_ns,
            "source": self.source,
        }


class ClockDiscontinuityDetector:
    """Compare wall and monotonic deltas to mark suspend/clock discontinuities."""

    def __init__(self, *, threshold_seconds: float = 30.0) -> None:
        if threshold_seconds <= 0:
            raise ValueError("sleep gap threshold must be positive")
        self._threshold_ns = int(threshold_seconds * 1_000_000_000)
        self._previous_wall_ns: int | None = None
        self._previous_monotonic_ns: int | None = None

    def observe(self, wall_ns: int, monotonic_ns: int) -> SleepGap | None:
        if wall_ns < 0 or monotonic_ns < 0:
            raise ValueError("clock samples must be non-negative")
        previous_wall = self._previous_wall_ns
        previous_monotonic = self._previous_monotonic_ns
        self._previous_wall_ns = wall_ns
        self._previous_monotonic_ns = monotonic_ns
        if previous_wall is None or previous_monotonic is None:
            return None
        wall_delta = wall_ns - previous_wall
        monotonic_delta = monotonic_ns - previous_monotonic
        discontinuity = wall_delta - monotonic_delta
        if discontinuity < self._threshold_ns:
            return None
        started = previous_wall + max(monotonic_delta, 0)
        return SleepGap(
            started_at_utc_ns=started,
            ended_at_utc_ns=wall_ns,
            duration_ns=discontinuity,
            source="wall_monotonic_discontinuity",
        )


class ProcessHandle(Protocol):
    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


ProcessFactory = Callable[[Sequence[str]], ProcessHandle]


def _start_process(arguments: Sequence[str]) -> ProcessHandle:
    return subprocess.Popen(
        list(arguments),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=True,
    )


class CaffeinateAssertion:
    """Own a scoped `caffeinate -i -w <service-pid>` child process."""

    def __init__(
        self,
        *,
        enabled: bool,
        process_factory: ProcessFactory = _start_process,
        service_pid: int | None = None,
    ) -> None:
        self.enabled = enabled
        self._process_factory = process_factory
        self._service_pid = service_pid or os.getpid()
        self._process: ProcessHandle | None = None

    @property
    def active(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def start(self) -> None:
        if not self.enabled or self._process is not None:
            return
        if sys.platform != "darwin":
            raise RuntimeError("prevent-sleep mode is supported only on macOS")
        process = self._process_factory(
            ("/usr/bin/caffeinate", "-i", "-w", str(self._service_pid))
        )
        if process.poll() is not None:
            raise RuntimeError("caffeinate exited before establishing an assertion")
        self._process = process

    def stop(self) -> None:
        process = self._process
        self._process = None
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5.0)


SleepCallback = Callable[[str, int], None]


class NoopSleepObserver:
    """Linux observer; clock discontinuities remain handled by the heartbeat."""

    def __init__(self, _callback: SleepCallback) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


def _load_workspace_frameworks() -> tuple[ModuleType, ModuleType]:
    if sys.platform != "darwin":
        raise RuntimeError("NSWorkspace sleep observation requires macOS")
    try:
        return importlib.import_module("AppKit"), importlib.import_module("Foundation")
    except ImportError as exc:
        raise RuntimeError("PyObjC Cocoa bindings are required on macOS") from exc


class MacSleepObserver:
    """Deliver NSWorkspace sleep/wake notifications from a private run-loop thread."""

    def __init__(self, callback: SleepCallback, *, run_loop_seconds: float = 0.25) -> None:
        if run_loop_seconds <= 0:
            raise ValueError("run-loop slice must be positive")
        self._callback = callback
        self._run_loop_seconds = run_loop_seconds
        self._stop = Event()
        self._ready = Event()
        self._thread: Thread | None = None
        self._failure: BaseException | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("sleep observer already started")
        _load_workspace_frameworks()
        self._stop.clear()
        self._ready.clear()
        self._failure = None
        self._thread = Thread(
            target=self._run,
            name="bmdr-macos-sleep-observer",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(timeout=5.0):
            self.stop()
            raise RuntimeError("sleep observer registration timed out")
        if self._failure is not None:
            failure = self._failure
            self.stop()
            raise RuntimeError(
                f"sleep observer registration failed: {type(failure).__name__}"
            ) from failure

    def stop(self) -> None:
        thread = self._thread
        self._thread = None
        if thread is None:
            return
        self._stop.set()
        thread.join(timeout=max(2.0, self._run_loop_seconds * 4))
        if thread.is_alive():
            raise RuntimeError("sleep observer did not stop")

    def _run(self) -> None:
        try:
            appkit, foundation = _load_workspace_frameworks()
            callback = self._callback

            class Receiver(appkit.NSObject):  # type: ignore[misc, name-defined]
                def workspaceWillSleep_(self, _notification: object) -> None:
                    callback("will_sleep", time.time_ns())

                def workspaceDidWake_(self, _notification: object) -> None:
                    callback("did_wake", time.time_ns())

            receiver = Receiver.alloc().init()
            center = appkit.NSWorkspace.sharedWorkspace().notificationCenter()
            center.addObserver_selector_name_object_(
                receiver,
                "workspaceWillSleep:",
                appkit.NSWorkspaceWillSleepNotification,
                None,
            )
            center.addObserver_selector_name_object_(
                receiver,
                "workspaceDidWake:",
                appkit.NSWorkspaceDidWakeNotification,
                None,
            )
            run_loop = foundation.NSRunLoop.currentRunLoop()
            self._ready.set()
            try:
                while not self._stop.is_set():
                    deadline = foundation.NSDate.dateWithTimeIntervalSinceNow_(
                        self._run_loop_seconds
                    )
                    run_loop.runUntilDate_(deadline)
            finally:
                center.removeObserver_(receiver)
        except BaseException as exc:
            self._failure = exc
            self._ready.set()
