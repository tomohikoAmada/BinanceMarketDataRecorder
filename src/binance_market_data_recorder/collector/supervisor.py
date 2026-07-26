"""Failure-isolated coordination for market Collectors."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from typing import Protocol


class MarketCollector(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...


class AllMarketCollectorsStopped(RuntimeError):
    """No core market Collector remains for launchd to supervise."""


class CoreMarketTerminalFailure(RuntimeError):
    """A core market terminated, so launchd must restart the whole service."""


class MarketCollectorSupervisor:
    """Run core markets and fail the process if either terminates unexpectedly."""

    def __init__(
        self,
        collectors: Mapping[str, MarketCollector],
        terminal_failure_observer: Callable[[str, BaseException], None] | None = None,
    ) -> None:
        if not collectors:
            raise ValueError("at least one market Collector is required")
        self.collectors = dict(collectors)
        self.failures: dict[str, BaseException] = {}
        self.terminal_failure_observer = terminal_failure_observer

    async def run(self, stop: asyncio.Event) -> None:
        child_stops = {name: asyncio.Event() for name in self.collectors}
        tasks = {
            name: asyncio.create_task(collector.run(child_stops[name]))
            for name, collector in self.collectors.items()
        }
        stop_task = asyncio.create_task(stop.wait())
        try:
            while tasks and not stop.is_set():
                done, _pending = await asyncio.wait(
                    {*tasks.values(), stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_task in done:
                    break
                for name, task in list(tasks.items()):
                    if task not in done:
                        continue
                    tasks.pop(name)
                    try:
                        task.result()
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        self.failures[name] = exc
                        if self.terminal_failure_observer is not None:
                            self.terminal_failure_observer(name, exc)
                        for child_stop in child_stops.values():
                            child_stop.set()
                        await asyncio.gather(*tasks.values(), return_exceptions=True)
                        raise CoreMarketTerminalFailure(
                            f"core market Collector terminated: {name}"
                        ) from exc
                    normal_exit = RuntimeError(
                        "core market Collector returned before global stop"
                    )
                    self.failures[name] = normal_exit
                    if self.terminal_failure_observer is not None:
                        self.terminal_failure_observer(name, normal_exit)
                    for child_stop in child_stops.values():
                        child_stop.set()
                    await asyncio.gather(*tasks.values(), return_exceptions=True)
                    raise CoreMarketTerminalFailure(
                        f"core market Collector terminated: {name}"
                    ) from normal_exit
            if not tasks and not stop.is_set():
                failed = ",".join(sorted(self.failures)) or "all"
                raise AllMarketCollectorsStopped(
                    f"all core market Collectors stopped; failed={failed}"
                )
        finally:
            stop_task.cancel()
            for child_stop in child_stops.values():
                child_stop.set()
            await asyncio.gather(*tasks.values(), return_exceptions=True)
