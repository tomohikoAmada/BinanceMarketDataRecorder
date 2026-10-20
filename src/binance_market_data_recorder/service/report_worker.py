"""One owned report child; slow reports cannot hold capture locks or delay stop."""

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from contextlib import suppress
from pathlib import Path

from ..logging import log_event


class DailyReportWorker:
    def __init__(
        self, data_root: Path, logger: logging.Logger, *,
        interval_seconds: float = 60, report_timeout_seconds: float = 300,
        stop_timeout_seconds: float = 5,
    ) -> None:
        self.arguments: tuple[str, ...] = (
            sys.executable, "-m", "binance_market_data_recorder.metrics.maintenance",
            str(data_root),
        )
        self.logger = logger
        self.interval_seconds = interval_seconds
        self.report_timeout_seconds = report_timeout_seconds
        self.stop_timeout_seconds = stop_timeout_seconds

    async def _stop_child(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        with suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), self.stop_timeout_seconds)
        except TimeoutError:
            with suppress(ProcessLookupError):
                process.kill()
            await asyncio.wait_for(process.wait(), self.stop_timeout_seconds)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            process = None
            waiter = None
            stopper = None
            try:
                process = await asyncio.create_subprocess_exec(
                    *self.arguments, stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL, close_fds=True,
                )
                waiter = asyncio.create_task(process.wait())
                stopper = asyncio.create_task(stop.wait())
                done, _pending = await asyncio.wait(
                    {waiter, stopper}, timeout=self.report_timeout_seconds,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if waiter not in done and not stop.is_set():
                    raise TimeoutError("daily report exceeded its background budget")
                if waiter in done and waiter.result() != 0:
                    raise RuntimeError("daily report process failed; pending inputs retained")
            except (OSError, RuntimeError, TimeoutError) as exc:
                log_event(
                    self.logger, logging.ERROR, "daily_report_failed",
                    "background daily report failed; capture continues and report will retry",
                    error_type=type(exc).__name__,
                )
            finally:
                if process is not None:
                    await self._stop_child(process)
                for task in (waiter, stopper):
                    if task is not None:
                        task.cancel()
                await asyncio.gather(
                    *(task for task in (waiter, stopper) if task is not None),
                    return_exceptions=True,
                )
            if not stop.is_set():
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self.interval_seconds)
