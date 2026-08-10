"""Shared WebSocket transport primitives for Spot and USD-M collectors.

``run_owned_blocking_call`` implements the M21.4.2 owned-worker semantics: a
cancellation of an asyncio Task awaiting ``asyncio.to_thread`` does not stop a
blocking Raw or SQLite operation already running in its worker thread. Every
mutating blocking call on the storage path must therefore have one explicit
asyncio owner that creates the worker Task and awaits it through
``asyncio.shield``. If the caller is cancelled, including repeatedly, the owner
retains ownership and waits for the worker to finish before retrieving the
worker result and allowing abort, close, or cancellation propagation. If the
worker succeeds, the original cancellation is propagated; if storage I/O fails
at the same time, the original storage exception takes priority and chains the
cancellation so the integrity failure remains fail-closed.

``ReconnectReason`` is the unified boundary vocabulary (M21.4.11): every
transport boundary that closes one connection and opens another must carry
persistent gap evidence, regardless of whether the close was intentional.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from enum import StrEnum

#: Reconnect boundary reasons that require persistent gap evidence.
RECONNECT_REASONS = frozenset(
    {
        "ingress_backpressure",
        "unexpected_disconnect",
        "planned_rotation",
        "server_shutdown",
        "session_restart",
    }
)


class ReconnectReason(StrEnum):
    """Unified transport-boundary reasons recorded in Catalog gap evidence."""

    INGRESS_BACKPRESSURE = "ingress_backpressure"
    UNEXPECTED_DISCONNECT = "unexpected_disconnect"
    PLANNED_ROTATION = "planned_rotation"
    SERVER_SHUTDOWN = "server_shutdown"
    SESSION_RESTART = "session_restart"


async def run_owned_blocking_call[**BlockingParams, BlockingResult](
    function: Callable[BlockingParams, BlockingResult],
    /,
    *args: BlockingParams.args,
    **kwargs: BlockingParams.kwargs,
) -> BlockingResult:
    """Own a non-cancellable worker until its mutation and outcome are complete."""

    worker_task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(worker_task)
    except asyncio.CancelledError as cancellation:
        # Cancelling the asyncio waiter cannot stop an in-flight thread. Keep
        # ownership until it exits and retrieve the outcome before cleanup may
        # touch the same Raw writer or Catalog connection.
        while not worker_task.done():
            try:
                await asyncio.shield(worker_task)
            except asyncio.CancelledError:
                # Repeated cancellation must not cancel the Task which owns the
                # still-running thread future.
                continue
            except BaseException:
                # Retrieve and classify the worker failure below.
                break
        try:
            worker_task.result()
        except BaseException as worker_error:
            # Integrity failures outrank coincident cancellation. Chaining
            # retains both facts while keeping storage failure fail-closed.
            raise worker_error from cancellation
        raise
