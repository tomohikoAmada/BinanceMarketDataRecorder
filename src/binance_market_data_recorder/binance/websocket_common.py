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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

from websockets.exceptions import ConnectionClosed

DEFAULT_RECEIPT_BYTE_CAPACITY = 64 * 1024 * 1024


@dataclass
class ConnectionReceiveControl:
    waiting_for_frame: bool = False
    retirement: str | None = None

    async def receive[T](self, receive: Callable[[], Awaitable[T]]) -> T:
        self.waiting_for_frame = True
        try:
            return await receive()
        finally:
            self.waiting_for_frame = False


async def run_connection_receiver(
    receiver: Callable[[ConnectionReceiveControl], Awaitable[str]],
    *,
    stop: asyncio.Event,
    writer_task: asyncio.Task[None],
    rotation_seconds: float,
    shutdown: asyncio.Event | None = None,
) -> str:
    """One receive task and control wait per connection, never per message.

    Retire an idle recv by cancellation. An already received frame instead
    finishes its bounded handoff, including the existing post-close stop path.
    The writer remains owned by its caller and storage failures take priority.
    """

    control = ConnectionReceiveControl()

    async def run_receiver() -> str:
        return await receiver(control)

    receive_task = asyncio.create_task(run_receiver())
    stop_task = asyncio.create_task(stop.wait())
    shutdown_task = None if shutdown is None else asyncio.create_task(shutdown.wait())
    owned: list[asyncio.Task[object]] = [receive_task, stop_task]
    if shutdown_task is not None:
        owned.append(shutdown_task)
    try:
        done, _ = await asyncio.wait(
            {*owned, writer_task}, timeout=rotation_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if writer_task in done:
            await writer_task
            raise RuntimeError("Raw writer stopped unexpectedly")
        if receive_task in done:
            return await receive_task
        control.retirement = (
            "stopped" if stop.is_set() else
            "server_shutdown" if shutdown is not None and shutdown.is_set() else
            "planned_rotation"
        )
        if control.waiting_for_frame:
            receive_task.cancel()
            await asyncio.gather(receive_task, return_exceptions=True)
            return control.retirement
        return await receive_task
    finally:
        for task in owned:
            if not task.done():
                task.cancel()
        await asyncio.gather(*owned, return_exceptions=True)

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


def capped_backoff_seconds(initial: float, maximum: float, failures: int) -> float:
    """Double only until the cap, even for an arbitrarily large failure count."""

    delay = initial
    remaining = failures - 1
    while remaining > 0 and delay < maximum:
        if delay >= maximum / 2:
            return maximum
        delay *= 2
        remaining -= 1
    return delay


def websocket_disconnect_log_fields(
    error: Exception,
    *,
    connected_at: float | None,
    observed_at: float,
) -> dict[str, object]:
    """Describe the close without logging exception messages or transport URLs.

    Missing close frames stay unknown. Duration uses the connection loop's
    monotonic clock at logging time, including any context-manager cleanup;
    it isn't an exact failure timestamp or an outage duration.
    """

    close = error if isinstance(error, ConnectionClosed) else None
    received = close.rcvd if close is not None else None
    sent = close.sent if close is not None else None
    cause = error.__cause__
    os_error = error if isinstance(error, OSError) else cause

    def reason_for_log(reason: str) -> str:
        # Protocol reasons are useful, but must not expose an echoed proxy URL.
        return "[redacted URL]" if "://" in reason else reason[:128]

    return {
        "connection_phase": "opening" if connected_at is None else "established",
        "connected_duration_seconds_at_log": (
            None if connected_at is None else observed_at - connected_at
        ),
        "close_received_code": None if received is None else int(received.code),
        "close_received_reason": (
            None if received is None else reason_for_log(received.reason)
        ),
        "close_sent_code": None if sent is None else int(sent.code),
        "close_sent_reason": None if sent is None else reason_for_log(sent.reason),
        "close_received_then_sent": None if close is None else close.rcvd_then_sent,
        "cause_type": None if cause is None else type(cause).__name__,
        "os_errno": os_error.errno if isinstance(os_error, OSError) else None,
    }


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


async def release_writer_preserving_errors(
    writer_task: asyncio.Task[None],
    intent_error: BaseException,
) -> None:
    """Release the boundary writer and preserve BOTH failure facts (REQ-109).

    Called when the reconnect-intent write failed: the writer is still owned
    by its task, which must drain and seal (or abort) the old generation
    before the intent failure propagates. If the writer also fails, the two
    causal facts must not collapse: the intent error remains the raised
    exception (its original message and type are unchanged) while the writer
    failure is retained as its cause/context. On cancellation the integrity
    failure outranks coincident cancellation, mirroring
    ``run_owned_blocking_call``.
    """

    writer_error: BaseException | None = None
    if isinstance(intent_error, asyncio.CancelledError):
        writer_task.cancel()
        (result,) = await asyncio.gather(writer_task, return_exceptions=True)
        if isinstance(result, BaseException) and not isinstance(
            result, asyncio.CancelledError
        ):
            writer_error = result
    else:
        try:
            await writer_task
        except asyncio.CancelledError as cancellation:
            writer_error = cancellation
        except BaseException as error:
            writer_error = error
    if writer_error is None:
        raise intent_error
    if isinstance(intent_error, asyncio.CancelledError):
        raise writer_error from intent_error
    raise intent_error from writer_error
