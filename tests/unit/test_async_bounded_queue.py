from __future__ import annotations

import asyncio

import pytest

from binance_market_data_recorder.spool.async_queue import BoundedAsyncQueue
from binance_market_data_recorder.spool.queue import (
    IngressBackpressureTimeout,
    IngressPostCloseHandoffTimeout,
    IngressStopRequested,
    IngressWriterStopped,
)


def test_short_burst_waits_without_loss_or_reordering() -> None:
    async def exercise() -> None:
        queue = BoundedAsyncQueue[int](
            2,
            put_timeout_seconds=0.2,
            saturation_timeout_seconds=0.5,
        )
        consumed: list[int] = []
        release = asyncio.Event()

        async def writer() -> None:
            await release.wait()
            for _ in range(8):
                consumed.append(await queue.get())
                queue.task_done()
                queue.note_consumer_progress()
                await asyncio.sleep(0)

        writer_task = asyncio.create_task(writer())

        async def produce() -> None:
            for value in range(8):
                await queue.put(value, writer_task=writer_task)

        produce_task = asyncio.create_task(produce())
        await asyncio.sleep(0)
        assert queue.depth == 2
        release.set()
        await produce_task
        await writer_task
        assert consumed == list(range(8))
        stats = queue.snapshot()
        assert stats.high_watermark == stats.capacity == 2
        assert stats.wait_count > 0
        assert stats.depth == 0

    asyncio.run(exercise())


def test_sustained_saturation_has_stable_timeout_and_boundary_put() -> None:
    async def exercise() -> None:
        queue = BoundedAsyncQueue[int](
            1,
            put_timeout_seconds=0.01,
            saturation_timeout_seconds=0.02,
        )
        release = asyncio.Event()

        async def writer() -> None:
            await release.wait()
            await queue.get()
            queue.task_done()
            await asyncio.sleep(0.02)
            await queue.get()
            queue.task_done()

        writer_task = asyncio.create_task(writer())
        await queue.put(1, writer_task=writer_task)
        with pytest.raises(IngressBackpressureTimeout):
            await queue.put(2, writer_task=writer_task)
        assert queue.depth == 1
        assert queue.snapshot().wait_count == 1
        release.set()
        waited = await queue.put_after_connection_close(
            2,
            writer_task=writer_task,
            timeout_seconds=0.2,
        )
        assert waited >= 0
        await writer_task
        assert queue.depth == 0

    asyncio.run(exercise())


def test_post_close_boundary_timeout_is_explicit() -> None:
    async def exercise() -> None:
        queue = BoundedAsyncQueue[int](
            1,
            put_timeout_seconds=0.01,
            saturation_timeout_seconds=0.02,
        )

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer_task = asyncio.create_task(idle_writer())
        await queue.put(1, writer_task=writer_task)
        with pytest.raises(IngressPostCloseHandoffTimeout):
            await queue.put_after_connection_close(
                2,
                writer_task=writer_task,
                timeout_seconds=0.01,
            )
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)

    asyncio.run(exercise())


def test_writer_exception_is_observed_by_waiting_producer() -> None:
    async def exercise() -> None:
        queue = BoundedAsyncQueue[int](
            1,
            put_timeout_seconds=0.5,
            saturation_timeout_seconds=1.0,
        )
        fail = asyncio.Event()

        async def writer() -> None:
            await fail.wait()
            raise OSError("injected fsync failure")

        writer_task = asyncio.create_task(writer())
        await queue.put(1, writer_task=writer_task)
        waiting = asyncio.create_task(queue.put(2, writer_task=writer_task))
        fail.set()
        with pytest.raises(IngressWriterStopped) as captured:
            await waiting
        assert isinstance(captured.value.__cause__, OSError)
        with pytest.raises(OSError, match="fsync"):
            await writer_task

    asyncio.run(exercise())


def test_cancelled_put_leaves_no_helper_task() -> None:
    async def exercise() -> None:
        baseline = set(asyncio.all_tasks())
        queue = BoundedAsyncQueue[int](
            1,
            put_timeout_seconds=10,
            saturation_timeout_seconds=10,
        )

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer_task = asyncio.create_task(idle_writer())
        await queue.put(1, writer_task=writer_task)
        waiting = asyncio.create_task(queue.put(2, writer_task=writer_task))
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)
        await asyncio.sleep(0)
        assert [
            task
            for task in asyncio.all_tasks()
            if task not in baseline and task is not asyncio.current_task() and not task.done()
        ] == []

    asyncio.run(exercise())


def test_stop_interrupts_wait_without_enqueuing_or_leaking_helper() -> None:
    async def exercise() -> None:
        baseline = set(asyncio.all_tasks())
        queue = BoundedAsyncQueue[int](
            1,
            put_timeout_seconds=10,
            saturation_timeout_seconds=10,
        )
        stop = asyncio.Event()

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer_task = asyncio.create_task(idle_writer())
        await queue.put(1, writer_task=writer_task)
        waiting = asyncio.create_task(queue.put(2, writer_task=writer_task, stop=stop))
        await asyncio.sleep(0)
        stop.set()
        with pytest.raises(IngressStopRequested):
            await asyncio.wait_for(waiting, timeout=0.1)
        assert queue.depth == 1
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)
        await asyncio.sleep(0)
        assert [
            task
            for task in asyncio.all_tasks()
            if task not in baseline and task is not asyncio.current_task() and not task.done()
        ] == []

    asyncio.run(exercise())


def test_cancelled_post_close_put_leaves_no_helper_task_or_duplicate() -> None:
    async def exercise() -> None:
        baseline = set(asyncio.all_tasks())
        queue = BoundedAsyncQueue[int](
            1,
            put_timeout_seconds=10,
            saturation_timeout_seconds=10,
        )

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer_task = asyncio.create_task(idle_writer())
        await queue.put(1, writer_task=writer_task)
        waiting = asyncio.create_task(
            queue.put_after_connection_close(
                2,
                writer_task=writer_task,
                timeout_seconds=10,
            )
        )
        await asyncio.sleep(0)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert queue.depth == 1
        writer_task.cancel()
        await asyncio.gather(writer_task, return_exceptions=True)
        await asyncio.sleep(0)
        assert [
            task
            for task in asyncio.all_tasks()
            if task not in baseline and task is not asyncio.current_task() and not task.done()
        ] == []

    asyncio.run(exercise())


def test_owned_bytes_stay_charged_while_writer_holds_dequeued_batch() -> None:
    async def exercise() -> None:
        queue = BoundedAsyncQueue[bytes](
            20,
            byte_capacity=8,
            size_of=len,
            put_timeout_seconds=0.2,
            saturation_timeout_seconds=1,
        )

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer = asyncio.create_task(idle_writer())
        try:
            first, second, third = b"first", b"sec", b"next"
            await queue.put(first, writer_task=writer)
            await queue.put(second, writer_task=writer)
            assert queue.get_nowait() is first
            assert queue.get_nowait() is second
            stats = queue.snapshot()
            assert stats.depth == stats.queued_bytes == 0
            assert stats.owned_bytes == stats.high_watermark_bytes == 8
            waiting = asyncio.create_task(queue.put(third, writer_task=writer))
            await asyncio.sleep(0)
            assert not waiting.done()
            # An out-of-order completion releases only this item's three bytes.
            queue.task_done(second)
            await asyncio.sleep(0)
            assert not waiting.done()
            assert queue.snapshot().owned_bytes == 5
            queue.task_done(first)
            await asyncio.wait_for(waiting, timeout=0.2)
            assert queue.get_nowait() is third
            queue.task_done(third)
            assert queue.snapshot().owned_bytes == queue.snapshot().queued_bytes == 0
            assert queue.snapshot().high_watermark_bytes == 8
            with pytest.raises(ValueError, match="in-flight"):
                queue.task_done(third)
        finally:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)

    asyncio.run(exercise())


@pytest.mark.parametrize("interruption", ["cancel", "stop", "writer_failure", "timeout"])
def test_weighted_wait_interruptions_never_release_inflight_or_charge_unadmitted(
    interruption: str,
) -> None:
    async def exercise() -> None:
        baseline = set(asyncio.all_tasks())
        queue = BoundedAsyncQueue[bytes](
            20,
            byte_capacity=4,
            size_of=len,
            put_timeout_seconds=0.01,
            saturation_timeout_seconds=0.03,
        )
        fail, stop = asyncio.Event(), asyncio.Event()

        async def writer_run() -> None:
            await fail.wait()
            raise OSError("fsync failed")

        writer = asyncio.create_task(writer_run())
        first = b"full"
        await queue.put(first, writer_task=writer)
        assert await queue.get() is first
        waiting = asyncio.create_task(queue.put(b"new", writer_task=writer, stop=stop))
        await asyncio.sleep(0)
        expected: type[BaseException]
        if interruption == "cancel":
            waiting.cancel()
            expected = asyncio.CancelledError
        elif interruption == "stop":
            stop.set()
            expected = IngressStopRequested
        elif interruption == "writer_failure":
            fail.set()
            expected = IngressWriterStopped
        else:
            expected = IngressBackpressureTimeout
        with pytest.raises(expected):
            await waiting
        assert queue.snapshot().owned_bytes == 4
        assert queue.depth == queue.snapshot().queued_bytes == 0
        if not writer.done():
            writer.cancel()
        await asyncio.gather(writer, return_exceptions=True)
        await asyncio.sleep(0)
        assert not [
            task for task in asyncio.all_tasks() if task not in baseline and not task.done()
        ]

    asyncio.run(exercise())


def test_weighted_boundary_waits_for_batch_release_and_rejects_oversize() -> None:
    async def exercise() -> None:
        queue = BoundedAsyncQueue[bytes](
            20,
            byte_capacity=4,
            size_of=len,
            put_timeout_seconds=0.01,
            saturation_timeout_seconds=0.03,
        )

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer = asyncio.create_task(idle_writer())
        try:
            first = b"full"
            await queue.put(first, writer_task=writer)
            assert await queue.get() is first
            with pytest.raises(IngressPostCloseHandoffTimeout):
                await queue.put_after_connection_close(
                    b"tail", writer_task=writer, timeout_seconds=0.01
                )
            boundary = asyncio.create_task(
                queue.put_after_connection_close(
                    b"tail",
                    writer_task=writer,
                    timeout_seconds=0.2,
                )
            )
            await asyncio.sleep(0)
            queue.task_done(first)
            await boundary
            tail = queue.get_nowait()
            assert tail == b"tail"
            queue.task_done(tail)
            with pytest.raises(IngressBackpressureTimeout, match="exceeds"):
                await queue.put(b"large", writer_task=writer)
            with pytest.raises(IngressPostCloseHandoffTimeout, match="exceeds"):
                await queue.put_after_connection_close(
                    b"large", writer_task=writer, timeout_seconds=0.1
                )
            assert queue.snapshot().owned_bytes == 0
        finally:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)

    asyncio.run(exercise())


def test_repeated_same_object_keeps_each_admission_charge() -> None:
    async def exercise() -> None:
        queue = BoundedAsyncQueue[bytes](
            2,
            byte_capacity=8,
            size_of=len,
            put_timeout_seconds=0.1,
            saturation_timeout_seconds=0.2,
        )

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer = asyncio.create_task(idle_writer())
        item = b"same"
        try:
            for _ in range(2):
                await queue.put(item, writer_task=writer)
            for expected_bytes in (4, 0):
                assert queue.get_nowait() is item
                queue.task_done(item)
                assert queue.snapshot().owned_bytes == expected_bytes
        finally:
            writer.cancel()
            await asyncio.gather(writer, return_exceptions=True)

    asyncio.run(exercise())
