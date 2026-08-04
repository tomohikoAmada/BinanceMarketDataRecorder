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
            if task not in baseline
            and task is not asyncio.current_task()
            and not task.done()
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
        waiting = asyncio.create_task(
            queue.put(2, writer_task=writer_task, stop=stop)
        )
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
            if task not in baseline
            and task is not asyncio.current_task()
            and not task.done()
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
            if task not in baseline
            and task is not asyncio.current_task()
            and not task.done()
        ] == []

    asyncio.run(exercise())
