"""P1-R1: retirement cannot be followed by a new idle recv after a scheduling yield."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from binance_market_data_recorder.binance import websocket_common as common
from binance_market_data_recorder.binance.spot.schema import SpotStream
from binance_market_data_recorder.binance.spot.websocket import SpotStreamCollector
from binance_market_data_recorder.binance.usdm.websocket import UsdMStreamCollector
from binance_market_data_recorder.spool.queue import IngressWriterStopped
from binance_market_data_recorder.storage.catalog import Catalog
from tests.integration import test_spot_ingress_backpressure as spot
from tests.integration import test_usdm_ingress_backpressure as usdm


class BufferedThenQuietSocket:
    def __init__(self, payloads: list[bytes]) -> None:
        self.payloads = payloads
        self.calls = 0
        self.close_reasons: list[str] = []

    async def recv(self, decode: bool | None = None) -> bytes:
        self.calls += 1
        if self.calls > len(self.payloads):
            await asyncio.Future[None]()
        return self.payloads[self.calls - 1]

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.close_reasons.append(reason)


def make_collector(
    root: Path,
    market: str,
    capacity: int,
) -> tuple[SpotStreamCollector | UsdMStreamCollector, Catalog]:
    if market == "spot":
        return spot.make_collector(
            root,
            stream=SpotStream.BOOK_TICKER,
            opener=None,
            capacity=capacity,
            drain_delay_seconds=0,
            put_timeout_seconds=1,
            saturation_timeout_seconds=2,
        )
    return usdm.make_collector(
        root,
        opener=None,
        capacity=capacity,
        drain_delay_seconds=0,
        put_timeout_seconds=1,
        saturation_timeout_seconds=2,
    )


async def drain_and_check(
    root: Path,
    collector: SpotStreamCollector | UsdMStreamCollector,
    payloads: list[bytes],
) -> None:
    producer_done = asyncio.Event()
    producer_done.set()
    await collector._write_until_done(producer_done)
    frames, _ = spot.captured(root)
    assert [frame.raw_payload for frame in frames] == payloads
    assert all(frame.connection_id == "review" for frame in frames)
    assert [frame.receive_monotonic_ns for frame in frames] == sorted(
        frame.receive_monotonic_ns for frame in frames
    )
    assert collector.receipt_queue_stats.owned_bytes == 0
    assert collector.receipt_queue_stats.depth == 0
    assert collector.spool.persistence_progress().synchronized_frames == len(payloads)


@pytest.mark.parametrize(
    ("market", "boundary", "buffered"),
    [("spot", "rotation", 2), ("usdm", "rotation", 2), ("spot", "server_shutdown", 3)],
)
def test_retirement_at_yield_exits_without_next_message(
    tmp_path: Path,
    market: str,
    boundary: str,
    buffered: int,
) -> None:
    """The reviewed 5218d9d enters one extra recv and fails this watchdog."""

    async def exercise() -> None:
        collector, catalog = make_collector(tmp_path, market, 32)
        collector.planned_rotation_seconds = 1e-9 if boundary == "rotation" else 60
        if boundary == "server_shutdown":
            assert isinstance(collector, SpotStreamCollector)
            collector._server_shutdown.set()
        payload = spot.book_ticker if market == "spot" else usdm.book_ticker
        socket = BufferedThenQuietSocket([payload(i) for i in range(buffered)])
        stop = asyncio.Event()

        async def idle_writer() -> None:
            await asyncio.Event().wait()

        writer = asyncio.create_task(idle_writer())
        receive = asyncio.create_task(collector._receive_connection(socket, "review", stop, writer))
        try:
            # The watchdog ends a failure; no next frame is ever supplied.
            done, _ = await asyncio.wait({receive}, timeout=1)
            assert receive in done, f"{boundary} stalled at recv {socket.calls}"
            assert await receive == (
                "planned_rotation" if boundary == "rotation" else "server_shutdown"
            )
            assert 0 < socket.calls <= buffered
            assert socket.close_reasons == [
                "planned 24-hour rotation" if boundary == "rotation" else "serverShutdown received"
            ]
            stop.set()
            await receive  # a later stop cannot leave a receiver or its controls waiting
            assert not writer.done()  # the caller still owns the healthy writer
            await drain_and_check(tmp_path, collector, socket.payloads[: socket.calls])
        finally:
            receive.cancel()
            writer.cancel()
            await asyncio.gather(receive, writer, return_exceptions=True)
            catalog.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("market", ["spot", "usdm"])
@pytest.mark.parametrize("action", ["handoff", "stop", "writer_failure"])
def test_retirement_keeps_in_hand_frame_owned_until_handoff_or_explicit_writer_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    market: str,
    action: str,
) -> None:
    controls: list[common.ConnectionReceiveControl] = []
    original_control = common.ConnectionReceiveControl

    def observed_control() -> common.ConnectionReceiveControl:
        control = original_control()
        controls.append(control)
        return control

    monkeypatch.setattr(common, "ConnectionReceiveControl", observed_control)

    async def exercise() -> None:
        collector, catalog = make_collector(tmp_path, market, 1)
        collector.planned_rotation_seconds = 1e-9
        payload = spot.book_ticker if market == "spot" else usdm.book_ticker
        socket = BufferedThenQuietSocket([payload(1)])
        stop = asyncio.Event()
        failure: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        async def writer_owner() -> None:
            await failure

        writer = asyncio.create_task(writer_owner())
        # Occupy the one receipt slot. The actual next received frame must wait
        # in _accept; retiring that task must not cancel/lose its in-hand payload.
        first = await collector._receive_once(BufferedThenQuietSocket([payload(0)]), "review")
        await collector._receipts.put(first, writer_task=writer)
        receive = asyncio.create_task(collector._receive_connection(socket, "review", stop, writer))
        try:
            async with asyncio.timeout(2):
                while not controls or controls[0].retirement is None:
                    await asyncio.sleep(0)
            assert controls[0].retirement == "planned_rotation"
            assert not controls[0].waiting_for_frame
            assert collector._receipts._saturation_started_ns is not None
            assert not receive.done()
            assert socket.calls == 1
            if action == "writer_failure":
                failure.set_exception(OSError("injected retired writer failure"))
                with pytest.raises(IngressWriterStopped) as raised:
                    await asyncio.wait_for(receive, timeout=2)
                assert isinstance(raised.value.__cause__, OSError)
                assert writer.done() and isinstance(writer.exception(), OSError)
                assert collector._receipts.get_nowait() == first
                collector._receipts.task_done(first)
            else:
                if action == "stop":
                    stop.set()
                    async with asyncio.timeout(2):
                        while not socket.close_reasons:
                            await asyncio.sleep(0)
                assert collector._receipts.get_nowait() == first
                collector._receipts.task_done(first)
                assert await asyncio.wait_for(receive, timeout=2) == (
                    "graceful_shutdown" if action == "stop" else "planned_rotation"
                )
                # Retain the prefix and newly admitted receipt in original order.
                second = collector._receipts.get_nowait()
                collector._receipts.task_done(second)
                assert second.raw_payload == payload(1)
                await collector._receipts.put(first, writer_task=writer)
                # Capacity is one, so drain the prefix before returning the second.
                producer_done = asyncio.Event()
                producer_done.set()
                await collector._write_until_done(producer_done)
                await collector._receipts.put(second, writer_task=writer)
                await collector._write_until_done(producer_done)
                frames, _ = spot.captured(tmp_path)
                assert [frame.raw_payload for frame in frames] == [payload(0), payload(1)]
                assert all(frame.connection_id == "review" for frame in frames)
                assert collector.receipt_queue_stats.owned_bytes == 0
                assert collector.spool.persistence_progress().synchronized_frames == 2
                assert not writer.done()
            assert socket.calls == 1
        finally:
            receive.cancel()
            writer.cancel()
            await asyncio.gather(receive, writer, return_exceptions=True)
            catalog.close()

    asyncio.run(exercise())
