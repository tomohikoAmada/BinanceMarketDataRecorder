from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.binance.spot.websocket import ReconnectBackoff
from binance_market_data_recorder.binance.usdm.side_data_schema import (
    UsdMSideStream,
    envelope_from_side_stream_frame,
)
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.collector.usdm_side_data import (
    SideDataStats,
    SideDataSupervisor,
    SideWebSocketExtension,
)
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout

FIXTURES = Path(__file__).parents[1] / "fixtures" / "binance" / "usdm"

SIDE_PAYLOADS = {
    "mark_price": (FIXTURES / "mark_price.json").read_bytes().rstrip(),
    "liquidation": (FIXTURES / "liquidation.json").read_bytes().rstrip(),
}


class ScriptedSocket:
    def __init__(
        self,
        messages: list[bytes],
        *,
        error: Exception | None = None,
        block_on_exhaustion: bool = False,
    ) -> None:
        self.messages = iter(messages)
        self.error = error
        self.block_on_exhaustion = block_on_exhaustion

    async def recv(self, decode: bool | None = None) -> bytes:
        try:
            return next(self.messages)
        except StopIteration:
            if self.block_on_exhaustion:
                await asyncio.Future[None]()
            raise (self.error or OSError("injected disconnect")) from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None


class FailOnDemandSpool(StreamSpool):
    """Raise on the next drain_all call once armed (deterministic injection)."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.armed = False

    def drain_all(self) -> int:
        if self.armed:
            raise OSError("injected terminal Raw drain failure")
        return super().drain_all()


def make_side_extension(
    root: Path,
    *,
    stream_name: str,
    wire_name: str,
    spool: StreamSpool,
    opener: Any,
    stats: SideDataStats,
) -> SideWebSocketExtension:
    def envelope_factory(
        *,
        raw_payload: bytes,
        connection_id: str,
        collector_instance_id: str,
        collector_version: str,
        receive_time_utc_ns: int,
        receive_monotonic_ns: int,
    ) -> Any:
        return envelope_from_side_stream_frame(
            raw_payload=raw_payload,
            connection_id=connection_id,
            collector_instance_id=collector_instance_id,
            collector_version=collector_version,
            receive_time_utc_ns=receive_time_utc_ns,
            receive_monotonic_ns=receive_monotonic_ns,
            stream=UsdMSideStream(stream_name),
        )

    collector = UsdMStreamCollector(
        stream=stream_name,
        symbol="BTCUSDT",
        route="market",
        wire_name=wire_name,
        spool=spool,
        collector_instance_id="side-integrity-test",
        collector_version="0.1.0+test",
        logger=logging.getLogger(f"test.side.{stream_name}"),
        receipt_queue_capacity=16,
        planned_rotation_seconds=60,
        backoff=ReconnectBackoff(
            initial_seconds=0.001, maximum_seconds=0.001, jitter_ratio=0
        ),
        opener=opener,
        envelope_factory=envelope_factory,
        envelope_observer=stats.observe_envelope,
        failure_observer=stats.observe_failure,
    )
    return SideWebSocketExtension(collector)


@pytest.mark.parametrize(
    ("stream_name", "wire_name", "payload"),
    [
        ("mark_price", "btcusdt@markPrice@1s", SIDE_PAYLOADS["mark_price"]),
        ("liquidation", "btcusdt@forceOrder", SIDE_PAYLOADS["liquidation"]),
    ],
)
def test_side_transport_terminal_storage_failure_fails_closed_without_replacement(
    tmp_path: Path,
    stream_name: str,
    wire_name: str,
    payload: bytes,
) -> None:
    """TEST-401/402/403: a terminal writer/storage failure must not silently
    open a replacement WebSocket; the side task enters FAILED while the
    shared core stop stays unset and no replacement opener call occurs."""

    async def exercise() -> dict[str, object]:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        stop = asyncio.Event()
        opener_calls = 0
        spool = FailOnDemandSpool(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream=stream_name,
            collector_instance_id="side-integrity-test",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        stats = SideDataStats(True)

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal opener_calls
            opener_calls += 1
            yield ScriptedSocket([payload], block_on_exhaustion=True)

        extension = make_side_extension(
            tmp_path,
            stream_name=stream_name,
            wire_name=wire_name,
            spool=spool,
            opener=opener,
            stats=stats,
        )

        def arm_after_first_persist(envelope: Any) -> None:
            stats.observe_envelope(envelope)
            if stats.accepted >= 1:
                spool.armed = True

        extension.collector.envelope_observer = arm_after_first_persist
        supervisor = SideDataSupervisor(
            {stream_name: lambda: extension},
            {stream_name: stats},
            logging.getLogger(f"test.side-supervisor.{stream_name}"),
            retry_initial_seconds=0.001,
            retry_maximum_seconds=0.001,
        )
        task = asyncio.create_task(supervisor.run(stop))
        try:
            for _ in range(300):
                if stats.status == "FAILED":
                    break
                await asyncio.sleep(0.01)
            assert stats.status == "FAILED"
            assert stats.accepted == 1
            assert not stop.is_set()
            assert opener_calls == 1
            return stats.public_dict(degraded_after_seconds=900.0)
        finally:
            stop.set()
            await asyncio.gather(task, return_exceptions=True)
            catalog.close()

    status = asyncio.run(exercise())
    assert status["status"] == "FAILED"
    assert int(cast(Any, status["consecutive_failures"])) >= 1


def test_side_transport_network_disconnect_recovers_inside_collector(
    tmp_path: Path,
) -> None:
    """TEST-405: a normal network disconnect is handled by the collector's own
    Reconnect Boundary logic; the supervisor task neither fails nor restarts."""

    async def exercise() -> SideDataStats:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        stop = asyncio.Event()
        attempts = 0

        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[WebSocketConnection]:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                yield ScriptedSocket(
                    [SIDE_PAYLOADS["mark_price"]],
                    error=OSError("injected disconnect"),
                )
            yield ScriptedSocket(
                [SIDE_PAYLOADS["mark_price"]], block_on_exhaustion=True
            )

        spool = StreamSpool(
            layout=layout,
            catalog=catalog,
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="mark_price",
            collector_instance_id="side-integrity-test",
            collector_version="0.1.0+test",
            queue_capacity=32,
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
        )
        stats = SideDataStats(True)
        extension = make_side_extension(
            tmp_path,
            stream_name="mark_price",
            wire_name="btcusdt@markPrice@1s",
            spool=spool,
            opener=opener,
            stats=stats,
        )
        supervisor = SideDataSupervisor(
            {"mark_price": lambda: extension},
            {"mark_price": stats},
            logging.getLogger("test.side-supervisor.mark_price"),
            retry_initial_seconds=0.001,
            retry_maximum_seconds=0.001,
        )
        task = asyncio.create_task(supervisor.run(stop))
        try:
            for _ in range(300):
                if attempts >= 2 and stats.accepted >= 2:
                    break
                await asyncio.sleep(0.01)
            assert attempts == 2
            assert stats.accepted == 2
            # The network failure was observed and recovered inside the
            # collector's Reconnect Boundary protocol: the supervisor never
            # recreated the task (one attempt, no terminal FAILED).
            assert stats.attempts == 1
            assert stats.failures == 1
            assert stats.status == "RUNNING"
            stop.set()
            await asyncio.wait_for(task, timeout=3)
            return stats
        finally:
            stop.set()
            await asyncio.gather(task, return_exceptions=True)
            catalog.close()

    stats = asyncio.run(exercise())
    assert stats.status == "STOPPED"
    assert stats.attempts == 1
