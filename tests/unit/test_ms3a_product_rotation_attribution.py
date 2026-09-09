from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from binance_market_data_recorder.binance.usdm.rest import UsdMSnapshotHttpError
from binance_market_data_recorder.binance.usdm.websocket import open_usdm_websocket
from binance_market_data_recorder.collector.spot import (
    SpotCollector,
    SpotCollectorSettings,
)
from binance_market_data_recorder.collector.supervisor import (
    MarketCollectorSupervisor,
)
from binance_market_data_recorder.collector.usdm import (
    UsdMCollector,
    UsdMCollectorSettings,
)
from binance_market_data_recorder.collector.usdm_side_data import (
    UsdMRestCooldown,
    UsdMSideDataManager,
    UsdMSideDataSettings,
)
from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


class StructuredRecordHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _logger(name: str) -> tuple[logging.Logger, StructuredRecordHandler]:
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    handler = StructuredRecordHandler()
    logger.addHandler(handler)
    return logger, handler


def _fields(record: logging.LogRecord) -> dict[str, Any]:
    fields = getattr(record, "structured_fields", None)
    assert isinstance(fields, dict)
    return fields


def _event(record: logging.LogRecord) -> str:
    event = getattr(record, "structured_event", None)
    assert isinstance(event, str)
    return event


def test_product_supervisor_tasks_have_distinguishable_names() -> None:
    async def exercise() -> None:
        started = {
            key: asyncio.Event()
            for key in (
                ProductKey("spot", "ETHUSDT"),
                ProductKey("um_perpetual", "SOLUSDT"),
            )
        }
        stop = asyncio.Event()

        class Collector:
            def __init__(self, key: ProductKey) -> None:
                self.key = key

            async def run(self, child_stop: asyncio.Event) -> None:
                started[self.key].set()
                await child_stop.wait()

        supervisor = MarketCollectorSupervisor(
            {key: Collector(key) for key in sorted(started)}
        )
        task = asyncio.create_task(supervisor.run(stop), name="GLOBAL:test-supervisor")
        await asyncio.gather(*(event.wait() for event in started.values()))
        names = {item.get_name() for item in asyncio.all_tasks()}
        assert "collector:spot:ETHUSDT" in names
        assert "collector:um_perpetual:SOLUSDT" in names
        stop.set()
        await task

    asyncio.run(exercise())


def test_global_side_data_task_namespace_is_explicitly_non_product(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        manager = UsdMSideDataManager(
            settings=UsdMSideDataSettings(),
            symbol="BTCUSDT",
            scope="global",
            layout=layout,
            catalog=catalog,
            collector_instance_id="global-test",
            collector_version="test",
            logger=logging.getLogger("test.ms3a.global"),
            queue_capacity=4,
            receipt_queue_capacity=4,
            rotation=RotationPolicy(),
            durability_interval_seconds=0,
            max_frame_bytes=1024 * 1024,
            planned_rotation_seconds=60,
            rest_timeout_ms=1000,
            rest_api=None,
            websocket_opener=open_usdm_websocket,
        )

        assert manager.task_name_prefix == "GLOBAL:side-data:um_perpetual"
        assert "BTCUSDT" not in manager.task_name_prefix
        assert manager.supervisor.log_context == {
            "scope": "global",
            "market": "um_perpetual",
            "owner": "usdm_side_data",
        }


def test_spot_and_usdm_backpressure_logs_identify_product(
    tmp_path: Path,
) -> None:
    logger, handler = _logger("test.ms3a.ingress")
    spot = SpotCollector(
        SpotCollectorSettings(
            data_root=tmp_path / "spot-eth",
            collector_instance_id="spot-eth",
            collector_version="test",
            symbol="ETHUSDT",
        ),
        logger=logger,
    )
    usdm = UsdMCollector(
        UsdMCollectorSettings(
            data_root=tmp_path / "um-sol",
            collector_instance_id="um-sol",
            collector_version="test",
            symbol="SOLUSDT",
            side_data=None,
        ),
        logger=logger,
        request_lock=asyncio.Lock(),
        cooldown=UsdMRestCooldown(),
    )
    try:
        spot.streams[0]._log_ingress_state(
            logging.WARNING,
            "spot_ingress_backpressure_started",
            "test",
            connection_id="spot-connection",
            outcome="WAITING",
        )
        usdm.streams[0]._log_ingress_state(
            logging.WARNING,
            "usdm_ingress_backpressure_started",
            "test",
            connection_id="um-connection",
            outcome="WAITING",
        )
        by_event = {_event(record): _fields(record) for record in handler.records}
        spot_fields = by_event["spot_ingress_backpressure_started"]
        usdm_fields = by_event["usdm_ingress_backpressure_started"]
        assert spot_fields["scope"] == "product"
        assert spot_fields["market"] == "spot"
        assert spot_fields["symbol"] == "ETHUSDT"
        assert spot_fields["stream"] == "diff_depth"
        assert usdm_fields["scope"] == "product"
        assert usdm_fields["market"] == "um_perpetual"
        assert usdm_fields["symbol"] == "SOLUSDT"
        assert usdm_fields["stream"] == "diff_depth"
        assert spot_fields["symbol"] != usdm_fields["symbol"]
    finally:
        spot.catalog.close()
        usdm.catalog.close()


def test_spot_and_usdm_lifecycle_logs_identify_product(tmp_path: Path) -> None:
    logger, handler = _logger("test.ms3a.lifecycle")
    spot = SpotCollector(
        SpotCollectorSettings(
            data_root=tmp_path / "spot-eth",
            collector_instance_id="spot-eth",
            collector_version="test",
            symbol="ETHUSDT",
        ),
        logger=logger,
    )
    usdm = UsdMCollector(
        UsdMCollectorSettings(
            data_root=tmp_path / "um-sol",
            collector_instance_id="um-sol",
            collector_version="test",
            symbol="SOLUSDT",
            side_data=None,
        ),
        logger=logger,
        request_lock=asyncio.Lock(),
        cooldown=UsdMRestCooldown(),
    )

    async def exercise() -> None:
        @asynccontextmanager
        async def opener(_url: str) -> AsyncIterator[Any]:
            yield object()

        async def planned_rotation(*_args: Any, **_kwargs: Any) -> str:
            return "planned_rotation"

        async def run_once(stream: Any) -> None:
            stream.opener = opener
            stream._receive_connection = planned_rotation
            writer_task = asyncio.create_task(asyncio.Event().wait())
            try:
                assert (
                    await stream._connection_loop(asyncio.Event(), writer_task)
                    == "planned_rotation"
                )
            finally:
                writer_task.cancel()
                await asyncio.gather(writer_task, return_exceptions=True)

        await run_once(spot.streams[0])
        await run_once(usdm.streams[0])

    try:
        asyncio.run(exercise())
        by_event = {_event(record): _fields(record) for record in handler.records}
        for event, market, symbol, stream in (
            ("spot_websocket_connected", "spot", "ETHUSDT", "diff_depth"),
            ("spot_planned_rotation", "spot", "ETHUSDT", "diff_depth"),
            ("usdm_websocket_connected", "um_perpetual", "SOLUSDT", "diff_depth"),
            ("usdm_planned_rotation", "um_perpetual", "SOLUSDT", "diff_depth"),
        ):
            fields = by_event[event]
            assert fields["scope"] == "product"
            assert fields["market"] == market
            assert fields["symbol"] == symbol
            assert fields["stream"] == stream
    finally:
        spot.catalog.close()
        usdm.catalog.close()


def test_snapshot_rate_limit_logs_identify_product(tmp_path: Path) -> None:
    logger, handler = _logger("test.ms3a.snapshot")
    collector = UsdMCollector(
        UsdMCollectorSettings(
            data_root=tmp_path,
            collector_instance_id="um-eth",
            collector_version="test",
            symbol="ETHUSDT",
            side_data=None,
        ),
        logger=logger,
        request_lock=asyncio.Lock(),
        cooldown=UsdMRestCooldown(),
    )
    try:
        assert collector._observe_public_rest_rate_limit(
            UsdMSnapshotHttpError(
                status=429,
                headers={},
                retry_after_seconds=0,
                retry_at_utc_ns=None,
            )
        )
        record = next(
            item
            for item in handler.records
            if _event(item) == "usdm_snapshot_rate_limited"
        )
        fields = _fields(record)
        assert fields["scope"] == "product"
        assert fields["market"] == "um_perpetual"
        assert fields["symbol"] == "ETHUSDT"
        assert fields["stream"] == "depth_snapshot"
    finally:
        collector.catalog.close()
