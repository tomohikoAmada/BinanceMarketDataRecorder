from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.collector.resync import DepthResyncCoordinator
from binance_market_data_recorder.collector.spot import (
    SpotCollector,
    SpotCollectorSettings,
)
from binance_market_data_recorder.collector.usdm import (
    UsdMCollector,
    UsdMCollectorSettings,
)
from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.service.resources import (
    current_rss_bytes,
    peak_rss_bytes,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.supervisor.readiness import CollectorReadiness


def _depth_event(
    market: Market, instance: str, sequence: int, connection: str = "connection-a"
) -> EventEnvelope:
    raw = (
        f'{{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":{sequence},'
        f'"u":{sequence},"pu":{sequence - 1},"b":[],"a":[]}}'
    ).encode()
    return EventEnvelope(
        market=market,
        symbol="BTCUSDT",
        stream="diff_depth",
        module=f"binance.{market}.websocket",
        connection_id=connection,
        collector_instance_id=instance,
        collector_version="test",
        receive_time_utc_ns=time.time_ns(),
        receive_monotonic_ns=time.monotonic_ns(),
        exchange_event_time=1,
        source_sequence={"U": sequence, "u": sequence, "pu": sequence - 1},
        raw_payload=raw,
    )


@pytest.mark.parametrize(
    "collector_kind,market",
    [("spot", "spot"), ("usdm", "um_perpetual")],
)
@pytest.mark.parametrize(
    "reason",
    ["planned_rotation", "unexpected_disconnect", "server_shutdown"],
)
def test_diff_depth_lifecycle_requests_market_local_resync(
    tmp_path: Path, collector_kind: str, market: Market, reason: str
) -> None:
    instance = f"{collector_kind}-instance"
    if collector_kind == "spot":
        collector: SpotCollector | UsdMCollector = SpotCollector(
            SpotCollectorSettings(tmp_path / collector_kind, instance, "test"),
            logger=logging.getLogger("test.m19.spot"),
        )
    else:
        collector = UsdMCollector(
            UsdMCollectorSettings(tmp_path / collector_kind, instance, "test"),
            logger=logging.getLogger("test.m19.usdm"),
        )
    collector._observe_persisted(_depth_event(market, instance, 10))
    observer = collector.streams[0].lifecycle_observer
    assert observer is not None
    observer(reason)
    request = collector.resync.active
    assert request is not None
    assert request.reason == reason
    assert request.original_connection_id == "connection-a"
    assert collector.resync.requested.is_set()
    with Catalog(collector.layout.catalog) as catalog:
        events = catalog.operational_events(event_type="DEPTH_RESYNC_REQUESTED")
    assert len(events) == 1
    evidence = cast(dict[str, object], events[0]["evidence"])
    assert evidence["market"] == market
    collector.catalog.close()


@pytest.mark.parametrize(
    "collector_kind,market",
    [("spot", "spot"), ("usdm", "um_perpetual")],
)
def test_bootstrap_overflow_requests_bounded_session_restart(
    tmp_path: Path, collector_kind: str, market: Market
) -> None:
    instance = f"{collector_kind}-overflow"
    if collector_kind == "spot":
        collector: SpotCollector | UsdMCollector = SpotCollector(
            SpotCollectorSettings(
                tmp_path / collector_kind,
                instance,
                "test",
                bootstrap_buffer_capacity=2,
            ),
            logger=logging.getLogger("test.m19.spot-overflow"),
        )
    else:
        collector = UsdMCollector(
            UsdMCollectorSettings(
                tmp_path / collector_kind,
                instance,
                "test",
                bootstrap_buffer_capacity=2,
            ),
            logger=logging.getLogger("test.m19.usdm-overflow"),
        )
    collector._observe_persisted(_depth_event(market, instance, 10))
    collector._observe_persisted(_depth_event(market, instance, 11))
    collector._observe_persisted(_depth_event(market, instance, 12))
    assert collector.resync.requested.is_set()
    assert collector.resync.active is not None
    assert collector.resync.active.reason == "bootstrap_buffer_overflow"
    collector.catalog.close()


def test_current_and_peak_rss_have_distinct_truthful_semantics() -> None:
    current = current_rss_bytes()
    peak = peak_rss_bytes()
    assert peak > 0
    assert current is None or 0 < current <= peak


def _snapshot_event(
    market: Market, instance: str, last_update_id: int
) -> EventEnvelope:
    payload = json.dumps(
        {
            "schema_version": "test-snapshot",
            "response": {
                "model": {
                    "lastUpdateId": last_update_id,
                    "bids": [["1", "1"]],
                    "asks": [["2", "1"]],
                }
            },
        }
    ).encode()
    return EventEnvelope(
        market=market,
        symbol="BTCUSDT",
        stream="depth_snapshot",
        module=f"binance.{market}.rest",
        connection_id="rest",
        collector_instance_id=instance,
        collector_version="test",
        receive_time_utc_ns=time.time_ns(),
        receive_monotonic_ns=time.monotonic_ns(),
        source_sequence={"lastUpdateId": last_update_id},
        payload_encoding="utf-8-json-provenance",
        raw_payload=payload,
    )


def test_resync_completion_records_applied_local_book_update_id(
    tmp_path: Path,
) -> None:
    instance = "resync-applied-id"
    catalog = Catalog(tmp_path / "catalog.sqlite")
    readiness = CollectorReadiness(
        market="spot",
        collector_instance_id=instance,
        collector_version="test",
    )
    coordinator = DepthResyncCoordinator(market="spot", catalog=catalog)
    first = _depth_event("spot", instance, 100, "new-connection")
    second = _depth_event("spot", instance, 101, "new-connection")
    readiness.observe_persisted(first)
    readiness.observe_persisted(second)
    snapshot = _snapshot_event("spot", instance, 99)
    coordinator.observe_depth(second)
    coordinator.request("sequence_gap")
    readiness.observe_snapshot_persisted(snapshot)
    assert readiness.reliable_update_id == 101
    recovered_update_id = readiness.reliable_update_id
    assert recovered_update_id is not None
    coordinator.complete(snapshot, recovered_update_id)
    completed = catalog.operational_events(event_type="DEPTH_RESYNC_COMPLETED")
    evidence = cast(dict[str, object], completed[0]["evidence"])
    assert int(cast(int, evidence["recovered_update_id"])) == 101
    catalog.close()


def test_usdm_core_failure_awaits_side_cleanup_before_catalog_close(
    tmp_path: Path,
) -> None:
    async def exercise() -> None:
        collector = UsdMCollector(
            UsdMCollectorSettings(
                tmp_path,
                "usdm-cleanup",
                "test",
            ),
            logger=logging.getLogger("test.m19.usdm-cleanup"),
        )
        side_stopped = asyncio.Event()

        class RunningSideData:
            async def run(self, stop: asyncio.Event) -> None:
                await stop.wait()
                collector.catalog.record_operational_event(
                    event_id="side-cleanup-complete",
                    event_type="SIDE_CLEANUP_COMPLETE",
                    occurred_at_utc_ns=time.time_ns(),
                    evidence={"sealed": True},
                )
                side_stopped.set()

            def status(self) -> dict[str, dict[str, object]]:
                return {}

        async def fail_core(_stop: asyncio.Event) -> None:
            raise RuntimeError("original core failure")

        collector_any = cast(Any, collector)
        collector_any.side_data = RunningSideData()
        collector_any._run_capture_session = fail_core
        with pytest.raises(RuntimeError, match="original core failure"):
            await collector.run(asyncio.Event())
        assert side_stopped.is_set()
        pending = [
            task
            for task in asyncio.all_tasks()
            if task is not asyncio.current_task() and not task.done()
        ]
        assert pending == []
        with Catalog(collector.layout.catalog) as catalog:
            assert len(
                catalog.operational_events(event_type="SIDE_CLEANUP_COMPLETE")
            ) == 1

    asyncio.run(exercise())
