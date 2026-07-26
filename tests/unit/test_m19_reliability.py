from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import cast

import pytest

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
