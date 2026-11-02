"""Unit and integration tests for Milestone M1.3 (ADR-0058):
- Kline closed-bar persistence & in-memory forming bar filtering
- Missed closed bar gap detection & automated REST backfill
- S12 direct deduplication (10 redundant intervals covered by S11)
- One-off seed source retirement from 1-second loops
- Malformed kline frame resilience
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from binance_market_data_recorder.binance.coverage_config import (
    SPOT_INTERVALS,
    SPOT_UTC8_INTERVALS,
    CoverageConfig,
)
from binance_market_data_recorder.binance.public_sources import (
    Source,
    product_sources,
)
from binance_market_data_recorder.binance.public_websocket import (
    StreamGroup,
    envelope_for_group,
)
from binance_market_data_recorder.collector.coverage import (
    SelectedAssetCapture,
)
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.product import Product, ProductKey
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


class DummyResponse:
    def __init__(self, data: Any, status: int = 200, headers: dict[str, str] | None = None) -> None:
        self._data = data
        self.status = status
        self.headers = headers or {}

    def data(self) -> Any:
        return self._data


def sample_product(venue: str = "spot", symbol: str = "BTCUSDT") -> Product:
    return Product(
        key=ProductKey("spot" if venue == "spot" else "um_perpetual", symbol),
        venue_market=venue,
        pair="BTCUSDT",
        base_asset="BTC",
        quote_asset="USDT",
        margin_asset=None,
        contract_type="PERPETUAL" if venue != "spot" else "",
        contract_size=None,
        onboard_time_ms=None,
        delivery_time_ms=None,
        status="TRADING",
    )


def test_s12_direct_deduplication_subscribes_only_distinct_intervals() -> None:
    cfg = CoverageConfig(underlying_assets=("BTC",))
    prod = sample_product("spot", "BTCUSDT")
    sources = product_sources(prod, cfg)

    s11_sources = [s for s in sources if s.family == "S11" and s.transport == "ws"]
    s12_sources = [s for s in sources if s.family == "S12" and s.transport == "ws"]

    assert len(s11_sources) == len(SPOT_INTERVALS)  # 16
    assert len(s12_sources) == len(SPOT_UTC8_INTERVALS)  # 6

    s12_streams = {s.stream for s in s12_sources}
    for interval in SPOT_UTC8_INTERVALS:
        assert f"kline_{interval}_utc8" in s12_streams

    # Verify the 10 redundant intervals are NOT subscribed
    redundant_intervals = ("1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "8h")
    for interval in redundant_intervals:
        assert f"kline_{interval}_utc8" not in s12_streams


def test_envelope_for_group_kline_parsing_and_malformed_checks() -> None:
    prod = sample_product("spot", "BTCUSDT")
    src = Source(
        family="S11",
        venue_market="spot",
        market="spot",
        symbol="BTCUSDT",
        stream="kline_1m",
        transport="ws",
        endpoint="wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
        cadence_seconds=0,
    )
    group = StreamGroup(
        root="wss://stream.binance.com:9443",
        route="",
        name="ws_btcusdt",
        sources=(src,),
    )

    # 1. Valid forming bar (k.x == False)
    payload_forming = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1700000005000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1700000000000,
                    "T": 1700000059999,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "x": False,
                    "o": "50000",
                    "c": "50100",
                    "h": "50200",
                    "l": "49900",
                    "v": "10",
                },
            },
        }
    ).encode()
    env_forming = envelope_for_group(
        group,
        prod,
        raw_payload=payload_forming,
        connection_id="conn-1",
        collector_instance_id="inst-1",
        collector_version="1.0",
        receive_time_utc_ns=1_000_000_000,
        receive_monotonic_ns=1_000_000,
    )
    assert "malformed" not in env_forming.capture_flags
    assert env_forming.source_sequence["klineClosed"] == 0
    assert env_forming.source_sequence["openTime"] == 1700000000000
    assert env_forming.source_sequence["closeTime"] == 1700000059999
    assert env_forming.source_sequence["interval"] == "1m"

    # 2. Valid closed bar (k.x == True)
    payload_closed = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1700000060000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1700000000000,
                    "T": 1700000059999,
                    "s": "BTCUSDT",
                    "i": "1m",
                    "x": True,
                    "o": "50000",
                    "c": "50150",
                    "h": "50200",
                    "l": "49900",
                    "v": "15",
                },
            },
        }
    ).encode()
    env_closed = envelope_for_group(
        group,
        prod,
        raw_payload=payload_closed,
        connection_id="conn-1",
        collector_instance_id="inst-1",
        collector_version="1.0",
        receive_time_utc_ns=2_000_000_000,
        receive_monotonic_ns=2_000_000,
    )
    assert "malformed" not in env_closed.capture_flags
    assert env_closed.source_sequence["klineClosed"] == 1

    # 3. Malformed: non-boolean x (e.g. string "true")
    payload_bad_x = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1700000060000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1700000000000,
                    "T": 1700000059999,
                    "s": "BTCUSDT",
                    "x": "true",
                },
            },
        }
    ).encode()
    env_bad_x = envelope_for_group(
        group,
        prod,
        raw_payload=payload_bad_x,
        connection_id="conn-1",
        collector_instance_id="inst-1",
        collector_version="1.0",
        receive_time_utc_ns=3_000_000_000,
        receive_monotonic_ns=3_000_000,
    )
    assert "malformed" in env_bad_x.capture_flags

    # 4. Malformed: missing k object
    payload_no_k = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1700000060000,
                "s": "BTCUSDT",
            },
        }
    ).encode()
    env_no_k = envelope_for_group(
        group,
        prod,
        raw_payload=payload_no_k,
        connection_id="conn-1",
        collector_instance_id="inst-1",
        collector_version="1.0",
        receive_time_utc_ns=4_000_000_000,
        receive_monotonic_ns=4_000_000,
    )
    assert "malformed" in env_no_k.capture_flags

    # 5. Malformed: openTime > closeTime
    payload_inverted = json.dumps(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1700000060000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1700000060000,
                    "T": 1700000050000,
                    "s": "BTCUSDT",
                    "x": True,
                },
            },
        }
    ).encode()
    env_inverted = envelope_for_group(
        group,
        prod,
        raw_payload=payload_inverted,
        connection_id="conn-1",
        collector_instance_id="inst-1",
        collector_version="1.0",
        receive_time_utc_ns=5_000_000_000,
        receive_monotonic_ns=5_000_000,
    )
    assert "malformed" in env_inverted.capture_flags


def test_kline_forming_filter_and_valid_source_liveness(tmp_path: Path) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        cfg = RecorderConfig(
            data_root=tmp_path,
            coverage=CoverageConfig(underlying_assets=("BTC",), spot_intervals=("1m",)),
        )
        capture = SelectedAssetCapture(
            cfg, MagicMock(), version="1.0", instance_id="test-inst", layout=layout, catalog=catalog
        )
        prod = sample_product("spot", "BTCUSDT")
        src = Source(
            family="S11",
            venue_market="spot",
            market="spot",
            symbol="BTCUSDT",
            stream="kline_1m",
            transport="ws",
            endpoint="wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
            cadence_seconds=0,
        )
        group = StreamGroup(
            root="wss://stream.binance.com:9443",
            route="",
            name="ws_btcusdt",
            sources=(src,),
        )
        capture.add_source(src)
        state = capture.sources[src.key]
        state.connected = True

        # 1. Simulate receiving forming bar (k.x == False) through group collector
        collector = capture.make_group(group, prod, lambda e: None, lambda event: None)
        payload_forming = json.dumps(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "E": 1700000005000,
                    "s": "BTCUSDT",
                    "k": {
                        "t": 1700000000000,
                        "T": 1700000059999,
                        "s": "BTCUSDT",
                        "i": "1m",
                        "x": False,
                        "o": "50000",
                        "c": "50100",
                        "h": "50200",
                        "l": "49900",
                        "v": "10",
                    },
                },
            }
        ).encode()

        assert collector.envelope_factory is not None
        # envelope_factory returns None for forming bar under closed_only policy
        result = collector.envelope_factory(
            raw_payload=payload_forming,
            connection_id="conn-1",
            collector_instance_id="test-inst",
            collector_version="1.0",
            receive_time_utc_ns=1_000_000_000,
            receive_monotonic_ns=1_000_000,
        )
        assert result is None
        assert state.forming_filtered_count == 1
        assert state.last_success == 1_000_000_000

        # Availability check should show source remains VALID without error
        capture._observe_availability(1_000_000_000)
        health = capture.availability.sources["/".join(src.key)]
        assert health.unavailable_since is None

        # 2. Receiving closed bar (k.x == True) returns envelope
        payload_closed = json.dumps(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "E": 1700000060000,
                    "s": "BTCUSDT",
                    "k": {
                        "t": 1700000000000,
                        "T": 1700000059999,
                        "s": "BTCUSDT",
                        "i": "1m",
                        "x": True,
                        "o": "50000",
                        "c": "50150",
                        "h": "50200",
                        "l": "49900",
                        "v": "15",
                    },
                },
            }
        ).encode()
        closed_env = collector.envelope_factory(
            raw_payload=payload_closed,
            connection_id="conn-1",
            collector_instance_id="test-inst",
            collector_version="1.0",
            receive_time_utc_ns=2_000_000_000,
            receive_monotonic_ns=2_000_000,
        )
        assert closed_env is not None
        assert state.last_closed_open_time_ms == 1700000000000

    asyncio.run(scenario())


def test_kline_gap_detection_triggers_automated_rest_backfill(tmp_path: Path) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        cfg = RecorderConfig(
            data_root=tmp_path,
            coverage=CoverageConfig(underlying_assets=("BTC",), spot_intervals=("1m",)),
        )
        capture = SelectedAssetCapture(
            cfg, MagicMock(), version="1.0", instance_id="test-inst", layout=layout, catalog=catalog
        )
        prod = sample_product("spot", "BTCUSDT")
        src = Source(
            family="S11",
            venue_market="spot",
            market="spot",
            symbol="BTCUSDT",
            stream="kline_1m",
            transport="ws",
            endpoint="wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
            cadence_seconds=0,
        )
        group = StreamGroup(
            root="wss://stream.binance.com:9443",
            route="",
            name="ws_btcusdt",
            sources=(src,),
        )
        capture.add_source(src)
        state = capture.sources[src.key]
        state.connected = True
        # Establish previous closed bar at open_time 1700000000000
        state.last_closed_open_time_ms = 1700000000000

        # Mock REST client for backfill
        mock_bars = [
            [
                1700000060000,
                "50000",
                "50100",
                "49900",
                "50050",
                "10",
                1700000119999,
                "500000",
                100,
                "5",
                "250000",
                "0",
            ]
        ]
        mock_client = MagicMock()
        mock_client.request.return_value = DummyResponse(mock_bars)
        capture.clients["spot"] = mock_client

        collector = capture.make_group(group, prod, lambda e: None, lambda event: None)

        # Bar arrives skipping 1 minute: open_time 1700000120000 (missing 1700000060000)
        payload_gap_closed = json.dumps(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "e": "kline",
                    "E": 1700000180000,
                    "s": "BTCUSDT",
                    "k": {
                        "t": 1700000120000,
                        "T": 1700000179999,
                        "s": "BTCUSDT",
                        "i": "1m",
                        "x": True,
                        "o": "50050",
                        "c": "50200",
                        "h": "50250",
                        "l": "50000",
                        "v": "20",
                    },
                },
            }
        ).encode()

        assert collector.envelope_factory is not None
        collector.envelope_factory(
            raw_payload=payload_gap_closed,
            connection_id="conn-1",
            collector_instance_id="test-inst",
            collector_version="1.0",
            receive_time_utc_ns=3_000_000_000,
            receive_monotonic_ns=3_000_000,
        )

        # Let background backfill task run
        assert state.backfill_start_ms is not None
        await capture._run_kline_backfill(state)

        # Verify REST was called with appropriate bounds
        assert mock_client.request.called
        call_args = mock_client.request.call_args[0]
        backfill_src, params = call_args[0], call_args[1]
        assert backfill_src.endpoint.endswith("/klines")
        assert params["symbol"] == "BTCUSDT"
        assert params["interval"] == "1m"
        assert params["startTime"] == 1700000060000
        assert params["endTime"] == 1700000119999

        # Verify spool recorded the backfill
        rest_spool = capture.spool("spot", "BTCUSDT", "rest_observations")
        assert rest_spool._written_frames > 0 or rest_spool.queue.depth > 0

    asyncio.run(scenario())


def test_reconnect_past_close_boundary_schedules_backfill(tmp_path: Path) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        cfg = RecorderConfig(
            data_root=tmp_path,
            coverage=CoverageConfig(underlying_assets=("BTC",), spot_intervals=("1m",)),
        )
        capture = SelectedAssetCapture(
            cfg, MagicMock(), version="1.0", instance_id="test-inst", layout=layout, catalog=catalog
        )
        src = Source(
            family="S11",
            venue_market="spot",
            market="spot",
            symbol="BTCUSDT",
            stream="kline_1m",
            transport="ws",
            endpoint="wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
            cadence_seconds=0,
        )
        capture.add_source(src)
        state = capture.sources[src.key]
        # Bar closed at open_time 1000 (1 second), which would expect next close at 60_999 ms
        state.last_closed_open_time_ms = 1_000

        mock_client = MagicMock()
        mock_client.request.return_value = DummyResponse([])
        capture.clients["spot"] = mock_client

        # Current time is far past next expected close boundary
        state.connected = False
        capture._check_reconnect_kline_backfill(src, state)

        assert state.backfill_start_ms is not None
        await capture._run_kline_backfill(state)
        assert mock_client.request.called

    asyncio.run(scenario())


def test_seed_source_retires_upon_successful_completion(tmp_path: Path) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        catalog = Catalog(layout.catalog)
        cfg = RecorderConfig(
            data_root=tmp_path,
            coverage=CoverageConfig(underlying_assets=("BTC",), spot_intervals=("1m",)),
        )
        capture = SelectedAssetCapture(
            cfg, MagicMock(), version="1.0", instance_id="test-inst", layout=layout, catalog=catalog
        )
        seed_src = Source(
            family="S11",
            venue_market="spot",
            market="spot",
            symbol="BTCUSDT",
            stream="kline_seed_1m",
            transport="rest",
            endpoint="/api/v3/klines",
            cadence_seconds=0,
            parameters=(("symbol", "BTCUSDT"), ("interval", "1m"), ("limit", 2)),
        )
        capture.add_source(seed_src)
        assert seed_src.key in capture._active_sources
        assert seed_src.key not in capture._completed_seed_sources

        # Mock response
        mock_client = MagicMock()
        mock_client.request.return_value = DummyResponse(
            [
                [
                    1700000000000,
                    "50000",
                    "50100",
                    "49900",
                    "50050",
                    "10",
                    1700000059999,
                    "500000",
                    100,
                    "5",
                    "250000",
                    "0",
                ],
                [
                    1700000060000,
                    "50050",
                    "50200",
                    "50000",
                    "50150",
                    "15",
                    1700000119999,
                    "750000",
                    150,
                    "8",
                    "400000",
                    "0",
                ],
            ]
        )
        capture.clients["spot"] = mock_client

        state = capture.sources[seed_src.key]
        await capture.poll_once(state)

        # 1. Retired: moved out of _active_sources into _completed_seed_sources
        assert seed_src.key not in capture._active_sources
        assert seed_src.key in capture._completed_seed_sources

        # 2. Still accessible via unified capture.sources and status
        assert seed_src.key in capture.sources
        assert any(s["stream"] == "kline_seed_1m" for s in capture.status()["sources"])

        # 3. Not evaluated in _observe_availability loop
        prev_avail = state.availability
        capture._observe_availability(time.time_ns())
        # State in _completed_seed_sources was untouched by _observe_availability
        assert state.availability == prev_avail

        # 4. If matching live WS source exists, initial openTime was seeded
        ws_src = Source(
            family="S11",
            venue_market="spot",
            market="spot",
            symbol="BTCUSDT",
            stream="kline_1m",
            transport="ws",
            endpoint="wss://stream.binance.com:9443/ws/btcusdt@kline_1m",
            cadence_seconds=0,
        )
        capture.add_source(ws_src)
        # Re-poll seed to test seeding WS source state
        state.last_success = None
        state.error = None
        await capture.poll_once(state)
        assert capture.sources[ws_src.key].last_closed_open_time_ms == 1700000060000

    asyncio.run(scenario())


