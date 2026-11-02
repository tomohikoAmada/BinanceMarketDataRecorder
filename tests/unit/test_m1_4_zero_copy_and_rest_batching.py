"""Unit tests for M1.4: Zero-copy hot path framing and merged REST persistence."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Any

import cbor2
import pytest

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.public_rest import (
    RestCaptureResult,
    RestObservation,
    _ObservedResponse,
    capture_public_response,
)
from binance_market_data_recorder.binance.public_sources import directory_source
from binance_market_data_recorder.binance.spot.exchange_info import PublicResponse
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from binance_market_data_recorder.supervisor.readiness import CollectorReadiness


def _make_envelope(
    *,
    market: Market = "spot",
    symbol: str = "BTCUSDT",
    stream: str = "trade",
    module: str = "spot",
    connection_id: str = "c1",
    receive_time_utc_ns: int = 1_000_000,
    receive_monotonic_ns: int = 2_000_000,
    source_sequence: dict[str, int | str] | None = None,
    capture_flags: tuple[str, ...] = (),
    raw_payload: bytes = b'{"e":"trade","s":"BTCUSDT"}',
) -> EventEnvelope:
    return EventEnvelope(
        market=market,
        symbol=symbol,
        stream=stream,
        module=module,
        connection_id=connection_id,
        collector_instance_id="inst-1",
        collector_version="ver-1",
        receive_time_utc_ns=receive_time_utc_ns,
        receive_monotonic_ns=receive_monotonic_ns,
        source_sequence={} if source_sequence is None else source_sequence,
        capture_flags=capture_flags,
        raw_payload=raw_payload,
    )


def test_canonical_mapping_matches_model_dump_and_produces_identical_cbor() -> None:
    envelope = _make_envelope(
        source_sequence={"u": 100, "U": 90, "sourceStream": "diff_depth"},
        capture_flags=("flag1", "flag2"),
    )
    direct_mapping = envelope.canonical_mapping()
    dumped_mapping = envelope.model_dump(mode="python", exclude_none=False)
    assert direct_mapping == dumped_mapping
    cbor_direct = cbor2.dumps(direct_mapping, canonical=True)
    cbor_dumped = cbor2.dumps(dumped_mapping, canonical=True)
    assert cbor_direct == cbor_dumped


def test_writer_directly_processes_event_envelopes(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="trade",
            collector_instance_id="inst-1",
            collector_version="ver-1",
            rotation=RotationPolicy(seconds=900.0, bytes=1024 * 1024),
            durability_interval_seconds=1.0,
        )
        assert writer.record_count == 0

        # Append single envelope
        env1 = _make_envelope(receive_time_utc_ns=100, receive_monotonic_ns=200)
        writer.append(env1)
        assert writer.record_count == 1

        # Append batch of envelopes
        env2 = _make_envelope(receive_time_utc_ns=101, receive_monotonic_ns=201)
        env3 = _make_envelope(
            connection_id="c2",
            receive_time_utc_ns=102,
            receive_monotonic_ns=202,
        )
        writer.append_batch([env2, env3])
        assert writer.record_count == 3
        writer.sync()
        assert writer.synced_record_count == 3

        # Mismatched envelope raises ValueError without corrupting state
        mismatched = _make_envelope(symbol="ETHUSDT")
        with pytest.raises(ValueError, match="envelope identity differs"):
            writer.append(mismatched)

        assert writer.record_count == 3
        assert not writer.poisoned


def test_rest_observation_response_headers_and_alias() -> None:
    envelope = _make_envelope(stream="exchange_info")
    obs = RestObservation(
        envelope=envelope,
        document={"symbols": []},
        valid=True,
        status=200,
        error=None,
        response_headers={"x-mbx-used-weight-1m": "10"},
    )
    assert obs.response_headers["x-mbx-used-weight-1m"] == "10"
    assert RestCaptureResult is RestObservation


def test_capture_public_response_accepts_evaluated_period() -> None:
    source = directory_source("spot", CoverageConfig(underlying_assets=("BTC",)))
    dummy_response = _ObservedResponse(
        status=200,
        headers={"content-type": "application/json"},
        document={
            "timezone": "UTC",
            "serverTime": 1700000000000,
            "rateLimits": [],
            "symbols": [],
        },
        receive_time_utc_ns=1_000_000_000,
        receive_monotonic_ns=2_000_000_000,
    )

    def dummy_request(src: Any, params: Any) -> PublicResponse:
        return dummy_response

    period = {
        "start_time_ms": 1700000000000,
        "end_time_ms": 1700000300000,
        "allowed_preceding_overlap_ms": 300_000,
    }
    result = capture_public_response(
        source,
        dummy_request,
        collector_instance_id="inst-1",
        collector_version="ver-1",
        evaluated_period=period,
    )
    assert result.valid
    assert result.response_headers == {"content-type": "application/json"}
    payload = json.loads(result.envelope.raw_payload)
    assert payload["request"]["evaluated_period"] == period


def test_poll_once_d16_uses_evaluated_period_without_double_json_parse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = SelectedAssetCapture(
            RecorderConfig(
                data_root=tmp_path, coverage=CoverageConfig(underlying_assets=("BTC",))
            ),
            logging.getLogger("test_m1_4"),
            "ver-1",
            "inst-1",
            layout,
            catalog,
        )
        from binance_market_data_recorder.binance.public_sources import Source

        source = Source(
            family="D16",
            venue_market="usdm",
            market="um_perpetual",
            symbol="BTCUSDT",
            stream="takerlongshortRatio",
            transport="rest",
            endpoint="/futures/data/takerlongshortRatio",
            cadence_seconds=300,
            parameters=(("pair", "BTCUSDT"), ("period", "5m")),
        )
        owner.add_source(source)
        state = owner.sources[source.key]

        class DummyClient:
            def request(self, src: Any, params: Any) -> PublicResponse:
                return _ObservedResponse(
                    status=200,
                    headers={"content-type": "application/json"},
                    document=[
                        {
                            "buySellRatio": "1.0",
                            "buyVol": "100.0",
                            "sellVol": "100.0",
                            "timestamp": params["startTime"],
                        }
                    ],
                    receive_time_utc_ns=1_000_000_000,
                    receive_monotonic_ns=2_000_000_000,
                )

        owner.clients["usdm"] = DummyClient()

        async def run_poll() -> None:
            result = await owner.poll_once(state)
            assert result.valid
            payload = json.loads(result.envelope.raw_payload)
            assert "evaluated_period" in payload["request"]
            assert (
                payload["request"]["evaluated_period"]["allowed_preceding_overlap_ms"]
                == 300_000
            )

        asyncio.run(run_poll())


def test_observe_rest_rejection_reads_headers_directly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        layout = ensure_storage_layout(tmp_path)
        with Catalog(layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(
                    data_root=tmp_path, coverage=CoverageConfig(underlying_assets=("BTC",))
                ),
                logging.getLogger("test_m1_4_cooldown"),
                "ver-1",
                "inst-1",
                layout,
                catalog,
            )
            source = directory_source("spot", owner.coverage)
            now = time.time_ns()
            envelope = _make_envelope(
                market="spot",
                symbol="BTCUSDT",
                stream=source.stream,
                receive_time_utc_ns=now,
                capture_flags=("source_failed",),
                raw_payload=b'{"not":"json-inspected"}',
            )
            obs = RestObservation(
                envelope=envelope,
                document={"msg": "Too many requests"},
                valid=False,
                status=429,
                error="TooManyRequestsError",
                response_headers={"retry-after": "45"},
            )
            # Verify _observe_rest_rejection does NOT call json.loads on raw_payload
            # If it called json.loads(raw_payload), it would fail to find headers.
            owner._observe_rest_rejection(source, obs)
            from binance_market_data_recorder.binance.request_budget import shared_request_budget

            budget = shared_request_budget()
            assert budget.delay({"spot_weight": 1}) >= 40

    asyncio.run(scenario())


def test_persist_rest_coalesces_concurrent_drains(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        owner = SelectedAssetCapture(
            RecorderConfig(
                data_root=tmp_path, coverage=CoverageConfig(underlying_assets=("BTC",))
            ),
            logging.getLogger("test_m1_4_persist"),
            "ver-1",
            "inst-1",
            layout,
            catalog,
        )
        drain_call_count = 0

        async def run_persist_coalescing() -> None:
            nonlocal drain_call_count
            spool = owner.spool("spot", "BTCUSDT", "rest_observations")
            real_drain = spool.drain_all

            def counted_drain() -> int:
                nonlocal drain_call_count
                drain_call_count += 1
                time.sleep(0.05)  # simulate disk I/O to allow concurrency
                return real_drain()

            spool.drain_all = counted_drain  # type: ignore[method-assign]

            env1 = _make_envelope(market="spot", symbol="BTCUSDT", stream="depth_snapshot")
            env2 = _make_envelope(market="spot", symbol="BTCUSDT", stream="funding_rate")
            env3 = _make_envelope(market="spot", symbol="BTCUSDT", stream="open_interest")

            # Fire 3 concurrent persist_rest calls for the same market/symbol
            await asyncio.gather(
                owner.persist_rest(env1),
                owner.persist_rest(env2),
                owner.persist_rest(env3),
            )

        asyncio.run(run_persist_coalescing())
        # All 3 were persisted, but drain was batched into fewer than 3 calls
        assert 1 <= drain_call_count < 3
        # Verify spool wrote all 3 frames
        spool = owner.spool("spot", "BTCUSDT", "rest_observations")
        assert spool._written_frames == 3


def test_collector_readiness_observe_persisted_handles_grouped_streams() -> None:
    readiness = CollectorReadiness(
        market="spot",
        symbol="BTCUSDT",
        collector_instance_id="inst-1",
        collector_version="ver-1",
    )
    readiness.observe_connected("diff_depth")
    readiness.observe_connected("book_ticker")
    readiness.observe_connected("agg_trade")

    # Grouped envelope has stream="ws_market_12345" and
    # source_sequence={"sourceStream": "diff_depth"}
    grouped_env = _make_envelope(
        market="spot",
        symbol="BTCUSDT",
        stream="ws_market_12345",
        source_sequence={"sourceStream": "diff_depth", "U": 10, "u": 20},
    )
    readiness.observe_persisted(grouped_env)
    snapshot = readiness.snapshot()
    assert "diff_depth" in snapshot.persisted_streams
