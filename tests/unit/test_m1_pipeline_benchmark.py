"""Regression checks for the fixed-input benchmark's measurement contract."""

from __future__ import annotations

import asyncio
import json
import socket
import sqlite3
from typing import Any

import pytest

from binance_market_data_recorder.binance.coverage_config import CoverageConfig
from binance_market_data_recorder.binance.products import discover_products
from binance_market_data_recorder.binance.public_websocket import envelope_for_group
from tools.benchmark_m1_m3_pipeline import (
    ConnectionProxy,
    StatsCollector,
    build_fixture_frames,
    directory,
    fixture_groups,
    logical_record,
    run_benchmark,
)


def test_fixed_seed_changes_content_and_all_ws_scopes_have_valid_frames() -> None:
    cfg = CoverageConfig(
        underlying_assets=("BTC", "ETH"),
        spot_intervals=("1m", "5m", "1M"),
        spot_utc8_intervals=("1m", "5m", "1M"),
        futures_intervals=("1m", "5m", "1M"),
        kline_forming_policy="all",
    )
    products = [
        p
        for venue in ("spot", "usdm", "coinm")
        for p in discover_products(venue, directory(venue), cfg)
        if p.status == "TRADING"
    ]
    feed, manifest = build_fixture_frames(products, cfg, 4, 123)
    assert (feed, manifest) == build_fixture_frames(products, cfg, 4, 123)
    assert manifest != build_fixture_frames(products, cfg, 4, 124)[1]
    groups = fixture_groups(products, cfg)
    assert set(feed) == {g.name for g in groups}
    assert any(s.stream == "continuous_kline_5m" for g in groups for s in g.sources)
    assert any(s.stream == "asset_index" for g in groups for s in g.sources)
    by_key = {(p.key.market, p.key.symbol): p for p in products}
    for group in groups:
        source = group.sources[0]
        product = by_key.get((source.market, source.symbol))
        for payload in feed[group.name]:
            envelope = envelope_for_group(
                group,
                product,
                raw_payload=payload,
                connection_id="connection",
                collector_instance_id="test",
                collector_version="test",
                receive_time_utc_ns=1,
                receive_monotonic_ns=1,
            )
            assert "malformed" not in envelope.capture_flags
            data = json.loads(payload)["data"]
            if isinstance(data, dict) and "k" in data:
                wire_interval = json.loads(payload)["stream"].rsplit("_", 1)[1].split("@")[0]
                assert data["k"]["i"] == wire_interval
                assert data["k"]["T"] > data["k"]["t"]
                if data["k"]["x"]:
                    assert data["E"] > data["k"]["T"]
        # A payload change with identical stream/count must change the digest.
        original = envelope
        assert logical_record(original) != logical_record(
            original.model_copy(update={"raw_payload": original.raw_payload + b" "})
        )
        assert logical_record(original) == logical_record(
            original.model_copy(
                update={
                    "connection_id": "another",
                    "receive_time_utc_ns": 99,
                    "receive_monotonic_ns": 99,
                }
            )
        )


def test_sql_statements_are_not_counted_as_transactions() -> None:
    stats = StatsCollector()
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.execute("CREATE TABLE sample (value INTEGER)")
    proxy = ConnectionProxy(connection, stats)
    proxy.execute("INSERT INTO sample VALUES (1)")  # autocommit
    proxy.execute("BEGIN IMMEDIATE")
    proxy.execute("INSERT INTO sample VALUES (2)")
    proxy.execute("UPDATE sample SET value = 3")
    proxy.executemany("INSERT INTO sample VALUES (?)", [(4,), (5,)])
    proxy.execute("COMMIT")
    proxy.execute("BEGIN")
    proxy.execute("DELETE FROM sample")
    proxy.execute("ROLLBACK")
    assert len(stats.latencies["sql_statement"]) == 4
    assert stats.sql_statement_count == 6
    assert len(stats.latencies["sql_batch"]) == 1
    assert len(stats.latencies["sql_transaction"]) == 2
    assert connection.execute("SELECT COUNT(*) FROM sample").fetchone()[0] == 4
    connection.close()


def test_pipeline_replays_and_checks_actual_raw_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_network(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("benchmark must remain offline")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    results = [asyncio.run(run_benchmark(events_per_stream=4, monitor_seconds=0)) for _ in range(2)]
    first, second = results
    assert first["workload"]["input_digest"] == second["workload"]["input_digest"]
    assert first["metrics"]["logical_content_digest"] == second["metrics"]["logical_content_digest"]
    assert first["summary"]["total_frames"] == second["summary"]["total_frames"]
    assert first["summary"]["total_frames"] == (
        first["workload"]["ws_frames"] + first["workload"]["rest_polls"]
    )
    for result in results:
        metrics = result["metrics"]
        assert sum(c["record_count"] for c in result["chunks"]) == metrics["total_persisted_frames"]
        assert metrics["latencies_ms"]["seal"]["count"] == metrics["raw_chunk_count"]
        assert metrics["latencies_ms"]["queue_wait"]["count"] == metrics["total_persisted_frames"]
        assert metrics["sealed_chunks_per_hour"] == pytest.approx(
            metrics["raw_chunk_count"] * 3600 / result["summary"]["wall_seconds"]
        )
        assert "not steady-state" in metrics["rate_note"]
        assert (
            metrics["sqlite_catalog_write_statements"]
            > metrics["latencies_ms"]["sql_transaction"]["count"]
        )
