from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.metrics.model import MetricAggregate
from binance_market_data_recorder.metrics.recorder import MetricsRecorder
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def utc_ns(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp() * 1_000_000_000)


def envelope(
    *,
    market: Market = "spot",
    stream: str = "diff_depth",
    receive_time_utc_ns: int,
    raw_payload: bytes = b'{"E":1000,"b":[["1","2"]],"a":[["2","3"],["3","4"]]}',
    module: str = "binance.spot.websocket.v1",
    flags: tuple[str, ...] = (),
) -> EventEnvelope:
    return EventEnvelope(
        market=market,
        symbol="BTCUSDT",
        stream=stream,
        module=module,
        connection_id="connection-1",
        collector_instance_id="collector-1",
        collector_version="test",
        receive_time_utc_ns=receive_time_utc_ns,
        receive_monotonic_ns=1,
        exchange_event_time=1000,
        raw_payload=raw_payload,
        capture_flags=flags,
    )


def test_fixed_metrics_fixture_produces_deterministic_json_csv_and_all_fields(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        recorder = MetricsRecorder(
            catalog=catalog,
            data_root=layout.root,
            collector_instance_id="metrics-fixture",
            sample_interval_ns=10**30,
        )
        at = utc_ns("2026-07-22T00:00:02")
        recorder.observe_written(
            envelope(receive_time_utc_ns=at), raw_frame_bytes=120, queue_depth=3
        )
        recorder.observe_operation(
            market="spot",
            stream="diff_depth",
            name="write_latency_ns",
            duration_ns=50_000,
            occurred_at_utc_ns=at,
        )
        recorder.observe_operation(
            market="spot",
            stream="diff_depth",
            name="fsync_latency_ns",
            duration_ns=5_000_000,
            occurred_at_utc_ns=at,
        )
        recorder.observe_lifecycle(
            market="spot",
            stream="diff_depth",
            event="unexpected_disconnect",
            occurred_at_utc_ns=at,
        )
        recorder.observe_quality(
            market="spot",
            stream="diff_depth",
            event="sequence_gap",
            occurred_at_utc_ns=at,
        )
        recorder.flush()

        reporter = DailyReporter(catalog=catalog, daily_directory=layout.daily_reports)
        generated = utc_ns("2026-07-22T23:59:59")
        first = reporter.write("2026-07-22", generated_at_utc_ns=generated)
        first_json = (layout.daily_reports / "2026-07-22.json").read_bytes()
        first_csv = (layout.daily_reports / "2026-07-22.csv").read_bytes()
        second = reporter.write("2026-07-22", generated_at_utc_ns=generated)

    assert first == second
    assert first_json == (layout.daily_reports / "2026-07-22.json").read_bytes()
    assert first_csv == (layout.daily_reports / "2026-07-22.csv").read_bytes()
    stream = cast(list[dict[str, Any]], first["streams"])[0]
    assert set(stream["input"]) == {
        "websocket_messages",
        "websocket_payload_bytes",
        "rest_responses",
        "rest_bytes",
        "depth_bid_level_updates",
        "depth_ask_level_updates",
        "agg_trade_records",
        "book_ticker_records",
    }
    assert set(stream["quality"]) == {
        "accepted",
        "duplicate",
        "malformed",
        "out_of_order",
        "sequence_gap",
        "orderbook_resync",
        "planned_reconnect",
        "unexpected_disconnect",
        "server_shutdown",
        "checksum_failure",
    }
    assert set(stream["output"]) == {
        "raw_records_written",
        "raw_bytes_written",
        "sealed_chunks",
        "compressed_bytes",
        "normalized_rows",
        "normalized_bytes",
        "archived_files",
        "archived_bytes",
        "deleted_local_bytes",
        "archive_backlog_bytes",
    }
    assert set(stream["performance"]) == {
        "receive_lag_ns",
        "queue_depth_max",
        "write_latency_ns",
        "fsync_latency_ns",
        "cpu_percent",
        "rss_memory_bytes",
        "internal_free_bytes",
        "external_free_bytes",
        "oldest_unarchived_age_ns",
        "last_event_age_ns",
    }
    assert stream["input"]["websocket_messages"] == 1
    assert stream["input"]["depth_bid_level_updates"] == 1
    assert stream["input"]["depth_ask_level_updates"] == 2
    assert stream["quality"]["accepted"] == 1
    assert stream["quality"]["sequence_gap"] == 1
    assert stream["quality"]["unexpected_disconnect"] == 1
    assert stream["output"]["raw_records_written"] == 1
    assert stream["output"]["raw_bytes_written"] == 120
    assert stream["output"]["normalized_rows"] is None
    assert stream["output"]["archived_files"] == 0
    assert stream["output"]["archived_bytes"] == 0
    assert stream["output"]["deleted_local_bytes"] == 0
    assert stream["performance"]["receive_lag_ns"]["status"] == "AVAILABLE"
    assert stream["performance"]["queue_depth_max"] == {
        "value": 3,
        "status": "AVAILABLE",
    }
    assert stream["performance"]["external_free_bytes"]["status"] == (
        "NO_REGISTERED_TARGET_SAMPLE"
    )
    rows = list(csv.DictReader(io.StringIO(first_csv.decode())))
    assert len(rows) == 1
    assert rows[0]["market"] == "spot"
    assert rows[0]["stream"] == "diff_depth"


def test_metric_batches_are_idempotent_and_restart_accumulates_without_recount(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    at = utc_ns("2026-07-22T12:00:00")
    with Catalog(layout.catalog) as catalog:
        aggregate = MetricAggregate()
        aggregate.increment("accepted")
        rows = [("2026-07-22", "spot", "agg_trade", aggregate.document())]
        assert catalog.record_metric_batch(batch_id="stable-batch", rows=rows)
        assert not catalog.record_metric_batch(batch_id="stable-batch", rows=rows)
        assert catalog.metric_batch_count("stable-batch") == 1

        first = MetricsRecorder(
            catalog=catalog,
            data_root=layout.root,
            collector_instance_id="before-restart",
            sample_interval_ns=10**30,
        )
        first.observe_written(
            envelope(stream="agg_trade", receive_time_utc_ns=at, raw_payload=b"{}"),
            raw_frame_bytes=20,
            queue_depth=0,
        )
        first.flush()
    with Catalog(layout.catalog) as catalog:
        second = MetricsRecorder(
            catalog=catalog,
            data_root=layout.root,
            collector_instance_id="after-restart",
            sample_interval_ns=10**30,
        )
        second.observe_written(
            envelope(stream="agg_trade", receive_time_utc_ns=at + 1, raw_payload=b"{}"),
            raw_frame_bytes=20,
            queue_depth=0,
        )
        second.flush()
        document = DailyReporter(
            catalog=catalog, daily_directory=layout.daily_reports
        ).build("2026-07-22", generated_at_utc_ns=at + 2)
    stream = cast(list[dict[str, Any]], document["streams"])[0]
    assert stream["quality"]["accepted"] == 3
    assert stream["output"]["raw_records_written"] == 2


def test_utc_midnight_rollover_flushes_previous_day_without_cross_counting(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    before = utc_ns("2026-07-22T23:59:59")
    after = utc_ns("2026-07-23T00:00:00")
    with Catalog(layout.catalog) as catalog:
        recorder = MetricsRecorder(
            catalog=catalog,
            data_root=layout.root,
            collector_instance_id="midnight",
            sample_interval_ns=10**30,
        )
        recorder.observe_written(
            envelope(receive_time_utc_ns=before), raw_frame_bytes=10, queue_depth=0
        )
        recorder.observe_written(
            envelope(receive_time_utc_ns=after), raw_frame_bytes=11, queue_depth=0
        )
        assert (layout.daily_reports / "2026-07-22.json").is_file()
        recorder.flush()
        reporter = DailyReporter(catalog=catalog, daily_directory=layout.daily_reports)
        day_one = reporter.build("2026-07-22", generated_at_utc_ns=after)
        day_two = reporter.build("2026-07-23", generated_at_utc_ns=after)
    day_one_stream = cast(list[dict[str, Any]], day_one["streams"])[0]
    day_two_stream = cast(list[dict[str, Any]], day_two["streams"])[0]
    assert day_one_stream["output"]["raw_bytes_written"] == 10
    assert day_two_stream["output"]["raw_bytes_written"] == 11


def test_rest_wire_bytes_are_explicitly_unavailable_not_invented(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    at = utc_ns("2026-07-22T12:00:00")
    with Catalog(layout.catalog) as catalog:
        recorder = MetricsRecorder(
            catalog=catalog,
            data_root=layout.root,
            collector_instance_id="rest",
            sample_interval_ns=10**30,
        )
        recorder.observe_written(
            envelope(
                stream="depth_snapshot",
                receive_time_utc_ns=at,
                raw_payload=json.dumps({"response": {"model": {}}}).encode(),
                module="binance.spot.rest.v1",
            ),
            raw_frame_bytes=50,
            queue_depth=0,
        )
        recorder.flush()
        report = DailyReporter(
            catalog=catalog, daily_directory=layout.daily_reports
        ).build("2026-07-22", generated_at_utc_ns=at)
    stream = cast(list[dict[str, Any]], report["streams"])[0]
    assert stream["input"]["rest_responses"] == 1
    assert stream["input"]["rest_bytes"] is None
    assert stream["availability"]["input.rest_bytes"] == "UNAVAILABLE_SDK_RAW_BODY"


def test_archive_backlog_is_cumulative_state_not_only_daily_new_bytes(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    first = MetricAggregate()
    first.increment("compressed_bytes", 100)
    first.increment("archive_backlog_bytes", 100)
    second = MetricAggregate()
    second.increment("compressed_bytes", 25)
    second.increment("archive_backlog_bytes", 25)
    second.increment("archived_bytes", 40)
    with Catalog(layout.catalog) as catalog:
        catalog.record_metric_batch(
            batch_id="backlog-day-one",
            rows=[("2026-07-22", "spot", "diff_depth", first.document())],
        )
        catalog.record_metric_batch(
            batch_id="backlog-day-two",
            rows=[("2026-07-23", "spot", "diff_depth", second.document())],
        )
        report = DailyReporter(
            catalog=catalog, daily_directory=layout.daily_reports
        ).build("2026-07-23", generated_at_utc_ns=utc_ns("2026-07-23T23:59:59"))
    stream = cast(list[dict[str, Any]], report["streams"])[0]
    assert stream["output"]["compressed_bytes"] == 25
    assert stream["output"]["archive_backlog_bytes"] == 85


def test_metrics_observer_failure_is_visible_and_isolated(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        recorder = MetricsRecorder(
            catalog=catalog,
            data_root=layout.root,
            collector_instance_id="isolated-failure",
        )
        assert not recorder.safely_observe_operation(
            market="spot",
            stream="diff_depth",
            name="not_a_histogram",
            duration_ns=1,
        )
    assert recorder.failure_count == 1
    assert recorder.last_error_type == "ValueError"
