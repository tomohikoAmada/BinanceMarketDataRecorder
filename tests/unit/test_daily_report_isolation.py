from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from binance_market_data_recorder.metrics.model import MetricAggregate
from binance_market_data_recorder.metrics.recorder import MetricsRecorder
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.service.report_worker import DailyReportWorker
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout


def at(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=UTC).timestamp() * 1e9)


def batch(catalog: Catalog, day: str, identity: str) -> None:
    aggregate = MetricAggregate()
    aggregate.increment("raw_bytes_written", 10)
    catalog.record_metric_batch(
        batch_id=identity, rows=[(day, "spot", "book_ticker", aggregate.document())],
    )


def test_rollover_observation_does_not_run_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("report must never run under the metrics observation lock")

    monkeypatch.setattr(DailyReporter, "write", forbidden)
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        metrics = MetricsRecorder(
            catalog=catalog, data_root=layout.root, collector_instance_id="rollover",
        )
        for instant in (at("2026-07-22T23:59:59"), at("2026-07-23T00:00:00")):
            metrics.observe_lifecycle(
                market="spot", stream="book_ticker", event="planned_rotation",
                occurred_at_utc_ns=instant,
            )
        assert len(catalog.metric_batches("2026-07-22")) == 1
        metrics.flush()
        assert len(catalog.metric_batches("2026-07-23")) == 1


def test_pending_reports_retry_late_earlier_inputs_and_missing_output(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        batch(catalog, "2026-07-22", "one")
        batch(catalog, "2026-07-23", "two")
        reporter = DailyReporter(
            catalog=catalog, daily_directory=layout.daily_reports,
            utc_clock_ns=lambda: at("2026-07-24"),
        )
        assert reporter.write_one_pending() == {"status": "COMPLETE", "utc_date": "2026-07-23"}
        assert reporter.write_one_pending() == {"status": "COMPLETE", "utc_date": "2026-07-22"}
        assert reporter.write_one_pending() == {"status": "IDLE"}
        batch(catalog, "2026-07-22", "late")
        # Both the late day and later reports' lifetime backlog inputs are invalidated.
        assert reporter.write_one_pending()["utc_date"] == "2026-07-23"
        assert reporter.write_one_pending()["utc_date"] == "2026-07-22"
        (layout.daily_reports / "2026-07-23.csv").unlink()
        assert reporter.write_one_pending()["utc_date"] == "2026-07-23"
        assert reporter.write_one_pending() == {"status": "IDLE"}


def test_interrupted_csv_and_changed_input_do_not_publish_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        batch(catalog, "2026-07-22", "one")
        reporter = DailyReporter(
            catalog=catalog, daily_directory=layout.daily_reports,
            utc_clock_ns=lambda: at("2026-07-24"),
        )
        original = reporter._atomic_write

        def fail_csv(path: Path, body: bytes) -> None:
            if path.suffix == ".csv":
                raise OSError("synthetic full disk after JSON")
            original(path, body)

        monkeypatch.setattr(reporter, "_atomic_write", fail_csv)
        with pytest.raises(OSError, match="synthetic"):
            reporter.write_one_pending()
        assert not (layout.daily_reports / ".2026-07-22.inputs.json").exists()
        monkeypatch.setattr(reporter, "_atomic_write", original)
        original_write = reporter._write_locked

        def late_write(day: str) -> dict[str, object]:
            result = original_write(day)
            batch(catalog, day, "late")
            return result

        monkeypatch.setattr(reporter, "_write_locked", late_write)
        assert reporter.write_one_pending()["status"] == "RETRY_INPUT_CHANGED"
        assert not (layout.daily_reports / ".2026-07-22.inputs.json").exists()
        monkeypatch.setattr(reporter, "_write_locked", original_write)
        assert reporter.write_one_pending()["status"] == "COMPLETE"
        document = json.loads((layout.daily_reports / "2026-07-22.json").read_bytes())
        assert document["streams"][0]["output"]["raw_bytes_written"] == 20


def test_failed_day_does_not_starve_other_reports(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        batch(catalog, "2026-07-22", "broken")
        batch(catalog, "2026-07-21", "healthy")
        (layout.daily_reports / "2026-07-22.json").mkdir()
        reporter = DailyReporter(
            catalog=catalog, daily_directory=layout.daily_reports,
            utc_clock_ns=lambda: at("2026-07-24"),
        )
        with pytest.raises(OSError):
            reporter.write_one_pending()
        assert reporter.write_one_pending() == {"status": "COMPLETE", "utc_date": "2026-07-21"}
        with pytest.raises(OSError):
            reporter.write_one_pending()
        (layout.daily_reports / "2026-07-22.json").rmdir()
        assert reporter.write_one_pending() == {"status": "COMPLETE", "utc_date": "2026-07-22"}


def test_historical_metric_pages_preserve_order_and_report_contents(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        for number in range(1030):
            batch(catalog, "2026-07-22" if number % 2 else "2026-07-23", f"b{number:04}")
        expected = catalog.metric_batches_through("2026-07-23")
        assert list(catalog.iter_metric_batches_through("2026-07-23")) == expected
        assert list(catalog.iter_metric_batches_through(
            "2026-07-23", start_date="2026-07-23",
        )) == catalog.metric_batches("2026-07-23")
        report = DailyReporter(catalog=catalog, daily_directory=layout.daily_reports).build(
            "2026-07-23", generated_at_utc_ns=at("2026-07-24"),
        )
        assert report["batch_count"] == 515
        streams = report["streams"]
        assert isinstance(streams, list) and isinstance(streams[0], dict)
        assert streams[0]["output"]["raw_bytes_written"] == 5150


def test_slow_child_does_not_block_heartbeat_and_is_killed_on_stop(tmp_path: Path) -> None:
    async def scenario() -> None:
        ready = tmp_path / "ready"
        worker = DailyReportWorker(
            tmp_path, logging.getLogger("test"), interval_seconds=.01,
            stop_timeout_seconds=.05,
        )
        code = (
            "import os,signal,time;from pathlib import Path;"
            "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
            f"Path({str(ready)!r}).write_text(str(os.getpid()));time.sleep(100)"
        )
        worker.arguments = (sys.executable, "-c", code)
        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))
        try:
            async def wait_ready() -> None:
                while not ready.exists():
                    await asyncio.sleep(.01)

            await asyncio.wait_for(wait_ready(), 10)
            pid = int(ready.read_text())
            # This coroutine and observation stay live while the report child is blocked.
            heartbeats = 0
            for _ in range(5):
                await asyncio.sleep(.01)
                heartbeats += 1
            assert heartbeats == 5 and not task.done()
            stop.set()
            await asyncio.wait_for(task, 2)
            with pytest.raises(ProcessLookupError):
                os.kill(pid, 0)
        finally:
            stop.set()
            await asyncio.wait_for(task, 2)

    asyncio.run(scenario())
