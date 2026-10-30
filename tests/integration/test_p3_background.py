from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from pathlib import Path

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.binance.public_sources import product_sources
from binance_market_data_recorder.binance.public_websocket import envelope_for_group, stream_groups
from binance_market_data_recorder.binance.usdm.websocket import (
    UsdMStreamCollector,
    WebSocketConnection,
)
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.normalize.raw import iter_source_records, load_source_chunks
from binance_market_data_recorder.replay import GapPolicy, ManifestCatalog, ReplayQuery
from binance_market_data_recorder.service.report_worker import DailyReportWorker
from binance_market_data_recorder.service.verification import verify_new
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import prepare_archive
from tests.integration.test_p2_capture import Client
from tests.normalization_support import seal_events
from tests.unit.test_p2_coverage import SMALL, products


def test_receive_and_normal_drain_continue_with_all_background_consumers(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)
    background_entered, release_background = threading.Event(), threading.Event()
    expected: list[bytes] = []

    def background() -> None:
        with Catalog(fixture.layout.catalog) as independent:
            through = independent.raw_verification_status()["enrollment_high_water_utc_ns"]
            assert isinstance(through, int)
            background_entered.set()
            assert release_background.wait(10)
            assert verify_new(
                fixture.layout, independent, through_utc_ns=through
            )["verified_files"] == 1
            archive = ArchiveManager(
                layout=fixture.layout,
                catalog=independent,
                target=fixture.target,
                io_bytes_per_second=16 * 1024 * 1024,
            ).run_once()
            assert archive.deleted_local_bytes > 0
            DailyReporter(catalog=independent, daily_directory=fixture.layout.daily_reports).write(
                "2026-01-01"
            )
            row = independent.chunk(fixture.chunk_ids[0])
            assert row is not None
            normalized = Normalizer(
                layout=fixture.layout,
                catalog=independent,
                manifest_paths=[fixture.layout.root / str(row["manifest_path"])],
                external_roots={fixture.target.storage_id: fixture.target.root},
            ).run()
            assert normalized.build_id is not None
            replay = ManifestCatalog(fixture.layout.root).open_build(normalized.build_id)
            assert len(list(replay.replay(ReplayQuery(gap_policy=GapPolicy.INCLUDE)))) == 1

    async def scenario() -> None:
        with Catalog(fixture.layout.catalog) as catalog:
            owner = SelectedAssetCapture(
                RecorderConfig(data_root=fixture.layout.root, coverage=SMALL),
                logging.getLogger("p3-background"),
                "v",
                "instance",
                fixture.layout,
                catalog,
                clients={"fixture": Client()},
            )
            product = next(
                p for p in products() if p.key.market == "spot" and p.key.symbol == "BTCUSDT"
            )
            source = next(s for s in product_sources(product, SMALL) if s.stream == "book_ticker")
            group = stream_groups((source,), 128)[0]
            stop = asyncio.Event()
            received: list[EventEnvelope] = []

            class Connection:
                number = 0

                async def recv(self, decode: bool | None = None) -> bytes:
                    await asyncio.sleep(0.002)
                    self.number += 1
                    raw = json.dumps(
                        {
                            "stream": group.wire_names[0],
                            "data": {
                                "u": self.number,
                                "s": product.key.symbol,
                                "b": "42.00000000",
                                "B": "1",
                                "a": "43.00000000",
                                "A": "2",
                            },
                        }
                    ).encode()
                    expected.append(raw)
                    return raw

                async def close(self, code: int = 1000, reason: str = "") -> None:
                    pass

            @asynccontextmanager
            async def opener(url: str) -> AsyncIterator[WebSocketConnection]:
                yield Connection()

            def persisted(event: EventEnvelope) -> None:
                received.append(event)
                if len(received) >= 5:
                    release_background.set()
                if len(received) >= 30:
                    stop.set()

            spool = owner.spool("spot", "BTCUSDT", group.name)
            spool.rotation = RotationPolicy(seconds=300, bytes=128 * 1024 * 1024)
            collector = UsdMStreamCollector(
                stream=group.name,
                symbol=product.key.symbol,
                route=group.route,
                wire_name=group.wire_names[0],
                combined_streams=group.wire_names,
                websocket_root=group.root,
                spool=spool,
                collector_instance_id="instance",
                collector_version="v",
                logger=logging.getLogger("p3-background"),
                opener=opener,
                envelope_factory=partial(envelope_for_group, group, product),
                envelope_observer=persisted,
            )
            task = asyncio.create_task(asyncio.to_thread(background))
            while not background_entered.is_set():
                await asyncio.sleep(0.001)
            await asyncio.wait_for(collector.run(stop), 10)
            await asyncio.wait_for(task, 10)
            assert len(received) >= 30
            progress = collector.capture_progress()
            assert progress["received_frames"] == progress["persisted_frames"] == len(expected)
            chunks = load_source_chunks(
                layout=fixture.layout,
                catalog=catalog,
                external_roots={fixture.target.storage_id: fixture.target.root},
            )
            restored = [
                record.envelope.raw_payload
                for chunk in chunks
                if chunk.manifest["stream"] == group.name
                for record in iter_source_records(chunk)
            ]
            assert restored == expected
            assert not list(fixture.layout.active.glob("*.partial"))
            assert verify_new(fixture.layout, catalog)["pending_files"] == 0

    asyncio.run(scenario())


def test_verifier_child_can_die_and_retry_without_losing_pending_data(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)

    async def scenario() -> None:
        worker = DailyReportWorker(
            fixture.layout.root,
            logging.getLogger("p3-verifier"),
            interval_seconds=0.01,
            module="binance_market_data_recorder.service.verification_worker",
            operation="raw_verification",
        )
        marker = tmp_path / "attempt"
        worker.arguments = (
            sys.executable,
            "-c",
            (
                "from pathlib import Path;import sys;"
                f"p=Path({str(marker)!r});"
                "p.write_text(str(int(p.read_text())+1) if p.exists() else '1');"
                "sys.exit(1)"
            ),
        )
        stop = asyncio.Event()
        task = asyncio.create_task(worker.run(stop))
        try:

            async def retried() -> None:
                while not marker.exists() or int(marker.read_text() or "0") < 2:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(retried(), 10)
        finally:
            stop.set()
            await asyncio.wait_for(task, 3)
        with Catalog(fixture.layout.catalog) as catalog:
            assert catalog.raw_verification_status()["pending_files"] == 1
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "binance_market_data_recorder.service.verification_worker",
            str(fixture.layout.root),
            stdout=asyncio.subprocess.PIPE,
        )
        output, _ = await asyncio.wait_for(process.communicate(), 10)
        assert process.returncode == 0 and json.loads(output)["verified_files"] == 1

    asyncio.run(scenario())


def test_normal_stop_restart_never_overwrites_a_previous_sealed_chunk(tmp_path: Path) -> None:
    fixture = prepare_archive(tmp_path)
    with Catalog(fixture.layout.catalog) as catalog:
        original = {path: path.read_bytes() for path in fixture.layout.sealed.iterdir()}
        from tests.factories import event

        for instance in ("first", "second"):
            new = event(7).model_copy(update={"collector_instance_id": instance})
            seal_events(layout=fixture.layout, catalog=catalog, events=[new])
        assert all(path.read_bytes() == body for path, body in original.items())
        assert verify_new(fixture.layout, catalog)["verified_files"] == 3
