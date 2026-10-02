"""Offline diagnostic probes for the architecture review, not acceptance credit.

Run from this checkout: PYTHONPATH=src:. python docs/reviews/2026-10-01-review-probes.py
Uses existing synthetic fixtures and temporary directories; no network or live data.
"""

import asyncio
import json
import logging
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.normalize.parser import parse_envelope
from binance_market_data_recorder.normalize.pipeline import (
    _candidate,
    _candidate_sort_key,
    _deduplicate_to_partitions,
    _external_sort,
    _PartitionSpools,
)
from binance_market_data_recorder.normalize.raw import SourceChunk, SourceRecord
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import LocalBookReconstructor
from binance_market_data_recorder.paths import discover_repository_root
from binance_market_data_recorder.service.acceptance import _publish
from binance_market_data_recorder.service.acceptance_v5_online import replay_online
from binance_market_data_recorder.service.runtime import ServiceRuntime
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import prepare_archive
from tests.factories import event
from tests.normalization_support import envelope
from tests.unit.test_acceptance_v5_online import advance, observer_fixture
from tests.unit.test_normalized_parser import _models
from tests.unit.test_service_runtime import FakeCollector, FakePowerAssertion, _config


def delta_probe(root):
    prepared = prepare_archive(root / "archive", chunk_count=0)
    with Catalog(prepared.layout.catalog) as catalog:
        catalog.migrate_acceptance_sequence()
    observer, clock, evaluator = observer_fixture(root / "observer")
    evaluator.expected_products = frozenset({ProductKey("spot", "BTCUSDT")})
    identity = replace(
        observer.identity,
        systemd_effective={
            **observer.identity.systemd_effective,
            "working_directory": str(prepared.layout.root),
        },
    )
    previous = json.loads(observer.predecessor_path.read_bytes())
    previous.update(
        deployment_identity=identity.document(), configured_products=[["spot", "BTCUSDT"]]
    )
    predecessor, _ = _publish(root / "new-baseline", "audit-root.json", previous)
    observer = replace(
        observer,
        data_root=prepared.layout.root,
        identity=identity,
        predecessor_path=predecessor,
        archive_root_resolver=lambda: {prepared.target.storage_id: prepared.target.root},
    )
    from binance_market_data_recorder.service.acceptance_v5_online import runtime_identity
    from binance_market_data_recorder.service.state import ServiceStateStore

    ServiceStateStore(prepared.layout.state / "service_state.json").write(
        {
            "status": "RUNNING",
            "pid": 123,
            "service_instance_id": "service-a",
            "deployment_identity": runtime_identity(identity),
        }
    )
    observer.start()
    samples = []
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(layout=prepared.layout, catalog=catalog, target=prepared.target)
        for window in range(2):
            for ordinal in range(60):
                writer = RawChunkWriter(
                    layout=prepared.layout,
                    catalog=catalog,
                    market="spot",
                    symbol="BTCUSDT",
                    stream="diff_depth",
                    collector_instance_id="collector-1",
                    collector_version="0.1.0+test",
                    durability_interval_seconds=0,
                )
                writer.append(event(window * 60 + ordinal + 1))
                writer.close()
                seal_partial(writer.path, layout=prepared.layout, catalog=catalog)
                manager.run_once()
            advance(clock, 300)
            _, _, sample = observer.sample()
            samples.append(
                {
                    "high_water": sample["high_water"],
                    "processed": sample["continuation"]["processed"],
                    "page_lengths": {k: len(v) for k, v in sample["delta_pages"].items()},
                    "delta_pending": sample["delta_pending"],
                    "findings": sample["blocking_findings"],
                }
            )
    replay_online(observer.evidence_root, observer.identity, require_target=False)
    return samples


def normalization_probe(root):
    payload = _models()["spot", "depth_snapshot"]
    rows = []
    for ordinal, symbol in enumerate(("BTCUSDT", "ETHUSDT")):
        source = envelope(
            market="spot", stream="depth_snapshot", raw_payload=payload, ordinal=ordinal + 1
        ).model_copy(update={"symbol": symbol})
        chunk = SourceChunk(
            str(ordinal),
            root / str(ordinal),
            root / str(ordinal),
            str(ordinal) * 64,
            {
                "uncompressed_sha256": str(ordinal) * 64,
                "capture_flags": [],
                "complete": True,
                "gap": False,
                "overlap": False,
                "recovered": False,
                "resync": False,
            },
        )
        rows.append(_candidate(SourceRecord(chunk, 0, source), parse_envelope(source)[0]))
    spools = _PartitionSpools(root)
    _deduplicate_to_partitions(sorted(rows, key=_candidate_sort_key), spools)
    spools.close()
    output = [
        json.loads(line)
        for path in spools.paths.values()
        for line in path.read_bytes().splitlines()
    ]
    return {
        "input_symbols": ["BTCUSDT", "ETHUSDT"],
        "output_symbols": [r["symbol"] for r in output],
        "duplicate_counts": [r["duplicate_count"] for r in output],
    }


async def heartbeat_probe(root):
    collectors = {
        ProductKey(market, "BTCUSDT"): FakeCollector(market, market)
        for market in ("spot", "um_perpetual")
    }
    runtime = ServiceRuntime(
        config=_config(root),
        logger=logging.getLogger("review"),
        collector_factory=lambda *_args: collectors,
        power_assertion=FakePowerAssertion(),
    )
    failed = asyncio.Event()

    original_write_state = runtime._write_state

    async def injected_write_state():
        current = asyncio.current_task()
        if current is not None and current.get_name() == "GLOBAL:service-heartbeat":
            failed.set()
            raise OSError("injected state publication failure")
        await original_write_state()

    # Exercise the real heartbeat; fail its publication boundary, not other state writes.
    runtime._write_state = injected_write_state
    task = asyncio.create_task(runtime.run())
    await failed.wait()
    for _ in range(100):
        if all(c.started for c in collectors.values()):
            break
        await asyncio.sleep(0.01)
    continued = all(c.started and not c.stopped for c in collectors.values()) and not task.done()
    runtime.request_stop("review-finished")
    await task
    return {
        "collectors_continue_after_heartbeat_failure": continued,
        "shutdown_status": runtime.state_store.read()["status"],
    }


def sort_probe(root):
    path = root / "candidates.ndjson"
    rows = [
        {
            "semantic_key_sha256": str(i),
            "logical_record_sha256": str(i),
            "provenance": {
                "receive_time_utc_ns": i,
                "collector_instance_id": "c",
                "connection_id": "c",
                "source_chunk_sha256": "0" * 64,
                "source_record_ordinal": i,
                "source_subrecord_ordinal": 0,
            },
        }
        for i in range(40)
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    opened = peak = 0
    original = Path.open

    # Count concurrent input sort-run readers, using one row/run to expose scaling cheaply.
    class Reader:
        def __init__(self, handle):
            self.handle = handle

        def __enter__(self):
            return self.handle

        def __exit__(self, *args):
            nonlocal opened
            opened -= 1
            self.handle.close()

    def counted(path, *args, **kwargs):
        nonlocal opened, peak
        handle = original(path, *args, **kwargs)
        if path.parent.name == "runs" and args and args[0] == "rb":
            opened += 1
            peak = max(peak, opened)
            return Reader(handle)
        return handle

    with (
        patch("binance_market_data_recorder.normalize.pipeline.SORT_ROWS_PER_RUN", 1),
        patch.object(Path, "open", counted),
    ):
        list(_external_sort(path, root / "runs"))
    return {"run_count": 40, "peak_open_run_readers": peak}


def main():
    with tempfile.TemporaryDirectory(prefix="recorder-review-") as tmp:
        root = Path(tmp).resolve()
        normalization_root, sort_root = root / "normalize", root / "sort"
        normalization_root.mkdir()
        sort_root.mkdir()
        book = LocalBookReconstructor("spot")
        update = DepthUpdate("spot", "BTCUSDT", 2, 2, None, (), (), 1)
        book.offer(update)
        book.synchronize(BookSnapshot("spot", "BTCUSDT", 1, (("1", "1"),), (("2", "1"),)))
        for _ in range(10000):
            book.offer(update)
        cwd_error = False
        try:
            with patch.object(Path, "exists", side_effect=PermissionError("unreadable cwd")):
                discover_repository_root(root)
        except PermissionError:
            cwd_error = True
        print(
            json.dumps(
                {
                    "delta_single_page": delta_probe(root / "delta"),
                    "cross_product_normalization": normalization_probe(normalization_root),
                    "heartbeat": asyncio.run(heartbeat_probe(root / "service")),
                    "sort_fan_in": sort_probe(sort_root),
                    "retained_quality_audits_after_10000_duplicates": len(book.audits),
                    "repository_discovery_propagates_permission_error": cwd_error,
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
