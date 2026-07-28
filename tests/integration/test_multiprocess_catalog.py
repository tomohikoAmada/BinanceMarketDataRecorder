"""Integration test: Recorder writer + Archive Drain share one SQLite Catalog."""

from __future__ import annotations

import json
import multiprocessing
import time
from pathlib import Path

from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import prepare_archive


def _writer_worker(
    catalog_path: str,
    layout_root: str,
    iteration_count: int,
    ready_event: str,
    done_event: str,
) -> None:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

    from binance_market_data_recorder.spool.seal import seal_partial
    from binance_market_data_recorder.spool.writer import RawChunkWriter
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout
    from tests.factories import event

    layout = ensure_storage_layout(Path(layout_root))
    with Catalog(Path(catalog_path)) as catalog:
        # Let parent know we're ready
        with open(ready_event, "w") as f:
            f.write("ready")
        # Wait for parent to start draining
        while not Path(done_event).exists():
            time.sleep(0.05)

        for i in range(iteration_count):
            writer = RawChunkWriter(
                layout=layout,
                catalog=catalog,
                market="spot",
                symbol="BTCUSDT",
                stream="diff_depth",
                collector_instance_id="mp-test",
                collector_version="0.1.0+test",
                durability_interval_seconds=0,
                created_at_utc_ns=1_700_000_000_000_000_000 + i,
            )
            writer.append(event(i + 1))
            writer.close()
            seal_partial(writer.path, layout=layout, catalog=catalog)


def _drain_worker(
    catalog_path: str,
    layout_root: str,
    storage_id: str,
    result_path: str,
) -> None:
    import json
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

    from binance_market_data_recorder.archive.drain import archive_drain
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout

    layout = ensure_storage_layout(Path(layout_root))
    with Catalog(Path(catalog_path)) as catalog:
        result = archive_drain(
            layout=layout,
            catalog=catalog,
            storage_id=storage_id,
            max_runtime_seconds=60,
            max_files=1000,
        )
    Path(result_path).write_text(json.dumps(result))


def test_multiprocess_recorder_and_archive_drain(tmp_path: Path) -> None:
    """Recorder (writer) and Archive Drain share the same Catalog concurrently."""

    prepared = prepare_archive(tmp_path, chunk_count=0)
    ready_file = str(tmp_path / "ready.txt")
    done_file = str(tmp_path / "done.txt")

    iteration_count = 200

    writer_proc = multiprocessing.Process(
        target=_writer_worker,
        args=(
            str(prepared.layout.catalog),
            str(prepared.layout.root),
            iteration_count,
            ready_file,
            done_file,
        ),
    )
    writer_proc.start()

    try:
        # Wait for writer to signal readiness
        timeout = 30
        start = time.time()
        while not Path(ready_file).exists():
            if time.time() - start > timeout:
                raise TimeoutError("writer process did not start in time")
            time.sleep(0.1)

        # Signal writer to start producing chunks
        Path(done_file).write_text("go")

        # Run drain concurrently while writer is still writing
        drain_result_path = tmp_path / "drain_result.json"
        _drain_worker(
            str(prepared.layout.catalog),
            str(prepared.layout.root),
            prepared.target.storage_id,
            str(drain_result_path),
        )
        drain_result = json.loads(Path(drain_result_path).read_text())

        writer_proc.join(timeout=60)
        if writer_proc.is_alive():
            writer_proc.terminate()
            writer_proc.join(timeout=5)

        assert writer_proc.exitcode == 0, f"Writer failed with exitcode {writer_proc.exitcode}"

        assert drain_result["lock_acquired"] is True
        assert drain_result.get("error_type") is None
        assert drain_result["failed_transactions"] == 0

        # Verify Catolog integrity
        with Catalog(prepared.layout.catalog) as catalog:
            state_counts: dict[str, int] = {}
            for chunk in catalog.chunks_in_states():
                s = str(chunk["state"])
                state_counts[s] = state_counts.get(s, 0) + 1

            # All chunks should be in a valid final state
            for state in state_counts:
                assert state in {
                    "SEALED", "ARCHIVE_COPYING", "ARCHIVE_VERIFYING",
                    "ARCHIVED_VERIFIED", "LOCAL_DELETE_PENDING", "LOCAL_DELETED",
                }, f"Unexpected chunk state: {state}"

            # No duplicate states
            transactions = catalog.archive_transactions()
            chunk_ids_from_txn = {t["chunk_id"] for t in transactions}
            assert len(transactions) == len(chunk_ids_from_txn), (
                "Duplicate archive transactions found"
            )

            # Verify SQLite integrity
            cursor = catalog._connection.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            assert result[0] == "ok", f"Integrity check failed: {result[0]}"

    finally:
        if writer_proc.is_alive():
            writer_proc.terminate()
            writer_proc.join(timeout=5)
