"""Certified Catalog boundary: one Recorder writer plus one Archive writer."""

from __future__ import annotations

import json
import multiprocessing
from pathlib import Path
from typing import Any

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.storage.catalog import (
    ArchiveState,
    Catalog,
    ChunkState,
)
from tests.archive_support import prepare_archive

INITIAL_BACKLOG = 50
WRITER_CHUNKS = 200


def _writer_worker(
    catalog_path: str,
    layout_root: str,
    iteration_count: int,
    start_barrier: Any,
    result_path: str,
) -> None:
    from binance_market_data_recorder.spool.seal import seal_partial
    from binance_market_data_recorder.spool.writer import RawChunkWriter
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout
    from tests.factories import event

    layout = ensure_storage_layout(Path(layout_root))
    created = 0
    start_barrier.wait(timeout=30)
    with Catalog(Path(catalog_path)) as catalog:
        for ordinal in range(iteration_count):
            writer = RawChunkWriter(
                layout=layout,
                catalog=catalog,
                market="spot",
                symbol="BTCUSDT",
                stream="diff_depth",
                collector_instance_id="multiprocess-recorder",
                collector_version="0.1.0+test",
                durability_interval_seconds=0,
                created_at_utc_ns=1_800_000_000_000_000_000 + ordinal,
            )
            writer.append(event(10_000 + ordinal))
            writer.close()
            seal_partial(writer.path, layout=layout, catalog=catalog)
            created += 1
    Path(result_path).write_text(
        json.dumps({"created_and_sealed": created}),
        encoding="utf-8",
    )


def _drain_worker(
    catalog_path: str,
    layout_root: str,
    storage_id: str,
    volume_uuid: str,
    mountpoint: str,
    start_barrier: Any,
    result_path: str,
) -> None:
    from binance_market_data_recorder.archive.drain import archive_drain
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.layout import ensure_storage_layout
    from binance_market_data_recorder.storage.macos import VolumeInfo
    from tests.archive_support import FixedVolumes

    layout = ensure_storage_layout(Path(layout_root))
    volumes = FixedVolumes(
        VolumeInfo(
            disk_id="multiprocess-test-volume",
            volume_uuid=volume_uuid,
            name="Multiprocess Test Archive",
            filesystem_type="testfs",
            mountpoint=Path(mountpoint),
            writable=True,
            internal=False,
            removable=True,
            total_bytes=100 * 1024**3,
            free_bytes=90 * 1024**3,
            observed_at_utc_ns=1,
        )
    )
    start_barrier.wait(timeout=30)
    with Catalog(Path(catalog_path)) as catalog:
        result = archive_drain(
            layout=layout,
            catalog=catalog,
            storage_id=storage_id,
            max_runtime_seconds=120,
            max_files=1000,
            volumes=volumes,
        )
    Path(result_path).write_text(json.dumps(result), encoding="utf-8")


def test_multiprocess_recorder_and_archive_drain(tmp_path: Path) -> None:
    """Both independently opened Catalog writers make real forward progress."""

    prepared = prepare_archive(
        tmp_path,
        chunk_count=INITIAL_BACKLOG,
        payload_bytes=32,
    )
    context = multiprocessing.get_context("spawn")
    start_barrier = context.Barrier(3)
    writer_result_path = tmp_path / "writer-result.json"
    drain_result_path = tmp_path / "drain-result.json"

    writer_proc = context.Process(
        name="certified-recorder-writer",
        target=_writer_worker,
        args=(
            str(prepared.layout.catalog),
            str(prepared.layout.root),
            WRITER_CHUNKS,
            start_barrier,
            str(writer_result_path),
        ),
    )
    archive_proc = context.Process(
        name="certified-archive-writer",
        target=_drain_worker,
        args=(
            str(prepared.layout.catalog),
            str(prepared.layout.root),
            prepared.target.storage_id,
            prepared.target.volume_uuid,
            str(prepared.target.root.parents[1]),
            start_barrier,
            str(drain_result_path),
        ),
    )
    writer_proc.start()
    archive_proc.start()
    start_barrier.wait(timeout=30)

    writer_proc.join(timeout=120)
    archive_proc.join(timeout=150)
    try:
        assert not writer_proc.is_alive(), "Recorder writer did not terminate"
        assert not archive_proc.is_alive(), "Archive writer did not terminate"
        assert writer_proc.exitcode == 0
        assert archive_proc.exitcode == 0

        writer_result = json.loads(writer_result_path.read_text(encoding="utf-8"))
        drain_result = json.loads(drain_result_path.read_text(encoding="utf-8"))
        assert writer_result["created_and_sealed"] == WRITER_CHUNKS
        assert drain_result["processed_files"] > 0
        assert drain_result["successful_transactions"] > 0
        assert drain_result["failed_transactions"] == 0
        assert drain_result.get("error_type") is None
        print(
            "multiprocess archive "
            f"processed_files={drain_result['processed_files']} "
            f"successful_transactions={drain_result['successful_transactions']}"
        )

        with Catalog(prepared.layout.catalog) as catalog:
            state_rows = catalog._connection.execute(
                "SELECT state, COUNT(*) AS count FROM chunks GROUP BY state"
            ).fetchall()
            state_counts = {
                str(row["state"]): int(row["count"])
                for row in state_rows
            }
            assert sum(state_counts.values()) == INITIAL_BACKLOG + WRITER_CHUNKS
            assert state_counts.get(str(ChunkState.LOCAL_DELETED), 0) > 0
            assert set(state_counts) <= {
                str(ChunkState.SEALED),
                str(ChunkState.ARCHIVE_COPYING),
                str(ChunkState.ARCHIVE_VERIFYING),
                str(ChunkState.ARCHIVED_VERIFIED),
                str(ChunkState.LOCAL_DELETE_PENDING),
                str(ChunkState.LOCAL_DELETED),
            }

            transactions = catalog.archive_transactions()
            transaction_chunk_ids = [
                str(transaction["chunk_id"])
                for transaction in transactions
            ]
            assert len(transaction_chunk_ids) == len(set(transaction_chunk_ids))
            incomplete_states = {
                str(ArchiveState.COPYING),
                str(ArchiveState.VERIFYING),
                str(ArchiveState.VERIFIED),
                str(ArchiveState.LOCAL_DELETE_PENDING),
            }
            assert {
                str(transaction["state"])
                for transaction in transactions
                if str(transaction["state"]) != str(ArchiveState.LOCAL_DELETED)
            } <= incomplete_states

            integrity = catalog._connection.execute(
                "PRAGMA integrity_check"
            ).fetchone()
            assert integrity is not None
            assert integrity[0] == "ok"

            verification = ArchiveManager(
                layout=prepared.layout,
                catalog=catalog,
                target=prepared.target,
            ).verify_all()
            assert verification["status"] == "VERIFIED"
            assert verification["verified_files"] > 0  # type: ignore[operator]
            assert verification["failed_files"] == 0
    finally:
        if writer_proc.is_alive():
            writer_proc.terminate()
            writer_proc.join(timeout=5)
        if archive_proc.is_alive():
            archive_proc.terminate()
            archive_proc.join(timeout=5)
