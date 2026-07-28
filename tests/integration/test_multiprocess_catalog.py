"""Certified Catalog boundary: Recorder + Archive writers and a Soak observer."""

from __future__ import annotations

import json
import multiprocessing
import time
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
SOAK_SAMPLES = 40


def _replace_attribute(target: object, name: str, value: object) -> None:
    setattr(target, name, value)


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
    started_at_monotonic_ns = time.monotonic_ns()
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
        json.dumps(
            {
                "created_and_sealed": created,
                "started_at_monotonic_ns": started_at_monotonic_ns,
                "finished_at_monotonic_ns": time.monotonic_ns(),
            }
        ),
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
    started_at_monotonic_ns = time.monotonic_ns()
    with Catalog(Path(catalog_path)) as catalog:
        result = archive_drain(
            layout=layout,
            catalog=catalog,
            storage_id=storage_id,
            max_runtime_seconds=120,
            max_files=1000,
            volumes=volumes,
        )
    result["started_at_monotonic_ns"] = started_at_monotonic_ns
    result["finished_at_monotonic_ns"] = time.monotonic_ns()
    Path(result_path).write_text(json.dumps(result), encoding="utf-8")


def _soak_worker(
    catalog_path: str,
    layout_root: str,
    storage_id: str,
    volume_uuid: str,
    mountpoint: str,
    start_barrier: Any,
    output_path: str,
    result_path: str,
) -> None:
    import contextlib

    import binance_market_data_recorder.soak.sample as sample_module
    import binance_market_data_recorder.storage.macos.registry as registry_module
    from binance_market_data_recorder.soak.sample import soak_sample
    from binance_market_data_recorder.storage.catalog import Catalog
    from binance_market_data_recorder.storage.macos import VolumeInfo
    from tests.archive_support import FixedVolumes

    statements: list[str] = []
    original_init = Catalog.__init__

    def traced_init(
        self: Catalog,
        path: Path,
        *,
        read_only: bool = False,
    ) -> None:
        if not read_only:
            raise AssertionError("Soak opened a writable Catalog")
        original_init(self, path, read_only=read_only)
        self._connection.set_trace_callback(statements.append)

    @contextlib.contextmanager
    def forbidden_transaction(_catalog: Catalog) -> Any:
        raise AssertionError("Soak attempted a Catalog write transaction")
        yield

    def forbidden_probe(_folder: Path) -> dict[str, object]:
        raise AssertionError("Soak attempted an external-directory probe")

    def forbidden_activate(
        _catalog: Catalog,
        _storage_id: str,
        *,
        occurred_at_utc_ns: int,
    ) -> None:
        raise AssertionError(
            f"Soak attempted storage activation at {occurred_at_utc_ns}"
        )

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
    _replace_attribute(Catalog, "__init__", traced_init)
    _replace_attribute(Catalog, "_transaction", forbidden_transaction)
    _replace_attribute(Catalog, "activate_storage_target", forbidden_activate)
    _replace_attribute(registry_module, "probe_directory", forbidden_probe)
    _replace_attribute(sample_module, "volume_adapter", lambda: volumes)
    _replace_attribute(
        sample_module,
        "_systemd_status",
        lambda: {
            "recorder_active_state": "inactive",
            "recorder_sub_state": "dead",
            "recorder_main_pid": None,
            "recorder_nrestarts": 0,
            "recorder_active_enter_timestamp_monotonic": None,
            "recorder_service_result": "success",
            "recorder_error": None,
            "archive_timer_active_state": "inactive",
            "archive_service_result": None,
        },
    )

    start_barrier.wait(timeout=30)
    started_at_monotonic_ns = time.monotonic_ns()
    samples: list[dict[str, object]] = []
    for _ in range(SOAK_SAMPLES):
        samples.append(
            soak_sample(
                data_root=Path(layout_root),
                output_path=Path(output_path),
                storage_id=storage_id,
                config_dict={"data_root": layout_root},
                recorder_version="0.1.0+test",
            )
        )
        time.sleep(0.025)
    lines = Path(output_path).read_text(encoding="utf-8").splitlines()
    parsed = [json.loads(line) for line in lines]
    Path(result_path).write_text(
        json.dumps(
            {
                "samples": len(samples),
                "valid_jsonl_lines": len(parsed),
                "all_archive_evidence_ok": all(
                    sample["archive"]["archive_evidence_status"] == "OK"  # type: ignore[index]
                    for sample in samples
                ),
                "all_external_evidence_ok": all(
                    sample["disk"]["external_evidence_status"] == "OK"  # type: ignore[index]
                    for sample in samples
                ),
                "begin_immediate_seen": any(
                    statement.lstrip().upper().startswith("BEGIN IMMEDIATE")
                    for statement in statements
                ),
                "write_statement_seen": any(
                    statement.lstrip().upper().startswith(
                        ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER")
                    )
                    for statement in statements
                ),
                "started_at_monotonic_ns": started_at_monotonic_ns,
                "finished_at_monotonic_ns": time.monotonic_ns(),
            }
        ),
        encoding="utf-8",
    )


def test_multiprocess_recorder_archive_and_soak_observer(tmp_path: Path) -> None:
    """Two writers progress while an independently opened observer only reads."""

    prepared = prepare_archive(
        tmp_path,
        chunk_count=INITIAL_BACKLOG,
        payload_bytes=32,
    )
    context = multiprocessing.get_context("spawn")
    start_barrier = context.Barrier(4)
    writer_result_path = tmp_path / "writer-result.json"
    drain_result_path = tmp_path / "drain-result.json"
    soak_output_path = tmp_path / "soak-samples.jsonl"
    soak_result_path = tmp_path / "soak-result.json"

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
    soak_proc = context.Process(
        name="certified-soak-observer",
        target=_soak_worker,
        args=(
            str(prepared.layout.catalog),
            str(prepared.layout.root),
            prepared.target.storage_id,
            prepared.target.volume_uuid,
            str(prepared.target.root.parents[1]),
            start_barrier,
            str(soak_output_path),
            str(soak_result_path),
        ),
    )
    writer_proc.start()
    archive_proc.start()
    soak_proc.start()
    start_barrier.wait(timeout=30)

    writer_proc.join(timeout=120)
    archive_proc.join(timeout=150)
    soak_proc.join(timeout=120)
    try:
        assert not writer_proc.is_alive(), "Recorder writer did not terminate"
        assert not archive_proc.is_alive(), "Archive writer did not terminate"
        assert not soak_proc.is_alive(), "Soak observer did not terminate"
        assert writer_proc.exitcode == 0
        assert archive_proc.exitcode == 0
        assert soak_proc.exitcode == 0

        writer_result = json.loads(writer_result_path.read_text(encoding="utf-8"))
        drain_result = json.loads(drain_result_path.read_text(encoding="utf-8"))
        soak_result = json.loads(soak_result_path.read_text(encoding="utf-8"))
        assert writer_result["created_and_sealed"] == WRITER_CHUNKS
        assert drain_result["processed_files"] > 0
        assert drain_result["successful_transactions"] > 0
        assert drain_result["failed_transactions"] == 0
        assert drain_result.get("error_type") is None
        assert soak_result["samples"] == SOAK_SAMPLES
        assert soak_result["valid_jsonl_lines"] == SOAK_SAMPLES
        assert soak_result["all_archive_evidence_ok"] is True
        assert soak_result["all_external_evidence_ok"] is True
        assert soak_result["begin_immediate_seen"] is False
        assert soak_result["write_statement_seen"] is False
        assert max(
            writer_result["started_at_monotonic_ns"],
            drain_result["started_at_monotonic_ns"],
            soak_result["started_at_monotonic_ns"],
        ) < min(
            writer_result["finished_at_monotonic_ns"],
            drain_result["finished_at_monotonic_ns"],
            soak_result["finished_at_monotonic_ns"],
        )
        print(
            "multiprocess archive "
            f"processed_files={drain_result['processed_files']} "
            f"successful_transactions={drain_result['successful_transactions']} "
            f"soak_samples={soak_result['samples']}"
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
        assert not any(
            path.name.startswith(".binance-market-data-recorder-probe-")
            for path in prepared.target.root.rglob("*")
        )
    finally:
        if writer_proc.is_alive():
            writer_proc.terminate()
            writer_proc.join(timeout=5)
        if archive_proc.is_alive():
            archive_proc.terminate()
            archive_proc.join(timeout=5)
        if soak_proc.is_alive():
            soak_proc.terminate()
            soak_proc.join(timeout=5)
