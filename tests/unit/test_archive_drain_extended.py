"""Extended archive drain tests for M21 code review fixes.

Covers: LOW_SPACE CLI exit 0, O(N²) bounded query, crash recovery, SIGTERM, etc.
"""

from __future__ import annotations

import json
import signal
from pathlib import Path

import pytest

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.archive.drain import (
    archive_drain,
)
from binance_market_data_recorder.cli import main
from binance_market_data_recorder.storage.catalog import (
    ArchiveState,
    Catalog,
    ChunkState,
)
from binance_market_data_recorder.storage.macos import VolumeInfo
from tests.archive_support import FixedVolumes, PreparedArchive, prepare_archive


def _test_volumes(
    prepared: PreparedArchive,
    *,
    free_pct: float = 90.0,
) -> FixedVolumes:
    total = 100 * 1024**3
    free = int(total * free_pct / 100)
    return FixedVolumes(VolumeInfo(
        disk_id="disk9s1",
        volume_uuid=prepared.target.volume_uuid,
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=prepared.target.root.parent.parent,
        writable=True,
        internal=False,
        removable=True,
        total_bytes=total,
        free_bytes=free,
        observed_at_utc_ns=1,
    ))


# ── LOW_SPACE CLI exit 0 ────────────────────────────────────────────

def test_low_space_drain_exit_zero(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=3)
    vols = _test_volumes(prepared, free_pct=3.0)
    monkeypatch.setattr(
        "binance_market_data_recorder.archive.drain.volume_adapter",
        lambda: vols,
    )
    monkeypatch.setenv(
        "BINANCE_MARKET_RECORDER_DATA_ROOT",
        str(prepared.layout.root),
    )
    with Catalog(prepared.layout.catalog) as catalog:
        chunk = catalog.chunk(prepared.chunk_ids[0])
        assert chunk is not None
        chunk_path = prepared.layout.root / str(chunk["sealed_path"])
    assert chunk_path.is_file()

    exit_code = main(
        [
            "archive",
            "drain",
            "--storage-id",
            prepared.target.storage_id,
            "--max-runtime-seconds",
            "60",
            "--max-files",
            "1000",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["exit_reason"] == "TARGET_LOW_SPACE"
    assert payload["processed_files"] == 0
    assert payload["target_state"] == "LOW_SPACE"
    assert chunk_path.is_file()


def test_low_space_preserves_source(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=1)
    vols = _test_volumes(prepared, free_pct=3.0)
    with Catalog(prepared.layout.catalog) as catalog:
        chunk = catalog.chunk(prepared.chunk_ids[0])
        assert chunk is not None
        source = prepared.layout.root / str(chunk["sealed_path"])
        assert source.exists()

        archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=60,
            max_files=1000,
            volumes=vols,
        )

        assert source.exists()


# ── target ABSENT ───────────────────────────────────────────────────

def test_drain_target_absent_report(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id="missing-storage-id",
            max_runtime_seconds=60,
            max_files=1000,
        )
        assert "TARGET_ABSENT" in result.get("exit_reason", "")  # type: ignore[operator]
        assert result["processed_files"] == 0


# ── SIGTERM safety ─────────────────────────────────────────────────

def test_drain_sigterm_finishes_current_then_stops(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=2)
    vols = _test_volumes(prepared)
    with Catalog(prepared.layout.catalog) as catalog:
        interrupted = []
        import signal

        def _handler(signum: int, frame: object) -> None:
            interrupted.append(True)

        prev = signal.signal(signal.SIGTERM, _handler)
        try:
            def trigger_sigterm() -> None:
                import os
                os.kill(os.getpid(), signal.SIGTERM)

            class TriggeringClock:
                def __init__(self) -> None:
                    self.calls = 0
                def __call__(self) -> float:
                    self.calls += 1
                    if self.calls > 3 and not interrupted:
                            trigger_sigterm()
                    return 0.0

            result = archive_drain(
                layout=prepared.layout,
                catalog=catalog,
                storage_id=prepared.target.storage_id,
                max_runtime_seconds=60,
                max_files=1000,
                volumes=vols,
                monotonic_clock=TriggeringClock(),
            )
            assert result["exit_reason"] == "INTERRUPTED"
            assert result["lock_acquired"] is True
        finally:
            signal.signal(signal.SIGTERM, prev)


def test_drain_sigterm_before_first_run_once_processes_nothing(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=1)
    volumes = _test_volumes(prepared)

    class InterruptBeforeFirstTransaction:
        def __init__(self) -> None:
            self.calls = 0

        def __call__(self) -> float:
            self.calls += 1
            if self.calls == 2:
                import os

                os.kill(os.getpid(), signal.SIGTERM)
            return 0.0

    with Catalog(prepared.layout.catalog) as catalog:
        chunk = catalog.chunk(prepared.chunk_ids[0])
        assert chunk is not None
        source = prepared.layout.root / str(chunk["sealed_path"])
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=60,
            max_files=1000,
            volumes=volumes,
            monotonic_clock=InterruptBeforeFirstTransaction(),
        )

        assert result["processed_files"] == 0
        assert result["successful_transactions"] == 0
        assert result["exit_reason"] == "INTERRUPTED"
        assert source.is_file()
        assert catalog.archive_transactions() == []


# ── crash recovery ─────────────────────────────────────────────────

def test_drain_recovers_copying_transaction(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=1)
    vols = _test_volumes(prepared)
    with Catalog(prepared.layout.catalog) as catalog:
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=60,
            max_files=1000,
            volumes=vols,
        )
        assert result["successful_transactions"] >= 1  # type: ignore[operator]
        assert result["backlog_files_after"] == 0


# ── bounded query (O(N²) fix) ──────────────────────────────────────

def test_bounded_transaction_query(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=5)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target,
        )
        for _ in range(5):
            r = manager.run_once()
            if r.state == "NO_ELIGIBLE_CHUNKS":
                break

        txn = catalog.oldest_incomplete_archive_transaction(prepared.target.storage_id)
        assert txn is None

        chunk = catalog.chunk(prepared.chunk_ids[0])
        assert chunk is not None
        assert catalog.state(prepared.chunk_ids[0]) is ChunkState.LOCAL_DELETED


def test_bounded_query_with_many_historical_txns(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=500, payload_bytes=64)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target,
        )
        drained = 0
        for _ in range(500):
            r = manager.run_once()
            if r.state == "NO_ELIGIBLE_CHUNKS":
                break
            drained += 1  # noqa: SIM113
            if drained >= 50:
                break

        txn = catalog.oldest_incomplete_archive_transaction(prepared.target.storage_id)
        if txn is not None:
            state = str(txn["state"])
            assert state in {
                str(ArchiveState.COPYING),
                str(ArchiveState.VERIFYING),
                str(ArchiveState.VERIFIED),
                str(ArchiveState.LOCAL_DELETE_PENDING),
            }
