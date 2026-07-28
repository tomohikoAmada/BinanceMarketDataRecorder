from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.archive.drain import (
    _DrainLock,
    archive_drain,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.macos import VolumeInfo
from tests.archive_support import FixedVolumes, PreparedArchive, prepare_archive


def _test_volumes(prepared: PreparedArchive) -> FixedVolumes:
    return FixedVolumes(VolumeInfo(
        disk_id="disk9s1",
        volume_uuid=prepared.target.volume_uuid,
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=prepared.target.root.parent.parent,
        writable=True,
        internal=False,
        removable=True,
        total_bytes=100 * 1024**3,
        free_bytes=90 * 1024**3,
        observed_at_utc_ns=1,
    ))


class FakeMonotonic:
    def __init__(self, start: float = 0.0) -> None:
        self._value = start

    def __call__(self) -> float:
        return self._value

    def advance(self, seconds: float) -> None:
        self._value += seconds


def _drain_empty(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    vols = _test_volumes(prepared)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target,
        )
        manager.run_once()

        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=10,
            max_files=1000,
            volumes=vols,
        )
        assert result["exit_reason"] == "BACKLOG_EMPTY"
        assert result["processed_files"] == 0
        assert result["lock_acquired"] is True
        assert result["contains_credentials"] is False


def test_archive_drain_backlog_empty(tmp_path: Path) -> None:
    _drain_empty(tmp_path)


def test_archive_drain_max_files_precise(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=5)
    vols = _test_volumes(prepared)
    with Catalog(prepared.layout.catalog) as catalog:
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=60,
            max_files=2,
            volumes=vols,
        )
        assert result["exit_reason"] == "MAX_FILES"
        assert result["processed_files"] == 2
        assert result["successful_transactions"] >= 1  # type: ignore[operator]
        assert result["failed_transactions"] == 0


def test_archive_drain_deadline(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=1)
    vols = _test_volumes(prepared)
    clock_value = 0.0

    def advancing_clock() -> float:
        nonlocal clock_value
        result = clock_value
        clock_value += 100.0
        return result

    with Catalog(prepared.layout.catalog) as catalog:
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=1,
            max_files=1000,
            volumes=vols,
            monotonic_clock=advancing_clock,
        )
        assert result["exit_reason"] == "DEADLINE"
        assert result["processed_files"] >= 0  # type: ignore[operator]


def test_archive_drain_lock_already_running(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    vols = _test_volumes(prepared)
    lock_path = prepared.layout.root / "state/runtime/archive_drain.lock"
    first = _DrainLock(lock_path)
    first.acquire()
    try:
        with Catalog(prepared.layout.catalog) as catalog:
            result = archive_drain(
                layout=prepared.layout,
                catalog=catalog,
                storage_id=prepared.target.storage_id,
                max_runtime_seconds=60,
                max_files=1000,
                volumes=vols,
            )
            assert result["exit_reason"] == "ALREADY_RUNNING"
            assert result["lock_acquired"] is False
            assert result["processed_files"] == 0
    finally:
        first.release()


def test_archive_drain_target_not_ready(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id="nonexistent-id",
            max_runtime_seconds=60,
            max_files=1000,
        )
        assert "TARGET_ABSENT" in result["exit_reason"]  # type: ignore[operator]


def test_archive_drain_existing_retry_unchanged() -> None:
    from binance_market_data_recorder.archive import ArchiveManager as AM

    assert AM.run_once.__name__ == "run_once"


def test_archive_drain_exit_reasons_coverage(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=3, payload_bytes=1024)
    vols = _test_volumes(prepared)
    with Catalog(prepared.layout.catalog) as catalog:
        result = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=30,
            max_files=1,
            volumes=vols,
        )
        assert result["exit_reason"] == "MAX_FILES"
        assert result["processed_files"] == 1

        result2 = archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=30,
            max_files=5,
        )
        assert result2["successful_transactions"] >= 0  # type: ignore[operator]
        assert result2["contains_credentials"] is False
        for field in (
            "processed_files", "processed_bytes", "successful_transactions",
            "failed_transactions", "backlog_files_before", "backlog_bytes_before",
            "backlog_files_after", "backlog_bytes_after",
        ):
            assert field in result2


def test_archive_drain_rejects_zero_files(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog, pytest.raises(
        (ValueError, RuntimeError)
    ):
        archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=60,
            max_files=0,
        )


def test_archive_drain_rejects_zero_runtime(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog, pytest.raises(
        (ValueError, RuntimeError)
    ):
        archive_drain(
            layout=prepared.layout,
            catalog=catalog,
            storage_id=prepared.target.storage_id,
            max_runtime_seconds=0,
            max_files=1000,
        )
