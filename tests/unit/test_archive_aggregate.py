"""Tests for Catalog archive_aggregate bounded query."""

from __future__ import annotations

from pathlib import Path

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import prepare_archive


def test_archive_aggregate_local_deleted_preserves_verified_at(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=3)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target,
        )
        for _ in range(3):
            r = manager.run_once()
            if r.state == "NO_ELIGIBLE_CHUNKS":
                break

        agg = catalog.archive_aggregate(prepared.target.storage_id)
        assert agg["local_deleted_files"] >= 1  # type: ignore[operator]
        assert agg["local_deleted_bytes"] > 0  # type: ignore[operator]
        assert agg["last_verified_at_utc_ns"] is not None


def test_archive_aggregate_verified_pending_counts(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=1)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target,
        )
        manager.run_once()

        agg = catalog.archive_aggregate(prepared.target.storage_id)
        assert agg["external_verified_files"] >= 1  # type: ignore[operator]
        assert agg["external_verified_bytes"] > 0  # type: ignore[operator]


def test_archive_aggregate_no_transactions(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=0)
    with Catalog(prepared.layout.catalog) as catalog:
        agg = catalog.archive_aggregate(prepared.target.storage_id)
        assert agg["external_verified_files"] == 0
        assert agg["external_verified_bytes"] == 0
        assert agg["backlog_files"] == 0
        assert agg["last_verified_at_utc_ns"] is None
        assert agg["latest_error_type"] is None


def test_archive_aggregate_latest_error_by_updated_at(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=2)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target,
        )
        manager.run_once()  # creates at least one transaction
        transactions = catalog.archive_transactions()
        assert len(transactions) >= 1
        tid = str(transactions[-1]["transaction_id"])
        catalog.record_archive_error(tid, "ArchiveError: DISAPPEARED_DURING_COPY")
        agg = catalog.archive_aggregate(prepared.target.storage_id)
        assert agg["latest_error_type"] == "DISAPPEARED_DURING_COPY"


def test_archive_aggregate_no_full_row_list(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=50, payload_bytes=16)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target,
        )
        drained = 0
        for _ in range(100):
            r = manager.run_once()
            if r.state == "NO_ELIGIBLE_CHUNKS":
                break
            drained += 1  # noqa: SIM113

        # The aggregation must be O(rows) not O(rows^2)
        agg = catalog.archive_aggregate(prepared.target.storage_id)
        assert isinstance(agg["transactions_by_state"], dict)
        assert isinstance(agg["external_verified_files"], int)
        assert isinstance(agg["local_deleted_files"], int)
        assert "raw_error_text" not in str(agg)
