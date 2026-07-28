"""Tests for bounded, target-aware Catalog archive aggregation."""

from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.archive import (
    ArchiveManager,
    ArchiveTarget,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from tests.archive_support import prepare_archive


def _stored_bytes(catalog: Catalog, chunk_ids: tuple[str, ...]) -> int:
    total = 0
    for chunk_id in chunk_ids:
        row = catalog.chunk(chunk_id)
        assert row is not None
        value = row["stored_bytes"]
        assert isinstance(value, int)
        total += value
    return total


def _leave_copying(
    *,
    prepared_target: ArchiveTarget,
    catalog: Catalog,
    layout: StorageLayout,
) -> None:
    def stop_after_reservation(stage: str, _path: Path | None) -> None:
        if stage == "attempt_started":
            raise RuntimeError("deterministic test stop")

    manager = ArchiveManager(
        layout=layout,
        catalog=catalog,
        target=prepared_target,
        fault_hook=stop_after_reservation,
    )
    with pytest.raises(RuntimeError, match="deterministic test stop"):
        manager.run_once()


def test_archive_aggregate_counts_100_global_unassigned_sealed(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=100, payload_bytes=16)
    with Catalog(prepared.layout.catalog) as catalog:
        expected_bytes = _stored_bytes(catalog, prepared.chunk_ids)
        aggregate = catalog.archive_aggregate(prepared.target.storage_id)

    assert aggregate["unassigned_sealed_scope"] == "GLOBAL"
    assert aggregate["unassigned_sealed_files"] == 100
    assert aggregate["unassigned_sealed_bytes"] == expected_bytes
    assert aggregate["target_inflight_files"] == 0
    assert aggregate["target_inflight_bytes"] == 0
    assert aggregate["backlog_files"] == 100
    assert aggregate["backlog_bytes"] == expected_bytes


def test_archive_aggregate_mixed_unassigned_and_target_inflight(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=3, payload_bytes=32)
    with Catalog(prepared.layout.catalog) as catalog:
        _leave_copying(
            prepared_target=prepared.target,
            catalog=catalog,
            layout=prepared.layout,
        )
        transaction = catalog.oldest_incomplete_archive_transaction(
            prepared.target.storage_id
        )
        assert transaction is not None
        inflight_bytes = transaction["stored_bytes"]
        assert isinstance(inflight_bytes, int)
        total_bytes = _stored_bytes(catalog, prepared.chunk_ids)

        aggregate = catalog.archive_aggregate(prepared.target.storage_id)

    assert aggregate["unassigned_sealed_files"] == 2
    assert aggregate["unassigned_sealed_bytes"] == total_bytes - inflight_bytes
    assert aggregate["target_inflight_files"] == 1
    assert aggregate["target_inflight_bytes"] == inflight_bytes
    assert aggregate["backlog_files"] == 3
    assert aggregate["backlog_bytes"] == total_bytes


def test_archive_aggregate_local_deleted_is_not_backlog(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=1)
    with Catalog(prepared.layout.catalog) as catalog:
        result = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        ).run_once()
        assert result.state == "LOCAL_DELETED"

        aggregate = catalog.archive_aggregate(prepared.target.storage_id)

    assert aggregate["unassigned_sealed_files"] == 0
    assert aggregate["target_inflight_files"] == 0
    assert aggregate["backlog_files"] == 0
    assert aggregate["backlog_bytes"] == 0
    assert aggregate["local_deleted_files"] == 1
    assert aggregate["local_deleted_bytes"] > 0  # type: ignore[operator]
    assert aggregate["last_verified_at_utc_ns"] is not None


def test_archive_aggregate_never_calls_chunks_in_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=5)
    with Catalog(prepared.layout.catalog) as catalog:
        def forbidden(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("archive_aggregate loaded Chunk rows")

        monkeypatch.setattr(catalog, "chunks_in_states", forbidden)
        aggregate = catalog.archive_aggregate(prepared.target.storage_id)

    assert aggregate["backlog_files"] == 5


def test_archive_aggregate_many_historical_rows_stays_aggregate_only(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=100, payload_bytes=8)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        )
        for _ in prepared.chunk_ids:
            assert manager.run_once().state == "LOCAL_DELETED"

        statements: list[str] = []
        catalog._connection.set_trace_callback(statements.append)
        try:
            aggregate = catalog.archive_aggregate(prepared.target.storage_id)
        finally:
            catalog._connection.set_trace_callback(None)

    normalized = [" ".join(statement.lower().split()) for statement in statements]
    assert not any("select * from chunks" in statement for statement in normalized)
    assert not any(
        "select * from archive_transactions" in statement
        for statement in normalized
    )
    assert aggregate["local_deleted_files"] == 100
    assert aggregate["backlog_files"] == 0


def test_archive_aggregate_target_inflight_is_storage_scoped(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=3)
    other_target = ArchiveTarget(
        storage_id="other-storage",
        volume_uuid="OTHER-VOLUME",
        registered_relative_path="Other/Archive",
        marker_nonce="other-marker",
        root=tmp_path / "other-volume" / "Other" / "Archive",
    )
    with Catalog(prepared.layout.catalog) as catalog:
        _leave_copying(
            prepared_target=other_target,
            catalog=catalog,
            layout=prepared.layout,
        )

        selected = catalog.archive_aggregate(prepared.target.storage_id)
        other = catalog.archive_aggregate(other_target.storage_id)

    assert selected["unassigned_sealed_scope"] == "GLOBAL"
    assert selected["unassigned_sealed_files"] == 2
    assert selected["target_inflight_files"] == 0
    assert selected["backlog_files"] == 2
    assert other["unassigned_sealed_files"] == 2
    assert other["target_inflight_files"] == 1
    assert other["backlog_files"] == 3


def test_archive_aggregate_latest_error_by_updated_at(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=1)
    with Catalog(prepared.layout.catalog) as catalog:
        _leave_copying(
            prepared_target=prepared.target,
            catalog=catalog,
            layout=prepared.layout,
        )
        transaction = catalog.oldest_incomplete_archive_transaction(
            prepared.target.storage_id
        )
        assert transaction is not None
        catalog.record_archive_error(
            str(transaction["transaction_id"]),
            "ArchiveError: DISAPPEARED_DURING_COPY",
        )

        aggregate = catalog.archive_aggregate(prepared.target.storage_id)

    assert aggregate["latest_error_type"] == "DISAPPEARED_DURING_COPY"
