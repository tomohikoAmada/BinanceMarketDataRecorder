from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from binance_market_data_recorder.archive.manager import ArchiveManager
from binance_market_data_recorder.archive.remote_authorization import (
    RemoteAuthorizationError,
    RemoteAuthorizer,
)
from binance_market_data_recorder.archive.remote_source import (
    RemoteSourceError,
    RemoteSourceExporter,
)
from binance_market_data_recorder.cli import _archive_status
from binance_market_data_recorder.storage.catalog import (
    Catalog,
    CatalogStateError,
    ChunkState,
)
from binance_market_data_recorder.storage.forecast import StorageForecaster
from tests.remote_authorization_support import (
    RemoteAuthorizationFixture,
    build_receipt,
    prepare_remote_authorization,
)

FIXTURE = Path(__file__).parents[1] / "fixtures" / "pre_m22_catalog_schema.sql"

REMOTE_TRANSACTIONS = """
CREATE TABLE remote_archive_transactions (
    receipt_id TEXT PRIMARY KEY,
    chunk_id TEXT NOT NULL UNIQUE REFERENCES chunks(chunk_id),
    state TEXT NOT NULL CHECK (state IN ('REMOTE_DELETE_PENDING', 'REMOTE_DELETED')),
    receipt_bytes BLOB NOT NULL,
    receipt_schema_version TEXT NOT NULL,
    session_id TEXT NOT NULL,
    verification_version TEXT NOT NULL,
    verification_outcome TEXT NOT NULL,
    source_descriptor_schema_version TEXT NOT NULL,
    source_descriptor_sha256 TEXT NOT NULL,
    market TEXT NOT NULL,
    stream TEXT NOT NULL,
    source_relative_path TEXT NOT NULL,
    source_manifest_relative_path TEXT NOT NULL,
    source_manifest_sha256 TEXT NOT NULL,
    stored_bytes INTEGER NOT NULL,
    stored_sha256 TEXT NOT NULL,
    archive_set_id TEXT NOT NULL,
    storage_id TEXT NOT NULL,
    artifact_relative_path TEXT NOT NULL,
    archive_set_entry_sha256 TEXT NOT NULL,
    created_at_utc_ns INTEGER NOT NULL,
    updated_at_utc_ns INTEGER NOT NULL,
    remote_deleted_at_utc_ns INTEGER,
    CHECK ((state = 'REMOTE_DELETE_PENDING' AND remote_deleted_at_utc_ns IS NULL)
        OR (state = 'REMOTE_DELETED' AND remote_deleted_at_utc_ns IS NOT NULL))
);
CREATE INDEX remote_archive_transactions_by_state
ON remote_archive_transactions(state, created_at_utc_ns, receipt_id);
"""
REMOTE_EVENTS = """
CREATE TABLE remote_archive_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id TEXT NOT NULL REFERENCES remote_archive_transactions(receipt_id),
    from_state TEXT,
    to_state TEXT NOT NULL,
    occurred_at_utc_ns INTEGER NOT NULL,
    evidence_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE
);
CREATE INDEX remote_archive_events_by_receipt
ON remote_archive_events(receipt_id, event_id);
"""


def _legacy_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(FIXTURE.read_text())
        connection.commit()
    finally:
        connection.close()


def _entries(path: Path) -> dict[str, bytes]:
    return {
        item.name: item.read_bytes()
        for item in path.parent.iterdir()
        if item.name.startswith(path.name)
    }


def test_pre_m22_writable_upgrade_is_additive_and_preserves_rows(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite"
    _legacy_database(path)
    connection = sqlite3.connect(path)
    before_tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    connection.close()

    with Catalog(path) as catalog:
        assert catalog.state("legacy-sealed-chunk") is ChunkState.SEALED
        assert catalog.remote_archive_transactions() == []
        assert catalog.table_columns("remote_archive_transactions")

    connection = sqlite3.connect(path)
    after_tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    connection.close()
    assert after_tables - before_tables == {
        "remote_archive_transactions",
        "remote_archive_events",
    }


def test_legacy_upgrade_validation_failure_rolls_back_both_remote_tables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "catalog.sqlite"
    _legacy_database(path)

    def fail_validation(_catalog: Catalog) -> None:
        raise CatalogStateError("injected schema validation failure")

    monkeypatch.setattr(Catalog, "_validate_remote_schema", fail_validation)
    with pytest.raises(CatalogStateError, match="injected schema validation"):
        Catalog(path)
    connection = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()
    assert "remote_archive_transactions" not in tables
    assert "remote_archive_events" not in tables


def test_pre_m22_read_only_remote_queries_do_not_create_schema_or_sidecars(
    tmp_path: Path,
) -> None:
    path = tmp_path / "catalog.sqlite"
    writer = sqlite3.connect(path)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.executescript(FIXTURE.read_text())
        writer.commit()
        before = _entries(path)
        with Catalog(path, read_only=True) as catalog:
            assert catalog.remote_archive_transactions() == []
            assert catalog.remote_archive_transaction_for_chunk("legacy-sealed-chunk") is None
            assert catalog.oldest_unowned_sealed_chunk() is not None
        after = _entries(path)
        assert set(after) == set(before)
        assert after["catalog.sqlite"] == before["catalog.sqlite"]
        if "catalog.sqlite-wal" in before:
            assert after["catalog.sqlite-wal"] == before["catalog.sqlite-wal"]
        tables = {
            row[0]
            for row in writer.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "remote_archive_transactions" not in tables
        assert "remote_archive_events" not in tables
    finally:
        writer.close()


@pytest.mark.parametrize("present", ["transactions", "events"])
def test_partial_remote_schema_fails_closed(tmp_path: Path, present: str) -> None:
    path = tmp_path / "catalog.sqlite"
    _legacy_database(path)
    connection = sqlite3.connect(path)
    connection.executescript(
        REMOTE_TRANSACTIONS if present == "transactions" else REMOTE_EVENTS
    )
    connection.close()
    with pytest.raises(CatalogStateError, match="partial remote archive schema"):
        Catalog(path)


@pytest.mark.parametrize(
    "transaction_sql,events_sql",
    [
        (
            REMOTE_TRANSACTIONS.replace(
                "receipt_id TEXT PRIMARY KEY", "receipt_id TEXT"
            ),
            REMOTE_EVENTS,
        ),
        (
            REMOTE_TRANSACTIONS.replace(
                "chunk_id TEXT NOT NULL UNIQUE", "chunk_id TEXT NOT NULL"
            ),
            REMOTE_EVENTS,
        ),
        (
            REMOTE_TRANSACTIONS.replace(
                "REFERENCES chunks(chunk_id)",
                "REFERENCES storage_targets(storage_id)",
            ),
            REMOTE_EVENTS,
        ),
        (REMOTE_TRANSACTIONS.replace("'REMOTE_DELETED'", "'REMOTE_FINISHED'", 1), REMOTE_EVENTS),
        (REMOTE_TRANSACTIONS.replace("    market TEXT NOT NULL,\n", ""), REMOTE_EVENTS),
        (
            REMOTE_TRANSACTIONS.replace(
                "CREATE INDEX remote_archive_transactions_by_state\n"
                "ON remote_archive_transactions"
                "(state, created_at_utc_ns, receipt_id);",
                "",
            ),
            REMOTE_EVENTS,
        ),
    ],
)
def test_malformed_remote_schema_fails_closed(
    tmp_path: Path, transaction_sql: str, events_sql: str
) -> None:
    path = tmp_path / "catalog.sqlite"
    _legacy_database(path)
    connection = sqlite3.connect(path)
    connection.executescript(transaction_sql + events_sql)
    connection.close()
    with pytest.raises(CatalogStateError):
        Catalog(path)


def _same_host_reserve(
    catalog: Catalog, fixture: RemoteAuthorizationFixture
) -> None:
    selection = fixture.selections[0]
    descriptor = selection.descriptor
    catalog.reserve_archive_transaction(
        transaction_id="same-host-race",
        chunk_id=descriptor.chunk_id,
        storage_id=fixture.prepared.target.storage_id,
        market=descriptor.market,
        stream=descriptor.stream,
        source_relative_path=descriptor.source_relative_path,
        source_manifest_relative_path=descriptor.source_manifest_relative_path,
        source_manifest_sha256=descriptor.source_manifest_sha256,
        target_relative_path="raw/race.bmdr.zst",
        target_temp_relative_path="raw/race.bmdr.zst.copying",
        external_manifest_relative_path="manifests/race.json",
        stored_bytes=descriptor.stored_bytes,
        stored_sha256=descriptor.stored_sha256,
    )


def test_remote_and_same_host_race_has_exactly_one_owner(tmp_path: Path) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    barrier = threading.Barrier(2)

    def remote() -> str:
        with Catalog(fixture.prepared.layout.catalog) as catalog:
            barrier.wait()
            try:
                RemoteAuthorizer(
                    layout=fixture.prepared.layout, catalog=catalog
                ).authorize(receipt.canonical_bytes(), fixture.selections[0])
            except RemoteAuthorizationError:
                return "remote-failed"
            return "remote-won"

    def local() -> str:
        with Catalog(fixture.prepared.layout.catalog) as catalog:
            barrier.wait()
            try:
                _same_host_reserve(catalog, fixture)
            except CatalogStateError:
                return "local-failed"
            return "local-won"

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (executor.submit(remote), executor.submit(local))
        results = {future.result() for future in futures}
    assert results in (
        {"remote-won", "local-failed"},
        {"local-won", "remote-failed"},
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        owners = (
            len(catalog.remote_archive_transactions()),
            len(catalog.archive_transactions()),
        )
        assert owners in {
            (1, 0),
            (0, 1),
        }


def test_concurrent_same_receipt_converges_and_different_receipts_conflict(
    tmp_path: Path,
) -> None:
    fixture = prepare_remote_authorization(tmp_path / "same")
    receipt = build_receipt(fixture)

    def authorize(body: bytes) -> str:
        with Catalog(fixture.prepared.layout.catalog) as catalog:
            return RemoteAuthorizer(
                layout=fixture.prepared.layout, catalog=catalog
            ).authorize(body, fixture.selections[0]).receipt_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        identifiers = list(executor.map(authorize, [receipt.canonical_bytes()] * 2))
    assert identifiers == [receipt.receipt_id, receipt.receipt_id]
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        assert len(catalog.remote_archive_events(receipt.receipt_id)) == 1

    different = prepare_remote_authorization(tmp_path / "different")
    first = build_receipt(different)
    second = build_receipt(
        different, session_id="87654321-4321-4321-8321-cba987654321"
    )

    def competing(body: bytes) -> str:
        with Catalog(different.prepared.layout.catalog) as catalog:
            try:
                RemoteAuthorizer(
                    layout=different.prepared.layout, catalog=catalog
                ).authorize(body, different.selections[0])
            except RemoteAuthorizationError:
                return "conflict"
            return "winner"

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(
            executor.map(competing, [first.canonical_bytes(), second.canonical_bytes()])
        )
    assert sorted(outcomes) == ["conflict", "winner"]


def test_remote_projection_changes_backlog_classification_only(tmp_path: Path) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        before = catalog.source_lifecycle_aggregate()
        assert before["unarchived_backlog_files"] == 1
        RemoteAuthorizer(layout=fixture.prepared.layout, catalog=catalog).authorize(
            receipt.canonical_bytes(), fixture.selections[0]
        )
        pending = catalog.source_lifecycle_aggregate()
        assert pending["unarchived_backlog_files"] == 0
        assert pending["remote_pending_files"] == 1
        assert pending["remote_pending_source_bytes"] == receipt.stored_bytes
        target_aggregate = catalog.archive_aggregate(
            fixture.prepared.target.storage_id
        )
        assert target_aggregate["backlog_files"] == 0
        assert target_aggregate["remote_pending_files"] == 1
        manager_status = ArchiveManager(
            layout=fixture.prepared.layout,
            catalog=catalog,
            target=fixture.prepared.target,
        ).status()
        assert manager_status["remote_pending_source_bytes"] == receipt.stored_bytes
        cli_status = _archive_status(catalog)
        assert cli_status["backlog_files"] == 0
        assert cli_status["remote_pending_files"] == 1
        forecaster = StorageForecaster(
            catalog=catalog, data_root=fixture.prepared.layout.root
        )
        forecaster.observe(
            scope_id="internal",
            storage_id=None,
            total_bytes=1000,
            free_bytes=500,
            observed_at_utc_ns=10,
        )
        assert catalog.space_samples("internal")[-1]["archive_backlog_bytes"] == 0
        catalog._connection.execute(
            "UPDATE remote_archive_transactions SET state = 'REMOTE_DELETED', "
            "remote_deleted_at_utc_ns = 9, updated_at_utc_ns = 9"
        )
        terminal = catalog.source_lifecycle_aggregate()
        assert terminal["unarchived_backlog_files"] == 0
        assert terminal["remote_pending_files"] == 0
        assert terminal["remote_pending_source_bytes"] == 0
        assert terminal["remote_deleted_files"] == 1


def test_remote_owned_source_is_ineligible_and_does_not_block_later_work(
    tmp_path: Path,
) -> None:
    fixture = prepare_remote_authorization(tmp_path, chunk_count=2)
    receipt = build_receipt(fixture)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(layout=fixture.prepared.layout, catalog=catalog).authorize(
            receipt.canonical_bytes(), fixture.selections[0]
        )
        exporter = RemoteSourceExporter(layout=fixture.prepared.layout, catalog=catalog)
        with pytest.raises(RemoteSourceError, match="remote archive ownership"):
            exporter.select_chunk(receipt.chunk_id)
        oldest = exporter.select_oldest()
        assert oldest is not None
        assert oldest.descriptor.chunk_id == fixture.prepared.chunk_ids[1]
        manager = ArchiveManager(
            layout=fixture.prepared.layout,
            catalog=catalog,
            target=fixture.prepared.target,
        )
        transaction = manager._next_transaction()
        assert transaction is not None
        assert transaction["chunk_id"] == fixture.prepared.chunk_ids[1]


def test_owner_acquired_first_rejects_the_other_path(tmp_path: Path) -> None:
    remote_first = prepare_remote_authorization(tmp_path / "remote")
    receipt = build_receipt(remote_first)
    with Catalog(remote_first.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(
            layout=remote_first.prepared.layout, catalog=catalog
        ).authorize(receipt.canonical_bytes(), remote_first.selections[0])
        with pytest.raises(CatalogStateError, match="remote archive ownership"):
            _same_host_reserve(catalog, remote_first)
        assert catalog.state(receipt.chunk_id) is ChunkState.SEALED
        assert catalog.archive_transactions() == []

    local_first = prepare_remote_authorization(tmp_path / "local")
    receipt = build_receipt(local_first)
    with Catalog(local_first.prepared.layout.catalog) as catalog:
        _same_host_reserve(catalog, local_first)
        with pytest.raises(RemoteAuthorizationError):
            RemoteAuthorizer(
                layout=local_first.prepared.layout, catalog=catalog
            ).authorize(receipt.canonical_bytes(), local_first.selections[0])
        assert catalog.remote_archive_transactions() == []
        assert catalog.state(receipt.chunk_id) is ChunkState.ARCHIVE_COPYING
