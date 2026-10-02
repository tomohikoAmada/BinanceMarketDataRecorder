from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.service.acceptance_v5_raw import capture_snapshot
from binance_market_data_recorder.storage.acceptance_delta import (
    DeltaAuthorityError,
    DeltaSnapshot,
    validate_sequence,
)
from binance_market_data_recorder.storage.catalog import Catalog, CatalogStateError


def insert_event(connection: sqlite3.Connection, event_id: str, timestamp: int) -> None:
    connection.execute(
        "INSERT OR IGNORE INTO operational_events "
        "(event_id,event_type,occurred_at_utc_ns,evidence_json) VALUES(?,?,?,?)",
        (event_id, "TEST", timestamp, "{}"),
    )


def test_four_pages_share_one_snapshot_and_stop_at_the_declared_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        catalog.migrate_acceptance_sequence()
        with catalog._transaction() as connection:
            for ordinal in range(1050):
                insert_event(connection, f"event-{ordinal}", ordinal)
        original = DeltaSnapshot.page
        inserted = False

        def page(self: DeltaSnapshot, family: str, processed: int, *, limit: int = 256) -> Any:
            nonlocal inserted
            rows = original(self, family, processed, limit=limit)
            if family == "operational" and not inserted:
                inserted = True
                insert_event(catalog._connection, "committed-after-frozen-high-water", 1050)
            return rows

        monkeypatch.setattr(DeltaSnapshot, "page", page)
        result = capture_snapshot(
            {
                "catalog_path": str(path),
                "processed": {"chunk": 0, "archive": 0, "operational": 0},
                "open_chunk_bound": 32,
                "max_pages_per_family": 4,
            }
        )
        assert result["high_water"]["operational"] == 1050
        assert len(result["candidates"]["operational"]) == 1024
        assert result["candidates"]["operational"][-1]["row"]["event_seq"] == 1024
        assert not any(
            entry["row"]["event_id"] == "committed-after-frozen-high-water"
            for entry in result["candidates"]["operational"]
        )


def test_atomic_backfill_and_trigger(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        insert_event(catalog._connection, "z", 20)
        insert_event(catalog._connection, "b", 10)
        insert_event(catalog._connection, "a", 10)
        catalog.migrate_acceptance_sequence()
        assert [
            tuple(row)
            for row in catalog._connection.execute(
                "SELECT event_seq,event_id FROM operational_event_sequence ORDER BY event_seq"
            )
        ] == [(1, "a"), (2, "b"), (3, "z")]
        insert_event(catalog._connection, "new", 1)
        insert_event(catalog._connection, "new", 1)
        validate_sequence(catalog._connection)
        with catalog.acceptance_delta_snapshot() as snapshot:
            assert snapshot.high_water == {"chunk": 0, "archive": 0, "operational": 4}
            assert [row["event_id"] for row in snapshot.page("operational", 0)] == [
                "a",
                "b",
                "z",
                "new",
            ]
    with Catalog(path) as reopened:
        reopened.migrate_acceptance_sequence()
        assert (
            reopened._connection.execute(
                "SELECT COUNT(*) FROM operational_event_sequence"
            ).fetchone()[0]
            == 4
        )


@pytest.mark.parametrize("phase", ["after_table", "after_backfill", "after_trigger"])
def test_migration_rolls_back_every_phase(tmp_path: Path, phase: str) -> None:
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        insert_event(catalog._connection, "a", 10)

        def fail(current: str) -> None:
            if current == phase:
                raise RuntimeError("injected crash")

        with pytest.raises(RuntimeError, match="injected crash"):
            catalog.migrate_acceptance_sequence(checkpoint=fail)
        assert (
            catalog._connection.execute(
                "SELECT 1 FROM sqlite_master WHERE name='operational_event_sequence'"
            ).fetchone()
            is None
        )
        catalog.migrate_acceptance_sequence()
        validate_sequence(catalog._connection)


def test_partial_migration_rejected(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        catalog._connection.execute(
            "CREATE TABLE operational_event_sequence(event_seq INTEGER PRIMARY KEY,event_id TEXT)"
        )
    with pytest.raises(CatalogStateError, match="invalid acceptance delta schema"):
        Catalog(path)


def test_bounded_pages_and_regression(tmp_path: Path) -> None:
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        catalog.migrate_acceptance_sequence()
        for ordinal in range(600):
            insert_event(catalog._connection, f"event-{ordinal}", ordinal)
        with catalog.acceptance_delta_snapshot() as snapshot:
            first = snapshot.page("operational", 0)
            second = snapshot.page("operational", first[-1]["event_seq"])
            third = snapshot.page("operational", second[-1]["event_seq"])
            assert [len(first), len(second), len(third)] == [256, 256, 88]
            with pytest.raises(DeltaAuthorityError, match="regression"):
                snapshot.page("operational", 601)
            with pytest.raises(DeltaAuthorityError, match="cap"):
                snapshot.page("operational", 0, limit=257)


def test_one_snapshot_excludes_later_commit(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path) as catalog:
        catalog.migrate_acceptance_sequence()
        catalog._connection.execute("PRAGMA journal_mode=WAL")
        insert_event(catalog._connection, "before", 1)
        with catalog.acceptance_delta_snapshot() as snapshot:
            with sqlite3.connect(path) as writer:
                insert_event(writer, "after", 2)
            assert snapshot.high_water["operational"] == 1
            assert [row["event_id"] for row in snapshot.page("operational", 0)] == ["before"]
            assert snapshot.exact("operational_events", "event_id", "after") is None
        with catalog.acceptance_delta_snapshot() as snapshot:
            assert snapshot.high_water["operational"] == 2


def test_missing_sequence_authority_rejected(tmp_path: Path) -> None:
    with (
        Catalog(tmp_path / "catalog.sqlite") as catalog,
        pytest.raises(DeltaAuthorityError, match="partial"),
    ):
        DeltaSnapshot(catalog._connection)
