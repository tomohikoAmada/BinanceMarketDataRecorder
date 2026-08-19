from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.archive.remote_authorization import (
    RemoteAuthorizationError,
    RemoteAuthorizer,
    RemoteRecoveryCase,
    classify_remote_recovery,
)
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.storage.catalog import (
    Catalog,
    CatalogStateError,
    ChunkState,
    RemoteArchiveState,
)
from tests.remote_authorization_support import (
    build_receipt,
    prepare_remote_authorization,
    source_snapshot,
)


def _canonical(document: dict[str, object]) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def test_pending_authorization_persists_exact_receipt_and_is_idempotent(
    tmp_path: Path,
) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    body = receipt.canonical_bytes()
    before = source_snapshot(fixture)
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        authorizer = RemoteAuthorizer(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: 1_900_000_000_000_000_000,
        )
        first = authorizer.authorize(body, fixture.selections[0])
        second = authorizer.authorize(body, fixture.selections[0])
        row = catalog.remote_archive_transaction(receipt.receipt_id)
        assert row is not None
        assert row["receipt_bytes"] == body
        assert row["created_at_utc_ns"] == 1_900_000_000_000_000_000
        assert first == second
        assert first.state is RemoteArchiveState.REMOTE_DELETE_PENDING
        assert catalog.state(receipt.chunk_id) is ChunkState.SEALED
        assert catalog.archive_transactions() == []
        assert len(catalog.remote_archive_events(receipt.receipt_id)) == 1
    assert source_snapshot(fixture) == before


def test_different_receipt_for_authorized_chunk_and_replay_other_chunk_fail(
    tmp_path: Path,
) -> None:
    fixture = prepare_remote_authorization(tmp_path, chunk_count=2)
    first = build_receipt(fixture)
    different_session = build_receipt(
        fixture, session_id="87654321-4321-4321-8321-cba987654321"
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        authorizer = RemoteAuthorizer(layout=fixture.prepared.layout, catalog=catalog)
        authorizer.authorize(first.canonical_bytes(), fixture.selections[0])
        with pytest.raises(RemoteAuthorizationError):
            authorizer.authorize(
                different_session.canonical_bytes(), fixture.selections[0]
            )
        with pytest.raises(RemoteAuthorizationError):
            authorizer.authorize(first.canonical_bytes(), fixture.selections[1])
        assert len(catalog.remote_archive_transactions()) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("stored_sha256", "0" * 64),
        ("source_manifest_sha256", "1" * 64),
        ("source_descriptor_sha256", "2" * 64),
        ("archive_set_id", "other-set"),
        ("storage_id", "other-storage"),
        ("artifact_relative_path", "raw/other.bmdr.zst"),
        ("session_id", "87654321-4321-4321-8321-cba987654321"),
        ("archive_set_entry_sha256", "3" * 64),
        ("chunk_id", "other-chunk"),
    ],
)
def test_receipt_byte_mutations_fail_closed(
    tmp_path: Path, field: str, value: object
) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    document = receipt.document()
    document[field] = value
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteAuthorizationError):
            RemoteAuthorizer(
                layout=fixture.prepared.layout, catalog=catalog
            ).authorize(_canonical(document), fixture.selections[0])
        assert catalog.remote_archive_transactions() == []


def test_source_mutation_before_authorization_persists_nothing(tmp_path: Path) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    fixture.selections[0].sealed_path.write_bytes(b"mutated")
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteAuthorizationError):
            RemoteAuthorizer(
                layout=fixture.prepared.layout, catalog=catalog
            ).authorize(receipt.canonical_bytes(), fixture.selections[0])
        assert catalog.remote_archive_transactions() == []

    manifest_fixture = prepare_remote_authorization(tmp_path / "manifest")
    manifest_receipt = build_receipt(manifest_fixture)
    manifest_fixture.selections[0].manifest_path.write_bytes(b"{}\n")
    with Catalog(manifest_fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RemoteAuthorizationError):
            RemoteAuthorizer(
                layout=manifest_fixture.prepared.layout, catalog=catalog
            ).authorize(
                manifest_receipt.canonical_bytes(), manifest_fixture.selections[0]
            )
        assert catalog.remote_archive_transactions() == []


def test_fault_before_commit_rolls_back_and_after_commit_survives(tmp_path: Path) -> None:
    first_fixture = prepare_remote_authorization(tmp_path / "before")
    first_receipt = build_receipt(first_fixture)

    def before(point: str) -> None:
        if point == "before_remote_authorization_commit":
            raise RuntimeError("injected before commit")

    with Catalog(first_fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RuntimeError, match="before commit"):
            RemoteAuthorizer(
                layout=first_fixture.prepared.layout,
                catalog=catalog,
                fault_hook=before,
            ).authorize(first_receipt.canonical_bytes(), first_fixture.selections[0])
        assert catalog.remote_archive_transactions() == []
        assert catalog.remote_archive_events(first_receipt.receipt_id) == []

    second_fixture = prepare_remote_authorization(tmp_path / "after")
    second_receipt = build_receipt(second_fixture)

    def after(point: str) -> None:
        if point == "after_remote_authorization_commit":
            raise RuntimeError("injected after commit")

    with Catalog(second_fixture.prepared.layout.catalog) as catalog:
        with pytest.raises(RuntimeError, match="after commit"):
            RemoteAuthorizer(
                layout=second_fixture.prepared.layout,
                catalog=catalog,
                fault_hook=after,
            ).authorize(second_receipt.canonical_bytes(), second_fixture.selections[0])
        assert len(catalog.remote_archive_transactions()) == 1
        decision = classify_remote_recovery(
            layout=second_fixture.prepared.layout,
            catalog=catalog,
            chunk_id=second_receipt.chunk_id,
        )
        assert decision.case is RemoteRecoveryCase.CASE_A


def test_daily_report_accounts_remote_authorization_by_authorization_day(
    tmp_path: Path,
) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    occurred = int(
        datetime(2026, 8, 19, 12, tzinfo=UTC).timestamp() * 1_000_000_000
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: occurred,
        ).authorize(receipt.canonical_bytes(), fixture.selections[0])
        report = DailyReporter(
            catalog=catalog,
            daily_directory=fixture.prepared.layout.daily_reports,
        ).build("2026-08-19", generated_at_utc_ns=occurred + 1)
    stream = cast(list[dict[str, Any]], report["streams"])[0]
    output = cast(dict[str, object], stream["output"])
    assert stream["market"] == "spot"
    assert output["archived_files"] == 1
    assert output["archived_bytes"] == receipt.stored_bytes


@pytest.mark.parametrize("field", ["market", "stream"])
def test_daily_report_rejects_persisted_descriptor_identity_relabel(
    tmp_path: Path, field: str
) -> None:
    fixture = prepare_remote_authorization(tmp_path)
    receipt = build_receipt(fixture)
    occurred = int(
        datetime(2026, 8, 19, 12, tzinfo=UTC).timestamp() * 1_000_000_000
    )
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(
            layout=fixture.prepared.layout,
            catalog=catalog,
            utc_clock_ns=lambda: occurred,
        ).authorize(receipt.canonical_bytes(), fixture.selections[0])
        catalog._connection.execute(
            f"UPDATE remote_archive_transactions SET {field} = ?",
            ("relabelled",),
        )
        with pytest.raises(CatalogStateError):
            DailyReporter(
                catalog=catalog,
                daily_directory=fixture.prepared.layout.daily_reports,
            ).build("2026-08-19", generated_at_utc_ns=occurred + 1)
