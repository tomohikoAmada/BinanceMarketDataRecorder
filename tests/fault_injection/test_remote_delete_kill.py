from __future__ import annotations

import multiprocessing
import os
import signal
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.archive.remote_authorization import RemoteAuthorizer
from binance_market_data_recorder.archive.remote_delete import RemoteDeleter
from binance_market_data_recorder.storage.catalog import (
    Catalog,
    RemoteArchiveState,
)
from binance_market_data_recorder.storage.layout import StorageLayout
from tests.remote_authorization_support import (
    RemoteAuthorizationFixture,
    build_receipt,
    prepare_remote_authorization,
)


def _authorized(root: Path) -> tuple[RemoteAuthorizationFixture, str]:
    fixture = prepare_remote_authorization(root)
    receipt = build_receipt(fixture)
    assert fixture.prepared.layout.root == root.resolve() / "internal"
    assert fixture.selections[0].sealed_path.parent == fixture.prepared.layout.sealed
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        RemoteAuthorizer(layout=fixture.prepared.layout, catalog=catalog).authorize(
            receipt.canonical_bytes(), fixture.selections[0]
        )
    return fixture, receipt.receipt_id


def _kill_worker(root: str, receipt_id: str, kill_point: str) -> None:
    layout = StorageLayout.from_root(Path(root))
    assert layout.root.name == "internal"

    def kill(point: str) -> None:
        if point == kill_point:
            os.kill(os.getpid(), signal.SIGKILL)

    with Catalog(layout.catalog) as catalog:
        RemoteDeleter(
            layout=layout,
            catalog=catalog,
            fault_hook=kill,
        ).delete_authorized(receipt_id)


@pytest.mark.parametrize(
    ("kill_point", "raw_present", "terminal"),
    [
        ("k1_after_validation_before_unlink", True, False),
        ("k2_after_unlink_before_parent_fsync", False, False),
        ("k3_after_parent_fsync_before_terminal", False, False),
        ("k4_after_remote_deleted_update_event_before_commit", False, False),
        ("k5_after_terminal_commit", False, True),
    ],
)
def test_real_process_kill_recovers_from_fresh_objects(
    tmp_path: Path,
    kill_point: str,
    raw_present: bool,
    terminal: bool,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    sibling = fixture.prepared.layout.sealed / "must-not-delete.sibling"
    sibling.write_bytes(b"unrelated test-owned sibling")
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=_kill_worker,
        args=(str(fixture.prepared.layout.root), receipt_id, kill_point),
    )
    process.start()
    process.join(30)
    assert process.exitcode == -signal.SIGKILL

    source = fixture.selections[0].sealed_path
    assert source.exists() is raw_present
    assert fixture.selections[0].manifest_path.is_file()
    fresh_layout = StorageLayout.from_root(fixture.prepared.layout.root)
    with Catalog(fresh_layout.catalog) as fresh_catalog:
        row = fresh_catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert (row["state"] == RemoteArchiveState.REMOTE_DELETED.value) is terminal
        assert len(fresh_catalog.remote_archive_events(receipt_id)) == (2 if terminal else 1)
        fresh_deleter = RemoteDeleter(layout=fresh_layout, catalog=fresh_catalog)
        if raw_present or terminal:
            result = fresh_deleter.delete_authorized(receipt_id)
        else:
            result = fresh_deleter.reconcile_absent_authorized(receipt_id)
        assert result.state is RemoteArchiveState.REMOTE_DELETED
        assert len(fresh_catalog.remote_archive_events(receipt_id)) == 2
    assert sibling.read_bytes() == b"unrelated test-owned sibling"


def _concurrent_worker(
    root: str,
    receipt_id: str,
    barrier: Any,
    results: Any,
) -> None:
    layout = StorageLayout.from_root(Path(root))

    def synchronize(point: str) -> None:
        if point == "k1_after_validation_before_unlink":
            barrier.wait(timeout=30)

    try:
        with Catalog(layout.catalog) as catalog:
            result = RemoteDeleter(
                layout=layout,
                catalog=catalog,
                fault_hook=synchronize,
            ).delete_authorized(receipt_id)
        results.put(("ok", result.source_deleted))
    except Exception as exc:
        results.put(("error", type(exc).__name__, str(exc)))


def test_two_process_same_receipt_converges_to_one_terminal_event(
    tmp_path: Path,
) -> None:
    fixture, receipt_id = _authorized(tmp_path)
    sibling = fixture.prepared.layout.sealed / "must-not-delete.sibling"
    sibling.write_bytes(b"unrelated test-owned sibling")
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    results = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_worker,
            args=(str(fixture.prepared.layout.root), receipt_id, barrier, results),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(30)
        assert process.exitcode == 0
    outcomes = [results.get(timeout=5) for _ in processes]
    assert all(outcome[0] == "ok" for outcome in outcomes), outcomes

    with Catalog(fixture.prepared.layout.catalog) as catalog:
        row = catalog.remote_archive_transaction(receipt_id)
        assert row is not None
        assert row["state"] == RemoteArchiveState.REMOTE_DELETED.value
        events = catalog.remote_archive_events(receipt_id)
        assert len(events) == 2
        assert sum(event["to_state"] == "REMOTE_DELETED" for event in events) == 1
    assert not fixture.selections[0].sealed_path.exists()
    assert sibling.read_bytes() == b"unrelated test-owned sibling"
    assert fixture.selections[0].manifest_path.is_file()
