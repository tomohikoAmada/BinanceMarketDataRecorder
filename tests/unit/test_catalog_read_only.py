from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from binance_market_data_recorder.storage.catalog import Catalog, CatalogStateError

STORAGE_ID = "11111111-2222-3333-4444-555555555555"


def _catalog_files(root: Path) -> dict[str, tuple[int, bytes]]:
    return {
        path.name: (path.stat().st_mtime_ns, path.read_bytes())
        for path in root.iterdir()
        if path.name.startswith("catalog.sqlite")
    }


def _create_catalog(path: Path) -> None:
    with Catalog(path) as catalog:
        catalog.register_storage_target(
            storage_id=STORAGE_ID,
            volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
            volume_name="Read Only Test",
            filesystem_type="testfs",
            relative_path="Archive",
            marker_nonce="read-only-marker-nonce",
            registered_at_utc_ns=1,
        )
        assert catalog.begin_storage_eject(
            storage_id=STORAGE_ID,
            request_id="read-only-control",
            occurred_at_utc_ns=2,
        ) == []


def test_read_only_catalog_reads_aggregate_targets_and_control_without_mutation(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "catalog.sqlite"
    _create_catalog(catalog_path)
    before = _catalog_files(tmp_path)

    with Catalog(catalog_path, read_only=True) as catalog:
        query_only = catalog._connection.execute("PRAGMA query_only").fetchone()
        assert query_only is not None
        assert query_only[0] == 1
        assert catalog.archive_aggregate(STORAGE_ID)["backlog_files"] == 0
        assert catalog.storage_targets()[0]["storage_id"] == STORAGE_ID
        assert catalog.storage_control(STORAGE_ID)["state"] == "EJECT_PENDING"

    assert _catalog_files(tmp_path) == before


@pytest.mark.parametrize(
    "write",
    [
        lambda catalog: catalog._transaction().__enter__(),
        lambda catalog: catalog._initialize(),
        lambda catalog: catalog.record_operational_event(
            event_id="event",
            event_type="TEST",
            occurred_at_utc_ns=1,
            evidence={},
        ),
        lambda catalog: catalog.begin_archive_attempt("missing-transaction"),
        lambda catalog: catalog.record_archive_error("missing-transaction", "error"),
        lambda catalog: catalog.register_quarantined_artifact(
            artifact_id="artifact",
            relative_path="quarantine/artifact",
            reason="test",
            sha256="0" * 64,
        ),
        lambda catalog: catalog.register_orderbook_checkpoint(
            checkpoint_id="checkpoint",
            market="spot",
            symbol="BTCUSDT",
            update_id=1,
            book_hash="1" * 64,
            relative_path="checkpoints/test",
            created_at_utc_ns=1,
        ),
        lambda catalog: catalog.checkpoint(),
    ],
)
def test_read_only_catalog_rejects_every_write_entrypoint_outside_transactions(
    tmp_path: Path,
    write: Callable[[Catalog], object],
) -> None:
    catalog_path = tmp_path / "catalog.sqlite"
    _create_catalog(catalog_path)

    with (
        Catalog(catalog_path, read_only=True) as catalog,
        pytest.raises(CatalogStateError, match="read-only"),
    ):
        write(catalog)


def test_read_only_catalog_missing_path_does_not_create_file_or_parent(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "missing" / "catalog.sqlite"

    with pytest.raises(CatalogStateError, match="does not exist"):
        Catalog(catalog_path, read_only=True)

    assert not catalog_path.exists()
    assert not catalog_path.parent.exists()
