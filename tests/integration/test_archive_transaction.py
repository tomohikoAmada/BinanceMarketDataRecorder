from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.archive import ArchiveError, ArchiveManager
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.storage.catalog import ArchiveState, Catalog, ChunkState
from tests.archive_support import prepare_archive


def test_oldest_sealed_is_verified_archived_then_locally_deleted(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=2)
    with Catalog(prepared.layout.catalog) as catalog:
        first_row = catalog.chunk(prepared.chunk_ids[0])
        second_row = catalog.chunk(prepared.chunk_ids[1])
        assert first_row is not None and second_row is not None
        first_source = prepared.layout.root / str(first_row["sealed_path"])
        first_size = first_source.stat().st_size
        result = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
            utc_clock_ns=lambda: 1_700_000_000_100_000_000,
        ).run_once()

        assert result.chunk_id == prepared.chunk_ids[0]
        assert result.state == ArchiveState.LOCAL_DELETED
        assert result.archived_bytes == first_size
        assert result.deleted_local_bytes == first_size
        assert not first_source.exists()
        assert catalog.state(prepared.chunk_ids[0]) is ChunkState.LOCAL_DELETED
        assert catalog.state(prepared.chunk_ids[1]) is ChunkState.SEALED

        transaction = catalog.archive_transaction_for_chunk(prepared.chunk_ids[0])
        assert transaction is not None
        external = prepared.target.root / str(transaction["target_relative_path"])
        external_manifest = prepared.target.root / str(
            transaction["external_manifest_relative_path"]
        )
        assert external.is_file()
        assert external.stat().st_size == first_size
        document = json.loads(external_manifest.read_text(encoding="utf-8"))
        assert document["archive_manifest_schema_version"] == (
            "external-archive-manifest.v1"
        )
        assert document["verification"] == {
            "full_readback": True,
            "sha256_match": True,
            "size_match": True,
        }
        assert document["raw_manifest"]["chunk_id"] == prepared.chunk_ids[0]
        assert document["raw_manifest_bytes_base64"]

        verify = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        ).verify_all()
        assert verify["status"] == "VERIFIED"
        assert verify["verified_files"] == 1

        utc_date = "2023-11-14"
        report = DailyReporter(
            catalog=catalog, daily_directory=prepared.layout.daily_reports
        ).build(utc_date, generated_at_utc_ns=1_700_000_000_100_000_000)
        stream = cast(list[dict[str, Any]], report["streams"])[0]
        assert stream["output"]["archived_files"] == 1
        assert stream["output"]["archived_bytes"] == first_size
        assert stream["output"]["deleted_local_bytes"] == first_size
        assert stream["output"]["archive_backlog_bytes"] >= 0


def test_retry_is_idempotent_and_archives_next_oldest_chunk(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=2)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )
        first = manager.run_once()
        second = manager.run_once()
        empty = manager.run_once()

        assert first.chunk_id == prepared.chunk_ids[0]
        assert second.chunk_id == prepared.chunk_ids[1]
        assert empty.state == "NO_ELIGIBLE_CHUNKS"
        assert len(catalog.archive_transactions()) == 2
        assert all(
            row["state"] == ArchiveState.LOCAL_DELETED
            for row in catalog.archive_transactions()
        )


def test_matching_final_is_reused_but_mismatch_is_never_overwritten(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )

        def stop_after_rename(point: str, _path: Path | None) -> None:
            if point == "after_final_rename":
                raise RuntimeError("stop after verified rename")

        manager.fault_hook = stop_after_rename
        with pytest.raises(ArchiveError, match="stop after verified rename"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        source = prepared.layout.root / str(transaction["source_relative_path"])
        final = prepared.target.root / str(transaction["target_relative_path"])
        original = final.read_bytes()
        assert source.is_file()

        manager.fault_hook = None
        assert manager.run_once().state == ArchiveState.LOCAL_DELETED
        assert final.read_bytes() == original

    mismatch = prepare_archive(tmp_path / "mismatch")
    with Catalog(mismatch.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=mismatch.layout, catalog=catalog, target=mismatch.target
        )

        def stop_after_reserve(point: str, _path: Path | None) -> None:
            if point == "after_reserve":
                raise RuntimeError("reserved")

        manager.fault_hook = stop_after_reserve
        with pytest.raises(RuntimeError, match="reserved"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        final = mismatch.target.root / str(transaction["target_relative_path"])
        final.parent.mkdir(parents=True, exist_ok=True)
        final.write_bytes(b"different existing user data")
        manager.fault_hook = None
        with pytest.raises(ArchiveError, match="size mismatch"):
            manager.run_once()
        assert final.read_bytes() == b"different existing user data"
        assert (mismatch.layout.root / str(transaction["source_relative_path"])).is_file()


def test_checksum_unplug_and_delete_failures_retain_internal_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checksum = prepare_archive(tmp_path / "checksum")
    with Catalog(checksum.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=checksum.layout, catalog=catalog, target=checksum.target
        )

        def corrupt_after_copy(point: str, path: Path | None) -> None:
            if point == "after_copy_fsync" and path is not None:
                path.write_bytes(b"corrupt")

        manager.fault_hook = corrupt_after_copy
        with pytest.raises(ArchiveError, match="size mismatch"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        assert (checksum.layout.root / str(transaction["source_relative_path"])).is_file()

    unplug = prepare_archive(tmp_path / "unplug")
    with Catalog(unplug.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=unplug.layout, catalog=catalog, target=unplug.target
        )

        def disappear(point: str, _path: Path | None) -> None:
            if point == "copy_progress":
                raise OSError(19, "device disappeared")

        manager.fault_hook = disappear
        with pytest.raises(ArchiveError, match="DISAPPEARED_DURING_COPY"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        assert "DISAPPEARED_DURING_COPY" in str(transaction["last_error"])
        assert manager.status()["status"] == "DISAPPEARED_DURING_COPY"
        assert (unplug.layout.root / str(transaction["source_relative_path"])).is_file()
        failure_events = catalog.operational_events(
            event_type="ARCHIVE_ATTEMPT_FAILED"
        )
        assert len(failure_events) == 1
        assert failure_events[0]["evidence"] == {
            "attempt_count": 1,
            "catalog_state": "COPYING",
            "chunk_id": transaction["chunk_id"],
            "error": (
                "ArchiveError: DISAPPEARED_DURING_COPY: "
                "[Errno 19] device disappeared"
            ),
            "failure_kind": "DISAPPEARED_DURING_COPY",
            "source_exists": True,
            "source_preserved": True,
            "storage_id": unplug.target.storage_id,
            "transaction_id": transaction["transaction_id"],
        }

    deletion = prepare_archive(tmp_path / "delete")
    with Catalog(deletion.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=deletion.layout, catalog=catalog, target=deletion.target
        )

        def fail_before_delete(point: str, _path: Path | None) -> None:
            if point == "before_local_delete":
                raise PermissionError("injected local delete failure")

        manager.fault_hook = fail_before_delete
        with pytest.raises(ArchiveError, match="injected local delete failure"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        source = deletion.layout.root / str(transaction["source_relative_path"])
        assert source.is_file()
        assert transaction["state"] == ArchiveState.LOCAL_DELETE_PENDING
        manager.fault_hook = None
        assert manager.run_once().state == ArchiveState.LOCAL_DELETED
        assert not source.exists()


def test_conflicting_external_manifest_is_not_overwritten(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )

        def stop_after_rename(point: str, _path: Path | None) -> None:
            if point == "after_final_rename":
                raise RuntimeError("leave final without manifest")

        manager.fault_hook = stop_after_rename
        with pytest.raises(ArchiveError, match="leave final without manifest"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        source = prepared.layout.root / str(transaction["source_relative_path"])
        manifest_path = prepared.target.root / str(
            transaction["external_manifest_relative_path"]
        )
        manifest_path.parent.mkdir(exist_ok=True)
        conflicting = b'{"unrelated":"user data"}\n'
        manifest_path.write_bytes(conflicting)

        manager.fault_hook = None
        with pytest.raises(ArchiveError, match="identity mismatch"):
            manager.run_once()
        assert source.is_file()
        assert manifest_path.read_bytes() == conflicting


def test_archive_changes_nothing_outside_registered_directory(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    volume_root = prepared.target.root.parents[1]
    sibling = prepared.target.root.parent / "Unrelated"
    sibling.mkdir()
    volume_sentinel = volume_root / "volume-user-file"
    sibling_sentinel = sibling / "sibling-user-file"
    volume_sentinel.write_bytes(b"volume")
    sibling_sentinel.write_bytes(b"sibling")

    with Catalog(prepared.layout.catalog) as catalog:
        assert ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        ).run_once().state == ArchiveState.LOCAL_DELETED

    assert volume_sentinel.read_bytes() == b"volume"
    assert sibling_sentinel.read_bytes() == b"sibling"
    assert sorted(path.name for path in volume_root.iterdir()) == [
        "QuantData",
        "volume-user-file",
    ]


def test_archive_rejects_symlinked_owned_subdirectory_and_retains_source(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (prepared.target.root / "raw").symlink_to(outside, target_is_directory=True)
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(prepared.chunk_ids[0])
        assert row is not None
        source = prepared.layout.root / str(row["sealed_path"])
        with pytest.raises(ArchiveError, match="symbolic link"):
            ArchiveManager(
                layout=prepared.layout,
                catalog=catalog,
                target=prepared.target,
            ).run_once()
        assert source.is_file()
        assert list(outside.iterdir()) == []


def test_verify_all_reports_post_archive_corruption(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )
        assert manager.run_once().state == ArchiveState.LOCAL_DELETED
        transaction = catalog.archive_transactions()[0]
        external = prepared.target.root / str(transaction["target_relative_path"])
        body = bytearray(external.read_bytes())
        body[-1] ^= 0xFF
        external.write_bytes(body)

        result = manager.verify_all()
        assert result["status"] == "FAILED"
        assert result["verified_files"] == 0
        assert result["failed_files"] == 1
        files = cast(list[dict[str, object]], result["files"])
        assert files[0]["status"] == "FAILED"
        assert "SHA-256 mismatch" in str(files[0]["reason"])


def test_emergency_release_only_processes_already_verified_transaction(
    tmp_path: Path,
) -> None:
    untouched = prepare_archive(tmp_path / "untouched")
    with Catalog(untouched.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=untouched.layout, catalog=catalog, target=untouched.target
        )
        assert manager.release_verified_once().state == "NO_VERIFIED_RELEASE"
        row = catalog.chunk(untouched.chunk_ids[0])
        assert row is not None
        assert (untouched.layout.root / str(row["sealed_path"])).is_file()

    prepared = prepare_archive(tmp_path / "verified")
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )

        def stop_after_verified_commit(point: str, _path: Path | None) -> None:
            if point == "after_catalog_commit":
                raise RuntimeError("leave verified source")

        manager.fault_hook = stop_after_verified_commit
        with pytest.raises(ArchiveError, match="leave verified source"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        source = prepared.layout.root / str(transaction["source_relative_path"])
        assert source.is_file()
        assert transaction["state"] == ArchiveState.VERIFIED

        manager.fault_hook = None
        assert manager.release_verified_once().state == ArchiveState.LOCAL_DELETED
        assert not source.exists()
