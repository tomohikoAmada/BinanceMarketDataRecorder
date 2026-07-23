from __future__ import annotations

import multiprocessing
import os
import signal
from pathlib import Path

import pytest

from binance_market_data_recorder.archive import ArchiveManager, ArchiveTarget
from binance_market_data_recorder.storage.catalog import ArchiveState, Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.archive_support import prepare_archive


def _kill_worker(
    data_root: str,
    target: ArchiveTarget,
    kill_point: str,
) -> None:
    layout = ensure_storage_layout(Path(data_root))

    def kill(point: str, _path: Path | None) -> None:
        if point == kill_point:
            os.kill(os.getpid(), signal.SIGKILL)

    with Catalog(layout.catalog) as catalog:
        ArchiveManager(
            layout=layout,
            catalog=catalog,
            target=target,
            fault_hook=kill,
        ).run_once()


@pytest.mark.parametrize(
    "kill_point",
    [
        "copy_progress",
        "verify_progress",
        "before_catalog_commit",
        "after_catalog_commit",
    ],
)
def test_kill9_never_deletes_source_and_retry_reconciles(
    tmp_path: Path, kill_point: str
) -> None:
    prepared = prepare_archive(tmp_path, payload_bytes=2 * 1024 * 1024)
    context = multiprocessing.get_context("fork")
    process = context.Process(
        target=_kill_worker,
        args=(str(prepared.layout.root), prepared.target, kill_point),
    )
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.kill()
        process.join()
        pytest.fail(f"archive worker did not reach fault point {kill_point}")
    assert process.exitcode == -signal.SIGKILL

    with Catalog(prepared.layout.catalog) as catalog:
        transaction = catalog.archive_transactions()[0]
        source = prepared.layout.root / str(transaction["source_relative_path"])
        assert source.is_file()
        recovered = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        ).run_once()
        assert recovered.state == ArchiveState.LOCAL_DELETED
        assert not source.exists()
        assert (
            ArchiveManager(
                layout=prepared.layout,
                catalog=catalog,
                target=prepared.target,
            ).verify_all()["status"]
            == "VERIFIED"
        )


def test_recovery_cleans_only_its_owned_copying_file(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, payload_bytes=2 * 1024 * 1024)
    raw_directory = prepared.target.root / "raw"
    raw_directory.mkdir()
    unrelated = raw_directory / ".someone-elses-file.copying"
    unrelated.write_bytes(b"do not touch")

    context = multiprocessing.get_context("fork")
    process = context.Process(
        target=_kill_worker,
        args=(str(prepared.layout.root), prepared.target, "copy_progress"),
    )
    process.start()
    process.join(timeout=15)
    assert process.exitcode == -signal.SIGKILL

    with Catalog(prepared.layout.catalog) as catalog:
        transaction = catalog.archive_transactions()[0]
        owned = prepared.target.root / str(transaction["target_temp_relative_path"])
        assert owned.exists()
        assert ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        ).run_once().state == ArchiveState.LOCAL_DELETED
    assert not owned.exists()
    assert unrelated.read_bytes() == b"do not touch"
