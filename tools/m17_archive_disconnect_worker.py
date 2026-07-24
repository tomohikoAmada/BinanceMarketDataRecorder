#!/usr/bin/env python3
"""Pause a real ArchiveManager copy for the interactive M17 disconnect test."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from binance_market_data_recorder.archive import ArchiveManager, ArchiveTarget
from binance_market_data_recorder.config import load_config
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import (
    ensure_storage_layout,
    fsync_directory,
)

MINIMUM_PAUSE_BYTES = 32 * 1024 * 1024


def _write_status(path: Path, document: dict[str, object]) -> None:
    temporary = path.with_suffix(".partial")
    body = json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    try:
        data = body.encode("utf-8")
        view = memoryview(data)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("status write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    fsync_directory(path.parent)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage-id", required=True)
    parser.add_argument("--allowed-root", type=Path, required=True)
    parser.add_argument("--release-file", type=Path, required=True)
    parser.add_argument("--status-file", type=Path, required=True)
    args = parser.parse_args()

    allowed_root = args.allowed_root
    if allowed_root.is_symlink() or not allowed_root.is_dir():
        raise RuntimeError("allowed root is unavailable or a symbolic link")
    resolved_root = allowed_root.resolve(strict=True)
    if resolved_root != allowed_root:
        raise RuntimeError("allowed root must be supplied as its exact realpath")

    loaded = load_config()
    layout_root = loaded.config.data_root
    expected_state = layout_root / "state"
    for control_path in (args.release_file, args.status_file):
        if control_path.parent.resolve() != expected_state.resolve():
            raise RuntimeError("worker control files must stay in internal state")
    args.release_file.unlink(missing_ok=True)
    args.status_file.unlink(missing_ok=True)

    with Catalog(expected_state / "catalog.sqlite") as catalog:
        target_row = next(
            (
                row
                for row in catalog.storage_targets()
                if row["storage_id"] == args.storage_id
            ),
            None,
        )
        if target_row is None:
            raise RuntimeError("registered storage target was not found")
        target = ArchiveTarget(
            storage_id=args.storage_id,
            volume_uuid=str(target_row["volume_uuid"]),
            registered_relative_path=str(target_row["relative_path"]),
            marker_nonce=str(target_row["marker_nonce"]),
            root=resolved_root,
        )
        paused = False

        def pause_after_progress(point: str, path: Path | None) -> None:
            nonlocal paused
            if paused or point != "copy_progress" or path is None:
                return
            copied_bytes = path.stat().st_size
            if copied_bytes < MINIMUM_PAUSE_BYTES:
                return
            transaction = next(
                row
                for row in catalog.archive_transactions(
                    storage_id=args.storage_id
                )
                if row["state"] == "COPYING"
            )
            source = layout_root / str(transaction["source_relative_path"])
            final = resolved_root / str(transaction["target_relative_path"])
            paused = True
            _write_status(
                args.status_file,
                {
                    "phase": "PAUSED_COPYING",
                    "pid": os.getpid(),
                    "storage_id": args.storage_id,
                    "transaction_id": transaction["transaction_id"],
                    "chunk_id": transaction["chunk_id"],
                    "copied_bytes": copied_bytes,
                    "stored_bytes": transaction["stored_bytes"],
                    "source": str(source),
                    "source_exists": source.is_file(),
                    "source_sha256": transaction["stored_sha256"],
                    "temporary": str(path),
                    "temporary_exists": path.is_file(),
                    "final": str(final),
                    "final_exists": final.exists(),
                    "catalog_state": transaction["state"],
                    "catalog_archived": False,
                    "safe_for_disconnect_test": (
                        source.is_file()
                        and path.is_file()
                        and not final.exists()
                        and transaction["state"] == "COPYING"
                    ),
                },
            )
            while not args.release_file.is_file():
                time.sleep(0.1)

        manager = ArchiveManager(
            layout=ensure_storage_layout(layout_root),
            catalog=catalog,
            target=target,
            fault_hook=pause_after_progress,
        )
        try:
            result = manager.run_once()
        except Exception as exc:
            transaction = next(
                row
                for row in catalog.archive_transactions(
                    storage_id=args.storage_id
                )
                if row["state"] != "LOCAL_DELETED"
            )
            source = layout_root / str(transaction["source_relative_path"])
            _write_status(
                args.status_file,
                {
                    "phase": "FAILED_AFTER_RELEASE",
                    "pid": os.getpid(),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "transaction_id": transaction["transaction_id"],
                    "chunk_id": transaction["chunk_id"],
                    "catalog_state": transaction["state"],
                    "catalog_archived": False,
                    "source": str(source),
                    "source_exists": source.is_file(),
                    "last_error": transaction["last_error"],
                },
            )
            return 2
        _write_status(
            args.status_file,
            {
                "phase": "UNEXPECTED_SUCCESS",
                "pid": os.getpid(),
                "transaction_id": result.transaction_id,
                "chunk_id": result.chunk_id,
                "catalog_state": result.state,
                "catalog_archived": result.state == "LOCAL_DELETED",
            },
        )
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
