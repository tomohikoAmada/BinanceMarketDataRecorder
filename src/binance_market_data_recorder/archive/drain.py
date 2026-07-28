"""Bounded, re-entrant archive drain with a dedicated cross-process lock.

The drain calls ArchiveManager.run_once() in a loop subject to explicit
max-files and max-runtime-second bounds.  A separate lock file prevents
concurrent drain processes; if one is already running the second caller
receives ALREADY_RUNNING and exits zero.
"""

from __future__ import annotations

import fcntl
import os
import signal
import time
from pathlib import Path
from typing import Any

from ..storage.catalog import Catalog, CatalogStateError, ChunkState
from ..storage.layout import StorageLayout
from ..storage.macos import PlatformVolumeError, StorageRegistrationError, StorageRegistry
from ..storage.platform import volume_adapter
from .manager import ArchiveError, ArchiveManager

_DRAIN_LOCK_RELATIVE = "state/runtime/archive_drain.lock"


class DrainAlreadyRunning(RuntimeError):
    pass


class _DrainLock:
    def __init__(self, lock_path: Path) -> None:
        self._path = lock_path
        self._descriptor: int | None = None

    @property
    def acquired(self) -> bool:
        return self._descriptor is not None

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise RuntimeError("drain lock is already acquired")
        self._path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise DrainAlreadyRunning(
                    f"another archive drain owns {self._path}"
                ) from exc
            os.ftruncate(descriptor, 0)
            os.write(descriptor, f"{os.getpid()}\n".encode())
            os.fsync(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        self._descriptor = descriptor

    def release(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _backlog_snapshot(catalog: Catalog) -> tuple[int, int]:
    backlog = catalog.chunks_in_states(
        ChunkState.SEALED,
        ChunkState.ARCHIVE_COPYING,
        ChunkState.ARCHIVE_VERIFYING,
        ChunkState.ARCHIVED_VERIFIED,
        ChunkState.LOCAL_DELETE_PENDING,
    )
    files = len(backlog)
    total_bytes = 0
    for row in backlog:
        value = row.get("stored_bytes")
        if isinstance(value, int) and not isinstance(value, bool):
            total_bytes += value
    return files, total_bytes


def archive_drain(
    *,
    layout: StorageLayout,
    catalog: Catalog,
    storage_id: str,
    max_runtime_seconds: float,
    max_files: int,
    volumes: Any = None,
    monotonic_clock: Any = time.monotonic,
    utc_clock_ns: Any = time.time_ns,
) -> dict[str, object]:
    if max_files <= 0:
        raise ArchiveError("max-files must be greater than 0")
    if max_runtime_seconds <= 0:
        raise ArchiveError("max-runtime-seconds must be greater than 0")

    data_root = layout.root

    before_files = 0
    before_bytes = 0
    processed_files = 0
    processed_bytes = 0
    successful_transactions = 0
    failed_transactions = 0
    last_chunk_id: str | None = None
    target_state: str | None = None
    lock = _DrainLock(data_root / _DRAIN_LOCK_RELATIVE)
    try:
        lock.acquire()
    except DrainAlreadyRunning:
        return {
            "command": "archive.drain",
            "storage_id": storage_id,
            "started_at_utc_ns": utc_clock_ns(),
            "ended_at_utc_ns": utc_clock_ns(),
            "elapsed_seconds": 0.0,
            "exit_reason": "ALREADY_RUNNING",
            "processed_files": 0,
            "processed_bytes": 0,
            "successful_transactions": 0,
            "failed_transactions": 0,
            "backlog_files_before": 0,
            "backlog_bytes_before": 0,
            "backlog_files_after": 0,
            "backlog_bytes_after": 0,
            "last_chunk_id": None,
            "lock_acquired": False,
            "target_state": None,
            "contains_credentials": False,
        }

    started_at = utc_clock_ns()
    deadline = monotonic_clock() + max_runtime_seconds

    interrupted = False

    def _handle_signal(signum: int, frame: object) -> None:
        nonlocal interrupted
        interrupted = True

    prev_sigterm = signal.signal(signal.SIGTERM, _handle_signal)
    prev_sigint = signal.signal(signal.SIGINT, _handle_signal)

    try:
        adapter = volumes if volumes is not None else volume_adapter()
        registry = StorageRegistry(
            catalog=catalog, volumes=adapter
        )
        statuses = registry.statuses()
        ready = [
            s for s in statuses
            if str(s["storage_id"]) == storage_id and s["state"] in {"READY", "LOW_SPACE"}
        ]
        if not ready:
            target_state = (
                "ABSENT" if not any(
                    str(s["storage_id"]) == storage_id for s in statuses
                )
                else "NOT_READY"
            )
            if target_state == "LOW_SPACE":
                target_state = "LOW_SPACE"
            for s in statuses:
                if str(s["storage_id"]) == storage_id:
                    target_state = str(s["state"]) if s["state"] else target_state
            if target_state is None:
                target_state = "ABSENT"
            if target_state == "LOW_SPACE":
                pass  # not an error, just report
            elif target_state not in ("READY",):
                ended_at = utc_clock_ns()
                elapsed = max(0.0, (ended_at - started_at) / 1e9)
                before_files, before_bytes = _backlog_snapshot(catalog)
                return {
                    "command": "archive.drain",
                    "storage_id": storage_id,
                    "started_at_utc_ns": started_at,
                    "ended_at_utc_ns": ended_at,
                    "elapsed_seconds": elapsed,
                    "exit_reason": f"TARGET_{target_state}",
                    "processed_files": 0,
                    "processed_bytes": 0,
                    "successful_transactions": 0,
                    "failed_transactions": 0,
                    "backlog_files_before": before_files,
                    "backlog_bytes_before": before_bytes,
                    "backlog_files_after": before_files,
                    "backlog_bytes_after": before_bytes,
                    "last_chunk_id": None,
                    "lock_acquired": True,
                    "target_state": target_state,
                    "contains_credentials": False,
                }

        target_status = ready[0]
        target_state = str(target_status["state"])
        target_rows = {
            str(row["storage_id"]): row for row in catalog.storage_targets()
        }
        target_row = target_rows[storage_id]
        resolved_path = target_status.get("resolved_path")
        if not isinstance(resolved_path, str):
            raise ArchiveError("READY target lacks a resolved path")

        from .manager import ArchiveTarget
        target = ArchiveTarget(
            storage_id=storage_id,
            volume_uuid=str(target_row["volume_uuid"]),
            registered_relative_path=str(target_row["relative_path"]),
            marker_nonce=str(target_row["marker_nonce"]),
            root=Path(resolved_path),
        )

        manager = ArchiveManager(
            layout=layout,
            catalog=catalog,
            target=target,
            utc_clock_ns=utc_clock_ns,
        )

        before_files, before_bytes = _backlog_snapshot(catalog)

        while not interrupted:
            if processed_files >= max_files:
                break
            if monotonic_clock() >= deadline:
                break

            if target_state == "LOW_SPACE":
                break

            result = manager.run_once()
            if result.state == "NO_ELIGIBLE_CHUNKS":
                break
            if result.state in {"COPYING", "VERIFYING"}:
                break

            processed_files += 1
            if result.archived_bytes > 0:
                processed_bytes += result.archived_bytes
            if result.state in {
                "LOCAL_DELETED", "VERIFIED", "LOCAL_DELETE_PENDING"
            }:
                successful_transactions += 1
                last_chunk_id = result.chunk_id
            elif result.state:
                failed_transactions += 1

        exit_reason = "BACKLOG_EMPTY"
        if interrupted:
            exit_reason = "INTERRUPTED"
        elif processed_files >= max_files:
            exit_reason = "MAX_FILES"
        elif monotonic_clock() >= deadline:
            exit_reason = "DEADLINE"
        elif target_state == "LOW_SPACE":
            exit_reason = "TARGET_LOW_SPACE"

        after_files, after_bytes = _backlog_snapshot(catalog)
        ended_at = utc_clock_ns()
        elapsed = max(0.0, (ended_at - started_at) / 1e9)

        return {
            "command": "archive.drain",
            "storage_id": storage_id,
            "started_at_utc_ns": started_at,
            "ended_at_utc_ns": ended_at,
            "elapsed_seconds": elapsed,
            "exit_reason": exit_reason,
            "processed_files": processed_files,
            "processed_bytes": processed_bytes,
            "successful_transactions": successful_transactions,
            "failed_transactions": failed_transactions,
            "backlog_files_before": before_files,
            "backlog_bytes_before": before_bytes,
            "backlog_files_after": after_files,
            "backlog_bytes_after": after_bytes,
            "last_chunk_id": last_chunk_id,
            "lock_acquired": True,
            "target_state": target_state,
            "contains_credentials": False,
        }

    except (ArchiveError, CatalogStateError, OSError,
            PlatformVolumeError, StorageRegistrationError, ValueError) as exc:
        after_files, after_bytes = _backlog_snapshot(catalog)
        ended_at = utc_clock_ns()
        elapsed = max(0.0, (ended_at - started_at) / 1e9)
        return {
            "command": "archive.drain",
            "storage_id": storage_id,
            "started_at_utc_ns": started_at,
            "ended_at_utc_ns": ended_at,
            "elapsed_seconds": elapsed,
            "exit_reason": "ERROR",
            "processed_files": processed_files,
            "processed_bytes": processed_bytes,
            "successful_transactions": successful_transactions,
            "failed_transactions": failed_transactions + 1,
            "backlog_files_before": before_files,
            "backlog_bytes_before": before_bytes,
            "backlog_files_after": after_files,
            "backlog_bytes_after": after_bytes,
            "last_chunk_id": last_chunk_id,
            "lock_acquired": True,
            "target_state": target_state,
            "error_type": type(exc).__name__,
            "contains_credentials": False,
        }

    finally:
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGINT, prev_sigint)
        lock.release()
