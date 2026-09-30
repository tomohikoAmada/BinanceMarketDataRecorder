from __future__ import annotations

import hashlib
import time
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import pytest
import zstandard

from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_raw import (
    AuditInterrupted,
    cancellable_unit,
    qualify_unit,
    scan_raw,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_historical_reconnect_audit import seal_chunk, usdm_envelope


def blocked_worker(_pipe: Connection, _task: dict[str, Any]) -> None:
    time.sleep(60)


def task(root: Path) -> dict[str, Any]:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        manifest = seal_chunk(layout, catalog, [usdm_envelope("one", 1), usdm_envelope("one", 2)])
        row = catalog.chunk(manifest["chunk_id"])
        assert row is not None
    from binance_market_data_recorder.service.acceptance import sha256_bytes

    return {
        "data_root": str(root),
        "manifest": manifest,
        "chunk": row,
        "manifest_path": row["manifest_path"],
        "manifest_sha256": sha256_bytes((root / str(row["manifest_path"])).read_bytes()),
        "archive": None,
    }


def test_raw_stream_statistics_and_worker(tmp_path: Path) -> None:
    unit = task(tmp_path / "data")
    expected = qualify_unit(unit)
    assert expected["local"]["first_frame"]["frame_index"] == 0
    assert expected["local"]["last_frame"]["frame_index"] == 1
    assert expected["local"]["intra_transition_counts"]["UNMARKED_RECONNECT"] == 0
    assert cancellable_unit(unit, remaining_ns=10_000_000_000) == expected


def test_raw_loss_and_corruption_fail_closed(tmp_path: Path) -> None:
    unit = task(tmp_path / "data")
    raw = Path(unit["data_root"]) / unit["manifest"]["relative_path"]
    original = raw.read_bytes()
    raw.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
    with pytest.raises(AcceptanceError, match="SHA disagreement"):
        scan_raw(Path(unit["data_root"]), unit["manifest"]["relative_path"], unit["manifest"])
    raw.unlink()
    with pytest.raises(AcceptanceError, match="unauthorized Raw absence"):
        qualify_unit(unit)


def test_cancellation_does_not_acknowledge_unit() -> None:
    # Deterministic injected BOOTTIME progression, no CI wall-clock threshold.
    ticks = iter([0, 1, 240_000_000_000])
    assert (
        cancellable_unit(
            {},
            remaining_ns=240_000_000_000,
            time_ns=lambda: next(ticks),
            worker=blocked_worker,
        )
        is None
    )


def test_no_remaining_budget_does_not_spawn() -> None:
    assert cancellable_unit({}, remaining_ns=0, worker=blocked_worker) is None


def test_terminal_watchdog_is_interruption_not_integrity_pass() -> None:
    ticks = iter([0, 1, 900_000_000_001])
    with pytest.raises(AuditInterrupted, match="watchdog"):
        cancellable_unit({}, remaining_ns=None, time_ns=lambda: next(ticks), worker=blocked_worker)


def test_raw_crc_verified_even_when_outer_hashes_match(tmp_path: Path) -> None:
    unit = task(tmp_path / "data")
    manifest = unit["manifest"]
    path = Path(unit["data_root"]) / manifest["relative_path"]
    decoded = bytearray(zstandard.ZstdDecompressor().decompress(path.read_bytes()))
    decoded[-1] ^= 1
    stored = zstandard.ZstdCompressor().compress(decoded)
    path.write_bytes(stored)
    manifest.update(
        stored_sha256=hashlib.sha256(stored).hexdigest(),
        stored_bytes=len(stored),
        uncompressed_sha256=hashlib.sha256(decoded).hexdigest(),
    )
    with pytest.raises(AcceptanceError, match="CRC disagreement"):
        scan_raw(Path(unit["data_root"]), manifest["relative_path"], manifest)
