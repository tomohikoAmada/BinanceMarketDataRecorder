from __future__ import annotations

import hashlib
import io
import os
import time
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import pytest
import zstandard

from binance_market_data_recorder.audit.reconnect_boundaries import _frame_document, _frame_identity
from binance_market_data_recorder.service import acceptance_v5_raw as raw_module
from binance_market_data_recorder.service.acceptance import AcceptanceError
from binance_market_data_recorder.service.acceptance_v5_io import _file_identity
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


@pytest.mark.parametrize("retire", [False, True])
def test_raw_crc_verified_even_when_outer_hashes_match(tmp_path: Path, retire: bool) -> None:
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
    calls = 0

    def progress(_size: int) -> None:
        nonlocal calls
        calls += 1
        if retire and calls == 2:
            path.unlink()

    with pytest.raises(AcceptanceError, match="CRC disagreement"):
        scan_raw(Path(unit["data_root"]), manifest["relative_path"], manifest, progress=progress)


def test_retirement_does_not_depend_on_ctime_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    unit = task(tmp_path / "data")
    root = Path(unit["data_root"])
    path = root / unit["manifest"]["relative_path"]
    monkeypatch.setattr(raw_module, "_file_identity", lambda st: (*_file_identity(st)[:4], 0))
    calls = 0

    def unlink(_size: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            path.unlink()

    with pytest.raises(raw_module.RawAuthorityPending, match="unauthorized Raw absence"):
        qualify_unit(unit, unlink)


@pytest.mark.parametrize("mutation", ["chmod", "replacement", "hardlink"])
def test_metadata_replacement_and_link_changes_are_not_retirement(
    tmp_path: Path, mutation: str
) -> None:
    unit = task(tmp_path / "data")
    root = Path(unit["data_root"])
    path = root / unit["manifest"]["relative_path"]
    original = path.read_bytes()
    if mutation == "hardlink":
        os.link(path, path.with_suffix(".retained"))
    calls = 0

    def mutate(_size: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            if mutation == "chmod":
                path.chmod(0o400)
            else:
                path.unlink()
                if mutation == "replacement":
                    path.write_bytes(original)

    with pytest.raises(AcceptanceError, match="Raw changed during qualification"):
        scan_raw(root, unit["manifest"]["relative_path"], unit["manifest"], progress=mutate)


def test_unlink_cannot_hide_statistics_disagreement(tmp_path: Path) -> None:
    unit = task(tmp_path / "data")
    root = Path(unit["data_root"])
    manifest = {**unit["manifest"], "record_count": unit["manifest"]["record_count"] + 1}
    path = root / manifest["relative_path"]
    calls = 0

    def unlink(_size: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            path.unlink()

    with pytest.raises(AcceptanceError, match="statistics/manifest disagreement"):
        scan_raw(root, manifest["relative_path"], manifest, progress=unlink)


def test_stable_chunk_materializes_only_three_boundary_identities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    envelopes = [usdm_envelope("stable", i) for i in range(1, 1001)]
    layout = ensure_storage_layout(tmp_path / "data")
    with Catalog(layout.catalog) as catalog:
        manifest = seal_chunk(layout, catalog, envelopes)
    materialized = []

    def counted(chunk_id: str, index: int, envelope: Any) -> Any:
        materialized.append(index)
        return _frame_identity(chunk_id, index, envelope)

    monkeypatch.setattr(raw_module, "_frame_identity", counted)
    proof = scan_raw(layout.root, manifest["relative_path"], manifest)
    assert materialized == [0, 999, 998]
    for key, index in (("first_frame", 0), ("last_frame", 999), ("penultimate_frame", 998)):
        assert proof[key] == _frame_document(
            _frame_identity(manifest["chunk_id"], index, envelopes[index])
        )


@pytest.mark.parametrize(
    ("previous_flags", "new_flags", "expected"),
    [
        ((), (), "UNMARKED_RECONNECT"),
        ((), ("sequence_gap",), "EXPLICIT_SEQUENCE_GAP"),
        (("sequence_gap",), (), "EXPLICIT_SEQUENCE_GAP"),
    ],
)
def test_lazy_identities_preserve_connection_boundary_evidence(
    tmp_path: Path, previous_flags: tuple[str, ...], new_flags: tuple[str, ...], expected: str
) -> None:
    envelopes = [
        usdm_envelope("old", 1),
        usdm_envelope("old", 2, previous_flags),
        usdm_envelope("new", 3, new_flags),
        usdm_envelope("new", 4),
    ]
    layout = ensure_storage_layout(tmp_path / "data")
    with Catalog(layout.catalog) as catalog:
        manifest = seal_chunk(layout, catalog, envelopes)
    proof = scan_raw(layout.root, manifest["relative_path"], manifest)
    assert proof["intra_transition_counts"][expected] == 1
    assert sum(proof["intra_transition_counts"].values()) == 1
    assert proof["intra_transition_time_ranges"][expected] == {
        "min": envelopes[2].receive_time_utc_ns,
        "max": envelopes[2].receive_time_utc_ns,
    }
    assert proof["intra_transition_digest"] != "0" * 64


def test_read_exact_fast_path_still_handles_short_reads_and_truncation() -> None:
    class ShortReader(io.BytesIO):
        def read(self, size: int | None = -1) -> bytes:
            return super().read(2 if size is None or size < 0 else min(size, 2))

    assert raw_module._read_exact(ShortReader(b"abcdef"), 6) == b"abcdef"
    assert raw_module._read_exact(ShortReader(b"abc"), 6) == b"abc"
    assert raw_module._read_exact(io.BytesIO(b"abc"), 0) == b""
