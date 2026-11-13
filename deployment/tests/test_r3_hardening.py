"""Regression tests for R3-A, R3-B, and R3-C hardening."""

from __future__ import annotations

import base64
import fcntl
import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENT = ROOT / "deployment"
PROTOTYPE = ROOT / "prototype"
BASELINE_SRC = ROOT / "vendor/python-baseline/src"

for entry in (str(PROTOTYPE), str(BASELINE_SRC)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

import partition_roots  # noqa: E402
import recover_raw  # noqa: E402
from binance_market_data_recorder.archive.drain import archive_drain  # noqa: E402
from binance_market_data_recorder.domain.event import EventEnvelope  # noqa: E402
from binance_market_data_recorder.spool.writer import RawChunkWriter  # noqa: E402
from binance_market_data_recorder.storage.catalog import Catalog  # noqa: E402
from binance_market_data_recorder.storage.layout import (  # noqa: E402
    ensure_storage_layout,
)
from binance_market_data_recorder.storage.macos.model import VolumeInfo  # noqa: E402
from binance_market_data_recorder.storage.macos.registry import (  # noqa: E402
    StorageRegistry,
)
from binance_market_data_recorder.storage.platform import volume_adapter  # noqa: E402


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


migrate_metadata = _load_module(
    "migrate_metadata", DEPLOYMENT / "migrate_metadata.py"
)
archive_tick = _load_module("archive_tick", DEPLOYMENT / "archive_tick.py")
qualify = _load_module("qualify", DEPLOYMENT / "qualify.py")
read_raw = _load_module("read_raw", PROTOTYPE / "read_raw.py")


class _StubVolumes:
    def __init__(self, mountpoint: Path, volume_uuid: str = "vol-r3-test") -> None:
        self._info = VolumeInfo(
            disk_id="disk-r3",
            volume_uuid=volume_uuid,
            name="r3-archive",
            filesystem_type="apfs",
            mountpoint=mountpoint,
            writable=True,
            internal=False,
            removable=True,
            total_bytes=100 * 1024**3,
            free_bytes=50 * 1024**3,
            observed_at_utc_ns=1_000_000_000,
        )

    def inventory(self) -> list[VolumeInfo]:
        return [self._info]

    def inspect_path(self, path: Path) -> VolumeInfo:
        return self._info


def _make_partition(
    container: Path,
    partition_id: str,
    state: str,
    first_ns: int,
    last_ns: int,
    *,
    with_chunk: bool = False,
    archive_tx_state: str | None = None,
    chunk_state: str = "SEALED",
    init_stub_catalog: bool = True,
) -> Path:
    part = container / "partitions" / partition_id
    for directory in ("state", "data/active", "data/sealed", "data/manifests"):
        (part / directory).mkdir(parents=True, exist_ok=True)
    (part / "partition.json").write_text(
        json.dumps(
            {
                "schema_version": "partition.v1",
                "partition_id": partition_id,
                "created_at_utc_ns": first_ns,
                "receive_time_utc_range_ns": {"min": first_ns, "max": last_ns},
                "state": state,
            }
        ),
        encoding="utf-8",
    )
    if init_stub_catalog:
        with sqlite3.connect(part / "state/catalog.sqlite") as db:
            db.execute(
                "CREATE TABLE chunks(chunk_id TEXT PRIMARY KEY, state TEXT, "
                "created_at_utc_ns INTEGER)"
            )
            db.execute(
                "CREATE TABLE archive_transactions("
                "transaction_id TEXT PRIMARY KEY, chunk_id TEXT UNIQUE, "
                "storage_id TEXT, state TEXT, target_relative_path TEXT, "
                "verified_at_utc_ns INTEGER)"
            )
            if with_chunk:
                db.execute(
                    "INSERT INTO chunks VALUES('chunk-1', ?, ?)",
                    (chunk_state, first_ns),
                )
                if archive_tx_state is not None:
                    db.execute(
                        "INSERT INTO archive_transactions VALUES("
                        "'tx-1', 'chunk-1', 'target-1', ?, 'spot/chunk-1.bmdr.zst', ?)",
                        (
                            archive_tx_state,
                            None if archive_tx_state == "COPYING" else first_ns + 1,
                        ),
                    )
            db.commit()
    return part


def _write_real_chunk(
    part_root: Path,
    *,
    market: str = "spot",
    symbol: str = "BTCUSDT",
    stream: str = "agg_trade",
    payload: bytes = b'{"e":"aggTrade","s":"BTCUSDT","a":1,"p":"100","q":"1","f":1,"l":1,"T":1000,"m":false}',
    receive_ns: int = 1_762_992_000_000_000_000,
    seal: bool = True,
) -> dict[str, object]:
    layout = ensure_storage_layout(part_root)
    env = EventEnvelope(
        market=market,
        symbol=symbol,
        stream=stream,
        module="binance.public.websocket.v1",
        connection_id="conn-1",
        collector_instance_id="inst-1",
        collector_version="test",
        receive_time_utc_ns=receive_ns,
        receive_monotonic_ns=1000,
        payload_encoding="utf-8-json",
        raw_payload=payload,
    )
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market=market,
            symbol=symbol,
            stream=stream,
            collector_instance_id="inst-1",
            collector_version="test",
            created_at_utc_ns=receive_ns,
        )
        writer.append(env)
        writer.sync()
        if seal:
            return writer.seal()
        # Leave partial unsealed intentionally for crash recovery tests.
        os.close(writer._descriptor)
        writer._closed = True
        return {"chunk_id": str(writer.header.chunk_id)}


def _subprocess_env() -> dict[str, str]:
    existing = os.environ.get("PYTHONPATH", "")
    parts = [str(BASELINE_SRC), str(PROTOTYPE)]
    if existing:
        parts.append(existing)
    return {**os.environ, "PYTHONPATH": os.pathsep.join(parts)}


def test_active_c1_lock_blocks_recover_raw_and_recover_root_subprocess(
    tmp_path: Path,
) -> None:
    container = tmp_path / "spot"
    part = _make_partition(
        container, "p-open", "open", 100, 200, init_stub_catalog=False
    )
    (container / "CURRENT").write_text("p-open\n", encoding="utf-8")
    _write_real_chunk(part, seal=False)
    lock_file = container / "state/c1.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with lock_file.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            recover_raw.recover(container)
        child = subprocess.run(
            [sys.executable, str(DEPLOYMENT / "recover_root.py"), str(container)],
            capture_output=True,
            text=True,
            env=_subprocess_env(),
            check=False,
        )
        assert child.returncode != 0


def test_recover_root_subprocess_recovers_open_and_closed_partitions(
    tmp_path: Path,
) -> None:
    container = tmp_path / "spot"
    p_closed = _make_partition(
        container, "p-2025-11-11", "closed", 10, 90, init_stub_catalog=False
    )
    p_stale_open = _make_partition(
        container, "p-2025-11-12", "open", 100, 200, init_stub_catalog=False
    )
    p_open = _make_partition(
        container, "p-2025-11-13", "open", 300, 400, init_stub_catalog=False
    )
    (container / "CURRENT").write_text("p-2025-11-13\n", encoding="utf-8")
    (container / "state").mkdir(parents=True, exist_ok=True)
    (container / "state/c1.lock").write_text("", encoding="utf-8")
    _write_real_chunk(p_closed, receive_ns=50, seal=True)
    _write_real_chunk(p_stale_open, receive_ns=150, seal=False)
    _write_real_chunk(p_open, receive_ns=350, seal=False)

    child = subprocess.run(
        [sys.executable, str(DEPLOYMENT / "recover_root.py"), str(container)],
        capture_output=True,
        text=True,
        env=_subprocess_env(),
        check=True,
    )
    decoder = json.JSONDecoder()
    text = child.stdout.lstrip()
    outputs = []
    while text:
        obj, end = decoder.raw_decode(text)
        outputs.append(obj)
        text = text[end:].lstrip()
    assert len(outputs) == 2
    assert list((p_stale_open / "data/active").glob("*")) == []
    assert list((p_open / "data/active").glob("*")) == []
    assert len(list((p_closed / "data/sealed").glob("*.bmdr.zst"))) == 1
    assert len(list((p_stale_open / "data/sealed").glob("*.bmdr.zst"))) == 1
    assert len(list((p_open / "data/sealed").glob("*.bmdr.zst"))) == 1


def test_partition_roots_ignores_partial_nonclosed_mismatch_and_rejects_missing_cold(
    tmp_path: Path,
) -> None:
    container = tmp_path / "hot" / "spot"
    p1_hot = _make_partition(container, "p1", "closed", 0, 100)
    p2_open = _make_partition(container, "p2", "open", 100, 200)
    (container / "CURRENT").write_text("p2\n", encoding="utf-8")

    cold = tmp_path / "cold"
    cold.mkdir()
    # Invalid cold entries that MUST be strictly ignored:
    partial_dir = cold / "p1.partial"
    partial_dir.mkdir()
    (partial_dir / "partition.json").write_text(
        json.dumps(
            {
                "schema_version": "partition.v1",
                "partition_id": "p1",
                "receive_time_utc_range_ns": {"min": 0, "max": 100},
                "state": "closed",
            }
        ),
        encoding="utf-8",
    )
    open_cold = cold / "p3"
    open_cold.mkdir()
    (open_cold / "partition.json").write_text(
        json.dumps(
            {
                "schema_version": "partition.v1",
                "partition_id": "p3",
                "receive_time_utc_range_ns": {"min": 0, "max": 100},
                "state": "open",
            }
        ),
        encoding="utf-8",
    )
    mismatch_cold = cold / "wrong-dir"
    mismatch_cold.mkdir()
    (mismatch_cold / "partition.json").write_text(
        json.dumps(
            {
                "schema_version": "partition.v1",
                "partition_id": "p4",
                "receive_time_utc_range_ns": {"min": 0, "max": 100},
                "state": "closed",
            }
        ),
        encoding="utf-8",
    )

    assert partition_roots.partition_roots(container, cold_root=cold) == [
        p1_hot,
        p2_open,
    ]
    assert partition_roots.resolve_roots(container, cold_root=cold) == [
        p1_hot,
        p2_open,
    ]

    missing_cold = tmp_path / "does-not-exist"
    with pytest.raises(ValueError, match="cold root is unavailable"):
        partition_roots.partition_roots(container, cold_root=missing_cold)
    with pytest.raises(ValueError, match="cold root is unavailable"):
        partition_roots.resolve_roots(container, cold_root=missing_cold)


def test_read_raw_reads_archived_raw_from_cold_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    container = tmp_path / "hot" / "spot"
    p1 = _make_partition(
        container, "p1", "closed", 10, 200, init_stub_catalog=False
    )
    p2 = _make_partition(
        container, "p2", "open", 200, 300, init_stub_catalog=False
    )
    (container / "CURRENT").write_text("p2\n", encoding="utf-8")
    with Catalog(ensure_storage_layout(p2).catalog):
        pass

    payload = b'{"e":"aggTrade","s":"BTCUSDT","a":42,"p":"50000.5","q":"0.2","f":1,"l":1,"T":120,"m":false}'
    _write_real_chunk(p1, payload=payload, receive_ns=150, seal=True)

    ext_mount = tmp_path / "ext"
    ext_slot = ext_mount / "archive_slot"
    ext_slot.mkdir(parents=True)
    volumes = _StubVolumes(ext_mount)
    monkeypatch.setattr(read_raw, "volume_adapter", lambda: volumes)
    monkeypatch.setattr(migrate_metadata, "volume_adapter", lambda: volumes, raising=False)

    layout = ensure_storage_layout(p1)
    with Catalog(layout.catalog) as catalog:
        reg = StorageRegistry(catalog=catalog, volumes=volumes)
        registered = reg.register(ext_slot)
        storage_id = str(registered["storage_id"])
        archive_drain(
            layout=layout,
            catalog=catalog,
            storage_id=storage_id,
            max_runtime_seconds=30.0,
            max_files=10,
            volumes=volumes,
        )
    assert list((p1 / "data/sealed").glob("*")) == []

    cold = tmp_path / "cold"
    # Monkeypatch volume_adapter inside migrate_metadata's _verify_external_targets import path.
    import binance_market_data_recorder.storage.platform as platform_mod

    monkeypatch.setattr(platform_mod, "volume_adapter", lambda: volumes)
    report = migrate_metadata.migrate(container, cold)
    assert report["migrated"] == ["p1"]
    assert not p1.exists()
    assert (cold / "p1" / "partition.json").is_file()

    monkeypatch.setattr(
        sys,
        "argv",
        ["read_raw.py", str(container), "--cold-root", str(cold)],
    )
    read_raw.main()
    lines = [
        json.loads(line)
        for line in capsys.readouterr().out.strip().splitlines()
        if line.strip()
    ]
    assert len(lines) == 1
    assert base64.b64decode(lines[0]["raw_payload_base64"]) == payload


def test_qualify_max_wal_bytes_tracks_max_across_partitions_and_legacy(
    tmp_path: Path,
) -> None:
    market_root = tmp_path / "spot"
    p1_wal = market_root / "partitions/p1/state/catalog.sqlite-wal"
    p2_wal = market_root / "partitions/p2/state/catalog.sqlite-wal"
    p1_wal.parent.mkdir(parents=True)
    p2_wal.parent.mkdir(parents=True)
    p1_wal.write_bytes(b"a" * 128)
    p2_wal.write_bytes(b"b" * 1024)
    assert qualify.max_wal_bytes(market_root) == 1024

    legacy = tmp_path / "legacy_spot"
    (legacy / "state").mkdir(parents=True)
    (legacy / "state/catalog.sqlite-wal").write_bytes(b"c" * 512)
    assert qualify.max_wal_bytes(legacy) == 512


def test_archive_ticks_archive_open_and_closed_while_migrate_moves_only_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    container = tmp_path / "usdm"
    _make_partition(container, "p-closed", "closed", 10, 100)
    _make_partition(container, "p-open", "open", 100, 200)
    (container / "CURRENT").write_text("p-open\n", encoding="utf-8")

    archived_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        archive_tick,
        "ensure_archive_target",
        lambda partition, target, env: {"status": "skipped", "reason": "test"},
    )

    def _fake_run_archive(partition: Path, env: dict) -> dict:
        meta = partition_roots.partition_metadata(partition)
        archived_calls.append((meta["partition_id"], str(meta["state"])))
        return {"failed": False, "exit_code": 0}

    monkeypatch.setattr(archive_tick, "run_archive", _fake_run_archive)

    results, failed = archive_tick.archive_partitions(container, "usdm", None, {})
    assert not failed
    assert archived_calls == [("p-closed", "closed"), ("p-open", "open")]
    assert [r["state"] for r in results] == ["closed", "open"]

    cold = tmp_path / "cold"
    mig = migrate_metadata.migrate(container, cold)
    assert mig["migrated"] == ["p-closed"]
    assert {"partition_id": "p-open", "reason": "open"} in mig["skipped"]
    assert (container / "partitions/p-open").is_dir()


def test_migrate_refuses_sealed_chunks_with_empty_or_incomplete_archive_transactions(
    tmp_path: Path,
) -> None:
    container = tmp_path / "hot" / "spot"
    part = _make_partition(
        container,
        "p-sealed-unarchived",
        "closed",
        10,
        100,
        with_chunk=True,
        archive_tx_state=None,
        chunk_state="SEALED",
    )
    part_q = _make_partition(
        container,
        "p-quarantined",
        "closed",
        200,
        300,
        with_chunk=True,
        archive_tx_state="LOCAL_DELETED",
        chunk_state="QUARANTINED",
    )
    cold = tmp_path / "cold"
    report = migrate_metadata.migrate(container, cold)
    assert report["migrated"] == []
    reasons = {item["partition_id"]: item["reason"] for item in report["skipped"]}
    assert reasons["p-sealed-unarchived"] == "raw-archive-pending"
    assert reasons["p-quarantined"] == "quarantined-chunks"
    assert part.exists()
    assert part_q.exists()

    # Remaining local file in data/sealed also blocks migration.
    part_local = _make_partition(
        container,
        "p-local-file",
        "closed",
        400,
        500,
        with_chunk=True,
        archive_tx_state="LOCAL_DELETED",
        chunk_state="LOCAL_DELETED",
    )
    (part_local / "data/sealed/chunk-1.bmdr.zst").write_bytes(b"still-here")
    report2 = migrate_metadata.migrate(container, cold)
    reasons2 = {item["partition_id"]: item["reason"] for item in report2["skipped"]}
    assert reasons2["p-local-file"] == "local-sealed-remaining"
    assert part_local.exists()


def test_reader_lock_consolidation_blocks_migrate_and_no_metadata_move_lock(
    tmp_path: Path,
) -> None:
    container = tmp_path / "spot"
    _make_partition(container, "p1", "closed", 0, 100)
    cold = tmp_path / "cold"

    with partition_roots.reader_lock(container, exclusive=False):
        report = migrate_metadata.migrate(container, cold)
        assert report["status"] == "BUSY"
        assert report["reason"] == "reader-active"
        assert report["migrated"] == []
        parts, _failed = archive_tick.archive_partitions(container, "spot", None, {})
        assert parts == [{"status": "busy-reader"}]

    report_ok = migrate_metadata.migrate(container, cold)
    assert report_ok["status"] == "OK"
    assert report_ok["migrated"] == ["p1"]
    assert not (container / "state/metadata-move.lock").exists()


def test_preflight_rejects_bad_cold_root_and_low_space_before_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    container = tmp_path / "spot"
    _make_partition(container, "p1", "closed", 0, 100)

    symlink_cold = tmp_path / "link_cold"
    target = tmp_path / "real_cold"
    target.mkdir()
    symlink_cold.symlink_to(target)
    with pytest.raises(ValueError, match="symlink"):
        migrate_metadata.migrate(container, symlink_cold)

    unmounted = Path("/Volumes/DEFINITELY_NOT_MOUNTED_R3_TEST/metadata/spot")
    with pytest.raises(ValueError, match="unavailable"):
        migrate_metadata.migrate(container, unmounted)

    cold = tmp_path / "low_space_cold"
    monkeypatch.setattr(
        shutil,
        "disk_usage",
        lambda _path: shutil._ntuple_diskusage(1000, 999, 1),
    )
    with pytest.raises(ValueError, match="insufficient cold_root free space"):
        migrate_metadata.migrate(container, cold)
    assert not (cold / "p1.partial").exists()


def test_migrate_fsyncs_all_files_and_dirs_before_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    container = tmp_path / "spot"
    part = _make_partition(container, "p1", "closed", 0, 100)
    (part / "data/manifests/c1.manifest.json").write_text(
        json.dumps({"chunk_id": "c1"}), encoding="utf-8"
    )
    with sqlite3.connect(part / "state/catalog.sqlite") as db:
        db.execute("INSERT INTO chunks VALUES('c1', 'LOCAL_DELETED', 10)")
        db.execute(
            "INSERT INTO archive_transactions VALUES("
            "'tx1', 'c1', 't1', 'LOCAL_DELETED', 'spot/c1.bmdr.zst', 11)"
        )
        db.commit()

    cold = tmp_path / "cold"
    fsynced_paths: list[str] = []
    orig_open = os.open
    orig_fsync = os.fsync
    orig_close = os.close
    fd_to_path: dict[int, str] = {}

    def spy_open(path, flags, *args, **kwargs):
        fd = orig_open(path, flags, *args, **kwargs)
        fd_to_path[fd] = str(path)
        return fd

    def spy_fsync(fd: int) -> None:
        if fd in fd_to_path:
            fsynced_paths.append(fd_to_path[fd])
        orig_fsync(fd)

    def spy_close(fd: int) -> None:
        fd_to_path.pop(fd, None)
        orig_close(fd)

    monkeypatch.setattr(os, "open", spy_open)
    monkeypatch.setattr(os, "fsync", spy_fsync)
    monkeypatch.setattr(os, "close", spy_close)

    report = migrate_metadata.migrate(container, cold)
    assert report["migrated"] == ["p1"]
    partial_prefix = str(cold / "p1.partial")
    assert any(p.endswith("p1.partial/partition.json") for p in fsynced_paths)
    assert any(p.endswith("p1.partial/state/catalog.sqlite") for p in fsynced_paths)
    assert any(p.endswith("p1.partial/data/manifests/c1.manifest.json") for p in fsynced_paths)
    assert partial_prefix in fsynced_paths
    assert str(cold) in fsynced_paths


def test_published_copy_retry_handles_match_and_rejects_mismatch(
    tmp_path: Path,
) -> None:
    container = tmp_path / "spot"
    part = _make_partition(
        container,
        "p1",
        "closed",
        0,
        100,
        with_chunk=True,
        archive_tx_state="LOCAL_DELETED",
        chunk_state="LOCAL_DELETED",
    )
    (part / "data/manifests/chunk-1.manifest.json").write_text(
        json.dumps({"chunk_id": "chunk-1"}), encoding="utf-8"
    )
    cold = tmp_path / "cold"
    cold.mkdir()
    dest = cold / "p1"
    shutil.copytree(part, dest)
    (cold / "p1.partial").mkdir()

    # Exact published match: finishes cleanup of hot source and stale .partial.
    report = migrate_metadata.migrate(container, cold)
    assert report["migrated"] == ["p1"]
    assert not part.exists()
    assert not (cold / "p1.partial").exists()
    assert (dest / "partition.json").is_file()

    # Mismatched cold copy: hot source preserved and explicit error raised.
    container2 = tmp_path / "spot2"
    part2 = _make_partition(
        container2,
        "p2",
        "closed",
        0,
        100,
        with_chunk=True,
        archive_tx_state="LOCAL_DELETED",
        chunk_state="LOCAL_DELETED",
    )
    cold2 = tmp_path / "cold2"
    cold2.mkdir()
    dest2 = cold2 / "p2"
    shutil.copytree(part2, dest2)
    (dest2 / "partition.json").write_text(
        json.dumps(
            {
                "schema_version": "partition.v1",
                "partition_id": "p2",
                "created_at_utc_ns": 999,
                "receive_time_utc_range_ns": {"min": 999, "max": 999},
                "state": "closed",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="mismatch"):
        migrate_metadata.migrate(container2, cold2)
    assert part2.exists()
