from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROTOTYPE = ROOT / "prototype"
sys.path.insert(0, str(PROTOTYPE))

import partition_roots  # noqa: E402

SPEC = importlib.util.spec_from_file_location(
    "migrate_metadata", Path(__file__).resolve().parents[1] / "migrate_metadata.py"
)
assert SPEC is not None and SPEC.loader is not None
migrate_metadata = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migrate_metadata
SPEC.loader.exec_module(migrate_metadata)


def _partition(container: Path, name: str, state: str, first: int, last: int) -> Path:
    root = container / "partitions" / name
    for directory in ("state", "data/active", "data/sealed", "data/manifests"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    (root / "partition.json").write_text(
        json.dumps(
            {
                "schema_version": "partition.v1",
                "partition_id": name,
                "created_at_utc_ns": first,
                "receive_time_utc_range_ns": {"min": first, "max": last},
                "state": state,
            }
        ),
        encoding="utf-8",
    )
    with sqlite3.connect(root / "state/catalog.sqlite") as db:
        db.execute(
            "CREATE TABLE chunks(chunk_id TEXT PRIMARY KEY, state TEXT, "
            "created_at_utc_ns INTEGER)"
        )
        db.commit()
    return root


def test_legacy_root_resolves_to_itself(tmp_path: Path) -> None:
    root = tmp_path / "spot"
    root.mkdir()
    assert partition_roots.resolve_roots(root) == [root]


def test_closed_cold_partition_wins_and_open_stays_hot(tmp_path: Path) -> None:
    container = tmp_path / "hot" / "spot"
    container.mkdir(parents=True)
    closed = _partition(container, "p1", "closed", 0, 100)
    opened = _partition(container, "p2", "open", 100, 200)
    (container / "CURRENT").write_text("p2\n", encoding="utf-8")
    cold = tmp_path / "cold"
    cold_partition = cold / "p1"
    cold.mkdir()
    cold_partition.mkdir()
    (cold_partition / "partition.json").write_text(
        (closed / "partition.json").read_text(), encoding="utf-8"
    )
    assert partition_roots.resolve_roots(
        container, cold_root=cold, start_ns=10, end_ns=150
    ) == [cold_partition, opened]


def test_closed_partition_without_archive_transactions_can_migrate(
    tmp_path: Path,
) -> None:
    container = tmp_path / "hot" / "spot"
    container.mkdir(parents=True)
    source = _partition(container, "p1", "closed", 0, 100)
    cold = tmp_path / "cold"
    report = migrate_metadata.migrate(container, cold)
    assert report["status"] == "OK"
    assert report["migrated"] == ["p1"]
    assert (cold / "p1" / "partition.json").is_file()
    assert not source.exists()


def test_open_partition_is_not_migrated(tmp_path: Path) -> None:
    container = tmp_path / "hot" / "spot"
    container.mkdir(parents=True)
    source = _partition(container, "p1", "open", 0, 100)
    report = migrate_metadata.migrate(container, tmp_path / "cold")
    assert report["migrated"] == []
    assert report["skipped"][0]["reason"] == "open"
    assert source.exists()


def test_active_files_block_migration(tmp_path: Path) -> None:
    container = tmp_path / "hot" / "spot"
    container.mkdir(parents=True)
    source = _partition(container, "p1", "closed", 0, 100)
    (source / "data/active/partial").write_text("x", encoding="utf-8")
    report = migrate_metadata.migrate(container, tmp_path / "cold")
    assert report["migrated"] == []
    assert report["skipped"][0]["reason"] == "active-files"
    assert source.exists()


def test_cold_only_partition_is_discovered_without_hot_copy(tmp_path: Path) -> None:
    container = tmp_path / "hot" / "spot"
    container.mkdir(parents=True)
    (container / "partitions").mkdir()
    cold = tmp_path / "cold"
    cold_partition = _partition(cold, "p1", "closed", 0, 100)
    resolved = partition_roots.resolve_roots(
        container, cold_root=cold / "partitions", start_ns=10, end_ns=50
    )
    assert resolved == [cold_partition]
    assert partition_roots.partition_roots(
        container, cold_root=cold / "partitions"
    ) == [cold_partition]


def _write_manifest(partition_root: Path, name: str, first_ns: int, last_ns: int) -> Path:
    path = partition_root / "data" / "manifests" / f"{name}.manifest.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "manifest.v1",
                "receive_time_utc_range_ns": {"min": first_ns, "max": last_ns},
            }
        ),
        encoding="utf-8",
    )
    return path


def test_reconcile_closed_partition_ranges_audits_and_repairs_hot_and_cold(
    tmp_path: Path,
) -> None:
    container = tmp_path / "hot" / "spot"
    (container / "partitions").mkdir(parents=True)
    cold_root = tmp_path / "cold" / "spot"
    cold_root.mkdir(parents=True)

    # Matching closed partition must stay untouched.
    healthy = _partition(container, "p-healthy", "closed", 10, 20)
    _write_manifest(healthy, "m1", 10, 20)
    healthy_before = (healthy / "partition.json").read_text(encoding="utf-8")

    # Narrowed hot closed partition (simulates old same-day restart bug).
    narrowed_hot = _partition(container, "p-hot-narrowed", "closed", 200, 250)
    _write_manifest(narrowed_hot, "m1", 100, 150)
    _write_manifest(narrowed_hot, "m2", 200, 250)
    raw_marker = narrowed_hot / "data" / "chunks" / "chunk.bmdr"
    raw_marker.parent.mkdir(parents=True, exist_ok=True)
    raw_marker.write_bytes(b"RAW")

    # Narrowed cold-only closed partition (after cold migration).
    narrowed_cold = _partition(
        tmp_path / "cold", "p-cold-narrowed", "closed", 400, 400
    )
    # Place inside cold_root directly as migrate_metadata does.
    cold_part = cold_root / "p-cold-narrowed"
    narrowed_cold.rename(cold_part)
    _write_manifest(cold_part, "m1", 300, 350)

    # Current open partition must not be modified.
    open_part = _partition(container, "p-open", "open", 500, 500)
    _write_manifest(open_part, "m1", 450, 500)
    open_before = (open_part / "partition.json").read_text(encoding="utf-8")
    (container / "CURRENT").write_text("p-open\n", encoding="utf-8")

    # Before repair: resolve_roots for pre-restart [100, 120) misses narrowed_hot.
    assert partition_roots.resolve_roots(
        container, cold_root=cold_root, start_ns=100, end_ns=120
    ) == [open_part]

    # Read-only audit reports both mismatches without changing files.
    findings = partition_roots.reconcile_closed_partition_ranges(
        container, cold_root=cold_root, apply=False
    )
    assert [item["partition_id"] for item in findings] == [
        "p-hot-narrowed",
        "p-cold-narrowed",
    ]
    assert all(not item["repaired"] for item in findings)
    assert (
        json.loads((narrowed_hot / "partition.json").read_text(encoding="utf-8"))[
            "receive_time_utc_range_ns"
        ]["min"]
        == 200
    )
    assert (container / "state" / "reader.lock").exists()

    # Apply atomic repair.
    repaired = partition_roots.reconcile_closed_partition_ranges(
        container, cold_root=cold_root, apply=True
    )
    assert all(item["repaired"] for item in repaired)
    assert (healthy / "partition.json").read_text(encoding="utf-8") == healthy_before
    assert (open_part / "partition.json").read_text(encoding="utf-8") == open_before
    assert raw_marker.read_bytes() == b"RAW"
    assert json.loads((narrowed_hot / "partition.json").read_text(encoding="utf-8"))[
        "receive_time_utc_range_ns"
    ] == {"min": 100, "max": 250}
    assert json.loads((cold_part / "partition.json").read_text(encoding="utf-8"))[
        "receive_time_utc_range_ns"
    ] == {"min": 300, "max": 350}

    # Both pre-restart [100, 120) and cold [300, 320) windows resolve properly now.
    assert partition_roots.resolve_roots(
        container, cold_root=cold_root, start_ns=100, end_ns=120
    ) == [narrowed_hot, open_part]
    assert partition_roots.resolve_roots(
        container, cold_root=cold_root, start_ns=300, end_ns=320
    ) == [cold_part, open_part]
    assert (
        partition_roots.reconcile_closed_partition_ranges(
            container, cold_root=cold_root, apply=False
        )
        == []
    )
