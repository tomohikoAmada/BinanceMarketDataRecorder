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
