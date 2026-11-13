from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "recover_root.py"
SPEC = importlib.util.spec_from_file_location("recover_root", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
recover_root = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = recover_root
SPEC.loader.exec_module(recover_root)


def _partition(root: Path, name: str, state: str) -> Path:
    path = root / "partitions" / name
    path.mkdir(parents=True)
    (path / "partition.json").write_text(
        json.dumps({"state": state}), encoding="utf-8"
    )
    return path


def test_legacy_root_is_recovered_as_one_root(tmp_path: Path) -> None:
    root = tmp_path / "spot"
    root.mkdir()
    assert recover_root.partition_roots(root) == [root]


def test_partitioned_root_selects_current_and_unclosed(tmp_path: Path) -> None:
    root = tmp_path / "spot"
    root.mkdir()
    closed = _partition(root, "p1", "closed")
    current = _partition(root, "p2", "open")
    archived = _partition(root, "p3", "archived")
    (root / "CURRENT").write_text("p2\n", encoding="utf-8")
    selected = recover_root.partition_roots(root)
    assert selected == [current]
    assert closed not in selected
    assert archived not in selected


def test_unknown_partition_state_is_recovered(tmp_path: Path) -> None:
    root = tmp_path / "spot"
    root.mkdir()
    unknown = root / "partitions" / "p1"
    unknown.mkdir(parents=True)
    assert recover_root.partition_roots(root) == [unknown]
