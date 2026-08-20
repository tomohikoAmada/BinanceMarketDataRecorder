from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.service import state as state_module
from binance_market_data_recorder.service.state import (
    SERVICE_STATE_SCHEMA,
    ServiceStateStore,
)


def _document(writer: str, sequence: int) -> dict[str, object]:
    return {
        "schema_version": SERVICE_STATE_SCHEMA,
        "status": "RUNNING",
        "writer": writer,
        "sequence": sequence,
    }


def test_eexist_does_not_remove_another_writers_temp_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ServiceStateStore(tmp_path / "service_state.json")
    collision_hex = "0123456789abcdef0123456789abcdef"
    collision_path = tmp_path / f".service_state.json.{collision_hex}.partial"
    sentinel = b"owned by another writer"
    collision_path.write_bytes(sentinel)
    monkeypatch.setattr(
        cast(Any, state_module),
        "uuid4",
        lambda: SimpleNamespace(hex=collision_hex),
    )

    with pytest.raises(FileExistsError):
        store.write(_document("collision", 0))

    assert collision_path.read_bytes() == sentinel
    assert not store.path.exists()


def test_concurrent_writes_publish_complete_documents_from_unique_temps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ServiceStateStore(tmp_path / "service_state.json")
    submitted = [_document("first", 1), _document("second", 2)]
    replace_sources: list[Path] = []
    source_inodes: list[int] = []
    replace_barrier = Barrier(2)
    original_replace = os.replace

    def synchronized_replace(
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
    ) -> None:
        source_path = Path(source)
        assert Path(destination) == store.path
        replace_sources.append(source_path)
        source_inodes.append(source_path.stat().st_ino)
        replace_barrier.wait(timeout=5)
        original_replace(source, destination)

    monkeypatch.setattr(os, "replace", synchronized_replace)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(store.write, document) for document in submitted]
        for future in futures:
            future.result()

    assert len(replace_sources) == 2
    assert len(set(replace_sources)) == 2
    assert len(set(source_inodes)) == 2
    final = store.read()
    assert final in submitted
    assert list(tmp_path.glob(f".{store.path.name}.*.partial")) == []
    assert json.loads(store.path.read_text(encoding="utf-8")) == final


def test_repeated_concurrent_writes_never_publish_mixed_json(tmp_path: Path) -> None:
    store = ServiceStateStore(tmp_path / "service_state.json")
    with ThreadPoolExecutor(max_workers=6) as executor:
        for round_number in range(20):
            submitted = [
                _document(f"writer-{writer}", round_number * 6 + writer)
                for writer in range(6)
            ]
            futures = [executor.submit(store.write, document) for document in submitted]
            for future in futures:
                future.result()
            final = store.read()
            assert final in submitted
            assert json.loads(store.path.read_text(encoding="utf-8")) == final

    assert list(tmp_path.glob(f".{store.path.name}.*.partial")) == []
