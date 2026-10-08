from __future__ import annotations

import itertools
import json
import random
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.normalize import pipeline
from binance_market_data_recorder.normalize.parser import canonical_json


def candidates(count: int) -> list[dict[str, Any]]:
    rows = []
    for index in range(count):
        rows.append({
            "semantic_key_sha256": f"{index % 7:064x}",
            "logical_record_sha256": f"{index % 3:064x}",
            "provenance": {
                "receive_time_utc_ns": index % 5,
                "source_chunk_sha256": "a" * 64,
                "source_record_ordinal": index % 5,
                "source_subrecord_ordinal": 0,
                "collector_instance_id": "fixture",
                "connection_id": "fixture",
            },
            "row": {"market": "spot", "stream": "agg_trade", "receive_date": "2026-10-04",
                    "receive_hour": 1, "winner_marker": index},
        })
    random.Random(20261005).shuffle(rows)
    return rows


def test_hierarchical_merge_dedup_matches_full_stable_oracle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = candidates(157)
    source = tmp_path / "input.ndjson"
    source.write_bytes(b"".join((canonical_json(r) + "\n").encode() for r in rows))
    monkeypatch.setattr(pipeline, "SORT_ROWS_PER_RUN", 3)
    monkeypatch.setattr(pipeline, "MERGE_FAN_IN", 3)
    expected = sorted(rows, key=pipeline._candidate_sort_key)
    merged = list(pipeline._external_sort(source, tmp_path / "runs"))
    assert merged == expected  # includes equal-key stable winners across generations
    root = tmp_path / "partitions"
    root.mkdir()
    spools = pipeline._PartitionSpools(root, maximum_open=2)
    pipeline._deduplicate_to_partitions(merged, spools)
    spools.close()
    actual = list(pipeline._read_documents(next(iter(spools.paths.values()))))
    oracle = []
    for _, group in itertools.groupby(expected, key=lambda d: d["semantic_key_sha256"]):
        variants = [list(v) for _, v in itertools.groupby(
            group, key=lambda d: d["logical_record_sha256"]
        )]
        for variant in variants:
            ordered = sorted(variant, key=pipeline._candidate_sort_key)
            oracle.append({**ordered[0]["row"], "duplicate_count": len(ordered),
                           "duplicate_sources_json": canonical_json(
                               [d["provenance"] for d in ordered]
                           ), "identity_conflict": len(variants) > 1})
    assert actual == oracle


def test_merge_bounds_handles_and_closes_on_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = candidates(69)
    source = tmp_path / "input.ndjson"
    source.write_bytes(b"".join((canonical_json(r) + "\n").encode() for r in rows))
    monkeypatch.setattr(pipeline, "SORT_ROWS_PER_RUN", 1)
    monkeypatch.setattr(pipeline, "MERGE_FAN_IN", 3)
    opened: list[Any] = []
    peak = 0
    original = Path.open

    def tracked(path: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal peak
        handle = original(path, *args, **kwargs)
        opened.append(handle)
        peak = max(peak, sum(not h.closed for h in opened))
        return handle

    monkeypatch.setattr(Path, "open", tracked)
    merged = pipeline._external_sort(source, tmp_path / "runs")
    assert next(merged) == sorted(rows, key=pipeline._candidate_sort_key)[0]
    merged.close()
    assert peak <= 4  # three merge readers plus one intermediate writer
    assert all(h.closed for h in opened)


@pytest.mark.parametrize("broken", [b"invalid\n", b"[]\n"])
def test_work_file_errors_close_all_merge_readers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, broken: bytes
) -> None:
    valid = tmp_path / "valid.ndjson"
    valid.write_text(canonical_json(candidates(1)[0]) + "\n")
    invalid = tmp_path / "invalid.ndjson"
    invalid.write_bytes(broken)
    opened: list[Any] = []
    original = Path.open

    def tracked(path: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original(path, *args, **kwargs)
        opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", tracked)
    with pytest.raises(pipeline.NormalizationError):
        list(pipeline._merge_runs([valid, invalid]))
    assert all(h.closed for h in opened)


def test_spool_eviction_and_close_flush_exact_bytes(tmp_path: Path) -> None:
    spools = pipeline._PartitionSpools(tmp_path, maximum_open=1)
    first = ("spot", "agg_trade", "2026-10-04", "01")
    second = ("spot", "agg_trade", "2026-10-04", "02")
    row = {"value": "é", "count": 3}
    spools.write(first, row)
    spools.write(second, row)
    expected = (canonical_json(row) + "\n").encode()
    assert spools.paths[first].read_bytes() == expected
    spools.write(first, row)
    spools.close()
    assert spools.paths[first].read_bytes() == expected * 2
    assert spools.paths[second].read_bytes() == expected
    assert json.loads(expected) == row


def test_flush_failure_still_closes_all_spool_owners(tmp_path: Path) -> None:
    spools = pipeline._PartitionSpools(tmp_path)
    closed: list[int] = []

    class Handle:
        def __init__(self, number: int) -> None:
            self.number = number

        def close(self) -> None:
            closed.append(self.number)
            if self.number == 1:
                raise OSError("fixture disk full")

    for number in range(3):
        spools._open[("spot", "agg_trade", "day", str(number))] = Handle(number)  # type: ignore[assignment]
    with pytest.raises(OSError, match="disk full"):
        spools.close()
    assert sorted(closed) == [0, 1, 2]
    assert not spools._open
