from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import fields
from pathlib import Path

import pytest

import binance_market_data_recorder.replay.reader as replay_reader
from binance_market_data_recorder.replay import (
    CheckpointSeekError,
    GapPolicy,
    ManifestCatalog,
    MissingExchangeTimeError,
    MissingExchangeTimePolicy,
    PartitionDescriptor,
    ReplayCatalogError,
    ReplayClock,
    ReplayEvent,
    ReplayGapError,
    ReplayQuery,
)
from tests.normalization_support import BASE_NS
from tests.replay_support import build_replay_fixture

ROOT = Path(__file__).resolve().parents[2]


def _ids(events: Sequence[ReplayEvent]) -> list[int]:
    output: list[int] = []
    for value in events:
        aggregate_id = value.row["aggregate_trade_id"]
        assert isinstance(aggregate_id, int)
        output.append(aggregate_id)
    return output


def _digest(events: Sequence[ReplayEvent]) -> str:
    rows = [
        {
            "event_time_ns": value.event_time_ns,
            "fallback": value.used_receive_time_fallback,
            "unreliable": value.is_unreliable,
            "row": dict(value.row),
        }
        for value in events
    ]
    encoded = json.dumps(
        rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def test_manifest_catalog_is_explicit_verified_and_location_independent(
    tmp_path: Path,
) -> None:
    prepared = build_replay_fixture(tmp_path)
    catalog = ManifestCatalog(prepared.layout.root)
    assert [item.build_id for item in catalog.list_builds()] == [prepared.build_id]
    dataset = catalog.open_build(prepared.build_id)
    assert dataset.summary.dataset_version == "normalized-dataset.v1"
    assert dataset.summary.source_chunk_count == 7
    assert dataset.summary.checkpoint_count == 1
    assert len(dataset.partitions()) == 4
    assert len(dataset.checkpoints()) == 1
    assert "path" not in {field.name for field in fields(PartitionDescriptor)}
    public_values = json.dumps(
        [
            {
                "market": item.market,
                "stream": item.stream,
                "stored_sha256": item.stored_sha256,
            }
            for item in dataset.partitions()
        ]
    )
    assert "/Volumes/" not in public_values
    assert str(prepared.layout.root) not in public_values


def test_receive_and_exchange_replay_have_stable_distinct_total_orders(
    tmp_path: Path,
) -> None:
    prepared = build_replay_fixture(tmp_path)
    dataset = ManifestCatalog(prepared.layout.root).open_build(prepared.build_id)
    receive_query = ReplayQuery(
        clock=ReplayClock.RECEIVE_TIME,
        markets=("spot",),
        streams=("agg_trade",),
    )
    exchange_query = ReplayQuery(
        clock=ReplayClock.EXCHANGE_TIME,
        markets=("spot",),
        streams=("agg_trade",),
    )
    receive_first = list(dataset.replay(receive_query))
    receive_second = list(dataset.replay(receive_query))
    exchange = list(dataset.replay(exchange_query))
    assert _ids(receive_first) == [3, 2, 1]
    assert _ids(exchange) == [2, 1, 3]
    assert exchange[0].event_time_ns == exchange[1].event_time_ns
    assert _digest(receive_first) == _digest(receive_second)
    assert all(item.order_version == "replay-order.v1" for item in exchange)


def test_time_range_is_half_open_in_the_selected_clock(tmp_path: Path) -> None:
    prepared = build_replay_fixture(tmp_path)
    dataset = ManifestCatalog(prepared.layout.root).open_build(prepared.build_id)
    events = list(
        dataset.replay(
            ReplayQuery(
                markets=("spot",),
                streams=("agg_trade",),
                start_time_ns=BASE_NS + 10_000_000,
                end_time_ns=BASE_NS + 20_000_000,
            )
        )
    )
    assert _ids(events) == [2]


def test_bounded_multipass_merge_keeps_the_same_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared = build_replay_fixture(tmp_path)
    dataset = ManifestCatalog(prepared.layout.root).open_build(prepared.build_id)
    monkeypatch.setattr(replay_reader, "REPLAY_BATCH_ROWS", 1)
    monkeypatch.setattr(replay_reader, "MERGE_FAN_IN", 2)
    events = list(
        dataset.replay(
            ReplayQuery(markets=("spot",), streams=("agg_trade",))
        )
    )
    assert _ids(events) == [3, 2, 1]


def test_missing_exchange_clock_requires_an_explicit_policy(tmp_path: Path) -> None:
    prepared = build_replay_fixture(tmp_path)
    dataset = ManifestCatalog(prepared.layout.root).open_build(prepared.build_id)
    with pytest.raises(MissingExchangeTimeError):
        list(
            dataset.replay(
                ReplayQuery(
                    clock=ReplayClock.EXCHANGE_TIME,
                    markets=("spot",),
                    streams=("book_ticker",),
                )
            )
        )
    assert list(
        dataset.replay(
            ReplayQuery(
                clock=ReplayClock.EXCHANGE_TIME,
                markets=("spot",),
                streams=("book_ticker",),
                missing_exchange_time=MissingExchangeTimePolicy.EXCLUDE,
            )
        )
    ) == []
    fallback = list(
        dataset.replay(
            ReplayQuery(
                clock=ReplayClock.EXCHANGE_TIME,
                markets=("spot",),
                streams=("book_ticker",),
                missing_exchange_time=(
                    MissingExchangeTimePolicy.FALLBACK_RECEIVE
                ),
            )
        )
    )
    assert len(fallback) == 1
    assert fallback[0].event_time_ns == BASE_NS + 30_000_000
    assert fallback[0].used_receive_time_fallback is True


def test_gap_policy_errors_includes_or_excludes_without_hiding_flag(
    tmp_path: Path,
) -> None:
    prepared = build_replay_fixture(tmp_path)
    dataset = ManifestCatalog(prepared.layout.root).open_build(prepared.build_id)
    with pytest.raises(ReplayGapError):
        list(
            dataset.replay(
                ReplayQuery(
                    markets=("um_perpetual",),
                    streams=("diff_depth",),
                )
            )
        )
    included = list(
        dataset.replay(
            ReplayQuery(
                markets=("um_perpetual",),
                streams=("diff_depth",),
                gap_policy=GapPolicy.INCLUDE,
            )
        )
    )
    assert len(included) == 1
    assert included[0].is_unreliable is True
    assert included[0].row["source_gap"] is True
    assert list(
        dataset.replay(
            ReplayQuery(
                markets=("um_perpetual",),
                streams=("diff_depth",),
                gap_policy=GapPolicy.EXCLUDE,
            )
        )
    ) == []


def test_verified_checkpoint_seek_returns_only_later_depth_updates(
    tmp_path: Path,
) -> None:
    prepared = build_replay_fixture(tmp_path)
    dataset = ManifestCatalog(prepared.layout.root).open_build(prepared.build_id)
    checkpoint = dataset.checkpoint(prepared.checkpoint_id)
    assert checkpoint.market == "spot"
    assert checkpoint.update_id == 160
    assert checkpoint.book["update_id"] == 160
    events = list(
        dataset.replay(
            ReplayQuery(
                markets=("spot",),
                streams=("diff_depth",),
                checkpoint_id=prepared.checkpoint_id,
            )
        )
    )
    assert [item.row["final_update_id"] for item in events] == [161]
    with pytest.raises(CheckpointSeekError, match="single market"):
        list(
            dataset.replay(
                ReplayQuery(
                    streams=("diff_depth",),
                    checkpoint_id=prepared.checkpoint_id,
                )
            )
        )


def test_unselected_orphan_artifact_cannot_enter_replay(tmp_path: Path) -> None:
    prepared = build_replay_fixture(tmp_path)
    catalog = ManifestCatalog(prepared.layout.root)
    dataset = catalog.open_build(prepared.build_id)
    query = ReplayQuery(markets=("spot",), streams=("agg_trade",))
    expected = _digest(list(dataset.replay(query)))
    orphan = (
        prepared.layout.root
        / "data/normalized/normalized-dataset.v1/artifacts/orphan.parquet"
    )
    orphan.write_bytes(b"not selected by a build manifest")
    observed = _digest(
        list(ManifestCatalog(prepared.layout.root).open_build(prepared.build_id).replay(query))
    )
    assert observed == expected


def test_partition_corruption_is_rejected_before_replay(tmp_path: Path) -> None:
    prepared = build_replay_fixture(tmp_path)
    build_path = (
        prepared.layout.root
        / "data/normalized/normalized-dataset.v1/builds"
        / f"{prepared.build_id}.manifest.json"
    )
    build = json.loads(build_path.read_text(encoding="utf-8"))
    artifact = prepared.layout.root / build["partitions"][0]["relative_path"]
    artifact.write_bytes(artifact.read_bytes() + b"tamper")
    with pytest.raises(ReplayCatalogError, match="size mismatch"):
        ManifestCatalog(prepared.layout.root).open_build(prepared.build_id)


def test_independent_example_uses_the_public_contract_only(tmp_path: Path) -> None:
    prepared = build_replay_fixture(tmp_path)
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(ROOT / "src")
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "examples" / "replay_consumer.py"),
            "--data-root",
            str(prepared.layout.root),
            "--build-id",
            prepared.build_id,
            "--market",
            "spot",
            "--stream",
            "agg_trade",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["consumer_contract_version"] == "consumer-contract.v1"
    assert result["build_id"] == prepared.build_id
    assert result["event_count"] == 3
    example = (ROOT / "examples" / "replay_consumer.py").read_text(encoding="utf-8")
    assert "binance_market_data_recorder.replay import" in example
    assert "binance_market_data_recorder.storage" not in example
    assert "binance_market_data_recorder.normalize" not in example
