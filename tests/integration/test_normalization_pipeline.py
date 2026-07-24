from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from binance_market_data_recorder.archive import ArchiveManager
from binance_market_data_recorder.metrics.report import DailyReporter
from binance_market_data_recorder.normalize import NormalizationError, Normalizer
from binance_market_data_recorder.orderbook.checkpoint import OrderBookCheckpointStore
from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate
from binance_market_data_recorder.orderbook.reconstructor import LocalBookReconstructor
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.archive_support import prepare_archive
from tests.normalization_support import envelope, fixture, provenance, seal_events


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _synchronized_book() -> LocalBookReconstructor:
    book = LocalBookReconstructor("spot")
    book.offer(
        DepthUpdate(
            "spot",
            "BTCUSDT",
            157,
            160,
            None,
            (("1.0", "2.0"),),
            (("2.0", "3.0"),),
            1,
        )
    )
    book.synchronize(
        BookSnapshot(
            "spot",
            "BTCUSDT",
            157,
            (("1.0", "1.0"),),
            (("2.0", "1.0"),),
        )
    )
    return book


def _raw_events() -> list[list[Any]]:
    depth_model = {
        "lastUpdateId": 160,
        "bids": [["1.0", "2.0"]],
        "asks": [["2.0", "3.0"]],
    }
    side_models = {
        name: json.loads(fixture("usdm", filename))
        for name, filename in {
            "premium_index_snapshot": "premium_index.json",
            "funding_history": "funding_history.json",
            "funding_info": "funding_info.json",
            "open_interest": "open_interest.json",
            "exchange_info": "exchange_info.json",
        }.items()
    }
    groups: list[list[Any]] = [
        [
            envelope(
                market="spot",
                stream="diff_depth",
                raw_payload=fixture("spot", "diff_depth.json"),
                ordinal=1,
                source_sequence={"U": 157, "u": 160},
            )
        ],
        [
            envelope(
                market="spot",
                stream="agg_trade",
                raw_payload=fixture("spot", "agg_trade.json"),
                ordinal=2,
                source_sequence={"a": 12345, "f": 100, "l": 105},
                flags=(
                    "blue_green_overlap",
                    "deployment_id=fixture-deployment",
                    "instance_role=active",
                ),
                collector_instance_id="active-instance",
            ),
            envelope(
                market="spot",
                stream="agg_trade",
                raw_payload=b"{broken",
                ordinal=3,
                flags=("malformed",),
                collector_instance_id="active-instance",
            ),
        ],
        [
            envelope(
                market="spot",
                stream="agg_trade",
                raw_payload=fixture("spot", "agg_trade.json"),
                ordinal=4,
                source_sequence={"a": 12345, "f": 100, "l": 105},
                flags=(
                    "blue_green_overlap",
                    "deployment_id=fixture-deployment",
                    "instance_role=candidate",
                ),
                collector_instance_id="candidate-instance",
            )
        ],
        [
            envelope(
                market="spot",
                stream="book_ticker",
                raw_payload=fixture("spot", "book_ticker.json"),
                ordinal=5,
                source_sequence={"u": 400900217},
            )
        ],
        [
            envelope(
                market="spot",
                stream="depth_snapshot",
                raw_payload=provenance(
                    schema_version="binance-spot-depth-snapshot-provenance.v1",
                    model=depth_model,
                    path="/api/v3/depth",
                ),
                ordinal=6,
                module="binance.spot.rest.v1",
                source_sequence={"lastUpdateId": 160},
            )
        ],
        [
            envelope(
                market="um_perpetual",
                stream="diff_depth",
                raw_payload=fixture("usdm", "diff_depth.json"),
                ordinal=7,
                source_sequence={"U": 157, "u": 160, "pu": 149},
                flags=("sequence_gap",),
            )
        ],
        [
            envelope(
                market="um_perpetual",
                stream="agg_trade",
                raw_payload=fixture("usdm", "agg_trade.json"),
                ordinal=8,
                source_sequence={"a": 12345, "f": 100, "l": 105},
            )
        ],
        [
            envelope(
                market="um_perpetual",
                stream="book_ticker",
                raw_payload=fixture("usdm", "book_ticker.json"),
                ordinal=9,
                source_sequence={"u": 400900217},
            )
        ],
        [
            envelope(
                market="um_perpetual",
                stream="depth_snapshot",
                raw_payload=provenance(
                    schema_version="binance-usdm-depth-snapshot-provenance.v1",
                    model=depth_model,
                    path="/fapi/v1/depth",
                ),
                ordinal=10,
                module="binance.usdm.rest.v1",
                source_sequence={"lastUpdateId": 160},
            )
        ],
        [
            envelope(
                market="um_perpetual",
                stream="mark_price",
                raw_payload=fixture("usdm", "mark_price.json"),
                ordinal=11,
            )
        ],
        [
            envelope(
                market="um_perpetual",
                stream="liquidation",
                raw_payload=fixture("usdm", "liquidation.json"),
                ordinal=12,
            )
        ],
    ]
    for ordinal, (stream, model) in enumerate(side_models.items(), start=13):
        groups.append(
            [
                envelope(
                    market="um_perpetual",
                    stream=stream,
                    raw_payload=provenance(
                        schema_version="binance-usdm-side-rest-provenance.v1",
                        model=model,
                        path=f"/fixture/{stream}",
                        kind=stream,
                    ),
                    ordinal=ordinal,
                    module="binance.usdm.side_rest.v1",
                )
            ]
        )
    return groups


def test_repeated_normalization_is_deterministic_traceable_and_duckdb_readable(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        manifests = [
            seal_events(layout=layout, catalog=catalog, events=events)
            for events in _raw_events()
        ]
        raw_paths = [layout.root / str(item["relative_path"]) for item in manifests]
        raw_hashes_before = {path: _hash(path) for path in raw_paths}
        source_hashes = tuple(
            sorted(str(item["uncompressed_sha256"]) for item in manifests)
        )
        checkpoint = OrderBookCheckpointStore(layout, catalog).save(
            _synchronized_book(),
            collector_version="normalization-test",
            source_chunk_hashes=(source_hashes[0],),
            utc_clock_ns=lambda: 123,
        )

        first = Normalizer(layout=layout, catalog=catalog).run()
        assert first.status == "BUILT"
        assert first.partitions == 15
        assert first.duplicate_rows_removed == 1
        assert first.identity_conflicts == 0
        assert first.build_manifest is not None
        build_path = layout.root / first.build_manifest
        first_build_bytes = build_path.read_bytes()
        build = json.loads(first_build_bytes)
        partition_paths = [
            layout.root / entry["relative_path"] for entry in build["partitions"]
        ]
        partition_hashes = {path: _hash(path) for path in partition_paths}

        stale_partial = (
            layout.root
            / "data/normalized/normalized-dataset.v1/artifacts/.interrupted.partial"
        )
        stale_partial.write_bytes(b"incomplete derived output")
        second = Normalizer(layout=layout, catalog=catalog).run()
        assert second == first
        assert not stale_partial.exists()
        assert build_path.read_bytes() == first_build_bytes
        assert {path: _hash(path) for path in partition_paths} == partition_hashes
        assert {path: _hash(path) for path in raw_paths} == raw_hashes_before

        assert len(build["raw_sources"]) == len(manifests)
        assert {
            source["uncompressed_sha256"] for source in build["raw_sources"]
        } == set(source_hashes)
        assert build["checkpoints"][0]["relative_path"] == layout.relative(checkpoint)
        assert build["checkpoints"][0]["source_chunk_hashes"] == [source_hashes[0]]

        report = DailyReporter(
            catalog=catalog, daily_directory=layout.daily_reports
        ).build("2026-01-01", generated_at_utc_ns=1_767_312_000_000_000_000)

    rows = cast(list[dict[str, Any]], report["streams"])
    assert sum(int(row["output"]["normalized_rows"]) for row in rows) == first.normalized_rows
    assert sum(int(row["output"]["normalized_bytes"]) for row in rows) > 0

    connection = duckdb.connect()
    try:
        observed = connection.execute(
            "SELECT count(*), count(DISTINCT stream) FROM read_parquet(?, hive_partitioning=true)",
            [list(map(str, partition_paths))],
        ).fetchone()
        assert observed == (first.normalized_rows, 11)
        agg_trade = next(
            path
            for path in partition_paths
            if "market=spot" in str(path) and "stream=agg_trade" in str(path)
        )
        agg_rows = connection.execute(
            """
            SELECT valid, aggregate_trade_id, duplicate_count
            FROM read_parquet(?)
            ORDER BY valid DESC
            """,
            [str(agg_trade)],
        ).fetchall()
    finally:
        connection.close()
    assert agg_rows == [(True, 12345, 2), (False, None, 1)]

    um_depth = next(
        path
        for path in partition_paths
        if "market=um_perpetual" in str(path) and "stream=diff_depth" in str(path)
    )
    depth_row = pq.read_table(um_depth).to_pylist()[0]
    assert depth_row["source_gap"] is True
    assert depth_row["source_complete"] is False
    assert depth_row["previous_final_update_id"] == 149


def test_normalization_conflict_preserves_both_variants(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    first_payload = json.loads(fixture("spot", "book_ticker.json"))
    second_payload = dict(first_payload)
    second_payload["b"] = "99.00000000"
    with Catalog(layout.catalog) as catalog:
        seal_events(
            layout=layout,
            catalog=catalog,
            events=[
                envelope(
                    market="spot",
                    stream="book_ticker",
                    raw_payload=json.dumps(first_payload).encode(),
                    ordinal=1,
                    source_sequence={"u": 400900217},
                ),
                envelope(
                    market="spot",
                    stream="book_ticker",
                    raw_payload=json.dumps(second_payload).encode(),
                    ordinal=2,
                    source_sequence={"u": 400900217},
                ),
            ],
        )
        result = Normalizer(layout=layout, catalog=catalog).run()
        assert result.normalized_rows == 2
        assert result.identity_conflicts == 2
        assert result.build_manifest is not None
        build = json.loads((layout.root / result.build_manifest).read_text())
    table = pq.read_table(layout.root / build["partitions"][0]["relative_path"])
    assert {row["identity_conflict"] for row in table.to_pylist()} == {True}


def test_normalization_fails_closed_when_manifested_raw_is_unavailable(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        manifest = seal_events(
            layout=layout,
            catalog=catalog,
            events=[
                envelope(
                    market="spot",
                    stream="agg_trade",
                    raw_payload=fixture("spot", "agg_trade.json"),
                    ordinal=1,
                )
            ],
        )
        (layout.root / str(manifest["relative_path"])).unlink()
        with pytest.raises(NormalizationError, match="unavailable"):
            Normalizer(layout=layout, catalog=catalog).run()


def test_normalization_resolves_verified_archived_raw_by_storage_identity(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        archived = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        ).run_once()
        assert archived.state == "LOCAL_DELETED"
        result = Normalizer(
            layout=prepared.layout,
            catalog=catalog,
            external_roots={prepared.target.storage_id: prepared.target.root},
        ).run()
    assert result.status == "BUILT"
    assert result.source_chunks == 1
    assert result.normalized_rows == 1
