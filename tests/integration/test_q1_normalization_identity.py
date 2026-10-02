from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

import binance_market_data_recorder.normalize.model as model
import binance_market_data_recorder.normalize.pipeline as pipeline
from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.normalize.parser import sha256_json
from binance_market_data_recorder.replay import ManifestCatalog, ReplayQuery
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.normalization_support import envelope, fixture, provenance, seal_events


@pytest.mark.parametrize("kind", ["server_shutdown", "depth_snapshot", "malformed"])
def test_equal_payloads_in_different_products_and_streams_remain_separate(
    tmp_path: Path,
    kind: str,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    streams = (
        ("depth_snapshot",)
        if kind == "depth_snapshot"
        else (
            "diff_depth",
            "agg_trade",
            "book_ticker",
        )
    )
    with Catalog(layout.catalog) as catalog:
        ordinal = 0
        for market in ("spot", "um_perpetual"):
            for symbol in ("BTCUSDT", "ETHUSDT"):
                for stream in streams:
                    ordinal += 1
                    payload = b'{"e":"serverShutdown","E":1672515782136}'
                    module = None
                    if kind == "depth_snapshot":
                        market_name = "spot" if market == "spot" else "usdm"
                        module = f"binance.{'spot' if market == 'spot' else 'usdm'}.rest.v1"
                        payload = provenance(
                            schema_version=f"binance-{market_name}-depth-snapshot-provenance.v1",
                            path="/api/v3/depth" if market == "spot" else "/fapi/v1/depth",
                            model={"lastUpdateId": 160, "bids": [["1", "2"]], "asks": [["2", "3"]]},
                        )
                    elif kind == "malformed":
                        payload = b"{broken"
                    event = envelope(
                        market=market,
                        stream=stream,
                        raw_payload=payload,
                        ordinal=ordinal,
                        module=module,
                    ).model_copy(update={"symbol": symbol})
                    seal_events(
                        layout=layout,
                        catalog=catalog,
                        events=[
                            event,
                            event.model_copy(
                                update={
                                    "connection_id": f"duplicate-{ordinal}",
                                    "receive_time_utc_ns": event.receive_time_utc_ns + 1,
                                }
                            ),
                        ],
                    )
        result = Normalizer(layout=layout, catalog=catalog).run()
        assert result.build_manifest is not None
        build = json.loads((layout.root / result.build_manifest).read_bytes())
        rows = [
            row
            for item in build["partitions"]
            for row in pq.ParquetFile(layout.root / item["relative_path"]).read().to_pylist()
        ]
        assert len(rows) == 4 * len(streams)
        assert len({row["semantic_key_sha256"] for row in rows}) == len(rows)
        assert all(row["duplicate_count"] == 2 for row in rows)
        assert all(row["identity_conflict"] is False for row in rows)
        assert all(row["valid"] is (kind != "malformed") for row in rows)
        assert build["dedup_version"] == "normalized-dedup.v2"
        assert Normalizer(layout=layout, catalog=catalog).run() == result


def test_new_build_identity_preserves_and_replays_legacy_immutable_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    original_candidate = pipeline._candidate

    def legacy_candidate(record: Any, parsed: Any) -> dict[str, object]:
        row = original_candidate(record, parsed)
        row["semantic_key_sha256"] = sha256_json(parsed.semantic_identity)
        return row

    def legacy_build_id(chunks: Any, checkpoints: Any) -> str:
        return sha256_json(
            {
                "dataset_version": model.DATASET_VERSION,
                "raw_sources": [
                    {
                        "chunk_id": chunk.chunk_id,
                        "manifest_sha256": chunk.manifest_sha256,
                        "stored_sha256": chunk.manifest["stored_sha256"],
                        "uncompressed_sha256": chunk.uncompressed_sha256,
                    }
                    for chunk in chunks
                ],
                "checkpoints": [
                    {"checkpoint_id": item["checkpoint_id"], "file_sha256": item["file_sha256"]}
                    for item in checkpoints
                ],
            }
        )

    with Catalog(layout.catalog) as catalog:
        seal_events(
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
        with monkeypatch.context() as legacy:
            legacy.setattr(pipeline, "DEDUP_VERSION", "normalized-dedup.v1")
            legacy.setattr(model, "DEDUP_VERSION", "normalized-dedup.v1")
            legacy.setattr(pipeline, "_candidate", legacy_candidate)
            legacy.setattr(pipeline, "_build_id", legacy_build_id)
            old = Normalizer(layout=layout, catalog=catalog).run()
        old_files = {
            path: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (layout.root / "data/normalized").rglob("*")
            if path.is_file()
            and (path.suffix == ".parquet" or path.name.endswith(".manifest.json"))
        }
        new = Normalizer(layout=layout, catalog=catalog).run()
        assert old.build_id != new.build_id
        assert {
            path: hashlib.sha256(path.read_bytes()).hexdigest() for path in old_files
        } == old_files
        manifests = ManifestCatalog(layout.root)
        for result, version in ((old, "normalized-dedup.v1"), (new, "normalized-dedup.v2")):
            assert result.build_id is not None
            dataset = manifests.open_build(result.build_id)
            assert dataset.summary.dedup_version == version
            assert len(list(dataset.replay(ReplayQuery()))) == 1
