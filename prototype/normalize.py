"""Offline frozen Raw -> Parquet -> deterministic replay adapter, including archives."""

import argparse
import json
from pathlib import Path

from binance_market_data_recorder.normalize.pipeline import Normalizer
from binance_market_data_recorder.replay.catalog import ManifestCatalog
from binance_market_data_recorder.replay.model import GapPolicy, ReplayQuery
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--manifest", type=Path, action="append")
    p.add_argument("--replay-symbol", default="BTCUSDT")
    a = p.parse_args()
    layout = StorageLayout.from_root(a.root)
    with Catalog(layout.catalog) as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
        roots = {
            s["storage_id"]: Path(s["resolved_path"])
            for s in registry.observe_statuses()
            if s.get("state") in {"READY", "LOW_SPACE"} and s.get("resolved_path")
        }
        result = Normalizer(
            layout=layout,
            catalog=catalog,
            external_roots=roots,
            manifest_paths=a.manifest,
        ).run()
    if not result.build_id:
        raise ValueError("no normalized build")
    dataset = ManifestCatalog(a.root).open_build(result.build_id)
    replayed = sum(
        1
        for _ in dataset.replay(
            ReplayQuery(symbol=a.replay_symbol, gap_policy=GapPolicy.INCLUDE)
        )
    )
    print(
        json.dumps(
            {
                "normalization": result.public_dict(),
                "replay_symbol": a.replay_symbol,
                "replayed_events": replayed,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
