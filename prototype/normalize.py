"""Offline frozen Raw -> Parquet -> deterministic replay adapter, including archives."""

import argparse
import json
from pathlib import Path

from binance_market_data_recorder.normalize import pipeline
from binance_market_data_recorder.normalize.model import public_observation_stream
from binance_market_data_recorder.normalize.parser import parse_envelope
from binance_market_data_recorder.normalize.pipeline import Normalizer
from binance_market_data_recorder.replay.catalog import ManifestCatalog
from binance_market_data_recorder.replay.model import GapPolicy, ReplayQuery
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter

ADAPTER = "sbmdr-combined-ws.v1"


def parse_capture_envelope(envelope, *, payload_sha256=None):
    """Give the frozen parser a data view; provenance/hash stay on the original wire."""
    if (
        envelope.payload_encoding == "utf-8-json-combined.v1"
        and not public_observation_stream(envelope.stream)
        and "malformed" not in envelope.capture_flags
    ):
        try:
            document = json.loads(envelope.raw_payload)
        except (ValueError, UnicodeDecodeError):
            document = None
        if isinstance(document, dict) and isinstance(document.get("data"), dict):
            envelope = envelope.model_copy(
                update={
                    "raw_payload": json.dumps(
                        document["data"], separators=(",", ":")
                    ).encode(),
                    "payload_encoding": "utf-8-json",
                }
            )
    return parse_envelope(envelope, payload_sha256=payload_sha256)


def install_adapter():
    # Pinned C0.1 pipeline: adapt only its parsing view, keep Raw/hash/build contracts.
    pipeline.parse_envelope = parse_capture_envelope


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--manifest", type=Path, action="append")
    p.add_argument("--replay-symbol", default="BTCUSDT")
    a = p.parse_args()
    install_adapter()
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
                "source_adapter": ADAPTER,
                "normalization": result.public_dict(),
                "replay_symbol": a.replay_symbol,
                "replayed_events": replayed,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
