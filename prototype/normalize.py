"""Offline frozen Raw -> Parquet -> deterministic replay adapter, including archives."""

import argparse
import json
from dataclasses import replace
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

from partition_roots import reader_lock, resolve_roots

ADAPTER = "sbmdr-combined-ws.v1"


def _unwrap_combined_document(envelope):
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
            return document["data"], envelope.model_copy(
                update={
                    "raw_payload": json.dumps(
                        document["data"], separators=(",", ":")
                    ).encode(),
                    "payload_encoding": "utf-8-json",
                }
            )
    try:
        decoded = json.loads(envelope.raw_payload)
    except (ValueError, UnicodeDecodeError):
        decoded = None
    return decoded if isinstance(decoded, dict) else None, envelope


def parse_capture_envelope(envelope, *, payload_sha256=None):
    """Give the frozen parser a data view; provenance/hash stay on the original wire."""
    payload_dict, unwrapped = _unwrap_combined_document(envelope)
    parsed = parse_envelope(unwrapped, payload_sha256=payload_sha256)
    if envelope.market != "um_perpetual" or "malformed" in envelope.capture_flags:
        return parsed
    if payload_dict is None:
        return parsed
    if envelope.stream == "agg_trade":
        updated = []
        for item in parsed:
            if not item.valid:
                updated.append(item)
                continue
            nq = payload_dict.get("nq") if "nq" in payload_dict else None
            if nq is not None and not isinstance(nq, str):
                raise ValueError("agg_trade nq must be string when present")
            fields = {**item.fields, "normal_quantity": nq}
            content = {**item.logical_content, "normal_quantity": nq}
            updated.append(replace(item, fields=fields, logical_content=content))
        return updated
    if envelope.stream == "mark_price":
        updated = []
        event_time = payload_dict.get("E")
        for item in parsed:
            if not item.valid:
                updated.append(item)
                continue
            ap = payload_dict.get("ap") if "ap" in payload_dict else None
            if ap is not None and not isinstance(ap, str):
                raise ValueError("mark_price ap must be string when present")
            fields = {**item.fields, "mark_price_moving_average": ap}
            content = {**item.logical_content, "mark_price_moving_average": ap}
            times = {
                "exchange_event_time_ms": (
                    event_time
                    if isinstance(event_time, int)
                    and not isinstance(event_time, bool)
                    else envelope.exchange_event_time
                ),
                "exchange_transaction_time_ms": None,
                "exchange_trade_time_ms": None,
            }
            updated.append(
                replace(
                    item,
                    fields=fields,
                    logical_content=content,
                    exchange_times=times,
                )
            )
        return updated
    return parsed


def install_adapter():
    # Pinned C0.1 pipeline: adapt only its parsing view, keep Raw/hash/build contracts.
    pipeline.parse_envelope = parse_capture_envelope


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--manifest", type=Path, action="append")
    p.add_argument("--replay-symbol", default="BTCUSDT")
    p.add_argument("--cold-root", type=Path)
    a = p.parse_args()
    install_adapter()
    with reader_lock(a.root):
        roots = resolve_roots(a.root, cold_root=a.cold_root)
        reports = []
        for physical_root in roots:
            layout = StorageLayout.from_root(physical_root)
            with Catalog(layout.catalog) as catalog:
                registry = StorageRegistry(
                    catalog=catalog, volumes=volume_adapter()
                )
                external = {
                    s["storage_id"]: Path(s["resolved_path"])
                    for s in registry.observe_statuses()
                    if s.get("state") in {"READY", "LOW_SPACE"}
                    and s.get("resolved_path")
                }
                result = Normalizer(
                    layout=layout,
                    catalog=catalog,
                    external_roots=external,
                    manifest_paths=a.manifest,
                ).run()
            if not result.build_id:
                continue
            dataset = ManifestCatalog(physical_root).open_build(result.build_id)
            replayed = sum(
                1
                for _ in dataset.replay(
                    ReplayQuery(
                        symbol=a.replay_symbol, gap_policy=GapPolicy.INCLUDE
                    )
                )
            )
            reports.append(
                {
                    "root": str(physical_root),
                    "normalization": result.public_dict(),
                    "replayed_events": replayed,
                }
            )
    if not reports:
        raise ValueError("no normalized build")
    if len(reports) == 1:
        report = reports[0]
        print(
            json.dumps(
                {
                    "source_adapter": ADAPTER,
                    "normalization": report["normalization"],
                    "replay_symbol": a.replay_symbol,
                    "replayed_events": report["replayed_events"],
                },
                indent=2,
            )
        )
        return
    print(
        json.dumps(
            {
                "source_adapter": ADAPTER,
                "roots": reports,
                "replay_symbol": a.replay_symbol,
                "replayed_events": sum(r["replayed_events"] for r in reports),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
