"""C4-C offline consumer check: one symbol/hour normalize + aggTrade 1m candles."""

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
for path in (REPO / "vendor/python-baseline/src", REPO / "prototype"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from candles import aggregate
from normalize import parse_capture_envelope

from binance_market_data_recorder.normalize.raw import (
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--market", required=True)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--interval-seconds", type=int, default=60)
    args = parser.parse_args()
    if not 0 < args.end_ms - args.start_ms <= 3600 * 1000:
        parser.error("require a positive range of at most one hour")
    layout = StorageLayout.from_root(args.root)
    with Catalog(layout.catalog, read_only=True) as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=volume_adapter())
        external = {
            status["storage_id"]: Path(status["resolved_path"])
            for status in registry.observe_statuses()
            if status.get("state") in {"READY", "LOW_SPACE"}
            and status.get("resolved_path")
        }
        chunks = load_source_chunks(
            layout=layout,
            catalog=catalog,
            external_roots=external,
            start_utc_ns=args.start_ms * 1_000_000,
            end_utc_ns=args.end_ms * 1_000_000,
        )
        records = valid = invalid = 0
        trades = []
        for chunk in sorted(chunks, key=lambda item: item.manifest["created_at_utc_ns"]):
            for record in iter_source_records(chunk):
                envelope = record.envelope
                if envelope.symbol != args.symbol:
                    continue
                records += 1
                parsed = parse_capture_envelope(envelope)
                if parsed and all(row.valid for row in parsed):
                    valid += 1
                else:
                    invalid += 1
                if envelope.stream == "agg_trade":
                    payload = json.loads(envelope.raw_payload)
                    trades.append(
                        (
                            envelope.market,
                            envelope.symbol,
                            payload["data"],
                            envelope.capture_flags,
                        )
                    )
    bars = list(
        aggregate(
            iter(trades),
            args.interval_seconds * 1000,
            args.start_ms,
            args.end_ms,
        )
    )
    result = {
        "market": args.market,
        "symbol": args.symbol,
        "start_ms": args.start_ms,
        "end_ms": args.end_ms,
        "chunks": len(chunks),
        "records": records,
        "valid": valid,
        "invalid": invalid,
        "agg_trades": len(trades),
        "candles": len(bars),
        "gap_candles": sum(1 for bar in bars if bar.get("capture_gap")),
        "first_bar": bars[0] if bars else None,
        "last_bar": bars[-1] if bars else None,
        "status": "PASS" if valid > 0 and invalid == 0 and bars else "FAIL",
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
