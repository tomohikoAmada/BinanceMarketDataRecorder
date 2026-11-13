"""Offline OHLCV from captured aggTrade, with exact decimals and explicit gap evidence."""

import argparse
import json
from decimal import Decimal, localcontext
from pathlib import Path

from binance_market_data_recorder.normalize.raw import (
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout
from binance_market_data_recorder.storage.macos import StorageRegistry
from binance_market_data_recorder.storage.platform import volume_adapter

from partition_roots import reader_lock, resolve_roots


def aggregate(events, width_ms, start_ms, end_ms):
    bars = {}
    previous = {}
    with localcontext() as context:
        context.prec = 50
        for market, symbol, data, flags in events:
            timestamp = int(data["T"])
            if not start_ms <= timestamp < end_ms:
                continue
            identity = (market, symbol)
            agg_id = int(data["a"])
            signature = (
                timestamp,
                data["p"],
                data["q"],
                int(data["f"]),
                int(data["l"]),
            )
            last = previous.get(identity)
            if last and agg_id <= last[0]:
                if agg_id == last[0] and signature == last[1]:
                    continue
                raise ValueError(
                    f"non-monotonic/conflicting aggTrade ID: {identity}/{agg_id}"
                )
            gap = bool(last and agg_id != last[0] + 1) or "sequence_gap" in flags
            previous[identity] = (agg_id, signature)
            price, quantity = Decimal(data["p"]), Decimal(data["q"])
            if (
                not price.is_finite()
                or not quantity.is_finite()
                or price <= 0
                or quantity <= 0
            ):
                raise ValueError("invalid trade decimal")
            key = (*identity, timestamp // width_ms * width_ms)
            ordering = (timestamp, agg_id)
            if key not in bars:
                bars[key] = {
                    "first": ordering,
                    "last": ordering,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": Decimal(0),
                    "quote_volume": Decimal(0),
                    "trade_count": 0,
                    "agg_trades": 0,
                    "capture_gap": False,
                }
            b = bars[key]
            if ordering < b["first"]:
                b["first"] = ordering
                b["open"] = price
            if ordering > b["last"]:
                b["last"] = ordering
                b["close"] = price
            b["high"] = max(b["high"], price)
            b["low"] = min(b["low"], price)
            b["volume"] += quantity
            b["quote_volume"] += price * quantity
            b["trade_count"] += int(data["l"]) - int(data["f"]) + 1
            b["agg_trades"] += 1
            b["capture_gap"] |= gap
        for (market, symbol, bucket), bar in sorted(bars.items()):
            yield {
                "market": market,
                "symbol": symbol,
                "open_time_ms": bucket,
                "close_time_ms": bucket + width_ms - 1,
                "coverage": "observed trades; no gap filling",
                **{
                    k: str(v) if isinstance(v, Decimal) else v
                    for k, v in bar.items()
                    if k not in {"first", "last"}
                },
            }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("root", type=Path)
    p.add_argument("--interval-seconds", type=int, required=True)
    p.add_argument("--start-ms", type=int, required=True)
    p.add_argument("--end-ms", type=int, required=True)
    p.add_argument("--cold-root", type=Path)
    a = p.parse_args()
    if (
        not 0 < a.end_ms - a.start_ms <= 31 * 86400000
        or a.interval_seconds < 60
        or a.interval_seconds % 60
    ):
        p.error("require a <=31 day range and a positive whole-minute interval")
    seen: set[str] = set()
    chunks = []
    with reader_lock(a.root):
        roots = resolve_roots(
            a.root,
            cold_root=a.cold_root,
            start_ns=a.start_ms * 1000000,
            end_ns=a.end_ms * 1000000,
        )
        for root in roots:
            layout = StorageLayout.from_root(root)
            with Catalog(layout.catalog, read_only=True) as catalog:
                # Production roots are registered before recording; use operations.py for unregistered fixtures.
                registry = StorageRegistry(
                    catalog=catalog, volumes=volume_adapter()
                )
                external = {
                    s["storage_id"]: Path(s["resolved_path"])
                    for s in registry.observe_statuses()
                    if s.get("state") in {"READY", "LOW_SPACE"}
                    and s.get("resolved_path")
                }
                loaded = load_source_chunks(
                    layout=layout,
                    catalog=catalog,
                    external_roots=external,
                    start_utc_ns=a.start_ms * 1000000,
                    end_utc_ns=a.end_ms * 1000000,
                )
            for chunk in loaded:
                if chunk.chunk_id in seen:
                    continue
                seen.add(chunk.chunk_id)
                chunks.append(chunk)

    def events():
        for chunk in sorted(
            chunks,
            key=lambda c: (
                c.manifest["receive_time_utc_range_ns"]["min"],
                c.chunk_id,
            ),
        ):
            if chunk.manifest["stream"] != "agg_trade":
                continue
            for record in iter_source_records(chunk):
                e = record.envelope
                yield (
                    e.market,
                    e.symbol,
                    json.loads(e.raw_payload)["data"],
                    e.capture_flags,
                )

    for bar in aggregate(
        events(), a.interval_seconds * 1000, a.start_ms, a.end_ms
    ):
        print(json.dumps(bar))


if __name__ == "__main__":
    main()
