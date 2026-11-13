"""C4-C offline consumer check: one symbol/hour normalize + aggTrade candles."""

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
for path in (REPO / "vendor/python-baseline/src", REPO / "prototype"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from candles import aggregate  # noqa: E402
from normalize import parse_capture_envelope  # noqa: E402
from partition_roots import reader_lock, resolve_roots  # noqa: E402

from binance_market_data_recorder.normalize.raw import (  # noqa: E402
    iter_source_records,
    load_source_chunks,
)
from binance_market_data_recorder.storage.catalog import Catalog  # noqa: E402
from binance_market_data_recorder.storage.layout import (  # noqa: E402
    StorageLayout,
)
from binance_market_data_recorder.storage.macos import (  # noqa: E402
    StorageRegistry,
)
from binance_market_data_recorder.storage.platform import (  # noqa: E402
    volume_adapter,
)

MARKET_ALIASES = {"spot": "spot", "usdm": "um_perpetual"}


def expected_market(market: str) -> str:
    try:
        return MARKET_ALIASES[market]
    except KeyError as error:
        raise ValueError(f"unsupported market {market!r}") from error


def record_is_in_window(envelope, start_ns: int, end_ns: int) -> bool:
    received = int(envelope.receive_time_utc_ns)
    return start_ns <= received < end_ns


def run_check(
    *,
    root: Path,
    market: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval_seconds: int,
    cold_root: Path | None = None,
) -> dict:
    """Return a deterministic result; callers use exit_code() for process status."""

    market_identity = expected_market(market)
    start_ns = start_ms * 1_000_000
    end_ns = end_ms * 1_000_000
    with reader_lock(root):
        records = valid = invalid = wrong_market = 0
        trades = []
        seen_chunks: set[str] = set()
        loaded_chunks = 0
        roots = resolve_roots(
            root,
            cold_root=cold_root,
            start_ns=start_ns,
            end_ns=end_ns,
        )
        physical_roots = list(roots)
        for physical_root in physical_roots:
            layout = StorageLayout.from_root(physical_root)
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
                    products=frozenset({(market_identity, symbol)}),
                    start_utc_ns=start_ns,
                    end_utc_ns=end_ns,
                )
            for chunk in sorted(
                chunks, key=lambda item: item.manifest["created_at_utc_ns"]
            ):
                if chunk.chunk_id in seen_chunks:
                    continue
                seen_chunks.add(chunk.chunk_id)
                loaded_chunks += 1
                for record in iter_source_records(chunk):
                    envelope = record.envelope
                    if envelope.symbol != symbol:
                        continue
                    if envelope.market != market_identity:
                        if envelope.market in {"um_shared", "cm_shared"}:
                            continue
                        wrong_market += 1
                        continue
                    if not record_is_in_window(envelope, start_ns, end_ns):
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
                interval_seconds * 1000,
                start_ms,
                end_ms,
            )
        )
    result = {
        "market": market,
        "expected_market": market_identity,
        "symbol": symbol,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "partitions": len(roots),
        "chunks": loaded_chunks,
        "records": records,
        "valid": valid,
        "invalid": invalid,
        "wrong_market_records": wrong_market,
        "agg_trades": len(trades),
        "candles": len(bars),
        "gap_candles": sum(1 for bar in bars if bar.get("capture_gap")),
        "first_bar": bars[0] if bars else None,
        "last_bar": bars[-1] if bars else None,
    }
    result["status"] = (
        "PASS"
        if records > 0
        and valid > 0
        and invalid == 0
        and trades
        and bars
        and wrong_market == 0
        else "FAIL"
    )
    return result


def exit_code(result: dict) -> int:
    return 0 if result.get("status") == "PASS" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--market", required=True, choices=tuple(MARKET_ALIASES))
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--cold-root", type=Path)
    args = parser.parse_args(argv)
    if not 0 < args.end_ms - args.start_ms <= 3600 * 1000:
        parser.error("require a positive range of at most one hour")
    result = run_check(
        root=args.root,
        market=args.market,
        symbol=args.symbol,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
        interval_seconds=args.interval_seconds,
        cold_root=args.cold_root,
    )
    print(json.dumps(result, indent=2))
    return exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
