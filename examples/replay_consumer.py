#!/usr/bin/env python3
"""Independent read-only example using only the published replay contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path

from binance_market_data_recorder.replay import (
    CONSUMER_CONTRACT_VERSION,
    GapPolicy,
    ManifestCatalog,
    MissingExchangeTimePolicy,
    ReplayClock,
    ReplayQuery,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read one explicit Recorder dataset build deterministically."
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--build-id", required=True)
    parser.add_argument("--market", action="append", default=[])
    parser.add_argument("--stream", action="append", default=[])
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--clock",
        choices=[value.value for value in ReplayClock],
        default="RECEIVE_TIME",
    )
    parser.add_argument("--start-time-ns", type=int)
    parser.add_argument("--end-time-ns", type=int)
    parser.add_argument(
        "--gap-policy",
        choices=[value.value for value in GapPolicy],
        default="ERROR",
    )
    parser.add_argument(
        "--missing-exchange-time",
        choices=[value.value for value in MissingExchangeTimePolicy],
        default="ERROR",
    )
    parser.add_argument("--checkpoint-id")
    parser.add_argument("--limit", type=int, help="stop after this many events")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be positive")
    dataset = ManifestCatalog(args.data_root).open_build(args.build_id)
    query = ReplayQuery(
        clock=ReplayClock(args.clock),
        markets=tuple(args.market),
        streams=tuple(args.stream),
        symbol=args.symbol,
        start_time_ns=args.start_time_ns,
        end_time_ns=args.end_time_ns,
        gap_policy=GapPolicy(args.gap_policy),
        missing_exchange_time=MissingExchangeTimePolicy(
            args.missing_exchange_time
        ),
        checkpoint_id=args.checkpoint_id,
    )
    digest = hashlib.sha256()
    count = 0
    first_time: int | None = None
    last_time: int | None = None
    unreliable = 0
    fallback = 0
    for event in dataset.replay(query):
        document = {
            "event_time_ns": event.event_time_ns,
            "used_receive_time_fallback": event.used_receive_time_fallback,
            "is_unreliable": event.is_unreliable,
            "row": dict(event.row),
        }
        digest.update(
            (
                json.dumps(
                    document,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
                + "\n"
            ).encode()
        )
        count += 1
        first_time = (
            event.event_time_ns if first_time is None else first_time
        )
        last_time = event.event_time_ns
        unreliable += int(event.is_unreliable)
        fallback += int(event.used_receive_time_fallback)
        if args.limit is not None and count >= args.limit:
            break
    print(
        json.dumps(
            {
                "consumer_contract_version": CONSUMER_CONTRACT_VERSION,
                "dataset_version": dataset.summary.dataset_version,
                "build_id": dataset.summary.build_id,
                "order_version": "replay-order.v1",
                "clock": query.clock.value,
                "event_count": count,
                "first_event_time_ns": first_time,
                "last_event_time_ns": last_time,
                "unreliable_event_count": unreliable,
                "receive_time_fallback_count": fallback,
                "event_digest_sha256": digest.hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
