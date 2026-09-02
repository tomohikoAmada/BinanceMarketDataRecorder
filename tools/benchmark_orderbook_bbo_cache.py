"""Deterministic evidence-only benchmark for OrderBook best-level lookup."""

from __future__ import annotations

import argparse
import gc
import json
import sys
import time
from collections.abc import Callable

from binance_market_data_recorder.orderbook.model import BookSnapshot, DepthUpdate, OrderBook

LevelUpdateRunner = Callable[[OrderBook, int], None]

ITERATIONS_BY_SIZE = {
    1_000: 2_000,
    5_000: 800,
    10_000: 400,
}


def _snapshot(levels_per_side: int) -> BookSnapshot:
    return BookSnapshot(
        market="spot",
        symbol="BTCUSDT",
        last_update_id=1,
        bids=tuple((str(price), "1") for price in range(levels_per_side, 0, -1)),
        asks=tuple(
            (str(price), "1")
            for price in range(levels_per_side + 1, (2 * levels_per_side) + 1)
        ),
    )


def _book(levels_per_side: int) -> OrderBook:
    return OrderBook(_snapshot(levels_per_side))


def _update(
    update_id: int,
    *,
    bids: tuple[tuple[str, str], ...] = (),
    asks: tuple[tuple[str, str], ...] = (),
) -> DepthUpdate:
    return DepthUpdate(
        market="spot",
        symbol="BTCUSDT",
        first_update_id=update_id,
        final_update_id=update_id,
        previous_final_update_id=None,
        bids=bids,
        asks=asks,
    )


def _equivalent_price_text(price: int) -> str:
    return f"{price}.0"


def _common_updates(levels_per_side: int, iterations: int) -> tuple[DepthUpdate, ...]:
    bid_price = levels_per_side // 2
    ask_price = levels_per_side + (levels_per_side // 2)
    return tuple(
        _update(
            update_id,
            bids=((str(bid_price), str((index % 97) + 1)),),
            asks=((str(ask_price), str((index % 89) + 1)),),
        )
        for index, update_id in enumerate(range(2, iterations + 2))
    )


def _best_quantity_updates(levels_per_side: int, iterations: int) -> tuple[DepthUpdate, ...]:
    return tuple(
        _update(
            update_id,
            bids=((str(levels_per_side), str((index % 97) + 1)),),
            asks=((str(levels_per_side + 1), str((index % 89) + 1)),),
        )
        for index, update_id in enumerate(range(2, iterations + 2))
    )


def _best_deletion_updates(levels_per_side: int, iterations: int) -> tuple[DepthUpdate, ...]:
    best_price = str(levels_per_side)
    replacement_price = _equivalent_price_text(levels_per_side)
    nonbest_price = str(levels_per_side // 2)
    return tuple(
        _update(
            update_id,
            bids=(
                ((best_price, "0"), (replacement_price, "1"))
                if index % 20 == 0
                else ((nonbest_price, str((index % 97) + 1)),)
            ),
        )
        for index, update_id in enumerate(range(2, iterations + 2))
    )


def _fallback_updates(levels_per_side: int, iterations: int) -> tuple[DepthUpdate, ...]:
    best_price = str(levels_per_side)
    replacement_price = _equivalent_price_text(levels_per_side)
    return tuple(
        _update(
            update_id,
            bids=((best_price, "0"), (replacement_price, "1")),
        )
        for update_id in range(2, iterations + 2)
    )


def _apply_and_audit(updates: tuple[DepthUpdate, ...]) -> LevelUpdateRunner:
    def run(book: OrderBook, count: int) -> None:
        for update in updates[:count]:
            book.apply(update)
            _ = book.is_crossed

    return run


def _apply_only(updates: tuple[DepthUpdate, ...]) -> LevelUpdateRunner:
    def run(book: OrderBook, count: int) -> None:
        for update in updates[:count]:
            book.apply(update)

    return run


def _direct_best_lookup(book: OrderBook, count: int) -> None:
    consumed = 0
    for _ in range(count):
        best_bid = book.best_bid
        best_ask = book.best_ask
        if best_bid is not None:
            consumed += int(best_bid[0])
        if best_ask is not None:
            consumed += int(best_ask[0])
    if consumed < 0:
        raise AssertionError("benchmark result was not consumed")


def _measure(
    levels_per_side: int,
    iterations: int,
    runner: LevelUpdateRunner,
) -> float:
    warmup_iterations = min(20, iterations)
    runner(_book(levels_per_side), warmup_iterations)
    measured_book = _book(levels_per_side)
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        started_ns = time.perf_counter_ns()
        runner(measured_book, iterations)
        elapsed_ns = time.perf_counter_ns() - started_ns
    finally:
        if was_enabled:
            gc.enable()
    return elapsed_ns / iterations / 1_000_000_000


def _benchmark_size(levels_per_side: int, iterations: int) -> dict[str, object]:
    common = _common_updates(levels_per_side, iterations)
    best_quantity = _best_quantity_updates(levels_per_side, iterations)
    best_deletion = _best_deletion_updates(levels_per_side, iterations)
    fallback = _fallback_updates(levels_per_side, iterations)
    runners: dict[str, LevelUpdateRunner] = {
        "common_nonbest_apply_audit": _apply_and_audit(common),
        "best_quantity_apply_audit": _apply_and_audit(best_quantity),
        "occasional_best_deletion_apply_audit": _apply_and_audit(best_deletion),
        "direct_best_lookup": _direct_best_lookup,
        "best_deletion_fallback_apply_only": _apply_only(fallback),
    }
    return {
        "levels_per_side": levels_per_side,
        "iterations": iterations,
        "seconds_per_iteration": {
            name: _measure(levels_per_side, iterations, runner)
            for name, runner in runners.items()
        },
    }


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--source-tree", required=True)
    return parser.parse_args()


def main() -> None:
    arguments = _arguments()
    report = {
        "label": arguments.label,
        "source_sha": arguments.source_sha,
        "source_tree": arguments.source_tree,
        "python": sys.version,
        "interpreter": sys.executable,
        "fixtures": [
            _benchmark_size(levels_per_side, iterations)
            for levels_per_side, iterations in ITERATIONS_BY_SIZE.items()
        ],
    }
    print(json.dumps(report, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
