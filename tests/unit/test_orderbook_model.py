from __future__ import annotations

from decimal import Decimal

from binance_market_data_recorder.orderbook.model import (
    BookSnapshot,
    DepthUpdate,
    OrderBook,
    canonical_decimal,
)


def test_absolute_levels_zero_delete_best_prices_and_hash_are_deterministic() -> None:
    book = OrderBook(
        BookSnapshot(
            market="spot",
            symbol="BTCUSDT",
            last_update_id=10,
            bids=(("99.00", "2.0"), ("98", "3")),
            asks=(("101.00", "4.0"), ("102", "5")),
        )
    )
    book.apply(
        DepthUpdate(
            market="spot",
            symbol="BTCUSDT",
            first_update_id=11,
            final_update_id=11,
            previous_final_update_id=None,
            bids=(("99", "0"), ("100.0", "7.00")),
            asks=(("101", "8"), ("999", "0")),
        )
    )
    assert tuple(map(str, book.best_bid or ())) == ("100.0", "7.00")
    assert tuple(map(str, book.best_ask or ())) == ("101.00", "8")
    assert book.canonical_mapping()["bids"] == [["100", "7"], ["98", "3"]]
    assert book.logical_hash() == book.logical_hash()
    equivalent = OrderBook.from_mapping(book.canonical_mapping())
    assert equivalent.logical_hash() == book.logical_hash()


def test_empty_and_crossed_properties() -> None:
    crossed = OrderBook(BookSnapshot("spot", "BTCUSDT", 1, (("101", "1"),), (("100", "1"),)))
    empty = OrderBook(BookSnapshot("spot", "BTCUSDT", 1, (), (("100", "1"),)))
    assert crossed.is_crossed
    assert not crossed.is_empty
    assert empty.is_empty


def test_decimal_canonicalization_does_not_round_to_process_context_precision() -> None:
    value = Decimal("123456789012345678901234567890.123456789000")
    assert canonical_decimal(value) == "123456789012345678901234567890.123456789"
