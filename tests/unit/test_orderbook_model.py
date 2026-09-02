from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from decimal import Decimal

import pytest

from binance_market_data_recorder.orderbook.model import (
    BookSnapshot,
    DepthUpdate,
    OrderBook,
    canonical_decimal,
)


def spot_update(
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


def make_book(
    *,
    update_id: int = 10,
    bids: tuple[tuple[str, str], ...] = (("100", "2"), ("99", "3")),
    asks: tuple[tuple[str, str], ...] = (("101", "4"), ("102", "5")),
) -> OrderBook:
    return OrderBook(BookSnapshot("spot", "BTCUSDT", update_id, bids, asks))


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


def test_initial_snapshot_best_prices_are_materialized_once() -> None:
    book = make_book(
        bids=(("99.00", "2.0"), ("100.0", "3.00"), ("98", "1")),
        asks=(("102", "4"), ("101.00", "5.0"), ("103", "1")),
    )
    assert book.best_bid == (Decimal("100.0"), Decimal("3.00"))
    assert book.best_ask == (Decimal("101.00"), Decimal("5.0"))


def test_better_bid_and_ask_insertions_update_only_the_price_cache() -> None:
    book = make_book()
    book.apply(spot_update(11, bids=(("101", "6"),), asks=(("100", "7"),)))
    assert book.best_bid == (Decimal("101"), Decimal("6"))
    assert book.best_ask == (Decimal("100"), Decimal("7"))


def test_quantity_updates_read_from_authoritative_mappings_at_best_and_nonbest() -> None:
    book = make_book()
    book.apply(spot_update(11, bids=(("100", "8.00"),), asks=(("101", "9.00"),)))
    assert book.best_bid == (Decimal("100"), Decimal("8.00"))
    assert book.best_ask == (Decimal("101"), Decimal("9.00"))

    book.apply(spot_update(12, bids=(("99", "10"),), asks=(("102", "11"),)))
    assert book.best_bid == (Decimal("100"), Decimal("8.00"))
    assert book.best_ask == (Decimal("101"), Decimal("9.00"))
    assert book.bids[Decimal("99")] == Decimal("10")
    assert book.asks[Decimal("102")] == Decimal("11")


def test_nonbest_bid_deletion_leaves_best_bid_unchanged() -> None:
    book = make_book()
    book.apply(spot_update(11, bids=(("99", "0"),)))
    assert book.best_bid == (Decimal("100"), Decimal("2"))
    assert book.bids == {Decimal("100"): Decimal("2")}


def test_nonbest_ask_deletion_leaves_best_ask_unchanged() -> None:
    book = make_book()
    book.apply(spot_update(11, asks=(("102", "0"),)))
    assert book.best_ask == (Decimal("101"), Decimal("4"))
    assert book.asks == {Decimal("101"): Decimal("4")}


def test_best_bid_deletion_falls_back_to_next_highest_level() -> None:
    book = make_book()
    book.apply(spot_update(11, bids=(("100", "0"),)))
    assert book.best_bid == (Decimal("99"), Decimal("3"))


def test_best_ask_deletion_falls_back_to_next_lowest_level() -> None:
    book = make_book()
    book.apply(spot_update(11, asks=(("101", "0"),)))
    assert book.best_ask == (Decimal("102"), Decimal("5"))


def test_deleting_the_last_level_clears_each_best_price() -> None:
    bid_only = make_book(bids=(("100", "2"),), asks=())
    bid_only.apply(spot_update(11, bids=(("100", "0"),)))
    assert bid_only.best_bid is None
    assert bid_only.is_empty

    ask_only = make_book(bids=(), asks=(("101", "4"),))
    ask_only.apply(spot_update(11, asks=(("101", "0"),)))
    assert ask_only.best_ask is None
    assert ask_only.is_empty


def test_best_deletion_and_replacement_in_one_batch_recomputes_final_best() -> None:
    book = make_book()
    book.apply(spot_update(11, bids=(("100", "0"), ("101", "6"))))
    assert book.best_bid == (Decimal("101"), Decimal("6"))


def test_deleted_best_can_be_reintroduced_with_its_new_decimal_key() -> None:
    book = make_book()
    book.apply(spot_update(11, bids=(("100", "0"), ("100.0", "8.00"))))
    assert book.best_bid is not None
    assert str(book.best_bid[0]) == "100.0"
    assert str(book.best_bid[1]) == "8.00"


def test_crossed_semantics_and_equivalent_book_hashes_are_unchanged() -> None:
    crossed = make_book(bids=(("101", "2"), ("100", "1")), asks=(("101", "3"),))
    assert crossed.is_crossed

    left = make_book()
    left.apply(spot_update(11, bids=(("100", "0"), ("101", "7")), asks=(("102", "8"),)))
    right = make_book(
        bids=(("101", "7"), ("99", "3")),
        asks=(("101", "4"), ("102", "8")),
        update_id=11,
    )
    assert left.canonical_mapping() == right.canonical_mapping()
    assert left.logical_hash() == right.logical_hash()


@pytest.mark.parametrize(
    ("bids", "asks", "expected"),
    [
        ((), (), False),
        ((), (("100", "1"),), False),
        ((("99", "1"),), (), False),
        ((("99", "1"),), (("100", "1"),), False),
        ((("100", "1"),), (("100", "1"),), True),
        ((("101", "1"),), (("100", "1"),), True),
    ],
)
def test_is_crossed_evaluates_each_best_side_once(
    bids: tuple[tuple[str, str], ...],
    asks: tuple[tuple[str, str], ...],
    expected: bool,
) -> None:
    class CountingOrderBook(OrderBook):
        best_bid_accesses = 0
        best_ask_accesses = 0

        @property
        def best_bid(self) -> tuple[Decimal, Decimal] | None:
            self.best_bid_accesses += 1
            return super().best_bid

        @property
        def best_ask(self) -> tuple[Decimal, Decimal] | None:
            self.best_ask_accesses += 1
            return super().best_ask

    book = CountingOrderBook(BookSnapshot("spot", "BTCUSDT", 1, bids, asks))

    assert book.is_crossed is expected
    assert book.best_bid_accesses == 1
    assert book.best_ask_accesses == 1


def test_decimal_canonicalization_does_not_round_to_process_context_precision() -> None:
    value = Decimal("123456789012345678901234567890.123456789000")
    assert canonical_decimal(value) == "123456789012345678901234567890.123456789"


def test_common_updates_do_not_scan_and_best_deletion_scans_once_per_side(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    book = make_book(
        bids=(("100", "2"), ("99", "3"), ("98", "4")),
        asks=(("101", "4"), ("102", "5"), ("103", "6")),
    )
    calls: list[tuple[bool, int]] = []
    original = OrderBook._recompute_best_price

    def spy(
        side: dict[Decimal, Decimal], *, is_bid: bool
    ) -> Decimal | None:
        calls.append((is_bid, len(side)))
        return original(side, is_bid=is_bid)

    monkeypatch.setattr(OrderBook, "_recompute_best_price", staticmethod(spy))

    for update_id in range(11, 111):
        book.apply(
            spot_update(
                update_id,
                bids=(("99", str(update_id)),),
                asks=(("102", str(update_id)),),
            )
        )
    assert calls == []

    book.apply(spot_update(111, bids=(("100", "0"),)))
    assert calls == [(True, 2)]
    book.apply(spot_update(112, asks=(("101", "0"),)))
    assert calls == [(True, 2), (False, 2)]


@dataclass
class ReferenceBook:
    """Small dictionary-only oracle for the cached-best differential test."""

    market: str
    symbol: str
    update_id: int
    bids: dict[Decimal, Decimal]
    asks: dict[Decimal, Decimal]

    @classmethod
    def from_snapshot(cls, snapshot: BookSnapshot) -> ReferenceBook:
        return cls(
            market=snapshot.market,
            symbol=snapshot.symbol,
            update_id=snapshot.last_update_id,
            bids=_reference_levels(snapshot.bids),
            asks=_reference_levels(snapshot.asks),
        )

    def apply(self, update: DepthUpdate) -> None:
        for side, levels in ((self.bids, update.bids), (self.asks, update.asks)):
            for price_text, quantity_text in levels:
                price = Decimal(price_text)
                quantity = Decimal(quantity_text)
                if quantity == 0:
                    side.pop(price, None)
                else:
                    side[price] = quantity
        self.update_id = update.final_update_id

    @property
    def best_bid(self) -> tuple[Decimal, Decimal] | None:
        if not self.bids:
            return None
        price = max(self.bids)
        return price, self.bids[price]

    @property
    def best_ask(self) -> tuple[Decimal, Decimal] | None:
        if not self.asks:
            return None
        price = min(self.asks)
        return price, self.asks[price]

    @property
    def is_empty(self) -> bool:
        return not self.bids or not self.asks

    @property
    def is_crossed(self) -> bool:
        best_bid = self.best_bid
        best_ask = self.best_ask
        return best_bid is not None and best_ask is not None and best_bid[0] >= best_ask[0]

    def canonical_mapping(self) -> dict[str, object]:
        return {
            "schema_version": "logical-orderbook.v1",
            "market": self.market,
            "symbol": self.symbol,
            "update_id": self.update_id,
            "bids": [
                [canonical_decimal(price), canonical_decimal(self.bids[price])]
                for price in sorted(self.bids, reverse=True)
            ],
            "asks": [
                [canonical_decimal(price), canonical_decimal(self.asks[price])]
                for price in sorted(self.asks)
            ],
        }

    def logical_hash(self) -> str:
        encoded = json.dumps(
            self.canonical_mapping(), sort_keys=True, separators=(",", ":")
        ).encode()
        return hashlib.sha256(encoded).hexdigest()


def _reference_levels(levels: tuple[tuple[str, str], ...]) -> dict[Decimal, Decimal]:
    side: dict[Decimal, Decimal] = {}
    for price_text, quantity_text in levels:
        price = Decimal(price_text)
        quantity = Decimal(quantity_text)
        if quantity == 0:
            side.pop(price, None)
        else:
            side[price] = quantity
    return side


def _reference_price(
    side: dict[Decimal, Decimal], *, is_bid: bool, better: bool
) -> Decimal:
    if not side:
        return Decimal("10000" if is_bid else "20000")
    if is_bid:
        return (max(side) + 1) if better else (min(side) - 1)
    return (min(side) - 1) if better else (max(side) + 1)


def _equivalent_price_text(price: Decimal) -> str:
    rendered = format(price, "f")
    return f"{rendered}0" if "." in rendered else f"{rendered}.0"


def _random_differential_update(
    rng: random.Random,
    reference: ReferenceBook,
    sequence: int,
    operation: int,
) -> tuple[DepthUpdate, str]:
    if operation == 8:
        is_bid = True
        kind = "empty_bid"
    elif operation == 9:
        is_bid = False
        kind = "empty_ask"
    else:
        is_bid = rng.choice((True, False))
        kind = (
            "better_insert",
            "nonbest_quantity",
            "best_quantity",
            "nonbest_delete",
            "best_delete",
            "worse_insert",
            "delete_reintroduce",
            "multi_side",
        )[operation]

    side = reference.bids if is_bid else reference.asks
    levels: list[tuple[str, str]] = []
    other_levels: list[tuple[str, str]] = []
    best = max(side) if is_bid and side else min(side) if side else None

    if operation == 0:
        levels.append((str(_reference_price(side, is_bid=is_bid, better=True)), "7"))
    elif operation == 1:
        if len(side) > 1:
            candidates = [price for price in side if price != best]
            price = rng.choice(candidates)
        else:
            price = _reference_price(side, is_bid=is_bid, better=False)
        levels.append((str(price), str(rng.randrange(1, 20))))
    elif operation == 2:
        price = best if best is not None else _reference_price(side, is_bid=is_bid, better=True)
        levels.append((str(price), str(rng.randrange(1, 20))))
    elif operation == 3:
        if len(side) > 1:
            candidates = [price for price in side if price != best]
            price = rng.choice(candidates)
        else:
            price = _reference_price(side, is_bid=is_bid, better=False)
        levels.append((str(price), "0"))
    elif operation == 4:
        price = best if best is not None else _reference_price(side, is_bid=is_bid, better=True)
        levels.append((str(price), "0"))
    elif operation == 5:
        levels.append((str(_reference_price(side, is_bid=is_bid, better=False)), "5"))
    elif operation == 6:
        price = best if best is not None else _reference_price(side, is_bid=is_bid, better=True)
        levels.extend(((str(price), "0"), (_equivalent_price_text(price), "13.00")))
    elif operation == 7:
        price = best if best is not None else _reference_price(side, is_bid=is_bid, better=True)
        levels.append((str(price), str(rng.randrange(1, 20))))
        other_side = reference.asks if is_bid else reference.bids
        other_best = (
            min(other_side)
            if is_bid and other_side
            else max(other_side)
            if other_side
            else None
        )
        other_price = (
            other_best
            if other_best is not None
            else _reference_price(other_side, is_bid=not is_bid, better=True)
        )
        other_levels.append((str(other_price), "17"))
    else:
        levels.extend((str(price), "0") for price in side)
        if not levels:
            price = _reference_price(side, is_bid=is_bid, better=True)
            levels.extend(((str(price), "1"), (str(price), "0")))

    return (
        spot_update(
            sequence,
            bids=tuple(levels if is_bid else other_levels),
            asks=tuple(other_levels if is_bid else levels),
        ),
        kind,
    )


def assert_differential_parity(candidate: OrderBook, reference: ReferenceBook) -> None:
    assert candidate.bids == reference.bids
    assert candidate.asks == reference.asks
    assert candidate.update_id == reference.update_id
    assert candidate.best_bid == reference.best_bid
    assert candidate.best_ask == reference.best_ask
    assert candidate.is_empty is reference.is_empty
    assert candidate.is_crossed is reference.is_crossed
    assert candidate.canonical_mapping() == reference.canonical_mapping()
    assert candidate.logical_hash() == reference.logical_hash()


def test_seeded_cached_orderbook_differential_parity_after_every_update() -> None:
    seed = 2026090201
    rng = random.Random(seed)
    operations: set[str] = set()
    applied_updates = 0

    for size in (1, 4, 16, 64):
        snapshot = BookSnapshot(
            market="spot",
            symbol="BTCUSDT",
            last_update_id=1000,
            bids=tuple((str(10000 - index), str(index + 1)) for index in range(size)),
            asks=tuple((str(20000 + index), str(index + 1)) for index in range(size)),
        )
        candidate = OrderBook(snapshot)
        reference = ReferenceBook.from_snapshot(snapshot)
        assert_differential_parity(candidate, reference)

        for offset in range(256):
            update_id = 1001 + offset
            item, kind = _random_differential_update(
                rng, reference, update_id, offset % 10
            )
            candidate.apply(item)
            reference.apply(item)
            assert_differential_parity(candidate, reference)
            operations.add(kind)
            applied_updates += 1

    assert applied_updates == 1024
    assert operations == {
        "better_insert",
        "nonbest_quantity",
        "best_quantity",
        "nonbest_delete",
        "best_delete",
        "worse_insert",
        "delete_reintroduce",
        "multi_side",
        "empty_bid",
        "empty_ask",
    }
