"""Official Spot and USD-M sequence bridging and quality state."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from ..domain.event import Market
from .model import (
    BookSnapshot,
    BookTicker,
    DepthUpdate,
    OrderBook,
    OrderBookDataError,
    canonical_decimal,
    decimal_value,
)


class ReconstructionState(StrEnum):
    BUFFERING = "BUFFERING"
    SYNCHRONIZED = "SYNCHRONIZED"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"


class SynchronizeResult(StrEnum):
    SYNCHRONIZED = "SYNCHRONIZED"
    SNAPSHOT_TOO_OLD = "SNAPSHOT_TOO_OLD"
    NEED_MORE_EVENTS = "NEED_MORE_EVENTS"
    SEQUENCE_GAP = "SEQUENCE_GAP"


class TickerComparison(StrEnum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    STALE = "STALE"
    AHEAD = "AHEAD"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class QualityAudit:
    kind: str
    market: Market
    update_id: int | None
    detail: str


@dataclass(frozen=True)
class UnreliableInterval:
    market: Market
    reason: str
    last_reliable_update_id: int
    offending_first_update_id: int
    offending_final_update_id: int
    started_at_receive_time_utc_ns: int
    ended_at_update_id: int | None = None
    complete: bool = False


class BookUnavailableError(RuntimeError):
    """Raised when callers request a book that is not currently reliable."""


class LocalBookReconstructor:
    """Buffer, bridge, apply, audit and resynchronize one Binance market."""

    algorithm_version = "binance-local-orderbook.v1"

    def __init__(self, market: Market, symbol: str = "BTCUSDT") -> None:
        if market not in {"spot", "um_perpetual"} or symbol != "BTCUSDT":
            raise OrderBookDataError("M6 supports Binance Spot/USD-M BTCUSDT only")
        self.market = market
        self.symbol = symbol
        self.state = ReconstructionState.BUFFERING
        self._buffer: list[DepthUpdate] = []
        self._book: OrderBook | None = None
        self.audits: list[QualityAudit] = []
        self.unreliable_intervals: list[UnreliableInterval] = []

    @property
    def buffered_event_count(self) -> int:
        return len(self._buffer)

    @property
    def is_reliable(self) -> bool:
        return self.state is ReconstructionState.SYNCHRONIZED

    @property
    def book(self) -> OrderBook:
        if not self.is_reliable or self._book is None:
            raise BookUnavailableError("order book is not synchronized and reliable")
        return self._book

    def offer(self, update: DepthUpdate) -> bool:
        """Buffer before snapshot or apply one live update after synchronization."""

        self._check_identity(update.market, update.symbol)
        if self.state is not ReconstructionState.SYNCHRONIZED:
            self._buffer.append(update)
            return False
        return self._apply_live(update)

    def synchronize(self, snapshot: BookSnapshot) -> SynchronizeResult:
        """Bridge a buffered stream to a snapshot using the official market rule."""

        self._check_identity(snapshot.market, snapshot.symbol)
        if not self._buffer:
            return SynchronizeResult.NEED_MORE_EVENTS
        ordered = self._buffer
        if snapshot.last_update_id < ordered[0].first_update_id:
            return SynchronizeResult.SNAPSHOT_TOO_OLD

        if self.market == "spot":
            candidates = [
                update for update in ordered if update.final_update_id > snapshot.last_update_id
            ]
        else:
            candidates = [
                update for update in ordered if update.final_update_id >= snapshot.last_update_id
            ]
        if not candidates:
            self._buffer.clear()
            return SynchronizeResult.NEED_MORE_EVENTS
        first = candidates[0]
        if not (first.first_update_id <= snapshot.last_update_id <= first.final_update_id):
            return SynchronizeResult.SNAPSHOT_TOO_OLD

        was_resync = self.state is ReconstructionState.RESYNC_REQUIRED
        self._book = OrderBook(snapshot)
        self.state = ReconstructionState.SYNCHRONIZED
        self._buffer = []
        for index, update in enumerate(candidates):
            if index == 0:
                self._book.apply(update)
                self._audit_book()
            elif not self._apply_live(update):
                self._buffer.extend(candidates[index + 1 :])
                return SynchronizeResult.SEQUENCE_GAP
        if was_resync:
            self._close_unreliable_interval()
            self.audits.append(
                QualityAudit(
                    kind="orderbook_resync",
                    market=self.market,
                    update_id=self.book.update_id,
                    detail="snapshot and buffered depth restored a reliable book",
                )
            )
        return SynchronizeResult.SYNCHRONIZED

    def _apply_live(self, update: DepthUpdate) -> bool:
        if self._book is None:
            raise BookUnavailableError("no snapshot-backed order book")
        if update.final_update_id <= self._book.update_id:
            self.audits.append(
                QualityAudit(
                    kind="duplicate_or_stale_depth",
                    market=self.market,
                    update_id=update.final_update_id,
                    detail="absolute update already covered by the local book",
                )
            )
            return True
        if self.market == "spot":
            if update.first_update_id > self._book.update_id + 1:
                self._mark_gap(update, "spot_first_update_id_exceeds_local_plus_one")
                return False
        elif update.previous_final_update_id != self._book.update_id:
            self._mark_gap(update, "usdm_pu_does_not_equal_previous_u")
            return False
        self._book.apply(update)
        self._audit_book()
        return True

    def _mark_gap(self, update: DepthUpdate, reason: str) -> None:
        if self._book is None:
            raise BookUnavailableError("cannot mark a gap before snapshot")
        self.unreliable_intervals.append(
            UnreliableInterval(
                market=self.market,
                reason=reason,
                last_reliable_update_id=self._book.update_id,
                offending_first_update_id=update.first_update_id,
                offending_final_update_id=update.final_update_id,
                started_at_receive_time_utc_ns=update.receive_time_utc_ns,
            )
        )
        self.audits.append(
            QualityAudit(
                kind="sequence_gap",
                market=self.market,
                update_id=self._book.update_id,
                detail=reason,
            )
        )
        self.state = ReconstructionState.RESYNC_REQUIRED
        self._buffer = [update]

    def _close_unreliable_interval(self) -> None:
        if self._book is None or not self.unreliable_intervals:
            return
        latest = self.unreliable_intervals[-1]
        if latest.ended_at_update_id is None:
            self.unreliable_intervals[-1] = replace(latest, ended_at_update_id=self._book.update_id)

    def _audit_book(self) -> None:
        if self._book is None:
            return
        if self._book.is_empty:
            self.audits.append(
                QualityAudit(
                    kind="empty_book_side",
                    market=self.market,
                    update_id=self._book.update_id,
                    detail="one or both local book sides are empty",
                )
            )
        if self._book.is_crossed:
            self.audits.append(
                QualityAudit(
                    kind="crossed_book",
                    market=self.market,
                    update_id=self._book.update_id,
                    detail="best bid is greater than or equal to best ask",
                )
            )

    def compare_book_ticker(self, ticker: BookTicker) -> TickerComparison:
        self._check_identity(ticker.market, ticker.symbol)
        if not self.is_reliable or self._book is None:
            return TickerComparison.UNAVAILABLE
        if ticker.update_id < self._book.update_id:
            return TickerComparison.STALE
        if ticker.update_id > self._book.update_id:
            return TickerComparison.AHEAD
        bid = self._book.best_bid
        ask = self._book.best_ask
        expected = None
        if bid is not None and ask is not None:
            expected = (
                canonical_decimal(bid[0]),
                canonical_decimal(bid[1]),
                canonical_decimal(ask[0]),
                canonical_decimal(ask[1]),
            )
        actual = (
            canonical_decimal(decimal_value(ticker.bid_price, positive=True)),
            canonical_decimal(decimal_value(ticker.bid_quantity, positive=False)),
            canonical_decimal(decimal_value(ticker.ask_price, positive=True)),
            canonical_decimal(decimal_value(ticker.ask_quantity, positive=False)),
        )
        if expected == actual:
            return TickerComparison.MATCH
        self.audits.append(
            QualityAudit(
                kind="book_ticker_mismatch",
                market=self.market,
                update_id=self._book.update_id,
                detail="same-update-id best levels differ; this is not an exchange checksum",
            )
        )
        return TickerComparison.MISMATCH

    def _check_identity(self, market: Market, symbol: str) -> None:
        if market != self.market or symbol != self.symbol:
            raise OrderBookDataError("Spot and USD-M reconstruction inputs cannot be mixed")

    @classmethod
    def from_checkpoint(
        cls,
        book: OrderBook,
        intervals: list[UnreliableInterval] | None = None,
    ) -> LocalBookReconstructor:
        restored = cls(book.market, book.symbol)
        restored._book = book
        restored.state = ReconstructionState.SYNCHRONIZED
        restored.unreliable_intervals = list(intervals or [])
        return restored
