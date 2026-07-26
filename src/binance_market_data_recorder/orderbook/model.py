"""特定市场的输入和确定性逻辑订单簿状态。

本模型负责结构完整性、价位状态和确定性序列化;市场特定的 update ID 连续性
决策位于 LocalBookReconstructor 中。

DepthUpdate 中编码的 Spot 与 USD-M 差异:
- Spot:previous_final_update_id 必须为 None(Spot 无 'pu' 字段)。
- USD-M:previous_final_update_id 必须是非负整数('pu' 字段)。
此差异在 __post_init__ 中通过市场特定验证强制执行。

DepthUpdate 的价格和数量通过 Decimal 验证:
- Price:必须有限且为正。
- Quantity:必须有限且非负。零数量移除一个价位。
- 移除不存在的价位不视为错误(dict.pop,默认 None)。

OrderBook.canonical_mapping() 为 SHA-256 哈希生成确定性 JSON 结构。
所有 decimal 值以无尾随零、无科学记数法呈现(canonical_decimal)。
映射序列化时按键排序,bids 按价格降序、asks 按价格升序排列,
确保无论插入顺序如何哈希均一致。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Literal, cast

from ..domain.event import Market

Side = Literal["bid", "ask"]
Level = tuple[str, str]


class OrderBookDataError(ValueError):
    """Raised when a derived order-book input is structurally invalid."""


def decimal_value(value: str, *, positive: bool) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise OrderBookDataError(f"invalid decimal value: {value!r}") from exc
    if not parsed.is_finite() or (parsed <= 0 if positive else parsed < 0):
        qualifier = "positive" if positive else "non-negative"
        raise OrderBookDataError(f"decimal value must be {qualifier}: {value!r}")
    return parsed


def canonical_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def validated_levels(levels: tuple[Level, ...]) -> tuple[tuple[Decimal, Decimal], ...]:
    return tuple(
        (decimal_value(price, positive=True), decimal_value(quantity, positive=False))
        for price, quantity in levels
    )


@dataclass(frozen=True)
class BookSnapshot:
    market: Market
    symbol: str
    last_update_id: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]

    def __post_init__(self) -> None:
        if (
            self.market not in {"spot", "um_perpetual"}
            or self.symbol != "BTCUSDT"
            or self.last_update_id < 0
        ):
            raise OrderBookDataError("invalid snapshot identity or update ID")
        validated_levels(self.bids)
        validated_levels(self.asks)


@dataclass(frozen=True)
class DepthUpdate:
    market: Market
    symbol: str
    first_update_id: int
    final_update_id: int
    previous_final_update_id: int | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    receive_time_utc_ns: int = 0

    def __post_init__(self) -> None:
        if self.market not in {"spot", "um_perpetual"} or self.symbol != "BTCUSDT":
            raise OrderBookDataError("unexpected depth symbol")
        if self.first_update_id < 0 or self.final_update_id < self.first_update_id:
            raise OrderBookDataError("invalid depth update ID range")
        if self.market == "spot" and self.previous_final_update_id is not None:
            raise OrderBookDataError("Spot updates must not carry USD-M pu")
        if self.market == "um_perpetual" and (
            self.previous_final_update_id is None or self.previous_final_update_id < 0
        ):
            raise OrderBookDataError("USD-M updates require non-negative pu")
        if self.receive_time_utc_ns < 0:
            raise OrderBookDataError("receive time must be non-negative")
        validated_levels(self.bids)
        validated_levels(self.asks)


@dataclass(frozen=True)
class BookTicker:
    market: Market
    symbol: str
    update_id: int
    bid_price: str
    bid_quantity: str
    ask_price: str
    ask_quantity: str

    def __post_init__(self) -> None:
        if (
            self.market not in {"spot", "um_perpetual"}
            or self.symbol != "BTCUSDT"
            or self.update_id < 0
        ):
            raise OrderBookDataError("invalid book ticker identity or update ID")
        decimal_value(self.bid_price, positive=True)
        decimal_value(self.ask_price, positive=True)
        decimal_value(self.bid_quantity, positive=False)
        decimal_value(self.ask_quantity, positive=False)


class OrderBook:
    """Absolute price/quantity state with deterministic serialization."""

    def __init__(self, snapshot: BookSnapshot) -> None:
        self.market = snapshot.market
        self.symbol = snapshot.symbol
        self.update_id = snapshot.last_update_id
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self._replace(self.bids, snapshot.bids)
        self._replace(self.asks, snapshot.asks)

    @staticmethod
    def _replace(side: dict[Decimal, Decimal], levels: tuple[Level, ...]) -> None:
        for price, quantity in validated_levels(levels):
            if quantity == 0:
                side.pop(price, None)
            else:
                side[price] = quantity

    def apply(self, update: DepthUpdate) -> None:
        if update.market != self.market or update.symbol != self.symbol:
            raise OrderBookDataError("cannot apply a different market or symbol")
        self._replace(self.bids, update.bids)
        self._replace(self.asks, update.asks)
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
        return (
            self.best_bid is not None
            and self.best_ask is not None
            and self.best_bid[0] >= self.best_ask[0]
        )

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

    @classmethod
    def from_mapping(cls, value: dict[str, object]) -> OrderBook:
        try:
            if value.get("schema_version") != "logical-orderbook.v1":
                raise TypeError
            market_value = value["market"]
            symbol = value["symbol"]
            update_id = value["update_id"]
            bids = value["bids"]
            asks = value["asks"]
            if market_value not in {"spot", "um_perpetual"}:
                raise TypeError
            market = cast(Market, market_value)
            if not isinstance(symbol, str) or not isinstance(update_id, int):
                raise TypeError
            if not isinstance(bids, list) or not isinstance(asks, list):
                raise TypeError
            parsed_bids = tuple(_mapping_level(level) for level in bids)
            parsed_asks = tuple(_mapping_level(level) for level in asks)
        except (KeyError, TypeError, ValueError) as exc:
            raise OrderBookDataError("invalid logical order-book mapping") from exc
        return cls(
            BookSnapshot(
                market=market,
                symbol=symbol,
                last_update_id=update_id,
                bids=parsed_bids,
                asks=parsed_asks,
            )
        )


def _mapping_level(value: object) -> Level:
    if (
        not isinstance(value, list)
        or len(value) != 2
        or not all(isinstance(item, str) for item in value)
    ):
        raise OrderBookDataError("invalid checkpoint price level")
    return value[0], value[1]
