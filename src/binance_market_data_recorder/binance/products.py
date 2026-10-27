"""Bounded selected-base discovery, retaining absent and over-budget identities."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

from ..domain.event import Market
from ..domain.product import Product, ProductKey
from .coverage_config import CoverageConfig

VENUE_MARKETS = ("spot", "usdm", "coinm")
TERMINAL_STATUSES = frozenset({"CLOSE", "DELIVERED", "SETTLED", "DELISTED"})


def _text(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise ValueError(f"directory product missing {field}")
    if any(x in value for x in "/\\@?#%&=: "):
        raise ValueError(f"directory product has unsafe {field}")
    return value


def _time(row: dict[str, Any], field: str) -> int | None:
    value = row.get(field)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"directory product has invalid {field}")
    return value


def discover_products(
    venue_market: str, document: dict[str, Any], config: CoverageConfig
) -> tuple[Product, ...]:
    if venue_market not in VENUE_MARKETS or not isinstance(document.get("symbols"), list):
        raise ValueError("invalid exchange directory")
    products = []
    seen: set[ProductKey] = set()
    for row in document["symbols"]:
        if not isinstance(row, dict):
            raise ValueError("invalid directory row")
        if row.get("baseAsset") not in config.underlying_assets:
            continue
        symbol, base, quote = (_text(row, k) for k in ("symbol", "baseAsset", "quoteAsset"))
        spot = venue_market == "spot"
        if spot and config.spot_quote_assets and quote not in config.spot_quote_assets:
            continue
        if spot and row.get("isSpotTradingAllowed") is not True:
            continue
        contract = "SPOT" if spot else _text(row, "contractType")
        if not spot and contract not in {
            "PERPETUAL",
            "CURRENT_MONTH",
            "NEXT_MONTH",
            "CURRENT_QUARTER",
            "NEXT_QUARTER",
            "CURRENT_QUARTER_DELIVERING",
            "NEXT_QUARTER_DELIVERING",
        }:
            # Preserve unrecognized selected products as a visible discovery error.
            raise ValueError(f"unsupported selected contract type: {contract}")
        market = cast(
            Market,
            "spot"
            if spot
            else ("um_" if venue_market == "usdm" else "cm_")
            + ("perpetual" if contract == "PERPETUAL" else "delivery"),
        )
        key = ProductKey(market, symbol)
        if key in seen:
            raise ValueError("duplicate directory identity")
        seen.add(key)
        size = row.get("contractSize")
        if venue_market == "coinm" and (not isinstance(size, (int, str)) or isinstance(size, bool)):
            raise ValueError("COIN-M contractSize missing or imprecise")
        # Linear quantity is in base units; COIN-M quantity is in contracts.
        size_text = str(size) if size is not None else "1" if not spot else None
        products.append(
            Product(
                key,
                venue_market,
                symbol if spot else _text(row, "pair"),
                base,
                quote,
                None if spot else _text(row, "marginAsset"),
                contract,
                size_text,
                _time(row, "onboardDate"),
                None if spot else _time(row, "deliveryDate"),
                _text(row, "status" if spot or venue_market == "usdm" else "contractStatus"),
                row.get("underlyingType", "COIN"),
            )
        )
    return tuple(sorted(products, key=lambda x: x.key))


class ProductDirectory:
    def __init__(self, config: CoverageConfig) -> None:
        self.config = config
        self.products: dict[ProductKey, Product] = {}
        self.observed_markets: set[str] = set()
        self.missing: set[ProductKey] = set()

    def apply(self, venue_market: str, document: dict[str, Any]) -> None:
        selected = discover_products(venue_market, document, self.config)
        current = {p.key: p for p in selected}
        for key, old in list(self.products.items()):
            if old.venue_market == venue_market and key not in current:
                self.missing.add(key)
                # Disappearance is not an authoritative delisting or expiry.
                self.products[key] = replace(old, status="MISSING_FROM_DIRECTORY")
        self.products.update(current)
        self.missing.difference_update(current)
        self.observed_markets.add(venue_market)

    def not_offered(self) -> list[dict[str, str]]:
        return [
            {
                "asset": asset,
                "venue_market": market,
                "contract": kind,
                "status": "EXCHANGE_NOT_OFFERED",
            }
            for asset in self.config.underlying_assets
            for market in sorted(self.observed_markets)
            for kind in (("SPOT",) if market == "spot" else ("PERPETUAL", "DELIVERY"))
            if not any(
                p.base_asset == asset
                and p.venue_market == market
                and (
                    p.contract_type == "SPOT"
                    if kind == "SPOT"
                    else p.perpetual
                    if kind == "PERPETUAL"
                    else not p.perpetual
                )
                for p in self.products.values()
            )
        ]
