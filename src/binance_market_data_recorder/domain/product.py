"""Recorder-local configured product identity."""

from dataclasses import dataclass

from .event import Market


@dataclass(frozen=True, order=True, slots=True)
class ProductKey:
    market: Market
    symbol: str


def configured_products(
    spot_symbols: tuple[str, ...], usdm_symbols: tuple[str, ...]
) -> tuple[ProductKey, ...]:
    """Return deterministic runtime order without changing durable identity."""
    return tuple(
        sorted(
            [ProductKey("spot", symbol) for symbol in spot_symbols]
            + [ProductKey("um_perpetual", symbol) for symbol in usdm_symbols]
        )
    )
