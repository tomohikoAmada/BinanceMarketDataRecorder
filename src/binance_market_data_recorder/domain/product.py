"""Recorder-local configured product identity."""

from dataclasses import dataclass

from .event import Market


@dataclass(frozen=True, order=True, slots=True)
class ProductKey:
    market: Market
    symbol: str


def product_log_fields(
    market: Market, symbol: str, *, stream: str | None = None
) -> dict[str, object]:
    """Return explicit structured-log identity for one configured product."""

    fields: dict[str, object] = {
        "scope": "product",
        "market": market,
        "symbol": symbol,
    }
    if stream is not None:
        fields["stream"] = stream
    return fields


def global_log_fields(
    market: Market, *, owner: str, stream: str | None = None
) -> dict[str, object]:
    """Return explicit structured-log identity for process-global ownership."""

    fields: dict[str, object] = {
        "scope": "global",
        "market": market,
        "owner": owner,
    }
    if stream is not None:
        fields["stream"] = stream
    return fields


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
