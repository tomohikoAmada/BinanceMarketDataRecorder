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


@dataclass(frozen=True, slots=True)
class Product:
    """Exchange directory identity; dated contracts never borrow perpetual identity."""

    key: ProductKey
    venue_market: str
    pair: str
    base_asset: str
    quote_asset: str
    margin_asset: str | None
    contract_type: str
    contract_size: str | None
    onboard_time_ms: int | None
    delivery_time_ms: int | None
    status: str
    underlying_type: str = "COIN"

    @property
    def symbol_type(self) -> int | None:
        return None if self.venue_market == "spot" else 1 if self.venue_market == "usdm" else 2

    @property
    def perpetual(self) -> bool:
        return self.contract_type == "PERPETUAL"

    def public_dict(self) -> dict[str, object]:
        from dataclasses import asdict

        return {"schema_version": "binance-product.v1", **asdict(self)}
