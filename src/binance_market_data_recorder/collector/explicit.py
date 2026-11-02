"""Map the original symbol/side-data configuration to the common source plan."""

from __future__ import annotations

from dataclasses import replace

from ..binance.coverage_config import legacy_symbols_to_coverage_config, parse_base_and_quote_asset
from ..binance.public_sources import Source, directory_source, product_sources, shared_sources
from ..config import RecorderConfig
from ..domain.product import Product, configured_products

# Source names are the existing Raw logical stream names. No full-coverage
# subscriptions are added when adapting the original explicit configuration.
SIDE_OPTIONS = {
    "mark_price": "mark_price",
    "liquidation": "liquidation",
    "premium_index_snapshot": "premium_index",
    "funding_history": "funding_history",
    "funding_info": "funding_info",
    "open_interest": "open_interest",
    "exchange_info": "exchange_info",
    "open_interest_statistics_5m": "open_interest_statistics",
    "taker_buy_sell_volume_5m": "taker_buy_sell_volume",
    "global_long_short_ratio_5m": "global_long_short_ratio",
    "top_long_short_account_ratio_5m": "top_long_short_account_ratio",
    "top_long_short_position_ratio_5m": "top_long_short_position_ratio",
    "basis_5m": "basis",
}
CORE_SOURCES = {"diff_depth", "agg_trade", "book_ticker", "depth_snapshot"}


def explicit_source_plan(config: RecorderConfig) -> tuple[tuple[Product, ...], tuple[Source, ...]]:
    coverage = legacy_symbols_to_coverage_config(config)
    products = []
    sources = []
    for key in configured_products(config.spot_symbols, config.usdm_symbols):
        spot = key.market == "spot"
        base, quote = parse_base_and_quote_asset(key.symbol)
        product = Product(
            key,
            "spot" if spot else "usdm",
            key.symbol,
            base,
            quote,
            None if spot else quote,
            "SPOT" if spot else "PERPETUAL",
            None if spot else "1",
            None,
            None,
            "TRADING",
        )
        products.append(product)
        for source in product_sources(product, coverage):
            if source.stream in CORE_SOURCES or (not spot and source.stream in SIDE_OPTIONS):
                sources.append(source)
        if spot and config.spot_exchange_info_enabled:
            sources.append(
                replace(
                    directory_source("spot", coverage),
                    symbol=key.symbol,
                    parameters=(("symbol", key.symbol),),
                    cadence_seconds=config.spot_exchange_info_interval_seconds,
                    required_fields=("symbols",),
                )
            )
        if not spot:
            # These two original USD-M side sources are outside the complete
            # coverage plan, but remain part of the explicit configuration.
            for family, kind, path in (
                ("D18", "account", "topLongShortAccountRatio"),
                ("D19", "position", "topLongShortPositionRatio"),
            ):
                sources.append(
                    Source(
                        family,
                        "usdm",
                        key.market,
                        key.symbol,
                        "top_long_short_" + kind + "_ratio_5m",
                        "rest",
                        "/futures/data/" + path,
                        300,
                        (("symbol", key.symbol), ("period", "5m"), ("limit", 2)),
                        weight=0,
                        required_fields=("timestamp", "longShortRatio"),
                    )
                )
    sources.extend(s for s in shared_sources(tuple(products)) if s.stream == "funding_info")
    if config.usdm_symbols and config.side_exchange_info_enabled:
        sources.append(directory_source("usdm", coverage))
    selected = []
    for source in sources:
        if source.venue_market == "usdm" and source.stream not in CORE_SOURCES:
            option = SIDE_OPTIONS[source.stream]
            if not getattr(config, f"side_{option}_enabled"):
                continue
            if source.transport == "rest":
                source = replace(
                    source, cadence_seconds=getattr(config, f"side_{option}_interval_seconds")
                )
            # The original basis source is product scoped, not contract scoped.
            if source.stream == "basis_5m":
                source = replace(
                    source, market="um_perpetual", symbol=str(dict(source.parameters)["pair"])
                )
            if source.symbol == "__GLOBAL__":
                source = replace(source, market="um_perpetual", symbol="BTCUSDT")
        selected.append(source)
    return tuple(products), tuple(selected)
