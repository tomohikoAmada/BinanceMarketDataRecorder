"""Concrete Binance source plan. No account endpoints or implicit source shedding."""

from __future__ import annotations

from dataclasses import dataclass, field
from importlib.metadata import version

from ..domain.event import Market
from ..domain.product import Product
from .coverage_config import CoverageConfig

REST_HOSTS = {
    "spot": "https://api.binance.com",
    "usdm": "https://fapi.binance.com",
    "coinm": "https://dapi.binance.com",
}
WS_ROOTS = {
    "spot": "wss://stream.binance.com:9443",
    "usdm": "wss://fstream.binance.com",
    "coinm": "wss://dstream.binance.com",
}
GLOBAL_MARKET: dict[str, Market] = {"spot": "spot", "usdm": "um_shared", "coinm": "cm_shared"}
REST_PREFIX = {"spot": "/api/v3", "usdm": "/fapi/v1", "coinm": "/dapi/v1"}


PUBLIC_SDK_VERSIONS = {
    "binance-sdk-spot": version("binance-sdk-spot"),
    "binance-common": version("binance-common"),
}


@dataclass(frozen=True, slots=True)
class Source:
    family: str
    venue_market: str
    market: Market
    symbol: str
    stream: str
    transport: str
    endpoint: str
    cadence_seconds: float
    parameters: tuple[tuple[str, str | int], ...] = ()
    weight: int = 1
    required_fields: tuple[str, ...] = ()
    priority: str = "unique_fact"
    lifecycle: str = "active"
    # Physical products linked to one shared source; not duplicate requests.
    products: tuple[str, ...] = field(default=(), compare=False)
    representation: str = "sdk-parsed-json.v1"

    @property
    def key(self) -> tuple[str, str, str, str]:
        return self.venue_market, self.symbol, self.stream, self.transport

    @property
    def charges(self) -> dict[str, int]:
        pool = "spot_weight" if self.venue_market == "spot" else "usdm_weight"
        charges = {pool: self.weight} if self.weight else {}
        if self.family in {"D12", "D13"}:
            charges["usdm_funding"] = 1
        if self.endpoint.startswith("/futures/data/") and self.family != "D31":
            charges["usdm_statistics"] = 1
        return charges

    def public_dict(self) -> dict[str, object]:
        from dataclasses import asdict

        return {
            **asdict(self),
            "parameters": dict(self.parameters),
            "representation": "ws-combined-exact.v1"
            if self.transport == "ws"
            else self.representation,
            "adapter_schema": "binance.public.websocket.v1"
            if self.transport == "ws"
            else "binance-public-rest-provenance.v1",
            "sdk_versions": None if self.transport == "ws" else PUBLIC_SDK_VERSIONS,
            "success_budget_seconds": 10,
            "empty_response": "retained_observation",
            "unknown_fields": "WS preserved; REST not guaranteed",
            "recovery": "snapshot_and_sequence_bridge"
            if self.stream == "diff_depth"
            else "new_valid_response_or_connection",
            "gap_semantics": "W" if self.transport == "ws" else "F",
        }


def websocket_fields(stream: str, venue: str) -> tuple[str, ...]:
    if stream in {"diff_depth", "rpi_depth"}:
        return ("e", "E", "s", "U", "u", "b", "a") + (
            () if venue == "spot" else ("pu", "T", "st", "ps")
        )
    if stream == "book_ticker":
        return ("s", "u", "b", "B", "a", "A") + (() if venue == "spot" else ("E", "T", "st", "ps"))
    if stream == "agg_trade":
        return ("e", "E", "s", "a", "p", "q", "f", "l", "T", "m") + (
            () if venue == "spot" else ("st",)
        )
    if stream in {"trade", "block_trade"}:
        return ("e", "E", "s", "t", "p", "q", "T", "m")
    if "kline" in stream:
        return ("e", "E", "k")
    return {
        "reference_price": ("e", "s", "r", "t"),
        "avg_price": ("e", "E", "s", "i", "w", "T"),
        "mark_price": ("e", "E", "s", "p", "i", "P", "r", "T"),
        "index_price": ("e", "E", "i", "p"),
        "liquidation": ("e", "E", "o"),
        "ticker": ("e", "E", "s", "o", "h", "l", "c", "v", "n"),
        "ticker_1h": ("e", "E", "s", "o", "h", "l", "c", "v"),
        "ticker_4h": ("e", "E", "s", "o", "h", "l", "c", "v"),
        "ticker_1d": ("e", "E", "s", "o", "h", "l", "c", "v"),
    }.get(stream, ())


def bar_period_seconds(interval: str) -> int:
    if interval == "1M":
        return 31 * 86400  # Calendar scheduling uses the received native close time.
    return int(interval[:-1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[interval[-1]]


def directory_source(venue: str, config: CoverageConfig) -> Source:
    return Source(
        "S01" if venue == "spot" else "D01",
        venue,
        GLOBAL_MARKET[venue],
        "__GLOBAL__",
        "exchange_info",
        "rest",
        REST_PREFIX[venue] + "/exchangeInfo",
        config.directory_interval_seconds,
        weight=20 if venue == "spot" else 1,
        required_fields=("symbols", "rateLimits"),
    )


def product_sources(product: Product, config: CoverageConfig) -> tuple[Source, ...]:
    p = product
    venue, market, symbol = p.venue_market, p.key.market, p.key.symbol
    root, prefix = WS_ROOTS[venue], REST_PREFIX[venue]
    spot, coin = venue == "spot", venue == "coinm"
    sources: list[Source] = []

    def ws(
        family: str,
        stream: str,
        wire: str,
        speed: float,
        *,
        scope: str = symbol,
        route: str = "market",
        priority: str = "unique_fact",
    ) -> None:
        base = root + ("/" + route if venue == "usdm" else "")
        sources.append(
            Source(
                family,
                venue,
                market if scope == symbol else GLOBAL_MARKET[venue],
                scope,
                stream,
                "ws",
                base + "/" + wire,
                speed,
                required_fields=websocket_fields(stream, venue),
                priority=priority,
                products=(symbol,),
            )
        )

    def rest(
        family: str,
        stream: str,
        path: str,
        seconds: float,
        parameters: dict[str, str | int] | None = None,
        *,
        scope: str = symbol,
        weight: int = 1,
        fields: tuple[str, ...] = (),
        lifecycle: str = "active",
        priority: str = "unique_fact",
    ) -> None:
        sources.append(
            Source(
                family,
                venue,
                market if scope == symbol else GLOBAL_MARKET[venue],
                scope,
                stream,
                "rest",
                path,
                seconds,
                tuple(
                    sorted((parameters if parameters is not None else {"symbol": symbol}).items())
                ),
                weight,
                fields,
                priority,
                lifecycle,
                (symbol,),
                "recent-trades-compact.v1"
                if family == "D06" and config.recent_trades_compact
                else "http-body-base64.v1"
                if family == "D31"
                else "sdk-parsed-json.v1",
            )
        )

    lower = symbol.lower()
    ws("S02" if spot else "D02", "diff_depth", lower + "@depth@100ms", 0.1, route="public")
    rest(
        "S02" if spot else "D02",
        "depth_snapshot",
        prefix + "/depth",
        0,
        {"symbol": symbol, "limit": 1000},
        weight=50 if spot else 20,
        fields=("lastUpdateId", "bids", "asks"),
    )
    ws("S03" if spot else "D05", "agg_trade", lower + "@aggTrade", 0 if spot else 0.1)
    ws("S05" if spot else "D08", "book_ticker", lower + "@bookTicker", 0, route="public")
    ws(
        "S13" if spot else "D24",
        "ticker",
        lower + "@ticker",
        1 if spot else 0.5,
        priority="native_statistics",
    )
    intervals = config.spot_intervals if spot else config.futures_intervals
    for interval in intervals:
        ws(
            "S11" if spot else "D21",
            "kline_" + interval,
            lower + "@kline_" + interval,
            1 if interval == "1s" else 2 if spot else 0.25,
            priority="forming_bars",
        )
        # Bounded startup REST repairs; the native live stream remains authoritative.
        rest(
            "S11" if spot else "D21",
            "kline_seed_" + interval,
            prefix + "/klines",
            0,
            {"symbol": symbol, "interval": interval, "limit": 2},
            weight=2 if spot else 1,
            priority="bar_repair",
        )
        if spot:
            ws(
                "S12",
                "kline_" + interval + "_utc8",
                lower + "@kline_" + interval + "@+08:00",
                1 if interval == "1s" else 2,
                priority="forming_bars",
            )
            rest(
                "S12",
                "kline_seed_" + interval + "_utc8",
                prefix + "/klines",
                0,
                {"symbol": symbol, "interval": interval, "timeZone": "8", "limit": 2},
                weight=2,
                priority="bar_repair",
            )
    if spot:
        ws("S04", "trade", lower + "@trade", 0)
        ws("S06", "block_trade", lower + "@blockTrade", 0)
        ws("S07", "reference_price", lower + "@referencePrice", 1)
        rest(
            "S07",
            "reference_price_seed",
            prefix + "/referencePrice",
            0,
            weight=2,
            fields=("symbol", "referencePrice", "timestamp"),
        )
        rest(
            "S08",
            "reference_price_calculation",
            prefix + "/referencePrice/calculation",
            3600,
            weight=2,
            fields=("symbol", "calculationType"),
        )
        # One selected-symbol batch is assembled by shared_sources, not per product.
        ws("S10", "avg_price", lower + "@avgPrice", 1)
        for window in ("1h", "4h", "1d"):
            ws(
                "S14",
                "ticker_" + window,
                lower + "@ticker_" + window,
                1,
                priority="native_statistics",
            )
        rest(
            "S15",
            "trading_day",
            prefix + "/ticker/tradingDay",
            60,
            {"symbol": symbol, "timeZone": "0"},
            weight=4,
            fields=("symbol", "openTime", "closeTime"),
            priority="native_statistics",
        )
        return tuple(sources)

    ws("D09", "mark_price", lower + "@markPrice@1s", 1)
    ws("D11-C" if coin else "D11-U", "liquidation", lower + "@forceOrder", 1)
    if not coin:
        ws("D03", "rpi_depth", lower + "@rpiDepth@500ms", 0.5, route="public")
        rest(
            "D03",
            "rpi_depth_snapshot",
            prefix + "/rpiDepth",
            0,
            {"symbol": symbol, "limit": 1000},
            weight=20,
            fields=("lastUpdateId", "bids", "asks"),
        )
        rest(
            "D28",
            "adl_risk",
            prefix + "/symbolAdlRisk",
            1800,
            fields=("symbol", "adlRisk", "updateTime"),
        )
    rest(
        "D06",
        "recent_trades",
        prefix + "/trades",
        config.recent_trades_intervals.get(
            f"{market}:{symbol}", config.recent_trades_interval_seconds
        ),
        {"symbol": symbol, "limit": 1000},
        weight=5,
        fields=("id", "price", "qty", "time", "isBuyerMaker"),
    )
    rest(
        "D09",
        "premium_index_snapshot",
        prefix + "/premiumIndex",
        60,
        fields=("symbol", "markPrice", "indexPrice", "time"),
        weight=1,
    )
    rest(
        "D14",
        "open_interest",
        prefix + "/openInterest",
        60,
        fields=("symbol", "openInterest", "time"),
    )
    if p.perpetual:
        rest(
            "D12",
            "funding_history",
            prefix + "/fundingRate",
            300,
            {"symbol": symbol, "limit": 100},
            fields=("symbol", "fundingTime", "fundingRate"),
        )
    pair_scope = p.pair
    contract_scope = p.pair + "_" + p.contract_type
    stats_scope = contract_scope if coin else symbol
    stats_parameters: dict[str, str | int] = {
        "pair" if coin else "symbol": p.pair if coin else symbol,
        "period": "5m",
        "limit": 2,
    }
    for family, stream, path, fields in (
        (
            "D15",
            "open_interest_statistics_5m",
            "openInterestHist",
            ("sumOpenInterest", "sumOpenInterestValue", "timestamp"),
        ),
        (
            "D16",
            "taker_buy_sell_volume_5m",
            "takerBuySellVol" if coin else "takerlongshortRatio",
            ("timestamp",),
        ),
        (
            "D17",
            "global_long_short_ratio_5m",
            "globalLongShortAccountRatio",
            ("longShortRatio", "longAccount", "shortAccount", "timestamp"),
        ),
    ):
        params = dict(stats_parameters)
        if coin and family in {"D15", "D16"}:
            params["contractType"] = p.contract_type
        rest(
            family,
            stream,
            "/futures/data/" + path,
            300,
            params,
            scope=stats_scope if family in {"D15", "D16"} else pair_scope if coin else symbol,
            weight=1 if coin else 0,
            fields=fields,
        )
    if coin:
        for family, kind, path in (
            ("D18", "account", "topLongShortAccountRatio"),
            ("D19", "position", "topLongShortPositionRatio"),
        ):
            rest(
                family,
                "top_long_short_" + kind + "_ratio_5m",
                "/futures/data/" + path,
                300,
                dict(stats_parameters),
                scope=pair_scope,
                fields=("timestamp", "longShortRatio"),
            )
        ws("D10", "index_price", p.pair.lower() + "@indexPrice@1s", 1, scope=pair_scope)
    rest(
        "D20",
        "basis_5m",
        "/futures/data/basis",
        300,
        {"pair": p.pair, "contractType": p.contract_type, "period": "5m", "limit": 2},
        scope=contract_scope,
        weight=1 if coin else 0,
        fields=("timestamp", "basis", "basisRate"),
    )
    rest(
        "D26",
        "index_constituents",
        prefix + "/constituents",
        3600,
        {"symbol": p.pair},
        scope=pair_scope,
        weight=2,
        fields=("constituents",),
    )
    if not coin and p.underlying_type == "INDEX":
        rest("D26", "composite_index_info", prefix + "/indexInfo", 3600, {"symbol": symbol})
        ws("D26", "composite_index", lower + "@compositeIndex", 1)
    for interval in config.futures_intervals:
        ws(
            "D22",
            "continuous_kline_" + interval,
            contract_scope.lower() + "@continuousKline_" + interval,
            0.25,
            scope=contract_scope,
            priority="forming_bars",
        )
        rest(
            "D22",
            "continuous_seed_" + interval,
            prefix + "/continuousKlines",
            0,
            {"pair": p.pair, "contractType": p.contract_type, "interval": interval, "limit": 2},
            scope=contract_scope,
            priority="bar_repair",
        )
        for price_type in ("mark", "index", "premium"):
            if (
                price_type == "premium"
                and market in {"um_delivery", "cm_delivery"}
                and not config.capture_dated_premium_klines
            ):
                continue
            scope = pair_scope if price_type == "index" else symbol
            parameter = "pair" if price_type == "index" else "symbol"
            if coin and price_type in {"mark", "index"}:
                ws(
                    "D23",
                    price_type + "_price_kline_" + interval,
                    scope.lower() + "@" + price_type + "PriceKline_" + interval,
                    0.25,
                    scope=scope,
                    priority="forming_bars",
                )
            rest(
                "D23",
                price_type + "_price_bars_" + interval,
                prefix
                + "/"
                + price_type
                + ("PriceKlines" if price_type != "premium" else "IndexKlines"),
                bar_period_seconds(interval),
                {parameter: scope, "interval": interval, "limit": 2},
                scope=scope,
                lifecycle="closed_bar",
                priority="native_bars",
            )
    if not coin and not p.perpetual:
        rest(
            "D31",
            "delivery_price",
            "/futures/data/delivery-price",
            60,
            {"pair": p.pair},
            scope=pair_scope,
            lifecycle="after_delivery",
            fields=("deliveryTime", "deliveryPrice"),
        )
    return tuple(sources)


def shared_sources(products: tuple[Product, ...]) -> tuple[Source, ...]:
    result = []
    for venue in {p.venue_market for p in products} - {"spot"}:
        prefix = REST_PREFIX[venue]
        result.append(
            Source(
                "D13",
                venue,
                GLOBAL_MARKET[venue],
                "__GLOBAL__",
                "funding_info",
                "rest",
                prefix + "/fundingInfo",
                3600,
                required_fields=("symbol", "fundingIntervalHours"),
            )
        )
    if any(p.venue_market == "spot" for p in products):
        import json

        symbols = sorted(p.key.symbol for p in products if p.venue_market == "spot")
        # Request only selected symbols; batching has a documented 100-symbol limit.
        for offset in range(0, len(symbols), 100):
            result.append(
                Source(
                    "S09",
                    "spot",
                    "spot",
                    f"__BATCH_{offset // 100}__",
                    "execution_rules",
                    "rest",
                    "/api/v3/executionRules",
                    3600,
                    (("symbols", json.dumps(symbols[offset : offset + 100])),),
                    20,
                    required_fields=("symbolRules",),
                )
            )
    if any(p.venue_market != "spot" for p in products):
        result.extend(
            (
                Source(
                    "D01",
                    "usdm",
                    "um_shared",
                    "__GLOBAL__",
                    "contract_info",
                    "ws",
                    WS_ROOTS["usdm"] + "/market/!contractInfo",
                    0,
                ),
                Source(
                    "D27",
                    "usdm",
                    "um_shared",
                    "__GLOBAL__",
                    "asset_index",
                    "ws",
                    WS_ROOTS["usdm"] + "/market/!assetIndex@arr",
                    1,
                ),
                Source(
                    "D29",
                    "usdm",
                    "um_shared",
                    "__GLOBAL__",
                    "insurance_balance",
                    "rest",
                    "/fapi/v1/insuranceBalance",
                    60,
                    required_fields=("symbols", "assets"),
                ),
            )
        )
    return tuple(result)


def source_plan(products: tuple[Product, ...], config: CoverageConfig) -> tuple[Source, ...]:
    """Exactly one source per real scope; retain reverse product associations."""
    from dataclasses import replace

    unique: dict[tuple[str, str, str, str], Source] = {}
    for source in (
        *shared_sources(products),
        *(s for p in products for s in product_sources(p, config)),
    ):
        old = unique.get(source.key)
        if old is not None:
            if (old.endpoint, old.parameters) != (source.endpoint, source.parameters):
                raise ValueError("conflicting shared source parameters")
            source = replace(old, products=tuple(sorted(set(old.products + source.products))))
        unique[source.key] = source
    return tuple(sorted(unique.values(), key=lambda s: s.key))


def nominal_derivative_weight_per_minute(
    sources: tuple[Source, ...], config: CoverageConfig
) -> float:
    """Selected steady polls plus both derivative directories; seeds/retries are extra."""
    return (
        sum(
            s.weight * 60 / s.cadence_seconds
            for s in sources
            if s.transport == "rest"
            and s.venue_market != "spot"
            and s.cadence_seconds > 0
            and s.lifecycle != "after_delivery"
        )
        + 120 / config.directory_interval_seconds
    )


def source_selection_report(
    products: tuple[Product, ...], config: CoverageConfig, selected: tuple[Source, ...]
) -> dict[str, object]:
    """Describe the admitted product plan without regenerating its complete source set."""
    uncaptured = [
        {
            "family": "D23",
            "market": p.key.market,
            "symbol": p.key.symbol,
            "stream": "premium_price_bars_" + interval,
            "transport": "rest",
            "interval": interval,
            "enabled": False,
            "status": "OWNER_APPROVED_UNCAPTURED",
            "official_applicability": "UNKNOWN",
            "reason": "ADR-0055 initial-release dated premium omission",
            "continuity_eligible": False,
        }
        for p in sorted(products, key=lambda p: p.key)
        if not config.capture_dated_premium_klines
        and p.key.market in {"um_delivery", "cm_delivery"}
        for interval in config.futures_intervals
    ]
    return {
        "policy": "complete_target"
        if config.capture_dated_premium_klines
        else "initial_production_adr_0055",
        "scope": "admitted_products and configured intervals; directories counted separately",
        "full_target_source_count": len(selected) + len(uncaptured),
        "selected_source_count": len(selected),
        "directory_source_count": 3,
        "selected_ws_streams": sum(s.transport == "ws" for s in selected),
        "selected_rest_sources": sum(s.transport == "rest" for s in selected),
        "uncaptured_source_count": len(uncaptured),
        "uncaptured_sources": uncaptured,
        "nominal_steady_derivative_weight_per_minute": nominal_derivative_weight_per_minute(
            selected, config
        ),
        "budget_excludes": "startup seeds, retries, delivery publication and other IP users",
    }
