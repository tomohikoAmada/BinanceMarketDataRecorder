package binance

import (
	"fmt"
	"sort"
	"strings"
)

// Market identifies the four supported Binance public market domains.
type Market string

const (
	MarketSpot    Market = "spot"
	MarketUSDM    Market = "usdm"
	MarketCOINM   Market = "coinm"
	MarketOptions Market = "options"
)

// CapabilityStatus tracks implementation and upstream availability honestly.
type CapabilityStatus string

const (
	StatusImplemented         CapabilityStatus = "implemented"
	StatusUnsupported         CapabilityStatus = "unsupported"
	StatusUpstreamUnavailable CapabilityStatus = "upstream-unavailable"
)

// Capability documents a single registered Binance REST / WS / Vision dataset entry.
type Capability struct {
	EndpointID        string           `json:"endpoint_id"`
	Market            Market           `json:"market"`
	Transport         string           `json:"transport"` // rest | vision | ws_bounded
	Method            string           `json:"method"`
	BaseURL           string           `json:"base_url"`
	Path              string           `json:"path"`
	RequiredParams    []string         `json:"required_params,omitempty"`
	OptionalParams    []string         `json:"optional_params,omitempty"`
	Weight            string           `json:"weight"`
	Auth              string           `json:"auth"` // none | api_key_required
	TimeUnit          string           `json:"time_unit"`
	QtyUnit           string           `json:"qty_unit"`
	Intervals         []string         `json:"intervals,omitempty"`
	VisionDatasets    []string         `json:"vision_datasets,omitempty"`
	VisionStatus      CapabilityStatus `json:"vision_status,omitempty"`
	RetentionBoundary string           `json:"retention_boundary"`
	TTLSeconds        int              `json:"ttl_seconds"` // 0 = do not serve stale cache for live snapshots
	DocURL            string           `json:"doc_url"`
	Status            CapabilityStatus `json:"status"`
	Notes             string           `json:"notes,omitempty"`
}

const (
	SpotBaseURL    = "https://api.binance.com"
	USDMBaseURL    = "https://fapi.binance.com"
	COINMBaseURL   = "https://dapi.binance.com"
	OptionsBaseURL = "https://eapi.binance.com"
	VisionBaseURL  = "https://data.binance.vision"

	SpotDocURL        = "https://developers.binance.com/en/docs/products/spot/rest-api"
	SpotWSDocURL      = "https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams"
	USDMDocURL        = "https://developers.binance.com/en/docs/derivatives/usds-margined-futures/market-data/rest-api"
	USDMWSBaseURL     = "wss://fstream.binance.com/market/ws"
	USDMPublicWSURL   = "wss://fstream.binance.com/public/ws"
	COINMDocURL       = "https://developers.binance.com/docs/derivatives/coin-margined-futures/market-data/rest-api/Get-Funding-Info"
	COINMWSBaseURL    = "wss://dstream.binance.com/ws"
	OptionsDocURL       = "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/market-data"
	OptionsWSBaseURL    = USDMWSBaseURL
	OptionsPublicWSURL  = USDMPublicWSURL
	OptionsWSDocURL     = "https://developers.binance.com/docs/derivatives/option/websocket-market-streams"
	SpotWSBaseURL     = "wss://stream.binance.com:9443/ws"
	VisionDocURL      = "https://github.com/binance/binance-public-data/blob/master/README.md"
)

var (
	spotRestIntervals = []string{"1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}
	futRestIntervals  = []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}
	optRestIntervals  = []string{"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d", "3d", "1w"}
	futMetricPeriods  = []string{"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
)

var registry = buildRegistry()

func buildRegistry() []Capability {
	entries := []Capability{
		// ===== SPOT =====
		{
			EndpointID: "spot.ping", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/ping", Weight: "1", Auth: "none",
			TimeUnit: "none", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.time", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/time", Weight: "1", Auth: "none",
			TimeUnit: "ms", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.exchangeInfo", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/exchangeInfo", OptionalParams: []string{"symbol", "symbols", "permissions", "showPermissionSets"},
			Weight: "20", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "current-exchange-rules", TTLSeconds: 300,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.depth", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/depth", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "5-250 by limit", Auth: "none", TimeUnit: "live-snapshot", QtyUnit: "base_asset",
			RetentionBoundary: "live-orderbook-only (never backfilled as historical book)", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.trades", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/trades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "25", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "recent-1000-trades", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.historicalTrades", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/historicalTrades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit", "fromId"},
			Weight: "25", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "older-spot-trades-by-fromId", TTLSeconds: 60,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.aggTrades", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/aggTrades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"fromId", "startTime", "endTime", "limit"},
			Weight: "4", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			VisionDatasets: []string{"daily/aggTrades", "monthly/aggTrades"},
			RetentionBoundary: "rest-plus-vision (Spot Vision >=2025-01-01 timestamps in microseconds)", TTLSeconds: 30,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.klines", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/klines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "timeZone", "limit"},
			Weight: "2", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote", Intervals: spotRestIntervals,
			VisionDatasets: []string{"daily/klines", "monthly/klines"},
			RetentionBoundary: "rest-plus-vision (Vision monthly uses 1mo vs REST 1M)", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.uiKlines", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/uiKlines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "timeZone", "limit"},
			Weight: "2", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote", Intervals: spotRestIntervals,
			RetentionBoundary: "rest-historical-ui-klines", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.avgPrice", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/avgPrice", RequiredParams: []string{"symbol"},
			Weight: "2", Auth: "none", TimeUnit: "mins", QtyUnit: "quote_per_base",
			RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ticker.24hr", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/ticker/24hr", OptionalParams: []string{"symbol", "symbols", "type", "symbolStatus"},
			Weight: "2-80", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-rolling-24h", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ticker", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/ticker", OptionalParams: []string{"symbol", "symbols", "windowSize", "type", "symbolStatus"},
			Weight: "4-200", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-rolling-window (1m..7d; default 1d)", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ticker.tradingDay", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/ticker/tradingDay", OptionalParams: []string{"symbol", "symbols", "timeZone", "type", "symbolStatus"},
			Weight: "4-200", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-trading-day (requires symbol or symbols)", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ticker.price", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/ticker/price", OptionalParams: []string{"symbol", "symbols", "symbolStatus"},
			Weight: "2-4", Auth: "none", TimeUnit: "live", QtyUnit: "quote_per_base",
			RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ticker.bookTicker", Market: MarketSpot, Transport: "rest", Method: "GET",
			BaseURL: SpotBaseURL, Path: "/api/v3/ticker/bookTicker", OptionalParams: []string{"symbol", "symbols", "symbolStatus"},
			Weight: "2-4", Auth: "none", TimeUnit: "live", QtyUnit: "base_asset",
			RetentionBoundary: "live-best-bid-ask", TTLSeconds: 0,
			DocURL: SpotDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.bookTicker", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@bookTicker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "live-event", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only (no historical WS backfill)", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.trade", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@trade", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.aggTrade", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@aggTrade", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.kline", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@kline_<interval>", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote", Intervals: spotRestIntervals,
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.depth", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@depth@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.partialDepth", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@depth<levels>@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"levels", "updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.miniTicker", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@miniTicker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.miniTickerArr", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "!miniTicker@arr", OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.ticker", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "<symbol>@ticker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "spot.ws.tickerArr", Market: MarketSpot, Transport: "ws_bounded", Method: "WS",
			BaseURL: SpotWSBaseURL, Path: "!ticker@arr", OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: SpotWSDocURL, Status: StatusImplemented,
		},

		// ===== USD-M FUTURES =====
		{
			EndpointID: "usdm.ping", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/ping", Weight: "1", Auth: "none",
			TimeUnit: "none", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.time", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/time", Weight: "1", Auth: "none",
			TimeUnit: "ms", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.exchangeInfo", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/exchangeInfo", Weight: "1", Auth: "none",
			TimeUnit: "ms", QtyUnit: "base/quote", RetentionBoundary: "current-usdm-contracts", TTLSeconds: 300,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.depth", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/depth", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "2-20", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-snapshot-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.trades", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/trades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "5", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "recent-trades-max-1000", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.historicalTrades", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/historicalTrades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit", "fromId"},
			Weight: "20", Auth: "api_key_required", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "requires BINANCE_API_KEY (MARKET_DATA read-only header, no trading perm)", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
			Notes:  "Direct probe confirmed HTTP 401 when X-MBX-APIKEY is absent; prefer aggTrades/Vision when unauthenticated.",
		},
		{
			EndpointID: "usdm.aggTrades", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/aggTrades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"fromId", "startTime", "endTime", "limit"},
			Weight: "20", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			VisionDatasets: []string{"daily/aggTrades", "monthly/aggTrades"},
			RetentionBoundary: "rest-plus-vision", TTLSeconds: 30,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.klines", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/klines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset", Intervals: futRestIntervals,
			VisionDatasets: []string{"daily/klines", "monthly/klines"},
			RetentionBoundary: "rest-plus-vision (Vision monthly uses 1mo)", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.continuousKlines", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/continuousKlines", RequiredParams: []string{"pair", "contractType", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset", Intervals: futRestIntervals,
			RetentionBoundary: "rest-continuous-contract-klines", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.markPriceKlines", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/markPriceKlines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base", Intervals: futRestIntervals,
			VisionDatasets: []string{"daily/markPriceKlines", "monthly/markPriceKlines"},
			RetentionBoundary: "rest-plus-vision", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.indexPriceKlines", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/indexPriceKlines", RequiredParams: []string{"pair", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base", Intervals: futRestIntervals,
			VisionDatasets: []string{"daily/indexPriceKlines", "monthly/indexPriceKlines"},
			RetentionBoundary: "rest-plus-vision", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.premiumIndexKlines", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/premiumIndexKlines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "rate", Intervals: futRestIntervals,
			RetentionBoundary: "rest-historical", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.premiumIndex", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/premiumIndex", OptionalParams: []string{"symbol"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base/rate",
			RetentionBoundary: "live-mark-and-funding", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.fundingRate", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/fundingRate", OptionalParams: []string{"symbol", "startTime", "endTime", "limit"},
			Weight: "share-500/5m", Auth: "none", TimeUnit: "ms", QtyUnit: "rate",
			VisionDatasets: []string{"monthly/fundingRate"},
			RetentionBoundary: "rest-plus-monthly-vision", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.fundingInfo", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/fundingInfo", Weight: "1", Auth: "none",
			TimeUnit: "hours", QtyUnit: "rate", RetentionBoundary: "current-funding-bounds", TTLSeconds: 300,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ticker.24hr", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/ticker/24hr", OptionalParams: []string{"symbol"},
			Weight: "1-40", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-rolling-24h", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ticker.price", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/ticker/price", OptionalParams: []string{"symbol"},
			Weight: "1-2", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base",
			RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ticker.bookTicker", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/ticker/bookTicker", OptionalParams: []string{"symbol"},
			Weight: "2-5", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.openInterest", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/openInterest", RequiredParams: []string{"symbol"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-snapshot-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.openInterestHist", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/futures/data/openInterestHist", RequiredParams: []string{"symbol", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset+usdt_value", Intervals: futMetricPeriods,
			VisionDatasets: []string{"daily/metrics"},
			RetentionBoundary: "rest-rolling-30d (older metrics via daily/metrics on Vision)", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.topLongShortAccountRatio", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/futures/data/topLongShortAccountRatio", RequiredParams: []string{"symbol", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "ratio", Intervals: futMetricPeriods,
			VisionDatasets: []string{"daily/metrics"},
			RetentionBoundary: "rest-rolling-30d (older metrics via daily/metrics on Vision)", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.topLongShortPositionRatio", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/futures/data/topLongShortPositionRatio", RequiredParams: []string{"symbol", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "ratio", Intervals: futMetricPeriods,
			VisionDatasets: []string{"daily/metrics"},
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.globalLongShortAccountRatio", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/futures/data/globalLongShortAccountRatio", RequiredParams: []string{"symbol", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "ratio", Intervals: futMetricPeriods,
			VisionDatasets: []string{"daily/metrics"},
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.takerlongshortRatio", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/futures/data/takerlongshortRatio", RequiredParams: []string{"symbol", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "ratio+volume", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.basis", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/futures/data/basis", RequiredParams: []string{"pair", "contractType", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base+rate", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.forceOrder", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: "wss://fstream.binance.com/ws", Path: "!forceOrder@arr", OptionalParams: []string{"symbol", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "ws-only-live-bounded (Binance deprecated REST forceOrders; historical only in Recorder)", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ticker.price.v2", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v2/ticker/price", OptionalParams: []string{"symbol"},
			Weight: "1-2", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base",
			RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.indexInfo", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/indexInfo", OptionalParams: []string{"symbol"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "index_components",
			RetentionBoundary: "current-composite-index-constituents", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.assetIndex", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/assetIndex", OptionalParams: []string{"symbol"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "multi_asset_index",
			RetentionBoundary: "live-multi-asset-index", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.constituents", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/constituents", RequiredParams: []string{"symbol"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "constituent_weights",
			RetentionBoundary: "live-index-price-constituents", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.insuranceBalance", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/insuranceBalance", OptionalParams: []string{"symbol"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "margin_asset_balance",
			RetentionBoundary: "live-insurance-fund-snapshot", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.symbolAdlRisk", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/symbolAdlRisk", RequiredParams: []string{"symbol"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "risk_rating",
			RetentionBoundary: "live-adl-risk-30m-refresh", TTLSeconds: 60,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.rpiDepth", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/rpiDepth", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "2-20", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-orderbook-including-rpi", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.deliveryPrice", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/futures/data/delivery-price", RequiredParams: []string{"pair"},
			Weight: "0", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base",
			RetentionBoundary: "quarterly-delivery-settlement-history", TTLSeconds: 300,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.tradingSchedule", Market: MarketUSDM, Transport: "rest", Method: "GET",
			BaseURL: USDMBaseURL, Path: "/fapi/v1/tradingSchedule",
			Weight: "5", Auth: "none", TimeUnit: "ms", QtyUnit: "schedule",
			RetentionBoundary: "1w-backward-1w-forward-trading-sessions", TTLSeconds: 300,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.aggTrade", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMWSBaseURL, Path: "<symbol>@aggTrade", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.markPrice", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMWSBaseURL, Path: "<symbol>@markPrice@1s", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base+rate",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.markPriceArr", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMWSBaseURL, Path: "!markPrice@arr@1s", OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base+rate",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.kline", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMWSBaseURL, Path: "<symbol>@kline_<interval>", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset", Intervals: futRestIntervals,
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.bookTicker", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMPublicWSURL, Path: "<symbol>@bookTicker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.bookTickerArr", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMPublicWSURL, Path: "!bookTicker", OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.depth", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMPublicWSURL, Path: "<symbol>@depth@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.partialDepth", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMPublicWSURL, Path: "<symbol>@depth<levels>@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"levels", "updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base_asset",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.ticker", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMWSBaseURL, Path: "<symbol>@ticker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "usdm.ws.tickerArr", Market: MarketUSDM, Transport: "ws_bounded", Method: "WS",
			BaseURL: USDMWSBaseURL, Path: "!ticker@arr", OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "base/quote",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: USDMDocURL, Status: StatusImplemented,
		},

		// ===== COIN-M FUTURES =====
		{
			EndpointID: "coinm.ping", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/ping", Weight: "1", Auth: "none",
			TimeUnit: "none", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.time", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/time", Weight: "1", Auth: "none",
			TimeUnit: "ms", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.exchangeInfo", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/exchangeInfo", Weight: "1", Auth: "none",
			TimeUnit: "ms", QtyUnit: "contracts (contractSize USD per contract)", RetentionBoundary: "current-coinm-contracts", TTLSeconds: 300,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.depth", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/depth", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "2-20", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-snapshot-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.trades", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/trades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "5", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+baseQty",
			RetentionBoundary: "recent-trades-max-1000", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.historicalTrades", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/historicalTrades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit", "fromId"},
			Weight: "20", Auth: "api_key_required", TimeUnit: "ms", QtyUnit: "contracts+baseQty",
			RetentionBoundary: "requires BINANCE_API_KEY (MARKET_DATA read-only header)", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
			Notes:  "Direct probe confirmed HTTP 401 when X-MBX-APIKEY is absent.",
		},
		{
			EndpointID: "coinm.aggTrades", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/aggTrades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"fromId", "startTime", "endTime", "limit"},
			Weight: "20", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			VisionDatasets: []string{"daily/aggTrades", "monthly/aggTrades"},
			RetentionBoundary: "rest-plus-vision", TTLSeconds: 30,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.klines", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/klines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+base_asset_equiv", Intervals: futRestIntervals,
			VisionDatasets: []string{"daily/klines", "monthly/klines"},
			RetentionBoundary: "rest-plus-vision", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.continuousKlines", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/continuousKlines", RequiredParams: []string{"pair", "contractType", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts", Intervals: futRestIntervals,
			RetentionBoundary: "rest-continuous", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.markPriceKlines", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/markPriceKlines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "usd_per_coin", Intervals: futRestIntervals,
			VisionDatasets: []string{"daily/markPriceKlines", "monthly/markPriceKlines"},
			RetentionBoundary: "rest-plus-vision", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.indexPriceKlines", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/indexPriceKlines", RequiredParams: []string{"pair", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "usd_per_coin", Intervals: futRestIntervals,
			VisionDatasets: []string{"daily/indexPriceKlines", "monthly/indexPriceKlines"},
			RetentionBoundary: "rest-plus-vision", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.premiumIndex", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/premiumIndex", OptionalParams: []string{"symbol", "pair"},
			Weight: "10", Auth: "none", TimeUnit: "ms", QtyUnit: "usd_per_coin/rate",
			RetentionBoundary: "live-mark-and-funding", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.fundingRate", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/fundingRate", RequiredParams: []string{"symbol"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "share", Auth: "none", TimeUnit: "ms", QtyUnit: "rate",
			VisionDatasets: []string{"monthly/fundingRate"},
			RetentionBoundary: "rest-plus-monthly-vision", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.fundingInfo", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/fundingInfo", Weight: "1", Auth: "none",
			TimeUnit: "hours", QtyUnit: "rate", RetentionBoundary: "current-funding-bounds", TTLSeconds: 300,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ticker.24hr", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/ticker/24hr", OptionalParams: []string{"symbol", "pair"},
			Weight: "1-40", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+baseVolume",
			RetentionBoundary: "live-rolling-24h", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ticker.price", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/ticker/price", OptionalParams: []string{"symbol", "pair"},
			Weight: "1-2", Auth: "none", TimeUnit: "ms", QtyUnit: "usd_per_coin",
			RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ticker.bookTicker", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/ticker/bookTicker", OptionalParams: []string{"symbol", "pair"},
			Weight: "2-5", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.openInterest", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/openInterest", RequiredParams: []string{"symbol"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-snapshot-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.openInterestHist", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/futures/data/openInterestHist", RequiredParams: []string{"pair", "contractType", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+coin_value", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d (uses pair+contractType, not symbol)", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.topLongShortAccountRatio", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/futures/data/topLongShortAccountRatio", RequiredParams: []string{"pair", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "ratio", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.topLongShortPositionRatio", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/futures/data/topLongShortPositionRatio", RequiredParams: []string{"pair", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "ratio", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.globalLongShortAccountRatio", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/futures/data/globalLongShortAccountRatio", RequiredParams: []string{"pair", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "ratio", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.takerBuySellVol", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/futures/data/takerBuySellVol", RequiredParams: []string{"pair", "contractType", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+coin_value", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d (COIN-M uses /futures/data/takerBuySellVol, unlike USD-M)", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.basis", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/futures/data/basis", RequiredParams: []string{"pair", "contractType", "period"}, OptionalParams: []string{"limit", "startTime", "endTime"},
			Weight: "0-share", Auth: "none", TimeUnit: "ms", QtyUnit: "usd_per_coin+rate", Intervals: futMetricPeriods,
			RetentionBoundary: "rest-rolling-30d", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.forceOrder", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "!forceOrder@arr", OptionalParams: []string{"symbol", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "ws-only-live-bounded", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.premiumIndexKlines", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/premiumIndexKlines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1-10", Auth: "none", TimeUnit: "ms", QtyUnit: "rate", Intervals: futRestIntervals,
			RetentionBoundary: "rest-historical", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.constituents", Market: MarketCOINM, Transport: "rest", Method: "GET",
			BaseURL: COINMBaseURL, Path: "/dapi/v1/constituents", RequiredParams: []string{"symbol"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "constituent_weights",
			RetentionBoundary: "live-index-price-constituents", TTLSeconds: 60,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.aggTrade", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "<symbol>@aggTrade", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.markPrice", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "<symbol>@markPrice@1s", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "usd_per_coin+rate",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.kline", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "<symbol>@kline_<interval>", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts", Intervals: futRestIntervals,
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.bookTicker", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "<symbol>@bookTicker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.depth", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "<symbol>@depth@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.partialDepth", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "<symbol>@depth<levels>@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"levels", "updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "coinm.ws.ticker", Market: MarketCOINM, Transport: "ws_bounded", Method: "WS",
			BaseURL: COINMWSBaseURL, Path: "<symbol>@ticker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+baseVolume",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: COINMDocURL, Status: StatusImplemented,
		},

		// ===== OPTIONS =====
		{
			EndpointID: "options.ping", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/ping", Weight: "1", Auth: "none",
			TimeUnit: "none", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.time", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/time", Weight: "1", Auth: "none",
			TimeUnit: "ms", QtyUnit: "none", RetentionBoundary: "live-only", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.exchangeInfo", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/exchangeInfo", Weight: "1", Auth: "none",
			TimeUnit: "ms", QtyUnit: "contracts", RetentionBoundary: "active-options-contracts", TTLSeconds: 300,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.index", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/index", RequiredParams: []string{"underlying"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "quote_per_base",
			RetentionBoundary: "live-spot-index", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.depth", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/depth", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "2-20", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-snapshot-only", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.trades", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/trades", RequiredParams: []string{"symbol"}, OptionalParams: []string{"limit"},
			Weight: "5", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "recent-trades-max-500", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.blockTrades", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/blockTrades", OptionalParams: []string{"symbol", "limit"},
			Weight: "5", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "recent-block-trades", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.klines", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/klines", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "1", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts", Intervals: optRestIntervals,
			RetentionBoundary: "rest-contract-lifetime", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.mark", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/mark", OptionalParams: []string{"symbol"},
			Weight: "5", Auth: "none", TimeUnit: "ms", QtyUnit: "greeks+iv+markPrice",
			RetentionBoundary: "live-greeks-iv-mark", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.ticker", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/ticker", OptionalParams: []string{"symbol"},
			Weight: "5", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+quote",
			RetentionBoundary: "live-rolling-24h", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.openInterest", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/openInterest", RequiredParams: []string{"underlyingAsset", "expiration"},
			Weight: "0", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+usd_sum",
			RetentionBoundary: "live-by-underlying-and-YYMMDD-expiration", TTLSeconds: 0,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.exerciseHistory", Market: MarketOptions, Transport: "rest", Method: "GET",
			BaseURL: OptionsBaseURL, Path: "/eapi/v1/exerciseHistory", RequiredParams: []string{"underlying"}, OptionalParams: []string{"startTime", "endTime", "limit"},
			Weight: "3", Auth: "none", TimeUnit: "ms", QtyUnit: "strike_price",
			RetentionBoundary: "recent-settlement-history", TTLSeconds: 300,
			DocURL: OptionsDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.ws.trade", Market: MarketOptions, Transport: "ws_bounded", Method: "WS",
			BaseURL: OptionsWSBaseURL, Path: "<symbol>@trade", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: OptionsWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.ws.kline", Market: MarketOptions, Transport: "ws_bounded", Method: "WS",
			BaseURL: OptionsWSBaseURL, Path: "<symbol>@kline_<interval>", RequiredParams: []string{"symbol", "interval"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts", Intervals: optRestIntervals,
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: OptionsWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.ws.ticker", Market: MarketOptions, Transport: "ws_bounded", Method: "WS",
			BaseURL: OptionsWSBaseURL, Path: "<symbol>@ticker", RequiredParams: []string{"symbol"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts+quote",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: OptionsWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.ws.markPrice", Market: MarketOptions, Transport: "ws_bounded", Method: "WS",
			BaseURL: OptionsWSBaseURL, Path: "<underlying>@optionMarkPrice", RequiredParams: []string{"underlying"}, OptionalParams: []string{"duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "greeks+iv+markPrice",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: OptionsWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.ws.depth", Market: MarketOptions, Transport: "ws_bounded", Method: "WS",
			BaseURL: OptionsPublicWSURL, Path: "<symbol>@depth@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: OptionsWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.ws.partialDepth", Market: MarketOptions, Transport: "ws_bounded", Method: "WS",
			BaseURL: OptionsPublicWSURL, Path: "<symbol>@depth<levels>@<updateSpeed>", RequiredParams: []string{"symbol"}, OptionalParams: []string{"levels", "updateSpeed", "duration_seconds", "max_events"},
			Weight: "connection-bounded", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			RetentionBoundary: "live-bounded-sample-only", TTLSeconds: 0,
			DocURL: OptionsWSDocURL, Status: StatusImplemented,
		},
		{
			EndpointID: "options.vision.EOHSummary", Market: MarketOptions, Transport: "vision", Method: "GET",
			BaseURL: VisionBaseURL, Path: "/data/option/daily/EOHSummary/{underlying}/{date}/{underlying}-{date}.zip",
			RequiredParams: []string{"underlying", "date"}, Weight: "static", Auth: "none", TimeUnit: "ms", QtyUnit: "contracts",
			VisionDatasets: []string{"daily/EOHSummary"}, VisionStatus: StatusUpstreamUnavailable,
			RetentionBoundary: "upstream-sparse (recent 2026-11 dates observed returning HTTP 404)", TTLSeconds: 86400,
			DocURL: VisionDocURL, Status: StatusUpstreamUnavailable,
			Notes:  "Probed on 2026-11-10 -> HTTP 404; reported honestly as upstream-unavailable rather than fabricating data.",
		},
		{
			EndpointID: "vision.archive", Market: MarketUSDM, Transport: "vision", Method: "GET",
			BaseURL: VisionBaseURL, Path: "/data/<market>/<cadence>/<dataset>/<symbol>/...",
			RequiredParams: []string{"market", "cadence", "dataset", "symbol"}, OptionalParams: []string{"interval", "date", "month"},
			Weight: "static", Auth: "none", TimeUnit: "ms/us", QtyUnit: "market-dependent",
			VisionDatasets: []string{"daily/aggTrades", "monthly/aggTrades", "daily/trades", "monthly/trades", "daily/klines", "monthly/klines", "daily/markPriceKlines", "monthly/markPriceKlines", "daily/indexPriceKlines", "monthly/indexPriceKlines", "daily/metrics", "monthly/fundingRate"},
			VisionStatus: StatusImplemented,
			RetentionBoundary: "official-bulk-zip-with-sha256-checksum (via manage_local_cache vision prefetch or FetchVisionArchive)", TTLSeconds: 86400,
			DocURL: VisionDocURL, Status: StatusImplemented,
			Notes:  "Supports Spot (2025+ us timestamps) and Futures UM/CM daily & monthly ZIP + CHECKSUM verification.",
		},
	}
	for i := range entries {
		if len(entries[i].VisionDatasets) > 0 && entries[i].VisionStatus == "" {
			entries[i].VisionStatus = StatusImplemented
		}
	}
	return entries
}

// Capabilities returns sorted capabilities filtered optionally by market or query substring.
func Capabilities(marketFilter, search string) []Capability {
	mf := NormalizeMarket(marketFilter)
	q := strings.ToLower(strings.TrimSpace(search))
	var out []Capability
	for _, c := range registry {
		if marketFilter != "" && mf != "" && c.Market != mf {
			continue
		}
		if q != "" {
			hay := strings.ToLower(c.EndpointID + " " + c.Path + " " + c.Notes + " " + string(c.Market))
			if !strings.Contains(hay, q) {
				continue
			}
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].EndpointID < out[j].EndpointID })
	return out
}

// LookupCapability returns an exact capability entry by endpoint_id.
func LookupCapability(endpointID string) (Capability, bool) {
	target := strings.TrimSpace(endpointID)
	for _, c := range registry {
		if c.EndpointID == target {
			return c, true
		}
	}
	return Capability{}, false
}

// NormalizeMarket maps market aliases to canonical market names.
func NormalizeMarket(raw string) Market {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "spot":
		return MarketSpot
	case "usdm", "um", "um_perpetual", "usds_m", "futures_um":
		return MarketUSDM
	case "coinm", "cm", "cm_perpetual", "coin_m", "futures_cm":
		return MarketCOINM
	case "options", "option", "eapi":
		return MarketOptions
	default:
		return ""
	}
}

// VisionInterval converts REST interval "1M" to Binance Vision directory spelling "1mo".
func VisionInterval(interval string) string {
	if interval == "1M" {
		return "1mo"
	}
	return interval
}

// IntervalMillis maps supported candle interval strings cleanly to exact milliseconds.
func IntervalMillis(interval string) (int64, error) {
	switch interval {
	case "1s":
		return 1_000, nil
	case "1m":
		return 60_000, nil
	case "3m":
		return 3 * 60_000, nil
	case "5m":
		return 5 * 60_000, nil
	case "15m":
		return 15 * 60_000, nil
	case "30m":
		return 30 * 60_000, nil
	case "1h":
		return 3_600_000, nil
	case "2h":
		return 2 * 3_600_000, nil
	case "4h":
		return 4 * 3_600_000, nil
	case "6h":
		return 6 * 3_600_000, nil
	case "8h":
		return 8 * 3_600_000, nil
	case "12h":
		return 12 * 3_600_000, nil
	case "1d":
		return 86_400_000, nil
	case "3d":
		return 3 * 86_400_000, nil
	case "1w":
		return 7 * 86_400_000, nil
	default:
		return 0, fmt.Errorf("unsupported fixed-width interval %q", interval)
	}
}
