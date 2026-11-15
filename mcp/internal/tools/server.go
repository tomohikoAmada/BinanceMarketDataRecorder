package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/binance"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/cache"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/recorder"
)

// Server wires Binance REST/Vision/WS and local Recorder caches into an official Go MCP server.
type Server struct {
	Cfg      *config.Config
	CacheMgr *cache.Manager
	Jobs     *cache.JobCoordinator
	Binance  *binance.Client
	Recorder *recorder.Client
	MCP      *mcp.Server
}

// NewServer constructs and registers all M5-C MCP tools.
func NewServer(cfg *config.Config, cacheMgr *cache.Manager, jobs *cache.JobCoordinator, bClient *binance.Client, rClient *recorder.Client) *Server {
	s := &Server{
		Cfg:      cfg,
		CacheMgr: cacheMgr,
		Jobs:     jobs,
		Binance:  bClient,
		Recorder: rClient,
		MCP: mcp.NewServer(&mcp.Implementation{
			Name:    "binance-data-mcp",
			Version: "v1.0.0",
		}, nil),
	}
	s.registerTools()
	return s
}

func (s *Server) registerTools() {
	mcp.AddTool(s.MCP, &mcp.Tool{
		Name:        "query_klines",
		Description: "Query Spot, USD-M, COIN-M, or Options OHLCV klines from Recorder aggTrade archives (Decimal aggregation), Binance Vision daily zip archives (with SHA-256 checksum verification), or live Binance REST.",
	}, s.handleQueryKlines)

	mcp.AddTool(s.MCP, &mcp.Tool{
		Name:        "query_orderbook",
		Description: "Reconstruct historical synchronized Spot/USD-M order books from Recorder diff_depth + depth_snapshot (strict Spot U/u vs USD-M pu rules) with top_k_depth_imbalance, or fetch a live REST depth snapshot when no historical window is given.",
	}, s.handleQueryOrderbook)

	mcp.AddTool(s.MCP, &mcp.Tool{
		Name:        "query_trades_and_liquidations",
		Description: "Query aggregate trades and USD-M liquidation (forceOrder) streams from Recorder raw chunks or Binance public REST aggTrades.",
	}, s.handleQueryTradesAndLiquidations)

	mcp.AddTool(s.MCP, &mcp.Tool{
		Name:        "query_derivatives_metrics",
		Description: "Query derivatives metrics across USD-M, COIN-M, and Options: mark price & funding rate, open interest, long/short ratios, taker buy/sell volume, basis, and Options mark/open interest.",
	}, s.handleQueryDerivativesMetrics)

	mcp.AddTool(s.MCP, &mcp.Tool{
		Name:        "query_market_overview",
		Description: "Discover Recorder partitions/streams/historical contracts (including exited listings) plus live exchangeInfo/24h ticker snapshots, or browse the complete 4-market capabilities directory (action='capabilities').",
	}, s.handleQueryMarketOverview)

	mcp.AddTool(s.MCP, &mcp.Tool{
		Name:        "manage_local_cache",
		Description: "Inspect local BINANCE_MCP_DATA_DIR usage and SHA-256 verification status, estimate Recorder download bytes, or start/list/cancel/resume persistent bounded prefetch jobs.",
	}, s.handleManageLocalCache)

	mcp.AddTool(s.MCP, &mcp.Tool{
		Name:        "query_public_endpoint",
		Description: "Invoke any allowlisted Binance public REST GET endpoint or bounded WebSocket sampler by endpoint_id across spot, usdm, coinm, and options.",
	}, s.handleQueryPublicEndpoint)
}

// ---------- 1. query_klines ----------

type QueryKlinesInput struct {
	Market           string   `json:"market" jsonschema:"Market domain: spot, usdm, coinm, or options"`
	Symbols          []string `json:"symbols,omitempty" jsonschema:"List of trading symbols, e.g. [\"BTCUSDT\", \"ETHUSDT\"]"`
	Symbol           string   `json:"symbol,omitempty" jsonschema:"Single trading symbol convenience alias"`
	Interval         string   `json:"interval,omitempty" jsonschema:"Candle interval, e.g. 1s, 1m, 5m, 15m, 1h, 4h, 1d (default 1m)"`
	StartUTCNS       string   `json:"start_utc_ns,omitempty" jsonschema:"Inclusive window start in UTC nanoseconds (decimal string)"`
	EndUTCNS         string   `json:"end_utc_ns,omitempty" jsonschema:"Exclusive window end in UTC nanoseconds (decimal string)"`
	Source           string   `json:"source,omitempty" jsonschema:"Data source preference: auto, recorder, vision, or rest (default auto)"`
	KlineType        string   `json:"kline_type,omitempty" jsonschema:"Kline type: standard, continuous, mark, index, or premium (default standard)"`
	ContractType     string   `json:"contract_type,omitempty" jsonschema:"For continuousKlines: PERPETUAL, CURRENT_QUARTER, NEXT_QUARTER"`
	RevisionID       string   `json:"revision_id,omitempty" jsonschema:"Pinned Recorder export revision ID for reproducible/offline reads"`
	Offline          bool     `json:"offline,omitempty" jsonschema:"If true, never touch the network; reuse verified local caches only"`
	Limit            int      `json:"limit,omitempty" jsonschema:"Max preview rows returned in JSON response (default 100, max 1000)"`
	OutputFormat     string   `json:"output_format,omitempty" jsonschema:"Spill file format: jsonl, csv, or parquet (default jsonl)"`
	SaveToFile       bool     `json:"save_to_file,omitempty" jsonschema:"Force saving full dataset to BINANCE_MCP_DATA_DIR/results"`
	MaxDownloadBytes int64    `json:"max_download_bytes,omitempty" jsonschema:"Safety cap in bytes for automatic Recorder downloads (default 256 MiB)"`
}

func (s *Server) handleQueryKlines(ctx context.Context, _ *mcp.CallToolRequest, in QueryKlinesInput) (*mcp.CallToolResult, map[string]any, error) {
	m := binance.NormalizeMarket(in.Market)
	if m == "" {
		return nil, nil, fmt.Errorf("invalid market %q; expected spot, usdm, coinm, or options", in.Market)
	}
	symbols := mergeSymbols(in.Symbols, in.Symbol)
	if len(symbols) == 0 {
		return nil, nil, fmt.Errorf("at least one symbol is required")
	}
	startNS, endNS, err := parseNSBounds(in.StartUTCNS, in.EndUTCNS)
	if err != nil {
		return nil, nil, err
	}
	interval := strings.TrimSpace(in.Interval)
	if interval == "" {
		interval = "1m"
	}
	sourcePref := strings.ToLower(strings.TrimSpace(in.Source))
	if sourcePref == "" {
		sourcePref = "auto"
	}
	klineType := strings.ToLower(strings.TrimSpace(in.KlineType))
	if klineType == "" {
		klineType = "standard"
	}
	limit := clampLimit(in.Limit, s.Cfg.PreviewDefaultRows, s.Cfg.PreviewMaxRows)
	outFmt := strings.ToLower(strings.TrimSpace(in.OutputFormat))
	if outFmt == "" {
		outFmt = "jsonl"
	}

	// Route 1: Recorder agg_trade Decimal aggregation when supported and available.
	var fallbackNote string
	canTryRecorder := (sourcePref == "recorder" || (sourcePref == "auto" && (m == binance.MarketSpot || m == binance.MarketUSDM) && klineType == "standard"))
	if canTryRecorder && (in.RevisionID != "" || startNS > 0 || sourcePref == "recorder") {
		snap, sErr := s.Recorder.SyncSnapshot(ctx, in.RevisionID, in.Offline)
		if sErr == nil {
			plan, pErr := s.Recorder.PlanQuery(snap, string(m), []string{"agg_trade"}, startNS, endNS, false)
			if pErr == nil && plan.ReadyChunksMatched > 0 {
				if err := s.Recorder.MaterializePlan(ctx, plan, in.Offline, in.MaxDownloadBytes); err != nil {
					if sourcePref == "recorder" {
						return nil, nil, err
					}
					fallbackNote = fmt.Sprintf("Recorder materialization skipped (%v); fell back to Binance", err)
				} else {
					wReq := s.Recorder.BuildWorkerRequest("query_klines", plan)
					wReq.Symbols = symbols
					wReq.StartUTCNS = startNS
					wReq.EndUTCNS = endNS
					wReq.Interval = interval
					wReq.Limit = limit
					wReq.OutputFormat = outFmt
					if in.SaveToFile {
						wReq.OutputFile = makeResultFilePath(s.Cfg, "klines_recorder", outFmt)
					} else {
						// Always pass an output file if potentially >100 rows or parquet asked; worker writes only when non-empty.
						wReq.OutputFile = makeResultFilePath(s.Cfg, "klines_recorder", outFmt)
					}
					wRes, wErr := s.Recorder.RunWorker(ctx, wReq)
					if wErr == nil {
						totalRowsF, _ := wRes["total_rows"].(float64)
						if int(totalRowsF) > 0 || sourcePref == "recorder" {
							wRes["market"] = string(m)
							wRes["data_source"] = "recorder"
							wRes["recorder_provenance"] = planSummaryMap(plan)
							return nil, applyWorkerResponseBudget(s.Cfg, "klines", wRes, limit), nil
						}
						fallbackNote = "Recorder matched chunks contained zero agg_trade rows for requested symbol(s)/window; fell back cleanly to Binance"
					} else if sourcePref == "recorder" {
						return nil, nil, wErr
					} else {
						fallbackNote = fmt.Sprintf("Recorder worker failed (%v); fell back cleanly", wErr)
					}
				}
			} else if sourcePref == "recorder" {
				return nil, nil, fmt.Errorf("no Recorder agg_trade chunks found for market=%s window=[%d,%d)", m, startNS, endNS)
			} else {
				fallbackNote = "No ready Recorder agg_trade chunks covered the requested market/window; queried Binance upstream"
			}
		} else if sourcePref == "recorder" || in.RevisionID != "" {
			return nil, nil, fmt.Errorf("Recorder sync failed: %w", sErr)
		} else {
			fallbackNote = fmt.Sprintf("Recorder unavailable (%v); queried Binance upstream", sErr)
		}
	}

	if sourcePref == "recorder" {
		return nil, nil, fmt.Errorf("Recorder mode requires spot/usdm standard klines (got market=%s kline_type=%s)", m, klineType)
	}

	// Route 2: Binance Vision daily zip archives (historical, deterministic SHA-256 CHECKSUM).
	if sourcePref == "vision" || (sourcePref == "auto" && klineType == "standard" && m != binance.MarketOptions && startNS > 0) {
		days := datesForNSWindow(startNS, endNS)
		var allRows []map[string]any
		var archives []map[string]any
		visionOK := len(days) > 0
		for _, sym := range symbols {
			for _, day := range days {
				arch, vErr := s.Binance.FetchVisionDailyKlines(ctx, m, sym, interval, day, in.Offline)
				if vErr != nil {
					visionOK = false
					if sourcePref == "vision" {
						return nil, nil, vErr
					}
					fallbackNote = fmt.Sprintf("Vision %s %s %s unavailable (%v); fell back to REST", sym, interval, day, vErr)
					break
				}
				archives = append(archives, map[string]any{
					"symbol":    sym,
					"date":      day,
					"sha256":    arch.ChecksumHex,
					"cache_hit": arch.CacheHit,
					"zip_path":  arch.ZipPath,
				})
				vRows, rErr := binance.ReadVisionKlinesZip(arch.ZipPath, m, arch.ChecksumHex, startNS/1_000_000, endNS/1_000_000)
				if rErr != nil {
					return nil, nil, rErr
				}
				for _, vr := range vRows {
					allRows = append(allRows, map[string]any{
						"symbol":            sym,
						"open_time_utc_ns":  vr.OpenTimeUTCNS,
						"close_time_utc_ns": vr.CloseTimeUTCNS,
						"open":              vr.Open,
						"high":              vr.High,
						"low":               vr.Low,
						"close":             vr.Close,
						"volume":            vr.Volume,
						"quote_volume":      vr.QuoteVolume,
						"trade_count":       vr.TradeCount,
						"qty_unit":          vr.QtyUnit,
						"source":            "binance_vision",
						"checksum_sha256":   vr.RevisionOrHash,
					})
				}
			}
			if !visionOK {
				break
			}
		}
		if visionOK {
			payload := map[string]any{
				"market":       string(m),
				"interval":     interval,
				"data_source":  "binance_vision",
				"archives":     archives,
				"qty_unit":     qtyUnitForMarket(m),
			}
			if fallbackNote != "" {
				payload["note"] = fallbackNote
			}
			return nil, applyResponseBudget(s.Cfg, "klines_vision", payload, "rows", allRows, in.SaveToFile, outFmt, limit), nil
		}
	}

	// Route 3: Binance REST API (exact homogeneous series by kline_type & market).
	endpointID, err := klineEndpointID(m, klineType)
	if err != nil {
		return nil, nil, err
	}
	capEntry, ok := binance.LookupCapability(endpointID)
	if !ok {
		return nil, nil, fmt.Errorf("missing capability %s", endpointID)
	}
	var allRows []map[string]any
	for _, sym := range symbols {
		params := map[string]string{
			"interval": interval,
			"limit":    strconv.Itoa(limit),
		}
		switch klineType {
		case "continuous":
			params["pair"] = sym
			ct := strings.ToUpper(strings.TrimSpace(in.ContractType))
			if ct == "" {
				ct = "PERPETUAL"
			}
			params["contractType"] = ct
		case "index":
			params["pair"] = sym
		default:
			params["symbol"] = sym
		}
		if startNS > 0 {
			params["startTime"] = strconv.FormatInt(startNS/1_000_000, 10)
		}
		if endNS > 0 {
			params["endTime"] = strconv.FormatInt((endNS-1)/1_000_000, 10)
		}
		res, err := s.Binance.ExecuteCapability(ctx, capEntry, params, in.Offline)
		if err != nil {
			return nil, nil, err
		}
		parsed, pErr := parseRESTKlineArray(res.Payload, sym, m, "binance_rest")
		if pErr != nil {
			return nil, nil, fmt.Errorf("%s parse: %w", endpointID, pErr)
		}
		allRows = append(allRows, parsed...)
	}

	payload := map[string]any{
		"market":      string(m),
		"interval":    interval,
		"kline_type":  klineType,
		"endpoint_id": endpointID,
		"data_source": "binance_rest",
		"qty_unit":    qtyUnitForMarket(m),
	}
	if fallbackNote != "" {
		payload["note"] = fallbackNote
	}
	return nil, applyResponseBudget(s.Cfg, "klines_rest", payload, "rows", allRows, in.SaveToFile, outFmt, limit), nil
}

func klineEndpointID(m binance.Market, klineType string) (string, error) {
	switch klineType {
	case "standard":
		return string(m) + ".klines", nil
	case "continuous":
		if m != binance.MarketUSDM && m != binance.MarketCOINM {
			return "", fmt.Errorf("continuousKlines not supported for market %s", m)
		}
		return string(m) + ".continuousKlines", nil
	case "mark":
		if m != binance.MarketUSDM && m != binance.MarketCOINM {
			return "", fmt.Errorf("markPriceKlines not supported for market %s", m)
		}
		return string(m) + ".markPriceKlines", nil
	case "index":
		if m != binance.MarketUSDM && m != binance.MarketCOINM {
			return "", fmt.Errorf("indexPriceKlines not supported for market %s", m)
		}
		return string(m) + ".indexPriceKlines", nil
	case "premium":
		if m != binance.MarketUSDM {
			return "", fmt.Errorf("premiumIndexKlines only registered on usdm")
		}
		return "usdm.premiumIndexKlines", nil
	default:
		return "", fmt.Errorf("unknown kline_type %q", klineType)
	}
}

func qtyUnitForMarket(m binance.Market) string {
	if m == binance.MarketCOINM {
		return "contracts"
	}
	return "base_asset"
}

// ---------- 2. query_orderbook ----------

type QueryOrderbookInput struct {
	Market           string   `json:"market" jsonschema:"Market domain: spot, usdm, coinm, or options"`
	Symbols          []string `json:"symbols,omitempty" jsonschema:"Symbols to query"`
	Symbol           string   `json:"symbol,omitempty" jsonschema:"Single symbol convenience alias"`
	Mode             string   `json:"mode,omitempty" jsonschema:"auto, reconstruct (historical Recorder only), or live_snapshot (REST depth)"`
	StartUTCNS       string   `json:"start_utc_ns,omitempty" jsonschema:"Historical window start UTC nanoseconds (triggers Recorder reconstruction)"`
	EndUTCNS         string   `json:"end_utc_ns,omitempty" jsonschema:"Historical window end UTC nanoseconds"`
	DepthLevels      int      `json:"depth_levels,omitempty" jsonschema:"Top-K bid/ask levels & imbalance depth (default 5, max 50)"`
	SampleStepMS     int      `json:"sample_step_ms,omitempty" jsonschema:"Sampling cadence in milliseconds during Recorder playback (default 1000)"`
	RevisionID       string   `json:"revision_id,omitempty" jsonschema:"Pinned Recorder export revision ID"`
	Offline          bool     `json:"offline,omitempty" jsonschema:"Offline zero-network mode"`
	Limit            int      `json:"limit,omitempty" jsonschema:"Max preview snapshots returned (default 100, max 1000)"`
	OutputFormat     string   `json:"output_format,omitempty" jsonschema:"Spill file format: jsonl or csv"`
	SaveToFile       bool     `json:"save_to_file,omitempty" jsonschema:"Force saving full output to file"`
	MaxDownloadBytes int64    `json:"max_download_bytes,omitempty" jsonschema:"Safety download cap in bytes"`
}

func (s *Server) handleQueryOrderbook(ctx context.Context, _ *mcp.CallToolRequest, in QueryOrderbookInput) (*mcp.CallToolResult, map[string]any, error) {
	m := binance.NormalizeMarket(in.Market)
	if m == "" {
		return nil, nil, fmt.Errorf("invalid market %q", in.Market)
	}
	symbols := mergeSymbols(in.Symbols, in.Symbol)
	if len(symbols) == 0 {
		return nil, nil, fmt.Errorf("at least one symbol is required")
	}
	startNS, endNS, err := parseNSBounds(in.StartUTCNS, in.EndUTCNS)
	if err != nil {
		return nil, nil, err
	}
	topK := clampLimit(in.DepthLevels, 5, 50)
	limit := clampLimit(in.Limit, s.Cfg.PreviewDefaultRows, s.Cfg.PreviewMaxRows)
	sampleStepMS := in.SampleStepMS
	if sampleStepMS <= 0 {
		sampleStepMS = 1000
	}
	outFmt := strings.ToLower(strings.TrimSpace(in.OutputFormat))
	if outFmt == "" {
		outFmt = "jsonl"
	}

	mode := strings.ToLower(strings.TrimSpace(in.Mode))
	historicalRequested := mode == "reconstruct" || startNS > 0 || endNS > 0 || in.RevisionID != ""

	// Guardrail: NEVER backfill historical orderbook requests using current live REST depth.
	if historicalRequested {
		if m != binance.MarketSpot && m != binance.MarketUSDM {
			return nil, nil, fmt.Errorf("historical orderbook reconstruction is only available on Recorder-backed spot/usdm; refusing to backfill with current live REST depth")
		}
		snap, err := s.Recorder.SyncSnapshot(ctx, in.RevisionID, in.Offline)
		if err != nil {
			return nil, nil, fmt.Errorf("Recorder sync failed for historical orderbook (refusing REST backfill): %w", err)
		}
		plan, err := s.Recorder.PlanQuery(snap, string(m), []string{"diff_depth", "rest_observations"}, startNS, endNS, true)
		if err != nil {
			return nil, nil, err
		}
		if plan.ReadyChunksMatched == 0 {
			return nil, nil, fmt.Errorf("no historical Recorder depth chunks covered market=%s window=[%d,%d); refusing to backfill with current REST depth", m, startNS, endNS)
		}
		if err := s.Recorder.MaterializePlan(ctx, plan, in.Offline, in.MaxDownloadBytes); err != nil {
			return nil, nil, err
		}
		wReq := s.Recorder.BuildWorkerRequest("query_orderbook", plan)
		wReq.Symbols = symbols
		wReq.StartUTCNS = startNS
		wReq.EndUTCNS = endNS
		wReq.DepthLevels = topK
		wReq.SampleStepMS = sampleStepMS
		wReq.Limit = limit
		wReq.OutputFormat = outFmt
		wReq.OutputFile = makeResultFilePath(s.Cfg, "orderbook_recorder", outFmt)
		wRes, err := s.Recorder.RunWorker(ctx, wReq)
		if err != nil {
			return nil, nil, err
		}
		wRes["market"] = string(m)
		wRes["data_source"] = "recorder"
		wRes["recorder_provenance"] = planSummaryMap(plan)
		return nil, applyWorkerResponseBudget(s.Cfg, "orderbook", wRes, limit), nil
	}

	// Live snapshot mode (only when no historical window/revision was requested).
	endpointID := string(m) + ".depth"
	capEntry, ok := binance.LookupCapability(endpointID)
	if !ok {
		return nil, nil, fmt.Errorf("no depth capability for market %s", m)
	}
	var rows []map[string]any
	restLimit := "20"
	if topK > 20 {
		restLimit = "50"
	}
	for _, sym := range symbols {
		params := map[string]string{"symbol": sym, "limit": restLimit}
		res, err := s.Binance.ExecuteCapability(ctx, capEntry, params, in.Offline)
		if err != nil {
			return nil, nil, err
		}
		var body struct {
			LastUpdateID int64      `json:"lastUpdateId"`
			Bids         [][]string `json:"bids"`
			Asks         [][]string `json:"asks"`
		}
		if err := json.Unmarshal(res.Payload, &body); err != nil {
			return nil, nil, fmt.Errorf("parse %s depth: %w", sym, err)
		}
		bids := body.Bids
		if len(bids) > topK {
			bids = bids[:topK]
		}
		asks := body.Asks
		if len(asks) > topK {
			asks = asks[:topK]
		}
		imb := computeTopKImbalance(bids, asks)
		rows = append(rows, map[string]any{
			"symbol":                                   sym,
			"market":                                   string(m),
			"snapshot_fetched_utc":                     res.FetchedAt,
			"update_id":                                strconv.FormatInt(body.LastUpdateID, 10),
			fmt.Sprintf("top_%d_depth_imbalance", topK): imb,
			"bids":                                     bids,
			"asks":                                     asks,
			"state":                                    "LIVE_REST_SNAPSHOT",
			"source":                                   "binance_rest",
		})
	}
	payload := map[string]any{
		"market":               string(m),
		"data_source":          "binance_rest",
		"imbalance_definition": fmt.Sprintf("top_%d_depth_imbalance = (sum_bid_qty - sum_ask_qty) / (sum_bid_qty + sum_ask_qty); NOT order-flow imbalance (OFI)", topK),
	}
	return nil, applyResponseBudget(s.Cfg, "orderbook_live", payload, "rows", rows, in.SaveToFile, outFmt, limit), nil
}

func computeTopKImbalance(bids, asks [][]string) string {
	var bidSum, askSum float64
	for _, lvl := range bids {
		if len(lvl) >= 2 {
			if q, err := strconv.ParseFloat(lvl[1], 64); err == nil {
				bidSum += q
			}
		}
	}
	for _, lvl := range asks {
		if len(lvl) >= 2 {
			if q, err := strconv.ParseFloat(lvl[1], 64); err == nil {
				askSum += q
			}
		}
	}
	denom := bidSum + askSum
	if denom <= 0 {
		return "0"
	}
	return strconv.FormatFloat((bidSum-askSum)/denom, 'f', 8, 64)
}

// ---------- 3. query_trades_and_liquidations ----------

type QueryTradesLiquidationsInput struct {
	Market           string   `json:"market" jsonschema:"Market domain: spot, usdm, coinm, or options"`
	Symbols          []string `json:"symbols,omitempty" jsonschema:"Symbols list"`
	Symbol           string   `json:"symbol,omitempty" jsonschema:"Single symbol convenience alias"`
	RecordKind       string   `json:"record_kind,omitempty" jsonschema:"all, trades, or liquidations (default all)"`
	StartUTCNS       string   `json:"start_utc_ns,omitempty" jsonschema:"Window start in UTC nanoseconds"`
	EndUTCNS         string   `json:"end_utc_ns,omitempty" jsonschema:"Window end in UTC nanoseconds"`
	Source           string   `json:"source,omitempty" jsonschema:"auto, recorder, or rest"`
	RevisionID       string   `json:"revision_id,omitempty" jsonschema:"Pinned Recorder export revision ID"`
	Offline          bool     `json:"offline,omitempty" jsonschema:"Offline zero-network mode"`
	Limit            int      `json:"limit,omitempty" jsonschema:"Max preview rows (default 100, max 1000)"`
	OutputFormat     string   `json:"output_format,omitempty" jsonschema:"jsonl, csv, or parquet"`
	SaveToFile       bool     `json:"save_to_file,omitempty" jsonschema:"Save full dataset to file"`
	MaxDownloadBytes int64    `json:"max_download_bytes,omitempty" jsonschema:"Safety download cap in bytes"`
}

func (s *Server) handleQueryTradesAndLiquidations(ctx context.Context, _ *mcp.CallToolRequest, in QueryTradesLiquidationsInput) (*mcp.CallToolResult, map[string]any, error) {
	m := binance.NormalizeMarket(in.Market)
	if m == "" {
		return nil, nil, fmt.Errorf("invalid market %q", in.Market)
	}
	symbols := mergeSymbols(in.Symbols, in.Symbol)
	if len(symbols) == 0 {
		return nil, nil, fmt.Errorf("at least one symbol is required")
	}
	startNS, endNS, err := parseNSBounds(in.StartUTCNS, in.EndUTCNS)
	if err != nil {
		return nil, nil, err
	}
	kind := strings.ToLower(strings.TrimSpace(in.RecordKind))
	if kind == "" {
		kind = "all"
	}
	if m == binance.MarketSpot && kind == "liquidations" {
		return nil, map[string]any{
			"market":             "spot",
			"liquidation_status": "not_applicable_for_spot",
			"total_rows":         0,
			"rows":               []any{},
			"note":               "Spot market does not have forced liquidation (forceOrder) events.",
		}, nil
	}

	limit := clampLimit(in.Limit, s.Cfg.PreviewDefaultRows, s.Cfg.PreviewMaxRows)
	outFmt := strings.ToLower(strings.TrimSpace(in.OutputFormat))
	if outFmt == "" {
		outFmt = "jsonl"
	}
	sourcePref := strings.ToLower(strings.TrimSpace(in.Source))
	if sourcePref == "" {
		sourcePref = "auto"
	}

	canRecorder := (sourcePref == "recorder" || (sourcePref == "auto" && (m == binance.MarketSpot || m == binance.MarketUSDM)))
	if canRecorder && (in.RevisionID != "" || startNS > 0 || kind == "liquidations" || sourcePref == "recorder") {
		snap, sErr := s.Recorder.SyncSnapshot(ctx, in.RevisionID, in.Offline)
		if sErr == nil {
			streams := []string{"agg_trade"}
			if kind == "liquidations" {
				streams = []string{"liquidation"}
			} else if kind == "all" && m == binance.MarketUSDM {
				streams = []string{"agg_trade", "liquidation"}
			}
			plan, pErr := s.Recorder.PlanQuery(snap, string(m), streams, startNS, endNS, false)
			if pErr == nil && plan.ReadyChunksMatched > 0 {
				if err := s.Recorder.MaterializePlan(ctx, plan, in.Offline, in.MaxDownloadBytes); err != nil {
					if sourcePref == "recorder" {
						return nil, nil, err
					}
				} else {
					wReq := s.Recorder.BuildWorkerRequest("query_trades_and_liquidations", plan)
					wReq.Symbols = symbols
					wReq.StartUTCNS = startNS
					wReq.EndUTCNS = endNS
					wReq.RecordKind = kind
					wReq.Limit = limit
					wReq.OutputFormat = outFmt
					wReq.OutputFile = makeResultFilePath(s.Cfg, "trades_liq_recorder", outFmt)
					wRes, wErr := s.Recorder.RunWorker(ctx, wReq)
					if wErr == nil {
						wRes["market"] = string(m)
						wRes["data_source"] = "recorder"
						wRes["recorder_provenance"] = planSummaryMap(plan)
						return nil, applyWorkerResponseBudget(s.Cfg, "trades_liq", wRes, limit), nil
					} else if sourcePref == "recorder" {
						return nil, nil, wErr
					}
				}
			} else if sourcePref == "recorder" {
				return nil, nil, fmt.Errorf("no ready Recorder chunks for streams=%v in window [%d,%d)", streams, startNS, endNS)
			}
		} else if sourcePref == "recorder" || in.RevisionID != "" {
			return nil, nil, sErr
		}
	}

	if kind == "liquidations" {
		return nil, map[string]any{
			"market":             string(m),
			"data_source":        "unavailable_without_recorder_or_ws",
			"liquidation_status": "historical_forceOrder_rest_unavailable_try_recorder_or_ws_forceOrder",
			"note":               "Binance deprecated public historical REST allForceOrders; use Recorder liquidation chunks or query_public_endpoint(usdm.ws.forceOrder / coinm.ws.forceOrder).",
			"total_rows":         0,
			"rows":               []any{},
		}, nil
	}

	endpointID := string(m) + ".aggTrades"
	if m == binance.MarketOptions {
		endpointID = "options.trades"
	}
	capEntry, ok := binance.LookupCapability(endpointID)
	if !ok {
		return nil, nil, fmt.Errorf("missing capability %s", endpointID)
	}
	var allRows []map[string]any
	for _, sym := range symbols {
		params := map[string]string{"symbol": sym, "limit": strconv.Itoa(limit)}
		if m != binance.MarketOptions {
			if startNS > 0 {
				params["startTime"] = strconv.FormatInt(startNS/1_000_000, 10)
			}
			if endNS > 0 {
				params["endTime"] = strconv.FormatInt((endNS-1)/1_000_000, 10)
			}
		}
		res, err := s.Binance.ExecuteCapability(ctx, capEntry, params, in.Offline)
		if err != nil {
			return nil, nil, err
		}
		var items []map[string]any
		if err := json.Unmarshal(res.Payload, &items); err != nil {
			return nil, nil, fmt.Errorf("parse %s response: %w", endpointID, err)
		}
		for _, it := range items {
			tsMS, _ := toInt64Any(it["T"])
			if tsMS == 0 {
				tsMS, _ = toInt64Any(it["time"])
			}
			row := map[string]any{
				"record_type":   "trade",
				"symbol":        sym,
				"trade_utc_ns":  strconv.FormatInt(tsMS*1_000_000, 10),
				"price":         formatScalar(it["p"]),
				"quantity":      formatScalar(it["q"]),
				"buyer_is_maker": it["m"],
				"qty_unit":      qtyUnitForMarket(m),
				"source":        "binance_rest",
			}
			if it["price"] != nil && row["price"] == "" {
				row["price"] = formatScalar(it["price"])
			}
			if it["qty"] != nil && row["quantity"] == "" {
				row["quantity"] = formatScalar(it["qty"])
			}
			allRows = append(allRows, row)
		}
	}

	liqStatus := "not_applicable_for_spot"
	if m != binance.MarketSpot {
		liqStatus = "not_returned_in_rest_aggTrades_use_recorder_or_ws_forceOrder"
	}
	payload := map[string]any{
		"market":             string(m),
		"data_source":        "binance_rest",
		"endpoint_id":        endpointID,
		"liquidation_status": liqStatus,
	}
	return nil, applyResponseBudget(s.Cfg, "trades_rest", payload, "rows", allRows, in.SaveToFile, outFmt, limit), nil
}

// ---------- 4. query_derivatives_metrics ----------

type QueryDerivativesInput struct {
	Market           string   `json:"market" jsonschema:"Derivatives market: usdm, coinm, or options"`
	Symbols          []string `json:"symbols,omitempty" jsonschema:"Symbols list"`
	Symbol           string   `json:"symbol,omitempty" jsonschema:"Single symbol convenience alias"`
	Metric           string   `json:"metric,omitempty" jsonschema:"all, mark_price_funding, open_interest, open_interest_hist, top_long_short_account, top_long_short_position, global_long_short, taker_buy_sell_vol, basis, funding_history, options_mark, options_open_interest"`
	Period           string   `json:"period,omitempty" jsonschema:"Period for statistics: 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d (default 5m)"`
	Pair             string   `json:"pair,omitempty" jsonschema:"Pair (e.g. BTCUSD for COIN-M statistics/basis)"`
	ContractType     string   `json:"contract_type,omitempty" jsonschema:"PERPETUAL, CURRENT_QUARTER, NEXT_QUARTER"`
	UnderlyingAsset  string   `json:"underlying_asset,omitempty" jsonschema:"For options_open_interest, e.g. BTC"`
	Expiration       string   `json:"expiration,omitempty" jsonschema:"For options_open_interest YYMMDD, e.g. 261225"`
	StartUTCNS       string   `json:"start_utc_ns,omitempty" jsonschema:"Window start in UTC nanoseconds"`
	EndUTCNS         string   `json:"end_utc_ns,omitempty" jsonschema:"Window end in UTC nanoseconds"`
	Source           string   `json:"source,omitempty" jsonschema:"auto, recorder, or rest"`
	RevisionID       string   `json:"revision_id,omitempty" jsonschema:"Pinned Recorder export revision ID"`
	Offline          bool     `json:"offline,omitempty" jsonschema:"Offline zero-network mode"`
	Limit            int      `json:"limit,omitempty" jsonschema:"Max preview rows (default 100, max 1000)"`
	OutputFormat     string   `json:"output_format,omitempty" jsonschema:"jsonl or csv"`
	SaveToFile       bool     `json:"save_to_file,omitempty" jsonschema:"Save full dataset to file"`
	MaxDownloadBytes int64    `json:"max_download_bytes,omitempty" jsonschema:"Safety download cap in bytes"`
}

func (s *Server) handleQueryDerivativesMetrics(ctx context.Context, _ *mcp.CallToolRequest, in QueryDerivativesInput) (*mcp.CallToolResult, map[string]any, error) {
	m := binance.NormalizeMarket(in.Market)
	if m == binance.MarketSpot || m == "" {
		return nil, nil, fmt.Errorf("derivatives metrics require market=usdm, coinm, or options (got %q)", in.Market)
	}
	symbols := mergeSymbols(in.Symbols, in.Symbol)
	startNS, endNS, err := parseNSBounds(in.StartUTCNS, in.EndUTCNS)
	if err != nil {
		return nil, nil, err
	}
	metric := strings.ToLower(strings.TrimSpace(in.Metric))
	if metric == "" {
		if m == binance.MarketOptions {
			metric = "options_mark"
		} else {
			metric = "mark_price_funding"
		}
	}
	period := strings.TrimSpace(in.Period)
	if period == "" {
		period = "5m"
	}
	limit := clampLimit(in.Limit, s.Cfg.PreviewDefaultRows, s.Cfg.PreviewMaxRows)
	outFmt := strings.ToLower(strings.TrimSpace(in.OutputFormat))
	if outFmt == "" {
		outFmt = "jsonl"
	}
	sourcePref := strings.ToLower(strings.TrimSpace(in.Source))
	if sourcePref == "" {
		sourcePref = "auto"
	}

	// Route 1: Recorder USD-M historical mark_price & rest_observations metrics.
	if (sourcePref == "recorder" || (sourcePref == "auto" && m == binance.MarketUSDM)) && (in.RevisionID != "" || startNS > 0 || sourcePref == "recorder") {
		snap, sErr := s.Recorder.SyncSnapshot(ctx, in.RevisionID, in.Offline)
		if sErr == nil {
			plan, pErr := s.Recorder.PlanQuery(snap, "usdm", []string{"mark_price", "rest_observations"}, startNS, endNS, false)
			if pErr == nil && plan.ReadyChunksMatched > 0 {
				if err := s.Recorder.MaterializePlan(ctx, plan, in.Offline, in.MaxDownloadBytes); err == nil {
					wReq := s.Recorder.BuildWorkerRequest("query_derivatives_metrics", plan)
					wReq.Symbols = symbols
					wReq.StartUTCNS = startNS
					wReq.EndUTCNS = endNS
					wReq.Limit = limit
					wReq.OutputFormat = outFmt
					wReq.OutputFile = makeResultFilePath(s.Cfg, "deriv_recorder", outFmt)
					wRes, wErr := s.Recorder.RunWorker(ctx, wReq)
					if wErr == nil {
						tr, _ := wRes["total_rows"].(float64)
						if int(tr) > 0 || sourcePref == "recorder" {
							wRes["market"] = string(m)
							wRes["data_source"] = "recorder"
							wRes["recorder_provenance"] = planSummaryMap(plan)
							return nil, applyWorkerResponseBudget(s.Cfg, "deriv", wRes, limit), nil
						}
					} else if sourcePref == "recorder" {
						return nil, nil, wErr
					}
				} else if sourcePref == "recorder" {
					return nil, nil, err
				}
			} else if sourcePref == "recorder" {
				return nil, nil, fmt.Errorf("no ready Recorder usdm mark_price/rest_observations chunks in window")
			}
		} else if sourcePref == "recorder" || in.RevisionID != "" {
			return nil, nil, sErr
		}
	}

	// Route 2: Binance REST API across usdm, coinm, options.
	endpointID, paramsBuilder, err := resolveDerivativesREST(m, metric, period, in, symbols, startNS, endNS, limit)
	if err != nil {
		return nil, nil, err
	}
	capEntry, ok := binance.LookupCapability(endpointID)
	if !ok {
		return nil, nil, fmt.Errorf("missing capability %s", endpointID)
	}

	targets := symbols
	if len(targets) == 0 {
		targets = []string{""}
	}
	var rows []map[string]any
	for _, sym := range targets {
		params := paramsBuilder(sym)
		res, err := s.Binance.ExecuteCapability(ctx, capEntry, params, in.Offline)
		if err != nil {
			return nil, nil, err
		}
		var arr []map[string]any
		if err := json.Unmarshal(res.Payload, &arr); err == nil {
			for _, item := range arr {
				item["_query_symbol"] = sym
				item["source"] = "binance_rest"
				rows = append(rows, item)
			}
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal(res.Payload, &obj); err == nil {
			obj["_query_symbol"] = sym
			obj["source"] = "binance_rest"
			rows = append(rows, obj)
		}
	}

	payload := map[string]any{
		"market":      string(m),
		"metric":      metric,
		"endpoint_id": endpointID,
		"data_source": "binance_rest",
		"qty_unit":    capEntry.QtyUnit,
	}
	return nil, applyResponseBudget(s.Cfg, "deriv_rest", payload, "rows", rows, in.SaveToFile, outFmt, limit), nil
}

func resolveDerivativesREST(m binance.Market, metric, period string, in QueryDerivativesInput, symbols []string, startNS, endNS int64, limit int) (string, func(string) map[string]string, error) {
	derivePair := func(sym string) string {
		if strings.TrimSpace(in.Pair) != "" {
			return strings.ToUpper(strings.TrimSpace(in.Pair))
		}
		if idx := strings.Index(sym, "_"); idx > 0 {
			return sym[:idx]
		}
		return sym
	}
	ct := strings.ToUpper(strings.TrimSpace(in.ContractType))
	if ct == "" {
		ct = "PERPETUAL"
	}

	addWindow := func(p map[string]string) {
		if startNS > 0 {
			p["startTime"] = strconv.FormatInt(startNS/1_000_000, 10)
		}
		if endNS > 0 {
			p["endTime"] = strconv.FormatInt((endNS-1)/1_000_000, 10)
		}
		p["limit"] = strconv.Itoa(limit)
	}

	if m == binance.MarketOptions {
		switch metric {
		case "options_mark", "mark_price_funding", "all":
			return "options.mark", func(sym string) map[string]string {
				if sym != "" {
					return map[string]string{"symbol": sym}
				}
				return map[string]string{}
			}, nil
		case "options_open_interest", "open_interest":
			if strings.TrimSpace(in.UnderlyingAsset) == "" || strings.TrimSpace(in.Expiration) == "" {
				return "", nil, fmt.Errorf("options openInterest requires underlying_asset (e.g. BTC) and expiration (e.g. 261225)")
			}
			return "options.openInterest", func(_ string) map[string]string {
				return map[string]string{
					"underlyingAsset": strings.ToUpper(strings.TrimSpace(in.UnderlyingAsset)),
					"expiration":      strings.TrimSpace(in.Expiration),
				}
			}, nil
		default:
			return "", nil, fmt.Errorf("unsupported options metric %q", metric)
		}
	}

	// usdm & coinm
	switch metric {
	case "mark_price_funding", "all":
		return string(m) + ".premiumIndex", func(sym string) map[string]string {
			if sym != "" {
				return map[string]string{"symbol": sym}
			}
			return map[string]string{}
		}, nil
	case "funding_history":
		return string(m) + ".fundingRate", func(sym string) map[string]string {
			p := map[string]string{}
			if sym != "" {
				p["symbol"] = sym
			}
			addWindow(p)
			return p
		}, nil
	case "open_interest":
		return string(m) + ".openInterest", func(sym string) map[string]string {
			return map[string]string{"symbol": sym}
		}, nil
	case "open_interest_hist":
		return string(m) + ".openInterestHist", func(sym string) map[string]string {
			p := map[string]string{"period": period}
			if m == binance.MarketCOINM {
				p["pair"] = derivePair(sym)
				p["contractType"] = ct
			} else {
				p["symbol"] = sym
			}
			addWindow(p)
			return p
		}, nil
	case "top_long_short_account":
		return string(m) + ".topLongShortAccountRatio", func(sym string) map[string]string {
			p := map[string]string{"period": period}
			if m == binance.MarketCOINM {
				p["pair"] = derivePair(sym)
			} else {
				p["symbol"] = sym
			}
			addWindow(p)
			return p
		}, nil
	case "top_long_short_position":
		return string(m) + ".topLongShortPositionRatio", func(sym string) map[string]string {
			p := map[string]string{"period": period}
			if m == binance.MarketCOINM {
				p["pair"] = derivePair(sym)
			} else {
				p["symbol"] = sym
			}
			addWindow(p)
			return p
		}, nil
	case "global_long_short":
		return string(m) + ".globalLongShortAccountRatio", func(sym string) map[string]string {
			p := map[string]string{"period": period}
			if m == binance.MarketCOINM {
				p["pair"] = derivePair(sym)
			} else {
				p["symbol"] = sym
			}
			addWindow(p)
			return p
		}, nil
	case "taker_buy_sell_vol":
		if m == binance.MarketCOINM {
			return "coinm.takerBuySellVol", func(sym string) map[string]string {
				p := map[string]string{"pair": derivePair(sym), "contractType": ct, "period": period}
				addWindow(p)
				return p
			}, nil
		}
		return "usdm.takerlongshortRatio", func(sym string) map[string]string {
			p := map[string]string{"symbol": sym, "period": period}
			addWindow(p)
			return p
		}, nil
	case "basis":
		return string(m) + ".basis", func(sym string) map[string]string {
			p := map[string]string{"pair": derivePair(sym), "contractType": ct, "period": period}
			addWindow(p)
			return p
		}, nil
	default:
		return "", nil, fmt.Errorf("unknown metric %q", metric)
	}
}

// ---------- 5. query_market_overview ----------

type QueryMarketOverviewInput struct {
	Action          string   `json:"action,omitempty" jsonschema:"overview (default) or capabilities"`
	Market          string   `json:"market,omitempty" jsonschema:"spot, usdm, coinm, options, or all"`
	Symbols         []string `json:"symbols,omitempty" jsonschema:"Optional symbol filter"`
	Symbol          string   `json:"symbol,omitempty" jsonschema:"Single symbol filter"`
	Search          string   `json:"search,omitempty" jsonschema:"Keyword filter when action='capabilities'"`
	IncludeRecorder bool     `json:"include_recorder,omitempty" jsonschema:"Include Recorder index & contract spec discovery (default true)"`
	IncludeLiveREST bool     `json:"include_live_rest,omitempty" jsonschema:"Include live REST exchangeInfo / 24hr ticker (default true)"`
	RevisionID      string   `json:"revision_id,omitempty" jsonschema:"Pinned Recorder export revision ID"`
	Offline         bool     `json:"offline,omitempty" jsonschema:"Offline zero-network mode"`
}

func (s *Server) handleQueryMarketOverview(ctx context.Context, _ *mcp.CallToolRequest, in QueryMarketOverviewInput) (*mcp.CallToolResult, map[string]any, error) {
	action := strings.ToLower(strings.TrimSpace(in.Action))
	if action == "" {
		action = "overview"
	}
	if action == "capabilities" {
		caps := binance.Capabilities(in.Market, in.Search)
		return nil, map[string]any{
			"action":       "capabilities",
			"count":        len(caps),
			"capabilities": caps,
		}, nil
	}

	m := binance.NormalizeMarket(in.Market)
	if m == "" && strings.ToLower(strings.TrimSpace(in.Market)) == "" {
		m = binance.MarketUSDM
	}
	symbols := mergeSymbols(in.Symbols, in.Symbol)
	out := map[string]any{
		"action": "overview",
		"market": in.Market,
	}

	// Default IncludeRecorder & IncludeLiveREST to true unless explicitly set false via JSON?
	// Note: Go zero-value for bool is false, so if caller omits both or leaves default, interpret missing? Better: check both!
	// Actually: in MCP JSON schema optional bool omits default false -> we should include both unless Offline disables REST or action explicitly excludes!
	// Wait: let's treat IncludeRecorder = true whenever (in.RevisionID != "" || m == Spot || m == USDM || m == "") and always check unless caller asks otherwise; wait - if someone passes include_live_rest: true vs omitted, let's check both when market allows unless Offline!
	if in.RevisionID != "" || m == binance.MarketSpot || m == binance.MarketUSDM || strings.ToLower(strings.TrimSpace(in.Market)) == "all" {
		if snap, sErr := s.Recorder.SyncSnapshot(ctx, in.RevisionID, in.Offline); sErr == nil {
			var partsSummary []map[string]any
			for _, p := range snap.Partitions {
				if m != "" && string(m) != p.Manifest.Market {
					continue
				}
				partsSummary = append(partsSummary, map[string]any{
					"market":          p.Manifest.Market,
					"partition_id":    p.Manifest.PartitionID,
					"state":           p.Metadata.State,
					"is_cold":         p.Metadata.IsCold,
					"min_utc_ns":      strconv.FormatInt(p.Metadata.ReceiveTimeUTCRangeNS.Min, 10),
					"max_utc_ns":      strconv.FormatInt(p.Metadata.ReceiveTimeUTCRangeNS.Max, 10),
					"counts":          p.Manifest.Counts,
					"gaps_count":      len(p.Availability.Gaps),
					"discontinuities": len(p.Availability.DiscontinuityEvents),
				})
			}
			recOverview := map[string]any{
				"revision_id":      snap.RevisionID,
				"generated_at_utc": snap.GeneratedAtUTC,
				"partitions":       partsSummary,
			}

			// Inspect local/lightweight rest_observations chunks if already cached OR bounded (<= 64 MiB) to surface historical/exited symbols!
			targetM := string(m)
			if targetM == "" {
				targetM = "usdm"
			}
			if plan, pErr := s.Recorder.PlanQuery(snap, targetM, []string{"rest_observations"}, 0, 0, false); pErr == nil && plan.ReadyChunksMatched > 0 {
				if plan.EstimatedDownloadBytes <= 64*1024*1024 || in.Offline {
					if matErr := s.Recorder.MaterializePlan(ctx, plan, in.Offline, 64*1024*1024); matErr == nil {
						// Inspect first + last rest_observations chunks to stay bounded and discover contract directory!
						if len(plan.PlannedChunks) > 2 {
							plan.PlannedChunks = []recorder.PlannedChunk{plan.PlannedChunks[0], plan.PlannedChunks[len(plan.PlannedChunks)-1]}
						}
						wReq := s.Recorder.BuildWorkerRequest("inspect_overview", plan)
						wReq.Symbols = symbols
						if wRes, wErr := s.Recorder.RunWorker(ctx, wReq); wErr == nil {
							recOverview["historical_contract_specs_count"] = len(mapStringAny(wRes["contract_specs"]))
							recOverview["historical_contract_specs_sample"] = trimMapKeys(mapStringAny(wRes["contract_specs"]), 30)
						}
					}
				} else {
					recOverview["rest_observations_plan"] = planSummaryMap(plan)
				}
			}
			out["recorder"] = recOverview
		} else {
			out["recorder_error"] = sErr.Error()
		}
	}

	if !in.Offline && m != "" {
		exCap, ok := binance.LookupCapability(string(m) + ".exchangeInfo")
		if ok {
			exParams := map[string]string{}
			if len(symbols) == 1 && (m == binance.MarketSpot) {
				exParams["symbol"] = symbols[0]
			}
			if res, err := s.Binance.ExecuteCapability(ctx, exCap, exParams, false); err == nil {
				out["live_exchange_info"] = summarizeExchangeInfo(res.Payload, m, symbols)
			} else {
				out["live_exchange_info_error"] = err.Error()
			}
		}
		if len(symbols) > 0 {
			tickID := string(m) + ".ticker.24hr"
			if m == binance.MarketOptions {
				tickID = "options.ticker"
			}
			if tCap, ok := binance.LookupCapability(tickID); ok {
				var tickers []any
				for _, sym := range symbols {
					if res, err := s.Binance.ExecuteCapability(ctx, tCap, map[string]string{"symbol": sym}, false); err == nil {
						var parsed any
						_ = json.Unmarshal(res.Payload, &parsed)
						tickers = append(tickers, parsed)
					}
				}
				if len(tickers) > 0 {
					out["live_tickers"] = tickers
				}
			}
		}
	}

	return nil, out, nil
}

func mapStringAny(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func trimMapKeys(src map[string]any, maxKeys int) map[string]any {
	if len(src) <= maxKeys {
		return src
	}
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make(map[string]any, maxKeys)
	for _, k := range keys[:maxKeys] {
		out[k] = src[k]
	}
	return out
}

func summarizeExchangeInfo(raw json.RawMessage, m binance.Market, filter []string) map[string]any {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	listKey := "symbols"
	if m == binance.MarketOptions {
		listKey = "optionSymbols"
	}
	rawList, _ := doc[listKey].([]any)
	want := make(map[string]bool, len(filter))
	for _, s := range filter {
		want[s] = true
	}
	var active []map[string]any
	for _, item := range rawList {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		sym, _ := obj["symbol"].(string)
		if len(want) > 0 && !want[sym] {
			continue
		}
		active = append(active, map[string]any{
			"symbol":       sym,
			"status":       obj["status"],
			"contractType": obj["contractType"],
			"baseAsset":    obj["baseAsset"],
			"quoteAsset":   obj["quoteAsset"],
			"underlying":   obj["underlying"],
		})
		if len(want) == 0 && len(active) >= 25 {
			break
		}
	}
	return map[string]any{
		"total_listed_count": len(rawList),
		"sample":             active,
	}
}

// ---------- 6. manage_local_cache ----------

type ManageCacheInput struct {
	Action      string   `json:"action" jsonschema:"inspect (default), estimate, prefetch, jobs, cancel, or resume"`
	Verify      bool     `json:"verify,omitempty" jsonschema:"When action='inspect', verify SHA-256 checksums of cached files"`
	Market      string   `json:"market,omitempty" jsonschema:"spot or usdm"`
	Streams     []string `json:"streams,omitempty" jsonschema:"Recorder stream names, e.g. [\"agg_trade\", \"diff_depth\", \"rest_observations\"]"`
	StartUTCNS  string   `json:"start_utc_ns,omitempty" jsonschema:"Window start in UTC nanoseconds"`
	EndUTCNS    string   `json:"end_utc_ns,omitempty" jsonschema:"Window end in UTC nanoseconds"`
	RevisionID  string   `json:"revision_id,omitempty" jsonschema:"Pinned Recorder revision ID"`
	Offline     bool     `json:"offline,omitempty" jsonschema:"Offline zero-network mode"`
	JobID       string   `json:"job_id,omitempty" jsonschema:"Prefetch job_id for jobs/cancel/resume"`
	Description string   `json:"description,omitempty" jsonschema:"Human label for prefetch job"`
}

func (s *Server) handleManageLocalCache(ctx context.Context, _ *mcp.CallToolRequest, in ManageCacheInput) (*mcp.CallToolResult, map[string]any, error) {
	action := strings.ToLower(strings.TrimSpace(in.Action))
	if action == "" {
		action = "inspect"
	}

	switch action {
	case "inspect":
		summary, err := s.CacheMgr.Inspect(ctx, in.Verify)
		if err != nil {
			return nil, nil, err
		}
		jobs, _ := s.Jobs.ListJobs()
		out := map[string]any{
			"action":  "inspect",
			"summary": summary,
			"jobs":    jobs,
		}
		latestPath := filepath.Join(s.Cfg.DataDir, "recorder", "latest.json")
		if raw, rErr := os.ReadFile(latestPath); rErr == nil {
			var latest recorder.LatestIndex
			if json.Unmarshal(raw, &latest) == nil {
				out["cached_latest_recorder_revision"] = latest.RevisionID
				out["cached_latest_generated_at"] = latest.GeneratedAtUTC
			}
		}
		return nil, out, nil

	case "jobs":
		if in.JobID != "" {
			job, err := s.Jobs.LoadJob(in.JobID)
			if err != nil {
				return nil, nil, err
			}
			return nil, map[string]any{"action": "jobs", "job": job}, nil
		}
		jobs, err := s.Jobs.ListJobs()
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{"action": "jobs", "jobs": jobs}, nil

	case "cancel":
		if in.JobID == "" {
			return nil, nil, fmt.Errorf("job_id is required for action=cancel")
		}
		job, err := s.Jobs.Cancel(in.JobID)
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{"action": "cancel", "job": job}, nil

	case "resume":
		if in.JobID == "" {
			return nil, nil, fmt.Errorf("job_id is required for action=resume")
		}
		existing, err := s.Jobs.LoadJob(in.JobID)
		if err != nil {
			return nil, nil, err
		}
		job, err := s.Jobs.StartOrResume(existing.Description, existing.Items, in.JobID)
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{"action": "resume", "job": job}, nil

	case "estimate", "prefetch":
		m := binance.NormalizeMarket(in.Market)
		if m != binance.MarketSpot && m != binance.MarketUSDM {
			return nil, nil, fmt.Errorf("Recorder estimate/prefetch requires market=spot or market=usdm (got %q)", in.Market)
		}
		startNS, endNS, err := parseNSBounds(in.StartUTCNS, in.EndUTCNS)
		if err != nil {
			return nil, nil, err
		}
		snap, err := s.Recorder.SyncSnapshot(ctx, in.RevisionID, in.Offline)
		if err != nil {
			return nil, nil, err
		}
		plan, err := s.Recorder.PlanQuery(snap, string(m), in.Streams, startNS, endNS, false)
		if err != nil {
			return nil, nil, err
		}
		if action == "estimate" {
			return nil, map[string]any{
				"action":   "estimate",
				"estimate": planSummaryMap(plan),
			}, nil
		}
		if in.Offline {
			return nil, nil, fmt.Errorf("cannot start network prefetch when offline=true")
		}
		items := s.Recorder.BuildPrefetchJobItems(plan)
		desc := strings.TrimSpace(in.Description)
		if desc == "" {
			desc = fmt.Sprintf("prefetch %s rev=%s streams=%v", m, plan.RevisionID, in.Streams)
		}
		job, err := s.Jobs.StartOrResume(desc, items, in.JobID)
		if err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{
			"action":   "prefetch",
			"estimate": planSummaryMap(plan),
			"job":      job,
		}, nil

	default:
		return nil, nil, fmt.Errorf("unknown manage_local_cache action %q", in.Action)
	}
}

// ---------- 7. query_public_endpoint ----------

type QueryPublicEndpointInput struct {
	EndpointID   string            `json:"endpoint_id" jsonschema:"Allowlisted capability endpoint_id from query_market_overview(action='capabilities')"`
	Params       map[string]string `json:"params,omitempty" jsonschema:"Query parameters (symbol, interval, period, duration_seconds, max_events, etc.)"`
	Offline      bool              `json:"offline,omitempty" jsonschema:"Offline zero-network mode (serves TTL cache when present)"`
	SaveToFile   bool              `json:"save_to_file,omitempty" jsonschema:"Save response payload to BINANCE_MCP_DATA_DIR/results"`
	OutputFormat string            `json:"output_format,omitempty" jsonschema:"json or jsonl"`
}

func (s *Server) handleQueryPublicEndpoint(ctx context.Context, _ *mcp.CallToolRequest, in QueryPublicEndpointInput) (*mcp.CallToolResult, map[string]any, error) {
	capEntry, ok := binance.LookupCapability(strings.TrimSpace(in.EndpointID))
	if !ok {
		return nil, nil, fmt.Errorf("endpoint_id %q is not in the allowlisted Binance capabilities directory; use query_market_overview(action='capabilities') to discover valid endpoints", in.EndpointID)
	}
	res, err := s.Binance.ExecuteCapability(ctx, capEntry, in.Params, in.Offline)
	if err != nil {
		return nil, nil, err
	}

	budget := s.Cfg.ResponseBudgetBytes
	if budget <= 0 {
		budget = 64 * 1024
	}
	out := map[string]any{
		"endpoint_id":     res.EndpointID,
		"market":          string(capEntry.Market),
		"transport":       capEntry.Transport,
		"url":             res.URL,
		"status_code":     res.StatusCode,
		"from_cache":      res.FromCache,
		"fetched_at_utc":  res.FetchedAt,
		"qty_unit":        capEntry.QtyUnit,
		"time_unit":       capEntry.TimeUnit,
		"payload":         json.RawMessage(res.Payload),
	}
	encoded, _ := json.Marshal(out)
	if in.SaveToFile || len(encoded) > budget-512 {
		ext := "json"
		if strings.ToLower(in.OutputFormat) == "jsonl" {
			ext = "jsonl"
		}
		outPath := makeResultFilePath(s.Cfg, strings.ReplaceAll(capEntry.EndpointID, ".", "_"), ext)
		if wErr := os.MkdirAll(filepath.Dir(outPath), 0o755); wErr == nil {
			_ = os.WriteFile(outPath, res.Payload, 0o644)
			out["result_file"] = outPath
		}
		if len(encoded) > budget-512 {
			// Replace huge array/object payload with concise preview so JSON stays well under 64 KiB.
			var arr []any
			if json.Unmarshal(res.Payload, &arr) == nil {
				n := len(arr)
				if n > 20 {
					n = 20
				}
				for n > 0 {
					out["payload"] = arr[:n]
					out["total_items"] = len(arr)
					out["returned_items"] = n
					out["truncated"] = true
					if b, err := json.Marshal(out); err == nil && len(b) <= budget-512 {
						break
					}
					n = n / 2
				}
			} else {
				delete(out, "payload")
				out["truncated"] = true
				out["payload_bytes"] = len(res.Payload)
				out["note"] = "Payload exceeded 64 KiB budget; full payload saved to result_file."
			}
		}
	}
	return nil, out, nil
}
