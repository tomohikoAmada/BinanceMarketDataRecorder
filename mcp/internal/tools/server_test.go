package tools

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/binance"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/cache"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/recorder"
)

type testRewriteRT struct {
	base string
	rt   http.RoundTripper
}

func (t testRewriteRT) RoundTrip(req *http.Request) (*http.Response, error) {
	c := req.Clone(req.Context())
	c.URL.Scheme = "http"
	c.URL.Host = strings.TrimPrefix(t.base, "http://")
	return t.rt.RoundTrip(c)
}

func setupTestMCPClient(t *testing.T, handler http.Handler) (*mcp.ClientSession, *config.Config) {
	t.Helper()
	httpSrv := httptest.NewServer(handler)
	t.Cleanup(httpSrv.Close)

	dataDir := t.TempDir()
	cfg := &config.Config{
		DataDir:             dataDir,
		RecorderHTTPSURL:    httpSrv.URL,
		RecorderConcurrency: 2,
		BinanceConcurrency:  4,
		PreviewDefaultRows:  100,
		PreviewMaxRows:      1000,
		ResponseBudgetBytes: 8 * 1024, // Tighten in unit test to trigger spill cleanly
		HTTPTimeout:         5 * time.Second,
	}
	if err := cfg.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: testRewriteRT{base: httpSrv.URL, rt: httpSrv.Client().Transport},
	}
	mgr := cache.NewManager(dataDir, httpClient, 2, 0, nil)
	jobs := cache.NewJobCoordinator(mgr)
	bClient := binance.NewClient(cfg, mgr)
	bClient.HTTP = httpClient
	rClient := recorder.NewClient(cfg, mgr)

	srv := NewServer(cfg, mgr, jobs, bClient, rClient)

	t1, t2 := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		_ = srv.MCP.Run(ctx, t1)
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1.0"}, nil)
	session, err := client.Connect(ctx, t2, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, cfg
}

func callToolMap(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) (map[string]any, *mcp.CallToolResult) {
	t.Helper()
	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      name,
		Arguments: args,
	})
	if err != nil {
		t.Fatalf("CallTool(%s) transport error: %v", name, err)
	}
	var out map[string]any
	if res.StructuredContent != nil {
		raw, _ := json.Marshal(res.StructuredContent)
		_ = json.Unmarshal(raw, &out)
	} else if len(res.Content) > 0 {
		if tc, ok := res.Content[0].(*mcp.TextContent); ok {
			_ = json.Unmarshal([]byte(tc.Text), &out)
		}
	}
	return out, res
}

func TestMCPTools_AllSevenAndGuardrails(t *testing.T) {
	session, _ := setupTestMCPClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v3/klines":
			var rows [][]any
			for i := 0; i < 200; i++ {
				ts := 1794700800000 + int64(i)*60000
				rows = append(rows, []any{ts, "90000.1", "90010.0", "89990.0", "90005.0", "1.25", ts + 59999, "112500.0", 42, "0.5", "45000.0", "0"})
			}
			_ = json.NewEncoder(w).Encode(rows)
		case "/dapi/v1/exchangeInfo":
			_, _ = w.Write([]byte(`{"symbols":[{"symbol":"BTCUSD_PERP","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USD"}]}`))
		case "/futures/data/takerBuySellVol":
			_, _ = w.Write([]byte(`[{"pair":"BTCUSD","contractType":"PERPETUAL","takerBuyVol":"120","takerSellVol":"80","timestamp":1794700800000}]`))
		case "/eapi/v1/openInterest":
			_, _ = w.Write([]byte(`[{"symbol":"BTC-261225-100000-C","sumOpenInterest":"15.2","sumOpenInterestUsd":"1520000","timestamp":"1794700800000"}]`))
		case "/api/v3/time":
			_, _ = w.Write([]byte(`{"serverTime":1794700800000}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	// 1. ListTools check
	listed, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(listed.Tools) != 7 {
		t.Fatalf("expected 7 registered MCP tools, got %d", len(listed.Tools))
	}

	// 2. query_market_overview capabilities mode
	capsOut, res := callToolMap(t, session, "query_market_overview", map[string]any{
		"action": "capabilities",
		"market": "coinm",
		"search": "taker",
	})
	if res.IsError || capsOut["count"] != float64(1) {
		t.Fatalf("capabilities check failed: isError=%v out=%v", res.IsError, capsOut)
	}

	// 3. query_klines spills cleanly when exceeding budget
	klOut, res := callToolMap(t, session, "query_klines", map[string]any{
		"market": "spot",
		"symbol": "BTCUSDT",
		"source": "rest",
		"limit":  200,
	})
	if res.IsError {
		t.Fatalf("query_klines error: %+v", res)
	}
	if klOut["truncated"] != true || klOut["total_rows"] != float64(200) {
		t.Fatalf("expected truncated=true total_rows=200, got %v", klOut)
	}
	rf, _ := klOut["result_file"].(string)
	if rf == "" {
		t.Fatal("expected spilled result_file")
	}
	if st, err := os.Stat(rf); err != nil || st.Size() == 0 {
		t.Fatalf("spilled result_file missing/empty: %v", err)
	}

	// 4. Historical orderbook MUST refuse REST backfill when Recorder unavailable
	_, obRes := callToolMap(t, session, "query_orderbook", map[string]any{
		"market":       "spot",
		"symbol":       "BTCUSDT",
		"start_utc_ns": "1794700800000000000",
	})
	if !obRes.IsError {
		t.Fatal("expected error refusing REST backfill for historical orderbook")
	}

	// 5. Spot liquidation returns explicit not_applicable_for_spot
	tlOut, tlRes := callToolMap(t, session, "query_trades_and_liquidations", map[string]any{
		"market":      "spot",
		"symbol":      "BTCUSDT",
		"record_kind": "liquidations",
	})
	if tlRes.IsError || tlOut["liquidation_status"] != "not_applicable_for_spot" {
		t.Fatalf("unexpected spot liquidation response: isError=%v out=%v", tlRes.IsError, tlOut)
	}

	// 6. COIN-M taker_buy_sell_vol + Options open interest
	cmOut, cmRes := callToolMap(t, session, "query_derivatives_metrics", map[string]any{
		"market": "coinm",
		"symbol": "BTCUSD_PERP",
		"metric": "taker_buy_sell_vol",
	})
	if cmRes.IsError || cmOut["endpoint_id"] != "coinm.takerBuySellVol" {
		t.Fatalf("coinm takerBuySellVol failed: isError=%v out=%v", cmRes.IsError, cmOut)
	}

	optOut, optRes := callToolMap(t, session, "query_derivatives_metrics", map[string]any{
		"market":           "options",
		"metric":           "options_open_interest",
		"underlying_asset": "BTC",
		"expiration":       "261225",
	})
	if optRes.IsError || optOut["total_rows"] != float64(1) {
		t.Fatalf("options open interest failed: isError=%v out=%v", optRes.IsError, optOut)
	}

	// 7. query_public_endpoint allowlist rejection vs valid call + manage_local_cache inspect
	_, badPub := callToolMap(t, session, "query_public_endpoint", map[string]any{
		"endpoint_id": "sapi.v1.evil",
	})
	if !badPub.IsError {
		t.Fatal("expected rejection for unapproved endpoint_id")
	}

	goodPub, goodRes := callToolMap(t, session, "query_public_endpoint", map[string]any{
		"endpoint_id": "spot.time",
	})
	if goodRes.IsError || fmt.Sprint(goodPub["status_code"]) != "200" {
		t.Fatalf("spot.time public endpoint failed: isError=%v out=%v", goodRes.IsError, goodPub)
	}

	cacheOut, cacheRes := callToolMap(t, session, "manage_local_cache", map[string]any{
		"action": "inspect",
		"verify": true,
	})
	if cacheRes.IsError || cacheOut["action"] != "inspect" {
		t.Fatalf("manage_local_cache inspect failed: isError=%v out=%v", cacheRes.IsError, cacheOut)
	}
}

func makeVisionZipAndSHA(t *testing.T, csvName, csvBody string) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	f, err := zw.Create(csvName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(csvBody)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

func TestM6E_M6F_M6H_Pagination_Guardrails_WriteErrors_And_VisionPrefetch(t *testing.T) {
	baseMS := int64(1794700800000) // 2026-11-15T00:00:00Z
	session, cfg := setupTestMCPClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case p == "/fapi/v1/klines":
			st, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
			lim, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if lim <= 0 {
				lim = 1000
			}
			end := st + int64(lim)*60000
			totalEnd := baseMS + 1500*60000
			if end > totalEnd {
				end = totalEnd
			}
			var rows [][]any
			for ts := st; ts < end; ts += 60000 {
				rows = append(rows, []any{ts, "90000", "90010", "89990", "90005", "2.0", ts + 59999, "180000", 10, "1.0", "90000", "0"})
			}
			_ = json.NewEncoder(w).Encode(rows)

		case p == "/dapi/v1/klines":
			rows := [][]any{{baseMS, "90000", "90010", "89990", "90005", "50", baseMS + 59999, "0.0555", 12, "30", "0.0333", "0"}}
			_ = json.NewEncoder(w).Encode(rows)

		case strings.HasPrefix(p, "/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-"):
			isChk := strings.HasSuffix(p, ".CHECKSUM")
			trim := strings.TrimSuffix(strings.TrimPrefix(p, "/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-"), ".CHECKSUM")
			dateStr := strings.TrimSuffix(trim, ".zip")
			dayStart, err := time.Parse("2006-01-02", dateStr)
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			openMS := dayStart.UTC().UnixMilli()
			csvLine := fmt.Sprintf("%d,90000,90010,89990,90005,1.5,%d,135000,5,0.7,63000,0\n", openMS, openMS+59999)
			zipBytes, shaHex := makeVisionZipAndSHA(t, fmt.Sprintf("BTCUSDT-1m-%s.csv", dateStr), csvLine)
			if isChk {
				_, _ = fmt.Fprintf(w, "%s  BTCUSDT-1m-%s.zip\n", shaHex, dateStr)
			} else {
				_, _ = w.Write(zipBytes)
			}

		case p == "/api/v3/time":
			_, _ = w.Write([]byte(`{"serverTime":1794700800000}`))

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	// 1. M6-E: Multi-page REST pagination + cursor continuation independent of preview limit
	p1Out, p1Res := callToolMap(t, session, "query_klines", map[string]any{
		"market":         "usdm",
		"symbol":         "BTCUSDT",
		"source":         "rest",
		"start_utc_ns":   strconv.FormatInt(baseMS*1_000_000, 10),
		"end_utc_ns":     strconv.FormatInt((baseMS+1500*60000)*1_000_000, 10),
		"limit":          5,
		"max_fetch_rows": 1200,
	})
	if p1Res.IsError {
		t.Fatalf("paginated query_klines page 1 failed: %+v", p1Res)
	}
	if p1Out["total_rows"] != float64(1200) || p1Out["has_more"] != true || p1Out["returned_rows"] != float64(5) {
		t.Fatalf("unexpected page 1 pagination metadata: %v", p1Out)
	}
	nextCur, _ := p1Out["next_cursor"].(string)
	if nextCur == "" {
		t.Fatal("expected non-empty next_cursor on page 1")
	}

	p2Out, p2Res := callToolMap(t, session, "query_klines", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"source":       "rest",
		"cursor":       nextCur,
		"end_utc_ns":   strconv.FormatInt((baseMS+1500*60000)*1_000_000, 10),
		"limit":        500,
	})
	if p2Res.IsError || p2Out["total_rows"] != float64(300) || p2Out["has_more"] != false {
		t.Fatalf("unexpected page 2 pagination result: isErr=%v out=%v", p2Res.IsError, p2Out)
	}

	// 2. M6-E: COIN-M klines map col 7 -> base_volume & qty_unit=contracts
	cmOut, cmRes := callToolMap(t, session, "query_klines", map[string]any{
		"market": "coinm",
		"symbol": "BTCUSD_PERP",
		"source": "rest",
	})
	if cmRes.IsError {
		t.Fatalf("coinm query_klines failed: %+v", cmRes)
	}
	cmRows, _ := cmOut["rows"].([]any)
	if len(cmRows) != 1 {
		t.Fatalf("expected 1 coinm row, got %d", len(cmRows))
	}
	r0 := cmRows[0].(map[string]any)
	if r0["base_volume"] != "0.0555" || r0["qty_unit"] != "contracts" || r0["quote_volume"] != nil {
		t.Fatalf("unexpected coinm kline fields: %v", r0)
	}

	// 3. M6-E: Guardrails rejecting non-standard Recorder klines, options.trades historical, invalid period/metric
	if _, r := callToolMap(t, session, "query_klines", map[string]any{"market": "usdm", "symbol": "BTCUSDT", "source": "recorder", "kline_type": "mark"}); !r.IsError {
		t.Fatal("expected error rejecting non-standard kline_type on Recorder")
	}
	if _, r := callToolMap(t, session, "query_trades_and_liquidations", map[string]any{"market": "options", "symbol": "BTC-261225-100000-C", "start_utc_ns": "1794700800000000000"}); !r.IsError {
		t.Fatal("expected error rejecting historical window on options.trades")
	}
	if _, r := callToolMap(t, session, "query_derivatives_metrics", map[string]any{"market": "usdm", "symbol": "BTCUSDT", "metric": "basis", "source": "recorder"}); !r.IsError {
		t.Fatal("expected error rejecting unsupported Recorder metric 'basis'")
	}
	if _, r := callToolMap(t, session, "query_derivatives_metrics", map[string]any{"market": "usdm", "symbol": "BTCUSDT", "metric": "open_interest_hist", "period": "7m"}); !r.IsError {
		t.Fatal("expected error rejecting invalid statistics period '7m'")
	}

	// 4. M6-E: >31-day Vision window returns explicit continuation cursor instead of silent truncation
	start35 := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	end35 := start35.Add(35 * 24 * time.Hour)
	v1Out, v1Res := callToolMap(t, session, "query_klines", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"interval":     "1m",
		"source":       "vision",
		"start_utc_ns": strconv.FormatInt(start35.UnixNano(), 10),
		"end_utc_ns":   strconv.FormatInt(end35.UnixNano(), 10),
	})
	if v1Res.IsError || v1Out["total_rows"] != float64(31) || v1Out["has_more"] != true {
		t.Fatalf("expected 31 rows with has_more=true, got isErr=%v out=%v", v1Res.IsError, v1Out)
	}
	v2Out, v2Res := callToolMap(t, session, "query_klines", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"interval":     "1m",
		"source":       "vision",
		"cursor":       v1Out["next_cursor"],
		"end_utc_ns":   strconv.FormatInt(end35.UnixNano(), 10),
	})
	if v2Res.IsError || v2Out["total_rows"] != float64(4) || v2Out["has_more"] != false {
		t.Fatalf("expected remaining 4 Vision days, got isErr=%v out=%v", v2Res.IsError, v2Out)
	}

	// 5. M6-H: manage_local_cache Binance Vision estimate -> prefetch -> offline cache hit
	estOut, estRes := callToolMap(t, session, "manage_local_cache", map[string]any{
		"action":   "estimate",
		"source":   "binance_vision",
		"market":   "usdm",
		"symbol":   "BTCUSDT",
		"dataset":  "klines",
		"cadence":  "daily",
		"interval": "1m",
		"period":   "2026-09-15",
	})
	if estRes.IsError || estOut["download_needed"] != float64(1) {
		t.Fatalf("unexpected Vision estimate before prefetch: isErr=%v out=%v", estRes.IsError, estOut)
	}
	preOut, preRes := callToolMap(t, session, "manage_local_cache", map[string]any{
		"action":   "prefetch",
		"source":   "binance_vision",
		"market":   "usdm",
		"symbol":   "BTCUSDT",
		"dataset":  "klines",
		"cadence":  "daily",
		"interval": "1m",
		"period":   "2026-09-15",
	})
	if preRes.IsError {
		t.Fatalf("Vision prefetch failed: %+v", preRes)
	}
	jobID := preOut["job"].(map[string]any)["job_id"].(string)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		jOut, jRes := callToolMap(t, session, "manage_local_cache", map[string]any{"action": "jobs", "job_id": jobID})
		if !jRes.IsError && jOut["job"].(map[string]any)["state"] == "completed" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	est2Out, est2Res := callToolMap(t, session, "manage_local_cache", map[string]any{
		"action":   "estimate",
		"source":   "binance_vision",
		"market":   "usdm",
		"symbol":   "BTCUSDT",
		"dataset":  "klines",
		"cadence":  "daily",
		"interval": "1m",
		"period":   "2026-09-15",
	})
	if est2Res.IsError || est2Out["already_cached"] != float64(1) || est2Out["download_needed"] != float64(0) {
		t.Fatalf("expected Vision archive cached after prefetch, got %v", est2Out)
	}

	// 6. M6-D: buildRecorderCoverage marks unrecorded symbols and unreliable orderbook cleanly
	cov := buildRecorderCoverage(&recorder.QueryPlan{
		ReadyChunksMatched: 2,
		RequestedSymbols:   []string{"BTCUSDT", "DOGEUSDT"},
		RecordedSymbols:    []string{"BTCUSDT"},
		UnrecordedSymbols:  []string{"DOGEUSDT"},
	}, []string{"BTCUSDT", "DOGEUSDT"}, map[string]any{"reliable_emissions": 0}, true, "no_snapshot_before_window")
	if cov["status"] != "unavailable" || cov["missing_symbols_count"] != 1 {
		t.Fatalf("expected unavailable status with missing symbol disclosure, got %v", cov)
	}

	// 7. M6-F: Write failure propagation in save_to_file paths
	resultsPath := filepath.Join(cfg.DataDir, "results")
	_ = os.RemoveAll(resultsPath)
	if err := os.WriteFile(resultsPath, []byte("block-mkdir"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, wRes := callToolMap(t, session, "query_klines", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"source":       "rest",
		"save_to_file": true,
	}); !wRes.IsError {
		t.Fatal("expected error when results directory cannot be created for save_to_file")
	}
	if _, wRes := callToolMap(t, session, "query_public_endpoint", map[string]any{
		"endpoint_id":  "spot.time",
		"save_to_file": true,
	}); !wRes.IsError {
		t.Fatal("expected error when query_public_endpoint save_to_file fails")
	}
}
