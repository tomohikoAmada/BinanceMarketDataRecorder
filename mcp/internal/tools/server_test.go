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

func TestM6R1_M6R4_M6R5_OrderbookInspect_Coverage_Pagination_And_VisionRouting(t *testing.T) {
	const baseMS int64 = 1794700800000

	var zipBytes bytes.Buffer
	zw := zip.NewWriter(&zipBytes)
	zf, _ := zw.Create("mark.csv")
	fmt.Fprintln(zf, "1794700800000,100,101,99,100,1,1794700859999,0,1,0,0,0")
	zw.Close()
	zipBody := zipBytes.Bytes()
	zipHash := fmt.Sprintf("%x", sha256.Sum256(zipBody))

	var seenPaths []string
	session, _ := setupTestMCPClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenPaths = append(seenPaths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, ".CHECKSUM"):
			fmt.Fprintf(w, "%s file.zip\n", zipHash)
		case strings.HasSuffix(r.URL.Path, ".zip"):
			w.Header().Set("Content-Type", "application/zip")
			w.Write(zipBody)
		case r.URL.Path == "/fapi/v1/aggTrades":
			n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if n <= 0 {
				n = 100
			}
			from, _ := strconv.Atoi(r.URL.Query().Get("fromId"))
			if from < 1 {
				from = 1
			}
			items := []map[string]any{}
			for id := from; id <= 1500 && len(items) < n; id++ {
				items = append(items, map[string]any{"a": id, "T": baseMS, "p": "100", "q": "1", "m": false})
			}
			json.NewEncoder(w).Encode(items)
		case r.URL.Path == "/dapi/v1/klines":
			n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if n <= 0 {
				n = 100
			}
			lo, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
			hi, _ := strconv.ParseInt(r.URL.Query().Get("endTime"), 10, 64)
			rows := [][]any{}
			for i := 0; i < 1500; i++ {
				ms := baseMS + int64(i)*60000
				if ms >= lo && ms <= hi {
					rows = append(rows, []any{ms, "100", "101", "99", "100", "1", ms + 59999, "0.01", 1, "1", "0.01", "0"})
				}
			}
			if len(rows) > n {
				rows = rows[len(rows)-n:]
			}
			json.NewEncoder(w).Encode(rows)
		case r.URL.Path == "/fapi/v1/fundingRate":
			n, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if n <= 0 {
				n = 100
			}
			rows := []map[string]any{}
			for i := 0; i < 10 && len(rows) < n; i++ {
				rows = append(rows, map[string]any{"symbol": "BTCUSDT", "fundingTime": baseMS + int64(i)*60000, "fundingRate": "0.001"})
			}
			json.NewEncoder(w).Encode(rows)
		case r.URL.Path == "/fapi/v1/openInterest":
			json.NewEncoder(w).Encode(map[string]any{"symbol": "BTCUSDT", "time": baseMS, "openInterest": "999"})
		case r.URL.Path == "/fapi/v1/premiumIndex" || r.URL.Path == "/eapi/v1/mark":
			json.NewEncoder(w).Encode(map[string]any{"symbol": "BTCUSDT", "markPrice": "100"})
		default:
			w.WriteHeader(404)
		}
	}))

	// 1. M6-R5-a: 1500 same-millisecond aggTrades split across two calls must yield 1500 unique IDs and 0 duplicates.
	tArgs := map[string]any{
		"market":         "usdm",
		"symbol":         "BTCUSDT",
		"source":         "rest",
		"record_kind":    "trades",
		"start_utc_ns":   strconv.FormatInt(baseMS*1_000_000, 10),
		"end_utc_ns":     strconv.FormatInt((baseMS+1000)*1_000_000, 10),
		"max_fetch_rows": 1000,
		"limit":          1,
	}
	t1, t1Res := callToolMap(t, session, "query_trades_and_liquidations", tArgs)
	if t1Res.IsError || t1["total_rows"] != float64(1000) || t1["has_more"] != true {
		t.Fatalf("expected 1000 rows + has_more on first page, got isErr=%v out=%v", t1Res.IsError, t1)
	}
	tArgs["cursor"] = t1["next_cursor"]
	t2, t2Res := callToolMap(t, session, "query_trades_and_liquidations", tArgs)
	if t2Res.IsError || t2["total_rows"] != float64(500) || t2["has_more"] != false {
		t.Fatalf("expected remaining 500 rows + has_more=false on second page, got isErr=%v out=%v", t2Res.IsError, t2)
	}
	readAggIDs := func(out map[string]any) []string {
		p, _ := out["result_file"].(string)
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read result_file %q: %v", p, err)
		}
		var ids []string
		for _, ln := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var row map[string]any
			if err := json.Unmarshal([]byte(ln), &row); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, fmt.Sprint(row["agg_trade_id"]))
		}
		return ids
	}
	seenIDs := make(map[string]bool)
	for _, id := range append(readAggIDs(t1), readAggIDs(t2)...) {
		seenIDs[id] = true
	}
	if len(seenIDs) != 1500 {
		t.Fatalf("expected 1500 unique same-ms agg_trade IDs across both calls, got %d", len(seenIDs))
	}

	// 2. M6-R5-b: COIN-M end-anchored klines must return all 1500 bars sorted chronologically ascending.
	cmOut, cmRes := callToolMap(t, session, "query_klines", map[string]any{
		"market":         "coinm",
		"symbol":         "BTCUSD_PERP",
		"source":         "rest",
		"interval":       "1m",
		"start_utc_ns":   strconv.FormatInt(baseMS*1_000_000, 10),
		"end_utc_ns":     strconv.FormatInt((baseMS+1500*60000)*1_000_000, 10),
		"max_fetch_rows": 5000,
		"limit":          1,
	})
	if cmRes.IsError || cmOut["total_rows"] != float64(1500) || cmOut["has_more"] != false {
		t.Fatalf("expected 1500 COIN-M bars with has_more=false, got isErr=%v out=%v", cmRes.IsError, cmOut)
	}
	cmFile, _ := cmOut["result_file"].(string)
	cmLines := strings.Split(strings.TrimSpace(mustReadString(t, cmFile)), "\n")
	if len(cmLines) != 1500 {
		t.Fatalf("expected 1500 COIN-M lines in file, got %d", len(cmLines))
	}
	var firstBar, lastBar map[string]any
	_ = json.Unmarshal([]byte(cmLines[0]), &firstBar)
	_ = json.Unmarshal([]byte(cmLines[1499]), &lastBar)
	if firstBar["open_time_utc_ns"] != strconv.FormatInt(baseMS*1_000_000, 10) || lastBar["open_time_utc_ns"] != strconv.FormatInt((baseMS+1499*60000)*1_000_000, 10) {
		t.Fatalf("unexpected COIN-M chronological range: first=%v last=%v", firstBar["open_time_utc_ns"], lastBar["open_time_utc_ns"])
	}

	// 3. M6-R5-c: Derivatives historical metric separates preview limit from pagination budget.
	fundOut, fundRes := callToolMap(t, session, "query_derivatives_metrics", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"source":       "rest",
		"metric":       "funding_history",
		"start_utc_ns": strconv.FormatInt(baseMS*1_000_000, 10),
		"end_utc_ns":   strconv.FormatInt((baseMS+10*60000)*1_000_000, 10),
		"limit":        2,
	})
	if fundRes.IsError || fundOut["total_rows"] != float64(10) || fundOut["returned_rows"] != float64(2) || fundOut["truncated"] != true {
		t.Fatalf("expected 10 total_rows / 2 preview rows / truncated=true, got isErr=%v out=%v", fundRes.IsError, fundOut)
	}

	// 4. M6-R5-d: Current-only REST metrics must reject historical windows explicitly.
	for _, tc := range []map[string]any{
		{"market": "usdm", "symbol": "BTCUSDT", "source": "rest", "metric": "open_interest", "start_utc_ns": "1577836800000000000", "end_utc_ns": "1577836860000000000"},
		{"market": "usdm", "symbol": "BTCUSDT", "source": "rest", "metric": "mark_price_funding", "start_utc_ns": "1577836800000000000", "end_utc_ns": "1577836860000000000"},
		{"market": "options", "symbol": "BTC-261225-100000-C", "source": "rest", "metric": "options_mark", "start_utc_ns": "1577836800000000000", "end_utc_ns": "1577836860000000000"},
	} {
		if _, errRes := callToolMap(t, session, "query_derivatives_metrics", tc); !errRes.IsError {
			t.Fatalf("expected historical current-only REST metric %v to fail loudly", tc)
		}
	}

	// 5. M6-R5-e: Explicit Vision kline_type="mark" routes to markPriceKlines (not klines), and unsupported type fails explicitly.
	seenPaths = nil
	vMarkOut, vMarkRes := callToolMap(t, session, "query_klines", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"source":       "vision",
		"kline_type":   "mark",
		"interval":     "1m",
		"start_utc_ns": "1794700800000000000",
		"end_utc_ns":   "1794700860000000000",
		"limit":        1,
	})
	if vMarkRes.IsError || vMarkOut["kline_type"] != "mark" || vMarkOut["qty_unit"] != "price_only" {
		t.Fatalf("unexpected explicit Vision mark response: isErr=%v out=%v", vMarkRes.IsError, vMarkOut)
	}
	if len(seenPaths) == 0 || !strings.Contains(seenPaths[0], "/daily/markPriceKlines/") || strings.Contains(seenPaths[0], "/daily/klines/") {
		t.Fatalf("expected Vision mark request to hit /daily/markPriceKlines/, got %v", seenPaths)
	}
	if _, vContRes := callToolMap(t, session, "query_klines", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"source":       "vision",
		"kline_type":   "continuous",
		"interval":     "1m",
		"start_utc_ns": "1794700800000000000",
		"end_utc_ns":   "1794700860000000000",
	}); !vContRes.IsError {
		t.Fatal("expected unsupported Vision kline_type=continuous to return explicit error")
	}

	// 6. M6-R4: Mixed-symbol partial orderbook and legacy unknown metadata must NEVER report complete.
	mixedPlan := &recorder.QueryPlan{
		ReadyChunksMatched: 3,
		RecordedSymbols:    []string{"BTCUSDT", "ETHUSDT"},
		RequestedSymbols:   []string{"BTCUSDT", "ETHUSDT"},
		HasSymbolMetadata:  true,
	}
	mixedCov := buildRecorderCoverage(mixedPlan, mixedPlan.RequestedSymbols, map[string]any{
		"reliable_emissions":  2,
		"window_resync_count": 0,
		"snapshots_seen":      1,
		"symbol_coverage": map[string]any{
			"BTCUSDT": map[string]any{"status": "complete"},
			"ETHUSDT": map[string]any{"status": "unavailable", "reason": "no_depth_snapshots_for_symbol"},
		},
	}, true, "")
	if mixedCov["status"] != "partial" {
		t.Fatalf("expected partial coverage when ETHUSDT lacks snapshot, got %v", mixedCov)
	}
	unknownCov := buildRecorderCoverage(&recorder.QueryPlan{
		ReadyChunksMatched: 1,
		RequestedSymbols:   []string{"BTCUSDT"},
	}, []string{"BTCUSDT"}, nil, false, "")
	if unknownCov["status"] != "unknown" {
		t.Fatalf("expected unknown status when legacy metadata lacks recorded symbols, got %v", unknownCov)
	}

	// 7. M6-R1 end-to-end Stage 1 inspect_orderbook_snapshots map parsing + >6h partition walkback + error propagation.
	recSession, recCfg := setupTestMCPClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	mockWorkerPath := filepath.Join(t.TempDir(), "mock_worker.py")
	mockWorkerPy := `import json, sys
req = json.load(sys.stdin)
op = req.get("operation")
cids = [c["chunk_id"] for c in req.get("chunks", [])]
if op == "inspect_orderbook_snapshots":
    if "FAIL_INSPECT" in req.get("symbols", []):
        print(json.dumps({"error": "injected inspect failure"}))
        sys.exit(1)
    before = {}
    if "snap-old-10h" in cids:
        before["BTCUSDT"] = "1000000000000"
    print(json.dumps({"operation": op, "snapshots_found": len(before), "latest_before_start_ns": before}))
elif op == "query_orderbook":
    print(json.dumps({
        "operation": op,
        "stats": {
            "snapshots_seen": 1, "depth_updates_seen": len(cids), "resync_count": 0,
            "warmup_resync_count": 0, "window_resync_count": 0, "recovery_count": 1,
            "reliable_emissions": 1,
            "symbol_coverage": {"BTCUSDT": {"status": "complete", "reason": None}},
        },
        "total_rows": 1,
        "rows": [{"symbol": "BTCUSDT", "chunks_seen": cids}],
    }))
`
	if err := os.WriteFile(mockWorkerPath, []byte(mockWorkerPy), 0o644); err != nil {
		t.Fatal(err)
	}
	recCfg.PythonBin = "/Users/amada/Documents/Development/Crypto/BinanceMarketDataRecorder/.venv/bin/python"
	recCfg.WorkerScript = mockWorkerPath

	revID := "rev-m6r1-e2e"
	recBase := filepath.Join(recCfg.DataDir, "recorder")
	stageChunk := func(id, sym, stream string, lo, hi int64, hasSnap bool) map[string]any {
		rawAbs := filepath.Join(recBase, "storage", "s1", "raw", id+".bmdr.zst")
		manAbs := filepath.Join(recBase, "storage", "s1", "manifests", id+".json")
		_ = os.MkdirAll(filepath.Dir(rawAbs), 0o755)
		_ = os.MkdirAll(filepath.Dir(manAbs), 0o755)
		_ = os.WriteFile(rawAbs, []byte(id), 0o644)
		_ = os.WriteFile(manAbs, []byte(`{"raw_manifest":{"symbol":"`+sym+`"}}`), 0o644)
		sum := sha256.Sum256([]byte(id))
		shaHex := fmt.Sprintf("%x", sum)
		_ = cache.WriteReceipt(rawAbs, cache.Receipt{SHA256: shaHex, SizeBytes: int64(len(id)), OfflineOK: true})
		_ = cache.WriteReceipt(manAbs, cache.Receipt{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(`{"raw_manifest":{"symbol":"`+sym+`"}}`))), SizeBytes: int64(len(`{"raw_manifest":{"symbol":"`+sym+`"}}`)), OfflineOK: true})
		obj := map[string]any{
			"chunk_id": id, "market": "um_perpetual", "symbol": sym, "stream": stream,
			"state": "ARCHIVED_VERIFIED", "ready": true, "storage_id": "s1",
			"target_relative_path": "raw/" + id + ".bmdr.zst",
			"external_manifest_relative_path": "manifests/" + id + ".json",
			"archive_url_path": "raw/" + id, "manifest_url_path": "man/" + id,
			"stored_bytes": len(id), "stored_sha256": shaHex, "uncompressed_bytes": len(id),
			"created_at_utc_ns": lo, "updated_at_utc_ns": hi,
			"receive_time_utc_range_ns": map[string]any{"min": lo, "max": hi},
		}
		if hasSnap {
			obj["sequence_ranges"] = map[string]any{"lastUpdateId": map[string]any{"min": 100, "max": 100}}
		}
		return obj
	}
	// p0 is >10h before startNS (37_000_000_000_000 ns = 37000s vs start = 37000s, p0 at 1000s = 1_000_000_000_000 ns -> 36000s = 10h gap!).
	const start10h int64 = 37_000_000_000_000
	const end10h int64 = 37_100_000_000_000
	writePartSnapshot := func(pid, state string, lo, hi int64, chunks []map[string]any) map[string]any {
		writeArtifact := func(fname string, content []byte) map[string]any {
			sum := fmt.Sprintf("%x", sha256.Sum256(content))
			short := sum[:16]
			dst := filepath.Join(recBase, "snapshots", "usdm", pid, short, fname)
			_ = os.MkdirAll(filepath.Dir(dst), 0o755)
			_ = os.WriteFile(dst, content, 0o644)
			_ = cache.WriteReceipt(dst, cache.Receipt{SHA256: sum, SizeBytes: int64(len(content)), OfflineOK: true})
			return map[string]any{"bytes": int64(len(content)), "sha256": sum, "url_path": "snapshots/usdm/" + pid + "/" + short + "/" + fname}
		}
		pDoc, _ := json.Marshal(map[string]any{"schema_version": "1", "market": "usdm", "partition_id": pid, "state": state, "receive_time_utc_range_ns": map[string]any{"min": lo, "max": hi}})
		aDoc, _ := json.Marshal(map[string]any{"market": "usdm", "partition_id": pid, "recorded_symbols": []string{"BTCUSDT", "FAIL_INSPECT"}, "gaps": []any{}, "discontinuity_events": []any{}, "quarantined_artifacts": []any{}})
		var lines []string
		for _, ch := range chunks {
			b, _ := json.Marshal(ch)
			lines = append(lines, string(b))
		}
		cDoc := []byte(strings.Join(lines, "\n") + "\n")
		return map[string]any{
			"market": "usdm", "partition_id": pid,
			"files": map[string]any{
				"partition_json":    writeArtifact("partition.json", pDoc),
				"availability_json": writeArtifact("availability.json", aDoc),
				"chunks_jsonl":      writeArtifact("chunks.jsonl", cDoc),
				"catalog_sqlite":    writeArtifact("catalog.sqlite", []byte("sqlite")),
			},
		}
	}
	p0Ref := writePartSnapshot("p0", "closed", 900_000_000_000, 2_000_000_000_000, []map[string]any{
		stageChunk("snap-old-10h", "BTCUSDT", "rest_observations", 950_000_000_000, 1_100_000_000_000, true),
		stageChunk("bridge-old-10h", "BTCUSDT", "diff_depth", 950_000_000_000, 2_000_000_000_000, false),
	})
	p1Ref := writePartSnapshot("p1", "closed", 10_000_000_000_000, 36_000_000_000_000, []map[string]any{
		stageChunk("bridge-mid-only", "BTCUSDT", "diff_depth", 10_000_000_000_000, 36_000_000_000_000, false),
	})
	p2Ref := writePartSnapshot("p2", "open", 36_500_000_000_000, 38_000_000_000_000, []map[string]any{
		stageChunk("win-depth", "BTCUSDT", "diff_depth", 36_500_000_000_000, 37_200_000_000_000, false),
	})
	manPath := filepath.Join(recBase, "revisions", revID, "manifest.json")
	_ = os.MkdirAll(filepath.Dir(manPath), 0o755)
	manBytes, _ := json.Marshal(map[string]any{
		"schema_version": "mcp-export.v1",
		"revision_id":    revID,
		"partitions":     []any{p0Ref, p1Ref, p2Ref},
	})
	_ = os.WriteFile(manPath, manBytes, 0o644)
	_ = cache.WriteReceipt(manPath, cache.Receipt{SHA256: fmt.Sprintf("%x", sha256.Sum256(manBytes)), SizeBytes: int64(len(manBytes)), OfflineOK: true})

	ob10hOut, ob10hRes := callToolMap(t, recSession, "query_orderbook", map[string]any{
		"market":       "usdm",
		"symbol":       "BTCUSDT",
		"revision_id":  revID,
		"offline":      true,
		"start_utc_ns": strconv.FormatInt(start10h, 10),
		"end_utc_ns":   strconv.FormatInt(end10h, 10),
	})
	if ob10hRes.IsError {
		t.Fatalf("expected 10h multi-partition offline query_orderbook to succeed, got %v", ob10hRes)
	}
	rowsAny, _ := ob10hOut["rows"].([]any)
	chunksSeenStr := fmt.Sprint(rowsAny[0])
	for _, need := range []string{"snap-old-10h", "bridge-old-10h", "bridge-mid-only", "win-depth"} {
		if !strings.Contains(chunksSeenStr, need) {
			t.Fatalf("expected Stage 2 chunks to contain %q across >6h two-partition bridge, got %s", need, chunksSeenStr)
		}
	}
	if _, inspErrRes := callToolMap(t, recSession, "query_orderbook", map[string]any{
		"market":       "usdm",
		"symbol":       "FAIL_INSPECT",
		"revision_id":  revID,
		"offline":      true,
		"start_utc_ns": strconv.FormatInt(start10h, 10),
		"end_utc_ns":   strconv.FormatInt(end10h, 10),
	}); !inspErrRes.IsError {
		t.Fatal("expected inspect_orderbook_snapshots failure to propagate loudly instead of silently being ignored")
	}
}

func mustReadString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
