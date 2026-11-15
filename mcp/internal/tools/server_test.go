package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
