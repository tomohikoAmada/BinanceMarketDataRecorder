package binance

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/cache"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
)

type rewriteTransport struct {
	targetBase string
	rt         http.RoundTripper
}

func (r rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = "http"
	clone.URL.Host = strings.TrimPrefix(r.targetBase, "http://")
	return r.rt.RoundTrip(clone)
}

func newTestClient(t *testing.T, handler http.Handler, apiKey string) (*Client, *config.Config) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	dataDir := t.TempDir()
	cfg := &config.Config{
		DataDir:             dataDir,
		BinanceAPIKey:       apiKey,
		RecorderConcurrency: 2,
		BinanceConcurrency:  4,
		HTTPTimeout:         5 * time.Second,
		PreviewDefaultRows:  100,
		PreviewMaxRows:      1000,
		ResponseBudgetBytes: 64 * 1024,
	}
	if err := cfg.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories: %v", err)
	}
	httpClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: rewriteTransport{targetBase: srv.URL, rt: srv.Client().Transport},
	}
	mgr := cache.NewManager(dataDir, httpClient, 2, 0, nil)
	cl := NewClient(cfg, mgr)
	cl.HTTP = httpClient
	return cl, cfg
}

func TestCapabilitiesRegistry(t *testing.T) {
	all := Capabilities("", "")
	if len(all) < 50 {
		t.Fatalf("expected full 4-market capabilities registry, got %d", len(all))
	}
	eoh, ok := LookupCapability("options.vision.EOHSummary")
	if !ok || eoh.Status != StatusUpstreamUnavailable {
		t.Fatalf("expected options.vision.EOHSummary to be upstream-unavailable, got ok=%v status=%q", ok, eoh.Status)
	}
	usdmHist, ok := LookupCapability("usdm.historicalTrades")
	if !ok || usdmHist.Auth != "api_key_required" {
		t.Fatalf("expected usdm.historicalTrades api_key_required, got ok=%v auth=%q", ok, usdmHist.Auth)
	}
	coinmCaps := Capabilities("coinm", "taker")
	if len(coinmCaps) != 1 || coinmCaps[0].EndpointID != "coinm.takerBuySellVol" {
		t.Fatalf("expected coinm.takerBuySellVol match, got %+v", coinmCaps)
	}
}

func TestExecuteCapability_ValidationRetryAfterAndTTL(t *testing.T) {
	var hits atomic.Int32
	cl, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		switch r.URL.Path {
		case "/api/v3/exchangeInfo":
			if n == 1 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"code":-1003,"msg":"rate limited"}`))
				return
			}
			_, _ = w.Write([]byte(`{"timezone":"UTC","symbols":[{"symbol":"BTCUSDT"}]}`))
		case "/api/v3/depth":
			_, _ = w.Write([]byte(`{"lastUpdateId":100,"bids":[["50000.0","1.0"]],"asks":[]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}), "")

	// API-key gated endpoint must fail before hitting network if key absent.
	usdmHist, _ := LookupCapability("usdm.historicalTrades")
	if _, err := cl.ExecuteCapability(context.Background(), usdmHist, map[string]string{"symbol": "BTCUSDT"}, false); err == nil {
		t.Fatal("expected missing BINANCE_API_KEY error for usdm.historicalTrades")
	}

	// Non-allowlisted param rejected.
	exInfo, _ := LookupCapability("spot.exchangeInfo")
	if _, err := cl.ExecuteCapability(context.Background(), exInfo, map[string]string{"badParam": "1"}, false); err == nil {
		t.Fatal("expected unapproved param error")
	}

	// 429 retry recovers cleanly and caches exchangeInfo (TTL > 0).
	res1, err := cl.ExecuteCapability(context.Background(), exInfo, map[string]string{"symbol": "BTCUSDT"}, false)
	if err != nil {
		t.Fatalf("exchangeInfo after 429 retry failed: %v", err)
	}
	if res1.FromCache {
		t.Fatal("expected first call FromCache=false")
	}
	hitsAfterFirst := hits.Load()
	if hitsAfterFirst != 2 {
		t.Fatalf("expected 2 HTTP calls (429 + 200), got %d", hitsAfterFirst)
	}

	res2, err := cl.ExecuteCapability(context.Background(), exInfo, map[string]string{"symbol": "BTCUSDT"}, false)
	if err != nil {
		t.Fatalf("cached exchangeInfo failed: %v", err)
	}
	if !res2.FromCache || hits.Load() != hitsAfterFirst {
		t.Fatalf("expected TTL cache hit without network, FromCache=%v hits=%d", res2.FromCache, hits.Load())
	}

	// Live depth has TTLSeconds == 0 and must NOT be served from cache online.
	depthCap, _ := LookupCapability("spot.depth")
	d1, err := cl.ExecuteCapability(context.Background(), depthCap, map[string]string{"symbol": "BTCUSDT"}, false)
	if err != nil || d1.FromCache {
		t.Fatalf("depth call 1 failed: err=%v cached=%v", err, d1 != nil && d1.FromCache)
	}
	d2, err := cl.ExecuteCapability(context.Background(), depthCap, map[string]string{"symbol": "BTCUSDT"}, false)
	if err != nil || d2.FromCache {
		t.Fatalf("depth call 2 must hit live endpoint again: err=%v cached=%v", err, d2 != nil && d2.FromCache)
	}
}

func TestVisionDailyKlines_SpotMicrosecondsAndOffline(t *testing.T) {
	// Build a Vision zip containing 1 pre-2025 ms row and 1 >=2025 us row (16 digits).
	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	fw, err := zw.Create("BTCUSDT-1m-2026-11-10.csv")
	if err != nil {
		t.Fatalf("zip create: %v", err)
	}
	csvBody := strings.Join([]string{
		"open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore",
		"1735689600000,90000.1,90010.0,89990.0,90005.5,1.25,1735689659999,112500.5,42,0.5,45000.0,0",
		"1794268800000000,100000.0,100050.0,99950.0,100025.25,2.50,1794268859999999,250050.0,99,1.0,100000.0,0",
	}, "\n")
	if _, err := fw.Write([]byte(csvBody)); err != nil {
		t.Fatalf("zip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	zipBytes := zipBuf.Bytes()
	sum := sha256.Sum256(zipBytes)
	sumHex := hex.EncodeToString(sum[:])

	var netHits atomic.Int32
	cl, _ := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		netHits.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, ".zip.CHECKSUM"):
			_, _ = w.Write([]byte(sumHex + "  BTCUSDT-1m-2026-11-10.zip\n"))
		case strings.HasSuffix(r.URL.Path, ".zip"):
			_, _ = w.Write(zipBytes)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}), "")

	res, err := cl.FetchVisionDailyKlines(context.Background(), MarketSpot, "BTCUSDT", "1m", "2026-11-10", false)
	if err != nil {
		t.Fatalf("FetchVisionDailyKlines failed: %v", err)
	}
	if res.ChecksumHex != sumHex {
		t.Fatalf("checksum mismatch: got %s want %s", res.ChecksumHex, sumHex)
	}

	rows, err := ReadVisionKlinesZip(res.ZipPath, MarketSpot, res.ChecksumHex, 0, 0)
	if err != nil {
		t.Fatalf("ReadVisionKlinesZip: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].OpenTimeUTCNS != "1735689600000000000" {
		t.Fatalf("ms timestamp conversion mismatch: %s", rows[0].OpenTimeUTCNS)
	}
	if rows[1].OpenTimeUTCNS != "1794268800000000000" || rows[1].CloseTimeUTCNS != "1794268859999999000" {
		t.Fatalf("spot 16-digit us conversion mismatch: open=%s close=%s", rows[1].OpenTimeUTCNS, rows[1].CloseTimeUTCNS)
	}

	// Offline mode must reuse cached archive with ZERO HTTP requests.
	before := netHits.Load()
	offlineRes, err := cl.FetchVisionDailyKlines(context.Background(), MarketSpot, "BTCUSDT", "1m", "2026-11-10", true)
	if err != nil {
		t.Fatalf("offline FetchVisionDailyKlines failed: %v", err)
	}
	if !offlineRes.CacheHit || netHits.Load() != before {
		t.Fatalf("offline hit expected zero network calls: cacheHit=%v hits=%d->%d", offlineRes.CacheHit, before, netHits.Load())
	}

	// Options Vision klines intentionally unsupported.
	if _, err := cl.FetchVisionDailyKlines(context.Background(), MarketOptions, "BTC-261225-100000-C", "1m", "2026-11-10", false); err == nil {
		t.Fatal("expected unsupported error for Options Vision daily klines")
	}

	// Silence unused import if any.
	_ = filepath.Base(res.ZipPath)
	_ = os.ErrNotExist
}
