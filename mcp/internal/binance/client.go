package binance

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/cache"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
)

const (
	maxHTTPRetries    = 3
	maxBackoffWait    = 15 * time.Second
	maxWSSampleWindow = 10 * time.Second
	maxWSSampleEvents = 200
)

// Client handles Binance REST requests, Vision archive downloads, and bounded public WS sampling.
type Client struct {
	Cfg      *config.Config
	CacheMgr *cache.Manager
	HTTP     *http.Client

	rateMu      sync.Mutex
	backoffTill time.Time
	lastReqAt   time.Time
}

// NewClient initializes a rate-bounded Binance public market data client.
func NewClient(cfg *config.Config, cacheMgr *cache.Manager) *Client {
	return &Client{
		Cfg:      cfg,
		CacheMgr: cacheMgr,
		HTTP:     &http.Client{Timeout: cfg.HTTPTimeout},
	}
}

// RESTResponse captures a verified public REST call with cache provenance and headers.
type RESTResponse struct {
	EndpointID string          `json:"endpoint_id"`
	URL        string          `json:"url"`
	StatusCode int             `json:"status_code"`
	FromCache  bool            `json:"from_cache"`
	FetchedAt  string          `json:"fetched_at_utc"`
	Payload    json.RawMessage `json:"payload"`
}

// ExecuteCapability runs an allowlisted Capability (GET REST or bounded WS). Rejects anything unapproved.
func (c *Client) ExecuteCapability(ctx context.Context, capEntry Capability, params map[string]string, offline bool) (*RESTResponse, error) {
	if capEntry.Status != StatusImplemented {
		return nil, fmt.Errorf("capability %s is %s: %s", capEntry.EndpointID, capEntry.Status, capEntry.Notes)
	}
	if capEntry.Transport == "ws_bounded" {
		if offline {
			return nil, fmt.Errorf("offline mode enabled: cannot open live websocket stream %s", capEntry.EndpointID)
		}
		return c.sampleWS(ctx, capEntry, params)
	}
	if capEntry.Transport != "rest" || capEntry.Method != "GET" {
		return nil, fmt.Errorf("endpoint %s (%s %s) cannot be invoked as REST GET", capEntry.EndpointID, capEntry.Method, capEntry.Transport)
	}
	for _, reqParam := range capEntry.RequiredParams {
		if strings.TrimSpace(params[reqParam]) == "" {
			return nil, fmt.Errorf("endpoint %s requires parameter %q", capEntry.EndpointID, reqParam)
		}
	}
	if capEntry.Auth == "api_key_required" && c.Cfg.BinanceAPIKey == "" {
		return nil, fmt.Errorf("endpoint %s requires BINANCE_API_KEY (MARKET_DATA public read-only key) which is not configured; prefer anonymous endpoints or Vision archives", capEntry.EndpointID)
	}

	q := url.Values{}
	allowed := make(map[string]bool)
	for _, k := range capEntry.RequiredParams {
		allowed[k] = true
	}
	for _, k := range capEntry.OptionalParams {
		allowed[k] = true
	}
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := strings.TrimSpace(params[k])
		if v == "" || k == "offline" {
			continue
		}
		if !allowed[k] {
			return nil, fmt.Errorf("parameter %q is not allowlisted for %s", k, capEntry.EndpointID)
		}
		q.Set(k, v)
	}

	fullURL := capEntry.BaseURL + capEntry.Path
	if encoded := q.Encode(); encoded != "" {
		fullURL += "?" + encoded
	}

	key := cache.CacheKey{
		Source:  "rest",
		Market:  string(capEntry.Market),
		Dataset: capEntry.EndpointID,
		Symbol:  params["symbol"],
		Pair:    params["pair"],
		Params:  params,
	}
	cacheFile, _ := c.CacheMgr.PathFor("rest_cache", string(capEntry.Market), capEntry.EndpointID, key.Digest()+".json")

	// Check finite TTL cache when TTLSeconds > 0 or offline mode requested.
	if st, err := os.Stat(cacheFile); err == nil && !st.IsDir() {
		if raw, rErr := os.ReadFile(cacheFile); rErr == nil {
			var cached RESTResponse
			if jErr := json.Unmarshal(raw, &cached); jErr == nil {
				if offline || (capEntry.TTLSeconds > 0 && time.Since(st.ModTime()) <= time.Duration(capEntry.TTLSeconds)*time.Second) {
					cached.FromCache = true
					return &cached, nil
				}
			}
		}
	}
	if offline {
		return nil, fmt.Errorf("offline mode enabled and no cached REST entry for %s", capEntry.EndpointID)
	}

	body, status, err := c.doGETWithBackoff(ctx, fullURL, capEntry.Auth == "api_key_required")
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		body, _ = json.Marshal(string(body))
	}
	res := &RESTResponse{
		EndpointID: capEntry.EndpointID,
		URL:        fullURL,
		StatusCode: status,
		FromCache:  false,
		FetchedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Payload:    json.RawMessage(body),
	}
	if capEntry.TTLSeconds > 0 {
		if encoded, mErr := json.Marshal(res); mErr == nil {
			_ = os.MkdirAll(filepath.Dir(cacheFile), 0o755)
			_ = os.WriteFile(cacheFile, encoded, 0o644)
		}
	}
	return res, nil
}

func (c *Client) doGETWithBackoff(ctx context.Context, rawURL string, attachAPIKey bool) ([]byte, int, error) {
	if err := c.waitSlot(ctx); err != nil {
		return nil, 0, err
	}

	var lastErr error
	for attempt := 0; attempt <= maxHTTPRetries; attempt++ {
		if err := c.waitBackoff(ctx); err != nil {
			return nil, 0, err
		}
		select {
		case c.CacheMgr.Sem <- struct{}{}:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}

		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if reqErr != nil {
			<-c.CacheMgr.Sem
			return nil, 0, reqErr
		}
		req.Header.Set("User-Agent", cache.DefaultUserAgent)
		if attachAPIKey && config.IsBinanceOrigin(rawURL) && c.Cfg.BinanceAPIKey != "" {
			req.Header.Set("X-MBX-APIKEY", c.Cfg.BinanceAPIKey)
		}

		resp, doErr := c.HTTP.Do(req)
		<-c.CacheMgr.Sem
		if doErr != nil {
			lastErr = doErr
			if attempt < maxHTTPRetries {
				if slpErr := sleepCtx(ctx, time.Duration(attempt+1)*400*time.Millisecond); slpErr != nil {
					return nil, 0, slpErr
				}
				continue
			}
			break
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 418 {
			wait := parseRetryAfter(resp.Header.Get("Retry-After"))
			if wait <= 0 {
				wait = time.Duration(attempt+2) * 2 * time.Second
			}
			c.setBackoff(wait)
			if wait > maxBackoffWait {
				return nil, resp.StatusCode, fmt.Errorf("binance rate limited (%d) requiring %v wait (> max %v)", resp.StatusCode, wait, maxBackoffWait)
			}
			lastErr = fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, rawURL, strings.TrimSpace(string(body)))
			if attempt < maxHTTPRetries {
				continue
			}
			break
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, resp.StatusCode, fmt.Errorf("HTTP %d from %s: %s", resp.StatusCode, rawURL, strings.TrimSpace(string(body)))
		}
		return body, resp.StatusCode, nil
	}
	return nil, 0, lastErr
}

func (c *Client) waitSlot(ctx context.Context) error {
	c.rateMu.Lock()
	now := time.Now()
	minGap := 120 * time.Millisecond
	wait := minGap - now.Sub(c.lastReqAt)
	if wait < 0 {
		wait = 0
	}
	c.lastReqAt = now.Add(wait)
	c.rateMu.Unlock()
	if wait > 0 {
		return sleepCtx(ctx, wait)
	}
	return nil
}

func (c *Client) waitBackoff(ctx context.Context) error {
	c.rateMu.Lock()
	wait := time.Until(c.backoffTill)
	c.rateMu.Unlock()
	if wait > 0 {
		return sleepCtx(ctx, wait)
	}
	return nil
}

func (c *Client) setBackoff(d time.Duration) {
	c.rateMu.Lock()
	until := time.Now().Add(d)
	if until.After(c.backoffTill) {
		c.backoffTill = until
	}
	c.rateMu.Unlock()
}

func parseRetryAfter(val string) time.Duration {
	val = strings.TrimSpace(val)
	if val == "" {
		return 0
	}
	if secs, err := strconv.Atoi(val); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(val); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Client) sampleWS(ctx context.Context, capEntry Capability, params map[string]string) (*RESTResponse, error) {
	durSec := 3
	if raw := strings.TrimSpace(params["duration_seconds"]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > int(maxWSSampleWindow.Seconds()) {
			return nil, fmt.Errorf("duration_seconds must be between 1 and %d", int(maxWSSampleWindow.Seconds()))
		}
		durSec = n
	}
	maxEvents := 25
	if raw := strings.TrimSpace(params["max_events"]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxWSSampleEvents {
			return nil, fmt.Errorf("max_events must be between 1 and %d", maxWSSampleEvents)
		}
		maxEvents = n
	}

	streamPath := capEntry.Path
	if strings.Contains(streamPath, "<symbol>") {
		sym := strings.ToLower(strings.TrimSpace(params["symbol"]))
		if sym == "" {
			return nil, fmt.Errorf("%s requires symbol", capEntry.EndpointID)
		}
		streamPath = strings.ReplaceAll(streamPath, "<symbol>", sym)
	}
	wsURL := strings.TrimRight(c.Cfg.RecorderHTTPSURL, "/") // unused
	_ = wsURL
	targetWS := strings.TrimRight(capEntry.BaseURL, "/") + "/" + strings.TrimLeft(streamPath, "/")

	sctx, cancel := context.WithTimeout(ctx, time.Duration(durSec)*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(sctx, targetWS, nil)
	if err != nil {
		return nil, fmt.Errorf("WS connect %s: %w", targetWS, err)
	}
	defer conn.CloseNow()

	filterSym := strings.ToUpper(strings.TrimSpace(params["symbol"]))
	var events []json.RawMessage
	for len(events) < maxEvents {
		_, msg, rErr := conn.Read(sctx)
		if rErr != nil {
			if errors.Is(rErr, context.DeadlineExceeded) || errors.Is(rErr, context.Canceled) {
				break
			}
			break
		}
		if filterSym != "" && strings.Contains(string(msg), `"o":`) {
			// Match liquidation target symbol when !forceOrder@arr delivers all symbols.
			if !strings.Contains(strings.ToUpper(string(msg)), `"`+filterSym+`"`) {
				continue
			}
		}
		if json.Valid(msg) {
			events = append(events, json.RawMessage(msg))
		}
	}
	_ = conn.Close(websocket.StatusNormalClosure, "bounded sample complete")

	wrapped, _ := json.Marshal(map[string]any{
		"transport":        "ws_bounded",
		"stream_url":       targetWS,
		"duration_seconds": durSec,
		"max_events":       maxEvents,
		"sampled_count":    len(events),
		"events":           events,
	})
	return &RESTResponse{
		EndpointID: capEntry.EndpointID,
		URL:        targetWS,
		StatusCode: 200,
		FromCache:  false,
		FetchedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		Payload:    wrapped,
	}, nil
}

// VisionArchiveResult contains a verified downloaded Binance Vision zip path and parsed SHA-256.
type VisionArchiveResult struct {
	ZipPath      string `json:"zip_path"`
	ChecksumHex  string `json:"sha256"`
	ChecksumURL  string `json:"checksum_url"`
	ZipURL       string `json:"zip_url"`
	RelSubPath   string `json:"rel_sub_path,omitempty"`
	CacheHit     bool   `json:"cache_hit"`
	Resumed      bool   `json:"resumed"`
}

// VisionArchiveRequest describes a daily or monthly Binance Vision archive download across supported datasets.
type VisionArchiveRequest struct {
	Market   Market
	Cadence  string // "daily" or "monthly"
	Dataset  string // e.g. "klines", "markPriceKlines", "indexPriceKlines", "aggTrades", "trades", "metrics", "fundingRate", "bookDepth", "bookTicker"
	Symbol   string // uppercase symbol (or underlying for options)
	Interval string // required for interval-indexed kline datasets; "1M" -> "1mo" handled cleanly
	Period   string // "YYYY-MM-DD" for daily, "YYYY-MM" for monthly
}

type visionLatestPointer struct {
	SHA256    string `json:"sha256"`
	ZipPath   string `json:"zip_path"`
	RelSub    string `json:"rel_sub"`
	ZipURL    string `json:"zip_url"`
	UpdatedAt string `json:"updated_at_utc"`
}

// VisionRelativePath computes the canonical relative path under https://data.binance.vision/data/.
func VisionRelativePath(req VisionArchiveRequest) (string, error) {
	cad := strings.ToLower(strings.TrimSpace(req.Cadence))
	if cad == "" {
		cad = "daily"
	}
	if cad != "daily" && cad != "monthly" {
		return "", fmt.Errorf("Vision cadence must be 'daily' or 'monthly', got %q", req.Cadence)
	}
	ds := strings.TrimSpace(req.Dataset)
	if ds == "" {
		return "", errors.New("Vision dataset is required")
	}
	sym := strings.ToUpper(strings.TrimSpace(req.Symbol))
	if sym == "" {
		return "", errors.New("Vision symbol is required")
	}
	period := strings.TrimSpace(req.Period)
	if period == "" {
		return "", errors.New("Vision period (YYYY-MM-DD or YYYY-MM) is required")
	}

	var marketPrefix string
	switch req.Market {
	case MarketSpot:
		marketPrefix = "spot"
	case MarketUSDM:
		marketPrefix = filepath.Join("futures", "um")
	case MarketCOINM:
		marketPrefix = filepath.Join("futures", "cm")
	case MarketOptions:
		if ds != "BVOLIndex" && ds != "EOHSummary" {
			return "", fmt.Errorf("Vision dataset %s unsupported for market %s", ds, req.Market)
		}
		marketPrefix = "option"
	default:
		return "", fmt.Errorf("Vision archives unsupported for market %q", req.Market)
	}

	intervalDatasets := ds == "klines" || ds == "markPriceKlines" || ds == "indexPriceKlines" || ds == "premiumIndexKlines"
	if intervalDatasets {
		vi := VisionInterval(strings.TrimSpace(req.Interval))
		if vi == "" {
			return "", fmt.Errorf("Vision dataset %s requires interval", ds)
		}
		filename := fmt.Sprintf("%s-%s-%s.zip", sym, vi, period)
		return filepath.Join(marketPrefix, cad, ds, sym, vi, filename), nil
	}
	if ds == "BVOLIndex" {
		return filepath.Join("option", cad, ds, sym, fmt.Sprintf("%s-%s.zip", sym, period)), nil
	}
	filename := fmt.Sprintf("%s-%s-%s.zip", sym, ds, period)
	return filepath.Join(marketPrefix, cad, ds, sym, filename), nil
}

// FetchVisionArchive downloads and checksum-verifies any supported daily/monthly Binance Vision ZIP archive
// using content-addressed storage keyed by SHA-256 so revised upstream files never overwrite earlier revisions.
func (c *Client) FetchVisionArchive(ctx context.Context, req VisionArchiveRequest, offline bool) (*VisionArchiveResult, error) {
	relSub, err := VisionRelativePath(req)
	if err != nil {
		return nil, err
	}
	datasetTag := strings.TrimSpace(req.Cadence) + "_" + strings.TrimSpace(req.Dataset)
	return c.fetchVisionZip(ctx, req.Market, datasetTag, strings.ToUpper(strings.TrimSpace(req.Symbol)), VisionInterval(req.Interval), req.Period, relSub, offline)
}

// InspectVisionArchiveLocal checks locally cached state (no network) for a Binance Vision archive.
func (c *Client) InspectVisionArchiveLocal(req VisionArchiveRequest) (relSub string, cached bool, sha256Hex string, zipPath string, err error) {
	relSub, err = VisionRelativePath(req)
	if err != nil {
		return "", false, "", "", err
	}
	legacyDestPath, err := c.CacheMgr.PathFor(filepath.Join("binance_vision", relSub))
	if err != nil {
		return "", false, "", "", err
	}
	ptrPath := legacyDestPath + ".latest.json"
	if rawPtr, pErr := os.ReadFile(ptrPath); pErr == nil {
		var ptr visionLatestPointer
		if json.Unmarshal(rawPtr, &ptr) == nil && ptr.ZipPath != "" && ptr.SHA256 != "" {
			if rec, ok := cache.LoadReceipt(ptr.ZipPath); ok && strings.EqualFold(rec.SHA256, ptr.SHA256) {
				return filepath.ToSlash(relSub), true, rec.SHA256, ptr.ZipPath, nil
			}
		}
	}
	if rec, ok := cache.LoadReceipt(legacyDestPath); ok {
		return filepath.ToSlash(relSub), true, rec.SHA256, legacyDestPath, nil
	}
	return filepath.ToSlash(relSub), false, "", "", nil
}

// BuildVisionPrefetchJobItem resolves the upstream .CHECKSUM and returns a content-addressed cache.JobItem.
func (c *Client) BuildVisionPrefetchJobItem(ctx context.Context, req VisionArchiveRequest) (cache.JobItem, error) {
	relSub, err := VisionRelativePath(req)
	if err != nil {
		return cache.JobItem{}, err
	}
	zipURL := VisionBaseURL + "/data/" + filepath.ToSlash(relSub)
	chkURL := zipURL + ".CHECKSUM"
	chkBody, _, cErr := c.doGETWithBackoff(ctx, chkURL, false)
	if cErr != nil {
		return cache.JobItem{}, fmt.Errorf("fetch Vision checksum %s: %w", chkURL, cErr)
	}
	expectedSHA, pErr := cache.ParseChecksumText(string(chkBody))
	if pErr != nil {
		return cache.JobItem{}, fmt.Errorf("parse Vision checksum %s: %w", chkURL, pErr)
	}
	expectedLower := strings.ToLower(expectedSHA)
	destRel := filepath.Join("binance_vision", relSub)
	if len(expectedLower) >= 2 {
		destRel = filepath.Join("binance_vision", "by_sha256", expectedLower[:2], expectedLower+".zip")
	}
	destAbs, err := c.CacheMgr.PathFor(destRel)
	if err != nil {
		return cache.JobItem{}, err
	}
	ptrRel := filepath.Join("binance_vision", relSub) + ".latest.json"
	ptrBytes, _ := json.Marshal(visionLatestPointer{
		SHA256:    expectedLower,
		ZipPath:   destAbs,
		RelSub:    filepath.ToSlash(relSub),
		ZipURL:    zipURL,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	cad := strings.ToLower(strings.TrimSpace(req.Cadence))
	if cad == "" {
		cad = "daily"
	}
	datasetTag := cad + "_" + strings.TrimSpace(req.Dataset)
	already := false
	if rec, ok := cache.LoadReceipt(destAbs); ok && strings.EqualFold(rec.SHA256, expectedLower) {
		already = true
	}
	return cache.JobItem{
		ItemID:      filepath.ToSlash(relSub),
		DestRelPath: destRel,
		SourceURL:   zipURL,
		SHA256:      expectedLower,
		Key: cache.CacheKey{
			Source:         "binance_vision",
			Market:         string(req.Market),
			Dataset:        datasetTag,
			Symbol:         strings.ToUpper(strings.TrimSpace(req.Symbol)),
			Interval:       VisionInterval(req.Interval),
			StartUTCNS:     req.Period,
			RevisionOrHash: expectedLower,
		},
		Completed:        already,
		PostWriteRelPath: ptrRel,
		PostWriteContent: ptrBytes,
	}, nil
}

// FetchVisionDailyKlines downloads and checksum-verifies one Binance Vision daily klines zip.
func (c *Client) FetchVisionDailyKlines(ctx context.Context, m Market, symbol, interval, dateYYYYMMDD string, offline bool) (*VisionArchiveResult, error) {
	return c.FetchVisionArchive(ctx, VisionArchiveRequest{
		Market:   m,
		Cadence:  "daily",
		Dataset:  "klines",
		Symbol:   symbol,
		Interval: interval,
		Period:   dateYYYYMMDD,
	}, offline)
}

func (c *Client) fetchVisionZip(ctx context.Context, m Market, dataset, symbol, interval, windowTag, relSub string, offline bool) (*VisionArchiveResult, error) {
	zipURL := VisionBaseURL + "/data/" + filepath.ToSlash(relSub)
	chkURL := zipURL + ".CHECKSUM"
	legacyDestRel := filepath.Join("binance_vision", relSub)
	legacyDestPath, err := c.CacheMgr.PathFor(legacyDestRel)
	if err != nil {
		return nil, err
	}
	ptrPath := legacyDestPath + ".latest.json"

	key := cache.CacheKey{
		Source:     "binance_vision",
		Market:     string(m),
		Dataset:    dataset,
		Symbol:     symbol,
		Interval:   interval,
		StartUTCNS: windowTag,
	}

	resolveOffline := func() (*VisionArchiveResult, bool) {
		if rawPtr, pErr := os.ReadFile(ptrPath); pErr == nil {
			var ptr visionLatestPointer
			if json.Unmarshal(rawPtr, &ptr) == nil && ptr.ZipPath != "" && ptr.SHA256 != "" {
				if rec, ok := cache.LoadReceipt(ptr.ZipPath); ok && strings.EqualFold(rec.SHA256, ptr.SHA256) {
					return &VisionArchiveResult{
						ZipPath:     ptr.ZipPath,
						ChecksumHex: rec.SHA256,
						ChecksumURL: chkURL,
						ZipURL:      zipURL,
						RelSubPath:  filepath.ToSlash(relSub),
						CacheHit:    true,
					}, true
				}
			}
		}
		if rec, ok := cache.LoadReceipt(legacyDestPath); ok {
			return &VisionArchiveResult{
				ZipPath:     legacyDestPath,
				ChecksumHex: rec.SHA256,
				ChecksumURL: chkURL,
				ZipURL:      zipURL,
				RelSubPath:  filepath.ToSlash(relSub),
				CacheHit:    true,
			}, true
		}
		return nil, false
	}

	if offline {
		if hit, ok := resolveOffline(); ok {
			return hit, nil
		}
		return nil, fmt.Errorf("offline mode enabled and no cached Vision archive for %s", relSub)
	}

	chkBody, _, cErr := c.doGETWithBackoff(ctx, chkURL, false)
	if cErr != nil {
		return nil, fmt.Errorf("fetch Vision checksum %s: %w", chkURL, cErr)
	}
	expectedSHA, pErr := cache.ParseChecksumText(string(chkBody))
	if pErr != nil {
		return nil, fmt.Errorf("parse Vision checksum %s: %w", chkURL, pErr)
	}
	expectedLower := strings.ToLower(expectedSHA)
	key.RevisionOrHash = expectedLower

	destRel := legacyDestRel
	if len(expectedLower) >= 2 {
		destRel = filepath.Join("binance_vision", "by_sha256", expectedLower[:2], expectedLower+".zip")
	}

	// Preserve existing legacy un-content-addressed cache if checksum already matches it.
	if rec, ok := cache.LoadReceipt(legacyDestPath); ok && strings.EqualFold(rec.SHA256, expectedLower) {
		return &VisionArchiveResult{
			ZipPath:     legacyDestPath,
			ChecksumHex: rec.SHA256,
			ChecksumURL: chkURL,
			ZipURL:      zipURL,
			RelSubPath:  filepath.ToSlash(relSub),
			CacheHit:    true,
		}, nil
	}

	res, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
		DestRelPath:    destRel,
		Key:            key,
		SourceURL:      zipURL,
		ExpectedSHA256: expectedLower,
		Offline:        false,
	})
	if err != nil {
		return nil, err
	}

	_ = os.MkdirAll(filepath.Dir(ptrPath), 0o755)
	if encoded, mErr := json.Marshal(visionLatestPointer{
		SHA256:    res.Receipt.SHA256,
		ZipPath:   res.LocalPath,
		RelSub:    filepath.ToSlash(relSub),
		ZipURL:    zipURL,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}); mErr == nil {
		_ = os.WriteFile(ptrPath, encoded, 0o644)
	}

	return &VisionArchiveResult{
		ZipPath:     res.LocalPath,
		ChecksumHex: res.Receipt.SHA256,
		ChecksumURL: chkURL,
		ZipURL:      zipURL,
		RelSubPath:  filepath.ToSlash(relSub),
		CacheHit:    res.CacheHit,
		Resumed:     res.Resumed,
	}, nil
}

// VisionKlineRow holds normalized exact-string kline values from Vision CSV or REST JSON.
type VisionKlineRow struct {
	OpenTimeUTCNS  string `json:"open_time_utc_ns"`
	CloseTimeUTCNS string `json:"close_time_utc_ns"`
	Open           string `json:"open"`
	High           string `json:"high"`
	Low            string `json:"low"`
	Close          string `json:"close"`
	Volume         string `json:"volume"`
	QuoteVolume    string `json:"quote_volume,omitempty"`
	BaseVolume     string `json:"base_volume,omitempty"`
	TradeCount     int64  `json:"trade_count"`
	QtyUnit        string `json:"qty_unit"`
	Source         string `json:"source"`
	RevisionOrHash string `json:"revision_or_hash,omitempty"`
}

// ReadVisionKlinesZip streams rows inside a verified Binance Vision klines zip cleanly, handling ms vs Spot >=2025 us!
func ReadVisionKlinesZip(zipPath string, m Market, checksumHex string, startMS, endMS int64) ([]VisionKlineRow, error) {
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer zr.Close()

	qtyUnit := "base_asset"
	if m == MarketCOINM {
		qtyUnit = "contracts"
	}

	var out []VisionKlineRow
	for _, f := range zr.File {
		if !strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, err
		}
		r := csv.NewReader(bufio.NewReader(rc))
		for {
			rec, rErr := r.Read()
			if rErr == io.EOF {
				break
			}
			if rErr != nil {
				rc.Close()
				return nil, rErr
			}
			if len(rec) < 9 || rec[0] == "open_time" {
				continue
			}
			openNS, openMS, err1 := normalizeVisionTimestampToNS(rec[0])
			closeNS, _, err2 := normalizeVisionTimestampToNS(rec[6])
			if err1 != nil || err2 != nil {
				continue
			}
			if startMS > 0 && openMS < startMS {
				continue
			}
			if endMS > 0 && openMS >= endMS {
				continue
			}
			cnt, _ := strconv.ParseInt(rec[8], 10, 64)
			row := VisionKlineRow{
				OpenTimeUTCNS:  openNS,
				CloseTimeUTCNS: closeNS,
				Open:           rec[1],
				High:           rec[2],
				Low:            rec[3],
				Close:          rec[4],
				Volume:         rec[5],
				TradeCount:     cnt,
				QtyUnit:        qtyUnit,
				Source:         "binance_vision",
				RevisionOrHash: checksumHex,
			}
			if m == MarketCOINM {
				row.BaseVolume = rec[7]
			} else {
				row.QuoteVolume = rec[7]
			}
			out = append(out, row)
		}
		rc.Close()
	}
	return out, nil
}

func normalizeVisionTimestampToNS(raw string) (string, int64, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return "", 0, err
	}
	// Spot Vision starting 2025-01-01 uses 16-digit microseconds; earlier & futures use 13-digit milliseconds.
	if v > 1_000_000_000_000_000 { // > 1e15 => microseconds
		ns := v * 1_000
		return strconv.FormatInt(ns, 10), v / 1_000, nil
	}
	ns := v * 1_000_000
	return strconv.FormatInt(ns, 10), v, nil
}
