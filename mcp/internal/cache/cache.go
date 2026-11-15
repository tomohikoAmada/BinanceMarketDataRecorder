package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DefaultUserAgent = "binance-data-mcp/1.0"

// CacheKey captures source-specific identity fields so markets, symbols, pairs, and windows never collide.
type CacheKey struct {
	Source         string            `json:"source"`
	Market         string            `json:"market"`
	Dataset        string            `json:"dataset"`
	Symbol         string            `json:"symbol,omitempty"`
	Pair           string            `json:"pair,omitempty"`
	Expiry         string            `json:"expiry,omitempty"`
	Interval       string            `json:"interval,omitempty"`
	StartUTCNS     string            `json:"start_utc_ns,omitempty"`
	EndUTCNS       string            `json:"end_utc_ns,omitempty"`
	RevisionOrHash string            `json:"revision_or_hash,omitempty"`
	Params         map[string]string `json:"params,omitempty"`
}

// Digest returns a deterministic SHA-256 hex digest of the canonical cache key.
func (k CacheKey) Digest() string {
	type canon struct {
		Source         string   `json:"source"`
		Market         string   `json:"market"`
		Dataset        string   `json:"dataset"`
		Symbol         string   `json:"symbol,omitempty"`
		Pair           string   `json:"pair,omitempty"`
		Expiry         string   `json:"expiry,omitempty"`
		Interval       string   `json:"interval,omitempty"`
		StartUTCNS     string   `json:"start_utc_ns,omitempty"`
		EndUTCNS       string   `json:"end_utc_ns,omitempty"`
		RevisionOrHash string   `json:"revision_or_hash,omitempty"`
		Params         []string `json:"params,omitempty"`
	}
	var pairs []string
	if len(k.Params) > 0 {
		keys := make([]string, 0, len(k.Params))
		for pk := range k.Params {
			keys = append(keys, pk)
		}
		sort.Strings(keys)
		for _, pk := range keys {
			pairs = append(pairs, pk+"="+k.Params[pk])
		}
	}
	payload, _ := json.Marshal(canon{
		Source:         k.Source,
		Market:         k.Market,
		Dataset:        k.Dataset,
		Symbol:         k.Symbol,
		Pair:           k.Pair,
		Expiry:         k.Expiry,
		Interval:       k.Interval,
		StartUTCNS:     k.StartUTCNS,
		EndUTCNS:       k.EndUTCNS,
		RevisionOrHash: k.RevisionOrHash,
		Params:         pairs,
	})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// Receipt records verified provenance and integrity metadata alongside a finalized cached artifact.
type Receipt struct {
	Key        CacheKey `json:"key"`
	SourceURL  string   `json:"source_url"`
	SizeBytes  int64    `json:"size_bytes"`
	SHA256     string   `json:"sha256"`
	ETag       string   `json:"etag,omitempty"`
	VerifiedAt string   `json:"verified_at"`
	OfflineOK  bool     `json:"offline_ok"`
}

// partMeta records in-flight download identity so .part resume validates entity match cleanly.
type partMeta struct {
	SourceURL string `json:"source_url"`
	ETag      string `json:"etag,omitempty"`
	TotalSize int64  `json:"total_size,omitempty"`
	SHA256    string `json:"expected_sha256,omitempty"`
}

// RateLimiter bounds sustained read throughput without bursty memory allocations.
type RateLimiter struct {
	mu          sync.Mutex
	bytesPerSec int64
	tokens      float64
	last        time.Time
}

// NewRateLimiter constructs a byte-rate limiter; bps <= 0 disables throttling.
func NewRateLimiter(bytesPerSec int64) *RateLimiter {
	if bytesPerSec <= 0 {
		return nil
	}
	return &RateLimiter{
		bytesPerSec: bytesPerSec,
		tokens:      float64(bytesPerSec),
		last:        time.Now(),
	}
}

// Wait consumes n byte tokens or respects context cancellation cleanly.
func (r *RateLimiter) Wait(ctx context.Context, n int) error {
	if r == nil || n <= 0 {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(r.last).Seconds()
		r.last = now
		r.tokens += elapsed * float64(r.bytesPerSec)
		maxBurst := float64(r.bytesPerSec)
		if r.tokens > maxBurst {
			r.tokens = maxBurst
		}
		if r.tokens >= 1 {
			take := float64(n)
			if take > r.tokens {
				take = r.tokens
			}
			r.tokens -= take
			remaining := n - int(take)
			r.mu.Unlock()
			if remaining <= 0 {
				return nil
			}
			n = remaining
			continue
		}
		waitDur := time.Duration(((1.0 - r.tokens) / float64(r.bytesPerSec)) * float64(time.Second))
		if waitDur < 5*time.Millisecond {
			waitDur = 5 * time.Millisecond
		}
		r.mu.Unlock()
		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// HeaderProvider resolves headers per URL so credentials never cross unintended origins.
type HeaderProvider func(rawURL string) map[string]string

// Manager coordinates deduplicated local file caching, streaming SHA-256 verification, and resume.
type Manager struct {
	DataDir        string
	HTTPClient     *http.Client
	HeaderProvider HeaderProvider
	Limiter        *RateLimiter
	Sem            chan struct{}

	mu       sync.Mutex
	inFlight map[string]*singleFlightCall
}

type singleFlightCall struct {
	wg   sync.WaitGroup
	path string
	rec  *Receipt
	err  error
}

// NewManager creates a cache Manager rooted at dataDir with bounded concurrency and throughput.
func NewManager(dataDir string, client *http.Client, concurrency int, rateBPS int64, hp HeaderProvider) *Manager {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	if concurrency < 1 {
		concurrency = 1
	}
	return &Manager{
		DataDir:        dataDir,
		HTTPClient:     client,
		HeaderProvider: hp,
		Limiter:        NewRateLimiter(rateBPS),
		Sem:            make(chan struct{}, concurrency),
		inFlight:       make(map[string]*singleFlightCall),
	}
}

// PathFor resolves a safe path inside BINANCE_MCP_DATA_DIR, rejecting directory traversal.
func (m *Manager) PathFor(relParts ...string) (string, error) {
	joined := filepath.Join(append([]string{m.DataDir}, relParts...)...)
	clean, err := filepath.Abs(joined)
	if err != nil {
		return "", err
	}
	base, err := filepath.Abs(m.DataDir)
	if err != nil {
		return "", err
	}
	if clean != base && !strings.HasPrefix(clean, base+string(filepath.Separator)) {
		return "", fmt.Errorf("cache path %q escapes data_dir %q", clean, base)
	}
	return clean, nil
}

// ReceiptPath returns the sibling provenance receipt path for a cached file.
func ReceiptPath(destPath string) string {
	return destPath + ".receipt.json"
}

// LoadReceipt checks whether a cached file already has a valid matching receipt without re-hashing big blobs.
func LoadReceipt(destPath string) (*Receipt, bool) {
	st, err := os.Stat(destPath)
	if err != nil || st.IsDir() {
		return nil, false
	}
	raw, err := os.ReadFile(ReceiptPath(destPath))
	if err != nil {
		return nil, false
	}
	var rec Receipt
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, false
	}
	if rec.SizeBytes != st.Size() || len(rec.SHA256) != 64 {
		return nil, false
	}
	return &rec, true
}

// WriteReceipt atomically persists provenance metadata for a local cached file.
func WriteReceipt(destPath string, rec Receipt) error {
	payload, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(ReceiptPath(destPath), payload)
}

// EnsureOptions controls pinned/offline reuse vs network fetch and resume checks.
type EnsureOptions struct {
	DestRelPath    string
	Key            CacheKey
	SourceURL      string
	FallbackURL    string
	ExpectedSHA256 string
	ExpectedBytes  int64
	Offline        bool
	ForceVerify    bool
}

// EnsureResult describes cache hit vs download and verified identity.
type EnsureResult struct {
	LocalPath string   `json:"local_path"`
	Receipt   *Receipt `json:"receipt"`
	CacheHit  bool     `json:"cache_hit"`
	Resumed   bool     `json:"resumed"`
	BytesRead int64    `json:"bytes_downloaded"`
}

// EnsureFile guarantees a verified cached artifact via receipt reuse, single-flight dedup, or resumable fetch.
func (m *Manager) EnsureFile(ctx context.Context, opts EnsureOptions) (*EnsureResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	destPath, err := m.PathFor(opts.DestRelPath)
	if err != nil {
		return nil, err
	}

	if rec, ok := LoadReceipt(destPath); ok {
		matchesExpected := opts.ExpectedSHA256 == "" || strings.EqualFold(rec.SHA256, opts.ExpectedSHA256)
		matchesBytes := opts.ExpectedBytes <= 0 || rec.SizeBytes == opts.ExpectedBytes
		if matchesExpected && matchesBytes {
			if opts.ForceVerify {
				actualHash, _, vErr := FileSHA256(destPath)
				if vErr != nil {
					return nil, vErr
				}
				if !strings.EqualFold(actualHash, rec.SHA256) {
					return nil, fmt.Errorf("cache verification failed for %s: expected %s got %s", destPath, rec.SHA256, actualHash)
				}
			}
			// Fixed hash or offline mode makes zero network requests on subsequent hits.
			return &EnsureResult{
				LocalPath: destPath,
				Receipt:   rec,
				CacheHit:  true,
			}, nil
		}
	}

	if opts.Offline {
		return nil, fmt.Errorf("offline/pinned cache miss for %s (key=%s)", opts.DestRelPath, opts.Key.Digest())
	}
	if opts.SourceURL == "" {
		return nil, fmt.Errorf("missing download URL for %s", opts.DestRelPath)
	}

	m.mu.Lock()
	if existing, ok := m.inFlight[destPath]; ok {
		m.mu.Unlock()
		existing.wg.Wait()
		if existing.err != nil {
			return nil, existing.err
		}
		return &EnsureResult{
			LocalPath: existing.path,
			Receipt:   existing.rec,
			CacheHit:  true,
		}, nil
	}
	call := &singleFlightCall{}
	call.wg.Add(1)
	m.inFlight[destPath] = call
	m.mu.Unlock()

	defer func() {
		call.wg.Done()
		m.mu.Lock()
		delete(m.inFlight, destPath)
		m.mu.Unlock()
	}()

	select {
	case m.Sem <- struct{}{}:
		defer func() { <-m.Sem }()
	case <-ctx.Done():
		call.err = ctx.Err()
		return nil, call.err
	}

	res, dlErr := m.downloadAndFinalize(ctx, destPath, opts)
	if dlErr != nil && opts.FallbackURL != "" && opts.FallbackURL != opts.SourceURL && ctx.Err() == nil {
		fallbackOpts := opts
		fallbackOpts.SourceURL = opts.FallbackURL
		fallbackOpts.FallbackURL = ""
		res, dlErr = m.downloadAndFinalize(ctx, destPath, fallbackOpts)
	}
	if dlErr != nil {
		call.err = dlErr
		return nil, dlErr
	}
	call.path = res.LocalPath
	call.rec = res.Receipt
	return res, nil
}

func (m *Manager) downloadAndFinalize(ctx context.Context, destPath string, opts EnsureOptions) (*EnsureResult, error) {
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return nil, err
	}
	partPath := destPath + ".part"
	metaPath := destPath + ".part.json"

	var existingOffset int64
	var savedMeta partMeta
	if pst, err := os.Stat(partPath); err == nil && !pst.IsDir() && pst.Size() > 0 {
		if rawMeta, mErr := os.ReadFile(metaPath); mErr == nil {
			if jsonErr := json.Unmarshal(rawMeta, &savedMeta); jsonErr == nil {
				// Only reuse .part if target SHA/URL matches this exact object identity.
				shaMatch := opts.ExpectedSHA256 == "" || savedMeta.SHA256 == "" || strings.EqualFold(savedMeta.SHA256, opts.ExpectedSHA256)
				urlMatch := savedMeta.SourceURL == opts.SourceURL
				if shaMatch && urlMatch {
					existingOffset = pst.Size()
				}
			}
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, opts.SourceURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", DefaultUserAgent)
	if m.HeaderProvider != nil {
		for k, v := range m.HeaderProvider(opts.SourceURL) {
			req.Header.Set(k, v)
		}
	}
	if existingOffset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", existingOffset))
		if savedMeta.ETag != "" {
			req.Header.Set("If-Range", savedMeta.ETag)
		}
	}

	resp, err := m.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", opts.SourceURL, err)
	}
	defer resp.Body.Close()

	resumed := false
	switch resp.StatusCode {
	case http.StatusPartialContent:
		if existingOffset <= 0 {
			return nil, fmt.Errorf("unexpected HTTP 206 without Range request for %s", opts.SourceURL)
		}
		okRange, totalSize := validContentRange(resp.Header.Get("Content-Range"), existingOffset)
		etag := strings.TrimSpace(resp.Header.Get("ETag"))
		if !okRange || (savedMeta.ETag != "" && etag != "" && etag != savedMeta.ETag) ||
			(opts.ExpectedBytes > 0 && totalSize > 0 && totalSize != opts.ExpectedBytes) {
			// Entity mismatch: truncate and retry cleanly from 0 instead of splicing corrupt bytes.
			_ = os.Remove(partPath)
			_ = os.Remove(metaPath)
			resp.Body.Close()
			optsRetry := opts
			return m.freshDownload(ctx, destPath, partPath, metaPath, optsRetry)
		}
		resumed = true
	case http.StatusOK:
		// Server returned full representation (200): restart .part from 0, never append onto old tail!
		existingOffset = 0
		resumed = false
	case http.StatusRequestedRangeNotSatisfiable:
		if opts.ExpectedBytes > 0 && existingOffset == opts.ExpectedBytes {
			// Verify already complete .part rather than losing completed bytes.
			return m.finalizePart(destPath, partPath, metaPath, opts, savedMeta.ETag, true, 0)
		}
		_ = os.Remove(partPath)
		_ = os.Remove(metaPath)
		resp.Body.Close()
		return m.freshDownload(ctx, destPath, partPath, metaPath, opts)
	default:
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, &HTTPError{StatusCode: resp.StatusCode, URL: opts.SourceURL, BodySnippet: string(snippet)}
	}

	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	metaPayload, _ := json.Marshal(partMeta{
		SourceURL: opts.SourceURL,
		ETag:      etag,
		TotalSize: opts.ExpectedBytes,
		SHA256:    opts.ExpectedSHA256,
	})
	_ = atomicWriteFile(metaPath, metaPayload)

	flags := os.O_CREATE | os.O_WRONLY
	if resumed {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(partPath, flags, 0o644)
	if err != nil {
		return nil, err
	}

	hasher := sha256.New()
	var copied int64
	buf := make([]byte, 64*1024)
	for {
		n, rErr := resp.Body.Read(buf)
		if n > 0 {
			if wErr := m.Limiter.Wait(ctx, n); wErr != nil {
				f.Close()
				return nil, wErr
			}
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				f.Close()
				return nil, wErr
			}
			if !resumed {
				hasher.Write(buf[:n])
			}
			copied += int64(n)
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			f.Close()
			return nil, fmt.Errorf("stream %s interrupted at +%d bytes (.part kept for resume): %w", opts.SourceURL, copied, rErr)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	if resumed {
		return m.finalizePart(destPath, partPath, metaPath, opts, etag, true, copied)
	}
	finalHash := hex.EncodeToString(hasher.Sum(nil))
	return m.commitPart(destPath, partPath, metaPath, opts, etag, copied, finalHash, false, copied)
}

func (m *Manager) freshDownload(ctx context.Context, destPath, partPath, metaPath string, opts EnsureOptions) (*EnsureResult, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, opts.SourceURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", DefaultUserAgent)
	if m.HeaderProvider != nil {
		for k, v := range m.HeaderProvider(opts.SourceURL) {
			req.Header.Set(k, v)
		}
	}
	resp, err := m.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, &HTTPError{StatusCode: resp.StatusCode, URL: opts.SourceURL, BodySnippet: string(snippet)}
	}
	etag := strings.TrimSpace(resp.Header.Get("ETag"))
	f, err := os.OpenFile(partPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	hasher := sha256.New()
	var copied int64
	buf := make([]byte, 64*1024)
	for {
		n, rErr := resp.Body.Read(buf)
		if n > 0 {
			if wErr := m.Limiter.Wait(ctx, n); wErr != nil {
				f.Close()
				return nil, wErr
			}
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				f.Close()
				return nil, wErr
			}
			hasher.Write(buf[:n])
			copied += int64(n)
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			f.Close()
			return nil, rErr
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	finalHash := hex.EncodeToString(hasher.Sum(nil))
	return m.commitPart(destPath, partPath, metaPath, opts, etag, copied, finalHash, false, copied)
}

func (m *Manager) finalizePart(destPath, partPath, metaPath string, opts EnsureOptions, etag string, resumed bool, copied int64) (*EnsureResult, error) {
	actualHash, actualSize, err := FileSHA256(partPath)
	if err != nil {
		return nil, err
	}
	return m.commitPart(destPath, partPath, metaPath, opts, etag, actualSize, actualHash, resumed, copied)
}

func (m *Manager) commitPart(destPath, partPath, metaPath string, opts EnsureOptions, etag string, size int64, actualHash string, resumed bool, bytesDownloaded int64) (*EnsureResult, error) {
	if opts.ExpectedBytes > 0 && size != opts.ExpectedBytes {
		_ = os.Remove(partPath)
		_ = os.Remove(metaPath)
		return nil, fmt.Errorf("byte size mismatch for %s: expected %d got %d", opts.DestRelPath, opts.ExpectedBytes, size)
	}
	if opts.ExpectedSHA256 != "" && !strings.EqualFold(actualHash, opts.ExpectedSHA256) {
		_ = os.Remove(partPath)
		_ = os.Remove(metaPath)
		return nil, fmt.Errorf("sha256 mismatch for %s: expected %s got %s", opts.DestRelPath, opts.ExpectedSHA256, actualHash)
	}
	if err := os.Rename(partPath, destPath); err != nil {
		return nil, err
	}
	_ = os.Remove(metaPath)
	rec := Receipt{
		Key:        opts.Key,
		SourceURL:  opts.SourceURL,
		SizeBytes:  size,
		SHA256:     actualHash,
		ETag:       etag,
		VerifiedAt: time.Now().UTC().Format(time.RFC3339Nano),
		OfflineOK:  true,
	}
	if err := WriteReceipt(destPath, rec); err != nil {
		return nil, err
	}
	return &EnsureResult{
		LocalPath: destPath,
		Receipt:   &rec,
		CacheHit:  false,
		Resumed:   resumed,
		BytesRead: bytesDownloaded,
	}, nil
}

// HTTPError captures non-2xx HTTP status details for retry/backoff decisions.
type HTTPError struct {
	StatusCode  int
	URL         string
	BodySnippet string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d from %s: %s", e.StatusCode, e.URL, strings.TrimSpace(e.BodySnippet))
}

func validContentRange(hdr string, expectedStart int64) (bool, int64) {
	// Expected format: "bytes <start>-<end>/<total|*>".
	hdr = strings.TrimSpace(hdr)
	if !strings.HasPrefix(strings.ToLower(hdr), "bytes ") {
		return false, 0
	}
	rest := strings.TrimSpace(hdr[6:])
	slashIdx := strings.IndexByte(rest, '/')
	if slashIdx <= 0 {
		return false, 0
	}
	rangePart := rest[:slashIdx]
	totalPart := rest[slashIdx+1:]
	dashIdx := strings.IndexByte(rangePart, '-')
	if dashIdx <= 0 {
		return false, 0
	}
	start, err1 := strconv.ParseInt(rangePart[:dashIdx], 10, 64)
	end, err2 := strconv.ParseInt(rangePart[dashIdx+1:], 10, 64)
	if err1 != nil || err2 != nil || start != expectedStart || end < start {
		return false, 0
	}
	if totalPart == "*" {
		return true, 0
	}
	total, err3 := strconv.ParseInt(totalPart, 10, 64)
	if err3 != nil || end >= total {
		return false, 0
	}
	return true, total
}

// FileSHA256 hashes a local file in 1 MiB sequential blocks.
func FileSHA256(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, f, make([]byte, 1024*1024))
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// ParseChecksumText extracts the SHA-256 hex digest from a Binance Vision *.CHECKSUM payload.
func ParseChecksumText(raw string) (string, error) {
	for _, field := range strings.Fields(raw) {
		if len(field) == 64 {
			if _, err := hex.DecodeString(field); err == nil {
				return strings.ToLower(field), nil
			}
		}
	}
	return "", errors.New("no 64-char sha256 digest found in CHECKSUM payload")
}

func atomicWriteFile(dest string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp := dest + fmt.Sprintf(".tmp-%d", time.Now().UnixNano())
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dest)
}
