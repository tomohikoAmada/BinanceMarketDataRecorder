package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func shaHex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func TestEnsureFile_PinnedZeroNetworkAndVerify(t *testing.T) {
	payload := []byte("binance-s-bmdr-fixed-artifact-v1")
	sum := shaHex(payload)
	var hits atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("ETag", `"etag-v1"`)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	mgr := NewManager(t.TempDir(), srv.Client(), 1, 0, nil)
	opts := EnsureOptions{
		DestRelPath:    "recorder/sbmdr/storage/s1/chunk.bmdr.zst",
		Key:            CacheKey{Source: "recorder", Market: "spot", Dataset: "raw", RevisionOrHash: sum},
		SourceURL:      srv.URL + "/chunk",
		ExpectedSHA256: sum,
		ExpectedBytes:  int64(len(payload)),
	}

	res1, err := mgr.EnsureFile(context.Background(), opts)
	if err != nil {
		t.Fatalf("first EnsureFile failed: %v", err)
	}
	if res1.CacheHit || hits.Load() != 1 {
		t.Fatalf("expected 1 network fetch on first call, cacheHit=%v hits=%d", res1.CacheHit, hits.Load())
	}

	// Second call (and offline call) must make zero network requests.
	optsOffline := opts
	optsOffline.Offline = true
	res2, err := mgr.EnsureFile(context.Background(), optsOffline)
	if err != nil {
		t.Fatalf("second offline EnsureFile failed: %v", err)
	}
	if !res2.CacheHit || hits.Load() != 1 {
		t.Fatalf("expected zero network hits on pinned reuse, cacheHit=%v hits=%d", res2.CacheHit, hits.Load())
	}

	// Tamper with cached file and confirm Inspect(verify=true) detects mismatch.
	if err := os.WriteFile(res1.LocalPath, []byte("corrupted-bytes-matching-length-!"), 0o644); err != nil {
		t.Fatalf("corrupt file: %v", err)
	}
	// Adjust only if same length, else recheck failure.
	sumInfo, err := mgr.Inspect(context.Background(), true)
	if err != nil {
		t.Fatalf("Inspect verify failed unexpectedly: %v", err)
	}
	if len(sumInfo.VerifyFailures) != 1 {
		t.Fatalf("expected 1 verify failure after tamper, got %v", sumInfo.VerifyFailures)
	}
}

func TestEnsureFile_Range206ResumeAnd200NoCorruption(t *testing.T) {
	payload := []byte(strings.Repeat("0123456789abcdef", 64))
	sum := shaHex(payload)

	// Subcase A: 206 Partial Content appends cleanly from existing .part
	var rangeCalls atomic.Int32
	srv206 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeCalls.Add(1)
		rng := r.Header.Get("Range")
		if rng == "" || r.Header.Get("If-Range") != `"v1"` {
			t.Fatalf("expected Range + If-Range=\"v1\", got Range=%q If-Range=%q", rng, r.Header.Get("If-Range"))
		}
		startStr := strings.TrimSuffix(strings.TrimPrefix(rng, "bytes="), "-")
		start, err := strconv.Atoi(startStr)
		if err != nil {
			t.Fatalf("bad range %q: %v", rng, err)
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(payload)-1, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start:])
	}))
	defer srv206.Close()

	dirA := t.TempDir()
	mgrA := NewManager(dirA, srv206.Client(), 1, 0, nil)
	destA, _ := mgrA.PathFor("binance_vision/spot/daily/klines/test.zip")
	_ = os.MkdirAll(filepath.Dir(destA), 0o755)
	_ = os.WriteFile(destA+".part", payload[:256], 0o644)
	_ = atomicWriteFile(destA+".part.json", []byte(fmt.Sprintf(`{"source_url":"%s","etag":"\"v1\"","expected_sha256":"%s"}`, srv206.URL+"/f", sum)))

	resA, err := mgrA.EnsureFile(context.Background(), EnsureOptions{
		DestRelPath:    "binance_vision/spot/daily/klines/test.zip",
		Key:            CacheKey{Source: "vision", Market: "spot", Dataset: "klines", Symbol: "BTCUSDT", RevisionOrHash: sum},
		SourceURL:      srv206.URL + "/f",
		ExpectedSHA256: sum,
		ExpectedBytes:  int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("206 resume failed: %v", err)
	}
	if !resA.Resumed || rangeCalls.Load() != 1 {
		t.Fatalf("expected resumed=true with 1 call, got resumed=%v calls=%d", resA.Resumed, rangeCalls.Load())
	}

	// Subcase B: Server ignores Range and returns 200 full body -> must overwrite, NOT append onto half .part!
	srv200 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v2"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv200.Close()

	dirB := t.TempDir()
	mgrB := NewManager(dirB, srv200.Client(), 1, 0, nil)
	destB, _ := mgrB.PathFor("binance_vision/spot/daily/klines/test200.zip")
	_ = os.MkdirAll(filepath.Dir(destB), 0o755)
	_ = os.WriteFile(destB+".part", payload[:200], 0o644)
	_ = atomicWriteFile(destB+".part.json", []byte(fmt.Sprintf(`{"source_url":"%s","etag":"\"v1\"","expected_sha256":"%s"}`, srv200.URL+"/f", sum)))

	resB, err := mgrB.EnsureFile(context.Background(), EnsureOptions{
		DestRelPath:    "binance_vision/spot/daily/klines/test200.zip",
		Key:            CacheKey{Source: "vision", Market: "spot", Dataset: "klines", Symbol: "BTCUSDT", RevisionOrHash: sum},
		SourceURL:      srv200.URL + "/f",
		ExpectedSHA256: sum,
		ExpectedBytes:  int64(len(payload)),
	})
	if err != nil {
		t.Fatalf("200 restart download failed: %v", err)
	}
	if resB.Resumed || resB.Receipt.SizeBytes != int64(len(payload)) {
		t.Fatalf("expected full overwrite without corruption, got resumed=%v size=%d", resB.Resumed, resB.Receipt.SizeBytes)
	}
}

func TestJobCoordinator_CancelAndResume(t *testing.T) {
	item1 := []byte("chunk-one-bytes")
	item2 := []byte("chunk-two-bytes")
	sum1, sum2 := shaHex(item1), shaHex(item2)

	var served2 atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/1") {
			_, _ = w.Write(item1)
			return
		}
		if served2.Add(1) == 1 {
			time.Sleep(150 * time.Millisecond)
		}
		_, _ = w.Write(item2)
	}))
	defer srv.Close()

	mgr := NewManager(t.TempDir(), srv.Client(), 1, 0, nil)
	jc := NewJobCoordinator(mgr)

	items := []JobItem{
		{ItemID: "1", DestRelPath: "recorder/sbmdr/storage/s1/1.zst", SourceURL: srv.URL + "/1", SHA256: sum1, SizeBytes: int64(len(item1)), Key: CacheKey{Source: "recorder", Market: "spot", Dataset: "raw", RevisionOrHash: sum1}},
		{ItemID: "2", DestRelPath: "recorder/sbmdr/storage/s1/2.zst", SourceURL: srv.URL + "/2", SHA256: sum2, SizeBytes: int64(len(item2)), Key: CacheKey{Source: "recorder", Market: "spot", Dataset: "raw", RevisionOrHash: sum2}},
	}
	job, err := jc.StartOrResume("test batch", items, "")
	if err != nil {
		t.Fatalf("start job: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	cancelled, err := jc.Cancel(job.JobID)
	if err != nil || cancelled.State != "cancelled" {
		t.Fatalf("cancel job unexpected: %+v err=%v", cancelled, err)
	}

	// Wait briefly for in-flight goroutine to exit, then resume.
	time.Sleep(180 * time.Millisecond)
	resumed, err := jc.StartOrResume("", nil, job.JobID)
	if err != nil {
		t.Fatalf("resume job failed: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cur, _ := jc.LoadJob(resumed.JobID)
		if cur.State == "completed" {
			if cur.CompletedItems != 2 {
				t.Fatalf("expected 2 completed items, got %d", cur.CompletedItems)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("resumed job did not complete in time")
}

func TestEnsureFile_CrossManagerFlockSerializesAndReuses(t *testing.T) {
	payload := []byte("cross-manager-shared-artifact-payload")
	sum := shaHex(payload)
	var hits atomic.Int32
	var forceHits atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
		if r.URL.Path == "/latest.json" {
			forceHits.Add(1)
			_, _ = w.Write([]byte(`{"revision_id":"r1"}`))
			return
		}
		hits.Add(1)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	dataDir := t.TempDir()
	mgr1 := NewManager(dataDir, srv.Client(), 2, 0, nil)
	mgr2 := NewManager(dataDir, srv.Client(), 2, 0, nil)

	// Subcase A: concurrent uncached fetch across two separate Managers hits network once via flock + receipt recheck.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, m := range []*Manager{mgr1, mgr2} {
		wg.Add(1)
		go func(mgr *Manager) {
			defer wg.Done()
			_, err := mgr.EnsureFile(context.Background(), EnsureOptions{
				DestRelPath:    "recorder/revisions/r1/manifest.json",
				Key:            CacheKey{Source: "recorder_manifest", Dataset: "manifest", RevisionOrHash: "r1"},
				SourceURL:      srv.URL + "/manifest.json",
				ExpectedSHA256: sum,
				ExpectedBytes:  int64(len(payload)),
			})
			errs <- err
		}(m)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent pinned EnsureFile failed: %v", err)
		}
	}
	if hits.Load() != 1 {
		t.Fatalf("expected 1 network fetch across locked Managers, got %d", hits.Load())
	}

	// Subcase B: concurrent ForceRefresh across 4 separate Managers serializes .part -> destPath rename with zero race.
	errs2 := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		mgr := NewManager(dataDir, srv.Client(), 2, 0, nil)
		go func(m *Manager) {
			defer wg.Done()
			_, err := m.EnsureFile(context.Background(), EnsureOptions{
				DestRelPath:  "recorder/latest.json",
				Key:          CacheKey{Source: "recorder_index", Dataset: "latest"},
				SourceURL:    srv.URL + "/latest.json",
				ForceRefresh: true,
			})
			errs2 <- err
		}(mgr)
	}
	wg.Wait()
	close(errs2)
	for err := range errs2 {
		if err != nil {
			t.Fatalf("concurrent ForceRefresh EnsureFile failed: %v", err)
		}
	}
	if forceHits.Load() != 4 {
		t.Fatalf("expected 4 serialized ForceRefresh fetches, got %d", forceHits.Load())
	}

	sumInspect, err := mgr1.Inspect(context.Background(), true)
	if err != nil {
		t.Fatalf("inspect after flock tests: %v", err)
	}
	if sumInspect.TotalFiles != 2 || sumInspect.ReceiptCount != 2 || len(sumInspect.VerifyFailures) != 0 {
		t.Fatalf("unexpected inspect summary (.locks should be ignored): %+v", sumInspect)
	}
}
