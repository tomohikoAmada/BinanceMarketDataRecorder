package recorder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/cache"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
)

func shaHex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func TestSyncPlanMaterializeAndOfflinePinned(t *testing.T) {
	catBytes := []byte("sqlite-placeholder-bytes")
	catSHA := shaHex(catBytes)
	raw1 := []byte("chunk1-zst-payload")
	raw1SHA := shaHex(raw1)
	raw2 := []byte("chunk2-zst-payload")
	raw2SHA := shaHex(raw2)
	man1 := []byte(`{"archive_manifest_schema_version":"external-archive-manifest.v1","chunk_id":"c1"}`)
	man2 := []byte(`{"archive_manifest_schema_version":"external-archive-manifest.v1","chunk_id":"c2"}`)

	partJSON, _ := json.Marshal(PartitionMetadata{
		SchemaVersion:         "partition.v1",
		Market:                "usdm",
		PartitionID:           "p1",
		State:                 "open",
		IsCold:                false,
		CreatedAtUTCNS:        1000,
		ReceiveTimeUTCRangeNS: TimeRangeNS{Min: 1000, Max: 5000},
	})
	availJSON, _ := json.Marshal(AvailabilityMetadata{
		Market:      "usdm",
		PartitionID: "p1",
		Gaps:        []map[string]any{{"gap_id": "g1"}},
	})

	chunksLines := []string{
		`{"chunk_id":"c0-base","market":"usdm","stream":"depth","state":"LOCAL_DELETED","ready":true,"storage_id":"s1","target_relative_path":"raw/c0.bmdr.zst","external_manifest_relative_path":"manifests/c0.archive-manifest.json","archive_url_path":"archive/s1/raw/c0.bmdr.zst","manifest_url_path":"archive/s1/manifests/c0.archive-manifest.json","stored_bytes":` + fmt.Sprint(len(raw1)) + `,"stored_sha256":"` + raw1SHA + `","uncompressed_bytes":100,"created_at_utc_ns":1000,"updated_at_utc_ns":1500}`,
		`{"chunk_id":"c2-win","market":"usdm","stream":"depth","state":"LOCAL_DELETED","ready":true,"storage_id":"s1","target_relative_path":"raw/c2.bmdr.zst","external_manifest_relative_path":"manifests/c2.archive-manifest.json","archive_url_path":"archive/s1/raw/c2.bmdr.zst","manifest_url_path":"archive/s1/manifests/c2.archive-manifest.json","stored_bytes":` + fmt.Sprint(len(raw2)) + `,"stored_sha256":"` + raw2SHA + `","uncompressed_bytes":200,"created_at_utc_ns":2500,"updated_at_utc_ns":3500}`,
		`{"chunk_id":"c3-pending","market":"usdm","stream":"depth","state":"ACTIVE","ready":false,"pending_reason":"active_chunk","created_at_utc_ns":3600,"updated_at_utc_ns":4000}`,
	}
	chunksBytes := []byte(strings.Join(chunksLines, "\n") + "\n")

	manifestDoc := ExportManifest{
		SchemaVersion:  "mcp-export.v1",
		RevisionID:     "rev-test-1",
		GeneratedAtUTC: "2026-11-15T10:00:00Z",
		Partitions: []PartitionManifest{
			{
				Market:      "usdm",
				PartitionID: "p1",
				Files: map[string]ArtifactRef{
					"catalog_sqlite":    {Bytes: int64(len(catBytes)), SHA256: catSHA, URLPath: "export/p1/catalog.sqlite"},
					"chunks_jsonl":      {Bytes: int64(len(chunksBytes)), SHA256: shaHex(chunksBytes), URLPath: "export/p1/chunks.jsonl"},
					"availability_json": {Bytes: int64(len(availJSON)), SHA256: shaHex(availJSON), URLPath: "export/p1/availability.json"},
					"partition_json":    {Bytes: int64(len(partJSON)), SHA256: shaHex(partJSON), URLPath: "export/p1/partition.json"},
				},
			},
		},
	}
	manifestBytes, _ := json.Marshal(manifestDoc)
	latestBytes, _ := json.Marshal(LatestIndex{
		SchemaVersion:   "mcp-export.v1",
		RevisionID:      "rev-test-1",
		GeneratedAtUTC:  "2026-11-15T10:00:00Z",
		ManifestRelPath: "revisions/rev-test-1/manifest.json",
		ManifestURLPath: "export/revisions/rev-test-1/manifest.json",
		ManifestSHA256:  shaHex(manifestBytes),
		PartitionCount:  1,
		Counts:          map[string]int{"ready": 2, "pending": 1, "gaps": 1},
	})

	routes := map[string][]byte{
		"/export/latest.json":                     latestBytes,
		"/export/revisions/rev-test-1/manifest.json": manifestBytes,
		"/export/p1/catalog.sqlite":               catBytes,
		"/export/p1/chunks.jsonl":                 chunksBytes,
		"/export/p1/availability.json":            availJSON,
		"/export/p1/partition.json":               partJSON,
		"/archive/s1/raw/c0.bmdr.zst":             raw1,
		"/archive/s1/manifests/c0.archive-manifest.json": man1,
		"/archive/s1/raw/c2.bmdr.zst":             raw2,
		"/archive/s1/manifests/c2.archive-manifest.json": man2,
	}

	var privHits, httpsHits, leakedHeader atomic.Int32
	privSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := privHits.Add(1)
		if r.Header.Get("CF-Access-Client-Id") != "" {
			leakedHeader.Add(1)
		}
		// Force fallback to HTTPS for chunks.jsonl once to exercise dual-path fallback!
		if r.URL.Path == "/export/p1/chunks.jsonl" && n > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if body, ok := routes[r.URL.Path]; ok {
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer privSrv.Close()

	httpsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpsHits.Add(1)
		if r.Header.Get("CF-Access-Client-Id") != "cid-test" || r.Header.Get("CF-Access-Client-Secret") != "csec-test" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if body, ok := routes[r.URL.Path]; ok {
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer httpsSrv.Close()

	dataDir := t.TempDir()
	cfg := &config.Config{
		DataDir:            dataDir,
		RecorderPrivateURL: privSrv.URL,
		RecorderHTTPSURL:   httpsSrv.URL,
		AccessCreds:        config.AccessCredentials{ClientID: "cid-test", ClientSecret: "csec-test"},
		HTTPTimeout:        5 * time.Second,
	}
	if err := cfg.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	hp := func(rawURL string) map[string]string {
		if strings.HasPrefix(rawURL, httpsSrv.URL) {
			return map[string]string{
				"CF-Access-Client-Id":     cfg.AccessCreds.ClientID,
				"CF-Access-Client-Secret": cfg.AccessCreds.ClientSecret,
			}
		}
		return nil
	}
	mgr := cache.NewManager(dataDir, &http.Client{Timeout: 5 * time.Second}, 2, 0, hp)
	cl := NewClient(cfg, mgr)

	snap, err := cl.SyncSnapshot(context.Background(), "", false)
	if err != nil {
		t.Fatalf("SyncSnapshot failed: %v", err)
	}
	if leakedHeader.Load() != 0 {
		t.Fatalf("CF-Access header leaked to private URL %d times", leakedHeader.Load())
	}
	if httpsHits.Load() == 0 {
		t.Fatal("expected fallback to HTTPS Access endpoint when private returned 503")
	}

	plan, err := cl.PlanQuery(snap, "usdm", []string{"depth"}, 2000, 4500, true)
	if err != nil {
		t.Fatalf("PlanQuery failed: %v", err)
	}
	// c0-base (pre-start baseline) + c2-win = 2 ready chunks, and c3-pending = 1 pending omitted, gap = 1.
	if plan.ReadyChunksMatched != 2 || plan.PendingOmittedCount != 1 || plan.GapCount != 1 {
		t.Fatalf("unexpected plan counts: %+v", plan)
	}

	// Budget guard must trigger cleanly before download when maxDownloadBytes is tiny.
	if err := cl.MaterializePlan(context.Background(), plan, false, 5); err == nil {
		t.Fatal("expected download cap error")
	}

	if err := cl.MaterializePlan(context.Background(), plan, false, 1024*1024); err != nil {
		t.Fatalf("MaterializePlan failed: %v", err)
	}

	// Offline pinned revision must hit cache with ZERO network requests.
	prevPriv := privHits.Load()
	prevHTTPS := httpsHits.Load()
	offSnap, err := cl.SyncSnapshot(context.Background(), "rev-test-1", true)
	if err != nil {
		t.Fatalf("offline SyncSnapshot failed: %v", err)
	}
	offPlan, err := cl.PlanQuery(offSnap, "usdm", []string{"depth"}, 2000, 4500, true)
	if err != nil {
		t.Fatalf("offline PlanQuery failed: %v", err)
	}
	if offPlan.AlreadyCachedChunks != 2 || offPlan.DownloadChunks != 0 {
		t.Fatalf("expected all chunks already cached offline: %+v", offPlan)
	}
	if err := cl.MaterializePlan(context.Background(), offPlan, true, 0); err != nil {
		t.Fatalf("offline MaterializePlan failed: %v", err)
	}
	if privHits.Load() != prevPriv || httpsHits.Load() != prevHTTPS {
		t.Fatalf("offline pinned flow hit network unexpectedly: priv %d->%d https %d->%d", prevPriv, privHits.Load(), prevHTTPS, httpsHits.Load())
	}
}
