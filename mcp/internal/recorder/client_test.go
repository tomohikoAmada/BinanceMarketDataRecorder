package recorder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
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

func TestM6APlanQueryOrderbookMultiBridgeCrossPartitionAndSymbolFilter(t *testing.T) {
	dataDir := t.TempDir()
	cfg := &config.Config{DataDir: dataDir, HTTPTimeout: 5 * time.Second}
	if err := cfg.EnsureDirectories(); err != nil {
		t.Fatal(err)
	}
	mgr := cache.NewManager(dataDir, &http.Client{Timeout: 5 * time.Second}, 1, 0, nil)
	cl := NewClient(cfg, mgr)

	p0Chunks := filepath.Join(dataDir, "p0_chunks.jsonl")
	p1Chunks := filepath.Join(dataDir, "p1_chunks.jsonl")

	// p0 (closed prior day): holds a pre-midnight BTCUSDT snapshot chunk in rest_observations
	// and a bridging diff_depth chunk, PLUS an ETHUSDT diff_depth chunk that must be filtered out.
	p0Lines := []string{
		`{"chunk_id":"snap-btc-prevday","market":"usdm","symbol":"BTCUSDT","stream":"rest_observations","state":"ARCHIVED_VERIFIED","ready":true,"storage_id":"s1","target_relative_path":"raw/snap.bmdr.zst","external_manifest_relative_path":"manifests/snap.json","archive_url_path":"a/r/snap","manifest_url_path":"a/m/snap","stored_bytes":100,"stored_sha256":"abc","uncompressed_bytes":200,"created_at_utc_ns":9500,"updated_at_utc_ns":9600,"receive_time_utc_range_ns":{"min":850,"max":900},"sequence_ranges":{"lastUpdateId":{"min":500,"max":500}}}`,
		`{"chunk_id":"diff-btc-bridge-1","market":"usdm","symbol":"BTCUSDT","stream":"diff_depth","state":"ARCHIVED_VERIFIED","ready":true,"storage_id":"s1","target_relative_path":"raw/db1.bmdr.zst","external_manifest_relative_path":"manifests/db1.json","archive_url_path":"a/r/db1","manifest_url_path":"a/m/db1","stored_bytes":100,"stored_sha256":"abc","uncompressed_bytes":200,"created_at_utc_ns":900,"updated_at_utc_ns":980,"receive_time_utc_range_ns":{"min":860,"max":990}}`,
		`{"chunk_id":"diff-eth-other","market":"usdm","symbol":"ETHUSDT","stream":"diff_depth","state":"ARCHIVED_VERIFIED","ready":true,"storage_id":"s1","target_relative_path":"raw/de1.bmdr.zst","external_manifest_relative_path":"manifests/de1.json","archive_url_path":"a/r/de1","manifest_url_path":"a/m/de1","stored_bytes":100,"stored_sha256":"abc","uncompressed_bytes":200,"created_at_utc_ns":900,"updated_at_utc_ns":980,"receive_time_utc_range_ns":{"min":860,"max":990}}`,
	}
	// p1 (open current day): holds two bridging diff_depth chunks before startNS=2000, an in-window chunk [2000, 3000),
	// a non-snapshot rest_observations chunk before startNS (without lastUpdateId), and an ACTIVE chunk with stream=null.
	p1Lines := []string{
		`{"chunk_id":"diff-btc-bridge-2","market":"usdm","symbol":"BTCUSDT","stream":"diff_depth","state":"ARCHIVED_VERIFIED","ready":true,"storage_id":"s1","target_relative_path":"raw/db2.bmdr.zst","external_manifest_relative_path":"manifests/db2.json","archive_url_path":"a/r/db2","manifest_url_path":"a/m/db2","stored_bytes":100,"stored_sha256":"abc","uncompressed_bytes":200,"created_at_utc_ns":1000,"updated_at_utc_ns":1500,"receive_time_utc_range_ns":{"min":1000,"max":1499}}`,
		`{"chunk_id":"diff-btc-bridge-3","market":"usdm","symbol":"BTCUSDT","stream":"diff_depth","state":"ARCHIVED_VERIFIED","ready":true,"storage_id":"s1","target_relative_path":"raw/db3.bmdr.zst","external_manifest_relative_path":"manifests/db3.json","archive_url_path":"a/r/db3","manifest_url_path":"a/m/db3","stored_bytes":100,"stored_sha256":"abc","uncompressed_bytes":200,"created_at_utc_ns":1500,"updated_at_utc_ns":1999,"receive_time_utc_range_ns":{"min":1500,"max":1999}}`,
		`{"chunk_id":"diff-btc-window","market":"usdm","symbol":"BTCUSDT","stream":"diff_depth","state":"ARCHIVED_VERIFIED","ready":true,"storage_id":"s1","target_relative_path":"raw/dbw.bmdr.zst","external_manifest_relative_path":"manifests/dbw.json","archive_url_path":"a/r/dbw","manifest_url_path":"a/m/dbw","stored_bytes":100,"stored_sha256":"abc","uncompressed_bytes":200,"created_at_utc_ns":2000,"updated_at_utc_ns":2900,"receive_time_utc_range_ns":{"min":2000,"max":2900}}`,
		`{"chunk_id":"active-unknown-stream","market":"usdm","stream":null,"state":"ACTIVE","ready":false,"pending_reason":"active_chunk","created_at_utc_ns":2800,"updated_at_utc_ns":2950}`,
	}
	if err := os.WriteFile(p0Chunks, []byte(strings.Join(p0Lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p1Chunks, []byte(strings.Join(p1Lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	snap := &SyncedSnapshot{
		RevisionID: "rev-m6a",
		Partitions: []LoadedPartition{
			{
				Manifest:     PartitionManifest{Market: "usdm", PartitionID: "2026-11-14"},
				Metadata:     PartitionMetadata{Market: "usdm", PartitionID: "2026-11-14", State: "closed", ReceiveTimeUTCRangeNS: TimeRangeNS{Min: 100, Max: 999}},
				Availability: AvailabilityMetadata{Market: "usdm", PartitionID: "2026-11-14", RecordedSymbols: []string{"BTCUSDT", "ETHUSDT"}},
				ChunksPath:   p0Chunks,
			},
			{
				Manifest:     PartitionManifest{Market: "usdm", PartitionID: "2026-11-15"},
				Metadata:     PartitionMetadata{Market: "usdm", PartitionID: "2026-11-15", State: "open", ReceiveTimeUTCRangeNS: TimeRangeNS{Min: 1000, Max: 3000}},
				Availability: AvailabilityMetadata{Market: "usdm", PartitionID: "2026-11-15", RecordedSymbols: []string{"BTCUSDT"}},
				ChunksPath:   p1Chunks,
			},
		},
	}

	plan, err := cl.PlanQueryWithOptions(snap, PlanOptions{
		Market:                   "usdm",
		Streams:                  []string{"diff_depth", "rest_observations"},
		Symbols:                  []string{"BTCUSDT", "DOGEUSDT"},
		StartUTCNS:               2000,
		EndUTCNS:                 3000,
		IncludePrecedingBaseline: true,
		OrderbookMode:            true,
	})
	if err != nil {
		t.Fatalf("PlanQueryWithOptions failed: %v", err)
	}

	var gotIDs []string
	for _, pc := range plan.PlannedChunks {
		gotIDs = append(gotIDs, pc.Chunk.ChunkID)
	}
	wantIDs := []string{
		"snap-btc-prevday",
		"diff-btc-bridge-1",
		"diff-btc-bridge-2",
		"diff-btc-bridge-3",
		"diff-btc-window",
	}
	if !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("planned chunk order/set mismatch:\n got=%v\nwant=%v", gotIDs, wantIDs)
	}
	if plan.PendingUnknownStreamCount != 1 || plan.PendingOmittedCount != 1 {
		t.Fatalf("expected active null-stream chunk to be tracked as pending: %+v", plan)
	}
	if !reflect.DeepEqual(plan.UnrecordedSymbols, []string{"DOGEUSDT"}) {
		t.Fatalf("expected DOGEUSDT unrecorded, got %+v", plan.UnrecordedSymbols)
	}
}
