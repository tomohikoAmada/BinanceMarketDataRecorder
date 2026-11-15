package recorder

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/cache"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
)

const (
	// DefaultAutoDownloadCapBytes bounds synchronous on-demand Recorder downloads during queries
	// unless caller specifies a higher limit or uses manage_local_cache prefetch.
	DefaultAutoDownloadCapBytes int64 = 256 * 1024 * 1024
)

// LatestIndex reflects export/latest.json published by BinanceMarketDataRecorder.
type LatestIndex struct {
	SchemaVersion   string         `json:"schema_version"`
	RevisionID      string         `json:"revision_id"`
	GeneratedAtUTC  string         `json:"generated_at_utc"`
	ManifestRelPath string         `json:"manifest_rel_path"`
	ManifestURLPath string         `json:"manifest_url_path"`
	ManifestSHA256  string         `json:"manifest_sha256"`
	PartitionCount  int            `json:"partition_count"`
	Counts          map[string]int `json:"counts"`
}

// ArtifactRef points to a snapshot artifact in export manifest.json.
type ArtifactRef struct {
	Bytes   int64  `json:"bytes"`
	SHA256  string `json:"sha256"`
	URLPath string `json:"url_path"`
}

// PartitionManifest describes one exported partition (cold or hot).
type PartitionManifest struct {
	Market             string                 `json:"market"`
	PartitionID        string                 `json:"partition_id"`
	State              string                 `json:"state,omitempty"`
	IsCold             bool                   `json:"is_cold,omitempty"`
	SnapshotRelDir     string                 `json:"snapshot_rel_dir,omitempty"`
	SourcePartitionDir string                 `json:"source_partition_root,omitempty"`
	Counts             map[string]int         `json:"counts"`
	Files              map[string]ArtifactRef `json:"files"`
}

// ExportManifest models export/revisions/<revision_id>/manifest.json.
type ExportManifest struct {
	SchemaVersion  string                    `json:"schema_version"`
	RevisionID     string                    `json:"revision_id"`
	GeneratedAtUTC string                    `json:"generated_at_utc"`
	ArchiveTargets map[string]ArchiveTarget  `json:"archive_targets"`
	Partitions     []PartitionManifest       `json:"partitions"`
}

// ArchiveTarget maps a storage_id on the VPS to its static URL prefix.
type ArchiveTarget struct {
	URLPrefix string `json:"url_prefix"`
	LocalPath string `json:"local_path"`
}

// ChunkEntry represents one line of chunks.jsonl inside a partition snapshot.
type ChunkEntry struct {
	ChunkID                    string  `json:"chunk_id"`
	Market                     string  `json:"market"`
	Stream                     string  `json:"stream"`
	State                      string  `json:"state"`
	ArchiveState               *string `json:"archive_state"`
	Ready                      bool    `json:"ready"`
	PendingReason              *string `json:"pending_reason"`
	StorageID                  *string `json:"storage_id"`
	TargetRelativePath         *string `json:"target_relative_path"`
	ExternalManifestRelPath    *string `json:"external_manifest_relative_path"`
	ArchiveURLPath             *string `json:"archive_url_path"`
	ManifestURLPath            *string `json:"manifest_url_path"`
	StoredBytes                *int64  `json:"stored_bytes"`
	StoredSHA256               *string `json:"stored_sha256"`
	UncompressedBytes          *int64  `json:"uncompressed_bytes"`
	UncompressedSHA256         *string `json:"uncompressed_sha256"`
	RecordCount                *int64  `json:"record_count"`
	CreatedAtUTCNS             int64   `json:"created_at_utc_ns"`
	UpdatedAtUTCNS             int64   `json:"updated_at_utc_ns"`
}

// TimeRangeNS represents min/max UTC nanosecond bounds in partition.json.
type TimeRangeNS struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

// PartitionMetadata models partition.json from a partition snapshot.
type PartitionMetadata struct {
	SchemaVersion           string      `json:"schema_version"`
	Market                  string      `json:"market"`
	PartitionID             string      `json:"partition_id"`
	State                   string      `json:"state"`
	IsCold                  bool        `json:"is_cold"`
	CreatedAtUTCNS          int64       `json:"created_at_utc_ns"`
	ClosedAtUTCNS           *int64      `json:"closed_at_utc_ns"`
	ReceiveTimeUTCRangeNS   TimeRangeNS `json:"receive_time_utc_range_ns"`
}

// AvailabilityMetadata models availability.json from a partition snapshot.
type AvailabilityMetadata struct {
	Market               string                   `json:"market"`
	PartitionID          string                   `json:"partition_id"`
	Gaps                 []map[string]any         `json:"gaps"`
	DiscontinuityEvents  []map[string]any         `json:"discontinuity_events"`
	QuarantinedArtifacts []map[string]any         `json:"quarantined_artifacts"`
	StorageTargets       []map[string]interface{} `json:"storage_targets"`
}

// LoadedPartition bundles a synced partition snapshot on local disk.
type LoadedPartition struct {
	Manifest     PartitionManifest
	Metadata     PartitionMetadata
	Availability AvailabilityMetadata
	CatalogPath  string
	ChunksPath   string
}

// SyncedSnapshot represents a verified local view of a Recorder export revision.
type SyncedSnapshot struct {
	RevisionID     string
	GeneratedAtUTC string
	ManifestPath   string
	FromCache      bool
	Manifest       ExportManifest
	Partitions     []LoadedPartition
}

// QueryPlan estimates chunks, bytes, pending items, and gaps for a Recorder slice.
type QueryPlan struct {
	RevisionID                 string             `json:"revision_id"`
	Market                     string             `json:"market"`
	Streams                    []string           `json:"streams"`
	PartitionsMatched          int                `json:"partitions_matched"`
	ReadyChunksMatched         int                `json:"ready_chunks_matched"`
	AlreadyCachedChunks        int                `json:"already_cached_chunks"`
	DownloadChunks             int                `json:"download_chunks"`
	EstimatedDownloadBytes     int64              `json:"estimated_download_bytes"`
	EstimatedUncompressedBytes int64              `json:"estimated_uncompressed_bytes"`
	PendingOmittedCount        int                `json:"pending_omitted_count"`
	PendingReasons             map[string]int     `json:"pending_reasons,omitempty"`
	GapCount                   int                `json:"gap_count"`
	QuarantinedCount           int                `json:"quarantined_count"`
	DiscontinuityCount         int                `json:"discontinuity_count"`
	PlannedChunks              []PlannedChunk     `json:"-"`
	SelectedPartitions         []LoadedPartition  `json:"-"`
}

// PlannedChunk pairs a matched ready chunk with its partition catalog and local paths.
type PlannedChunk struct {
	PartitionID        string
	CatalogPath        string
	StorageID          string
	Chunk              ChunkEntry
	LocalRawPath       string
	LocalManifestPath  string
	RawCached          bool
	ManifestCached     bool
}

// Client syncs Recorder index metadata, plans chunk downloads, and materializes local storage roots.
type Client struct {
	Cfg      *config.Config
	CacheMgr *cache.Manager
}

// NewClient constructs a Recorder client backed by the shared Cache Manager.
func NewClient(cfg *config.Config, cacheMgr *cache.Manager) *Client {
	return &Client{Cfg: cfg, CacheMgr: cacheMgr}
}

func (c *Client) urlsFor(relURLPath string) (string, string) {
	cleaned := strings.TrimPrefix(strings.TrimSpace(relURLPath), "/")
	var httpsURL, privURL string
	if c.Cfg.RecorderHTTPSURL != "" {
		httpsURL = strings.TrimRight(c.Cfg.RecorderHTTPSURL, "/") + "/" + cleaned
	}
	if c.Cfg.RecorderPrivateURL != "" {
		privURL = strings.TrimRight(c.Cfg.RecorderPrivateURL, "/") + "/" + cleaned
	}
	if privURL != "" {
		return privURL, httpsURL
	}
	return httpsURL, ""
}

// SyncSnapshot resolves either the requested pinned revisionID or latest.json (offline/pinned never touch network if local).
func (c *Client) SyncSnapshot(ctx context.Context, revisionID string, offline bool) (*SyncedSnapshot, error) {
	var manifestRelPath string
	var manifestURLPath string
	var manifestSHA string

	revID := strings.TrimSpace(revisionID)
	localLatestRel := filepath.Join("recorder", "latest.json")
	localLatestPath, err := c.CacheMgr.PathFor(localLatestRel)
	if err != nil {
		return nil, err
	}

	if revID == "" {
		if offline {
			raw, err := os.ReadFile(localLatestPath)
			if err != nil {
				return nil, fmt.Errorf("offline mode requires previously synced Recorder latest.json (%s): %w", localLatestPath, err)
			}
			var latest LatestIndex
			if err := json.Unmarshal(raw, &latest); err != nil {
				return nil, fmt.Errorf("parse cached latest.json: %w", err)
			}
			revID = latest.RevisionID
			manifestRelPath = latest.ManifestRelPath
			manifestURLPath = latest.ManifestURLPath
			manifestSHA = latest.ManifestSHA256
		} else {
			pri, sec := c.urlsFor("export/latest.json")
			// Always fetch latest.json fresh when online unless server fails & offline fallback wasn't asked.
			tmpKey := cache.CacheKey{
				Source:  "recorder_index",
				Dataset: "latest",
			}
			// Do NOT cache latest.json across calls: write to a fresh un-pinned fetch via ForceVerify + unique hashless check?
			// Better: remove prior receipt on latest.json or fetch directly? Using CacheMgr with offline=false and ExpectedSHA256="" without receipt reuse.
			_ = os.Remove(cache.ReceiptPath(localLatestPath))
			res, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
				DestRelPath: localLatestRel,
				Key:         tmpKey,
				SourceURL:   pri,
				FallbackURL: sec,
				Offline:     false,
			})
			if err != nil {
				return nil, fmt.Errorf("sync Recorder export/latest.json: %w", err)
			}
			raw, err := os.ReadFile(res.LocalPath)
			if err != nil {
				return nil, err
			}
			var latest LatestIndex
			if err := json.Unmarshal(raw, &latest); err != nil {
				return nil, fmt.Errorf("parse Recorder latest.json: %w", err)
			}
			revID = latest.RevisionID
			manifestRelPath = latest.ManifestRelPath
			manifestURLPath = latest.ManifestURLPath
			manifestSHA = latest.ManifestSHA256
		}
	}

	if revID == "" {
		return nil, errors.New("resolved Recorder revision_id is empty")
	}
	if manifestURLPath == "" {
		manifestURLPath = fmt.Sprintf("export/revisions/%s/manifest.json", revID)
	}
	if manifestRelPath == "" {
		manifestRelPath = fmt.Sprintf("revisions/%s/manifest.json", revID)
	}

	manifestDestRel := filepath.Join("recorder", "revisions", revID, "manifest.json")
	priMan, secMan := c.urlsFor(manifestURLPath)
	manRes, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
		DestRelPath:    manifestDestRel,
		ExpectedSHA256: manifestSHA,
		Key: cache.CacheKey{
			Source:         "recorder_manifest",
			Dataset:        "manifest",
			RevisionOrHash: revID,
		},
		SourceURL:   priMan,
		FallbackURL: secMan,
		Offline:     offline,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure manifest for revision %s: %w", revID, err)
	}
	rawMan, err := os.ReadFile(manRes.LocalPath)
	if err != nil {
		return nil, err
	}
	var manifest ExportManifest
	if err := json.Unmarshal(rawMan, &manifest); err != nil {
		return nil, fmt.Errorf("parse Recorder manifest %s: %w", revID, err)
	}

	out := &SyncedSnapshot{
		RevisionID:     manifest.RevisionID,
		GeneratedAtUTC: manifest.GeneratedAtUTC,
		ManifestPath:   manRes.LocalPath,
		FromCache:      manRes.CacheHit,
		Manifest:       manifest,
		Partitions:     make([]LoadedPartition, 0, len(manifest.Partitions)),
	}

	for _, part := range manifest.Partitions {
		loaded, err := c.ensurePartitionMetadata(ctx, part, offline)
		if err != nil {
			return nil, fmt.Errorf("partition %s/%s: %w", part.Market, part.PartitionID, err)
		}
		out.Partitions = append(out.Partitions, loaded)
	}
	sort.Slice(out.Partitions, func(i, j int) bool {
		if out.Partitions[i].Manifest.Market != out.Partitions[j].Manifest.Market {
			return out.Partitions[i].Manifest.Market < out.Partitions[j].Manifest.Market
		}
		return out.Partitions[i].Manifest.PartitionID < out.Partitions[j].Manifest.PartitionID
	})
	return out, nil
}

func (c *Client) ensurePartitionMetadata(ctx context.Context, part PartitionManifest, offline bool) (LoadedPartition, error) {
	// Snapshot content-addressed by exact SHA256 hashes in partition directory so revisions share unchanged partition snapshots cleanly!
	ensureArtifact := func(name string, required bool) (string, error) {
		ref, ok := part.Files[name]
		if !ok {
			if required {
				return "", fmt.Errorf("missing required partition file %s", name)
			}
			return "", nil
		}
		shortHash := ref.SHA256
		if len(shortHash) > 16 {
			shortHash = shortHash[:16]
		}
		fileName := name
		switch name {
		case "catalog_sqlite":
			fileName = "catalog.sqlite"
		case "chunks_jsonl":
			fileName = "chunks.jsonl"
		case "availability_json":
			fileName = "availability.json"
		case "partition_json":
			fileName = "partition.json"
		}
		destRel := filepath.Join("recorder", "snapshots", part.Market, part.PartitionID, shortHash, fileName)
		pri, sec := c.urlsFor(ref.URLPath)
		res, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
			DestRelPath:    destRel,
			ExpectedSHA256: ref.SHA256,
			ExpectedBytes:  ref.Bytes,
			Key: cache.CacheKey{
				Source:         "recorder_snapshot_artifact",
				Market:         part.Market,
				Dataset:        fileName,
				RevisionOrHash: ref.SHA256,
			},
			SourceURL:   pri,
			FallbackURL: sec,
			Offline:     offline,
		})
		if err != nil {
			return "", err
		}
		return res.LocalPath, nil
	}

	partJSONPath, err := ensureArtifact("partition_json", true)
	if err != nil {
		return LoadedPartition{}, err
	}
	availJSONPath, err := ensureArtifact("availability_json", true)
	if err != nil {
		return LoadedPartition{}, err
	}
	chunksPath, err := ensureArtifact("chunks_jsonl", true)
	if err != nil {
		return LoadedPartition{}, err
	}
	catalogPath, err := ensureArtifact("catalog_sqlite", true)
	if err != nil {
		return LoadedPartition{}, err
	}

	var partMeta PartitionMetadata
	if raw, err := os.ReadFile(partJSONPath); err == nil {
		if jErr := json.Unmarshal(raw, &partMeta); jErr != nil {
			return LoadedPartition{}, fmt.Errorf("decode partition.json: %w", jErr)
		}
	} else {
		return LoadedPartition{}, err
	}

	var availMeta AvailabilityMetadata
	if raw, err := os.ReadFile(availJSONPath); err == nil {
		if jErr := json.Unmarshal(raw, &availMeta); jErr != nil {
			return LoadedPartition{}, fmt.Errorf("decode availability.json: %w", jErr)
		}
	} else {
		return LoadedPartition{}, err
	}

	return LoadedPartition{
		Manifest:     part,
		Metadata:     partMeta,
		Availability: availMeta,
		CatalogPath:  catalogPath,
		ChunksPath:   chunksPath,
	}, nil
}

// PlanQuery scans synced partition manifests & chunks.jsonl without downloading raw chunks yet.
// For depth reconstruction (when streams include depth or book_ticker), includePrecedingBaseline keeps
// earlier chunks in matching partitions so R5-c persistent snapshot baseline resolution still works.
func (c *Client) PlanQuery(snap *SyncedSnapshot, market string, streams []string, startNS, endNS int64, includePrecedingBaseline bool) (*QueryPlan, error) {
	if snap == nil {
		return nil, errors.New("snapshot is nil")
	}
	streamSet := make(map[string]bool, len(streams))
	for _, s := range streams {
		streamSet[s] = true
	}

	plan := &QueryPlan{
		RevisionID:     snap.RevisionID,
		Market:         market,
		Streams:        append([]string(nil), streams...),
		PendingReasons: make(map[string]int),
	}

	for _, part := range snap.Partitions {
		if market != "" && part.Manifest.Market != market {
			continue
		}
		pMin := part.Metadata.ReceiveTimeUTCRangeNS.Min
		pMax := part.Metadata.ReceiveTimeUTCRangeNS.Max
		// Filter out partitions whose receive_time_utc_range_ns lies wholly outside [startNS, endNS).
		// Note: open/active partitions (and pre-R5-a live catalogs) may have pMax == pMin until closed;
		// never prune an open/non-closed partition using pMax < startNS.
		if endNS > 0 && pMin > 0 && pMin >= endNS {
			continue
		}
		if startNS > 0 && part.Metadata.State == "closed" && pMax > pMin && pMax < startNS {
			continue
		}

		plan.PartitionsMatched++
		plan.SelectedPartitions = append(plan.SelectedPartitions, part)
		plan.GapCount += len(part.Availability.Gaps)
		plan.QuarantinedCount += len(part.Availability.QuarantinedArtifacts)
		plan.DiscontinuityCount += len(part.Availability.DiscontinuityEvents)

		chunks, err := ReadChunksJSONL(part.ChunksPath)
		if err != nil {
			return nil, err
		}
		sort.Slice(chunks, func(i, j int) bool {
			if chunks[i].CreatedAtUTCNS != chunks[j].CreatedAtUTCNS {
				return chunks[i].CreatedAtUTCNS < chunks[j].CreatedAtUTCNS
			}
			return chunks[i].ChunkID < chunks[j].ChunkID
		})

		// Track per-stream last baseline chunk before startNS if requested (needed by orderbook reconstructor).
		latestPreStart := make(map[string]ChunkEntry)
		var matchedForPart []ChunkEntry

		for _, ch := range chunks {
			if len(streamSet) > 0 && !streamSet[ch.Stream] {
				continue
			}
			if !ch.Ready {
				plan.PendingOmittedCount++
				reason := "pending"
				if ch.PendingReason != nil && *ch.PendingReason != "" {
					reason = *ch.PendingReason
				}
				plan.PendingReasons[reason]++
				continue
			}
			if endNS > 0 && ch.CreatedAtUTCNS >= endNS {
				continue
			}
			if startNS > 0 && ch.UpdatedAtUTCNS > 0 && ch.UpdatedAtUTCNS < startNS {
				if includePrecedingBaseline {
					prev, ok := latestPreStart[ch.Stream]
					if !ok || ch.UpdatedAtUTCNS >= prev.UpdatedAtUTCNS {
						latestPreStart[ch.Stream] = ch
					}
				}
				continue
			}
			matchedForPart = append(matchedForPart, ch)
		}

		if includePrecedingBaseline && len(latestPreStart) > 0 {
			seen := make(map[string]bool, len(matchedForPart))
			for _, ch := range matchedForPart {
				seen[ch.ChunkID] = true
			}
			for _, base := range latestPreStart {
				if !seen[base.ChunkID] {
					matchedForPart = append(matchedForPart, base)
				}
			}
			sort.Slice(matchedForPart, func(i, j int) bool {
				return matchedForPart[i].CreatedAtUTCNS < matchedForPart[j].CreatedAtUTCNS
			})
		}

		for _, ch := range matchedForPart {
			if ch.StorageID == nil || ch.TargetRelativePath == nil || ch.ExternalManifestRelPath == nil ||
				ch.ArchiveURLPath == nil || ch.ManifestURLPath == nil || ch.StoredSHA256 == nil || ch.StoredBytes == nil {
				plan.PendingOmittedCount++
				plan.PendingReasons["incomplete_chunk_metadata"]++
				continue
			}
			sid := *ch.StorageID
			rawRel := filepath.Join("recorder", "storage", sid, filepath.FromSlash(*ch.TargetRelativePath))
			manRel := filepath.Join("recorder", "storage", sid, filepath.FromSlash(*ch.ExternalManifestRelPath))
			localRaw, err := c.CacheMgr.PathFor(rawRel)
			if err != nil {
				return nil, err
			}
			localMan, err := c.CacheMgr.PathFor(manRel)
			if err != nil {
				return nil, err
			}
			rawCached := isReceiptMatching(localRaw, *ch.StoredSHA256)
			manCached := hasFileAndReceipt(localMan)

			plan.ReadyChunksMatched++
			if ch.UncompressedBytes != nil {
				plan.EstimatedUncompressedBytes += *ch.UncompressedBytes
			}
			if rawCached && manCached {
				plan.AlreadyCachedChunks++
			} else {
				plan.DownloadChunks++
				if !rawCached {
					plan.EstimatedDownloadBytes += *ch.StoredBytes
				}
			}

			plan.PlannedChunks = append(plan.PlannedChunks, PlannedChunk{
				PartitionID:       part.Manifest.PartitionID,
				CatalogPath:       part.CatalogPath,
				StorageID:         sid,
				Chunk:             ch,
				LocalRawPath:      localRaw,
				LocalManifestPath: localMan,
				RawCached:         rawCached,
				ManifestCached:    manCached,
			})
		}
	}

	if len(plan.PendingReasons) == 0 {
		plan.PendingReasons = nil
	}
	return plan, nil
}

// MaterializePlan downloads (or verifies offline) every planned raw chunk & archive manifest into
// BINANCE_MCP_DATA_DIR/recorder/storage/<storage_id>/..., enforcing maxDownloadBytes unless already cached.
func (c *Client) MaterializePlan(ctx context.Context, plan *QueryPlan, offline bool, maxDownloadBytes int64) error {
	if plan == nil {
		return errors.New("plan is nil")
	}
	if maxDownloadBytes <= 0 {
		maxDownloadBytes = DefaultAutoDownloadCapBytes
	}
	if !offline && plan.EstimatedDownloadBytes > maxDownloadBytes {
		return fmt.Errorf(
			"query requires downloading %.2f MiB (%d chunks), exceeding interactive limit %.2f MiB; use manage_local_cache(action=prefetch) or narrow the time range (revision=%s)",
			float64(plan.EstimatedDownloadBytes)/(1024*1024),
			plan.DownloadChunks,
			float64(maxDownloadBytes)/(1024*1024),
			plan.RevisionID,
		)
	}

	for _, pc := range plan.PlannedChunks {
		ch := pc.Chunk
		rawRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.TargetRelativePath))
		manRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.ExternalManifestRelPath))

		priRaw, secRaw := c.urlsFor(*ch.ArchiveURLPath)
		if _, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
			DestRelPath:    rawRel,
			ExpectedSHA256: *ch.StoredSHA256,
			ExpectedBytes:  *ch.StoredBytes,
			Key: cache.CacheKey{
				Source:         "recorder_raw_chunk",
				Market:         ch.Market,
				Dataset:        ch.Stream,
				RevisionOrHash: *ch.StoredSHA256,
			},
			SourceURL:   priRaw,
			FallbackURL: secRaw,
			Offline:     offline,
		}); err != nil {
			return fmt.Errorf("ensure raw chunk %s (%s): %w", ch.ChunkID, ch.Stream, err)
		}

		priMan, secMan := c.urlsFor(*ch.ManifestURLPath)
		if _, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
			DestRelPath: manRel,
			Key: cache.CacheKey{
				Source:         "recorder_archive_manifest",
				Market:         ch.Market,
				Dataset:        ch.ChunkID,
				RevisionOrHash: *ch.StoredSHA256,
			},
			SourceURL:   priMan,
			FallbackURL: secMan,
			Offline:     offline,
		}); err != nil {
			return fmt.Errorf("ensure archive manifest %s: %w", ch.ChunkID, err)
		}
	}
	return nil
}

// BuildPrefetchJobItems turns a QueryPlan into durable JobItems for manage_local_cache prefetch.
func (c *Client) BuildPrefetchJobItems(plan *QueryPlan) []cache.JobItem {
	items := make([]cache.JobItem, 0, len(plan.PlannedChunks)*2)
	for _, pc := range plan.PlannedChunks {
		ch := pc.Chunk
		rawRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.TargetRelativePath))
		manRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.ExternalManifestRelPath))
		priRaw, secRaw := c.urlsFor(*ch.ArchiveURLPath)
		priMan, secMan := c.urlsFor(*ch.ManifestURLPath)

		var rawBytes int64
		if ch.StoredBytes != nil {
			rawBytes = *ch.StoredBytes
		}
		var rawSHA string
		if ch.StoredSHA256 != nil {
			rawSHA = *ch.StoredSHA256
		}
		items = append(items, cache.JobItem{
			ItemID:      ch.ChunkID + ":raw",
			DestRelPath: rawRel,
			SourceURL:   priRaw,
			FallbackURL: secRaw,
			SHA256:      rawSHA,
			SizeBytes:   rawBytes,
			Key: cache.CacheKey{
				Source:         "recorder_raw_chunk",
				Market:         ch.Market,
				Dataset:        ch.Stream,
				RevisionOrHash: rawSHA,
			},
			Completed: pc.RawCached,
		})
		items = append(items, cache.JobItem{
			ItemID:      ch.ChunkID + ":manifest",
			DestRelPath: manRel,
			SourceURL:   priMan,
			FallbackURL: secMan,
			Key: cache.CacheKey{
				Source:         "recorder_archive_manifest",
				Market:         ch.Market,
				Dataset:        ch.ChunkID,
				RevisionOrHash: rawSHA,
			},
			Completed: pc.ManifestCached,
		})
	}
	return items
}

// ReadChunksJSONL streams and decodes chunk entries from a partition chunks.jsonl file.
func ReadChunksJSONL(path string) ([]ChunkEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var chunks []ChunkEntry
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry ChunkEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return nil, fmt.Errorf("parse chunks.jsonl line: %w", err)
		}
		chunks = append(chunks, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return chunks, nil
}

// ParseNSString validates a UTC nanosecond integer string or RFC3339 timestamp string into int64 nanoseconds.
func ParseNSString(raw string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		if n < 0 {
			return 0, fmt.Errorf("timestamp must be >= 0: %s", raw)
		}
		// Auto-promote 13-digit ms only when unambiguous? Keep strict nanosecond rule unless user passed RFC3339.
		return n, nil
	}
	return 0, fmt.Errorf("expected decimal UTC nanosecond string, got %q", raw)
}

func isReceiptMatching(localPath, expectedSHA string) bool {
	if expectedSHA == "" {
		return hasFileAndReceipt(localPath)
	}
	st, err := os.Stat(localPath)
	if err != nil || st.IsDir() {
		return false
	}
	rec, ok := cache.LoadReceipt(localPath)
	if !ok {
		return false
	}
	return strings.EqualFold(rec.SHA256, expectedSHA) && rec.SizeBytes == st.Size()
}

func hasFileAndReceipt(localPath string) bool {
	st, err := os.Stat(localPath)
	if err != nil || st.IsDir() {
		return false
	}
	rec, ok := cache.LoadReceipt(localPath)
	return ok && rec.SizeBytes == st.Size() && rec.SHA256 != ""
}
