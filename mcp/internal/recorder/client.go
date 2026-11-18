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
	"sync"

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
	ChunkID                    string                 `json:"chunk_id"`
	Market                     string                 `json:"market"`
	Symbol                     *string                `json:"symbol,omitempty"`
	Stream                     string                 `json:"stream"`
	State                      string                 `json:"state"`
	ArchiveState               *string                `json:"archive_state"`
	Ready                      bool                   `json:"ready"`
	PendingReason              *string                `json:"pending_reason"`
	StorageID                  *string                `json:"storage_id"`
	TargetRelativePath         *string                `json:"target_relative_path"`
	ExternalManifestRelPath    *string                `json:"external_manifest_relative_path"`
	ArchiveURLPath             *string                `json:"archive_url_path"`
	ManifestURLPath            *string                `json:"manifest_url_path"`
	StoredBytes                *int64                 `json:"stored_bytes"`
	StoredSHA256               *string                `json:"stored_sha256"`
	UncompressedBytes          *int64                 `json:"uncompressed_bytes"`
	UncompressedSHA256         *string                `json:"uncompressed_sha256"`
	RecordCount                *int64                 `json:"record_count"`
	CreatedAtUTCNS             int64                  `json:"created_at_utc_ns"`
	UpdatedAtUTCNS             int64                  `json:"updated_at_utc_ns"`
	ReceiveTimeUTCRangeNS      *TimeRangeNS           `json:"receive_time_utc_range_ns,omitempty"`
	ExchangeTimeRanges         map[string]TimeRangeNS `json:"exchange_time_ranges,omitempty"`
	SequenceRanges             map[string]any         `json:"sequence_ranges,omitempty"`
	ConnectionIDs              []string               `json:"connection_ids,omitempty"`
}

// ReceiveTimeRangeNS returns the chunk's receive_time_utc_range_ns bounds in UTC nanoseconds.
func (ch ChunkEntry) ReceiveTimeRangeNS() (int64, int64, bool) {
	if ch.ReceiveTimeUTCRangeNS != nil && ch.ReceiveTimeUTCRangeNS.Max > 0 {
		return ch.ReceiveTimeUTCRangeNS.Min, ch.ReceiveTimeUTCRangeNS.Max, true
	}
	return 0, 0, false
}

// ExchangeTimeRangeNS returns the chunk's exchange_time_ranges bounds scaled to UTC nanoseconds.
func (ch ChunkEntry) ExchangeTimeRangeNS() (int64, int64, bool) {
	var have bool
	var minNS, maxNS int64
	for _, r := range ch.ExchangeTimeRanges {
		if r.Max <= 0 {
			continue
		}
		rMin := r.Min * 1_000_000
		rMax := r.Max * 1_000_000
		if !have {
			minNS = rMin
			maxNS = rMax
			have = true
		} else {
			if rMin > 0 && (minNS == 0 || rMin < minNS) {
				minNS = rMin
			}
			if rMax > maxNS {
				maxNS = rMax
			}
		}
	}
	return minNS, maxNS, have
}

// SequenceUMax returns the chunk's max diff_depth final update ID (`sequence_ranges.u.max`) if present.
func (ch ChunkEntry) SequenceUMax() (int64, bool) {
	_, maxU, _, hasMax := ch.SequenceURange()
	return maxU, hasMax
}

// SequenceURange returns the chunk's diff_depth final update ID bounds (`sequence_ranges.u.{min,max}`) if present.
func (ch ChunkEntry) SequenceURange() (minU int64, maxU int64, hasMin bool, hasMax bool) {
	if len(ch.SequenceRanges) == 0 {
		return 0, 0, false, false
	}
	uRaw, ok := ch.SequenceRanges["u"].(map[string]any)
	if !ok {
		return 0, 0, false, false
	}
	minU, hasMin = parseSequenceAnyInt64(uRaw["min"])
	maxU, hasMax = parseSequenceAnyInt64(uRaw["max"])
	return minU, maxU, hasMin, hasMax
}

func parseSequenceAnyInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case float64:
		if x >= 0 {
			return int64(x), true
		}
	case int64:
		if x >= 0 {
			return x, true
		}
	case int:
		if x >= 0 {
			return int64(x), true
		}
	case json.Number:
		if n, err := x.Int64(); err == nil && n >= 0 {
			return n, true
		}
	case string:
		if n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil && n >= 0 {
			return n, true
		}
	}
	return 0, false
}

// EffectiveTimeRangeNS returns the chunk's data time range in UTC nanoseconds when available.
// When preferExchange is true and exchange bounds exist, it builds the hull over receive and exchange bounds.
func (ch ChunkEntry) EffectiveTimeRangeNS(preferExchange bool) (int64, int64, bool) {
	minNS, maxNS, have := ch.ReceiveTimeRangeNS()
	if preferExchange {
		exMin, exMax, hasEx := ch.ExchangeTimeRangeNS()
		if hasEx {
			if !have {
				return exMin, exMax, true
			}
			if exMin > 0 && (minNS == 0 || exMin < minNS) {
				minNS = exMin
			}
			if exMax > maxNS {
				maxNS = exMax
			}
		}
	}
	return minNS, maxNS, have
}

// SymbolName returns the normalized upper-case symbol name if known.
func (ch ChunkEntry) SymbolName() string {
	if ch.Symbol == nil {
		return ""
	}
	return strings.ToUpper(strings.TrimSpace(*ch.Symbol))
}

// IsProvenNonDepthSnapshotREST returns true only when sequence_ranges.sourceStream metadata
// explicitly proves a rest_observations chunk contains no depth_snapshot records.
func (ch ChunkEntry) IsProvenNonDepthSnapshotREST() bool {
	if ch.Stream != "rest_observations" || len(ch.SequenceRanges) == 0 {
		return false
	}
	if _, ok := ch.SequenceRanges["lastUpdateId"]; ok {
		return false
	}
	if _, ok := ch.SequenceRanges["depthSnapshotEpoch"]; ok {
		return false
	}
	if ssRaw, ok := ch.SequenceRanges["sourceStream"].(map[string]any); ok {
		minS, _ := ssRaw["min"].(string)
		maxS, _ := ssRaw["max"].(string)
		if minS != "" && maxS != "" {
			return !(minS <= "depth_snapshot" && "depth_snapshot" <= maxS)
		}
	}
	return false
}

// HasDepthSnapshotMetadata reports whether sequence_ranges permits a rest_observations chunk to contain a depth snapshot.
func (ch ChunkEntry) HasDepthSnapshotMetadata() bool {
	if ch.Stream != "rest_observations" {
		return false
	}
	return !ch.IsProvenNonDepthSnapshotREST()
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
	RecordedSymbols      []string                 `json:"recorded_symbols,omitempty"`
	RecordedStreams      []string                 `json:"recorded_streams,omitempty"`
	Gaps                 []map[string]any         `json:"gaps"`
	DiscontinuityEvents  []map[string]any         `json:"discontinuity_events"`
	QuarantinedArtifacts []map[string]any         `json:"quarantined_artifacts"`
	StorageTargets       []map[string]interface{} `json:"storage_targets"`
}

// LoadedPartition bundles a synced partition snapshot on local disk.
type LoadedPartition struct {
	Manifest      PartitionManifest
	Metadata      PartitionMetadata
	Availability  AvailabilityMetadata
	CatalogPath   string
	ChunksPath    string
	ChunksLoadErr error
}

// SyncOptions scopes metadata syncing to the needed market and time window while deferring large catalogs.
type SyncOptions struct {
	Market             string
	StartUTCNS         int64
	EndUTCNS           int64
	PreferExchangeTime bool
}

// OrderbookBaselineHint passes resolved Stage 1 depth_snapshot metadata into Stage 2 diff_depth planning.
type OrderbookBaselineHint struct {
	ReceiveTimeUTCNS int64  `json:"receive_time_utc_ns"`
	LastUpdateID     int64  `json:"last_update_id"`
	SourceChunkID    string `json:"source_chunk_id"`
}

// DownloadBudget tracks a shared byte transfer cap across multiple materialization stages.
type DownloadBudget struct {
	MaxBytes   int64
	SpentBytes int64
}

func (b *DownloadBudget) Remaining() int64 {
	if b == nil || b.MaxBytes <= 0 {
		return 0
	}
	rem := b.MaxBytes - b.SpentBytes
	if rem < 0 {
		return 0
	}
	return rem
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

// PlanOptions configures M6-A metadata-aware Recorder slice planning.
type PlanOptions struct {
	Market                   string
	Streams                  []string
	Symbols                  []string
	StartUTCNS               int64
	EndUTCNS                 int64
	IncludePrecedingBaseline bool
	PreferExchangeTime       bool
	OrderbookMode            bool
	BaselineSnapshots        map[string]OrderbookBaselineHint
}

// QueryPlan estimates chunks, bytes, pending items, and gaps for a Recorder slice.
type QueryPlan struct {
	RevisionID                  string                           `json:"revision_id"`
	Market                      string                           `json:"market"`
	Streams                     []string                         `json:"streams"`
	RequestedSymbols            []string                         `json:"requested_symbols,omitempty"`
	RecordedSymbols             []string                         `json:"recorded_symbols,omitempty"`
	UnrecordedSymbols           []string                         `json:"unrecorded_symbols,omitempty"`
	RequestedStartUTCNS         int64                            `json:"requested_start_utc_ns,omitempty"`
	RequestedEndUTCNS           int64                            `json:"requested_end_utc_ns,omitempty"`
	BaselineStartUTCNS          int64                            `json:"baseline_start_utc_ns,omitempty"`
	BaselineStatus              string                           `json:"baseline_status,omitempty"`
	WarmupConfirmed             bool                             `json:"warmup_confirmed,omitempty"`
	BaselineSnapshots           map[string]OrderbookBaselineHint `json:"baseline_snapshots,omitempty"`
	MissingProbeChunks          []string                         `json:"missing_probe_chunks,omitempty"`
	EstimatedProbeDownloadBytes int64                            `json:"estimated_probe_download_bytes,omitempty"`
	PartitionsMatched           int                              `json:"partitions_matched"`
	ReadyChunksMatched          int                              `json:"ready_chunks_matched"`
	AlreadyCachedChunks         int                              `json:"already_cached_chunks"`
	DownloadChunks              int                              `json:"download_chunks"`
	EstimatedDownloadBytes      int64                            `json:"estimated_download_bytes"`
	EstimatedRawBytes           int64                            `json:"estimated_raw_bytes"`
	EstimatedCatalogBytes       int64                            `json:"estimated_catalog_bytes"`
	EstimatedUncompressedBytes  int64                            `json:"estimated_uncompressed_bytes"`
	MissingCatalogCount         int                              `json:"missing_catalog_count"`
	MissingManifestCount        int                              `json:"missing_manifest_count"`
	UnknownSizeManifestCount    int                              `json:"unknown_size_manifest_count"`
	TotalJobItems               int                              `json:"total_job_items"`
	PartialFileCount            int                              `json:"partial_file_count,omitempty"`
	PartialBytes                int64                            `json:"partial_bytes,omitempty"`
	HTTPBytesDownloaded         int64                            `json:"http_bytes_downloaded,omitempty"`
	PendingOmittedCount         int                              `json:"pending_omitted_count"`
	PendingUnknownStreamCount   int                              `json:"pending_unknown_stream_count,omitempty"`
	PendingReasons              map[string]int                   `json:"pending_reasons,omitempty"`
	GapCount                    int                              `json:"gap_count"`
	QuarantinedCount            int                              `json:"quarantined_count"`
	DiscontinuityCount          int                              `json:"discontinuity_count"`
	HasSymbolMetadata           bool                             `json:"has_symbol_metadata,omitempty"`
	HasTimeRangeMetadata        bool                             `json:"has_time_range_metadata,omitempty"`
	SelectionTimeBasis          string                           `json:"selection_time_basis,omitempty"`
	HasExchangeTimeMetadata     bool                             `json:"has_exchange_time_metadata,omitempty"`
	UnknownExchangeRangeChunks  int                              `json:"unknown_exchange_range_chunks,omitempty"`
	OmittedUnknownOfflineChunks int                              `json:"omitted_unknown_offline_chunks,omitempty"`
	PlannedChunks               []PlannedChunk                   `json:"-"`
	SelectedPartitions          []LoadedPartition                `json:"-"`
	MatchedPartitionIDs         map[string]bool                  `json:"-"`
}

// PlannedChunk pairs a matched ready chunk with its partition catalog and local paths.
type PlannedChunk struct {
	PartitionID         string
	CatalogPath         string
	StorageID           string
	Chunk               ChunkEntry
	LocalRawPath        string
	LocalManifestPath   string
	RawCached           bool
	ManifestCached      bool
	ConservativeUnknown bool
}

// Client syncs Recorder index metadata, plans chunk downloads, and materializes local storage roots.
type Client struct {
	Cfg        *config.Config
	CacheMgr   *cache.Manager
	workerOnce sync.Once
	workerSem  chan struct{}
}

// NewClient constructs a Recorder client backed by the shared Cache Manager.
func NewClient(cfg *config.Config, cacheMgr *cache.Manager) *Client {
	return &Client{
		Cfg:       cfg,
		CacheMgr:  cacheMgr,
		workerSem: make(chan struct{}, 1),
	}
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
	return c.SyncSnapshotWithOptions(ctx, revisionID, offline, SyncOptions{})
}

// SyncSnapshotWithOptions resolves metadata scoped by SyncOptions, deferring large catalog.sqlite downloads to selected partitions.
func (c *Client) SyncSnapshotWithOptions(ctx context.Context, revisionID string, offline bool, opts SyncOptions) (*SyncedSnapshot, error) {
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
			res, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
				DestRelPath:  localLatestRel,
				Key:          tmpKey,
				SourceURL:    pri,
				FallbackURL:  sec,
				Offline:      false,
				ForceRefresh: true,
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

	targetMarket := strings.ToLower(strings.TrimSpace(opts.Market))
	for _, part := range manifest.Partitions {
		if targetMarket != "" && strings.ToLower(strings.TrimSpace(part.Market)) != targetMarket {
			continue
		}
		loaded, err := c.ensurePartitionMetadata(ctx, part, offline, opts)
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

func (c *Client) partitionArtifactDest(part PartitionManifest, name string) (string, ArtifactRef, bool, error) {
	ref, ok := part.Files[name]
	if !ok {
		return "", ArtifactRef{}, false, nil
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
	localPath, err := c.CacheMgr.PathFor(destRel)
	if err != nil {
		return "", ArtifactRef{}, false, err
	}
	return localPath, ref, true, nil
}

func (c *Client) ensurePartitionArtifact(ctx context.Context, part PartitionManifest, name string, required bool, offline bool) (string, error) {
	res, err := c.ensurePartitionArtifactResult(ctx, part, name, required, offline)
	if err != nil || res == nil {
		return "", err
	}
	return res.LocalPath, nil
}

func (c *Client) ensurePartitionArtifactResult(ctx context.Context, part PartitionManifest, name string, required bool, offline bool) (*cache.EnsureResult, error) {
	ref, ok := part.Files[name]
	if !ok {
		if required {
			return nil, fmt.Errorf("missing required partition file %s", name)
		}
		return nil, nil
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
		return nil, err
	}
	return res, nil
}

func (c *Client) ensurePartitionMetadata(ctx context.Context, part PartitionManifest, offline bool, opts SyncOptions) (LoadedPartition, error) {
	partJSONPath, err := c.ensurePartitionArtifact(ctx, part, "partition_json", true, offline)
	if err != nil {
		return LoadedPartition{}, err
	}
	availJSONPath, err := c.ensurePartitionArtifact(ctx, part, "availability_json", true, offline)
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

	// Compute deterministic catalog path lazily without downloading large catalog.sqlite during index sync.
	catalogPath, _, hasCat, pathErr := c.partitionArtifactDest(part, "catalog_sqlite")
	if pathErr != nil {
		return LoadedPartition{}, pathErr
	}
	if !hasCat {
		return LoadedPartition{}, fmt.Errorf("missing required partition file catalog_sqlite")
	}

	var chunksPath string
	var chunksErr error
	pMin := partMeta.ReceiveTimeUTCRangeNS.Min
	skipFutureReceiveChunks := opts.EndUTCNS > 0 && !opts.PreferExchangeTime && pMin > 0 && pMin >= opts.EndUTCNS
	if !skipFutureReceiveChunks {
		chunksPath, chunksErr = c.ensurePartitionArtifact(ctx, part, "chunks_jsonl", true, offline)
		if chunksErr != nil {
			if cp, _, ok, pErr := c.partitionArtifactDest(part, "chunks_jsonl"); pErr == nil && ok {
				chunksPath = cp
			}
		}
	} else {
		chunksErr = fmt.Errorf("chunks.jsonl skipped during sync because partition receive start (%d) >= query end (%d)", pMin, opts.EndUTCNS)
	}

	return LoadedPartition{
		Manifest:      part,
		Metadata:      partMeta,
		Availability:  availMeta,
		CatalogPath:   catalogPath,
		ChunksPath:    chunksPath,
		ChunksLoadErr: chunksErr,
	}, nil
}

// PlanQuery scans synced partition manifests & chunks.jsonl without downloading raw chunks yet.
// For legacy compatibility, this calls PlanQueryWithOptions without symbol filtering.
func (c *Client) PlanQuery(snap *SyncedSnapshot, market string, streams []string, startNS, endNS int64, includePrecedingBaseline bool) (*QueryPlan, error) {
	return c.PlanQueryWithOptions(snap, PlanOptions{
		Market:                   market,
		Streams:                  streams,
		StartUTCNS:               startNS,
		EndUTCNS:                 endNS,
		IncludePrecedingBaseline: includePrecedingBaseline,
	})
}

// PlanQueryWithOptions selects partitions & chunks using data timestamps, symbol filters, and orderbook baseline rules.
func (c *Client) PlanQueryWithOptions(snap *SyncedSnapshot, opts PlanOptions) (*QueryPlan, error) {
	if snap == nil {
		return nil, errors.New("snapshot is nil")
	}
	streamSet := make(map[string]bool, len(opts.Streams))
	for _, s := range opts.Streams {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			streamSet[trimmed] = true
		}
	}
	symbolSet := make(map[string]bool, len(opts.Symbols))
	var normalizedSyms []string
	for _, sym := range opts.Symbols {
		u := strings.ToUpper(strings.TrimSpace(sym))
		if u != "" && !symbolSet[u] {
			symbolSet[u] = true
			normalizedSyms = append(normalizedSyms, u)
		}
	}
	sort.Strings(normalizedSyms)

	selectionBasis := "receive_time_utc"
	if opts.PreferExchangeTime {
		selectionBasis = "exchange_time"
	}
	plan := &QueryPlan{
		RevisionID:          snap.RevisionID,
		Market:              opts.Market,
		Streams:             append([]string(nil), opts.Streams...),
		RequestedSymbols:    normalizedSyms,
		RequestedStartUTCNS: opts.StartUTCNS,
		RequestedEndUTCNS:   opts.EndUTCNS,
		PendingReasons:      make(map[string]int),
		SelectionTimeBasis:  selectionBasis,
		MatchedPartitionIDs: make(map[string]bool),
	}

	type candPart struct {
		part                 LoadedPartition
		baselineOnly         bool
		exchangeBoundaryOnly bool
	}
	var marketParts []LoadedPartition
	for _, part := range snap.Partitions {
		if opts.Market != "" && part.Manifest.Market != opts.Market {
			continue
		}
		marketParts = append(marketParts, part)
	}
	sort.Slice(marketParts, func(i, j int) bool {
		pi, pj := marketParts[i].Metadata.ReceiveTimeUTCRangeNS.Min, marketParts[j].Metadata.ReceiveTimeUTCRangeNS.Min
		if pi != pj {
			return pi < pj
		}
		return marketParts[i].Manifest.PartitionID < marketParts[j].Manifest.PartitionID
	})

	var selected []candPart
	var precedingClosedIdxs []int
	for i, part := range marketParts {
		pMin := part.Metadata.ReceiveTimeUTCRangeNS.Min
		pMax := part.Metadata.ReceiveTimeUTCRangeNS.Max
		if opts.EndUTCNS > 0 && pMin > 0 && pMin >= opts.EndUTCNS {
			if !opts.PreferExchangeTime {
				continue
			}
			// Never drop based on fixed receive tolerance when PreferExchangeTime is active; inspect chunks safely.
			selected = append(selected, candPart{part: part, exchangeBoundaryOnly: true})
			continue
		}
		if opts.StartUTCNS > 0 && part.Metadata.State == "closed" && pMax > pMin && pMax < opts.StartUTCNS {
			precedingClosedIdxs = append(precedingClosedIdxs, i)
			if opts.PreferExchangeTime && !opts.IncludePrecedingBaseline {
				selected = append(selected, candPart{part: part, exchangeBoundaryOnly: true})
			}
			continue
		}
		selected = append(selected, candPart{part: part, baselineOnly: false})
	}
	if opts.IncludePrecedingBaseline && opts.StartUTCNS > 0 && len(precedingClosedIdxs) > 0 {
		if !opts.OrderbookMode {
			lastIdx := precedingClosedIdxs[len(precedingClosedIdxs)-1]
			selected = append([]candPart{{part: marketParts[lastIdx], baselineOnly: true}}, selected...)
		} else {
			satisfied := make(map[string]bool)
			requiredFloor := func(sym string) int64 {
				if len(opts.BaselineSnapshots) == 0 {
					return 0
				}
				if b, ok := opts.BaselineSnapshots[strings.ToUpper(sym)]; ok && b.ReceiveTimeUTCNS > 0 {
					return b.ReceiveTimeUTCNS
				}
				if b, ok := opts.BaselineSnapshots["*"]; ok && b.ReceiveTimeUTCNS > 0 {
					return b.ReceiveTimeUTCNS
				}
				return 0
			}
			allSymsSatisfied := func() bool {
				if satisfied["*"] && len(opts.BaselineSnapshots) == 0 {
					return true
				}
				if len(symbolSet) == 0 {
					return satisfied["*"]
				}
				for sym := range symbolSet {
					if !satisfied[sym] && !satisfied["*"] {
						return false
					}
				}
				return true
			}
			checkPartSnapshots := func(p LoadedPartition, requireStrictPre bool) error {
				if p.ChunksLoadErr != nil {
					return fmt.Errorf("partition %s/%s: %w", p.Manifest.Market, p.Manifest.PartitionID, p.ChunksLoadErr)
				}
				chunks, cErr := ReadChunksJSONL(p.ChunksPath)
				if cErr != nil {
					return nil
				}
				for _, ch := range chunks {
					if !ch.Ready || ch.Stream != "rest_observations" || !ch.HasDepthSnapshotMetadata() {
						continue
					}
					_, cMax, hasRange := ch.EffectiveTimeRangeNS(false)
					if requireStrictPre {
						strictPre := (hasRange && cMax > 0 && cMax < opts.StartUTCNS) || (!hasRange && ch.UpdatedAtUTCNS > 0 && ch.UpdatedAtUTCNS < opts.StartUTCNS)
						if !strictPre {
							continue
						}
					}
					sym := ch.SymbolName()
					if sym == "" || sym == "__GLOBAL__" {
						floor := requiredFloor("*")
						if floor == 0 || !hasRange || p.Metadata.ReceiveTimeUTCRangeNS.Min <= floor {
							satisfied["*"] = true
						}
					} else if len(symbolSet) == 0 || symbolSet[sym] {
						floor := requiredFloor(sym)
						if floor == 0 || !hasRange || cMax >= floor || p.Metadata.ReceiveTimeUTCRangeNS.Min <= floor {
							if floor == 0 || p.Metadata.ReceiveTimeUTCRangeNS.Min <= floor {
								satisfied[sym] = true
							}
						}
					}
				}
				return nil
			}
			// Check whether already-selected window partitions definitely contain a strict pre-start snapshot baseline.
			for _, sp := range selected {
				if sp.exchangeBoundaryOnly {
					continue
				}
				if err := checkPartSnapshots(sp.part, len(opts.BaselineSnapshots) == 0); err != nil {
					return nil, err
				}
			}
			if !allSymsSatisfied() {
				var prependParts []candPart
				for k := len(precedingClosedIdxs) - 1; k >= 0; k-- {
					p := marketParts[precedingClosedIdxs[k]]
					prependParts = append(prependParts, candPart{part: p, baselineOnly: true})
					if err := checkPartSnapshots(p, false); err != nil {
						return nil, err
					}
					if allSymsSatisfied() {
						break
					}
				}
				// Prepend in ascending chronological order (reverse of walk-back).
				for i, j := 0, len(prependParts)-1; i < j; i, j = i+1, j-1 {
					prependParts[i], prependParts[j] = prependParts[j], prependParts[i]
				}
				selected = append(prependParts, selected...)
			}
		}
	}

	recordedSymSet := make(map[string]bool)
	type chunkCand struct {
		partID               string
		catalogPath          string
		ch                   ChunkEntry
		localRaw             string
		localMan             string
		rawCached            bool
		manCached            bool
		minNS                int64
		maxNS                int64
		hasDataRange         bool
		hasSourceRange       bool
		conservativeUnknown  bool
		baselineOnly         bool
		exchangeBoundaryOnly bool
	}
	var allCandidates []chunkCand
	partByID := make(map[string]LoadedPartition, len(selected))
	partIncludedInPlan := make(map[string]bool, len(selected))

	for _, sp := range selected {
		part := sp.part
		partByID[part.Manifest.PartitionID] = part
		if !sp.baselineOnly && !sp.exchangeBoundaryOnly {
			plan.PartitionsMatched++
			plan.GapCount += len(part.Availability.Gaps)
			plan.QuarantinedCount += len(part.Availability.QuarantinedArtifacts)
			plan.DiscontinuityCount += len(part.Availability.DiscontinuityEvents)
			plan.SelectedPartitions = append(plan.SelectedPartitions, part)
			partIncludedInPlan[part.Manifest.PartitionID] = true
		} else if sp.baselineOnly {
			plan.SelectedPartitions = append(plan.SelectedPartitions, part)
			partIncludedInPlan[part.Manifest.PartitionID] = true
		}
		for _, sym := range part.Availability.RecordedSymbols {
			u := strings.ToUpper(strings.TrimSpace(sym))
			if u != "" && u != "__GLOBAL__" {
				recordedSymSet[u] = true
			}
		}

		if part.ChunksLoadErr != nil {
			return nil, fmt.Errorf("partition %s/%s: %w", part.Manifest.Market, part.Manifest.PartitionID, part.ChunksLoadErr)
		}
		chunks, err := ReadChunksJSONL(part.ChunksPath)
		if err != nil {
			return nil, err
		}

		for _, ch := range chunks {
			if !ch.Ready {
				if len(streamSet) > 0 && ch.Stream != "" && !streamSet[ch.Stream] {
					continue
				}
				var cMin, cMax int64
				var hasRange bool
				if opts.PreferExchangeTime {
					cMin, cMax, hasRange = ch.ExchangeTimeRangeNS()
				} else {
					cMin, cMax, hasRange = ch.ReceiveTimeRangeNS()
				}
				if hasRange {
					if opts.EndUTCNS > 0 && cMin >= opts.EndUTCNS {
						continue
					}
					if opts.StartUTCNS > 0 && cMax > 0 && cMax < opts.StartUTCNS {
						continue
					}
				} else if sp.baselineOnly || (sp.exchangeBoundaryOnly && !opts.PreferExchangeTime) {
					continue
				}
				plan.PendingOmittedCount++
				if ch.Stream == "" {
					plan.PendingUnknownStreamCount++
				}
				reason := "pending"
				if ch.PendingReason != nil && *ch.PendingReason != "" {
					reason = *ch.PendingReason
				}
				plan.PendingReasons[reason]++
				continue
			}

			if len(streamSet) > 0 && !streamSet[ch.Stream] {
				continue
			}
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

			if ch.ReceiveTimeUTCRangeNS == nil || ch.Symbol == nil || (opts.PreferExchangeTime && len(ch.ExchangeTimeRanges) == 0) || (opts.OrderbookMode && (len(ch.SequenceRanges) == 0 || len(ch.ConnectionIDs) == 0)) {
				hydrateChunkMetadataFromLocalManifest(&ch, localMan)
			}
			if sym := ch.SymbolName(); sym != "" && sym != "__GLOBAL__" {
				recordedSymSet[sym] = true
			}
			if len(symbolSet) > 0 {
				sym := ch.SymbolName()
				if sym != "" && sym != "__GLOBAL__" && !symbolSet[sym] {
					continue
				}
			}

			recvMin, recvMax, hasRecv := ch.ReceiveTimeRangeNS()
			exMin, exMax, hasEx := ch.ExchangeTimeRangeNS()
			minNS, maxNS, hasDataRange := recvMin, recvMax, hasRecv
			hasSourceRange := hasRecv
			conservativeUnknown := false
			if opts.PreferExchangeTime {
				if hasEx {
					minNS, maxNS, hasDataRange, hasSourceRange = exMin, exMax, true, true
				} else {
					hasDataRange = false
					hasSourceRange = false
					conservativeUnknown = true
				}
			}
			allCandidates = append(allCandidates, chunkCand{
				partID:               part.Manifest.PartitionID,
				catalogPath:          part.CatalogPath,
				ch:                   ch,
				localRaw:             localRaw,
				localMan:             localMan,
				rawCached:            isReceiptMatching(localRaw, *ch.StoredSHA256),
				manCached:            hasFileAndReceipt(localMan),
				minNS:                minNS,
				maxNS:                maxNS,
				hasDataRange:         hasDataRange,
				hasSourceRange:       hasSourceRange,
				conservativeUnknown:  conservativeUnknown,
				baselineOnly:         sp.baselineOnly,
				exchangeBoundaryOnly: sp.exchangeBoundaryOnly,
			})
		}
	}

	sort.Slice(allCandidates, func(i, j int) bool {
		ti, tj := allCandidates[i].minNS, allCandidates[j].minNS
		if !allCandidates[i].hasDataRange {
			ti = allCandidates[i].ch.CreatedAtUTCNS
		}
		if !allCandidates[j].hasDataRange {
			tj = allCandidates[j].ch.CreatedAtUTCNS
		}
		if ti != tj {
			return ti < tj
		}
		if allCandidates[i].ch.CreatedAtUTCNS != allCandidates[j].ch.CreatedAtUTCNS {
			return allCandidates[i].ch.CreatedAtUTCNS < allCandidates[j].ch.CreatedAtUTCNS
		}
		return allCandidates[i].ch.ChunkID < allCandidates[j].ch.ChunkID
	})

	type baseKey struct {
		market string
		symbol string
		stream string
	}
	chosen := make(map[string]bool)
	strictPreSnapshotMinNS := make(map[string]int64)
	overlapPreSnapshotMinNS := make(map[string]int64)
	latestPreRestChunk := make(map[baseKey]int)
	latestPreStreamChunk := make(map[baseKey]int)

	lookupResolvedBaseline := func(sym string) (OrderbookBaselineHint, bool) {
		if len(opts.BaselineSnapshots) == 0 {
			return OrderbookBaselineHint{}, false
		}
		if sym != "" {
			if b, ok := opts.BaselineSnapshots[strings.ToUpper(sym)]; ok && b.ReceiveTimeUTCNS > 0 {
				return b, true
			}
		}
		if b, ok := opts.BaselineSnapshots["*"]; ok && b.ReceiveTimeUTCNS > 0 {
			return b, true
		}
		return OrderbookBaselineHint{}, false
	}

	recordRestSnapshotStart := func(symKey string, symName string, cMin int64, overlapsWindow bool) {
		// When Stage 1 already resolved the exact latest pre-start snapshot timestamp for this symbol,
		// never rewind its baseline back to the earliest timestamp of a spanning REST chunk (M6-Q3).
		if _, resolved := lookupResolvedBaseline(symName); resolved {
			return
		}
		if overlapsWindow {
			if prev, ok := overlapPreSnapshotMinNS[symKey]; !ok || cMin < prev {
				overlapPreSnapshotMinNS[symKey] = cMin
			}
			return
		}
		if prev, ok := strictPreSnapshotMinNS[symKey]; !ok || cMin > prev {
			strictPreSnapshotMinNS[symKey] = cMin
		}
	}

	for idx, cand := range allCandidates {
		ch := cand.ch
		if opts.OrderbookMode && ch.Stream == "rest_observations" && ch.IsProvenNonDepthSnapshotREST() {
			continue
		}
		if cand.hasDataRange {
			if opts.EndUTCNS > 0 && cand.minNS >= opts.EndUTCNS {
				continue
			}
			if opts.OrderbookMode && ch.Stream == "rest_observations" {
				if b, resolved := lookupResolvedBaseline(ch.SymbolName()); resolved {
					if cand.maxNS < b.ReceiveTimeUTCNS && (b.SourceChunkID == "" || ch.ChunkID != b.SourceChunkID) {
						continue
					}
					if ch.ChunkID == b.SourceChunkID || cand.maxNS >= b.ReceiveTimeUTCNS {
						chosen[ch.ChunkID] = true
						continue
					}
				}
			}
			inOrAfterStart := opts.StartUTCNS <= 0 || cand.maxNS == 0 || cand.maxNS >= opts.StartUTCNS
			if inOrAfterStart {
				if !cand.baselineOnly {
					chosen[ch.ChunkID] = true
				}
				if opts.IncludePrecedingBaseline && opts.OrderbookMode && ch.Stream == "rest_observations" && ch.HasDepthSnapshotMetadata() {
					if opts.StartUTCNS <= 0 || cand.minNS <= opts.StartUTCNS {
						recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), ch.SymbolName(), cand.minNS, true)
					}
				}
				continue
			}
			if opts.IncludePrecedingBaseline {
				bk := baseKey{market: ch.Market, symbol: ch.SymbolName(), stream: ch.Stream}
				if ch.Stream == "rest_observations" {
					if ch.HasDepthSnapshotMetadata() {
						latestPreRestChunk[bk] = idx
						recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), ch.SymbolName(), cand.minNS, false)
					} else if _, hasSnap := latestPreRestChunk[bk]; !hasSnap {
						latestPreRestChunk[bk] = idx
					}
				} else {
					latestPreStreamChunk[bk] = idx
				}
			}
			continue
		}

		if cand.conservativeUnknown {
			// Requested exchange_time bounds are absent: retain candidate for local payload filtering instead of pruning on proxy/receive time.
			if !cand.baselineOnly || opts.IncludePrecedingBaseline {
				chosen[ch.ChunkID] = true
			}
			continue
		}

		if opts.EndUTCNS > 0 && ch.CreatedAtUTCNS >= opts.EndUTCNS && ch.Stream != "rest_observations" {
			continue
		}
		if opts.StartUTCNS > 0 && ch.UpdatedAtUTCNS > 0 && ch.UpdatedAtUTCNS < opts.StartUTCNS {
			if opts.IncludePrecedingBaseline {
				bk := baseKey{market: ch.Market, symbol: ch.SymbolName(), stream: ch.Stream}
				if ch.Stream == "rest_observations" {
					latestPreRestChunk[bk] = idx
					if ch.HasDepthSnapshotMetadata() {
						recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), ch.SymbolName(), ch.CreatedAtUTCNS, false)
					}
				} else {
					latestPreStreamChunk[bk] = idx
				}
			}
			continue
		}
		if !cand.baselineOnly || opts.IncludePrecedingBaseline {
			chosen[ch.ChunkID] = true
			if opts.IncludePrecedingBaseline && opts.OrderbookMode && ch.Stream == "rest_observations" && ch.HasDepthSnapshotMetadata() {
				if opts.StartUTCNS <= 0 || ch.CreatedAtUTCNS <= opts.StartUTCNS {
					recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), ch.SymbolName(), ch.CreatedAtUTCNS, true)
				}
			}
		}
	}

	if opts.IncludePrecedingBaseline {
		for _, idx := range latestPreRestChunk {
			cand := allCandidates[idx]
			if opts.OrderbookMode {
				if b, resolved := lookupResolvedBaseline(cand.ch.SymbolName()); resolved {
					if cand.hasDataRange && cand.maxNS < b.ReceiveTimeUTCNS && (b.SourceChunkID == "" || cand.ch.ChunkID != b.SourceChunkID) {
						continue
					}
				}
			}
			chosen[cand.ch.ChunkID] = true
		}
		latestPreSnapshotMinNS := make(map[string]int64)
		for k, v := range strictPreSnapshotMinNS {
			latestPreSnapshotMinNS[k] = v
		}
		for k, v := range overlapPreSnapshotMinNS {
			if prev, ok := latestPreSnapshotMinNS[k]; !ok || v < prev {
				latestPreSnapshotMinNS[k] = v
			}
		}
		lookupSnapStart := func(market, sym string) (int64, bool) {
			if b, resolved := lookupResolvedBaseline(sym); resolved {
				return b.ReceiveTimeUTCNS, true
			}
			candidates := []string{
				market + "|" + sym,
				market + "|__GLOBAL__",
				market + "|",
			}
			switch market {
			case "um_perpetual", "um_delivery", "usdm":
				candidates = append(candidates, "um_shared|"+sym, "um_shared|__GLOBAL__", "um_shared|")
			case "cm_perpetual", "cm_delivery", "coinm":
				candidates = append(candidates, "cm_shared|"+sym, "cm_shared|__GLOBAL__", "cm_shared|")
			}
			for _, key := range candidates {
				if v, ok := latestPreSnapshotMinNS[key]; ok {
					return v, true
				}
			}
			return 0, false
		}
		if opts.OrderbookMode && (len(latestPreSnapshotMinNS) > 0 || len(opts.BaselineSnapshots) > 0) {
			// When a resolved baseline lacks sequence_ranges.u metadata, retain same-connection contiguous
			// pre-snapshot chunks (when ConnectionIDs are present) or the single immediately preceding
			// diff_depth chunk before baseline.ReceiveTimeUTCNS so unindexed bridges are preserved.
			hasMatchingConn := func(a, b []string) bool {
				for _, ca := range a {
					for _, cb := range b {
						if ca != "" && ca == cb {
							return true
						}
					}
				}
				return false
			}
			latestPreBaselineDiff := make(map[baseKey]int)
			unindexedPreByKey := make(map[baseKey][]int)
			bridgeSatisfied := make(map[baseKey]bool)
			for idx, cand := range allCandidates {
				if cand.ch.Stream != "diff_depth" {
					continue
				}
				sym := cand.ch.SymbolName()
				b, resolved := lookupResolvedBaseline(sym)
				if cand.hasDataRange {
					if opts.EndUTCNS > 0 && cand.minNS >= opts.EndUTCNS {
						continue
					}
					if resolved {
						bk := baseKey{market: cand.ch.Market, symbol: sym, stream: cand.ch.Stream}
						uMin, uMax, hasUMin, hasUMax := cand.ch.SequenceURange()
						targetUID := b.LastUpdateID
						if cand.ch.Market == "spot" || opts.Market == "spot" {
							targetUID++
						}
						if b.LastUpdateID > 0 && hasUMax {
							if uMax >= targetUID {
								chosen[cand.ch.ChunkID] = true
								if hasUMin && uMin <= targetUID {
									bridgeSatisfied[bk] = true
									delete(latestPreBaselineDiff, bk)
									delete(unindexedPreByKey, bk)
								}
							} else {
								// Any unindexed chunk strictly earlier than an indexed uMax < targetUID chunk cannot reach targetUID.
								delete(latestPreBaselineDiff, bk)
								delete(unindexedPreByKey, bk)
							}
							continue
						}
						if cand.maxNS >= b.ReceiveTimeUTCNS {
							chosen[cand.ch.ChunkID] = true
						} else if !bridgeSatisfied[bk] {
							latestPreBaselineDiff[bk] = idx
							unindexedPreByKey[bk] = append(unindexedPreByKey[bk], idx)
						}
						continue
					}
					if snapStart, ok := lookupSnapStart(cand.ch.Market, sym); ok && cand.maxNS >= snapStart {
						chosen[cand.ch.ChunkID] = true
					}
				} else {
					if opts.EndUTCNS > 0 && cand.ch.CreatedAtUTCNS >= opts.EndUTCNS {
						continue
					}
					if snapStart, ok := lookupSnapStart(cand.ch.Market, sym); ok && (cand.ch.UpdatedAtUTCNS == 0 || cand.ch.UpdatedAtUTCNS >= snapStart) {
						chosen[cand.ch.ChunkID] = true
					}
				}
			}
			for bk, idx := range latestPreBaselineDiff {
				lastPre := allCandidates[idx]
				chosen[lastPre.ch.ChunkID] = true
				if len(lastPre.ch.ConnectionIDs) > 0 {
					seq := unindexedPreByKey[bk]
					for k := len(seq) - 2; k >= 0; k-- {
						prevCand := allCandidates[seq[k]]
						if !hasMatchingConn(prevCand.ch.ConnectionIDs, lastPre.ch.ConnectionIDs) {
							break
						}
						chosen[prevCand.ch.ChunkID] = true
					}
				}
			}
		} else {
			for _, idx := range latestPreStreamChunk {
				chosen[allCandidates[idx].ch.ChunkID] = true
			}
		}
	}

	allChosenHaveTime := true
	allChosenHaveEx := true
	unknownExCount := 0
	for _, cand := range allCandidates {
		if !chosen[cand.ch.ChunkID] {
			continue
		}
		if !partIncludedInPlan[cand.partID] {
			partIncludedInPlan[cand.partID] = true
			if p, ok := partByID[cand.partID]; ok {
				plan.SelectedPartitions = append(plan.SelectedPartitions, p)
				plan.GapCount += len(p.Availability.Gaps)
				plan.QuarantinedCount += len(p.Availability.QuarantinedArtifacts)
				plan.DiscontinuityCount += len(p.Availability.DiscontinuityEvents)
				if _, isMatched := plan.MatchedPartitionIDs[cand.partID]; !isMatched {
					// Check whether this partition was window-matched (non-baselineOnly).
					for _, sp := range selected {
						if sp.part.Manifest.PartitionID == cand.partID && !sp.baselineOnly && !sp.exchangeBoundaryOnly {
							plan.MatchedPartitionIDs[cand.partID] = true
							break
						}
					}
				}
			}
		}
		if !cand.hasDataRange {
			allChosenHaveTime = false
		}
		if cand.conservativeUnknown {
			allChosenHaveEx = false
			unknownExCount++
		}
		ch := cand.ch
		plan.ReadyChunksMatched++
		if ch.UncompressedBytes != nil {
			plan.EstimatedUncompressedBytes += *ch.UncompressedBytes
		}
		plan.PlannedChunks = append(plan.PlannedChunks, PlannedChunk{
			PartitionID:         cand.partID,
			CatalogPath:         cand.catalogPath,
			StorageID:           *ch.StorageID,
			Chunk:               ch,
			LocalRawPath:        cand.localRaw,
			LocalManifestPath:   cand.localMan,
			RawCached:           cand.rawCached,
			ManifestCached:      cand.manCached,
			ConservativeUnknown: cand.conservativeUnknown,
		})
	}

	for sym := range recordedSymSet {
		plan.RecordedSymbols = append(plan.RecordedSymbols, sym)
	}
	sort.Strings(plan.RecordedSymbols)
	plan.HasSymbolMetadata = len(plan.RecordedSymbols) > 0
	plan.HasTimeRangeMetadata = plan.ReadyChunksMatched > 0 && allChosenHaveTime
	if opts.PreferExchangeTime {
		plan.HasExchangeTimeMetadata = plan.ReadyChunksMatched > 0 && allChosenHaveEx
		plan.UnknownExchangeRangeChunks = unknownExCount
	}
	if len(normalizedSyms) > 0 && len(recordedSymSet) > 0 {
		for _, sym := range normalizedSyms {
			if !recordedSymSet[sym] {
				plan.UnrecordedSymbols = append(plan.UnrecordedSymbols, sym)
			}
		}
	}
	if len(plan.PendingReasons) == 0 {
		plan.PendingReasons = nil
	}
	c.RecomputePlanObjectMetrics(plan)
	return plan, nil
}

// RecomputePlanObjectMetrics computes unified deduplicated counts, cached hits, missing bytes,
// missing catalogs, unknown-size manifests, partial files, and job items from plan objects.
func (c *Client) RecomputePlanObjectMetrics(plan *QueryPlan) {
	if plan == nil {
		return
	}
	// Deduplicate PlannedChunks preserving chronological order.
	dedupChunks := make([]PlannedChunk, 0, len(plan.PlannedChunks))
	seenChunk := make(map[string]bool, len(plan.PlannedChunks))
	for _, pc := range plan.PlannedChunks {
		if seenChunk[pc.Chunk.ChunkID] {
			continue
		}
		seenChunk[pc.Chunk.ChunkID] = true
		dedupChunks = append(dedupChunks, pc)
	}
	plan.PlannedChunks = dedupChunks

	// Deduplicate SelectedPartitions.
	partByKey := make(map[string]LoadedPartition, len(plan.SelectedPartitions))
	dedupParts := make([]LoadedPartition, 0, len(plan.SelectedPartitions))
	for _, lp := range plan.SelectedPartitions {
		k := lp.Manifest.Market + "|" + lp.Manifest.PartitionID
		if _, exists := partByKey[k]; !exists {
			partByKey[k] = lp
			dedupParts = append(dedupParts, lp)
		}
	}
	plan.SelectedPartitions = dedupParts
	if len(plan.MatchedPartitionIDs) > 0 {
		plan.PartitionsMatched = len(plan.MatchedPartitionIDs)
	} else {
		plan.PartitionsMatched = len(dedupParts)
	}

	selectedByPID := make(map[string]LoadedPartition, len(dedupParts))
	for _, lp := range dedupParts {
		selectedByPID[lp.Manifest.PartitionID] = lp
	}

	plan.ReadyChunksMatched = len(plan.PlannedChunks)
	plan.AlreadyCachedChunks = 0
	plan.DownloadChunks = 0
	plan.EstimatedRawBytes = 0
	plan.EstimatedCatalogBytes = 0
	plan.EstimatedDownloadBytes = 0
	plan.EstimatedUncompressedBytes = 0
	plan.MissingCatalogCount = 0
	plan.MissingManifestCount = 0
	plan.UnknownSizeManifestCount = 0
	plan.TotalJobItems = 0
	plan.PartialFileCount = 0
	plan.PartialBytes = 0
	plan.BaselineStartUTCNS = 0

	statPartial := func(path string) {
		if path == "" {
			return
		}
		if st, err := os.Stat(path + ".part"); err == nil && !st.IsDir() {
			plan.PartialFileCount++
			plan.PartialBytes += st.Size()
		}
	}

	seenCat := make(map[string]bool)
	seenObjPath := make(map[string]bool)
	for _, pc := range plan.PlannedChunks {
		ch := pc.Chunk
		if minNS, _, hasRange := ch.ReceiveTimeRangeNS(); hasRange && minNS > 0 {
			if plan.BaselineStartUTCNS == 0 || minNS < plan.BaselineStartUTCNS {
				plan.BaselineStartUTCNS = minNS
			}
		} else if ch.CreatedAtUTCNS > 0 && (plan.BaselineStartUTCNS == 0 || ch.CreatedAtUTCNS < plan.BaselineStartUTCNS) {
			plan.BaselineStartUTCNS = ch.CreatedAtUTCNS
		}
		if ch.UncompressedBytes != nil {
			plan.EstimatedUncompressedBytes += *ch.UncompressedBytes
		}
		if lp, ok := selectedByPID[pc.PartitionID]; ok && !seenCat[pc.PartitionID] {
			seenCat[pc.PartitionID] = true
			if catRef, hasCat := lp.Manifest.Files["catalog_sqlite"]; hasCat && catRef.URLPath != "" {
				plan.TotalJobItems++
				if !isReceiptMatching(lp.CatalogPath, catRef.SHA256) {
					plan.MissingCatalogCount++
					plan.EstimatedCatalogBytes += catRef.Bytes
					statPartial(lp.CatalogPath)
				}
			}
		}
		if ch.StoredSHA256 != nil {
			pc.RawCached = isReceiptMatching(pc.LocalRawPath, *ch.StoredSHA256)
		}
		pc.ManifestCached = hasFileAndReceipt(pc.LocalManifestPath)
		if pc.RawCached && pc.ManifestCached {
			plan.AlreadyCachedChunks++
		} else {
			plan.DownloadChunks++
		}
		if !seenObjPath[pc.LocalRawPath] {
			seenObjPath[pc.LocalRawPath] = true
			plan.TotalJobItems++
			if !pc.RawCached {
				if ch.StoredBytes != nil {
					plan.EstimatedRawBytes += *ch.StoredBytes
				}
				statPartial(pc.LocalRawPath)
			}
		}
		if !seenObjPath[pc.LocalManifestPath] {
			seenObjPath[pc.LocalManifestPath] = true
			plan.TotalJobItems++
			if !pc.ManifestCached {
				plan.MissingManifestCount++
				plan.UnknownSizeManifestCount++
				statPartial(pc.LocalManifestPath)
			}
		}
	}
	plan.EstimatedDownloadBytes = plan.EstimatedRawBytes + plan.EstimatedCatalogBytes
}

// PlanAllObjectsCached returns true when every catalog, raw chunk, and external manifest referenced by plan is already verified locally.
func (c *Client) PlanAllObjectsCached(plan *QueryPlan) bool {
	if plan == nil {
		return false
	}
	if plan.MissingCatalogCount > 0 || plan.MissingManifestCount > 0 {
		return false
	}
	for _, pc := range plan.PlannedChunks {
		if !pc.RawCached || !pc.ManifestCached {
			return false
		}
	}
	return true
}

// MergeQueryPlans merges an orderbook warmup+window plan with an additional target-window stream plan,
// deduplicating objects and partitions cleanly.
func MergeQueryPlans(base, extra *QueryPlan, client *Client) *QueryPlan {
	if base == nil {
		return extra
	}
	if extra == nil {
		return base
	}
	out := *base
	out.PlannedChunks = append(append([]PlannedChunk(nil), base.PlannedChunks...), extra.PlannedChunks...)
	out.SelectedPartitions = append(append([]LoadedPartition(nil), base.SelectedPartitions...), extra.SelectedPartitions...)
	out.MatchedPartitionIDs = make(map[string]bool)
	for pid := range base.MatchedPartitionIDs {
		out.MatchedPartitionIDs[pid] = true
	}
	for pid := range extra.MatchedPartitionIDs {
		out.MatchedPartitionIDs[pid] = true
	}
	streamSet := make(map[string]bool)
	var mergedStreams []string
	for _, s := range append(append([]string(nil), base.Streams...), extra.Streams...) {
		if s != "" && !streamSet[s] {
			streamSet[s] = true
			mergedStreams = append(mergedStreams, s)
		}
	}
	out.Streams = mergedStreams
	recSet := make(map[string]bool)
	for _, s := range append(append([]string(nil), base.RecordedSymbols...), extra.RecordedSymbols...) {
		if s != "" {
			recSet[s] = true
		}
	}
	out.RecordedSymbols = nil
	for s := range recSet {
		out.RecordedSymbols = append(out.RecordedSymbols, s)
	}
	sort.Strings(out.RecordedSymbols)
	out.HasSymbolMetadata = len(out.RecordedSymbols) > 0
	out.HasTimeRangeMetadata = base.HasTimeRangeMetadata && extra.HasTimeRangeMetadata
	out.GapCount = 0
	out.QuarantinedCount = 0
	out.DiscontinuityCount = 0
	seenPartMeta := make(map[string]bool)
	for _, lp := range out.SelectedPartitions {
		k := lp.Manifest.Market + "|" + lp.Manifest.PartitionID
		if !seenPartMeta[k] && out.MatchedPartitionIDs[lp.Manifest.PartitionID] {
			seenPartMeta[k] = true
			out.GapCount += len(lp.Availability.Gaps)
			out.QuarantinedCount += len(lp.Availability.QuarantinedArtifacts)
			out.DiscontinuityCount += len(lp.Availability.DiscontinuityEvents)
		}
	}
	if client != nil {
		client.RecomputePlanObjectMetrics(&out)
	}
	return &out
}

func hydrateChunkMetadataFromLocalManifest(ch *ChunkEntry, localMan string) {
	if localMan == "" {
		return
	}
	rawBytes, err := os.ReadFile(localMan)
	if err != nil {
		return
	}
	var doc struct {
		RawManifest *struct {
			Symbol                string                 `json:"symbol"`
			ReceiveTimeUTCRangeNS *TimeRangeNS           `json:"receive_time_utc_range_ns"`
			ExchangeTimeRanges    map[string]TimeRangeNS `json:"exchange_time_ranges"`
			SequenceRanges        map[string]any         `json:"sequence_ranges"`
			ConnectionIDs         []string               `json:"connection_ids"`
		} `json:"raw_manifest"`
	}
	if json.Unmarshal(rawBytes, &doc) != nil || doc.RawManifest == nil {
		return
	}
	if ch.Symbol == nil && doc.RawManifest.Symbol != "" {
		s := doc.RawManifest.Symbol
		ch.Symbol = &s
	}
	if ch.ReceiveTimeUTCRangeNS == nil && doc.RawManifest.ReceiveTimeUTCRangeNS != nil {
		ch.ReceiveTimeUTCRangeNS = doc.RawManifest.ReceiveTimeUTCRangeNS
	}
	if len(ch.ExchangeTimeRanges) == 0 && len(doc.RawManifest.ExchangeTimeRanges) > 0 {
		ch.ExchangeTimeRanges = doc.RawManifest.ExchangeTimeRanges
	}
	if len(ch.SequenceRanges) == 0 && len(doc.RawManifest.SequenceRanges) > 0 {
		ch.SequenceRanges = doc.RawManifest.SequenceRanges
	}
	if len(ch.ConnectionIDs) == 0 && len(doc.RawManifest.ConnectionIDs) > 0 {
		ch.ConnectionIDs = doc.RawManifest.ConnectionIDs
	}
}

// HydrateMissingManifests downloads small archive manifests for candidate chunks whose requested source
// time range is absent in partition metadata so a follow-up PlanQueryWithOptions can filter without downloading raw chunks.
func (c *Client) HydrateMissingManifests(ctx context.Context, plan *QueryPlan, offline bool) (int, error) {
	if plan == nil || offline || plan.UnknownExchangeRangeChunks == 0 {
		return 0, nil
	}
	fetched := 0
	for _, pc := range plan.PlannedChunks {
		if !pc.ConservativeUnknown || pc.ManifestCached || pc.Chunk.ManifestURLPath == nil || pc.Chunk.ExternalManifestRelPath == nil {
			continue
		}
		ch := pc.Chunk
		manRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.ExternalManifestRelPath))
		priMan, secMan := c.urlsFor(*ch.ManifestURLPath)
		res, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
			DestRelPath: manRel,
			Key: cache.CacheKey{
				Source:         "recorder_archive_manifest",
				Market:         ch.Market,
				Dataset:        ch.ChunkID,
				RevisionOrHash: *ch.StoredSHA256,
			},
			SourceURL:   priMan,
			FallbackURL: secMan,
			Offline:     false,
		})
		if err != nil {
			continue
		}
		if !res.CacheHit {
			fetched++
		}
	}
	return fetched, nil
}

// HydrateOrderbookBridgeManifests fetches small external manifests for unindexed pre-baseline diff_depth
// chunks so Stage 2 can verify sequence_ranges.u without downloading GB of raw chunks.
func (c *Client) HydrateOrderbookBridgeManifests(ctx context.Context, plan *QueryPlan, offline bool, budget *DownloadBudget) (int, error) {
	if plan == nil || offline {
		return 0, nil
	}
	fetched := 0
	for _, pc := range plan.PlannedChunks {
		ch := pc.Chunk
		if ch.Stream != "diff_depth" || pc.ManifestCached || ch.ManifestURLPath == nil || ch.ExternalManifestRelPath == nil || ch.StoredSHA256 == nil {
			continue
		}
		if _, hasU := ch.SequenceUMax(); hasU {
			continue
		}
		manRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.ExternalManifestRelPath))
		priMan, secMan := c.urlsFor(*ch.ManifestURLPath)
		res, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
			DestRelPath: manRel,
			Key: cache.CacheKey{
				Source:         "recorder_archive_manifest",
				Market:         ch.Market,
				Dataset:        ch.ChunkID,
				RevisionOrHash: *ch.StoredSHA256,
			},
			SourceURL:   priMan,
			FallbackURL: secMan,
			Offline:     false,
		})
		if err != nil {
			continue
		}
		if res != nil && !res.CacheHit {
			fetched++
			if budget != nil && res.BytesRead > 0 {
				budget.SpentBytes += res.BytesRead
				if budget.MaxBytes > 0 && budget.SpentBytes > budget.MaxBytes {
					return fetched, c.formatBudgetExceededError(plan, budget.MaxBytes, plan.EstimatedDownloadBytes)
				}
			}
		}
	}
	return fetched, nil
}

func (c *Client) formatBudgetExceededError(plan *QueryPlan, maxDownloadBytes int64, requiredBytes int64) error {
	startNS := plan.RequestedStartUTCNS
	if startNS == 0 {
		startNS = plan.BaselineStartUTCNS
	}
	endNS := plan.RequestedEndUTCNS
	symsArg := ""
	if len(plan.RequestedSymbols) > 0 {
		symsArg = fmt.Sprintf(", symbols=%q", strings.Join(plan.RequestedSymbols, ","))
	}
	endArg := ""
	if endNS > 0 {
		endArg = fmt.Sprintf(", end_utc_ns=%d", endNS)
	}
	return fmt.Errorf(
		"query requires downloading %.2f MiB (%d raw chunks, %d catalogs, %d unknown-size manifests), exceeding interactive budget %.2f MiB; pass max_download_bytes>=%d on this query or call manage_local_cache(action=prefetch, source=recorder, market=%s, revision_id=%s%s, include_baseline=true, start_utc_ns=%d%s)",
		float64(requiredBytes)/(1024*1024),
		plan.DownloadChunks,
		plan.MissingCatalogCount,
		plan.UnknownSizeManifestCount,
		float64(maxDownloadBytes)/(1024*1024),
		requiredBytes+1024*1024,
		plan.Market,
		plan.RevisionID,
		symsArg,
		startNS,
		endArg,
	)
}

// MaterializePlan downloads (or verifies offline) every planned raw chunk & archive manifest into
// BINANCE_MCP_DATA_DIR/recorder/storage/<storage_id>/..., enforcing maxDownloadBytes unless already cached.
func (c *Client) MaterializePlan(ctx context.Context, plan *QueryPlan, offline bool, maxDownloadBytes int64) error {
	return c.MaterializePlanWithBudget(ctx, plan, offline, maxDownloadBytes, nil)
}

// MaterializePlanWithBudget enforces a shared DownloadBudget across probing and final materialization.
func (c *Client) MaterializePlanWithBudget(ctx context.Context, plan *QueryPlan, offline bool, maxDownloadBytes int64, budget *DownloadBudget) error {
	if plan == nil {
		return errors.New("plan is nil")
	}
	if maxDownloadBytes <= 0 {
		maxDownloadBytes = DefaultAutoDownloadCapBytes
	}
	if budget == nil {
		budget = &DownloadBudget{MaxBytes: maxDownloadBytes}
	} else if budget.MaxBytes <= 0 {
		budget.MaxBytes = maxDownloadBytes
	}
	if !offline && plan.EstimatedDownloadBytes > budget.Remaining() {
		return c.formatBudgetExceededError(plan, budget.MaxBytes, plan.EstimatedDownloadBytes+budget.SpentBytes)
	}

	chargeTransfer := func(res *cache.EnsureResult) error {
		if res == nil || res.CacheHit {
			return nil
		}
		plan.HTTPBytesDownloaded += res.BytesRead
		if !offline && res.BytesRead > 0 {
			budget.SpentBytes += res.BytesRead
			if budget.MaxBytes > 0 && budget.SpentBytes > budget.MaxBytes {
				return c.formatBudgetExceededError(plan, budget.MaxBytes, budget.SpentBytes)
			}
		}
		return nil
	}

	// M6-Q1: Download/verify catalog.sqlite lazily ONLY for partitions actually referenced by PlannedChunks.
	if len(plan.PlannedChunks) > 0 {
		selectedByID := make(map[string]LoadedPartition, len(plan.SelectedPartitions))
		for _, lp := range plan.SelectedPartitions {
			selectedByID[lp.Manifest.PartitionID] = lp
		}
		seenCat := make(map[string]bool)
		for _, pc := range plan.PlannedChunks {
			if seenCat[pc.PartitionID] {
				continue
			}
			seenCat[pc.PartitionID] = true
			if lp, ok := selectedByID[pc.PartitionID]; ok {
				if _, hasCat := lp.Manifest.Files["catalog_sqlite"]; hasCat {
					catRes, err := c.ensurePartitionArtifactResult(ctx, lp.Manifest, "catalog_sqlite", true, offline)
					if err != nil {
						return fmt.Errorf("partition %s/%s catalog_sqlite: %w", lp.Manifest.Market, lp.Manifest.PartitionID, err)
					}
					if bErr := chargeTransfer(catRes); bErr != nil {
						return bErr
					}
				}
			}
		}
	}

	var kept []PlannedChunk
	var firstOfflineOmitErr error
	for _, pc := range plan.PlannedChunks {
		ch := pc.Chunk
		rawRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.TargetRelativePath))
		manRel := filepath.Join("recorder", "storage", pc.StorageID, filepath.FromSlash(*ch.ExternalManifestRelPath))

		priRaw, secRaw := c.urlsFor(*ch.ArchiveURLPath)
		rawRes, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
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
		})
		if err != nil {
			if offline && pc.ConservativeUnknown {
				if firstOfflineOmitErr == nil {
					firstOfflineOmitErr = fmt.Errorf("ensure raw chunk %s (%s): %w", ch.ChunkID, ch.Stream, err)
				}
				plan.OmittedUnknownOfflineChunks++
				continue
			}
			return fmt.Errorf("ensure raw chunk %s (%s): %w", ch.ChunkID, ch.Stream, err)
		}
		if bErr := chargeTransfer(rawRes); bErr != nil {
			return bErr
		}

		priMan, secMan := c.urlsFor(*ch.ManifestURLPath)
		manRes, err := c.CacheMgr.EnsureFile(ctx, cache.EnsureOptions{
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
		})
		if err != nil {
			if offline && pc.ConservativeUnknown {
				if firstOfflineOmitErr == nil {
					firstOfflineOmitErr = fmt.Errorf("ensure archive manifest %s: %w", ch.ChunkID, err)
				}
				plan.OmittedUnknownOfflineChunks++
				continue
			}
			return fmt.Errorf("ensure archive manifest %s: %w", ch.ChunkID, err)
		}
		if bErr := chargeTransfer(manRes); bErr != nil {
			return bErr
		}
		kept = append(kept, pc)
	}
	if offline && len(plan.PlannedChunks) > 0 && len(kept) == 0 && firstOfflineOmitErr != nil {
		return firstOfflineOmitErr
	}
	if plan.OmittedUnknownOfflineChunks > 0 {
		plan.PlannedChunks = kept
	}
	return nil
}

// BuildPrefetchJobItems turns a QueryPlan into durable JobItems for manage_local_cache prefetch.
func (c *Client) BuildPrefetchJobItems(plan *QueryPlan) []cache.JobItem {
	items := make([]cache.JobItem, 0, len(plan.PlannedChunks)*2+len(plan.SelectedPartitions))
	selectedByID := make(map[string]LoadedPartition, len(plan.SelectedPartitions))
	for _, lp := range plan.SelectedPartitions {
		selectedByID[lp.Manifest.PartitionID] = lp
	}
	seenCat := make(map[string]bool)
	for _, pc := range plan.PlannedChunks {
		if !seenCat[pc.PartitionID] {
			seenCat[pc.PartitionID] = true
			if lp, ok := selectedByID[pc.PartitionID]; ok {
				if catRef, hasCat := lp.Manifest.Files["catalog_sqlite"]; hasCat && catRef.URLPath != "" {
					shortHash := catRef.SHA256
					if len(shortHash) > 16 {
						shortHash = shortHash[:16]
					}
					catRel := filepath.Join("recorder", "snapshots", lp.Manifest.Market, lp.Manifest.PartitionID, shortHash, "catalog.sqlite")
					priCat, secCat := c.urlsFor(catRef.URLPath)
					items = append(items, cache.JobItem{
						ItemID:      lp.Manifest.PartitionID + ":catalog",
						DestRelPath: catRel,
						SourceURL:   priCat,
						FallbackURL: secCat,
						SHA256:      catRef.SHA256,
						SizeBytes:   catRef.Bytes,
						Key: cache.CacheKey{
							Source:         "recorder_snapshot_artifact",
							Market:         lp.Manifest.Market,
							Dataset:        "catalog.sqlite",
							RevisionOrHash: catRef.SHA256,
						},
						Completed: isReceiptMatching(lp.CatalogPath, catRef.SHA256),
					})
				}
			}
		}
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
