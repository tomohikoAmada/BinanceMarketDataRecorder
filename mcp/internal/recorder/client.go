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

// HasDepthSnapshotMetadata reports whether sequence_ranges indicates a rest_observations chunk holds a depth snapshot.
func (ch ChunkEntry) HasDepthSnapshotMetadata() bool {
	if ch.Stream != "rest_observations" || len(ch.SequenceRanges) == 0 {
		return ch.Stream == "rest_observations"
	}
	if _, ok := ch.SequenceRanges["lastUpdateId"]; ok {
		return true
	}
	if _, ok := ch.SequenceRanges["depthSnapshotEpoch"]; ok {
		return true
	}
	if ssRaw, ok := ch.SequenceRanges["sourceStream"].(map[string]any); ok {
		minS, _ := ssRaw["min"].(string)
		maxS, _ := ssRaw["max"].(string)
		if minS != "" && maxS != "" {
			return minS <= "depth_snapshot" && "depth_snapshot" <= maxS
		}
	}
	return false
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
}

// QueryPlan estimates chunks, bytes, pending items, and gaps for a Recorder slice.
type QueryPlan struct {
	RevisionID                 string             `json:"revision_id"`
	Market                     string             `json:"market"`
	Streams                    []string           `json:"streams"`
	RequestedSymbols           []string           `json:"requested_symbols,omitempty"`
	RecordedSymbols            []string           `json:"recorded_symbols,omitempty"`
	UnrecordedSymbols          []string           `json:"unrecorded_symbols,omitempty"`
	PartitionsMatched          int                `json:"partitions_matched"`
	ReadyChunksMatched         int                `json:"ready_chunks_matched"`
	AlreadyCachedChunks        int                `json:"already_cached_chunks"`
	DownloadChunks             int                `json:"download_chunks"`
	EstimatedDownloadBytes     int64              `json:"estimated_download_bytes"`
	EstimatedUncompressedBytes int64              `json:"estimated_uncompressed_bytes"`
	PendingOmittedCount        int                `json:"pending_omitted_count"`
	PendingUnknownStreamCount  int                `json:"pending_unknown_stream_count,omitempty"`
	PendingReasons             map[string]int     `json:"pending_reasons,omitempty"`
	GapCount                    int               `json:"gap_count"`
	QuarantinedCount            int               `json:"quarantined_count"`
	DiscontinuityCount          int               `json:"discontinuity_count"`
	HasSymbolMetadata           bool              `json:"has_symbol_metadata,omitempty"`
	HasTimeRangeMetadata        bool              `json:"has_time_range_metadata,omitempty"`
	SelectionTimeBasis          string            `json:"selection_time_basis,omitempty"`
	HasExchangeTimeMetadata     bool              `json:"has_exchange_time_metadata,omitempty"`
	UnknownExchangeRangeChunks  int               `json:"unknown_exchange_range_chunks,omitempty"`
	OmittedUnknownOfflineChunks int               `json:"omitted_unknown_offline_chunks,omitempty"`
	PlannedChunks               []PlannedChunk    `json:"-"`
	SelectedPartitions          []LoadedPartition `json:"-"`
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
		RevisionID:         snap.RevisionID,
		Market:             opts.Market,
		Streams:            append([]string(nil), opts.Streams...),
		RequestedSymbols:   normalizedSyms,
		PendingReasons:     make(map[string]int),
		SelectionTimeBasis: selectionBasis,
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
			allSymsSatisfied := func() bool {
				if satisfied["*"] {
					return true
				}
				if len(symbolSet) == 0 {
					return false
				}
				for sym := range symbolSet {
					if !satisfied[sym] {
						return false
					}
				}
				return true
			}
			// Check whether already-selected window partitions definitely contain a strict pre-start snapshot baseline.
			for _, sp := range selected {
				if sp.exchangeBoundaryOnly {
					continue
				}
				if chunks, cErr := ReadChunksJSONL(sp.part.ChunksPath); cErr == nil {
					for _, ch := range chunks {
						if !ch.Ready || ch.Stream != "rest_observations" || !ch.HasDepthSnapshotMetadata() {
							continue
						}
						_, cMax, hasRange := ch.EffectiveTimeRangeNS(false)
						strictPre := (hasRange && cMax > 0 && cMax < opts.StartUTCNS) || (!hasRange && ch.UpdatedAtUTCNS > 0 && ch.UpdatedAtUTCNS < opts.StartUTCNS)
						if !strictPre {
							continue
						}
						sym := ch.SymbolName()
						if sym == "" || sym == "__GLOBAL__" {
							satisfied["*"] = true
						} else if len(symbolSet) == 0 || symbolSet[sym] {
							satisfied[sym] = true
						}
					}
				}
			}
			if !allSymsSatisfied() {
				var prependParts []candPart
				for k := len(precedingClosedIdxs) - 1; k >= 0; k-- {
					p := marketParts[precedingClosedIdxs[k]]
					prependParts = append(prependParts, candPart{part: p, baselineOnly: true})
					if chunks, cErr := ReadChunksJSONL(p.ChunksPath); cErr == nil {
						for _, ch := range chunks {
							if !ch.Ready || ch.Stream != "rest_observations" || !ch.HasDepthSnapshotMetadata() {
								continue
							}
							sym := ch.SymbolName()
							if sym == "" || sym == "__GLOBAL__" {
								satisfied["*"] = true
							} else if len(symbolSet) == 0 || symbolSet[sym] {
								satisfied[sym] = true
							}
						}
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

			if ch.ReceiveTimeUTCRangeNS == nil || ch.Symbol == nil || (opts.PreferExchangeTime && len(ch.ExchangeTimeRanges) == 0) {
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

	recordRestSnapshotStart := func(symKey string, cMin int64, overlapsWindow bool) {
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
		if cand.hasDataRange {
			if opts.EndUTCNS > 0 && cand.minNS >= opts.EndUTCNS {
				continue
			}
			inOrAfterStart := opts.StartUTCNS <= 0 || cand.maxNS == 0 || cand.maxNS >= opts.StartUTCNS
			if inOrAfterStart {
				if !cand.baselineOnly {
					chosen[ch.ChunkID] = true
				}
				if opts.IncludePrecedingBaseline && opts.OrderbookMode && ch.Stream == "rest_observations" && ch.HasDepthSnapshotMetadata() {
					if opts.StartUTCNS <= 0 || cand.minNS <= opts.StartUTCNS {
						recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), cand.minNS, true)
					}
				}
				continue
			}
			if opts.IncludePrecedingBaseline {
				bk := baseKey{market: ch.Market, symbol: ch.SymbolName(), stream: ch.Stream}
				if ch.Stream == "rest_observations" {
					if ch.HasDepthSnapshotMetadata() {
						latestPreRestChunk[bk] = idx
						recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), cand.minNS, false)
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
						recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), ch.CreatedAtUTCNS, false)
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
					recordRestSnapshotStart(ch.Market+"|"+ch.SymbolName(), ch.CreatedAtUTCNS, true)
				}
			}
		}
	}

	if opts.IncludePrecedingBaseline {
		for _, idx := range latestPreRestChunk {
			chosen[allCandidates[idx].ch.ChunkID] = true
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
		if opts.OrderbookMode && len(latestPreSnapshotMinNS) > 0 {
			for _, cand := range allCandidates {
				if cand.ch.Stream != "diff_depth" {
					continue
				}
				if cand.hasDataRange {
					if opts.EndUTCNS > 0 && cand.minNS >= opts.EndUTCNS {
						continue
					}
					if snapStart, ok := lookupSnapStart(cand.ch.Market, cand.ch.SymbolName()); ok && cand.maxNS >= snapStart {
						chosen[cand.ch.ChunkID] = true
					}
				} else {
					if opts.EndUTCNS > 0 && cand.ch.CreatedAtUTCNS >= opts.EndUTCNS {
						continue
					}
					if snapStart, ok := lookupSnapStart(cand.ch.Market, cand.ch.SymbolName()); ok && (cand.ch.UpdatedAtUTCNS == 0 || cand.ch.UpdatedAtUTCNS >= snapStart) {
						chosen[cand.ch.ChunkID] = true
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
				plan.PartitionsMatched++
				plan.GapCount += len(p.Availability.Gaps)
				plan.QuarantinedCount += len(p.Availability.QuarantinedArtifacts)
				plan.DiscontinuityCount += len(p.Availability.DiscontinuityEvents)
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
		if cand.rawCached && cand.manCached {
			plan.AlreadyCachedChunks++
		} else {
			plan.DownloadChunks++
			if !cand.rawCached && ch.StoredBytes != nil {
				plan.EstimatedDownloadBytes += *ch.StoredBytes
			}
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
	return plan, nil
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

	var kept []PlannedChunk
	var firstOfflineOmitErr error
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
			if offline && pc.ConservativeUnknown {
				if firstOfflineOmitErr == nil {
					firstOfflineOmitErr = fmt.Errorf("ensure raw chunk %s (%s): %w", ch.ChunkID, ch.Stream, err)
				}
				plan.OmittedUnknownOfflineChunks++
				continue
			}
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
			if offline && pc.ConservativeUnknown {
				if firstOfflineOmitErr == nil {
					firstOfflineOmitErr = fmt.Errorf("ensure archive manifest %s: %w", ch.ChunkID, err)
				}
				plan.OmittedUnknownOfflineChunks++
				continue
			}
			return fmt.Errorf("ensure archive manifest %s: %w", ch.ChunkID, err)
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
