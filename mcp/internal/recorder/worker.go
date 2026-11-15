package recorder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

// WorkerPartitionRef identifies one partition snapshot catalog passed to python/worker.py.
type WorkerPartitionRef struct {
	PartitionID string `json:"partition_id"`
	CatalogPath string `json:"catalog_path"`
}

// WorkerChunkRef identifies one ready local chunk (.bmdr.zst + .archive-manifest.json) passed to python/worker.py.
type WorkerChunkRef struct {
	PartitionID  string `json:"partition_id"`
	ChunkID      string `json:"chunk_id"`
	StorageID    string `json:"storage_id"`
	Stream       string `json:"stream"`
	ManifestPath string `json:"manifest_path"`
	RawPath      string `json:"raw_path"`
}

// WorkerRequest serializes an offline local-cache query for python/worker.py.
type WorkerRequest struct {
	Operation    string               `json:"operation"`
	Market       string               `json:"market"`
	Symbols      []string             `json:"symbols,omitempty"`
	StartUTCNS   int64                `json:"start_utc_ns,omitempty"`
	EndUTCNS     int64                `json:"end_utc_ns,omitempty"`
	Interval     string               `json:"interval,omitempty"`
	DepthLevels  int                  `json:"depth_levels,omitempty"`
	SampleStepMS int                  `json:"sample_step_ms,omitempty"`
	RecordKind   string               `json:"record_kind,omitempty"`
	Limit        int                  `json:"limit,omitempty"`
	OutputFile   string               `json:"output_file,omitempty"`
	OutputFormat string               `json:"output_format,omitempty"`
	Partitions   []WorkerPartitionRef `json:"partitions"`
	Chunks       []WorkerChunkRef     `json:"chunks"`
	ExternalRoots map[string]string   `json:"external_roots"`
}

// BuildWorkerRequest constructs the Python offline worker request from a materialized QueryPlan.
func (c *Client) BuildWorkerRequest(op string, plan *QueryPlan) WorkerRequest {
	req := WorkerRequest{
		Operation:     op,
		Market:        plan.Market,
		ExternalRoots: make(map[string]string),
	}
	seenPart := make(map[string]bool)
	for _, part := range plan.SelectedPartitions {
		if !seenPart[part.Manifest.PartitionID] {
			seenPart[part.Manifest.PartitionID] = true
			req.Partitions = append(req.Partitions, WorkerPartitionRef{
				PartitionID: part.Manifest.PartitionID,
				CatalogPath: part.CatalogPath,
			})
		}
	}
	for _, pc := range plan.PlannedChunks {
		req.Chunks = append(req.Chunks, WorkerChunkRef{
			PartitionID:  pc.PartitionID,
			ChunkID:      pc.Chunk.ChunkID,
			StorageID:    pc.StorageID,
			Stream:       pc.Chunk.Stream,
			ManifestPath: pc.LocalManifestPath,
			RawPath:      pc.LocalRawPath,
		})
		if _, ok := req.ExternalRoots[pc.StorageID]; !ok {
			if rootDir, err := c.CacheMgr.PathFor("recorder", "storage", pc.StorageID); err == nil {
				req.ExternalRoots[pc.StorageID] = rootDir
			}
		}
	}
	return req
}

// RunWorker executes python/worker.py against local verified Recorder caches and returns parsed JSON.
func (c *Client) RunWorker(ctx context.Context, req WorkerRequest) (map[string]any, error) {
	if c.Cfg.PythonBin == "" || c.Cfg.WorkerScript == "" {
		return nil, fmt.Errorf("python worker not configured (python=%q script=%q)", c.Cfg.PythonBin, c.Cfg.WorkerScript)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	cmd := exec.CommandContext(ctx, c.Cfg.PythonBin, c.Cfg.WorkerScript)
	cmd.Stdin = bytes.NewReader(payload)
	if c.Cfg.RecorderRepoDir != "" {
		cmd.Env = append(os.Environ(), "SBMDR_REPO_DIR="+c.Cfg.RecorderRepoDir)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()
	var parsed map[string]any
	if stdout.Len() > 0 {
		_ = json.Unmarshal(stdout.Bytes(), &parsed)
	}
	if runErr != nil {
		if parsed != nil {
			if msg, ok := parsed["error"].(string); ok && msg != "" {
				return nil, fmt.Errorf("recorder python worker error: %s", msg)
			}
		}
		return nil, fmt.Errorf("recorder python worker failed (%v): stderr=%s stdout=%s", runErr, stderr.String(), stdout.String())
	}
	if parsed == nil {
		return nil, fmt.Errorf("recorder python worker emitted invalid JSON: %s", stdout.String())
	}
	if msg, ok := parsed["error"].(string); ok && msg != "" {
		return nil, fmt.Errorf("recorder python worker error: %s", msg)
	}
	return parsed, nil
}
