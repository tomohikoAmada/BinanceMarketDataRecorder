package cache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// JobItem describes a single downloadable file inside a persistent prefetch job.
type JobItem struct {
	ItemID      string   `json:"item_id"`
	DestRelPath string   `json:"dest_rel_path"`
	SourceURL   string   `json:"source_url"`
	FallbackURL string   `json:"fallback_url,omitempty"`
	SHA256      string   `json:"sha256,omitempty"`
	SizeBytes   int64    `json:"size_bytes,omitempty"`
	Key         CacheKey `json:"key"`
	Completed   bool     `json:"completed"`
	Error       string   `json:"error,omitempty"`
}

// PrefetchJob tracks durable chunk-level/file-level bulk download progress across cancellations and restarts.
type PrefetchJob struct {
	JobID          string    `json:"job_id"`
	Description    string    `json:"description"`
	State          string    `json:"state"` // queued | running | completed | cancelled | failed
	CreatedAt      string    `json:"created_at"`
	UpdatedAt      string    `json:"updated_at"`
	TotalItems     int       `json:"total_items"`
	CompletedItems int       `json:"completed_items"`
	TotalBytes     int64     `json:"total_bytes"`
	CompletedBytes int64     `json:"completed_bytes"`
	Items          []JobItem `json:"items"`
	LastError      string    `json:"last_error,omitempty"`
}

// JobCoordinator runs at most one local sequential download worker and persists job checkpoints to jobs/.
type JobCoordinator struct {
	Mgr    *Manager
	mu     sync.Mutex
	active map[string]context.CancelFunc
}

// NewJobCoordinator creates a coordinator bound to a cache Manager.
func NewJobCoordinator(mgr *Manager) *JobCoordinator {
	return &JobCoordinator{
		Mgr:    mgr,
		active: make(map[string]context.CancelFunc),
	}
}

func (jc *JobCoordinator) jobPath(jobID string) (string, error) {
	clean := filepath.Base(strings.TrimSpace(jobID))
	if clean == "" || clean == "." || clean == ".." {
		return "", fmt.Errorf("invalid job_id %q", jobID)
	}
	return jc.Mgr.PathFor("jobs", clean+".json")
}

// SaveJob atomically persists a job record under BINANCE_MCP_DATA_DIR/jobs/.
func (jc *JobCoordinator) SaveJob(job *PrefetchJob) error {
	path, err := jc.jobPath(job.JobID)
	if err != nil {
		return err
	}
	job.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	recomputeCounts(job)
	raw, err := json.MarshalIndent(job, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(path, raw)
}

// LoadJob reads a persisted prefetch job record from disk.
func (jc *JobCoordinator) LoadJob(jobID string) (*PrefetchJob, error) {
	path, err := jc.jobPath(jobID)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var job PrefetchJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

// ListJobs summarizes all durable prefetch jobs sorted newest-first.
func (jc *JobCoordinator) ListJobs() ([]*PrefetchJob, error) {
	jobsDir, err := jc.Mgr.PathFor("jobs")
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return nil, err
	}
	var out []*PrefetchJob
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".json") || strings.Contains(ent.Name(), ".tmp-") {
			continue
		}
		id := strings.TrimSuffix(ent.Name(), ".json")
		if j, lErr := jc.LoadJob(id); lErr == nil {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	return out, nil
}

// StartOrResume creates (if new) or resumes an interrupted/cancelled job asynchronously.
func (jc *JobCoordinator) StartOrResume(description string, items []JobItem, existingJobID string) (*PrefetchJob, error) {
	var job *PrefetchJob
	if existingJobID != "" {
		loaded, err := jc.LoadJob(existingJobID)
		if err != nil {
			return nil, fmt.Errorf("load job %s for resume: %w", existingJobID, err)
		}
		job = loaded
	} else {
		var rnd [6]byte
		_, _ = rand.Read(rnd[:])
		id := fmt.Sprintf("job-%d-%s", time.Now().UTC().Unix(), hex.EncodeToString(rnd[:]))
		job = &PrefetchJob{
			JobID:       id,
			Description: description,
			State:       "queued",
			CreatedAt:   time.Now().UTC().Format(time.RFC3339Nano),
			Items:       items,
		}
	}

	jc.mu.Lock()
	if _, already := jc.active[job.JobID]; already {
		jc.mu.Unlock()
		return job, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	jc.active[job.JobID] = cancel
	job.State = "running"
	job.LastError = ""
	if err := jc.SaveJob(job); err != nil {
		delete(jc.active, job.JobID)
		jc.mu.Unlock()
		cancel()
		return nil, err
	}
	jc.mu.Unlock()

	go jc.runWorker(ctx, job)
	return job, nil
}

// Cancel stops an active download job cleanly while keeping all completed files and .part progress.
func (jc *JobCoordinator) Cancel(jobID string) (*PrefetchJob, error) {
	jc.mu.Lock()
	cancel, ok := jc.active[jobID]
	jc.mu.Unlock()
	if ok {
		cancel()
	}
	job, err := jc.LoadJob(jobID)
	if err != nil {
		return nil, err
	}
	if job.State == "running" || job.State == "queued" {
		job.State = "cancelled"
		_ = jc.SaveJob(job)
	}
	return job, nil
}

func (jc *JobCoordinator) runWorker(ctx context.Context, job *PrefetchJob) {
	defer func() {
		jc.mu.Lock()
		delete(jc.active, job.JobID)
		jc.mu.Unlock()
	}()

	for i := range job.Items {
		if ctx.Err() != nil {
			job.State = "cancelled"
			_ = jc.SaveJob(job)
			return
		}
		item := &job.Items[i]
		if item.Completed {
			continue
		}
		// Skip already locally cached files without redoing work across resume.
		if dest, err := jc.Mgr.PathFor(item.DestRelPath); err == nil {
			if rec, ok := LoadReceipt(dest); ok && (item.SHA256 == "" || strings.EqualFold(rec.SHA256, item.SHA256)) {
				item.Completed = true
				item.Error = ""
				_ = jc.SaveJob(job)
				continue
			}
		}
		_, err := jc.Mgr.EnsureFile(ctx, EnsureOptions{
			DestRelPath:    item.DestRelPath,
			Key:            item.Key,
			SourceURL:      item.SourceURL,
			FallbackURL:    item.FallbackURL,
			ExpectedSHA256: item.SHA256,
			ExpectedBytes:  item.SizeBytes,
		})
		if err != nil {
			if ctx.Err() != nil {
				job.State = "cancelled"
				_ = jc.SaveJob(job)
				return
			}
			item.Error = err.Error()
			job.State = "failed"
			job.LastError = err.Error()
			_ = jc.SaveJob(job)
			return
		}
		item.Completed = true
		item.Error = ""
		_ = jc.SaveJob(job)
	}
	job.State = "completed"
	job.LastError = ""
	_ = jc.SaveJob(job)
}

func recomputeCounts(job *PrefetchJob) {
	job.TotalItems = len(job.Items)
	job.CompletedItems = 0
	job.TotalBytes = 0
	job.CompletedBytes = 0
	for _, it := range job.Items {
		job.TotalBytes += it.SizeBytes
		if it.Completed {
			job.CompletedItems++
			job.CompletedBytes += it.SizeBytes
		}
	}
}

// InspectSummary reports disk usage across cache roots without modifying or deleting cached data.
type InspectSummary struct {
	DataDir          string            `json:"data_dir"`
	TotalFiles       int               `json:"total_files"`
	ReceiptCount     int               `json:"receipt_count"`
	PartialFiles     int               `json:"partial_files"`
	TotalBytes       int64             `json:"total_bytes"`
	BytesByTopFolder map[string]int64  `json:"bytes_by_folder"`
	VerifyFailures   []string          `json:"verify_failures,omitempty"`
	VerifyChecked    int               `json:"verify_checked,omitempty"`
}

// Inspect scans BINANCE_MCP_DATA_DIR and optionally verifies SHA-256 of cached files when verify=true.
func (m *Manager) Inspect(ctx context.Context, verify bool) (*InspectSummary, error) {
	sum := &InspectSummary{
		DataDir:          m.DataDir,
		BytesByTopFolder: make(map[string]int64),
	}
	err := filepath.WalkDir(m.DataDir, func(path string, d fs.DirEntry, wErr error) error {
		if wErr != nil {
			return wErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			if d.Name() == ".locks" {
				return filepath.SkipDir
			}
			return nil
		}
		info, iErr := d.Info()
		if iErr != nil {
			return nil
		}
		rel, rErr := filepath.Rel(m.DataDir, path)
		if rErr != nil {
			return nil
		}
		top := strings.Split(rel, string(filepath.Separator))[0]
		sz := info.Size()
		sum.TotalBytes += sz
		sum.BytesByTopFolder[top] += sz

		name := d.Name()
		if strings.HasSuffix(name, ".receipt.json") {
			sum.ReceiptCount++
			return nil
		}
		if strings.HasSuffix(name, ".part") || strings.HasSuffix(name, ".part.json") {
			sum.PartialFiles++
			return nil
		}
		sum.TotalFiles++

		if verify {
			if rawRec, rErr := os.ReadFile(ReceiptPath(path)); rErr == nil {
				sum.VerifyChecked++
				var rec Receipt
				if jErr := json.Unmarshal(rawRec, &rec); jErr != nil {
					sum.VerifyFailures = append(sum.VerifyFailures, fmt.Sprintf("%s: corrupt receipt %v", rel, jErr))
				} else if rec.SizeBytes != sz {
					sum.VerifyFailures = append(sum.VerifyFailures, fmt.Sprintf("%s: size expected %d got %d", rel, rec.SizeBytes, sz))
				} else {
					actual, _, hErr := FileSHA256(path)
					if hErr != nil {
						sum.VerifyFailures = append(sum.VerifyFailures, fmt.Sprintf("%s: %v", rel, hErr))
					} else if !strings.EqualFold(actual, rec.SHA256) {
						sum.VerifyFailures = append(sum.VerifyFailures, fmt.Sprintf("%s: sha256 expected %s got %s", rel, rec.SHA256, actual))
					}
				}
			}
		}
		return nil
	})
	return sum, err
}
