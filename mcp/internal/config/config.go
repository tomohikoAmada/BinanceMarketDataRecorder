package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// AccessCredentials holds Cloudflare Zero Trust Service Token headers for the Recorder HTTPS origin.
type AccessCredentials struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

// Config centralizes runtime paths, endpoints, bounded concurrency, and rate limits for binance-data-mcp.
type Config struct {
	DataDir             string
	RecorderHTTPSURL    string
	RecorderPrivateURL  string
	AccessTokenFile     string
	AccessCreds         AccessCredentials
	RecorderRepoDir     string
	PythonBin           string
	WorkerScript        string
	BinanceAPIKey       string
	RecorderConcurrency int
	BinanceConcurrency  int
	RecorderRateBPS     int64
	PreviewDefaultRows  int
	PreviewMaxRows      int
	ResponseBudgetBytes int
	HTTPTimeout         time.Duration
}

// Load reads environment configuration cleanly and resolves absolute workspace/cache paths.
func Load() (*Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home directory: %w", err)
	}

	dataDir := strings.TrimSpace(os.Getenv("BINANCE_MCP_DATA_DIR"))
	if dataDir == "" {
		dataDir = filepath.Join(home, "binance-mcp-data")
	}
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve BINANCE_MCP_DATA_DIR %q: %w", dataDir, err)
	}

	recorderRepo := strings.TrimSpace(os.Getenv("SBMDR_REPO_DIR"))
	if recorderRepo == "" {
		recorderRepo = strings.TrimSpace(os.Getenv("SBMDR_RECORDER_REPO"))
	}
	exePath, exeErr := os.Executable()
	if recorderRepo == "" && exeErr == nil {
		// Binary lives under <repo>/mcp/bin/binance-data-mcp -> parent of mcp/ is repo root.
		candRepo := filepath.Dir(filepath.Dir(filepath.Dir(exePath)))
		if _, stErr := os.Stat(filepath.Join(candRepo, "vendor", "python-baseline")); stErr == nil {
			recorderRepo = candRepo
		}
	}
	if recorderRepo == "" {
		if cwd, cErr := os.Getwd(); cErr == nil {
			for cur := cwd; ; cur = filepath.Dir(cur) {
				if _, stErr := os.Stat(filepath.Join(cur, "vendor", "python-baseline")); stErr == nil {
					recorderRepo = cur
					break
				}
				if filepath.Dir(cur) == cur {
					break
				}
			}
		}
	}

	pythonBin := strings.TrimSpace(os.Getenv("SBMDR_PYTHON_BIN"))
	if pythonBin == "" {
		if recorderRepo != "" {
			pythonBin = findVenvPython(recorderRepo, runtime.GOOS)
		}
		if pythonBin == "" {
			pythonBin = "python3"
		}
	}

	workerScript := strings.TrimSpace(os.Getenv("SBMDR_WORKER_SCRIPT"))
	if workerScript == "" {
		if exeErr == nil {
			for _, rel := range []string{
				filepath.Join(filepath.Dir(exePath), "python", "worker.py"),
				filepath.Join(filepath.Dir(filepath.Dir(exePath)), "python", "worker.py"),
			} {
				if _, stErr := os.Stat(rel); stErr == nil {
					workerScript = rel
					break
				}
			}
		}
		if workerScript == "" && recorderRepo != "" {
			cand := filepath.Join(recorderRepo, "mcp", "python", "worker.py")
			if _, stErr := os.Stat(cand); stErr == nil {
				workerScript = cand
			}
		}
	}

	tokenFile := strings.TrimSpace(os.Getenv("SBMDR_ACCESS_TOKEN_FILE"))
	if tokenFile == "" {
		tokenFile = filepath.Join(home, ".config", "sbmdr", "access-service-token.json")
	}

	cfg := &Config{
		DataDir:             absDataDir,
		RecorderHTTPSURL:    strings.TrimRight(envOr("SBMDR_EXPORT_HTTPS_URL", "https://recorder-data.nanoblade.top"), "/"),
		RecorderPrivateURL:  strings.TrimRight(envOr("SBMDR_EXPORT_PRIVATE_URL", "http://10.250.250.10:18080"), "/"),
		AccessTokenFile:     tokenFile,
		RecorderRepoDir:     recorderRepo,
		PythonBin:           pythonBin,
		WorkerScript:        workerScript,
		BinanceAPIKey:       strings.TrimSpace(os.Getenv("BINANCE_API_KEY")),
		RecorderConcurrency: envInt("SBMDR_MAX_CONCURRENCY", 1, 1, 8),
		BinanceConcurrency:  envInt("BINANCE_MAX_CONCURRENCY", 2, 1, 8),
		RecorderRateBPS:     envInt64("SBMDR_RATE_LIMIT_BPS", 2*1024*1024, 64*1024, 256*1024*1024),
		PreviewDefaultRows:  100,
		PreviewMaxRows:      1000,
		ResponseBudgetBytes: envInt("MCP_RESPONSE_BUDGET_BYTES", 64*1024, 8*1024, 256*1024),
		HTTPTimeout:         time.Duration(envInt("MCP_HTTP_TIMEOUT_SECONDS", 60, 5, 600)) * time.Second,
	}

	if id := strings.TrimSpace(os.Getenv("CF_ACCESS_CLIENT_ID")); id != "" {
		cfg.AccessCreds.ClientID = id
	}
	if sec := strings.TrimSpace(os.Getenv("CF_ACCESS_CLIENT_SECRET")); sec != "" {
		cfg.AccessCreds.ClientSecret = sec
	}
	if (cfg.AccessCreds.ClientID == "" || cfg.AccessCreds.ClientSecret == "") && cfg.AccessTokenFile != "" {
		if raw, readErr := os.ReadFile(cfg.AccessTokenFile); readErr == nil {
			var parsed AccessCredentials
			if jsonErr := json.Unmarshal(raw, &parsed); jsonErr == nil {
				if cfg.AccessCreds.ClientID == "" {
					cfg.AccessCreds.ClientID = strings.TrimSpace(parsed.ClientID)
				}
				if cfg.AccessCreds.ClientSecret == "" {
					cfg.AccessCreds.ClientSecret = strings.TrimSpace(parsed.ClientSecret)
				}
			}
		}
	}

	if err := cfg.EnsureDirectories(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// EnsureDirectories initializes the canonical BINANCE_MCP_DATA_DIR layout.
func (c *Config) EnsureDirectories() error {
	for _, rel := range []string{"recorder", "binance_vision", "rest_cache", "results", "jobs"} {
		if err := os.MkdirAll(filepath.Join(c.DataDir, rel), 0o755); err != nil {
			return fmt.Errorf("create cache dir %s: %w", rel, err)
		}
	}
	return nil
}

// RecorderAccessHost returns the HTTPS hostname allowed to receive CF-Access service headers.
func (c *Config) RecorderAccessHost() string {
	u, err := url.Parse(c.RecorderHTTPSURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
}

// IsBinanceOrigin checks whether a hostname belongs to Binance public API endpoints.
func IsBinanceOrigin(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	h := strings.ToLower(u.Hostname())
	return h == "api.binance.com" || h == "fapi.binance.com" || h == "dapi.binance.com" || h == "eapi.binance.com"
}

func envOr(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

func envInt(k string, def, minV, maxV int) int {
	raw := strings.TrimSpace(os.Getenv(k))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < minV || n > maxV {
		return def
	}
	return n
}

func envInt64(k string, def, minV, maxV int64) int64 {
	raw := strings.TrimSpace(os.Getenv(k))
	if raw == "" {
		return def
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < minV || n > maxV {
		return def
	}
	return n
}

func findVenvPython(repoDir, goos string) string {
	candidates := []string{
		filepath.Join(repoDir, ".venv", "bin", "python"),
		filepath.Join(repoDir, ".venv", "Scripts", "python.exe"),
	}
	if goos == "windows" {
		candidates = []string{
			filepath.Join(repoDir, ".venv", "Scripts", "python.exe"),
			filepath.Join(repoDir, ".venv", "bin", "python"),
		}
	}
	for _, candidate := range candidates {
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate
		}
	}
	return ""
}
