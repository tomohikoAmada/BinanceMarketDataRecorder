package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/binance"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/cache"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/config"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/recorder"
	"github.com/tomohikoAmada/BinanceMarketDataMCP/internal/tools"
)

func buildServer() (*tools.Server, *config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, err
	}
	if err := cfg.EnsureDirectories(); err != nil {
		return nil, nil, err
	}

	accessHost := cfg.RecorderAccessHost()
	headerProvider := func(rawURL string) map[string]string {
		u, uErr := url.Parse(rawURL)
		if uErr != nil {
			return nil
		}
		if accessHost != "" && strings.EqualFold(u.Hostname(), accessHost) && strings.EqualFold(u.Scheme, "https") {
			if cfg.AccessCreds.ClientID != "" && cfg.AccessCreds.ClientSecret != "" {
				return map[string]string{
					"CF-Access-Client-Id":     cfg.AccessCreds.ClientID,
					"CF-Access-Client-Secret": cfg.AccessCreds.ClientSecret,
				}
			}
		}
		return nil
	}

	httpClient := &http.Client{Timeout: cfg.HTTPTimeout}
	cacheMgr := cache.NewManager(cfg.DataDir, httpClient, cfg.RecorderConcurrency, cfg.RecorderRateBPS, headerProvider)
	jobs := cache.NewJobCoordinator(cacheMgr)
	bClient := binance.NewClient(cfg, cacheMgr)
	rClient := recorder.NewClient(cfg, cacheMgr)
	srv := tools.NewServer(cfg, cacheMgr, jobs, bClient, rClient)
	return srv, cfg, nil
}

func runDoctor() int {
	srv, cfg, err := buildServer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "doctor failed: %v\n", err)
		return 1
	}
	summary, _ := srv.CacheMgr.Inspect(context.Background(), false)
	out := map[string]any{
		"status":                   "ok",
		"data_dir":                 cfg.DataDir,
		"recorder_private_url":     cfg.RecorderPrivateURL,
		"recorder_https_url":       cfg.RecorderHTTPSURL,
		"access_token_configured":  cfg.AccessCreds.ClientID != "" && cfg.AccessCreds.ClientSecret != "",
		"binance_api_key_present":  cfg.BinanceAPIKey != "",
		"python_bin":               cfg.PythonBin,
		"worker_script":            cfg.WorkerScript,
		"recorder_repo_dir":        cfg.RecorderRepoDir,
		"capabilities_registered":  len(binance.Capabilities("", "")),
		"cache_summary":            summary,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
	return 0
}

func main() {
	// Strictly log ONLY to stderr so stdout remains clean stdio JSON-RPC.
	log.SetOutput(os.Stderr)

	subcmd := "serve"
	if len(os.Args) > 1 {
		subcmd = strings.ToLower(strings.TrimSpace(os.Args[1]))
	}
	switch subcmd {
	case "doctor":
		os.Exit(runDoctor())
	case "serve", "":
		srv, _, err := buildServer()
		if err != nil {
			log.Fatalf("binance-data-mcp initialization failed: %v", err)
		}
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()
		if err := srv.MCP.Run(ctx, &mcp.StdioTransport{}); err != nil {
			log.Fatalf("binance-data-mcp serve error: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "usage: %s [serve|doctor]\n", os.Args[0])
		os.Exit(2)
	}
}
