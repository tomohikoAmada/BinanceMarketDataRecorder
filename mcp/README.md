# BinanceMarketDataMCP (`BinanceMarketDataRecorder/mcp`)

Local Go stdio MCP server integrating:
1. **BinanceMarketDataRecorder** synced snapshots (Cloudflare WARP private `http://10.250.250.10:18080` + Cloudflare Access HTTPS `https://recorder-data.nanoblade.top`), reusing the Recorder Python offline `load_source_chunks` / `iter_source_records` / `LocalBookReconstructor` reader via `python/worker.py`.
2. **Binance Public Vision Archives** (`data.binance.vision`) with `.CHECKSUM` SHA-256 verification and Spot >= 2025-01-01 16-digit microsecond timestamp normalization.
3. **Binance Public REST & Bounded WebSocket** across all four markets (`spot`, `usdm`, `coinm`, `options`).

## Quick Start

```sh
cd mcp
go build -o bin/binance-data-mcp ./cmd/binance-data-mcp
./bin/binance-data-mcp doctor
```

Register in Codex / OpenCode:

```sh
codex mcp add binance_data \
  --env BINANCE_MCP_DATA_DIR=$HOME/binance-mcp-data \
  --env SBMDR_EXPORT_HTTPS_URL=https://recorder-data.nanoblade.top \
  --env SBMDR_EXPORT_PRIVATE_URL=http://10.250.250.10:18080 \
  --env SBMDR_ACCESS_TOKEN_FILE=$HOME/.config/sbmdr/access-service-token.json \
  -- $(pwd)/bin/binance-data-mcp serve

opencode mcp add --global \
  --env BINANCE_MCP_DATA_DIR=$HOME/binance-mcp-data \
  --env SBMDR_EXPORT_HTTPS_URL=https://recorder-data.nanoblade.top \
  --env SBMDR_EXPORT_PRIVATE_URL=http://10.250.250.10:18080 \
  --env SBMDR_ACCESS_TOKEN_FILE=$HOME/.config/sbmdr/access-service-token.json \
  binance_data -- $(pwd)/bin/binance-data-mcp serve
```

Then run OpenCode with DeepSeek:
```sh
opencode --model deepseek/deepseek-v4-pro
```

Environment variables:
- `BINANCE_MCP_DATA_DIR` (default `~/binance-mcp-data`)
- `SBMDR_EXPORT_HTTPS_URL` (e.g. `https://recorder-data.nanoblade.top`)
- `SBMDR_EXPORT_PRIVATE_URL` (e.g. `http://10.250.250.10:18080`)
- `SBMDR_ACCESS_TOKEN_FILE` (e.g. `~/.config/sbmdr/access-service-token.json`)
- `SBMDR_REPO_DIR` / `SBMDR_PYTHON_BIN` (auto-detected from `mcp/bin/binance-data-mcp` layout or overridden explicitly)
- `BINANCE_API_KEY` (optional, only for endpoints marked `api_key_required`)

## Tools
1. `query_klines`
2. `query_orderbook`
3. `query_trades_and_liquidations`
4. `query_derivatives_metrics`
5. `query_market_overview` (`overview` + `capabilities`)
6. `manage_local_cache` (`inspect`, `estimate`, `prefetch`, `jobs`, `cancel`, `resume`)
7. `query_public_endpoint`
