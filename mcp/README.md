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

## Tools & Capabilities (124 Registered)

1. `query_klines` — Recorder `agg_trade` aggregation, Binance Vision daily/monthly ZIPs (Spot 16-digit microsecond normalized), and REST klines (`v1:` per-symbol cursor pagination).
2. `query_orderbook` — Historical depth reconstruction via Recorder snapshots + `diff_depth` (warm-up/window resync stats & per-symbol coverage) or live REST `/depth`.
3. `query_trades_and_liquidations` — `trade` / `aggTrade` / sampled `forceOrder` liquidations across Recorder, Vision, and REST (`v1:` per-symbol cursor pagination).
4. `query_derivatives_metrics` — Recorder USD-M historical `mark_price_funding`, `open_interest`, `open_interest_hist`, `top_long_short_account`, `top_long_short_position`, `global_long_short`, `taker_buy_sell_vol`, `contract_specs`; plus 4-market REST (`funding_history` with market-wide multi-symbol continuation, `basis`, Options mark/OI live snapshots).
5. `query_market_overview` — `overview` (including `recorded_symbols`, `recorded_streams`, and `raw_access` instructions) + `capabilities` discovery across **124** static entries (`spot`: 15 REST + 10 WS, `usdm`: 34 REST + 11 WS + 1 Vision, `coinm`: 26 REST + 8 WS, `options`: 12 REST + 6 WS + 1 Vision).
6. `manage_local_cache` — `inspect`, `estimate`, `prefetch`, `jobs`, `cancel`, `resume` for Recorder raw `.raw.bmdr.zst` + `.archive-manifest.json` (`selection_time_basis="receive_time_utc"`; omit `streams`/`symbols` to select all ready chunks in a partition/window) and Binance Vision archives.
7. `query_public_endpoint` — Allowlisted public REST GET (including `/api/v3/ticker`, `/api/v3/ticker/tradingDay`, `/fapi/v1/{indexInfo,assetIndex,constituents,insuranceBalance,symbolAdlRisk,rpiDepth,tradingSchedule}`, `/fapi/v2/ticker/price`, `/futures/data/delivery-price`, `/dapi/v1/{premiumIndexKlines,constituents}`) and bounded live WebSocket sampling (`<symbol>`, `<underlying>`, `<interval>`, `<levels>`, `<updateSpeed>`).

> After rebuilding `bin/binance-data-mcp`, restart/reload Codex or OpenCode MCP client sessions so the updated binary takes effect.
