# Binance Market Data Recorder Agent Contract

## Pragmatic Engineering Principles (No Over-Engineering or Over-Defense)

1. **Simplicity & High Throughput First**: Favor straightforward, maintainable Python code and low-I/O paths over defensive ceremony, multi-layer wrappers, or speculative abstractions.
2. **Trust In-Process Memory & OS `fsync`**:
   - Do not add defensive deep copies of internal objects/dicts passed between author-controlled functions in the same process.
   - During live capture, streaming write-time `CRC32C` + `SHA-256` calculation followed by `fsync` and atomic rename is sufficient to seal a chunk. Do **not** immediately re-read and decompress sealed `.zst` files from disk on the live capture path or run background live re-verification loops. Reserve full-file readback for `.partial` crash recovery, cross-volume archive copies, and manual `verify-raw` CLI checks.
3. **Pragmatic Metadata & State Tracking**:
   - Use `PRAGMA journal_mode = WAL` and `PRAGMA synchronous = NORMAL` for routine `Catalog` metadata updates so SQLite commits do not block the capture loop on disk `fsync`.
   - Track per-source health state transitions (`AVAILABLE` ↔ `UNAVAILABLE`/`UNKNOWN`) when they actually change, coalescing steady-state health watermarks in memory rather than flooding SQLite with per-poll rows.
   - Do not create 0-record `.zst` files or extra boundary files on process startup; record startup boundaries in `Catalog` and on the first real chunk's manifest.
4. **No Redundant Data Subscriptions**:
   - Persist closed K-line bars (`k.x == True`) with automatic REST backfill on reconnect gaps; filter intermediate forming updates (`k.x == False`) in memory.
   - Subscribe only to the 6 distinct timezone-dependent intervals (`6h, 12h, 1d, 3d, 1w, 1M`) on `S12` (`@+08:00`), avoiding duplicate subscriptions for the 10 intervals already identically covered by `S11` (UTC).

## Current Milestone Direction (November 2026)

The previous P0–P6 milestone plan has been **abolished and superseded** by the pragmatic Python optimization & qualification plan in [`docs/production_milestone_plan.md`](docs/production_milestone_plan.md) (`M1` → `M2` → `M3` → `M4`):
- **`M1` (Core I/O & Hot-Path Optimization)**: `15m / 128 MiB` chunk rotation (`M1.1`), SQLite `WAL + NORMAL` with in-memory availability coalescing (`M1.2`), closed-bar K-line persistence with disconnect REST backfill & `S12` deduplication (`M1.3`), and zero-copy hot-path framing + single-hop REST persistence (`M1.4`).
- **`M2` (De-bloating & Removing Over-Defense)**: Remove live post-seal full-file readback and background re-verification (`M2.1`), remove startup 0-record `.zst` files (`M2.2`), unify the dual collector stack and WebSocket base (`M2.3`), and isolate peripheral/legacy modules via lazy CLI imports while splitting `storage/catalog.py` (`M2.4`).
- **`M3` (Benchmark & Stability Convergence)**: Multi-rotation (`45–60m`) three-way benchmark (`M3.1`), full test/stress/recovery suite (`M3.2`), and production wheel/config freeze (`M3.3`).
- **`M4` (GreenCloud VPS Qualification)**: Single continuous `BTC + ETH` run verified at **`2h` → `12h` → `24h`** checkpoints (`M4.1`–`M4.4`).

Historical P0–P4 records are condensed in [`docs/history/pre-P0-AGENTS.md`](docs/history/pre-P0-AGENTS.md) and [`docs/milestones/`](docs/milestones/).

## Frozen project identity

- Display name: **Binance Market Data Recorder**
- Repository directory: `BinanceMarketDataRecorder`
- Repository path:
  macOS `/Users/amada/Documents/Development/Crypto/BinanceMarketDataRecorder`;
  Ubuntu ARM64 `/home/orangepi/BinanceMarketDataRecorder`
- Python distribution: `binance-market-data-recorder`
- Python import package: `binance_market_data_recorder`
- CLI: `binance-market-recorder`
- macOS application data:
  `~/Library/Application Support/BinanceMarketDataRecorder/`
- Linux interactive data:
  `~/.local/share/BinanceMarketDataRecorder/`
- Linux system service data/config:
  `/var/lib/binance-market-data-recorder/` and
  `/etc/binance-market-data-recorder/recorder.toml`

## Non-goals

- No current GUI, web frontend, FastAPI product API, or trading interface.
- No factors, Alpha DSL, strategies, positions, backtests, account ledger, or live/simulated execution.
- No account endpoints, orders, API keys, secrets, or credential discovery.
- No other exchanges or unbounded all-exchange subscriptions.
- No Docker as the certified V1 deployment, Kafka, Kubernetes, or cloud-first stateless collection.
- No automatic formatting, repair, repartitioning, or exclusive ownership of an external volume or VPS host.
- No production data under the repository, Desktop, Documents, iCloud Drive, or `/tmp`.

## Architecture boundary

Dependency direction is one-way:

```text
Binance public APIs
  -> Binance Spot / USD-M / COIN-M modules
  -> Binance Market Data Recorder
  -> immutable raw chunks
  -> normalized datasets and manifests
  -> generic replay and consumer contracts
  -> arbitrary research, backtest, and monitoring consumers
```

Recorder is an independent repository and service. Consumers may depend only on its published contracts, Catalog read interfaces, manifests, and replay APIs. Alpha101Crypto is one optional external consumer example; it has no privileged influence on Recorder protocols. Supporting another exchange requires a separate future architecture review.

## Core Data-Integrity Rules

1. Collector writes only to the internal active area (`~/Library/Application Support/BinanceMarketDataRecorder/` on macOS, `~/.local/share/BinanceMarketDataRecorder/` or `/var/lib/binance-market-data-recorder` on Linux). Never use the repository or `/Users/amada/Documents/Development/Crypto` as a production data root.
2. Preserve exact WebSocket payload bytes and single primary REST representations (ADR-0053) with exchange times, receive UTC/monotonic times, market, symbol, stream, connection ID, and sequence IDs in Raw v1 format (`ADR-0010`).
3. Raw files are append-only while active (`.partial`) and immutable after sealing (`.zst` + `.json`).
4. Truncated or checksum-failed `.partial` tails are recovered to the last valid frame or quarantined on startup; never label an incomplete interval as complete.
5. Never silently delete unarchived raw data. At the hard disk reserve, seal gracefully, stop collection, and emit `DISK_EMERGENCY_STOP`. Cross-volume archive copies verify the target file before deleting local source chunks.

## Official-Source, SDK, and Security Policy

- For Binance behavior, consult official Binance documentation (`llms.txt`, developer portal pages/changelogs, `binance/binance-connector-python`, `binance/binance-spot-api-docs`, `binance/binance-toolbox-python`) and record new source references in `docs/binance_sources.md`. Do not load all of `llms-full.txt` by default.
- Use `binance-sdk-spot` and `binance-sdk-derivatives-trading-usds-futures` where appropriate. Do not add deprecated `binance-futures-connector-python` or `python-binance`.
- Never place trades, call account or order endpoints, read API keys, inspect credential stores, add secret fields to configuration, or request trading permissions. Public, unsigned market-data endpoints only.
- Never emit a raw proxy URL into logs, status, Raw, manifests, or Catalog records.

## Development Workflow & Remote Safety

1. Follow the owner's latest task and current code; check Git status and preserve unrelated changes.
2. Keep changes focused and practical; run relevant checks (`pytest`, `ruff`, `mypy`, `python3.12 tests/verify_m0_contracts.py`, `git diff --check`).
3. **Remote SSH & Cleanup Safety**: Never recursively remove `/`, `/root`, home directories, `/etc`, `/opt`, `/var`, `/srv`, mount roots, or unvalidated shell variables across SSH. Record `systemctl is-active` and `systemctl is-enabled` during service maintenance.
