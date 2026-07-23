# Binance Market Data Recorder

Binance Market Data Recorder is an independent, unofficial project. It is not
affiliated with, maintained by, sponsored by, or endorsed by Binance.

It is a long-running recorder specifically for Binance public market data on
macOS Apple Silicon. It preserves exact Binance payloads, proves data quality,
recovers from crashes, and publishes deterministic datasets and generic replay
contracts for research, backtest, monitoring, and simulation consumers. The
project name identifies its data source; it does not claim an official
relationship. This project does not use Binance logos or official visual
identity.

## Status

M4 and M5 provide independent Binance Spot and USD-M BTCUSDT public-market
Collector libraries for diff depth at 100 ms, aggregate trades, book ticker,
and official-SDK REST depth snapshots. They write only to the internal Raw
spool. M6 reconstructs deterministic local books, detects sequence gaps,
audits best levels/bookTicker, and writes derived checkpoints. M7 adds
failure-isolated USD-M mark/index/funding, open-interest, liquidation and
exchange/filter data. M8 adds idempotent per-stream operational aggregates,
UTC JSON/CSV daily reports and honest structured status. M9 registers and
re-resolves optional macOS external directories. M10 safely copies, fully
verifies, commits, and then separately deletes eligible internal Raw chunks.
Normalization, launchd service installation, accounts, credentials, and
trading remain unimplemented.

## Identity

- Repository: `BinanceMarketDataRecorder`
- Distribution: `binance-market-data-recorder`
- Import package: `binance_market_data_recorder`
- CLI: `binance-market-recorder`
- Application data: `~/Library/Application Support/BinanceMarketDataRecorder/`

## Boundary

Recorder owns Binance Spot/USD-M modules, capture, immutable raw chunks, local
spool/catalog state, quality evidence, deterministic replay inputs,
normalization, manifests, optional external archival, and operational reporting.
Consumers own computation, research, factors, strategies, backtesting, and
monitoring behavior.

Recorder never owns trading, orders, accounts, API keys, strategies, factors,
backtests, or a GUI. The dependency is one-way through published data
contracts; Recorder does not import, modify, or specialize its core for any
consumer.

Support for another exchange is not a V1 goal. It would require a separate
architecture review rather than speculative abstraction in the current code.

## V1 capture scope

- Binance Spot BTCUSDT: diff depth at 100 ms, aggregate trades, book ticker,
  and public REST depth snapshots.
- Binance USD-M BTCUSDT perpetual: the same three streams and public REST depth
  snapshots.
- Isolated USD-M side data: mark/index/premium, funding, open interest,
  liquidation events, and exchange/filter snapshots.

All live data is written first to internal storage. External archive folders
are optional and may disappear without stopping capture.

## Documents

- [Project contract](docs/project_contract.md)
- [Architecture](docs/architecture.md)
- [Milestone plan](docs/milestone_plan.md)
- [Requirements traceability](docs/requirements_traceability.md)
- [Data contract](docs/data_contract.md)
- [Storage contract](docs/storage_contract.md)
- [macOS operations contract](docs/macos_operations.md)
- [Official Binance sources](docs/binance_sources.md)
- [Risk register](docs/risk_register.md)
- [Architecture decisions](docs/adr/README.md)
- [M0.1 acceptance](docs/milestone_acceptance/M0.1.md)
- [M0.2 acceptance](docs/milestone_acceptance/M0.2.md)
- [M1 acceptance](docs/milestone_acceptance/M1.md)
- [M2 acceptance](docs/milestone_acceptance/M2.md)
- [M3 acceptance](docs/milestone_acceptance/M3.md)
- [M4 acceptance](docs/milestone_acceptance/M4.md)
- [M5 acceptance](docs/milestone_acceptance/M5.md)
- [M6 acceptance](docs/milestone_acceptance/M6.md)
- [M7 acceptance](docs/milestone_acceptance/M7.md)
- [M8 acceptance](docs/milestone_acceptance/M8.md)
- [M9 acceptance](docs/milestone_acceptance/M9.md)
- [M10 acceptance](docs/milestone_acceptance/M10.md)

## Install and verify M10

Use Python 3.12 in a virtual environment:

```bash
python3.12 -m pip install --require-hashes \
  -r requirements/macos-arm64-python312.lock
python3.12 -m pip install -e '.[dev]'
binance-market-recorder --version
binance-market-recorder config show
binance-market-recorder doctor
binance-market-recorder status
binance-market-recorder report daily --date 2026-07-22
binance-market-recorder storage list
binance-market-recorder storage inspect /Volumes/Archive/QuantData/Recorder
binance-market-recorder storage register /Volumes/Archive/QuantData/Recorder
binance-market-recorder storage status
binance-market-recorder archive status
binance-market-recorder archive retry
binance-market-recorder archive verify <storage-id>
python3.12 -m pytest -q
python3.12 -m ruff check .
python3.12 -m mypy
python3.12 tests/verify_m0_contracts.py
```

Configuration precedence is defaults, an optional TOML file, then environment.
Pass a file with `--config PATH` or
`BINANCE_MARKET_RECORDER_CONFIG_FILE`. Supported settings are `data_root`,
`log_level`, `rotation_seconds`, `rotation_bytes`,
`durability_interval_seconds`, `ingress_queue_capacity`, and
`max_frame_bytes`; unknown settings are rejected. The default data root is
`~/Library/Application Support/BinanceMarketDataRecorder/`, and diagnostic CLI
commands do not create it.

External storage is optional. Discovery uses macOS Disk Arbitration and volume
UUIDs; displayed mountpoint/name values are not identity. `storage list` and
`inspect` never write. Registration requires an existing folder below the
volume root, then performs write/fsync/rename/readback only inside that folder
and creates its marker there. Recorder never formats, repairs, remounts, or
claims the whole volume. M10 archive retry selects one oldest sealed chunk,
copies it under the registered folder, fsyncs and fully re-reads it, checks size
and SHA-256, commits an external manifest and Catalog state, and only then
deletes the internal artifact. After that deletion, the external artifact may
be the only Raw copy; this feature is not a backup policy.

```toml
[recorder]
data_root = "/safe/absolute/path"
log_level = "INFO"
rotation_seconds = 60.0
rotation_bytes = 134217728
durability_interval_seconds = 1.0
ingress_queue_capacity = 8192
max_frame_bytes = 16777216
```

See [dependency policy](docs/dependency_policy.md) for the deliberately small
runtime and development dependency sets. M2 selected exact-pinned official
Spot and USD-M SDKs for unsigned public REST snapshots and `websockets` for
market-stream transport. M4/M5 use the official SDKs only for public
`GET /api/v3/depth` and `GET /fapi/v1/depth` snapshots. Each market uses three
independent documented raw WebSocket streams. The implementation never reads
credentials or calls account/order APIs.

The documentation updater is intentionally selective and writes to the user
cache by default:

```bash
python3.12 tools/update_binance_docs.py
python3.12 tools/probe_binance_transports.py
```

The updater downloads only allowlisted official sources, refuses
`llms-full.txt` by default, and never executes remote content. The capability
probe is offline by default. Its `--online-rest` option makes only unsigned
public depth requests and is never part of the default test suite.

The M5 combined live gate is explicit and excluded unless enabled. It requires
at least 1,800 seconds and writes only to pytest's temporary directory:

```bash
BINANCE_MARKET_RECORDER_ONLINE=1 \
  python3.12 -m pytest -m online -s -q \
  tests/integration/test_spot_usdm_live.py
```

M3's resource-intensive Raw gate remains separately available:

```bash
python3.12 -m pytest -m stress -q tests/stress/test_million_events.py
go run tools/verify_raw_chunk_golden.go
```
