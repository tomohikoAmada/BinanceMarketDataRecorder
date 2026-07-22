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

M1 provides an installable, offline Python 3.12 skeleton, strict credential-free
configuration, structured logging, and diagnostic CLI. It does not connect to
Binance and does not yet implement a Collector or running service.

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
- Later, isolated USD-M side data: mark/index/premium, funding, open interest,
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

## Install and verify M3

Use Python 3.12 in a virtual environment:

```bash
python3.12 -m pip install --require-hashes \
  -r requirements/macos-arm64-python312.lock
python3.12 -m pip install -e '.[dev]'
binance-market-recorder --version
binance-market-recorder config show
binance-market-recorder doctor
binance-market-recorder status
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
runtime and development dependency sets. M2 adds exact-pinned official Spot and
USD-M SDKs for unsigned public REST snapshots and `websockets` for the future
market-stream transport. It does not add a Collector or open a WebSocket.

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

M3 adds no network Collector. It provides the executable EventEnvelope and
internal Raw/Catalog foundation described by [ADR-0010](docs/adr/0010-raw-chunk-v1-byte-format.md).
Run the resource-intensive acceptance gate separately:

```bash
python3.12 -m pytest -m stress -q tests/stress/test_million_events.py
go run tools/verify_raw_chunk_golden.go
```
