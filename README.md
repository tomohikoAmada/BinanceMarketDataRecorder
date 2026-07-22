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

Milestone M0 is complete when the contracts, architecture decisions, full
milestone plan, source inventory, risk register, and minimal offline acceptance
test are committed. M0 intentionally contains no production package,
WebSocket client, REST client, service, CLI, or storage implementation.

M0.2 finalizes the Binance-specific project identity and workspace without
adding production code. The next milestone is M1: Python engineering skeleton,
configuration, and CLI.

## Identity

- Repository: `BinanceMarketDataRecorder`
- Distribution: `binance-market-data-recorder`
- Import package: `binance_market_data_recorder`
- Future CLI: `binance-market-recorder`
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

## M0 verification

M0 has no runtime dependencies beyond Python 3.12 for the contract verifier.
If pytest is available, run both gates:

```bash
python3.12 -m pytest -q
python3.12 tests/verify_m0_contracts.py
```

Production installation and CLI commands intentionally arrive in M1.
