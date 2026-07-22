# Crypto Market Data Recorder

Crypto Market Data Recorder is an independent, long-running crypto market-data
recording service for macOS Apple Silicon. It preserves exact public exchange
payloads, proves data quality, recovers from crashes, and publishes generic,
deterministic datasets and replay contracts for arbitrary research, backtest,
and monitoring consumers. Binance is its first adapter, not its project
identity.

## Status

Milestone M0 is complete when the contracts, architecture decisions, full
milestone plan, source inventory, risk register, and minimal offline acceptance
test are committed. M0 intentionally contains no production package,
WebSocket client, REST client, service, CLI, or storage implementation.

M0.1 corrects the project identity and workspace without adding production
code. The next milestone is M1: Python engineering skeleton, configuration, and
CLI.

## Identity

- Repository: `CryptoMarketDataRecorder`
- Distribution: `crypto-market-data-recorder`
- Import package: `crypto_market_data_recorder`
- Future CLI: `crypto-market-recorder`
- Application data: `~/Library/Application Support/CryptoMarketDataRecorder/`

## Boundary

Recorder owns exchange adapters, capture, immutable raw chunks, local
spool/catalog state, quality evidence, deterministic replay inputs,
normalization, manifests, optional external archival, and operational reporting.
Consumers own computation, research, factors, strategies, backtesting, and
monitoring behavior.

Recorder never owns trading, orders, accounts, API keys, strategies, factors,
backtests, or a GUI. The dependency is one-way through published data
contracts; Recorder does not import, modify, or specialize its core for any
consumer.

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

## M0 verification

M0 has no runtime dependencies beyond Python 3.12 for the contract verifier.
If pytest is available, run both gates:

```bash
python3.12 -m pytest -q
python3.12 tests/verify_m0_contracts.py
```

Production installation and CLI commands intentionally arrive in M1.
