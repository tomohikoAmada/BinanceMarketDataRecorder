# Binance Market Data Recorder

> **Mac Developer Preview — `0.1.0a1`**
>
> This independent, unofficial project records only Binance public market
> data. It has no API-key configuration, account interface, or order function.
> Long-running acceptance has not been completed. Do not use it for
> real-money trading.

Binance Market Data Recorder is not affiliated with, maintained by, sponsored
by, or endorsed by Binance. The name identifies the public data source; the
project does not use Binance logos or claim an official relationship.
This project is specifically for Binance public market data.

连续72小时和168小时长期运行验收尚未执行。
静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。
当前版本仅为Mac Developer Preview，不得用于真实资金交易。

## What it records

- BTCUSDT Binance Spot diff depth at 100 ms, aggregate trades, book ticker,
  and public REST depth snapshots.
- BTCUSDT Binance USD-M perpetual equivalents.
- Failure-isolated USD-M public auxiliary data: mark/index/premium, funding,
  open interest, liquidation events, and exchange/filter snapshots.

Exact WebSocket payload bytes and receive/exchange timing are written first to
an internal append-only Raw spool. Normalized Parquet and replay are derived,
versioned outputs. Optional external archival uses only a folder explicitly
registered by the user.

The Recorder does not implement accounts, credentials, orders, strategies,
factors, backtests, a GUI, or support for another exchange.

## Identity

- Distribution: `binance-market-data-recorder`
- Import package: `binance_market_data_recorder`
- CLI: `binance-market-recorder`
- Python: 3.12
- Certified platform: macOS Apple Silicon, logged-in-user LaunchAgent
- Default data root:
  `~/Library/Application Support/BinanceMarketDataRecorder/`

## Install

Install the wheel from the Developer Preview bundle into a clean Python 3.12
virtual environment:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install dist/binance_market_data_recorder-0.1.0a1-py3-none-any.whl
binance-market-recorder --version
binance-market-recorder doctor
binance-market-recorder status
```

`doctor` is offline. `status` reports `NOT_RUNNING` unless a live service PID
has a fresh heartbeat; it never invents Collector health.

For a source checkout:

```bash
python3.12 -m pip install --require-hashes \
  -r requirements/macos-arm64-python312.lock
python3.12 -m pip install -e '.[dev]'
python3.12 -m pytest -q
python3.12 -m ruff check .
python3.12 -m mypy
python3.12 tests/verify_m0_contracts.py
go run tools/verify_raw_chunk_golden.go
```

## Documentation

The concise operator-facing set is:

- [macOS quickstart](docs/quickstart_macos.md)
- [architecture](docs/architecture.md)
- [data and storage](docs/data_and_storage.md)
- [operations](docs/operations.md)
- [known limitations](docs/known_limitations.md)
- [official Binance sources](docs/binance_sources.md)

Detailed contracts, ADRs, milestone evidence, and historical acceptance records
remain under `docs/`; they are engineering evidence rather than duplicate
operator guides.

## Safety boundary

Collector writes target internal application storage only. Recorder never
formats, repairs, remounts, or claims an external volume. After an external
artifact is fully reread, size/hash verified, atomically committed, and
recorded in the Catalog, policy may delete its internal copy. The external
artifact can then be the only Raw copy; this is not a backup policy.

LaunchAgent installation is rootless and requires an author-controlled
reverse-DNS label ending in `.BinanceMarketDataRecorder`. Namespaces resembling
an official Binance-owned namespace are rejected. Uninstalling the LaunchAgent
removes service registration only and never deletes the data root.
