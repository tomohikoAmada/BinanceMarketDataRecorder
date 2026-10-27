# Binance Market Data Recorder

A Python 3.12 recorder for Binance Spot and USD-M perpetual public market data.
It captures original payloads, stores immutable Raw chunks, tracks gaps and
recovery, and provides verified archival, Parquet normalization, and deterministic
replay for downstream research.

## Status

Version `0.1.0a1`. Multi-product collection and V5 acceptance evidence are
implemented. Production qualification remains incomplete; deployment and
qualification records are specific to their source revision and run.

See [current deployment state](docs/CURRENT_PRODUCTION_STATE.md) and the
[developer handoff](docs/PROJECT_HANDOFF.md).

The [current production milestone plan](docs/production_milestone_plan.md)
replaces the retired plan as of October 26, 2026. It designs for at least ten
assets and targets stable BTC/ETH production across Spot, USD-M and COIN-M
perpetual/delivery markets, with 99.5% per-source continuity. [P0 review corrections are complete, including the remaining R2 classifier fix](docs/milestones/P0/review-20261026.md); retained boundaries have an appended v4 classification and the NON_STEADY profile remains diagnostic only. [P1 passed independent re-review, including its R1 fix](docs/milestones/P1/README.md); its local interleaved results and limits remain explicit. Expanded
market support and production readiness are not yet achieved.
Historical plans and acceptance evidence remain preserved.

The [pre-milestone research](docs/research/2026-10-26/README.md) compares reusable
projects, describes the expanded Spot/USD-M/COIN-M scope, and links the first
reproducible VPS core-path baseline and flamegraphs. It is research evidence,
not a full production-capacity result.

## Features

- Configurable Spot and USD-M products in one Recorder process.
- 100 ms depth updates, aggregate trades, book ticker, and REST depth snapshots.
- USD-M mark price, liquidation, funding, open interest, and period statistics;
  Spot and USD-M exchange metadata.
- Exact payload bytes with receive/exchange timestamps and connection provenance.
- Framed Raw storage, compression, checksums, manifests, and SQLite lifecycle records.
- Order-book reconstruction, explicit gap/resync evidence, and crash recovery.
- Verified archival to registered storage, with source retirement after verification.
- Rebuildable Parquet datasets, deterministic replay, and historical archive imports.
- Status, daily reports, capacity forecasts, launchd, and systemd integration.

[Data coverage](docs/data_coverage.md) lists individual streams and their limits.

## Install for development

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
binance-market-recorder --version
binance-market-recorder doctor
binance-market-recorder config show
```

Python 3.12 is required. Platform dependency locks are in `requirements/`;
see [dependency policy](docs/dependency_policy.md) for reproducible builds.

## Configure products

Save a TOML configuration outside the repository:

```toml
[recorder]
spot_symbols = ["BTCUSDT", "ETHUSDT"]
usdm_symbols = ["BTCUSDT", "ETHUSDT"]
```

```bash
binance-market-recorder --config /path/to/recorder.toml config show
binance-market-recorder --config /path/to/recorder.toml doctor
binance-market-recorder --config /path/to/recorder.toml status
```

When both symbol fields are absent, the default is BTCUSDT in both markets.
When either field is supplied, an omitted sibling is empty. At least one product
is required. Product changes take effect on restart.

Default data locations:

| Profile | Data root |
|---|---|
| macOS user | `~/Library/Application Support/BinanceMarketDataRecorder/` |
| Linux user | `~/.local/share/BinanceMarketDataRecorder/` |
| Linux system service | `/var/lib/binance-market-data-recorder/` |

The active writer uses internal storage; registered archive folders receive
verified sealed chunks. Keep production data outside the source checkout.

## Run and inspect

Collection runs through launchd or systemd. Follow the platform setup guide:

- [macOS quickstart](docs/quickstart_macos.md)
- [Ubuntu ARM64 operations](docs/ubuntu_rk3588_operations.md)
- [Ubuntu x86_64 VPS operations](docs/vps_operations.md)

Common commands:

```bash
binance-market-recorder status
binance-market-recorder report daily --date 2026-10-01
binance-market-recorder storage status
binance-market-recorder storage forecast
binance-market-recorder archive status
binance-market-recorder archive status --details --limit 100 --offset 0
binance-market-recorder normalize run
binance-market-recorder normalize status
```

Use `--config /path/to/recorder.toml` before the command to select a configuration.
Subcommand help lists available options.

`archive status` returns compact totals and backlog. Use `--details` for a bounded
transaction page; scripts reading the `transactions` array must request details.

## Data flow

```text
Public WebSocket / REST
  -> Spot and USD-M collectors
  -> Raw chunks + manifests + Catalog
  -> verified archive
  -> normalization / replay
  -> downstream consumers
```

Raw preserves capture evidence. Normalized datasets can be rebuilt from Raw;
replay provides deterministic ordering and explicit gap handling. Historical
imports retain their archive-source clock rather than inventing receive times.
See the [consumer contract](docs/consumer_contract.md) and
[replay example](examples/replay_consumer.py).

## Tests

```bash
python -m pytest -q
python -m ruff check .
python -m mypy
python tests/verify_m0_contracts.py
go run tools/verify_raw_chunk_golden.go
```

The default suite is offline and excludes stress tests. Opt-in online tests and
platform validation are described in the [test environment matrix](docs/test_environment_matrix.md).

## Documentation

- [Developer guide](docs/developer_guide.md): code map, tests, and V5 workflow.
- [Project handoff](docs/PROJECT_HANDOFF.md): completed work and remaining tasks.
- [Current production state](docs/CURRENT_PRODUCTION_STATE.md): exact artifact and stop status.
- [Architecture](docs/architecture.md) and [project contract](docs/project_contract.md).
- [Data and storage](docs/data_and_storage.md) and [data contract](docs/data_contract.md).
- [V5 acceptance contract](docs/acceptance_evidence_v5.md).
- [Known limitations](docs/known_limitations.md) and [risk register](docs/risk_register.md).

macOS Apple Silicon is the development/local profile; Ubuntu ARM64 has separate
validation evidence. Ubuntu 24.04 x86_64 is the production target. The portable
archive-client rollout and long-duration qualification remain unfinished.
