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
M11 persists internal/per-target capacity history, reports robust multi-window
growth and threshold ETAs, and freezes the archive-first/hard-reserve emergency
stop policy. M12 adds non-forced, system-confirmed macOS safe eject serialized
against archive transactions, plus forced-removal/reinsertion recovery. M13
adds readiness-gated Spot/USD-M blue/green handoff, durable deployment audit,
identifiable Raw overlap, rollback, and proactive 24-hour connection rotation.
M14 adds the logged-in-user LaunchAgent, crash restart, SIGTERM sealing,
single-service-process locking, honest atomic runtime status, sleep-gap
evidence, and optional scoped idle-sleep prevention. M15 adds immutable,
content-addressed normalized Parquet for every current Spot/USD-M stream,
deterministic overlap deduplication, explicit gap/malformed evidence, Raw and
checkpoint lineage, and DuckDB interoperability validation. M16 adds generic
replay build discovery, receive/exchange event clocks, versioned deterministic total
ordering, explicit gap/missing-time policies, verified checkpoint seek, a
typed generic consumer contract, and an independent read-only example. Accounts,
credentials, and trading remain unimplemented.

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
- [M11 acceptance](docs/milestone_acceptance/M11.md)
- [M12 acceptance](docs/milestone_acceptance/M12.md)
- [M13 acceptance](docs/milestone_acceptance/M13.md)
- [M14 acceptance](docs/milestone_acceptance/M14.md)
- [M15 acceptance](docs/milestone_acceptance/M15.md)
- [M16 acceptance](docs/milestone_acceptance/M16.md)
- [Generic consumer contract](docs/consumer_contract.md)

## Install and verify M15

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
binance-market-recorder normalize status
binance-market-recorder normalize run
python3.12 examples/replay_consumer.py \
  --data-root "$HOME/Library/Application Support/BinanceMarketDataRecorder" \
  --build-id <64-hex-build-id> \
  --market spot \
  --stream agg_trade
binance-market-recorder storage list
binance-market-recorder storage inspect /Volumes/Archive/QuantData/Recorder
binance-market-recorder storage register /Volumes/Archive/QuantData/Recorder
binance-market-recorder storage status
binance-market-recorder storage eject <storage-id>
binance-market-recorder storage forecast
binance-market-recorder archive status
binance-market-recorder archive retry
binance-market-recorder archive verify <storage-id>
binance-market-recorder launchd install \
  --label "$AUTHOR_CONTROLLED_LABEL" \
  --author-controls-namespace
binance-market-recorder launchd status
binance-market-recorder launchd stop
binance-market-recorder launchd start
binance-market-recorder launchd uninstall
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
`max_frame_bytes`; service settings are `heartbeat_seconds`,
`sleep_gap_threshold_seconds`, and
`prevent_sleep`; unknown settings are rejected. The default data root is
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

`storage eject <storage-id>` first blocks new archive allocation. It returns
`BUSY` while an archive transaction is incomplete; complete it with
`archive retry` and retry eject. Recorder fsyncs its archive directories and
Catalog, then requests default non-forced Disk Arbitration unmount and eject.
Only confirmation of both operations returns zero and “可以拔出”. A refusal,
timeout, unmount-only result, or forced removal never claims safe-to-remove and
never authorizes deletion of internal Raw. Timeout conservatively keeps archive
allocation blocked until an explicit eject retry resolves the asynchronous
outcome.

```toml
[recorder]
data_root = "/safe/absolute/path"
log_level = "INFO"
rotation_seconds = 60.0
rotation_bytes = 134217728
durability_interval_seconds = 1.0
ingress_queue_capacity = 8192
max_frame_bytes = 16777216
heartbeat_seconds = 5.0
sleep_gap_threshold_seconds = 30.0
prevent_sleep = false
```

LaunchAgent installation requires a real reverse-DNS label in a namespace the
project author controls. Set `AUTHOR_CONTROLLED_LABEL` before using the command
above. The label must end in `.BinanceMarketDataRecorder`; Binance-owned-looking
and placeholder namespaces are rejected. No root daemon is installed.
Generated stdout/stderr logs live under the internal `logs/` directory.

The service starts after login, and launchd restarts unsuccessful exits.
`binance-market-recorder status` reports `RUNNING` only for a live PID with a
fresh atomic heartbeat. SIGTERM drains and seals Collector spools. macOS sleep
creates an explicit gap; closed-lid capture is not promised. Optional
`prevent_sleep=true` owns a service-PID-scoped `caffeinate` assertion and never
changes permanent power settings.

See [dependency policy](docs/dependency_policy.md) for the deliberately small
runtime and development dependency sets. M2 selected exact-pinned official
Spot and USD-M SDKs for unsigned public REST snapshots and `websockets` for
market-stream transport. M4/M5 use the official SDKs only for public
`GET /api/v3/depth` and `GET /fapi/v1/depth` snapshots. Each market uses three
independent documented raw WebSocket streams. The implementation never reads
credentials or calls account/order APIs.

M15 exact-pins PyArrow as the production Parquet writer. DuckDB is an
exact-pinned development-only interoperability check and is not used as the
Catalog or event store. `normalize run` is an explicit offline derived task:
it verifies every selected sealed Raw artifact, writes only below
`data/normalized/normalized-dataset.v1/`, and fails rather than publish a
silently partial build if Raw is unavailable. `normalize status` is read-only.
Build and partition manifests are content-addressed; consumers must select a
specific build manifest and must not glob artifacts from different builds.

M16 consumers use `binance_market_data_recorder.replay.ManifestCatalog` with
one configured application-data root and one explicit build ID. Public
descriptors expose hashes, counts and versions, not absolute artifact or
external archive paths. Replay supports half-open nanosecond ranges,
receive/exchange clocks, deterministic equal-time tie-breaks, explicit
gap/missing-exchange-time policies, and verified depth-checkpoint continuation.
There is deliberately no automatic “latest build”. See the generic consumer
contract before building an adapter.

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
