# Project Contract

Status: frozen by M0 on 2026-07-22. Changes require a dedicated ADR and must
retain traceability in `requirements_traceability.md`.

## Purpose and certified environment

Crypto Market Data Recorder is an independent, general-purpose, stateful crypto
market-data infrastructure service. Its identity and core contracts are not
bound to Alpha101, a strategy, backtest engine, or exchange. Its first certified
platform is macOS on Apple Silicon with Python 3.12, deployed as a user
`launchd` LaunchAgent while the user is logged in. Docker is not the V1
production deployment.

Binance is the first adapter. V1 records public BTCUSDT market data:

| Market | Core streams | Recovery/bootstrap |
| --- | --- | --- |
| Binance Spot | diff depth 100 ms, aggTrade, bookTicker | public REST depth snapshot |
| Binance USD-M perpetual | diff depth 100 ms, aggTrade, bookTicker | public REST depth snapshot |

USD-M mark/index/premium data, funding, open interest, liquidation events, and
exchange/filter snapshots are isolated side data added after both core L2
collectors. Side-data failure must never block the core collectors.

## Ownership and dependency contract

The Recorder core owns:

- public market-data transport and exact receive evidence;
- immutable raw chunks and their manifests;
- crash recovery, gap/resync quality state, local spool, Catalog, and reports;
- normalized/versioned datasets and deterministic replay;
- registered external-directory discovery and verified archival;
- a generic, versioned, read-only consumer contract.

Consumers own computation, factors, research, strategies, target generation,
execution simulation, account ledgers, backtesting, monitoring, and UI/API
surfaces. Recorder never imports consumer code, and consumers may not control
Recorder internals. Integration is through generic immutable datasets,
manifests, Catalog queries, replay APIs, and the M16 consumer contract.
Alpha101Crypto may be checked read-only as one ordinary consumer example, but
that validation is optional and cannot define V1 completion.

## Data invariants

1. Exact WebSocket payload bytes are recoverable; normalized fields alone are
   insufficient.
2. Each event preserves exchange event/transaction/trade time when supplied,
   UTC receive wall time, monotonic receive time, market, symbol, stream,
   connection ID, collector instance ID/version, and source sequence IDs.
3. Raw accepts duplicates and is never edited after seal. Derived layers own
   documented deterministic deduplication and repartitioning.
4. Every chunk records time bounds, count, uncompressed/stored bytes, schema
   and collector versions, SHA-256, sequence bounds, and gap/resync markers.
5. Active files use `.partial`; only a complete verified seal can remove that
   status. Crash recovery never promotes an unverified tail.
6. Gaps, out-of-order input, malformed input, checksum failure, planned
   reconnects, unexpected disconnects, sleep, and emergency stops are explicit
   events/intervals, never hidden.
7. Replay is deterministic under a versioned ordering, tie-break, deduplication,
   gap, and checkpoint policy.

## Storage invariants

- Collector always writes to the internal data root, never directly to an
  external volume. Default root:
  `~/Library/Application Support/CryptoMarketDataRecorder/`.
- Production data is forbidden under the repository, Desktop, Documents,
  iCloud Drive, persistent `/tmp`, and the repository parent
  `/Users/amada/Documents/Development/Crypto`.
- External archive is optional. Only a user-registered directory is writable;
  the volume remains shared and its existing filesystem stays unchanged.
- External identity combines volume UUID, volume name, filesystem type,
  registered relative path, marker inside that directory, and `storage_id`.
- Mountpoints are re-resolved by UUID. A fixed `/Volumes/<name>` is not an
  identity.
- Readiness requires writable/access checks plus actual write, fsync, atomic
  rename, and readback probes inside the registered directory.
- Internal deletion is allowed only after target temp write, fsync, full
  readback, size/SHA-256 verification, atomic rename, external manifest commit,
  and Catalog transaction. Failure is retryable/idempotent and retains source.
- Deleting the internal copy may leave the external volume as the only copy;
  status and documentation must say so explicitly.

## Operations and safety

- No GUI. Operability is CLI, structured JSON state, logs, and UTC daily
  JSON/CSV reports.
- No API keys, account access, order endpoints, trading permissions, or real
  trading.
- No root LaunchDaemon installation by default.
- macOS controls normal mounting. Recorder observes Disk Arbitration events and
  may request safe unmount/eject; it never formats or repairs a filesystem.
- Sleep/lid-close gaps must be marked. V1 does not promise collection while a
  MacBook is asleep or closed.
- Planned upgrades use blue/green overlap. Raw duplicates are acceptable;
  unmarked planned gaps are not.

## Space policy

Internal free-space severity is:

- WARNING at remaining space <= 40%;
- CRITICAL at remaining space <= 15%;
- EMERGENCY at remaining space <= `max(10 GiB, 5%)`.

Forecasts use at least 1 h, 6 h, 24 h, and 7 d net-growth windows and report
UTC ETAs for all thresholds. Insufficient history yields `INSUFFICIENT_DATA`;
non-positive growth yields `NOT_APPROACHING`. Emergency mode stops compactors
and non-core derivation, prioritizes verified archive/delete, and never deletes
unarchived raw data. At the hard reserve it seals active files, stops Collector,
emits `DISK_EMERGENCY_STOP`, and records the gap start.

## Daily reporting contract

UTC-day summaries are partitioned by market and stream and include:

- input: WebSocket/REST messages and bytes, depth side updates, aggTrade and
  bookTicker counts;
- quality: accepted, duplicate, malformed, out-of-order, gaps, resyncs,
  planned/unexpected disconnects, server shutdowns, checksum failures;
- output: raw records/bytes, sealed/compressed/normalized/archive/delete and
  backlog counts/bytes;
- performance/state: receive-lag percentiles, queues, write/fsync latency, CPU,
  RSS, internal/external space, oldest unarchived and last-event ages.

JSON and CSV live at `reports/daily/YYYY-MM-DD.*`; structured summaries also
live in SQLite. SQLite does not store the market-event corpus.

## Explicit V1 exclusions

Qt, web UI, FastAPI product API, trading UI, strategies, factors, backtest
engine, orders, account connection, API-key management, live trading, maker
queue simulation, multiple exchanges/symbols, Kafka, Kubernetes, cloud
stateless capture, automatic disk formatting/repair, mandatory SMART support,
and Windows/Ubuntu certification are excluded.

The design must leave adapters possible for Ubuntu storage, an API gateway,
separate UI, additional symbols/exchanges, and independent strategy, backtest,
monitoring, or paper-trading consumers. Those consumers remain outside
Recorder, and exchange-specific behavior remains behind adapters.
