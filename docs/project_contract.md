# Project Contract

Status: frozen by M0, identity-corrected by ADR-0007/M0.2, extended to Ubuntu
ARM64 Developer Preview / Soak Candidate by M20, and prospectively extended to
the VPS/offline architecture by ADR-0028/0029/0030 on 2026-08-17.
Changes require a dedicated ADR and must retain traceability in
`requirements_traceability.md`.

## Purpose and certified environment

Binance Market Data Recorder is an independent, unofficial project. It is not
affiliated with, maintained by, sponsored by, or endorsed by Binance.

It is a stateful infrastructure service specifically for Binance public market
data. The name identifies the connected data source and APIs; it does not imply
an official relationship. The project must not use Binance logos, official
visual identity, or identifiers that suggest Binance ownership. The primary
future production profile is Ubuntu 24.04 LTS x86_64 with Python 3.12,
systemd, and a non-root service. macOS Apple Silicon remains a development/local
profile; Ubuntu ARM64/RK3588 remains a distinct Linux validation and historical
evidence profile. Docker is not the production deployment.

V1 records public BTCUSDT Binance market data:

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
- future VPS-to-local archive transfer, receipt authorization, and Archive Set
  custody as defined by ADR-0029/0030;
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
  external volume. Interactive defaults are macOS
  `~/Library/Application Support/BinanceMarketDataRecorder/` and Linux
  `~/.local/share/BinanceMarketDataRecorder/`; Linux systemd uses
  `/var/lib/binance-market-data-recorder`.
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
  Catalog transaction, and (for the future VPS profile) durable local receipt
  authorization plus source revalidation. Failure is retryable/idempotent and
  retains source.
- Deleting the internal copy may leave the external volume as the only copy;
  status and documentation must say so explicitly.

## Operations and safety

- No current GUI. Operability is CLI, structured JSON state, logs, and UTC daily
  JSON/CSV reports. A future Web UI is separately authorized and must keep View,
  Health, and Control concerns separate from Recorder core.
- No API keys, account access, order endpoints, trading permissions, or real
  trading.
- No root LaunchDaemon installation by default.
- Linux Collector processes also never run as root. The managed systemd unit
  takes an explicit User/Group and keeps proxy behavior in TOML, not SSH
  environment variables.
- macOS controls normal mounting. Recorder observes Disk Arbitration events and
  may request default non-forced safe unmount/eject; only both successful
  callbacks mean safe-to-remove. It never formats or repairs a filesystem.
- Linux observes only already-mounted external block filesystems through
  mountinfo/findmnt/lsblk. It never mounts or unmounts them; without a reliable
  eject backend it reports manual action and no safe-removal success.
- Sleep/lid-close gaps must be marked. V1 does not promise collection while a
  MacBook is asleep or closed.
- Planned upgrades use ADR-0018 make-before-break blue/green overlap. Candidate
  socket-open alone is insufficient: all core streams must durably write,
  the public snapshot must durably write, and the local book must synchronize.
  Fresh old/new post-readiness events prove overlap before old shutdown.
- Raw overlap duplicates are acceptable and carry deployment/role/reason plus
  existing instance/version provenance; unmarked planned gaps are not.
- Reverse-version rollback and pre-24-hour connection rotation use the same
  readiness gate.
- ADR-0019 installs only a logged-in-user LaunchAgent under an explicitly
  author-controlled label. A kernel-held service lock prevents competing
  process writers while allowing M13 overlap inside that process.
- SIGTERM drains/seals before `STOPPED`; crashes exit nonzero for launchd
  restart and the next instance runs Raw recovery. Runtime status is trusted
  only with a live PID and fresh atomic heartbeat.
- Sleep/wake and inferred clock discontinuities open explicit operational gaps.
  Optional prevent-sleep is a service-PID-scoped idle assertion, never a
  permanent setting or closed-lid guarantee.

## Space policy

The primary 40 GB-class VPS profile uses real filesystem free space and
observed ingest/net-growth data:

- NORMAL: free > 18 GiB;
- WARNING: free <= 18 GiB or ETA to hard reserve <= 7 days;
- CRITICAL: free <= 14 GiB or ETA <= 72 hours;
- EMERGENCY: free <= 12 GiB or ETA <= 24 hours;
- HARD RESERVE: free <= 10 GiB.

WARNING continues capture and requests archive planning. CRITICAL continues
integrity-critical work and strongly requests archive. EMERGENCY prioritizes
capture, seal, Catalog, recovery, and archive-space-recovery work. At HARD
RESERVE the Recorder drains/seals what can be proven, emits
`DISK_EMERGENCY_STOP`, records the gap start, and stops accepting new capture;
it never deletes unarchived Raw. Forecasts retain the 1 h, 6 h, 24 h, and 7 d
observed-growth windows. These are initial VPS thresholds and require a new ADR
to revise.

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

Qt, current web UI, FastAPI product API, trading UI, strategies, factors, backtest
engine, orders, account connection, API-key management, live trading, maker
queue simulation, other exchanges/additional symbols, Kafka, Kubernetes, cloud
stateless capture, automatic disk formatting/repair, mandatory SMART support,
and Windows certification are excluded from the current implementation. Ubuntu
ARM64 long-run certification, zero-interruption claims, Linux blue/green
certification, and VPS production acceptance remain separate gates. A future
Web UI and notification system are deferred and separately authorized rather
than prohibited forever.

The design retains clean Binance Spot/USD-M modules, a Linux storage adapter,
an API gateway boundary, and independent strategy, backtest,
monitoring, or paper-trading consumers. Those consumers remain outside
Recorder. Multi-exchange support is not a V1 design or acceptance goal; any
future exchange requires its own architecture review.

## Deployment and offline roles

The VPS is responsible for the latency/integrity-critical live path. Normalize,
heavy Replay/analytical scans, and Historical Backfill remain Recorder-owned
capabilities but execute in local/offline profiles. This is one Recorder
distribution with distinct execution roles, not a new repository or
microservice. See `docs/vps_operations.md` and `docs/offline_workspace.md`.

Future service, launchd, and package-publisher reverse-DNS identifiers must use
a namespace owned or controlled by the project author. Binance-owned-looking
namespaces are forbidden, and M0.2 does not guess a replacement namespace.
