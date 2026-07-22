# Architecture

## Context

Crypto Market Data Recorder is the system of record for public crypto
market-data capture and storage provenance. Binance is the first exchange
adapter; it does not define core identity or consumer contracts. External
consumers have different lifecycles and may include research, backtest, or
monitoring systems. ADR-0001 and ADR-0006 freeze that separation.

```text
public exchange REST + WebSocket
              |
              v
  exchange transport adapters
  (Binance Spot / USD-M first, isolated)
              |
              v
  bounded ingress -> raw spool writer -> seal/recovery
              |                            |
              |                            v
              |                      Catalog/manifests
              |                            |
              +--> quality/orderbook ------+
                                           |
                          +----------------+----------------+
                          v                                 v
                 archive manager                    normalization/replay
             (registered folder only)                       |
                                                            v
                                               arbitrary consumers
```

## Component responsibilities

| Component | Owns | Must not own |
| --- | --- | --- |
| exchange adapters (`binance` first) | Official public schema/transport adapters, REST snapshot provenance | accounts, keys, orders, generic core policy |
| `collector` | connection/session lifecycle, receive timestamps, bounded handoff | compression, Parquet, factors |
| `spool` | framed append, rotation, fsync, seal, crash recovery | external mount logic |
| `storage` | paths, Catalog, manifests, durable state transitions | market strategy semantics |
| `orderbook` | official sequence validation, reconstruction, gap/resync evidence | execution/queue fills |
| `archive` | oldest-sealed copy/verify/commit/delete transaction | writes outside registered folder |
| `storage.macos` | Disk Arbitration observation, UUID resolution, probes, eject | format/repair/root daemon |
| `normalize` | versioned schemas, deterministic dedup/partitioning, lineage | mutation of Raw |
| `replay` | deterministic event clocks, seeks, gap policy | strategy/backtest behavior |
| `metrics` | counters, lag/storage forecasts, daily UTC reports | market-event corpus in SQLite |
| `supervisor` | independent worker health, blue/green handoff, emergency stop | hiding gaps or coupling markets |
| `cli` | local control/status/report/storage commands | GUI, trading interface |

These are planned package boundaries, not M0/M0.1 implementations. Generic
domain, storage, Raw, Catalog, normalize, replay, and archive modules must not
import an exchange adapter; adapters translate official exchange semantics into
the generic envelope without erasing raw bytes.

## Runtime isolation

Spot and USD-M use separate connection/session state, queues, failure budgets,
checkpoints, and metrics. Failure of one market cannot stop the other. USD-M
side-data tasks are still more weakly coupled and cannot block core L2.

Collector callbacks only timestamp, envelope, validate the minimum framing
preconditions, and enqueue. They never compress, build Parquet, reconstruct
complex books, or perform network archive I/O. Backpressure behavior must be
explicit: no unbounded queue and no silent drop. A persistence inability is a
visible collector fault/gap, not permission to discard payloads.

## Data planes

### Raw plane

`EventEnvelope v1` contains metadata and exact payload bytes. ADR-0002 selects
an endian-defined, length-prefixed CBOR frame with per-frame CRC32C, a versioned
chunk header, SHA-256 at seal, and Zstandard only after a verified seal. Active
uncompressed `.partial` files are recoverable by forward scan. Compression is
never applied in place.

### State plane

SQLite holds Catalog objects, state transitions, archive transactions,
checkpoints, and aggregates—not full market events. Files/manifests are the
source artifacts; Catalog makes their lifecycle queryable. Transitions are
idempotent and reconcile filesystem state after a crash.

### Derived plane

Normalization and replay outputs are rebuildable and contain source chunk
hashes, dataset/schema versions, deterministic dedup/gap decisions, and UTC
partitions. They do not rewrite Raw.

## Internal directory contract

```text
~/Library/Application Support/CryptoMarketDataRecorder/
├── data/
│   ├── active/
│   ├── sealed/
│   ├── manifests/
│   ├── checkpoints/
│   ├── normalized/
│   ├── reports/
│   └── quarantine/
├── state/
│   ├── catalog.sqlite
│   └── service_state.json
└── logs/
```

The code repository's `var/` is test-only and Git-ignored.

## External archival

ADR-0003 defines an external target as a registered subdirectory, never a
volume. UUID-based discovery resolves its current mountpoint. The archive
transaction is copy-to-`.copying`, fsync, reopen/read/hash, size/hash compare,
atomic rename, external manifest commit, Catalog commit, then local delete.
Every crash boundary is reconciled idempotently. Disappearance changes storage
state while the internal Collector continues.

## Deterministic time and replay

Wall-clock receive time is UTC and suitable for replay/event ordering;
monotonic receive time measures within-process intervals and cannot be compared
across boots. Exchange-provided times are preserved as data, not assumed unique
or ordered. ADR-0004 freezes clock meanings and requires a versioned total-order
tie-break using stable raw provenance.

## Lifecycle and upgrade

M14 installs a user LaunchAgent. M13 blue/green upgrade starts a candidate with
an independent connection, obtains snapshot/book readiness, overlaps old and
new raw capture, then stops old only after candidate readiness. Duplicate Raw
is expected and deterministically resolved later. The same overlap mechanism
supports planned 24-hour connection rotation.

## Portability

Platform-specific Disk Arbitration and launchd code sits behind `storage.macos`
and supervisor adapters. Exchange-specific behavior sits behind transport/schema
adapters. File/chunk/manifests use specified language-neutral formats and UTC
timestamps so arbitrary consumers, future Go/Rust readers, and an Ubuntu
adapter do not require macOS or Binance internals.
