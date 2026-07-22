# Architecture

## Context

Binance Market Data Recorder is the system of record for Binance public
market-data capture and storage provenance. Its current product boundary is
Binance Spot and USD-M perpetual data. External consumers have different
lifecycles and may include research, backtest, monitoring, or simulation
systems. ADR-0001 and ADR-0007 freeze that separation.

```text
Binance public REST + WebSocket
              |
              v
  Binance Spot / USD-M modules (isolated)
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
| `binance.spot` | Official Spot public schema/transport, REST snapshot provenance | accounts, keys, orders, USD-M policy |
| `binance.usdm` | Official USD-M public schema/transport, REST snapshot provenance | accounts, keys, orders, Spot policy |
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

M4 implements `binance.spot` plus the Spot portion of `collector`: three
independent raw WebSocket connections and one official-SDK REST depth snapshot.
M3 implements the shared EventEnvelope, bounded spool, Raw
writer/recovery/seal, internal layout, and SQLite Catalog. Other rows remain
planned package boundaries. USD-M remains M5 work. Storage, Raw, Catalog,
normalize, replay, and archive remain independent of consumer code.
Do not add an abstraction framework for unplanned exchanges. Another exchange
would require a separate architecture review.

## Runtime isolation

Spot and USD-M use separate connection/session state, queues, failure budgets,
checkpoints, and metrics. Failure of one market cannot stop the other. USD-M
side-data tasks are still more weakly coupled and cannot block core L2.

The M4 socket receive boundary timestamps immediately after `recv(decode=False)`
and places the exact bytes plus clocks in a bounded receipt queue before JSON
parsing. A separate persistence loop extracts only Raw metadata, envelopes, and
hands off to the bounded spool. It never compresses in the callback, builds
Parquet, reconstructs books, or performs network archive I/O. Both transport
and Recorder queues are finite; saturation is a visible collector fault, never
a silent drop.

Each Spot stream uses its own raw endpoint and connection ID. This preserves a
known stream identity even for malformed JSON and avoids combined-stream wrapper
ambiguity. The generic WebSocket library's client Ping loop is disabled while
its protocol layer automatically echoes server Ping payloads; a local protocol
test proves that behavior. Recorder replaces connections at 23 h 50 min, before
the official 24-hour disconnect, and immediately replaces a connection after a
persisted `serverShutdown` event.

## Data planes

### Raw plane

`EventEnvelope v1` contains metadata and exact payload bytes. ADR-0002 selects
an endian-defined, length-prefixed CBOR frame with per-frame CRC32C, a versioned
chunk header, SHA-256 at seal, and Zstandard only after a verified seal. Active
uncompressed `.partial` files are recoverable by forward scan. Compression is
never applied in place. ADR-0010 freezes the exact byte layout, canonical CBOR
profile, file names, checksum coverage, compression parameters, and fsync order.

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
~/Library/Application Support/BinanceMarketDataRecorder/
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
and supervisor boundaries. Binance Spot and USD-M behavior sits in their own
transport/schema modules. File/chunk/manifests use specified language-neutral
formats and UTC timestamps so arbitrary consumers, future Go/Rust readers, and
an Ubuntu storage adapter do not require macOS internals. This portability does
not make multi-exchange support a current goal.
