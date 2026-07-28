# Architecture

This document describes the `0.1.0a1` Mac Developer Preview and M20 Ubuntu
ARM64/RK3588 Developer Preview / Soak Candidate. The implemented
system remains subject to the deferred long-running reliability limitation in
`docs/known_limitations.md`.

## M19 recovery boundary

Each core market owns a depth capture session and resync coordinator. A depth
lifecycle break, sequence gap, or bounded bootstrap overflow restarts only that
market's connections and snapshot bridge. A terminal core task records
evidence, seals the other core market, exits nonzero, and lets launchd perform
whole-process recovery. Auxiliary public datasets restart independently and
can degrade status without stopping core Raw.

Historical Importer is an offline sibling of Live collection. It writes below
`data/historical`, uses official archive checksums/revisions, and publishes
archive-clock Parquet with source lineage. It does not enter Live Raw chunks or
pretend to possess local receive time.

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

The implemented `binance.spot` and `binance.usdm` modules each own three
independent raw WebSocket connections and one official-SDK REST depth snapshot.
The shared EventEnvelope, bounded spool, Raw writer/recovery/seal, internal
layout, and SQLite Catalog support both markets. Storage, Raw, Catalog,
normalize, replay, and archive remain independent of consumer code and are all
implemented within the preview boundary.
Do not add an abstraction framework for unplanned exchanges. Another exchange
would require a separate architecture review.

M6 implements `orderbook` as a derived plane over immutable M4/M5 envelopes.
It buffers before snapshot, uses separate Spot `U/u` and USD-M `U/u/pu`
continuity branches, withholds an unreliable book after a gap, and persists
hash-verified checkpoints plus Catalog metadata. It never writes reconstructed
levels into Raw or SQLite.

M7 implements USD-M auxiliary tasks behind ADR-0012. Mark-price and
liquidation WebSockets and five official-SDK REST polling kinds each own a Raw
spool and independent configuration. A side-data supervisor contains failures
without setting either core market's stop event. REST calls share a local
serialization lock and record documented weight plus observed rate headers;
M8, not M7, owns durable daily aggregation.

M8 implements the ADR-0013 observability path. Raw spools emit post-append and
post-seal summaries into retry-idempotent Catalog batches; connection and M6
audit boundaries contribute quality counters. UTC rollover and graceful stop
commit aggregate-only rows and atomically publish deterministic JSON/CSV under
`data/reports/daily/`. SQLite contains no Raw payload or per-market-event row.
The CLI reads these summaries while continuing to report `NOT_RUNNING` until a
later supervised service supplies validated runtime state.

M9 implements `storage.macos` under ADR-0014. PyObjC bridges Disk Arbitration
startup, appeared, description-changed and disappeared callbacks. Only
explicitly non-internal UUID-bearing volumes are candidates. Registration
persists UUID/relative-path/marker identity in Catalog and proves write, fsync,
rename and readback inside the chosen folder. External absence or failure never
enters the Collector dependency path.

M10 implements `archive` under ADR-0015. It reserves the oldest sealed Raw
chunk, streams only to a transaction-owned external temporary file, verifies a
full reopened read by size and SHA-256, commits the immutable final artifact and
external manifest, then commits external verification in Catalog. Local source
deletion is separately authorized and recorded. Every filesystem/Catalog crash
boundary is restart-reconcilable; archive failure never stops Spot or USD-M
capture.

M11 implements capacity history and emergency control under ADR-0016.
Aggregate-only samples in Catalog drive robust 1 h/6 h/24 h/7 d net-growth
rates, threshold alerts and UTC ETAs independently for internal and registered
external storage. The emergency coordinator is above spool/archive components:
it may suspend non-core work and prioritize verified archive, but only the
hard-reserve path seals active Raw, stops Collectors, records
`DISK_EMERGENCY_STOP` and opens a gap. It has no unverified-delete capability.

M12 implements safe external-volume release under ADR-0017. A Catalog
`EJECT_PENDING` latch and archive reservation share an immediate transaction:
active archive work wins and eject reports `BUSY`, or eject wins and no new
work can be allocated. Recorder fsyncs its registered archive directories and
Catalog before requesting non-forced Disk Arbitration unmount then eject.
Only both successful system callbacks produce `SAFE_TO_REMOVE`. Refusal or
physical disappearance never authorizes local deletion or stops internal
capture; verified reinsertion reactivates allocation.

M14 implements the native process boundary under ADR-0019. One logged-in-user
LaunchAgent owns a kernel `flock`, runs M3 startup recovery, supervises isolated
Spot/USD-M workers, writes an atomic PID/freshness-validated state heartbeat,
and maps SIGTERM to Collector drain/seal. All core workers stopping makes the
process fail for launchd restart; one market failure remains isolated.
NSWorkspace notifications plus wall/monotonic discontinuity evidence mark
sleep gaps. Optional `caffeinate -i -w <pid>` is scoped to the service lifetime
and never changes persistent power policy.

M15 implements `normalize` under ADR-0020 as an explicit non-core derived
process. It verifies sealed Raw by stored and decompressed hashes, externally
merge-sorts stable semantic identities, preserves conflicts and malformed
evidence, and writes explicit-schema Zstandard Parquet to content-addressed UTC
market/stream/date/hour partitions. Immutable build manifests bind partitions
and verified M6 checkpoints to Raw content hashes without recording external
mountpoints. Collector callbacks and the launchd capture path never execute
normalization.

M16 implements `replay` under ADR-0021 as a public read-only consumer boundary.
A manifest catalog opens one explicit content-addressed build, verifies all
selected partition/checkpoint identities, and resolves relative paths without
exposing archive mountpoints. Replay scans Parquet in fixed batches and uses
bounded external merge passes to implement receive/exchange clocks, stable
equal-time ordering, explicit gap/missing-clock policies and verified depth
checkpoint continuation. The independent example imports only this public
package; no consumer code enters Recorder core.

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
a silent drop. `ingress_queue_capacity` applies to both receipt and spool
queues. Time-based Raw rotations use a stable market/stream phase inside the
configured period, spreading compression/fsync load without delaying any
stream beyond that period. The RK3588 deployment uses an explicitly larger
bounded capacity than the generic default; M21 must validate its queue and RSS
trend under 72-hour and 168-hour load.

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

The M21.0 Soak sampler is a read-only state-plane observer. Its Catalog
connection uses SQLite URI `mode=ro`, `query_only=ON`, and a busy timeout; it
does not initialize schema, set journal mode, enter `BEGIN IMMEDIATE`, probe an
external directory, activate a storage target, or change storage control. The
certified concurrent boundary is one Recorder writer, one Archive writer, and
one read-only Soak observer—not three writers or arbitrary writer fan-out.

### Derived plane

`normalized-dataset.v1` is rebuildable and contains source chunk hashes,
dataset/stream-schema/dedup/writer-profile versions, deterministic
dedup/conflict decisions, propagated gap state, and UTC partitions. Candidate
sorting and partition spooling use bounded batches on internal storage; output
is atomically committed only after Parquet logical readback.
`consumer-contract.v1`/`replay-order.v1` selects one such build and exposes
deterministic read-only events without filesystem-order or mountpoint semantics.
Neither derived layer rewrites Raw.

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

ADR-0018 defines M13 as a per-market, make-before-break Collector handoff. A
candidate uses a distinct instance ID/version and independent Spot or USD-M
connections. Readiness requires current connections and durably written events
for all three core streams, a durably written public REST snapshot, and an M6
market-specific synchronized local book. The supervisor then requires fresh
post-readiness events from old and new before stopping old.

Overlap Raw remains immutable and carries deployment ID, active/candidate role,
handoff reason, collector instance/version, and connection provenance. Catalog
durably audits readiness, overlap, cutover, and rollback transitions. M15 owns
deterministic deduplication. Candidate failure or loss of readiness before
cutover leaves old running. Reverse-version rollback and proactive 23 h 40 min
connection rotation use the same gate; the M4/M5 stream-local 23 h 50 min
reconnect remains a marked fallback.

M14 installs a user LaunchAgent and provides process-level locking compatible
with the explicit supervised overlap identity. It requires an operator-supplied
author-controlled reverse-DNS label; no Binance-owned or guessed namespace is
built in. The process lock covers the service, while M13 old/candidate
Collectors coexist inside that one process.

## Portability

Platform-specific Disk Arbitration/launchd and mountinfo/systemd code sits
behind `storage.macos`, `storage.linux`, and service boundaries. Linux never
imports PyObjC. Binance Spot and USD-M behavior sits in transport/schema
modules, while ADR-0025's `ProxyPolicy` is injected at assembly time into every
WebSocket, urllib, official-SDK REST, and Historical exit.

macOS keeps its Application Support, NSWorkspace/caffeinate, LaunchAgent, and
safe-eject behavior. Linux uses an XDG interactive path, a no-op native sleep
observer plus clock-discontinuity detection, a non-root systemd unit, journald,
and read-only discovery of already-mounted external filesystems. The production
unit contains no proxy environment variables; redacted policy state is written
to `service-state.v1`.

File/chunk/manifests use the unchanged language-neutral formats and UTC
timestamps, so consumers do not depend on either platform adapter. This
portability does not make multi-exchange support a current goal, and M20 does
not certify Linux blue/green or long-run operation.
