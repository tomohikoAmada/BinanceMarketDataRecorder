# Architecture

This document describes the implemented Recorder, the approved future
deployment topology, and the accepted future configurable product-set target.
Verify live
GitHub `main` at takeover. The post-MS1 implementation/behavior authority is
`d38180074b5f76ab6b7778eea7fc505160c671ae` (tree
`95f16f05b30b7db23e43ebb6439ed0d055081902`); MS1 is merged, but live GitHub
`main` is not deployed. Documentation-only descendants may make live `main`
newer without changing that behavior authority. The independently qualified deployed artifact is the
pre-MS1 source `c421605e302d2ad46acdb2466627f64644181c9a`, whose clean 24-hour
non-formal stage is complete. The MS2 candidate implements configurable
product runtime and has offline acceptance; independent review/merge is pending. Formal M22.9 has not started and Production Ready is not
authorized. See `docs/PROJECT_HANDOFF.md` and
`docs/CURRENT_PRODUCTION_STATE.md`.

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

## Approved deployment roles

The VPS owns the latency/integrity-critical live path: Binance public
acquisition, Raw active/spool and sealing, Catalog, recovery, gap/provenance
state, metrics/status, and support for local archive export. Normalize, heavy
Replay/analytical scans, and Historical Backfill remain Recorder-owned
capabilities but execute in local/offline profiles using the same distribution.
This is an execution-role boundary, not a new repository or microservice.

The preferred future infrastructure direction is a dedicated Recorder VPS,
separate from a Gateway/Projection host. Recorder and Gateway remain
independent services and failure domains: Recorder is optimized for durable
capture, while Gateway may use a separate realtime-latency profile. This is
planning only and does not create a runtime dependency or change the software
boundary.

The local Offline Workspace contains the Cold Archive, derived Normalized
Dataset, separate Historical Archive, Catalog backups, and rebuildable Archive
Set discovery index. See `docs/offline_workspace.md`.

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
| `archive` | oldest-sealed copy/verify/commit/delete transaction and future receipt seam | writes outside registered folder or decides transport policy |
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

## M21.4 USD-M ingress backpressure and stream recovery

M21.4 replaces the prior `put_nowait`-based bounded receipt queue with
bounded, Writer-aware, stop-aware awaitable backpressure for USD-M WebSocket
streams. The key architectural changes are:

- **Stream-local recovery**: Sustained receipt queue saturation causes
  connection/stream-level closure and recovery rather than immediate
  process-wide `CoreMarketTerminalFailure`. Only Writer, Raw sync, Catalog,
  and seal integrity failures remain process-level fatal.

- **Generation and Gap boundaries**: `diff_depth` enters UNTRUSTED on
  saturation; a fresh REST Snapshot and correct `U/u/pu` bridging are
  required before READY. `book_ticker` and `agg_trade` cannot reconstruct
  historical completeness; lost events produce persistent gap evidence with
  `gap=true` and `complete=false`.

- **First-new marker ordering**: The first-new `sequence_gap` Raw event is
  appended, drained, and explicitly `StreamSpool.sync()`ed before Catalog
  `STREAM_DISCONTINUITY_COMPLETED` is committed. Raw sync precedes Catalog
  completion, and an unmatched `STARTED` record is recovered across process
  restart. Multiple conflicting unmatched gaps fail closed.

- **Owned blocking worker**: Cancellation of an asyncio Task awaiting
  `asyncio.to_thread(...)` does not stop a blocking Raw or SQLite operation
  already executing in its worker thread. The asyncio owner retains ownership
  through `asyncio.shield` and waits for the worker to finish. Permanently
  hung kernel I/O therefore remains a known risk.

- **Scope**: These changes apply to USD-M only. Spot streams have not
  received the same backpressure repair; their existing `put_nowait` overflow
  behavior (visible collector fault → `CoreMarketTerminalFailure`) remains.

- **Schema compatibility**: No EventEnvelope, Raw chunk, manifest,
  normalized, replay, or Catalog market-data schema changed. The two new
  operational event types (`STREAM_DISCONTINUITY_STARTED`,
  `STREAM_DISCONTINUITY_COMPLETED`) are internal additive evidence, not a
  schema upgrade. The existing `sequence_gap`, `gap=true`, and
  `complete=false` semantics are reused.

M21.4 passed the 2h, 12h, and 24h formal windows with PID unchanged and
NRestarts=0. **The formal 72h window FAILED on data integrity**
(M21.4.10/M21.4.11): the 2026-08-07T14:08:24Z USD-M `book_ticker`
unexpected disconnect and every planned rotation sealed their reconnect
boundaries without gap evidence. The 12h/24h data-integrity acceptance is
SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING; their process-stability results
stand. The 24h PASS's readiness record (Spot 280/280; USD-M 279/280, both
orderbooks 280/280) and the gen5 RECOVERY_CONTRACT_PASS remain valid
process/orderbook evidence. The M21.4.11 forward fix later entered the M22.9
incident artifact; the additional R-054 continuity correction is local-only,
and the staged chain must restart after a separate deployment.

### M21.4.11 Reconnect boundary integrity

The 72h failure root cause: ordinary and planned reconnect paths
(`unexpected_disconnect`, `planned_rotation`, `server_shutdown`,
depth-driven `session_restart`) reused the same generation and writer and
could seal cross-connection Raw as `gap=false/complete=true`. The repair
routes every transport boundary through one state machine in both Spot and
USD-M collectors:

- Catalog `STREAM_DISCONTINUITY_STARTED` is durable before old-generation
  storage mutation in the normal path; the old generation then drains and
  seals (manifest-level `reconnect_gap` forced incomplete when no unpersisted
  last-old frame exists) before `generation++` and replacement open.
- If STARTED fails with no active writer, an atomic Catalog transaction
  publishes the preallocated zero-record marker directly as SEALING with the
  exact `seal_intent` before its Raw header is created. Every later crash phase
  restores the same gap_id; the marker is always fail-closed and retains Raw
  header collector provenance without fabricating frame provenance.
- The first new Raw frame carries `sequence_gap`; Raw sync precedes
  `STREAM_DISCONTINUITY_COMPLETED`; `historical_continuity_restored=false`.
- A connection failing before its first frame extends the pending gap (one
  gap_id, one STARTED, one generation transition); a unique unmatched
  STARTED of any reason is recovered across restart; conflicts fail closed.
- `diff_depth` never reconnects in place: any boundary retires the capture
  session (fresh connection + fresh REST Snapshot + U/u/pu bridge before
  READY); Raw gap evidence is independent of orderbook recovery.
- Intentional close (planned rotation) is not an integrity exemption;
  `server_shutdown` and `sequence_gap` evidence coexist.
- Seal defense in depth: a chunk with multiple connection_ids whose
  connection transitions lack boundary-local evidence fails closed to
  `reconnect_gap` (`gap=true`, `complete=false`). Blue/green overlap is safe
  only when it covers the exact transition.
- `tools/audit_reconnect_boundaries.py` is a strictly read-only historical
  scanner (classifications EXPLICIT_SEQUENCE_GAP / BLUE_GREEN_OVERLAP /
  UNMARKED_RECONNECT / UNKNOWN, boundary-local per connection transition,
  deterministic canonical output with cutoff + manifest inventory) used to
  quantify the 4,680 unmarked historical boundaries; historical sealed
  evidence is never rewritten.
- The audit retains consecutive zero-record manifests between the nearest
  connection-bearing chunks and emits one logical transition, so
  A -> empty... -> B cannot disappear from the denominator. Exact-pair Catalog
  semantics remain unchanged.

Full record: `docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md`.

### M21.4 backpressure timing semantics (24h evidence)

The formal 24h window clarified the deployed timing semantics. These are
architectural facts, not implementation changes:

- **`queue_backpressure_recovered` ≠ stream recovery completed**:
  `usdm_ingress_backpressure_recovered` fires when the queue depth falls
  below the low watermark. The complete stream recovery boundary is the new
  connection plus the first-new `sequence_gap` persisted plus Raw sync plus
  Catalog `STREAM_DISCONTINUITY_COMPLETED`. In the gen5 formal-window cycle
  these are distinct instants (queue recovered 13:51:09.996638Z; old
  generation sealed 13:54:38.251Z; new connection 13:54:38.577Z; first-new
  Raw and Catalog COMPLETED 13:54:38.582474Z).

- **The 30 s saturation budget is not a strict 30 s wall-clock closure
  ceiling**: the saturation timer accumulates only while the queue stays
  above low_watermark; the timeout raises only when a later `put` again
  encounters a full queue after the budget is exhausted. The gen6 cycle
  accumulated 1358.9 s of saturation with only 7 put waits before the
  boundary frame hit a full queue and triggered the timeout.

- **low_watermark reset semantics**: the timer resets only when the queue
  drops to or below low_watermark; a queue hovering above it continues
  accumulating saturation time even while the writer drains continuously.

- **Connection closure still implies a potential exchange-side gap**: an
  internal zero-drop guarantee (no Recorder queue drop, persisted frames
  CRC-verified) does not prove that exchange events between the close and
  the first new connection frame were captured. Missing interval events are
  therefore persisted as `sequence_gap`/`gap=true`/`complete=false` and
  `historical_continuity_restored=false`.

- **Zero internal drop ≠ zero market-data absence**: the Recorder can prove
  it dropped nothing internally, but it cannot prove the exchange sent
  nothing it missed. Do not equate the two.

## Configurable product runtime (MS2 implemented; MS3/MS4 pending)

ADR-0032 supersedes ADR-0031. The Recorder remains Binance-specific with
current markets Spot and USD-M perpetual, while the operator configures the
finite symbol list independently for each market. There is no fixed symbol
allowlist, automatic all-symbol discovery, or exchange/plugin framework. A
configuration change takes effect on normal process restart; topology does not
hot-reload.

`ProductKey = (market, symbol)`. One Recorder process owns one durable Catalog
and one existing `SpotCollector` or `UsdMCollector` per configured ProductKey.
The runtime authority is `Mapping[ProductKey, RuntimeCollector]`, not one
process per symbol. Each product owns its three core WebSocket streams,
snapshot/bootstrap, continuity/reconnect, order-book resync, bounded ingress,
readiness, and product-local side-data state. Recoverable product-local
transport/resync/readiness failure must not mutate another product. A terminal
core integrity/owner/storage failure remains process-fatal and restarts through
the service manager.

The implemented `[recorder]` surface is `spot_symbols = [...]` and
`usdm_symbols = [...]`. The lists are independent finite sets; the same symbol
in both creates two ProductKeys. If neither field is present, legacy
compatibility mode resolves both to `BTCUSDT`. If either field is present,
explicit product-selection mode uses supplied lists exactly and resolves an
omitted sibling to an empty list; both resolved lists empty is invalid. Symbols
are canonicalized once to uppercase at the configuration boundary;
empty/control/whitespace-invalid symbols and within-market duplicates are
rejected. Parsing does not query Binance. There are no symbol environment
variables, CLI product DSL, or ADR-level product-count maximum. MS2 acceptance is recorded in `milestone_acceptance/MS2.md`.

Spot reuses the existing process/event-loop shared Spot IP limiter. USD-M uses
one process-owned asyncio request lock and one process-owned `UsdMRestCooldown`,
injected into every configured USD-M Collector and the process-global USD-M
side-data owner. Shared rate authority is required; a shared SDK client is not
required without an established thread-safety decision.

If the resolved USD-M ProductKey set is empty, MS2 instantiates zero
`UsdMCollector` instances and zero product-specific USD-M side-data managers,
create no process-global USD-M side-data owner, and perform no USD-M REST or
WebSocket traffic. The global owner exists only when at least one USD-M
ProductKey is configured and at least one global kind is enabled; when present,
it runs `funding_info` and `exchange_info` at most once per process and shares
the same request lock/cooldown. An empty Spot set likewise creates no Spot
Collector or Spot side-data traffic.

USD-M side data has one process-global execution owner. `funding_info` and
`exchange_info` are global REST kinds. `mark_price`, `liquidation`,
`premium_index_snapshot`, `funding_history`, `open_interest`,
`open_interest_statistics_5m`, `taker_buy_sell_volume_5m`,
`global_long_short_ratio_5m`, `top_long_short_account_ratio_5m`,
`top_long_short_position_ratio_5m`, and `basis_5m` are product-specific. The
six persisted five-minute cursor families remain keyed by `(kind, symbol)`.
`GLOBAL_SIDE_DATA_SYMBOL="BTCUSDT"` remains the legacy global-side-data
sentinel for compatibility; it is not an implicitly configured BTC product.

Global core readiness requires a non-empty configured set, exact equality
between configured and actual runtime ProductKeys, and readiness of every
configured product under the existing core contract. Missing or unexpected
products fail closed; readiness is not “all observed collectors are ready”.
Service state exposes `products.spot.<SYMBOL>` and
`products.um_perpetual.<SYMBOL>` readiness plus
`expected_product_count`, `ready_product_count`, and `core_ready`. Market-level
status may remain a derived compatibility summary. VPS readiness independently
derives or receives the expected set and verifies exact topology.

Hard-reserve handling iterates configured ProductKeys × core streams and emits
the existing MS1 `(market, symbol, stream)` discontinuity identity. Historical
data for removed products is retained; unconfigured intervals are outside
active collection scope and are not continuity-complete. Adding or re-adding a
product starts a normal current bootstrap/recovery session. No Catalog
migration, Raw v1 change, Contracts production code/schema change, or Projection
change is required.

The sequence is MS2 configurable product runtime, MS3 shared-resource scaling/
rotation/observability, and MS4 configurable-product integration plus bounded
live qualification. A fixed qualification workload is evidence only, not a
supported-symbol allowlist. MS2 is offline-accepted; MS3/MS4 are not implemented
and no multi-product artifact is live-qualified;
Formal M22.9 and Production Ready remain unauthorized.

## Runtime isolation

Configured products use separate connection/session state, queues, failure
budgets, checkpoints, and readiness. Recoverable failure stays product-local;
terminal core failure stops the service coherently. USD-M
side-data tasks are still more weakly coupled and cannot block core L2.

The M4 socket receive boundary timestamps immediately after `recv(decode=False)`
and places the exact bytes plus clocks in a bounded receipt queue before JSON
parsing. A separate persistence loop extracts only Raw metadata, envelopes, and
hands off to the bounded spool. It never compresses in the callback, builds
Parquet, reconstructs books, or performs network archive I/O. Both transport
and Recorder queues are finite; for USD-M streams, sustained saturation now
triggers stream-level backpressure and recovery rather than an immediate
process-wide `CoreMarketTerminalFailure` (see M21.4 section above). Spot
streams retain the prior behavior where saturation is a visible collector
fault. `ingress_queue_capacity` applies to both receipt and spool
queues. Time-based Raw rotations use a stable market/stream phase inside the
configured period, spreading compression/fsync load without delaying any
stream beyond that period. The RK3588 deployment uses an explicitly larger
bounded capacity than the generic default; M21.4 validated queue and RSS trends
in the 2h, 12h, and 24h windows; 72h/168h windows remain pending.

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

M23.4 removes only the redundant semantic scan for a normal live-owned clean
writer. Before encoding, the writer copies all Raw semantics (including the
mutable caller-owned `source_sequence`) into one private immutable snapshot;
the exact canonical frame bytes and incremental statistics/transition delta
come from that same snapshot. Exact bytes enter the SHA/verified-byte authority
only after a complete write. Any write, evidence-commit, fsync, close, or
post-write invariant failure permanently poisons that writer's clean evidence.
After final fsync and close, one-shot memory-only evidence enters the same
durable seal implementation, whose compression read consumes the source and
whose decompressed byte count and SHA-256 must equal the evidence. General
`seal_partial()` and every restart/recovery/unknown-partial path still perform a
full Raw scan; no durable clean-evidence format or second seal protocol exists.

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

The current implementation follows ADR-0003/0015: an external target is a
registered subdirectory, never a volume; UUID-based discovery resolves its
current mountpoint; and the archive transaction is copy-to-`.copying`, fsync,
reopen/read/hash, size/hash compare, atomic rename, external manifest commit,
Catalog commit, then local delete. Every crash boundary is reconciled
idempotently. Disappearance changes storage state while the internal Collector
continues.

The approved future topology uses a local archive client pulling immutable
sealed Raw from the VPS over SSH through a transport-neutral seam. Durable
local verification, Archive Set identity, a receipt, VPS source revalidation,
and deletion authorization precede VPS deletion. See
`docs/archive_transfer_contract.md` and ADR-0029. This future workflow is not
implemented by the current local ArchiveManager.

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
and read-only discovery of already-mounted external filesystems. The future
VPS production profile uses direct Binance connectivity by default; redacted
proxy policy state is written to `service-state.v1` and local proxy modes remain
testable.

File/chunk/manifests use the unchanged language-neutral formats and UTC
timestamps, so consumers do not depend on either platform adapter. This
portability does not make multi-exchange support a current goal. The future
archive client targets macOS, Linux, and Windows, but those client
implementations and certifications do not yet exist. M20 does not certify
Linux blue/green, VPS deployment, or long-run operation.
