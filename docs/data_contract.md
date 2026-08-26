# Data Contract

Status: EventEnvelope v1 and Raw chunk v1 are executable and byte-frozen by M3
and ADR-0010. M4-M7 implement current Spot/USD-M Raw mappings. M15 implements
the rebuildable `normalized-dataset.v1` contract under ADR-0020.

The approved VPS/local archive topology changes physical custody and execution
roles only. It does not change EventEnvelope, Raw chunk, manifest, Catalog
market-data, normalized, or replay semantics. Exact Raw bytes, provenance,
explicit gaps, and historical/live clock separation remain authoritative.

## M20 transport and platform compatibility

M20 changes no EventEnvelope, Raw chunk, manifest, normalized, replay, or
Catalog market-data schema. `direct`, `environment`, and `explicit` select only
the transport used before bytes reach the existing receive boundary.

The proxy URL, proxy host, environment variable names/values, Mihomo node,
controller information, and credentials are forbidden from Raw payloads,
manifests, Catalog event bodies, normalized rows, and ordinary logs. Runtime
state may contain only mode, scheme, loopback, and port. A proxy disconnect
does not authorize an inferred complete interval: the existing connection ID,
unexpected-disconnect, gap/unreliable interval, snapshot, resync, and sequence
contracts apply unchanged.

Linux and macOS write byte-compatible Raw and Catalog state. Platform paths and
service managers are operational metadata, not data identity. Historical
Backfill continues to use archive-source clocks and now shares the selected
transport policy; it never invents a receive clock.

M20 additively annotates future `DEPTH_RESYNC_REQUESTED` and
`DEPTH_RESYNC_COMPLETED` operational-event evidence with
`interval_classification="UNRELIABLE"`; completion also carries
`gap_ended_at_utc_ns`. Existing Catalog rows are immutable and are not
backfilled. Readers of the open evidence object must continue to ignore unknown
fields. EventEnvelope, Raw chunk, manifest, normalized, and replay versions are
unchanged.

## M19 public side-data and historical clocks

Spot `exchange_info` preserves BTCUSDT `filters`, `status`, `orderTypes`,
response rate-limit headers, request/receive times, and server time when
supplied. Each USD-M stream ending `_5m` owns a durable, monotonic Cursor with
kind, last durably persisted period timestamp, update time, and official
retention-window label. It catches up in bounded pages from Cursor + 5 minutes
through the last closed period. Raw is drained and fsynced before Cursor
advance. Duplicate Raw polls are allowed; normalization owns deterministic
deduplication. A failed or empty requested period does not advance the Cursor,
is not zero, and is never forward-filled. Once a missing period ages beyond
the official window it becomes an explicit unrecoverable gap.

The public `takerBuySellVol` endpoint was observed to include one leading
5-minute overlap and to require the next period boundary as `endTime`. The
request provenance preserves the actual query and logical requested range;
Raw preserves the overlap, while Cursor advancement uses only contiguous
timestamps inside the logical requested range.

Historical source manifests use `historical-source.v1`; gaps use
`historical-gap.v1`. Historical normalized rows expose
`archive_event_time_utc_ns`, source revision, ZIP SHA-256 and
`clock_semantics=archive_source`. They have no receive UTC/monotonic clock and
must not be admitted to receive-time replay.

### M19 Live normalized consumer contract

M19 Live Raw additions are consumable through `normalized-dataset.v1` without
changing any pre-existing stream field or dataset meaning. Spot
`exchange_info` uses a market-specific v1 schema containing symbol presence,
server time, trading status, canonical filters/order-types/rate-limits and
optional permissions/permission-sets JSON, plus the canonical response-model
SHA-256. USD-M `exchange_info` retains its existing v1 fields and meanings:
symbol presence, server time, contract type, trading status, filters JSON and
rate-limits JSON. A shared stream name therefore does not imply a shared
market-incompatible schema.

Each USD-M 5-minute REST response model is expanded into one normalized row per
exchange period. Its semantic identity is
`(market, stream, symbol-or-pair, timestamp_ms)` and excludes receive time,
connection/Collector identity and REST request identity. Repeated catch-up,
restart and leading-overlap observations of identical content collapse
deterministically; differing content with the same identity remains visible as
an identity conflict. Exchange decimal values remain exact strings.

An empty response becomes an explicit `empty_observation` with no fabricated
period timestamp. Its identity uses the logical requested start/end and the
canonical response-model hash, so it cannot collide with a real period.
Out-of-range leading records remain ordinary timestamp-identified observations
and can deduplicate against the same period returned by another request.
Historical and Live datasets are not automatically joined, and M19.2 does not
provide historical L2.

## EventEnvelope v1 minimum fields

| Field | Meaning |
| --- | --- |
| `schema_version` | Envelope schema identifier, initially `event-envelope.v1` |
| `venue` | Fixed project venue identifier `binance` in V1 |
| `market` | Binance market identifier: `spot` or `um_perpetual` in V1 |
| `symbol` | Binance symbol exactly associated with the payload, initially `BTCUSDT` |
| `stream` | Binance stream kind such as `diff_depth`, `agg_trade`, `book_ticker`, or versioned REST kind |
| `module` | `binance.spot` or `binance.usdm` plus schema/implementation version |
| `connection_id` | Unique transport connection/session ID |
| `collector_instance_id` | Unique process/deployment instance ID |
| `collector_version` | Release plus Git commit provenance |
| `receive_time_utc_ns` | UTC wall-clock receive timestamp at raw payload boundary |
| `receive_monotonic_ns` | Process-local monotonic receive timestamp |
| `exchange_event_time` | Exact exchange event time when the schema supplies one |
| `exchange_transaction_time` | Exact transaction time when supplied |
| `exchange_trade_time` | Exact trade time when supplied |
| `source_sequence` | Stream-specific IDs without lossy coercion, e.g. `U/u/pu` |
| `payload_encoding` | Initially `utf-8-json` for documented JSON streams |
| `raw_payload` | Exact bytes received for the exchange payload |
| `capture_flags` | Planned rotation, overlap, server shutdown, recovery provenance |

Missing exchange fields remain absent/null with schema meaning; no field is
fabricated. Binance market-specific sequence fields remain lossless inside a
versioned mapping and Spot/USD-M meanings are not conflated. M4 selects separate
Spot raw stream endpoints. Therefore `raw_payload` is the exact unwrapped
WebSocket message bytes and `stream` is known from the endpoint even if parsing
fails. M5 also selects separate routed USD-M raw endpoints, so it preserves the
same exact unwrapped-message boundary.

M4 Spot mappings are:

| Raw stream | Envelope exchange times | `source_sequence` |
| --- | --- | --- |
| `btcusdt@depth@100ms` | `E` as event time | `U`, `u` |
| `btcusdt@aggTrade` | `E` as event time; `T` as trade time | `a`, `f`, `l` |
| `btcusdt@bookTicker` | none in the documented payload | `u` |

M5 USD-M mappings are:

| Routed raw stream | Envelope exchange times | `source_sequence` |
| --- | --- | --- |
| `/public/ws/btcusdt@depth@100ms` | `E` as event time; `T` as transaction time | `U`, `u`, `pu` |
| `/market/ws/btcusdt@aggTrade` | `E` as event time; `T` as trade time | `a`, `f`, `l` |
| `/public/ws/btcusdt@bookTicker` | `E` as event time; `T` as transaction time | `u` |

After Binance's CM migration, the core USD-M WebSocket schemas also require
the documented `st` discriminator to be integer `1` (UM). Public depth and
individual bookTicker additionally require their documented `ps` pair field to
be `BTCUSDT`; aggTrade does not require an undocumented `ps` field.
Payloads failing these identity checks are retained as exact Raw bytes and
marked malformed, never admitted under the `um_perpetual/BTCUSDT` identity.

Schema-invalid messages are retained byte-for-byte with `malformed`; duplicates
and out-of-order source IDs remain in Raw. `serverShutdown` is retained with its
`E` and a `server_shutdown` capture flag before reconnect.

REST snapshots use a versioned Binance envelope with Spot/USD-M module identity,
request URL/path, public request
parameters, response status/headers needed for rate-limit provenance, request
and response receive times, market/symbol, transport and SDK version, the
returned `lastUpdateId`, and an explicit response-payload provenance mode. The
M2-selected official SDK exposes parsed models and headers, not the exact HTTP
response body, so a snapshot must not claim byte-exact body retention. It
stores a deterministic encoded representation and declares that boundary.
Snapshots never include credentials. Existing M4 Spot records use
`binance-spot-depth-snapshot-provenance.v1` and retain the SDK model with
`raw_http_body_available=false`. M17 adds
`binance-spot-depth-snapshot-provenance.v2`/`binance.spot.rest.v2`: a minimal
unsigned official-host transport stores requested limit and weight,
allowlisted response/rate-limit headers, clocks, parsed model, and the exact
HTTP body as base64 with `raw_http_body_available=true`. Both versions remain
immutable and readable.
M5 uses the same declared provenance boundary for `/fapi/v1/depth`, with
schema `binance-usdm-depth-snapshot-provenance.v1`, the official USD-M SDK
package/version, limit 1000, and `lastUpdateId`.

## M6 local order-book and quality contract

`binance-local-orderbook.v2` consumes only versioned snapshot and diff-depth
inputs derived from immutable envelopes. Spot and USD-M inputs cannot be mixed.
For Spot, the bootstrap target and each live target are the prior reliable
update ID plus one; an event is accepted only if `U <= target <= u`. Existing
v1 checkpoints are derived and must be rebuilt rather than relabeled. USD-M
`pu` continuity is unchanged. Full semantics and the official-source conflict
follow ADR-0011. Price/quantity strings
are parsed as finite decimals; updates are absolute; zero removes a level, and
removing a missing level is valid.

A reliable logical book contains market, symbol, last applied update ID,
descending bid levels and ascending ask levels. Its deterministic hash is
SHA-256 over canonical JSON with normalized non-exponential decimal strings.
Repeated origin replay and checkpoint continuation must converge to that hash.

Sequence failure creates an `UnreliableInterval` with last reliable ID,
offending `U/u`, receive time, reason and optional resync end ID. Its
`complete` field is always false, even after resync. A book in
`RESYNC_REQUIRED` cannot be returned as reliable or checkpointed.

M6 audits empty sides, crossed best prices, resyncs, duplicate/stale updates and
same-update-ID bookTicker mismatches. bookTicker is not treated as an exchange
checksum. Tickers ahead of or behind the local update ID are classified but
not compared as if simultaneous.

## M7 USD-M auxiliary data contract

M7 adds seven independent Raw stream kinds under `um_perpetual` and BTCUSDT:

| Stream | Source | Time/sequence meaning |
| --- | --- | --- |
| `mark_price` | exact `/market/ws/btcusdt@markPrice@1s` frame | `E` is event time; `T` is `nextFundingTime`, never transaction time |
| `liquidation` | exact `/market/ws/btcusdt@forceOrder` frame | `E` event time; nested order `T` trade time; event-sparse snapshot flag |
| `premium_index_snapshot` | official SDK `/fapi/v1/premiumIndex` | response `time` and `nextFundingTime` |
| `funding_history` | official SDK `/fapi/v1/fundingRate` | observed first/last `fundingTime` and record count |
| `funding_info` | official SDK `/fapi/v1/fundingInfo` | sparse adjustment record count and observed `fundingIntervalHours` |
| `open_interest` | official SDK `/fapi/v1/openInterest` | response observation `time` |
| `exchange_info` | official SDK `/fapi/v1/exchangeInfo` | symbol/rate-limit counts and complete SDK model |

REST payloads use `binance-usdm-side-rest-provenance.v1`: public request path
and parameters, request/receive UTC and monotonic clocks, documented weight or
shared budget, safe response rate headers, canonical SDK model, package/version
and `raw_http_body_available=false`. No absent value is forward-filled. Empty
funding history, absent BTCUSDT funding adjustment metadata and WebSocket
liquidation silence remain absence, not synthetic records.

The liquidation stream is explicitly Binance's latest snapshot within a
1000 ms window, not an exhaustive liquidation ledger. Repeated REST history
responses may duplicate Raw events; later normalization owns deduplication.

## M8 operational metrics and report contract

`operational-metric-aggregate.v1` groups statistics by UTC receive date,
market, and stream. It stores counters, bounded histograms, last/max gauges and
first/last receive timestamps only. It never stores payloads, prices,
quantities, source sequences or one SQLite row per market event. Stable batch
IDs make Catalog retries idempotent.

`daily-operational-report.v1` produces one lexically ordered row per
market/stream in both JSON and flattened CSV. Required input, quality, output
and performance names always exist. A numeric zero means an available metric
with no observations; unavailable values are `null` with an explicit reason:

- REST wire bytes: `UNAVAILABLE_SDK_RAW_BODY`;
- absent histogram/runtime samples: `INSUFFICIENT_DATA`;
- external free space: sampled for each READY registered target when a storage
  monitor is active; otherwise `NO_REGISTERED_TARGET_SAMPLE` rather than a
  fabricated zero;
- normalized outputs: available from M15 as idempotent `normalized_rows` and
  `normalized_bytes` partition-commit counters;
- archive/delete outputs: available from M10 as idempotent
  `archived_files`, `archived_bytes`, and `deleted_local_bytes` counters.

Receive lag is non-negative local receive UTC minus documented exchange event
time where that event time exists. p50/p95/p99 are deterministic upper bounds
from the fixed ADR-0013 histogram. Raw output bytes include each chunk header
once plus every framed envelope, enabling reconciliation with uncompressed
chunk size. Archive backlog is sealed stored bytes minus verified archived
bytes, clamped at zero. M10 uses stable transaction-derived metric batch IDs,
so a retry cannot double-count archive or local deletion output.

Reports are immutable-source derived outputs at
`data/reports/daily/YYYY-MM-DD.json` and `.csv`. Rebuilding or overwriting a
report never modifies Raw, manifests or metric batches.

## M11 capacity history contract

`storage_space_samples` contains aggregate capacity observations only: scope,
storage ID where applicable, UTC time, total/free bytes, archive backlog, and
oldest-unarchived time. It never contains payload, price, quantity, sequence or
one row per market event. `storage-forecast.v1` exposes exact threshold bytes,
space severity, per-window rate availability, selected net growth, UTC ETAs,
backlog and oldest age. Missing and non-positive evidence use the exact
`INSUFFICIENT_DATA` and `NOT_APPROACHING` sentinels; JSON forbids NaN/infinity.

## Raw chunk logical contract

ADR-0002 selects the format family and ADR-0010 is the authoritative byte
profile. A chunk has:

- versioned magic/header and format identifiers;
- a sequence of independently length-delimited frames;
- per-frame Castagnoli CRC32C covering length, flags/reserved, and encoded body;
- exact raw payload bytes encoded as a CBOR byte string;
- a sealed manifest with file SHA-256 and required metadata;
- an unambiguous partial/sealed state.

Initial rotation defaults are 60 seconds or 128 MiB, first reached, with a
maximum configurable one-second durability window. These are configuration,
not format constants.

Every sealed chunk manifest contains:

- chunk/storage identity and relative path;
- start/end exchange and receive times where available;
- record count;
- uncompressed and stored bytes;
- envelope, chunk, and compression schema versions;
- collector version and instance/session provenance;
- SHA-256 of the stored artifact and, if compressed, SHA-256 of the canonical
  uncompressed frame stream;
- stream-specific sequence min/max plus connection boundaries;
- gap, resync, recovery/truncation, overlap, and completeness markers;
- seal and fsync completion time.

An incomplete interval cannot carry `complete=true`. Tail recovery records the
exact removed byte count and forces the later sealed manifest incomplete.

## Layer semantics

### Raw

Raw records exactly what arrived and permits duplicates, reordering, malformed
payload evidence, and blue/green overlap. Raw is append-only until seal and
immutable thereafter. Recovery may truncate only a `.partial` tail to the last
valid frame and must record that action; unrecoverable files go to quarantine.

During an ADR-0018 handoff, overlap events add stable string flags for
`blue_green_overlap`, deployment ID, `active` or `candidate` role, and handoff
reason. These flags supplement rather than replace collector instance/version,
connection ID, clocks, sequence provenance, and exact payload bytes. The
EventEnvelope schema and Raw frame encoding do not change. M13 does not dedupe;
M15 must use explicit versioned semantics and preserve source-chunk lineage.

### Normalized

`normalized-dataset.v1` is rebuildable and immutable. One explicit build
manifest selects an exact sorted Raw/checkpoint source set and the exact
content-addressed partitions belonging to that build. Consumers must not glob
all artifact files, because builds may share or supersede partitions.

Partitions live below
`market=<market>/stream=<stream>/date=<receive UTC date>/hour=<HH>` and use
explicit Arrow schemas. Every row carries the dataset, stream schema and
`normalized-dedup.v1` versions; receive/exchange clocks with units in their
names; Collector/connection provenance; source chunk ID/SHA and record
ordinal; source sequence/capture flags; exact Raw payload SHA; and semantic and
logical identity hashes. Decimal exchange values remain strings. Complex level
arrays, filters and rate-limit models are canonical JSON text.

The current matrix covers Spot diff depth, aggregate trade, book ticker and
depth snapshot, plus the same USD-M core inputs and mark price, liquidation,
premium-index snapshot, funding history, funding info, open interest and
exchange info. Funding-history empty responses and missing BTCUSDT funding-info
records become explicit observations. Invalid payloads become `valid=false`
rows; they are not silently dropped.

Deduplication uses versioned stream semantic identities, not receive paths or
filesystem order. Same identity and same logical content select the smallest
stable Raw provenance tuple while retaining every contributing source and a
duplicate count. Same identity with conflicting logical content retains every
variant with `identity_conflict=true`. Source manifest completeness, gap,
resync and recovery fields propagate to rows and partition manifests. No
forward fill hides missing events. Partition manifests bind logical/stored
SHA-256, counts, time bounds, source hashes and the exact Parquet writer
profile; build manifests index verified M6 checkpoints and their Raw lineage.

### Order-book checkpoints

Checkpoints are derived artifacts. Each records input chunk hashes, last
accepted sequence IDs, book hash, checkpoint schema/algorithm version, and gap
state. Rebuilding from origin and restoring from a checkpoint must converge to
the same deterministic book hash.

### Replay

`consumer-contract.v1` exposes one explicit `normalized-dataset.v1` build
through `ManifestCatalog`; it never infers “latest” or globs across builds.
Replay query bounds are half-open Unix nanoseconds in the selected clock.

`replay-order.v1` supports receive UTC and stream-specific documented exchange
time. Exchange milliseconds convert exactly to nanoseconds. Streams without a
documented exchange clock require explicit error, exclusion, or marked
receive-time fallback. Total ordering uses event time, stable
market/stream/symbol, Collector/connection, source-sequence, Raw chunk/ordinal
and logical identity fields; exchange order also uses receive UTC. Filesystem
order and cross-instance monotonic comparison are never semantic.

Gap policy is explicit error/include/exclude. Included events retain all source
quality fields and expose `is_unreliable`; excluded data is not relabeled
complete. A verified M6 checkpoint can seed only its single-market/symbol
diff-depth query and skips already covered final update IDs. Full public fields,
clock precedence, errors and compatibility rules are in
`docs/consumer_contract.md` and ADR-0021.

## Daily metrics schema categories

Per UTC date, market, and stream, retain at least:

- inputs: WebSocket messages/bytes, REST responses/bytes, depth bid/ask level
  updates, aggTrade and bookTicker records;
- quality: accepted, duplicate, malformed, out-of-order, sequence gaps,
  order-book resyncs, planned reconnects, unexpected disconnects,
  serverShutdown, checksum failures;
- outputs: raw records/bytes, sealed chunks, compressed bytes, normalized
  rows/bytes, archived files/bytes, local deleted bytes, archive backlog bytes;
- performance: receive-lag p50/p95/p99, queue depth, write/fsync latency, CPU,
  RSS, internal/external free bytes, oldest-unarchived and last-event ages.

JSON and CSV contain schema/report versions. Catalog aggregation is idempotent
across restart and does not duplicate counts.

## M21.4 USD-M stream discontinuity and gap evidence

M21.4 adds additive operational evidence for stream-local USD-M backpressure
recovery without changing public data schemas:

- **`sequence_gap`**: Marked on both the last-old boundary frame and the
  first-new recovered frame. The first-new marker is appended, drained, and
  explicitly `StreamSpool.sync()`ed before `STREAM_DISCONTINUITY_COMPLETED`
  is committed to Catalog. Raw sync precedes Catalog completion.

- **`gap=true`, `complete=false`**: Already-existing manifest fields are
  reused. Chunks containing a discontinuity boundary remain incomplete.
  The last-old and first-new generations must not be mixed in one chunk.

- **`STREAM_DISCONTINUITY_STARTED`** and **`STREAM_DISCONTINUITY_COMPLETED`**:
  Internal additive operational event evidence. A unique unmatched STARTED
  record is recoverable across process restart; multiple conflicting
  unmatched gaps fail closed. A matched pair clears recovery state.

- **`historical_continuity_restored=false`**: `book_ticker` and `agg_trade`
  streams cannot reconstruct lost historical events from a Snapshot.
  The gap is persistent and visible.

- **Generation boundaries**: A closed/saturated connection seals its
  generation before a new connection opens. The `connection_id` change
  flows through Raw, manifest, and Catalog metadata.

- **`diff_depth` reliability contract**: After sustained saturation,
  `diff_depth` enters UNTRUSTED. A fresh REST Snapshot and correct
  `U/u/pu` bridging are required before READY is restored. Existing
  bridging rules (Spot `U <= snapshot.last_update_id + 1 <= u`,
  USD-M `U <= snapshot.last_update_id <= u`, `next.pu == current_local_book.update_id`)
  are unchanged.

No EventEnvelope, Raw chunk, manifest, normalized, replay, or Catalog
market-data schema version changed. The two new operational event names
are internal additive evidence, not a consumer-facing schema upgrade.
Readers of operational event evidence must continue to ignore unknown fields.

### M21.4 24h forensic confirmation of gap semantics

The formal 24-hour window and its corrective/contract forensic reviews
confirmed the following contract semantics on production evidence:

- **No queue drop is not synonymous with `complete=true`**: the Recorder can
  prove it dropped nothing internally (0 drop events, persisted frames
  CRC-verified) while the interval between a WebSocket close and the first
  new connection frame cannot be proven complete from the exchange side.
  A cycle without internal drops therefore still writes
  `sequence_gap`/`gap=true`/`complete=false`.

- **Potential loss across reconnect must remain `gap=true`/`complete=false`**:
  after a backpressure-driven reconnect, the last-old boundary frame and the
  first-new frame both carry `sequence_gap` when the post-close boundary
  handoff succeeds; both old and new manifests stay incomplete. If that
  handoff times out, the unpersisted boundary payload remains absent from Raw,
  its digest and `boundary_frame_persisted=false` document the missing edge,
  and manifest-level `reconnect_gap` keeps the old tail incomplete. No layer
  may relabel either interval complete.

- **`historical_continuity_restored=false`**: `book_ticker` and `agg_trade`
  cannot reconstruct missed events from a Snapshot; the persisted gap remains
  visible in Raw flags, manifest fields, and Catalog
  `STREAM_DISCONTINUITY_COMPLETED` evidence.

- **Evidence responsibilities differ by layer**: Catalog
  `operational_events` owns STARTED/COMPLETED pairing and gap_id; Raw owns
  the exact first-new `sequence_gap` EventEnvelope; manifests own
  `gap`/`complete` per chunk; the journal logs the backpressure lifecycle but
  **never** logs the STREAM_DISCONTINUITY operational events. Journal string
  counts must not substitute for Catalog/Raw/Manifest conclusions.

- **Stream recovery completion boundary**: queue `recovered` (below
  low_watermark) is not stream recovery completion. The completion boundary
  is the new connection plus first-new `sequence_gap` persisted plus Raw sync
  plus Catalog COMPLETED.

### M21.4.11 Reconnect boundary gap contract (all transports)

The formal 72h window failed because ordinary and planned reconnect paths
sealed cross-connection intervals as complete. The M21.4.11 contract
generalizes the M21.4 backpressure evidence to **every** transport boundary
in Spot and USD-M:

- **Reason taxonomy**: `ingress_backpressure`, `unexpected_disconnect`,
  `planned_rotation`, `server_shutdown`, `session_restart`. Intentional
  close is not an exemption: exchange-side completeness between close and
  the first new frame can never be proven, so every one of these reasons
  requires persistent gap evidence.
- **Manifest-level forced gap**: when no unpersisted last-old frame exists
  (unexpected disconnect, planned rotation, server shutdown, session
  restart), the sealed tail chunk receives the `reconnect_gap` manifest
  flag: `gap=true`, `complete=false`. The flag is written only to the
  manifest (and seal evidence), never to Raw frames; already-persisted
  frames are never mutated and no exchange payload is fabricated. Catalog
  evidence states `boundary_frame_persisted=false`,
  `boundary_kind=no_last_frame_available`, and records the disconnect
  transport time rather than a payload hash. A session restart whose stop
  interrupts queue admission can instead retain an authentic received
  boundary frame in memory; if its post-close handoff times out, the frame is
  not written to Raw, its hash is retained with
  `boundary_kind=last_frame_in_hand` and
  `boundary_frame_persisted=false`, and the old manifest still receives
  `reconnect_gap`. A true global stop authorizes no replacement connection and
  does not fabricate a reconnect discontinuity from the same queue exception.
- **Ordering**: detect boundary and mint one gap identity -> Catalog
  `STREAM_DISCONTINUITY_STARTED` durable -> drain/seal old generation (forced
  gap) -> `generation++` -> open new connection -> first new frame
  `sequence_gap` -> Raw sync -> Catalog `STREAM_DISCONTINUITY_COMPLETED` ->
  connected with `historical_continuity_restored=false`. If STARTED fails and
  there is no active writer, the zero-record fallback precommits the same exact
  intent in an atomic Catalog ACTIVE+SEALING transaction **before** creating
  its Raw header. The drain-completion interleaving may write STARTED after the
  drain only when that drain closed the preceding gap; it still happens before
  a replacement connection can open.
- **Pending-gap extension**: a connection that fails before its first frame
  extends the pending gap; one gap_id, one STARTED, one generation
  transition, one COMPLETED. No nested STARTED, no GapStateConflict, no
  per-attempt generation bump.
- **Generation isolation**: one generation never contains multiple
  non-overlap connections; old receipts drain before `generation++`.
- **Seal defense in depth**: any chunk with more than one `connection_id`
  and no `sequence_gap`/`reconnect_gap`/`blue_green_overlap` provenance
  fails closed to `reconnect_gap`. Blue/green deployment overlap remains the
  only explicit safe multi-connection provenance.
- **`diff_depth`**: every boundary retires the capture session (UNTRUSTED ->
  fresh REST Snapshot -> correct U/u/pu bridge -> READY). Raw gap evidence
  stays incomplete regardless of orderbook recovery.
- **Server shutdown**: the shutdown frame keeps its `server_shutdown` Raw
  flag; the boundary additionally carries `reconnect_gap`/STARTED/COMPLETED.
  Both flags coexist.
- **Graceful global stop** creates no gap; a depth-resync session restart
  (which reopens connections) is a reconnect boundary for every stream.
- **Zero-record boundary markers** are legal Raw v1 chunks. They contain no
  exchange frame, connection id, frame timestamp, sequence range, or payload.
  They are always `reconnect_gap`/`gap=true`/`complete=false` and retain only
  authentic header-level collector instance/version provenance. Their exact
  seal intent is durable before marker filesystem/Catalog fallback state.
- **Operational lifecycle idempotency** requires an ignored STARTED or
  COMPLETED insert to read back as the exact same event type, timestamp, and
  canonical evidence; an event-id collision or missing ignored insert fails
  closed.

No EventEnvelope, Raw chunk, normalized, replay, or Catalog schema changed.
The manifest flag set gains the additive value `reconnect_gap`; manifests
remain `raw-chunk-manifest.v1` because the flag set is open-ended and only
existing `gap`/`complete` semantics are reused.

## Compatibility policy

- Additive optional envelope/manifest fields are minor schema changes.
- Removing, renaming, changing units, changing raw-byte boundaries, checksum
  coverage, ordering, deduplication, or gap meaning is a major change and needs
  migration documentation plus an ADR.
- Readers must reject unsupported major versions rather than guess.
- Raw artifacts remain readable by a version-matched reader and can regenerate
  every derived dataset.

The V1 format is designed for Binance data and generic downstream consumption,
not as a speculative multi-exchange interchange standard. Another exchange
would require a new architecture and compatibility review.
