# Data Contract

Status: EventEnvelope v1 and Raw chunk v1 are executable and byte-frozen by M3
and ADR-0010. M4-M7 implement current Spot/USD-M Raw mappings. M15 implements
the rebuildable `normalized-dataset.v1` contract under ADR-0020.

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
Snapshots never include credentials. M4's Spot snapshot payload encoding is
`utf-8-json-provenance` with schema
`binance-spot-depth-snapshot-provenance.v1`; it contains the canonical SDK model,
allowlisted public response/rate-limit headers, request/receive clocks,
`/api/v3/depth`, `BTCUSDT`, requested limit, package/version, and the explicit
`raw_http_body_available=false` boundary.
M5 uses the same declared provenance boundary for `/fapi/v1/depth`, with
schema `binance-usdm-depth-snapshot-provenance.v1`, the official USD-M SDK
package/version, limit 1000, and `lastUpdateId`.

## M6 local order-book and quality contract

`binance-local-orderbook.v1` consumes only versioned snapshot and diff-depth
inputs derived from immutable envelopes. Spot and USD-M inputs cannot be mixed.
Snapshot bootstrap and live continuity follow ADR-0011. Price/quantity strings
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
