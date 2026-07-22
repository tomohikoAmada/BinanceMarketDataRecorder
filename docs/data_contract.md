# Data Contract

Status: EventEnvelope v1 and Raw chunk v1 are executable and byte-frozen by M3
and ADR-0010. M4 implements the Spot field mappings and M5 implements the
separate USD-M mappings and official fixtures below.

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

### Normalized

Normalized datasets are rebuildable, versioned, partitioned by UTC date/hour,
and trace every partition to source chunk hashes. They own explicit schema
validation, deduplication, timestamp normalization, gap propagation, and
market-specific field interpretation. No forward fill hides missing events.

### Order-book checkpoints

Checkpoints are derived artifacts. Each records input chunk hashes, last
accepted sequence IDs, book hash, checkpoint schema/algorithm version, and gap
state. Rebuilding from origin and restoring from a checkpoint must converge to
the same deterministic book hash.

### Replay

Replay supports receive-time and exchange-time clocks. Ordering is defined by a
versioned policy and a stable provenance tie-break, never filesystem listing
order. Callers select an explicit gap policy. Equal timestamps and duplicate
events produce deterministic output.

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
