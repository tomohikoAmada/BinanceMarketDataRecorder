# Data Contract

Status: M0 logical contract. Exact binary test vectors and executable schemas
are M3 deliverables; Binance field mappings and fixtures require M2 evidence.

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
versioned mapping and Spot/USD-M meanings are not conflated. Combined-stream wrapper
bytes versus inner payload bytes must be resolved by the M2 transport ADR and
recorded explicitly—never silently mixed.

REST snapshots use a versioned Binance envelope with Spot/USD-M module identity,
request URL/path, public request
parameters, response status/headers needed for rate-limit provenance, request
and response receive times, exact response bytes, market/symbol, transport and
SDK version, and the returned `lastUpdateId`. They never include credentials.

## Raw chunk logical contract

ADR-0002 is authoritative. A chunk has:

- versioned magic/header and format identifiers;
- a sequence of independently length-delimited frames;
- per-frame CRC32C covering the encoded frame data and declared length;
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

An incomplete interval cannot carry `complete=true`.

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
