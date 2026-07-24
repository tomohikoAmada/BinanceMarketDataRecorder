# ADR-0021: Deterministic replay and generic consumer boundary

- Status: Accepted
- Date: 2026-07-24
- Milestone: M16
- Refines: ADR-0004 and ADR-0020

## Context

`normalized-dataset.v1` provides immutable typed Parquet plus content-addressed
build, partition and checkpoint lineage. A consumer still needs a stable way to
select one build, query its contents, choose a clock, handle missing exchange
times and gaps, and seek from a verified order-book checkpoint. Filesystem glob
order, a mutable “latest” pointer, external-volume mountpoints, and consumer
project internals cannot become part of replay semantics.

Exchange time is not universal. Some Binance events have event, transaction or
trade time; some REST observations have an observation/funding/server time;
Spot bookTicker, depth snapshots and funding-info observations have no
documented exchange clock. Receive monotonic values cannot be compared across
Collector instances.

## Decision

### Public boundary

Add a public `binance_market_data_recorder.replay` package. Its supported
surface is the names exported from that package:

- `ManifestCatalog` lists and opens explicit normalized build IDs;
- `ReplayDataset` queries public partition/checkpoint descriptors and replays;
- `ReplayQuery` freezes filters and policies;
- `ReplayEvent` exposes an immutable row mapping plus selected event time;
- `ReplayClock`, `GapPolicy`, and `MissingExchangeTimePolicy` require explicit
  clock/quality choices.

Consumers provide the Recorder application-data root and an exact build ID.
The implementation resolves and verifies manifest-relative paths internally.
Public descriptors contain identities, hashes, counts and time bounds but no
absolute path or external archive mountpoint. There is deliberately no
automatic newest-build selection.

### Query bounds

A query filters optional market and stream sets, one symbol, and a half-open
`[start_time_ns, end_time_ns)` interval in the selected clock. All public time
bounds are Unix nanoseconds. Filters are applied to normalized rows from only
the selected build manifest; files belonging only to another build are never
read.

### Clock policy

`replay-order.v1` supports:

- `RECEIVE_TIME`: `receive_time_utc_ns`;
- `EXCHANGE_TIME`: a documented stream-specific millisecond field converted
  exactly to nanoseconds.

The exchange-time field policy is:

| Stream | Field precedence |
| --- | --- |
| `diff_depth` | transaction time, then event time |
| `agg_trade` | trade time, then event time |
| `book_ticker` | transaction time, then event time |
| `mark_price` | event time |
| `liquidation` | event time, then order trade time |
| `premium_index_snapshot`, `open_interest` | observation time |
| `funding_history` | funding time |
| `exchange_info` | server time |
| `depth_snapshot`, `funding_info` | no exchange clock |

Missing exchange time uses a required policy:

- `ERROR` (default): reject the matching query before yielding any event;
- `EXCLUDE`: omit that row;
- `FALLBACK_RECEIVE`: use receive UTC explicitly and mark the event as a
  fallback.

The total order never uses file enumeration. Receive order is:

```text
event_time_ns, market, stream, symbol, collector_instance_id, connection_id,
receive_monotonic_ns, source_sequence_json, source_chunk_sha256,
source_record_ordinal, source_subrecord_ordinal, logical_record_sha256
```

Exchange order inserts `receive_time_utc_ns` immediately after
`event_time_ns`, then uses the same stable fields. Monotonic time is ordered
only after Collector identity, so it is never compared as a global clock.

### Gap policy

An unreliable row is one with `source_gap=true` or `source_complete=false`.
Every yielded row retains the original gap/resync/recovery columns.

- `ERROR` (default): reject the query before yielding any event if a selected
  row is unreliable;
- `INCLUDE`: yield it with `is_unreliable=true`;
- `EXCLUDE`: omit it without relabeling other data as complete.

No policy forward-fills, synthesizes missing market events, or erases build and
checkpoint gap evidence.

### Checkpoint seek

A checkpoint can seek only a single-market, single-symbol `diff_depth` query.
It must be named in the selected build manifest, pass file/document/book hash
verification, use the supported checkpoint/reconstruction version, and have
source hashes contained in that build. The public handle exposes its immutable
book state and gap history. Replay then skips depth rows whose
`final_update_id <= checkpoint.update_id`; normal time and gap policies still
apply. A checkpoint is an acceleration/state seed, not proof that later data
is gap-free.

### Verification and resource behavior

Opening a build verifies build/partition manifest agreement, safe relative
paths, artifact size/SHA-256 and checkpoint identities. Replay scans Parquet in
fixed batches and performs a bounded-fan-in external merge sort in an ephemeral
temporary directory. Temporary work is not persistent Recorder data. Results
are immutable mappings; the API never writes Raw, normalized artifacts,
manifests, Catalog, archive state or consumer repositories.

## Consequences

- Identical selected build, query and policies produce an identical total
  order and event content.
- Consumers need one configured Recorder root, not knowledge of internal versus
  external Raw location or mounted archive paths.
- Exchange-time analysis cannot silently reinterpret events without a
  documented exchange clock.
- Explicit build selection makes retention and reproducibility visible to the
  caller.
- The independent example consumer can depend only on the exported replay
  package and the documented contract.

## Alternatives rejected

- Glob all Parquet files: mixes builds and duplicates logical data.
- Infer a newest build from filename or mtime: neither is a published semantic
  chronology.
- Use exchange event time for every stream: invents meaning where Binance does
  not provide it.
- Fall back to receive time silently: makes clock studies irreproducible.
- Compare monotonic clocks across instances: invalid across process/boot
  domains.
- Expose archive transactions/mountpoints to consumers: reverses the storage
  boundary.
- Add a strategy/backtest-specific adapter to Recorder core: violates the
  generic consumer contract.

## Rollback

Retain `normalized-dataset.v1`, Raw, manifests and checkpoints. Remove the M16
public replay package/example and revert the contract/ADR changes. Consumers
that require `replay-order.v1` retain the matching package version or export
their selected deterministic event sequence. Never rewrite source datasets as
replay rollback.
