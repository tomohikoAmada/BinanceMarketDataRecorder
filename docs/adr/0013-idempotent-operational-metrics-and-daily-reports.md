# ADR-0013: Idempotent Operational Metrics and UTC Daily Reports

- Status: Accepted
- Date: 2026-07-22
- Milestone: M8

## Context

Operations needs per-market/per-stream input, quality, output and performance
evidence across UTC midnight and process restarts. SQLite may retain Catalog and
statistics but must not become a second market-event store. Counters must not
double when a failed persistence attempt is retried, and a report must not hide
fields merely because the responsible later milestone is not implemented.

Official modular REST SDK responses expose parsed models but not exact HTTP
response bytes. Reporting a canonical model size as wire traffic would be
semantically false. External-volume capacity belongs to M9, archive completion
to M10, and normalized output to M15.

## Decision

Use `operational-metric-aggregate.v1` batches. A batch contains only counters,
bounded histogram buckets, last/max gauges and first/last receive times grouped
by UTC date, market and stream. It contains no Raw payload, price, quantity,
sequence event or per-event identity. The Catalog `metric_batches` table has a
composite primary key rooted in a stable batch ID. A whole batch is committed in
one `BEGIN IMMEDIATE` transaction; retrying the same ID is a no-op.

Each process keeps a stable pending batch ID until commit succeeds. A successful
commit clears the in-memory aggregate and creates a new ID. Restart creates a
new batch and therefore continues the daily sum without replaying an already
committed batch. The fixed acceptance fixture also retries an identical batch
ID to prove it cannot increment twice.

The Raw spool observes an envelope only after its complete framed bytes were
appended. The first record in a chunk includes the chunk header in
`raw_bytes_written`, so per-chunk sums reconcile with uncompressed artifact
bytes. `os.write` and `fsync` durations are separate histograms. Seal success
adds chunk, compressed-byte and archive-backlog counters only after the verified
manifest exists. Connection lifecycle records planned rotations and unexpected
disconnects. M6 audit callbacks expose duplicate/stale depth, sequence gaps and
resyncs without putting order-book levels in SQLite.

UTC date is derived solely from `receive_time_utc_ns`. The first observation of
a newer UTC date atomically commits pending older aggregates and writes their
reports. Graceful Collector shutdown commits and writes all remaining dates.
Out-of-order older evidence remains assigned to its actual UTC date.

`daily-operational-report.v1` is deterministically merged by
`(market, stream, batch_id)`. JSON uses sorted compact keys and a terminal
newline. CSV has one row per market/stream and lexically sorted flattened
columns. Both are written as mode-0600 temporary files, fsynced, atomically
renamed and followed by directory fsync under
`data/reports/daily/YYYY-MM-DD.*`.

Histograms use fixed versioned nanosecond upper bounds and expose p50/p95/p99 as
the containing upper bound. Missing samples report `INSUFFICIENT_DATA` rather
than zero. REST response wire bytes report `UNAVAILABLE_SDK_RAW_BODY`.
External free bytes report `UNAVAILABLE_UNTIL_M9`; normalized/archive/delete
outputs report `NOT_IMPLEMENTED` until their owning milestones. A zero is used
only for an available counter with no observations.

`binance-market-recorder status` reads existing evidence but never creates a
fictional running service. Until supervised service state is implemented, it
returns `NOT_RUNNING`, `network_connected=false`, Catalog/chunk evidence,
internal capacity and the latest daily report. `report daily` builds the
authoritative report from Catalog aggregates and never reads market payloads.

Collector-facing metric callbacks are failure-isolated. An aggregation,
rollover, Catalog or report exception increments an in-memory failure count,
records the error type in structured logs and returns control to Raw capture.
The strict methods remain available to tests and explicit callers so invalid
metric input is never silently accepted.

## Consequences

SQLite grows with flush batches rather than event rate. Reports can be rebuilt
from the Catalog batches and overwritten deterministically for the same input
and generated timestamp. Histogram percentiles are bounded approximations, not
stored raw samples. CPU/RSS/free-space samples are periodic and may be
`INSUFFICIENT_DATA` for very short streams. Queue depth is the bounded Raw spool
queue; transport receipt overflow remains a separate explicit quality failure.

A sudden kill may lose the uncommitted metrics batch even though Raw recovery
retains the event. Later reconciliation can rebuild Raw output counts from
manifests; full automated reconciliation is part of the M17 fault campaign.
This limitation never changes or deletes Raw.

## Rollback

Stop report generation, retain Raw and existing report evidence, and revert M8.
The additive `metric_batches` table and report files may be ignored or rebuilt;
rollback must not edit sealed chunks or manifests.
