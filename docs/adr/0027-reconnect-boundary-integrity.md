# ADR-0027: Every WebSocket reconnect boundary carries persistent gap evidence

- Status: Accepted (M21.4.11)
- Date: 2026-08-10
- Relates to: ADR-0009 (WebSocket transport), ADR-0023 (depth resync and
  terminal recovery)

## Context

The formal 72-hour validation of the M21.4 artifact failed on data integrity.
On 2026-08-07T14:08:24Z a USD-M `book_ticker` connection closed with
`ConnectionClosedError` and the collector reconnected within the same
generation and the same writer task. The chunk `7917819a` therefore sealed
with two connection_ids and `gap=false`/`complete=true`, and the Catalog held
no `STREAM_DISCONTINUITY_STARTED`/`COMPLETED`. The same silent-gap class
applies to every `planned_rotation` (all five observed rotations plus the
post-window rotation), to Spot `server_shutdown`, and to the depth-driven
capture-session restarts that close and reopen every stream connection.

Exchange-side completeness between a WebSocket close and the first frame of a
new connection can never be proven: `book_ticker`/`agg_trade` have no
sequence-continuity proof and no snapshot recovery, and `diff_depth` can
recover a book state but never the missing Raw event stream. Intentional
close (planned rotation) is not an exemption. The backpressure-only gap
evidence added in M21.4 was correct but too narrow.

## Decision

Every transport boundary that closes one connection and opens another in
Spot and USD-M goes through one Reconnect Boundary state machine:

1. drain the old generation;
2. seal it — when no unpersisted last-old frame exists (unexpected
   disconnect, planned rotation, server shutdown, session restart) the
   manifest carries the additive `reconnect_gap` flag forcing
   `gap=true`/`complete=false`; persisted Raw frames are never mutated and
   no exchange payload is fabricated;
3. persist Catalog `STREAM_DISCONTINUITY_STARTED` (durable, any reason from
   `{ingress_backpressure, unexpected_disconnect, planned_rotation,
   server_shutdown, session_restart}`);
4. increment the generation;
5. open the new connection;
6. mark the first new frame `sequence_gap`;
7. Raw sync, then Catalog `STREAM_DISCONTINUITY_COMPLETED` with
   `historical_continuity_restored=false`.

Supporting rules:

- A connection failing before its first frame extends the pending gap: one
  gap_id, one STARTED, one generation transition, one COMPLETED. No nested
  STARTED, no per-attempt generation bump.
- A unique unmatched STARTED of any reason is restored across restart with
  the same gap identity; conflicting gaps fail closed
  (`IngressGapStateConflict`).
- `diff_depth` never reconnects in place: any boundary retires the capture
  session; READY returns only after a fresh REST Snapshot and correct
  U/u/pu bridging. Raw gap evidence is independent of orderbook recovery.
- A true global stop creates no gap; a depth-resync session restart (which
  reopens connections) is a reconnect boundary for every stream.
- `server_shutdown` Raw flags and reconnect gap evidence coexist.
- Seal defense in depth: a chunk with more than one connection_id and no
  `sequence_gap`/`reconnect_gap`/`blue_green_overlap` provenance fails
  closed to `reconnect_gap`. Blue/green overlap remains the only explicit
  safe multi-connection provenance.
- Historical sealed evidence is immutable. `tools/audit_reconnect_boundaries.py`
  is a read-only scanner classifying
  EXPLICIT_SEQUENCE_GAP / BLUE_GREEN_OVERLAP / UNMARKED_RECONNECT / UNKNOWN;
  additive remediation (e.g. Catalog integrity-correction events or a
  versioned superseding index) is designed but not executed in this ADR.

## Alternatives

- Patching only the `ConnectionClosedError` path would have left planned
  rotation, server shutdown, and session restart silent; the 72h forensics
  proved rotation is equally unprovable.
- Rewriting already-persisted Raw frames to carry `sequence_gap` violates
  Raw immutability and would fabricate marker semantics on old data.
- Treating orderbook synchronization as proof of `book_ticker`/`agg_trade`
  completeness is invalid: the book is derived from `diff_depth` only.

## Consequences

- Every reconnect boundary now costs one generation seal and one
  STARTED/COMPLETED pair; the 1000-cycle stress test seals 1000 generations
  (~11-52s observed). Rare in production (one rotation per stream per day).
- Manifests gain the additive `reconnect_gap` flag value; `raw-chunk-manifest.v1`
  is unchanged because the flag set is open-ended and only existing
  `gap`/`complete` semantics are reused.
- 4,680 historical unmarked boundaries (read-only audit) remain immutable;
  consumers must treat them as unreliable until an additive correction
  ships.

## Rollback

Reverting to the old behavior would re-introduce `gap=false/complete=true`
across reconnect boundaries and invalidate the data-integrity contract;
do not revert without a replacement evidence mechanism.
