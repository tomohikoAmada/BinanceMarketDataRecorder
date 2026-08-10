# ADR-0027: Every WebSocket reconnect boundary carries persistent gap evidence

- Status: Accepted (M21.4.11), Corrected (M21.4.11-R1..R5)
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

1. detect the boundary and capture the closing connection identity;
2. persist Catalog `STREAM_DISCONTINUITY_STARTED` (the durable reconnect
   intent) **BEFORE any storage mutation whose correct crash recovery
   depends on it** — in particular before the old-generation seal
   (M21.4.11-R1);
3. drain the old generation and seal it — when no unpersisted last-old
   frame exists (unexpected disconnect, planned rotation, server shutdown,
   session restart) the manifest carries the additive `reconnect_gap` flag
   forcing `gap=true`/`complete=false`; persisted Raw frames are never
   mutated and no exchange payload is fabricated;
4. increment the generation;
5. open the new connection;
6. mark the first new frame `sequence_gap`; the gap may only be completed
   by a frame whose connection_id differs from the gap's
   `original_connection_id` (a boundary frame drained with the old
   generation never closes the gap);
7. Raw sync, then Catalog `STREAM_DISCONTINUITY_COMPLETED` with
   `historical_continuity_restored=false`.

### Crash-durable state machine (M21.4.11-R1)

The durable ordering makes the reconnect fact survive every crash phase:

| Phase | Durable state after crash | Recovery behavior |
|---|---|---|
| `BOUNDARY_DETECTED` | in-memory only; nothing durable | no evidence exists; Raw preserved as-is |
| `BOUNDARY_INTENT_DURABLE` | Catalog STARTED committed | startup restores the same gap_id |
| `OLD_GENERATION_DRAINED` | STARTED + partial (ACTIVE) | partial preserved; never sealed complete |
| `OLD_GENERATION_SEALING` | STARTED + Catalog SEALING + partial | `recover_storage` re-seals with forced `reconnect_gap` derived from the open gap |
| `OLD_GENERATION_SEALED_INCOMPLETE` | STARTED + sealed manifest `gap=true` | `reconcile_sealed` keeps the manifest; no rewrite |
| `NEW_GENERATION_AUTHORIZED` | STARTED + old manifest | first new frame carries `sequence_gap` |
| `FIRST_NEW_RAW_SYNCED` | STARTED + first-new frame | pending gap restores; COMPLETED re-recorded idempotently |
| `DISCONTINUITY_COMPLETED` | STARTED + COMPLETED | no duplicate events on restart |

Startup recovery (`spool/recovery.py`) derives fail-closed seal flags from
durable state: a partial re-sealed for a market/stream that has an unclosed
Catalog STARTED is forced to `reconnect_gap`. In-memory `forced_flags` are
never required for correctness after a crash. An existing manifest that
contradicts freshly derived completeness semantics (e.g. `complete=true`
while durable reconnect intent requires `gap=true`) is rejected, never
silently adopted (`seal_partial._validate_existing_manifest`).

The intent decision has two points, both before any replacement connection
may deliver frames (M21.4.11-R1 review):

1. before the old-generation drain/seal — the normal case: the pending gap
   (if any) belongs to an earlier boundary and is not touched by this
   generation's drain, so STARTED for the current boundary is recorded
   first (INV-007);
2. after the drain, only when the drain itself completed the pending gap —
   the pending gap's first-new frame was still in the writer queue when the
   boundary was detected, and persisting it during the drain recorded
   COMPLETED for the earlier gap. In that interleaving the current boundary
   would otherwise open its replacement with no durable intent and an
   unmarked first frame (INV-009/INV-010), so STARTED is recorded
   immediately after the drain, before the replacement connection opens.
   There is no in-hand boundary frame for this transition; the old
   connection's frames were already drained with its generation.

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
- Seal defense in depth: a chunk with more than one connection_id whose
  connection transitions lack boundary-local evidence fails closed to
  `reconnect_gap`. Blue/green overlap is safe only when it covers the exact
  transition: both boundary frames carry the overlap flag with a shared
  deployment identity. A lone overlap flag elsewhere in the chunk never
  exempts an unrelated transition (M21.4.11-R2/REQ-601).
- Historical sealed evidence is immutable.

### Side-data transport tasks fail closed (M21.4.11-R4)

Side-data WebSocket collectors (`mark_price`, `liquidation`) handle their own
network reconnects through the same state machine. Any exception that
escapes the collector is a terminal integrity/storage failure: the old
writer cannot be proven safely reconciled, so `SideDataSupervisor` marks the
task `FAILED` and never opens a replacement connection without a durable
boundary. The side stream may stay FAILED (recovered only by a service
restart that runs startup recovery) while the core continues. REST pollers
are stateless per request and remain retryable.

### Historical audit (M21.4.11-R2/R3/R5)

`tools/audit_reconnect_boundaries.py` classifies each connection transition
boundary-locally as EXPLICIT_SEQUENCE_GAP / BLUE_GREEN_OVERLAP /
UNMARKED_RECONNECT / UNKNOWN:

- Intra-chunk transitions use the exact boundary pair (`last_old_frame`,
  `first_new_frame`). A `sequence_gap` on the first frame of the new
  connection (recovery marker) or an end marker on the last frame of the
  old connection (backpressure boundary frame) is boundary-specific; a
  marker on a single-frame connection is ambiguous (UNKNOWN).
- Inter-chunk transitions use the exact pair plus boundary-specific
  manifest proof: a single-connection old chunk sealed with
  `reconnect_gap` documents exactly its own end boundary, and a Catalog
  gap interval whose `original_connection_id`/`new_connection_id` match the
  exact connection pair proves the boundary. Unattributable evidence is
  UNKNOWN; adjacent-manifest flags are never borrowed for an unrelated
  transition.
- The tool is strictly read-only: it never creates directories, opens the
  Catalog read-only, rejects `--output` that resolves inside the data root
  (including through symlinks), and works on read-only mounts.
- Output is split into a deterministic canonical payload (byte-identical
  for the same manifest inventory + `--cutoff-utc-ns`) and a non-canonical
  execution wrapper carrying `generated_at_utc*` and `canonical_sha256`.
  The canonical payload states `audit_cutoff_utc_ns`,
  `manifest_inventory_count`, and `manifest_inventory_sha256`.
- Catalog summary counts use exact gap_id pairing: `matched_pairs`,
  `unmatched_started`, `unmatched_completed`; never count subtraction.

## Alternatives

- Patching only the `ConnectionClosedError` path would have left planned
  rotation, server shutdown, and session restart silent; the 72h forensics
  proved rotation is equally unprovable.
- Rewriting already-persisted Raw frames to carry `sequence_gap` violates
  Raw immutability and would fabricate marker semantics on old data.
- Treating orderbook synchronization as proof of `book_ticker`/`agg_trade`
  completeness is invalid: the book is derived from `diff_depth` only.
- Recording the intent after the seal (M21.4.11 initial head) leaves a
  crash window in which startup re-seals the old partial without forced
  flags and could claim complete=true; the corrected ordering records
  STARTED first.

## Consequences

- Every reconnect boundary now costs one generation seal and one
  STARTED/COMPLETED pair; the 1000-cycle stress test seals 1000 generations
  (~11-52s observed). Rare in production (one rotation per stream per day).
- Manifests gain the additive `reconnect_gap` flag value; `raw-chunk-manifest.v1`
  is unchanged because the flag set is open-ended and only existing
  `gap`/`complete` semantics are reused.
- Corrected read-only historical audit (2026-08-10, cutoff
  1786349202047196027, inventory 161,817 manifests, SHA-256
  `ffaf34bdc29c016b0251f64252bc2c35edd43faba014c030b0834b9cc585dad3`,
  canonical payload SHA-256
  `7143bc0cd3370c831df773a5bd9246d86ee95377beb57e08097519a9c8a520b3`):
  **4,691 connection transitions, 11 explicit, 4,680 unmarked, 0 unknown/
  ambiguous**. The 11 explicit transitions are exactly the known USD-M
  `book_ticker` backpressure cycles, now proven by exact Catalog
  connection-pair identity. The earlier 4,680/11 figures from the
  manifest-flag-classified scanner are retained only as
  `SUPERSEDED_BY_CORRECTED_BOUNDARY_LOCAL_AUDIT`; the corrected rerun
  confirms the same totals with boundary-local evidence.
- Consumers must treat the 4,680 unmarked intervals as unreliable until an
  additive correction ships.
- M21.4.11 is a correction to the PR #10 implementation; the production
  artifact has not been deployed, and no production data was modified by
  the audit (before/after inventory diff shows only the running recorder's
  own writes).

## Rollback

Reverting to the old behavior would re-introduce `gap=false/complete=true`
across reconnect boundaries and invalidate the data-integrity contract;
do not revert without a replacement evidence mechanism.
