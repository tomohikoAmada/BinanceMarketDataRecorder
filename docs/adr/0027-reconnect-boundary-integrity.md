# ADR-0027: Every WebSocket reconnect boundary carries persistent gap evidence

- Status: Accepted (M21.4.11), Corrected (M21.4.11-R1..R5, M21.4.11-R2,
  M21.4.11-R2.1, M21.4.11-R2.2, M21.4.11-R3, M21.4.11-R3.1,
  M21.4.11-R3.2, M21.4.11-R3.3)
- Date: 2026-08-10
- Relates to: ADR-0009 (WebSocket transport), ADR-0023 (depth resync and
  terminal recovery), ADR-0004 (clock and replay semantics)

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

### Crash-durable state machine (M21.4.11-R1, extended M21.4.11-R2)

Startup recovery (`spool/recovery.py`) derives fail-closed seal flags from
durable state (see the R2 authority rules below). In-memory `forced_flags`
are never required for correctness after a crash. An existing manifest that
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

### Crash fallback: durable SEALING seal intent (M21.4.11-R2/P1-A)

Recording STARTED before the seal closes every single-fault crash path, but
a double fault can still erase the reconnect semantics:

1. the boundary is detected and the intent decision runs;
2. the Catalog `STREAM_DISCONTINUITY_STARTED` write fails before committing;
3. the writer still seals the old generation with the in-memory forced
   flags, committing `ChunkState.SEALING`;
4. the seal then crashes before SEALED, and the writer error must not be
   swallowed by the intent failure;
5. on restart the SEALING coordination previously derived forced flags only
   from unmatched STARTED records — with STARTED absent the old partial
   could be sealed `gap=false`/`complete=true`.

R2 closes the double fault with a durable seal-intent authority:

- The writer's `seal_partial` call persists the reconnect seal semantics
  into the `ChunkState.SEALING` transition evidence (`seal_intent`), BEFORE
  any artifact, manifest, or SEALED mutation. The intent carries the
  required forced flags plus the exact boundary identity: `gap_id`, reason,
  market, stream, `original_connection_id`, `original_generation`,
  `gap_started_at_utc_ns`, boundary kind, and (when a boundary frame was in
  hand) its payload SHA-256. The `gap_id` is minted once per boundary in
  `run()` and shared by the seal intent and the STARTED/COMPLETED pair, so
  recovery never invents a second logical gap (INV-010).
- Startup recovery derives the required forced flags from ALL durable
  authority, in priority order:
  1. the durable SEALING seal intent (authority B — survives the STARTED
     write failure);
  2. an unclosed `STREAM_DISCONTINUITY_STARTED` (authority A).
  When the intent exists without a matching STARTED, recovery
  deterministically materializes the pending discontinuity with the same
  `gap_id` (same identity fields), so the replacement generation restores
  it, marks its first new frame `sequence_gap`, and later closes exactly one
  coherent COMPLETED event. When both authorities exist they must agree on
  `gap_id`, market, stream, reason, original connection, and generation; a
  mismatch fails closed (`RecoveryConflictError`), never guessing.
- A SEALING chunk with no reconnect intent at all is sealed with ordinary
  semantics: blanket "every SEALING chunk is a gap" forcing is prohibited
  (TEST-106).
- The propagated intent failure retains the writer failure as its cause
  (REQ-109): a seal failure after the SEALING commit never disappears.
- A crash before the intent write and before any SEALING transition leaves
  a clean orphan ACTIVE partial. Startup deliberately retains it (P2-A):
  it is registered ACTIVE, never auto-sealed complete, no manifest is
  written, no gap event is fabricated, and the next collector opens a fresh
  generation. There is no durable evidence that it was cut at a transport
  boundary, and unsealed Raw evidence stays recoverable; the recorded
  interval is never claimed complete=true without a boundary decision.
- Spot owns its blocking writer work with the same deferred-cancellation
  policy as USD-M (P2-B): `drain_all`/`close_and_seal` run through
  `run_owned_blocking_call`, and a cancelled writer task waits for its
  worker then releases the descriptor via the owned `abort_writer` chain. A
  cancelled asyncio owner never reports complete while a worker thread still
  mutates the StreamSpool.

The durable ordering table therefore gains one phase:

| Phase | Durable state after crash | Recovery behavior |
|---|---|---|
| `BOUNDARY_DETECTED` | in-memory only; nothing durable | no evidence exists; Raw preserved as-is; orphan ACTIVE partial retained, never sealed complete |
| `SEAL_INTENT_DURABLE` | SEALING evidence `seal_intent` (STARTED absent) | STARTED materialized with the same gap_id; old partial sealed fail-closed |
| `BOUNDARY_INTENT_DURABLE` | Catalog STARTED committed | startup restores the same gap_id |
| `OLD_GENERATION_DRAINED` | STARTED + partial (ACTIVE) | partial preserved; never sealed complete |
| `OLD_GENERATION_SEALING` | STARTED + Catalog SEALING + partial | `recover_storage` re-seals with forced `reconnect_gap` derived from the open gap |
| `OLD_GENERATION_SEALED_INCOMPLETE` | STARTED + sealed manifest `gap=true` | `reconcile_sealed` keeps the manifest; no rewrite |
| `NEW_GENERATION_AUTHORIZED` | STARTED + old manifest | first new frame carries `sequence_gap` |
| `FIRST_NEW_RAW_SYNCED` | STARTED + first-new frame | pending gap restores; COMPLETED re-recorded idempotently |
| `DISCONTINUITY_COMPLETED` | STARTED + COMPLETED | no duplicate events on restart |

### Gap-lifecycle-keyed recovery (M21.4.11-R2.1)

Every startup-recovery decision about a durable SEALING seal intent is keyed
by the intent's own gap_id lifecycle, never by "some unmatched gap exists on
this market/stream". `Catalog.stream_discontinuity_lifecycle` returns one of
`ABSENT` (no STARTED and no COMPLETED record for the gap), `OPEN` (unmatched
STARTED), or `CLOSED` (a COMPLETED record exists).

- A CLOSED intent is historical: recovery neither re-materializes it, nor
  reopens it, nor compares it against an unrelated currently-open gap, nor
  changes its completed history (REQ-101, INV-002/INV-003). Historical
  COMPLETED G1 therefore never conflicts merely because a later G2 is open
  (RR-001: the pre-R2.1 code scanned every historical SEALING transition as a
  current pending intent and falsely raised
  `RECOVERY_SEAL_INTENT_STARTED_CONFLICT` on that exact legal history).
- An OPEN intent and its same-gap unmatched STARTED must agree exactly on
  gap_id, market, stream, reason, original_connection_id and
  original_generation; a mismatch fails closed (REQ-102, INV-004).
- An ABSENT intent with no other unmatched discontinuity on the
  market/stream materializes STARTED with the SAME durable gap_id; never a
  second gap_id (REQ-103, INV-001).
- An ABSENT intent next to a genuinely different unmatched gap is a true
  competing open gap and fails closed (REQ-104, INV-005); the fix does not
  suppress conflicts. Repeated startup remains idempotent (REQ-107).

### No-active-writer boundary marker chunk (M21.4.11-R2.1/AUDIT-001)

When the last active chunk auto-rotated and sealed, no new frame created
another writer, and the transport boundary arrives, `close_and_seal()` has no
active chunk to carry the fallback SEALING seal intent. If the STARTED write
also failed (P1-A double fault) the intent silently vanished and a later
restart opened an unmarked first frame — the silent-gap class this ADR
prohibits. `StreamSpool._seal_current` now seals an explicit zero-record
boundary marker chunk carrying the intent and the forced flags when a seal
intent is requested and no writer exists: no Raw frame is fabricated
(INV-008), the marker manifest documents the boundary
`gap=true`/`complete=false`, and startup recovery reconstructs the pending
discontinuity from its SEALING evidence so the replacement generation marks
its first frame `sequence_gap` (INV-010). When a writer does exist the
behavior is unchanged.

### Catalog-first marker birth (M21.4.11-R2.2/IR-001)

R2.1 created the zero-record writer before calling `seal_partial`. The writer
made its Raw header, directory entry, and Catalog ACTIVE registration durable;
only the later ACTIVE -> SEALING transition carried `seal_intent`. A crash
between those operations left a clean orphan ACTIVE marker with no durable
intent. `recover_partials` correctly retained that orphan, but could not
restore the gap, recreating silent continuity.

R2.2 retains the marker but reverses its durable birth order without changing
Raw v1 or the Catalog schema:

1. preallocate the marker `chunk_id`, Raw path, and creation time;
2. in one SQLite transaction insert the chunk identity and both logical ACTIVE
   and ACTIVE -> SEALING transitions, publishing the row directly as SEALING;
   the SEALING evidence contains the exact reconnect `seal_intent`;
3. only after that transaction commits, create and fsync the zero-record Raw
   header and continue the ordinary idempotent seal.

There is consequently no committed state equivalent to `marker ACTIVE + no
intent`. A crash before the transaction publishes no fallback identity. A
crash immediately after it, during Raw creation, after SEALING, after the
artifact, or after the manifest always leaves the same `gap_id` recoverable
from the SEALING transition. Missing or truncated marker Raw remains an
explicit incomplete/quarantined artifact condition; it cannot erase the gap.

Marker seal flags are independently fail-closed: `reconnect_gap` is added even
when the caller's reason-specific forced flags are empty (notably USD-M
`ingress_backpressure`). A zero-record manifest takes
`collector_instance_id` and `collector_version` from its authentic Raw header;
it keeps empty connection/time/sequence statistics and fabricates no frame.

Reconnect STARTED, COMPLETED, and recovery materialization use an exact
operational-event write/readback contract. `INSERT OR IGNORE` returning false
is legal only when the existing event has the identical event type, timestamp,
and canonical evidence. A missing or conflicting event fails closed; recovery
never reports `pending_discontinuity_materialized` without that proof.

### Already-SEALING chunk with a later intent (M21.4.11-R2.1/AUDIT-002)

`seal_partial()` called on a chunk already in `ChunkState.SEALING` with a new
`seal_intent` does not persist the later intent when the prior SEALING
evidence carries none. This state is unreachable by contract: every certified
caller that passes a seal intent is `StreamSpool._seal_current`, which only
seals a freshly created writer whose chunk was just registered ACTIVE;
`drain_one` rotation seals never pass an intent; startup recovery re-seals
without an intent; after a failed seal the spool releases the writer and the
same chunk is never re-sealed in-process. The existing guard still fails
closed (`SealError`) if a prior intent ever differs.

### Side-data transport tasks fail closed (M21.4.11-R4)

Side-data WebSocket collectors (`mark_price`, `liquidation`) handle their own
network reconnects through the same state machine. Any exception that
escapes the collector is a terminal integrity/storage failure: the old
writer cannot be proven safely reconciled, so `SideDataSupervisor` marks the
task `FAILED` and never opens a replacement connection without a durable
boundary. The side stream may stay FAILED (recovered only by a service
restart that runs startup recovery) while the core continues. REST pollers
are stateless per request and remain retryable.

### Historical audit (M21.4.11-R2/R3/R5, exact-pair matching R2)

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
  gap interval whose connection pair matches exactly proves the boundary.
  Unattributable evidence is UNKNOWN; adjacent-manifest flags are never
  borrowed for an unrelated transition.
- **Frame-less chunks (R2.2/IR-002).** A legal zero-record chunk never replaces
  the most recent connection-bearing chunk. One or more consecutive empty
  chunks are retained as `intervening_manifests`, and the next
  connection-bearing chunk yields exactly one logical old -> new transition.
  A `reconnect_gap` on an intervening zero-record boundary marker is exact for
  that collapsed boundary; other unattributable evidence remains UNKNOWN.
  `record_count=0` in the immutable manifest preserves this behavior when the
  marker Raw body was archived. A -> empty -> A creates no transition.
- **Exact Catalog boundary identity (R2/P1-B).** The transition identity is
  the pair `(old_connection_id, new_connection_id)`; the Catalog interval
  identity is `(original_connection_id, new_connection_id)`. The match is
  classified explicitly, never collapsed into one boolean:
  `EXACT_PAIR` (all four identities exist and `old == original AND new ==
  interval.new`), `PARTIAL_OLD`, `PARTIAL_NEW`, `TIME_ONLY`, `AMBIGUOUS`,
  or `NONE`. Only `EXACT_PAIR` may classify an inter-chunk boundary
  EXPLICIT_SEQUENCE_GAP from Catalog evidence alone; a one-sided identity
  (old matches, new matches, or either missing) is at best UNKNOWN, never
  optimistic EXPLICIT. Multiple exact candidates are AMBIGUOUS. Matching is
  market/stream specific: identical connection ids on a different stream
  never match. Every transition record exposes the decision provenance:
  `catalog_gap_match`, `catalog_identity_match`,
  `catalog_identity_match_kind`, and `catalog_matched_gap_id`.
- **UNKNOWN vs UNMARKED (R2/P2-C).** `UNMARKED_RECONNECT` means no relevant
  gap/overlap evidence exists anywhere near the boundary. If any adjacent
  manifest carries gap or overlap evidence that cannot be attributed to the
  exact inter-chunk boundary (for example a multi-connection old chunk
  sealed with `reconnect_gap` whose exact transition location is unknown,
  or an unattributable overlap flag), the boundary is UNKNOWN — evidence
  exists but cannot be placed, and it must never be labelled unmarked.
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

### Pending-gap extensions and orphan seal-intent prevention (M21.4.11-R3)

A reconnect boundary whose closing attempt delivered no reliable
first-new frame EXTENDS the already-open pending logical gap: one gap_id,
one STARTED, one generation transition, one COMPLETED. The extension itself
must not create a second recoverable logical identity:

- When the boundary is detected while `_pending_gap` is open and the
  pending gap's first-new marker frame was never enqueued
  (`_recovery_marker_enqueued` false), the seal intent built for the
  boundary reuses the pending gap's canonical identity fields (gap_id,
  reason, original_connection_id, original_generation,
  gap_started_at_utc_ns). Current-attempt information is preserved as
  separate observational metadata under the intent's `extension` key
  (attempt connection, attempt generation, attempt reason, detection
  time) and never masquerades as the canonical logical-gap identity.
- Startup recovery therefore sees the marker intent with an OPEN (or
  later CLOSED) parent lifecycle and never materializes a second STARTED.
- A boundary detected while the pending gap's first-new marker frame IS
  enqueued will have that frame persisted during the drain, which
  completes the parent gap; the boundary is then a genuine new logical
  discontinuity and keeps its own freshly minted gap identity (intent
  decision point 2, INV-009/INV-010).

### Legacy extension-orphan recovery (M21.4.11-R3, corrected R3.1, corrected R3.2)

The pre-R3 runtime persisted extension intents with a freshly minted
gap_id that never received a STARTED (production example: the
2026-08-13T08:20:35Z um_perpetual `book_ticker` session_restart extension
of gap `70ace625` persisted marker `7223d5ba` with orphan gap_id
`33e6420b`). Startup recovery must recognize these historical orphan
shapes without materializing phantom STARTED events, while REQ-103 must
remain authoritative for genuine intent-only crashes.

R3 originally classified a CLOSED-parent orphan by UTC containment plus
generation equality; **that proof was unsound and was withdrawn in
R3.1.** R3.1 replaced it with sound identity proofs plus an operator
authority, but kept UTC containment as an engagement gate for the
ambiguity review and consulted the authority before durable proofs.
**R3.2 withdraws both remaining UTC dependencies and the authority
precedence**: UTC wall-clock timestamps are observational evidence,
never causal-ordering authority (ADR-0004), and the wall clock may step
backwards in either direction (a genuine post-completion boundary can
carry a timestamp inside the closed interval; a true orphan can carry a
timestamp outside it). UTC is retained only for logging, diagnostics,
and human correlation.

**R3.2 exhaustive three-way partition** (`spool/legacy_reconnect.py`,
the single decision engine shared by startup recovery and the read-only
preflight command):

- **PROVEN_LEGITIMATE → materialize REQ-103.** Durable identity proves
  the intent is a genuine intent-only crash:
  - `verified_frames > 0` from trustworthy SEALING evidence (an
    extension attempt always seals a zero-record marker);
  - `original_connection_id == CLOSED.new_connection_id` at
    `original_generation == CLOSED.new_generation` (the boundary was
    detected on the exact connection whose first frame completed the
    parent);
  - ~~no possible parent~~ **WITHDRAWN IN R3.3 (REV-001):** the R3.2
    "no possible parent" quantification was unsound because the searched
    historical universe is not provably complete (malformed lifecycle
    authority can disappear from it). R3.3 replaces it with the
    versioned intent contract for new intents and the conservative
    legacy policy for unversioned intents (see the R3.3 section below).
- **PROVEN_EXTENSION → ignore lifecycle creation.** An ABSENT intent
  next to exactly one OPEN parent with the durable extension shape:
  trustworthy `verified_frames == 0`, generation equal to the parent's
  replacement generation (`parent.original_generation + 1`), and no
  wall-time predicate. A frame-bearing intent beside an OPEN parent
  stays the REQ-104 hard conflict.
- **AMBIGUOUS → fail closed unless resolved.** Everything else in the
  legacy ambiguity universe (an ABSENT fresh-gap intent that durable
  facts cannot prove legitimate) is AMBIGUOUS. Recovery never defaults
  an ambiguous candidate to materialization or to ignore: startup fails
  closed before ANY legacy lifecycle mutation
  (`RECOVERY_LEGACY_PREDECISION_INELIGIBLE` with the full blocker
  list), and only an exact operator classification resolves it.
- Competing genuinely distinct OPEN authority remains REQ-104 fail
  closed; positive proofs never bypass competing-gap invariants.

**Authority ordering and contradiction rule.** The operator authority
is consulted ONLY for candidates already classified AMBIGUOUS, and only
after the exact same-gap lifecycle handling, durable proofs, and
competing-gap invariants. If durable facts prove LEGITIMATE but the
authority says `extension_orphan`, or durable facts prove EXTENSION but
the authority says `legitimate_req103`, recovery fails closed
(`RECOVERY_LEGACY_AUTHORITY_CONTRADICTION`). An entry whose candidate
is now lifecycle-resolved (OPEN validated or CLOSED) was applied by an
earlier pass and stays idempotent (REQ-107).

**Strongly bound authority (schema
`legacy-reconnect-classification.v2`).** The additive operator file
`legacy_reconnect_classifications.json` in the data root binds every
entry to the exact immutable persisted intent:

```json
{"gap_id": "...", "market": "...", "stream": "...",
 "chunk_id": "...", "seal_intent_sha256": "...",
 "classification": "extension_orphan" | "legitimate_req103",
 "note": "..."}
```

`seal_intent_sha256 = sha256(canonical_json({"chunk_id": chunk_id,
"seal_intent": intent}))` with sorted keys, compact separators, ASCII
escaping — the exact immutable SEALING transition evidence value plus
the chunk identity. No mutable/live fields are part of the digest. A
stale digest, wrong chunk, copied authority, duplicate binding, unknown
schema/classification, or malformed digest fails closed; an entry with
no corresponding candidate makes startup ineligible. The recorder never
writes or edits the file; schema v1 is rejected (it was never deployed,
so no migration exists).

**SUPERSEDED IN R3.3 (REV-002/REV-003):** the v2 digest did not bind
`verified_frames` although it drives classification, and the documented
`root:root 0600` mode was unreadable by the production service. R3.3
replaces this with authority schema
`legacy-reconnect-classification.v3` whose
`classification_evidence_sha256` binds `{chunk_id, seal_intent,
verified_frames}` and the owner=root group=orangepi mode=0640
installation contract (see the R3.3 section below).

**Read-only deterministic preflight and two-phase startup.** The CLI
`binance-market-recorder recovery legacy-reconnect-preflight` runs the
SAME decision engine against a read-only Catalog and emits a
deterministic machine-readable inventory (schema
`legacy-reconnect-preflight.v1`): every candidate with gap identity,
chunk_id, intent digest, verified_frames, lifecycle state, automatic
decision, authority state, final decision, and the global counts
(proven_legitimate / proven_extension / ambiguous / classified /
unclassified / stale / unmatched / contradiction / conflict) plus
`first_corrected_startup_eligible`. Startup performs Phase A (the same
read-only pre-decision) and only then Phase B (apply the proven-safe
materializations and ignores, then the existing partial/seal/reconcile
work): no legacy lifecycle mutation ever precedes the global
pre-decision, so a later ambiguity can never follow a partial
materialization. CLOSED intervals are paired strictly by exact
`gap_id`, expose full durable identity, and are loaded once per pass;
an inverted wall pair is flagged `NON_MONOTONIC` but never removed from
the classification universe, and identity-degraded pairs are surfaced
as explicit degraded-authority blockers by R3.3 (never silently
counted away as possible-parent evidence).

The legitimate REQ-103 intent-only crash case is unchanged: a PROVEN
legitimate intent materializes exactly one STARTED with the same
durable gap_id, idempotently across repeated recovery.

The corrected artifact has not passed production validation; production
deployment, recovery, and the 168h gate remain separately authorized.
The first corrected startup additionally requires the complete
pre-start legacy classification sequence documented in
`docs/operations.md` and `docs/ubuntu_rk3588_operations.md`.

### Legacy recovery authority hardening (M21.4.11-R3.3, PR #11 R3.2 rejection correction)

The independent exact-head review of R3.2 rejected it
(CODE_REVIEW_VERDICT=REQUEST_CHANGES, P1=3, P2=1):

- **REV-001 (P1):** historical parent authority can disappear from the
  searched universe — malformed/unkeyable lifecycle rows were silently
  skipped by Catalog helpers, so "no possible parent" could become a
  false positive legitimacy proof.
- **REV-002 (P1):** the authority digest bound only `chunk_id` +
  `seal_intent`, while `verified_frames` (which drives the frame-bearing
  legitimacy proof) lived outside the digest.
- **REV-003 (P1):** the documented `root:root 0600` authority mode is
  unreadable by the production service (`User=orangepi Group=orangepi`).
- **REV-004 (P2):** the supposedly read-only preflight called
  `ensure_storage_layout()`, which can mkdir/fsync missing directories.

R3.3 corrects all four without regressing the R3.2 guarantees (no UTC
ordering, exhaustive inventory, global pre-decision before mutation,
shared engine, authority-only-resolves-AMBIGUOUS, contradictions fail
closed, inverted wall-time lifecycle retention, exact lifecycle
idempotence, OPEN-parent hard-conflict semantics).

**Versioned reconnect-intent authority.** The legacy no-parent absence
proof is REMOVED: absence of a possible parent is never positive
legitimacy evidence because the historical universe is not provably
complete. Instead, every reconnect intent emitted by the R3.3+ runtime
(Spot and USD-M, fresh and pure-extension branches) carries the durable
`intent_schema: "reconnect-seal-intent.v2"` provenance persisted inside
the immutable SEALING transition evidence. Under the versioned runtime
prevention contract a pure extension can never mint an independent
orphan gap identity (it reuses the pending gap's canonical identity) and
decision-point-2 uses a fresh genuine logical gap, so a versioned fresh
ABSENT intent safely represents the legitimate REQ-103 intent-only
crash shape: startup materializes it automatically, subject to the
normal exact lifecycle/open-gap invariants. The version field is
validated strictly: an unknown future schema or a malformed value fails
closed; an intent without the field is a pre-R3 unversioned legacy
intent. This is a forward persistent evidence-contract revision, NOT a
SQLite schema migration (SCHEMA_MIGRATION_REQUIRED=false,
CATALOG_MUTATION_REQUIRED=false).

**Legacy unversioned policy.** An unversioned ABSENT candidate may be
automatically PROVEN_LEGITIMATE only by independently-sound positive
durable proof: trustworthy `verified_frames > 0`, or the exact
completing-connection proof. "No parent found", generation mismatch
against searched history, and well-ordered timestamps are NEVER positive
proof. Everything else is AMBIGUOUS and fails closed unless an exact
authority classification resolves it. This may increase one-time
operator classification work; integrity correctness is more important
than minimizing classification work.

**Malformed historical lifecycle authority.** Unkeyable
STARTED/COMPLETED rows (evidence not JSON/object, or missing/empty
market, stream, or gap_id) and identity-degraded CLOSED pairs are no
longer silently skipped or counted away: they are surfaced as explicit
`degraded_authority` predecision blockers (event_id/event_type/reason,
or market/stream/gap_id/reason, machine-readable in the preflight
output) and make `first_corrected_startup_eligible=false`. Malformed
evidence widens uncertainty; it is never paired with arbitrary other
events — exact valid lifecycle pairing remains by gap_id.

**Complete classification-evidence digest (authority schema
`legacy-reconnect-classification.v3`).** The authority entry field is
renamed to `classification_evidence_sha256 =
sha256(canonical_json({"chunk_id", "seal_intent",
"verified_frames"}))` — sorted keys, compact separators, ASCII
escaping. It binds the exact immutable SEALING evidence values the
operator reviewed and the decision engine consumes: the chunk identity,
the full seal-intent document (including `intent_schema`), and the exact
`verified_frames` value (`null` when absent). Changing ANY immutable
candidate-side fact that can change classification invalidates the
binding; reverse-direction staleness is tested both ways. No
mutable/live field, path, time, or operator note is part of the digest.
Historical lifecycle CONTEXT is deliberately not part of the candidate
digest: startup always recomputes the automatic decision first, so a
later context change that yields a PROVEN decision or HARD_CONFLICT
fails closed as a contradiction instead of being overridden by an older
authority (tested). The never-deployed v1/v2 schemas are rejected.

**Service-readable authority installation contract.** The Ubuntu
production authority file is installed as owner=root group=orangepi
mode=0640: root/operator controls writes, the Recorder service group can
read, everyone else cannot. The recorder never writes the file; the
deployment procedure writes the temporary file with the FINAL
owner/group/mode, fsyncs it, atomically renames it on the same
filesystem, and fsyncs the parent directory, avoiding any post-rename
window with unsafe permissions. A deterministic permission-contract
test asserts the documented constants against the rendered systemd unit
identity (`User=orangepi Group=orangepi`): readable=true,
writable=false for the service account, unreadable for others. macOS
interactive installs keep owner=user/mode 0600 (the CLI and the service
share the interactive account).

**Intrinsically read-only preflight.** The preflight derives the layout
through `StorageLayout.from_root()` (pure path derivation, no mkdir,
touch, chmod, or creation fsync) and explicitly validates only that the
data root and Catalog already exist. A missing layout directory is
never recreated; a missing required input is an error, never a repair.
Exit status is now a gate: eligible → 0; ineligible → 2 with the full
machine-readable JSON report (the `first_corrected_startup_eligible`
Boolean remains); malformed/runtime errors → 2 with an error payload.
Automation cannot accidentally ignore ineligibility.

**Final post-stop preflight is mandatory.** The corrected deployment
sequence requires, after stopping the old service and before starting
the corrected artifact: a final read-only preflight against the frozen
Catalog plus the installed authority, requiring eligible. Corrected
startup then reruns the entire global predecision pass itself before any
collector starts, so the accepted TOCTOU analysis (old service creating
new candidates after an early exploratory preflight) stays safe. The
authority may be created offline and installed before the stop; only the
post-stop final coverage validation is mandatory.

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
- Adding another best-effort Catalog event after `_record_gap_started`
  fails (R2 rejected alternative): a fallback that depends on the same
  failed operation without durable state cannot close the double fault.
  The seal intent is persisted as part of the SEALING transition evidence
  itself, before any artifact/manifest mutation (REQ-102).
- **R2.2 Option A, remove markers and add a boundary-intent authority, was
  rejected.** It adds another persistent lifecycle/event type (or sidecar)
  solely for the no-writer case, and a second operational-event write shares
  the original STARTED failure path unless it also introduces separate
  durability machinery.
- **R2.2 Option B, retain the marker with intent-first durability, is
  selected.** The atomic Catalog ACTIVE+SEALING transaction reuses the
  existing chunk lifecycle and recovery authority, requires no schema or Raw
  format migration, and preserves an explicit immutable boundary artifact.
- Retaining the R2.1 marker-first ordering is forbidden because it exposes the
  reproduced ACTIVE-only window. A new mutable sidecar or historical manifest
  repair was rejected as a larger persistent-state surface and would not be
  compatible with immutable historical evidence.

## Consequences

- Every reconnect boundary now costs one generation seal and one
  STARTED/COMPLETED pair; the 1000-cycle stress test seals 1000 generations
  (~11-52s observed). Rare in production (one rotation per stream per day).
- Manifests gain the additive `reconnect_gap` flag value; `raw-chunk-manifest.v1`
  is unchanged because the flag set is open-ended and only existing
  `gap`/`complete` semantics are reused.
- Corrected read-only historical audit re-run (M21.4.11-R2, 2026-08-10,
  fixed cutoff `1786349202047196027`, inventory 161,817 manifests, SHA-256
  `ffaf34bdc29c016b0251f64252bc2c35edd43faba014c030b0834b9cc585dad3`,
  canonical payload SHA-256
  `1122431c56ebd8367bbbed1a8fc0e30f1f020d7edfd9c34602c9988d89d4b35f`):
  **4,691 connection transitions, 11 explicit, 4,680 unmarked, 0 unknown,
  0 blue/green overlap**. The totals are an observed reproduction of the
  R1 corrected run (not a hardcoded expectation, REQ-607): with the
  exact-pair classifier, every one of the 11 explicit transitions is
  Catalog-proven with `catalog_identity_match_kind == EXACT_PAIR` and an
  explicit `catalog_matched_gap_id` (the known USD-M `book_ticker`
  backpressure cycles). The production inventory contains no inter-chunk
  boundary with unattributable adjacent-manifest evidence, so the P2-C
  taxonomy change does not alter the counts at this cutoff. The canonical
  payload SHA changed from `7143bc0c...` because every transition record
  now carries the identity-match provenance fields.
- The R2.2 fixed-cutoff rerun on 2026-08-11 observed the same 161,817-manifest
  inventory SHA-256
  `ffaf34bdc29c016b0251f64252bc2c35edd43faba014c030b0834b9cc585dad3`
  and the same 4,691 / 11 / 4,680 / 0 / 0 classifications. Canonical SHA-256
  remained
  `1122431c56ebd8367bbbed1a8fc0e30f1f020d7edfd9c34602c9988d89d4b35f`.
  This is observed output, not a fixed expectation: the corpus contains no
  R2.1 zero-record markers because that artifact was never deployed.
- Consumers must treat the 4,680 unmarked intervals as unreliable until an
  additive correction ships.
- M21.4.11 is a correction to the PR #10 implementation; the production
  artifact has not been deployed, and no production data was modified by
  the audit (before/after inventory diff at the R2 run shows zero
  pre-existing production files modified; the only changes are the running
  recorder's own sealed/active/state writes).

## Rollback

Reverting to the old behavior would re-introduce `gap=false/complete=true`
across reconnect boundaries and invalidate the data-integrity contract;
do not revert without a replacement evidence mechanism.
