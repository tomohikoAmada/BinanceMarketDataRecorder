# ADR-0034: M22.9 V5 bounded online observation and terminal full audit

October 2 implementation refinement: [ADR-0035](0035-v5-bounded-delta-batches.md)
defines an explicitly declared bounded-batch policy for new starts. Original
starts retain the one-page semantics below. All other authority is unchanged;
the new implementation candidate is not deployed or independently reviewed yet.

- **Status:** Accepted policy authority; implementation pending
- **Date:** 2026-09-29
- **Scope:** M22.9 acceptance evidence and qualification policy; documentation only
- **Base:** `354f5199eda687dfe2223b0b481c8de669dfdabf` / tree `101703dbd6a6da6f8633f2a46e1323ab5b87188c`

The independent review accepted PR #77 at reviewed head
`0a03bd2f54af2d516ea6f3ef7861f524c72f17da`: `P0=0`, `P1=0`,
`P2=1`, `P3=0`; `PR_77_ARCHITECTURE_REVIEW=ACCEPT` and
`ADR_0034_ARCHITECTURE=ACCEPT`. The reviewed P1-A
`QUIESCENCE_CORPUS_FREEZE` and P1-B `CROSS_CURSOR_CAUSALITY` corrections are
accepted. The nonblocking architecture-monitoring P2 is
`TERMINAL_FULL_RAW_AUDIT_SCALABILITY`. Future implementation qualification
must measure baseline and terminal audit throughput, Raw and archive bytes/sec,
peak RSS, and total terminal audit duration at the current production corpus
size; it cannot weaken terminal full-integrity semantics.

## Context and decision

The exact V4 artifact's 2h final
(`59bf31036069029ca48c94c2f0f225820e1843e11c783604c9a405348d5a760f`) remains historical
`ACCEPT`, with `7676.600247655` accepted seconds. Its 12h final
(`49f1e6e610edbc7e4ebfa5a52d92ffb26867dcbef1e6e18a9c6af6025d7dd5de`) remains
`INCOMPLETE`, zero accepted seconds: sample 96 to 97 was `627.60286927` BOOTTIME seconds
and the installed verifier rejects the chain as excessive. Neither record is changed or
reinterpreted.

The reviewed fault classification is `RECORDER_DEFECT=NO`,
`ACCEPTANCE_OBSERVER_DEFECT=YES`, `ARCHIVE_DEFECT=NO`,
`HOST_RESOURCE_DEFECT=NO_EVIDENCE`, `OPERATOR_DEFECT=NO`, `CADENCE_SCHEDULER_DEFECT=NO`,
`SAMPLE_WORK_BUDGET_DEFECT=YES`, `CUMULATIVE_MANIFEST_SCALING=YES_IN_SOURCE`,
`ARCHIVE_CONTENTION_MATERIAL=NO_EVIDENCE`, `MEMORY_PRESSURE_MATERIAL=NO_EVIDENCE`,
`CODE_CHANGE_REQUIRED=YES`. Every V4 ordinary observation calls
`strict_manifest_inventory(data_root, deep_scan=False)`: Raw deep scans are
substantially incremental, but all historical manifests are enumerated, read, parsed,
hashed, and compared again. The exact 96th internal phase duration was not logged, so
the full-history path is a proven structural defect, not a measured exclusive
explanation for every excess second.

V5 **changes detection timing**. An old manifest's byte mutation or deletion need not be
rediscovered in the next 300-second ordinary sample. Any mutation or loss **already
present in the control corpus frozen for terminal qualification** must fail eligibility;
the independently verified frozen corpus is required before
`eligible_for_next_stage=true`. The qualification point is the
`QUIESCENCE_CORPUS_FREEZE` described below. V5 does not claim continuing proof of
arbitrary live production bytes after their audit read. A new schema,
`m22.9-acceptance-evidence.v5`, makes the detection-timing change explicit. V1–V4
evidence, dispatch, and verifier decisions remain immutable.

The frozen online authorities remain `SAMPLE_INTERVAL_NS=300000000000`,
`MAX_EVIDENCE_GAP_NS=600000000000`, and `READINESS_RECOVERY_DEADLINE_NS=900000000000`.
ADR-0033's one global, acceptance-observed `CLOCK_BOOTTIME` readiness episode, exact
recoverable ProductKey core `NOT_READY` classification, inclusive `<=900s` recovery,
sticky failure after `>900s`, sticky `FAILED`, and terminal `NOT_READY` failure carry
into V5. Only the timing of full historical integrity detection changes. The 600-second
bound applies to the timed online chain through its terminal target observation; a
post-target audit earns no Formal duration and is outside that cadence bound.

V5 has two mandatory phases: bounded **online continuity observation**, ending in
immutable `stage-target.json`, followed after operator-controlled quiescence by
**terminal full-integrity qualification**. A stage is eligible only after both
independently verify. The observer remains an observer and never stops Recorder.

## Existing Catalog authority and required addition

The current Catalog already has durable SQLite `INTEGER PRIMARY KEY AUTOINCREMENT`
cursors `chunk_transitions.transition_id` and `archive_transaction_events.event_id`. A
`chunk_transitions` row to `SEALED` is committed after the manifest's atomic publication
and carries the chunk identity; archive transitions update chunk and archive state in
one transaction. Queries `WHERE id > prior_id AND id <= frozen_high_water ORDER BY id`
can be additive read APIs with a strict page limit. IDs are ordered by commit
visibility, not by `occurred_at_utc_ns`; gaps in IDs are allowed.
`archive_transactions.transaction_id` is text and is not itself a cursor.

`operational_events.event_id` is a text primary key; timestamp plus event ID and SQLite
`rowid` do **not** prove durable insertion order. V5 therefore requires one minimal
additive Catalog table:

```sql
CREATE TABLE operational_event_sequence (
    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE REFERENCES operational_events(event_id)
);
```

A same-transaction `AFTER INSERT` trigger records every newly inserted operational
event, including inserts outside the normal API. `INSERT OR IGNORE` replays add no
sequence entry. In the stopped first-V5 baseline migration, backfill exactly one ledger
row per existing event in deterministic `(occurred_at_utc_ns, event_id)` order, then
verify a one-to-one join and install the trigger before any V5 T0. Historical backfill
order is only a deterministic baseline order; newly committed `event_seq` values are the
online insertion cursor. Migration, trigger, FK/index checks, and rollback must be
atomic and fail closed. Updates or deletions of old rows have no legitimate online
delta; terminal verification detects them against the prior audit authority. No other
Catalog schema or writer-format change is authorized by this decision.

Each online observation uses **one bounded SQLite read transaction** to freeze all
three high-water marks, read one bounded page per cursor family, and perform causally
required indexed companion lookups in that **same snapshot**. Cursor consumption and
causal lookup are distinct: a lookup proves a selected row's dependency but does not
advance the companion family's processed cursor. The read transaction ends before
filesystem/Raw work, so a later archive commit cannot alter this sample's frozen rows.
Evidence binds previous/high-water/processed cursors, canonical row digests, and
cross-cursor causal references. Cursor regression, unavailable history, rows beyond
the frozen mark, missing ledger linkage, or impossible lifecycle transitions block
the stage. Resume uses the last independently verified published sample's cursor
tuple, pending causal references, open lifecycle map, and rolling aggregates; it
neither resets authority nor performs a full-history inventory. The first V5
stage-start binds the baseline audit root and its cursor tuple. Later stage-starts
bind the predecessor's terminal audit root and its cursor tuple, then consume exact
intervening deltas; they do not repeat a full pre-T0 scan.

A selected row A is acknowledged only if every required companion B is visible,
at or below B's frozen high-water, and semantically sufficient in that snapshot.
If B is absent, post-high-water, ambiguous, or malformed, A remains unacknowledged
and pending, or fails under the existing lifecycle semantics. The processed cursor
cannot skip A. A causal reference records B's family, numeric ID, primary identity,
canonical full-row SHA-256, transaction ID, chunk ID, lifecycle state, and A's ID.
When B is later reached by its own cursor, its exact primary identity, canonical
row digest, transaction/chunk identity, and state must match that reference; mismatch
blocks the stage. Its own cursor advances only after this comparison and ordinary
validation. The verifier reconstructs these references from the sample chain rather
than trusting a producer's processed-cursor summary. At most 256 unconsumed causal
references may remain in continuation; reaching the cap without reconciliation is
a fail-closed online blocker, not an invitation to grow an unbounded map.

Every causal lookup has a fixed legal fan-out: `chunks` by `chunk_id`,
`archive_transactions` by unique `transaction_id` or `chunk_id`, and archive/chunk
events by unique `idempotency_key` return **at most one row each**. The existing
`operational_events_discontinuity_identity` index permits an exact
`(event_type, market, symbol, stream, gap_id)` pair lookup with at most one STARTED
and one COMPLETED row; fetch at most three to detect an illegal extra row and fail
closed. Companion queries never scan all transaction events, chunk history, or
operational history. A needed lookup without an appropriate index is an
implementation blocker until a minimal additive index exists. No per-row fan-out
above these bounds is accepted.

Archive lifecycle corroboration uses the immutable transition/event rows, not an
assumption that a mutable `chunks` or `archive_transactions` row still has A's
historical state. Those current rows may already be at a later legal state in the
same snapshot. For one transaction ID, the five possible archive event keys are
`reserve:<transaction_id>`, `archive-verifying:<transaction_id>`,
`archive-verified:<transaction_id>`, `local-delete-pending:<transaction_id>`,
and `local-deleted:<transaction_id>`. The matching chunk transition key for
reserve is `archive-reserve:<transaction_id>`; for the other four it is
`chunk:<archive_event_key>`. These exact-key lookups return **at most five archive
events and five chunk transitions**, each an indexed at-most-one-row query.
Require the exact required predecessor/successor chain
through the observed current state; a missing, extra, conflicting, or illegal step
leaves the selected row unacknowledged or blocks. Any causally referenced row that
the other cursor has not consumed remains bound for exact later replay. This fixed
five-step maximum follows the current production ArchiveManager protocol and legal
archive state graph, not an unbounded per-chunk history query. An archive event
using an unrecognized key or a second event for one step is not silently
classified as a legal successor; it blocks qualification.

Normative archive example: before a sample, the chunk-transition processed cursor
is 900 and the archive-event cursor is 300. One Catalog transaction commits
`LOCAL_DELETE_PENDING`: it changes the exact `chunks` and `archive_transactions`
rows, inserts chunk transition 901 with idempotency key `chunk:<key>`, and inserts
archive event 600 with unique idempotency key `<key>`. The sample's one SQLite
snapshot freezes high-waters 901 and 600. Its chunk page contains 901; its
256-row archive page can contain only events 301–556. For 901, it reads the exact
chunk and transaction rows and looks up event 600 by `<key>` in the same snapshot.
If those rows prove the transaction and state coherently, it binds event 600's
identity and canonical digest and advances the chunk cursor to 901. The archive
cursor advances only through its actually consumed contiguous page (at most 556),
**not** to 600. When a later archive page consumes event 600, it must match the
stored causal reference byte-for-byte at the canonical row level before its cursor
advances; then the reference is retired. If event 600 is absent or ambiguous in the
first snapshot, chunk 901 stays pending and the chunk cursor stays 900. The same
rule applies to the atomic `LOCAL_DELETED` transition and in reverse when the
archive event is selected before its paired chunk transition.

Each SQL page contains at most **256 rows per cursor family**. The observer has a
**240-second BOOTTIME delta-work cutoff** measured from the sample start, leaving 60
seconds of the 300-second target for bounded live checks and publication. This cutoff is
a work budget, not a relaxation of the 600-second evidence-gap failure. The page cap is
deliberately above the observed peak of 126 new manifests in the failed V4 interval
while preventing an unbounded catch-up loop. The sample records both
`observed_high_water` and `processed_cursor`; any remaining work is explicit
`delta_pending`. The next sample continues from the processed cursor, not from the
high-water. A single Raw unit must be streamed with an enforceable timeout/cancellation
boundary; if it cannot finish within the budget it is not acknowledged, its cursor does
not advance, and a bounded blocker is published. At target, **any** pending delta
through the target high-water makes online qualification `INCOMPLETE`; terminal audit
cannot excuse missing timed continuity. A sample that nevertheless exceeds 600 seconds
still fails its chain. Implementation must test the cutoff and single-unit cancellation
without relying on scheduler luck.

For a newly visible `SEALED` transition, the observer reads the named manifest's exact
bytes once, parses/validates it, hashes it, validates corresponding newly sealed Raw as
required, checks the matching Catalog row/transition, and binds the identity to its
delta evidence. The Catalog `chunks` primary key rules out duplicate Catalog chunk IDs
online. Filesystem-only extra/duplicate manifests and arbitrary old byte changes are
found by terminal audit. `ACTIVE`/`SEALING` and pending archive/reconnect lifecycles are
kept in an open-state map limited to configured ProductKey stream contexts and their
active/sealing chunks; archive backlog is queried by indexed state aggregate rather than
retained as an all-history map. Exceeding the configured topology is a blocker. A
manifest published before its `SEALED` commit is not yet online authority; a row
committed after a sample's snapshot is deferred as a unit. Stable `LOCAL_DELETED` is
classified from its exact authorized transaction, not mistaken for loss. A newly
observed ambiguous local absence fails closed or is explicitly deferred only when its
Catalog transition is post-boundary; no silent skip is permitted.

## Ordinary sample cost and authority

The observer checks boot ID, process incarnation, service instance, deployment identity,
readiness, capacity/hard reserve, three Catalog deltas, new manifests/Raw, archive
transitions, reconnect/discontinuity events, and the bounded open-state map. Identity
file/installed-artifact checks have a fixed deployment footprint. Online evidence
contains bounded deltas and rolling digests, never a full historical membership array. A
sample that itself exceeds 600 seconds still produces `acceptance_observation_gap`; no
timer, deadline, or scheduler relaxation is allowed.

The current full-history operations are assigned as follows:

| Current operation | V5 authority |
|---|---|
| `strict_manifest_inventory` directory enumeration, every old manifest read/parse/hash, old Raw absence loop | First baseline and every terminal audit; online only new `SEALED` deltas |
| `Catalog.chunks_in_states_snapshot` and full Catalog/manifest row comparison | Baseline and terminal audit; online only changed chunk IDs and bounded open states |
| `PRAGMA integrity_check` in `_catalog_evidence`, `incremental_audit_data_root`, and acceptance's readiness evaluator | Baseline and terminal audit; online checks Catalog availability, query/snapshot consistency, and bounded lifecycle invariants |
| Full `operational_events` discontinuity history and malformed/degraded scans | Baseline and terminal audit; online `event_seq` deltas plus persisted open/episode state |
| Full archive transaction history | Baseline and terminal audit; online archive-event deltas and changed transaction rows |

The acceptance-specific online readiness path must preserve ADR-0033 state/reason and
episode decisions while avoiding its current hidden full `PRAGMA integrity_check`;
deployment readiness outside V5 is unchanged. V5's delayed full Catalog corruption
detection is part of the explicit new-schema timing change. A full `quick_check` or a
full-row count cannot be substituted into every online sample. New read APIs must page
by numeric cursor and not `fetchall()` an unbounded historical query.

## Baseline, target, and quiescent handoff

A **fresh V5 artifact** requires the stopped/quiescent, one-time full baseline before
its first 2h T0: exact identity, then baseline audit, then separately authorized
Recorder start, readiness, 2h. Baseline uses the same streaming shards and checks as
terminal audit, binds the new Catalog ledger/cursors, and grants no duration. A
first-baseline `baseline-preflight.json` freezes the exact identity, config, Catalog
ledger migration result, registered targets, and quiescent service state; its immutable
SHA-256 anchors baseline shards. A subsequent stage uses the previous V5 terminal root
plus all exact intervening deltas, including controlled stop/start activity. Any missing
or conflicting predecessor/delta is ineligible. An old V4 2h result is never a V5
predecessor.

At or after the required BOOTTIME duration, the detached observer makes one ordinary
terminal online observation, subject to the same 600-second chain bound, publishes its
sample, then publishes `stage-target.json` once. The target binds stage/run, T0
UTC/BOOTTIME/boot ID, required and actual target BOOTTIME, terminal sample UTC/BOOTTIME,
process incarnation, service instance, last sample ordinal/hash, online blockers,
terminal readiness, Catalog/reconnect projection, cursor tuple, continuation digest,
predecessor/baseline root, and deployment identity. The **terminal sample's** BOOTTIME
minus T0 is the only candidate duration; target publication and later work add no
credit. A target with online blockers remains immutable and ineligible. Once target
exists, observer resume cannot create another sample, target, T0, or run ID.

The operator then gracefully stops and disables Recorder; waits for archive drain to
settle; pauses archive scheduling for the bounded quiescent audit; verifies the archive
worker is inactive, zero active `.partial`, Catalog availability, and exact
source/target identity. This post-target pause is for a coherent terminal snapshot,
never for hiding online archive contention. The operator, not the observer, performs
these actions. Hold all sanctioned Recorder/archive mutators stopped until final
evidence is published, then restore archive scheduling under a separately reviewed
operational procedure. Co-resident services are outside project control.

### QUIESCENCE_CORPUS_FREEZE

The **semantic qualification point** is publication of immutable
`quiescence-corpus.json` after the private control corpus has been frozen. It is a
claim about that exact frozen corpus, not perpetual immutability of original live
paths. All sanctioned Recorder and archive mutators remain stopped through freeze
and terminal audit. The finalizer makes a SQLite-consistent **read-only online
backup** of Catalog in the private acceptance evidence root, fsyncs and readback-
hashes it, and binds its exact SHA-256, schema, three cursor high-waters, table
counts, deployment identity, and registered targets. Qualification never reads a
Catalog row from the mutable live database after this freeze.

The manifest control corpus receives an actual private snapshot of **exact bytes**.
The finalizer enumerates canonical root-relative manifest paths using bounded-memory
external sorting. For each path it opens without symlink traversal, records
`dev/inode/size/mtime_ns/ctime_ns` from an open descriptor, streams bytes to an
exclusive temporary private object while hashing and counting them, fsyncs the
object, then rereads the original through a verified descriptor and compares the
second exact-byte length/SHA-256 plus pre/post descriptor and path identity.
Replacement, disappearance, metadata change, or unequal passes fail closed. The
private object is published no-overwrite, fsynced, reopened, and readback-verified;
an existing content-addressed object may be reused only after exact readback
verification. A bounded, canonically ordered manifest-freeze shard records each
relative path, byte length, byte SHA-256, and private snapshot-object identity.
Use the same **512-record / 1-MiB** canonical JSON shard caps as terminal audit;
an oversized single record fails closed. Let `anchor` be the raw 32-byte Catalog
backup SHA-256, `F0=SHA256(UTF8("BMDR-V5-MANIFEST-FREEZE\0") || anchor)`, and
`Fi=SHA256(F(i-1) || uint64_be(i-1) || SHA256(exact_freeze_shard_bytes))`.
`quiescence-corpus.json` stores the final `Fk`, shard count, record count, and
Catalog anchor; the verifier streams and reproduces them. This requires neither
all manifest bytes nor the full membership map in RAM. A second independently
sorted directory-membership pass is merge-compared with the initial path stream;
any missing, extra, duplicate, or reordered path aborts freeze. If a coherent
per-file read cannot be proved, no corpus root is published.

Only after both controls are durable does `quiescence-corpus.json` bind the Catalog
backup SHA and metadata, manifest-freeze shard count/root digest and object count,
service/boot state, active-partial count, source and registered archive-target
identity, stage-target SHA (or first-baseline preflight SHA), and quiescence
certificate. Publication is no-overwrite with fsync/readback. The independent
verifier reads the private manifest snapshots
and frozen Catalog backup, recomputes every mapping and digest, and rejects a
missing/replaced private object or altered backup before allowing eligibility.
The terminal auditor likewise reads **these frozen controls**, never original live
manifest paths. A modification of an original manifest after its verified snapshot
read is not silently reinterpreted as a change to the frozen corpus. Unexpected
sanctioned mutation or loss of quiescence aborts qualification; this policy makes
no continuous live-file proof against an out-of-scope malicious root actor.

The frozen corpus includes every manifest and relevant Catalog authority present
at quiescence, including post-target handoff objects. The timed online projection
includes only events through the target cursor tuple. Later transitions are
separately tagged `POST_TARGET_HANDOFF`, audited for integrity, and earn zero
duration. A manifest published before target but committed `SEALED` afterward
belongs to handoff because Catalog commit order defines online authority. Archive
`LOCAL_DELETE_PENDING`/`LOCAL_DELETED` transitions on either side of target are
interpreted by their frozen event IDs and exact transaction state; no pre/post-
boundary row mixing is permitted.

## Streaming full audit and evidence binding

`deployment acceptance finalize` is a separate repository-owned auditor that reads
production data/Catalog without writing them; it writes only to its private acceptance
evidence root. It validates the frozen manifest mappings and exact private snapshot
bytes, manifest presence, byte SHA-256, schema/parse, unique path and chunk ID,
Catalog/manifest fields, every local Raw or authorized archived Raw/manifest readback
and checksum, source retirement transaction/registered target authority, unauthorized
Raw absence, reconnect/discontinuity historical authority, Catalog `PRAGMA
integrity_check` on the frozen backup, stage online chain/target, predecessor, and
deployment identity. Historical Raw and archive payload **are not copied** into the
acceptance evidence root; exact bytes may be streamed from registered storage while
sanctioned mutators remain stopped. Qualification proves the frozen control corpus
and the exact Raw/archive content observed by terminal audit, not perpetual post-audit
immutability of production paths. It compares historical authority against the prior
baseline/terminal root plus allowed logged transitions; omission, replacement,
deletion, malformed records, or inconsistent archive state is a blocker. Producer
blocker summaries alone are never accepted.

Records are streamed in fixed family order (`manifest`, `catalog_chunk`,
`chunk_transition`, `archive_transaction`, `archive_event`, `operational_event`,
`raw_location`) and then by normalized UTF-8 relative path, numeric ID, or exact
primary-key bytes as appropriate. SQLite reads use ordered cursors; manifest paths are
externally merge-sorted in bounded temporary runs so enumeration does not create an
N-sized RAM array. Each record binds its family/key, observed exact-byte digest or
canonical Catalog-row digest, comparison authority, and outcome. Paths must be
root-relative, symlink-free, unique, and canonical. The verifier rejects missing,
duplicate, extra, reordered, or noncanonical records. It merge-joins the prior audit's
streamed records with current records and the ordered allowed transition ledger; this
compares old authority without loading either full inventory into memory.

Freeze two shard caps: **512 records and 1 MiB canonical JSON bytes**, whichever is hit
first. V4's 12h corpus had about 143,000 manifests, so its manifest portion yields a few
hundred small shards rather than another 223 MB document; Catalog and Raw-location
records add further shards. The 1 MiB cap bounds malformed/large records independently
of count and is far below the VPS's observed multi-GB available memory. A single record
exceeding the byte cap fails closed. Shards are numbered `shard-00000000.json` onward,
immutable, UTF-8 canonical JSON (`sort_keys`, compact separators, no ASCII escaping, one
trailing LF), no overwrite, fsync/readback/sha256. The next shard carries the prior
shard hash. `audit-root.json` binds schema, stage/run, target hash (or first-baseline
identity/preflight hash), predecessor/baseline root, `quiescence-corpus.json` SHA,
Catalog backup SHA, manifest-freeze root, counts by family and total, shard count,
ordered-shard hash-chain digest, final aggregate digest, findings, completion state,
and exact identity. Let `anchor` be
the raw 32-byte target SHA-256 for a terminal audit or the raw 32-byte exact
identity/preflight SHA-256 for the first baseline. Define
`H0=SHA256(UTF8("BMDR-V5-AUDIT-SHARDS\0") || anchor)` and `Hi=SHA256(H(i-1) ||
uint64_be(i-1) || SHA256(exact_shard_file_bytes))`; the root stores `Hk`. The chain
binds **every ordered shard hash** without a growing list in the root. The independent
verifier streams shards in sequence and reproduces all counts, canonical digests,
per-record comparisons, chain state, and root bindings with O(one shard + active
lifecycle state) RAM. `stage-final.json` binds the audit-root SHA-256.

The auditor emits a durable progress checkpoint after each shard and while streaming a
large Raw file at least every 60 seconds of forward byte progress. A watchdog aborts
after 900 seconds with no forward byte/record progress; there is no fixed total-duration
cap. Watchdog outcome, phase, last completed shard, bytes processed, and exact reason
are immutable evidence, never a PASS. The 900-second **audit watchdog** is a separate
liveness setting, not the ADR-0033 readiness recovery deadline; implementation must name
the two distinctly.

### P2: terminal full Raw audit scalability

`P2=TERMINAL_FULL_RAW_AUDIT_SCALABILITY` remains open. Exact terminal verification
may intentionally be O(total qualified Raw and archive bytes) because it is outside
the timed online cadence; the P2 does not justify skipping bytes or weakening the
full-integrity gate. Before an implementation can be qualified for deployment,
measure the first baseline and later terminal audits at the **current production
corpus size**: baseline throughput, terminal throughput, Raw bytes verified per
second, archive bytes verified per second, peak RSS, and total audit duration.
Record the dataset size, storage target, and observed bottlenecks. Use those
measurements to size progress/watchdog operation and operator windows, without
transferring audit wall time into Formal duration credit.

On crash/interruption, published shards remain immutable. Resume rechecks target,
identity, `quiescence-corpus.json`, Catalog backup SHA, required private manifest
snapshot objects, and every published audit shard's bytes/hash/order and rolling
digest, then continues at the exact next deterministic record/shard. It never reuses
a shard against a changed Catalog backup/corpus, never
overwrites, and never creates a second target/T0. A partial last temporary file is
untrusted and ignored; only fully published shards count. If quiescence cannot be
re-established against the exact frozen corpus, resume is refused and the stage remains
ineligible. No `stage-final.json` is published until an audit has a terminal `PASS`,
`FAIL`, or explicit `INCOMPLETE` audit-root outcome. Absence of a final is itself
ineligible; an interrupted audit can be closed `INCOMPLETE` with exact reason, never
`PASS`.

## Independent outcome and threat model

V5 dispatch is explicit alongside immutable V1/V2/V3/V4 readers. The verifier streams
stage-start, ordinal-contiguous online samples, exactly one target,
`quiescence-corpus.json`, frozen Catalog backup, manifest-freeze shards and private
objects, terminal audit root/shards, and final. It reconstructs hashes, BOOTTIME
gaps, sticky blockers, ADR-0033 readiness episode, three cursor chains and pending
causal references, open lifecycles, target boundary, frozen-corpus/audit aggregates,
predecessor and identity. It must not trust a producer's `result`, `findings`,
`eligible_for_next_stage`, shard count, or digest without reconstruction. The frozen V4
627.603-second chain remains rejected by the V4 verifier.

`PASS_CANDIDATE` with `eligible_for_next_stage=true` requires `ONLINE_STAGE_PASS`
**and** `TERMINAL_FULL_AUDIT_PASS`, no blockers, valid predecessor/identity, terminal
READY, and full required BOOTTIME. Online cadence interruption is `INCOMPLETE`; terminal
online `NOT_READY` or sticky safety/readiness failure is `FAIL` under ADR-0033; exact
terminal corruption/loss is `FAIL`; interrupted/incomplete terminal audit is
`INCOMPLETE` or has no final, always `eligible=false`. No later audit time contributes
duration. Stage advancement is never automatic.

The threat model covers mutation, loss, or replacement **already present in the
quiescence-frozen control corpus**, duplicate identity, Catalog disagreement,
unauthorized Raw loss, broken archive authority, and evidence chain deletion/rewrite
before eligibility. Sanctioned project mutators are stopped during corpus freeze and
audit. The audit verifies exact Raw/archive bytes it observes from registered storage.
It does not claim perpetual post-audit immutability or resistance to a malicious root
operator who coherently rewrites production data, Catalog, artifacts, and all hash
authorities. No `chattr`, fs-verity, fanotify, inotify, or Linux audit daemon is a
required V5 correctness dependency. Loss of quiescence aborts qualification.

## Fresh chain and implementation boundary

V5 implementation will change source, wheel, and deployment identity. The old V4 2h
remains historical ACCEPT but is **not** a valid V5 12h predecessor; old V4 12h remains
historical INCOMPLETE. After separate implementation, review, deployment and live
authorization, the required chain is fresh identity → baseline audit → readiness → 2h →
12h → 24h → 72h → 168h. Architecture acceptance authorizes no code change,
VPS operation, Recorder start, Formal T0, deployment, or retry. V5 implementation
requires its own task and review.
