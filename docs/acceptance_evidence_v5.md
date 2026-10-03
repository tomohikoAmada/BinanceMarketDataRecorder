# M22.9 acceptance evidence V5 implementation contract

Policy authority is [accepted ADR-0034](adr/0034-m22-9-v5-bounded-online-terminal-audit.md).
Original bounded batching follows [ADR-0035](adr/0035-v5-bounded-delta-batches.md).
New-start v2 sharing follows [ADR-0037](adr/0037-v5-shared-archive-companions.md)
and is independently reviewed and deployed in the frozen cf3909e9 release.
Current artifact and actual qualification status are recorded in
[current state](CURRENT_PRODUCTION_STATE.md); historical PR #78 is its predecessor.
V1/V2/V3/V4 readers and their historical decisions retain their existing routing.
`CURRENT_SCHEMA_VERSION` and `V5_SCHEMA_VERSION` are
`m22.9-acceptance-evidence.v5`; the historical `SCHEMA_VERSION` alias remains V3.

## Frozen limits

| Authority | Value |
|---|---:|
| Online sample interval | 300000000000 ns |
| Maximum online evidence gap | 600000000000 ns |
| ADR-0033 global readiness recovery | 900000000000 ns |
| SQL page cap per family | 256 |
| Delta work budget from observation start | 240000000000 BOOTTIME ns |
| SQL pages per family per observation | Original start: 1; declared bounded-batch start: at most 4 |
| Pending causal references | Original: 256 per row; v1: 1024 per row; v2: 1024 at observation end |
| Canonical delta budget | Original: 4 MiB entries; v1: 7 MiB entries; v2: 7 MiB entries plus companion table |
| Manifest freeze shard | 512 records / 1048576 canonical bytes |
| Terminal audit shard | 512 records / 1048576 canonical bytes |
| Audit forward-progress publication | 60 seconds |
| Audit no-forward-progress watchdog | 900 seconds, distinct from readiness |

The implementation additionally caps an online document at 8 MiB. Publication
limits leave work pending rather than increasing a cursor or relaxing cadence.
Configured products determine the bounded reconnect context and open chunk map;
the open-row safety cap is 32 rows per configured ProductKey. Excess topology
fails closed. Terminal replay reconstructs open IDs/states from the frozen ledger.

## Durable delta and online evidence

The stopped first baseline explicitly and atomically adds
`operational_event_sequence(event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
event_id TEXT NOT NULL UNIQUE REFERENCES operational_events(event_id))`, its
same-transaction `AFTER INSERT` trigger, and an open-chunk state index. Historical
backfill order is `(occurred_at_utc_ns,event_id)`; new order is insertion visibility.
Partial schema, missing one-to-one authority, or failed migration is rejected;
rollback leaves no partial objects. Reopen is idempotent. Ordinary observations
never migrate or run the full one-to-one/integrity scans.

Cursor families are `chunk_transitions.transition_id`,
`archive_transaction_events.event_id`, and the new operational `event_seq`.
One read transaction captures their high-waters, ordered SQL pages of at most 256
rows each, bounded exact companions, and indexed open/backlog projections. A new
stage freezes the exact `bounded-batches.v2` policy in `delta_policy` at T0 and
repeats it in every sample. At most four pages/family are captured in the same
snapshot; the existing evidence lists contain at most 1024 ordered entries/family.
Missing `delta_policy` retains the original one-page policy on read/resume;
exact v1 starts retain v1. V2 shares exact archive bundles within each document;
replay expands and verifies them before ordinary causal checks. Its persisted
reference cap applies after every finite observation, including start and target.
Unknown or changed policy fails. It
ends before Raw/filesystem qualification. Indexed unfinished archive-state
aggregates may depend on unfinished backlog B; they exclude retired historical
transactions and share the cancellable work budget. There is no full manifest
inventory, full chunk membership query, retired archive count, or operational
history query in the ordinary path.

The acceptance CLI injects `catalog_available` into the real
`VpsReadinessEvaluator` constructor for V5 new stages, online resume and
standalone readiness. The observer also retains its direct-API compatibility
guard. V4 and non-V5 deployment readiness retain the legacy Catalog integrity
check. Full Catalog integrity remains mandatory in the frozen baseline/terminal
audit, outside the online cadence budget.

`stage-start.json` freezes identity, run/stage, T0 UTC/BOOTTIME/boot ID,
process incarnation, service instance, configured products, frozen policy,
predecessor reference, and the first normal online delta. Each
`sample-NNNNNNNN.json` binds start/previous SHA, ordinal and observation clocks,
live identity/readiness/capacity, three high-waters, bounded delta pages,
`snapshot_status`, open rows, archive backlog, continuation and its SHA,
explicit `delta_pending`, reconstructed blockers, and the V4 readiness episode.

Each page entry has a row, same-snapshot companions, optional newly sealed
manifest/Raw proof, and acknowledgement/pending status. Companion lookup does
not advance another cursor. References bind family/numeric and primary IDs,
canonical row SHA, transaction/chunk/state and originating row. Later consumption
must match and retires the reference. Five fixed archive event keys and their
five paired chunk transition keys are the maximum archive fan-out. A
discontinuity lookup returns at most one STARTED and one COMPLETED row.

Continuation contains processed/observed cursors, pending references, rolling
row digests, configured stream reconnect projections, open discontinuities,
latest bounded discontinuity context, archive aggregates, open rows and sticky
integrity findings. Resume independently streams the existing online chain and
restores this state, T0 and the readiness episode without a history inventory.
Boot/process/service changes refuse resume. Missing/extra samples and hash,
ordinal, policy, predecessor, cursor or causal mismatch are rejected.

SQLite capture and manifest/Raw qualification have a process cancellation
boundary. A reusable stateless worker amortizes per-unit spawn overhead inside
an observation/audit. Budget exhaustion or unresolved retirement authority
acknowledges no unfinished unit and advances no cursor over it. A missing local
Raw file cannot be combined with an archive commit after the frozen boundary;
only a later coherent snapshot may qualify its exact authorized retirement.
A local inode unlinked during its open-descriptor scan takes this same absence
path only after stored/decompressed hashes, every CRC and all manifest statistics
validate. Device/inode/size/mtime and pre-scan identities must remain exact, link
count must fall1→0, and the no-follow pathname must be absent. A changed ctime
alone is not accepted. Content/metadata mutation, replacement, hardlinks and
archive-copy deletion retain failure behavior. No live Catalog requery authorizes
retirement. Proof/evidence bytes and original/v1/v2 replay contracts are unchanged.
Unresolved target work is `INCOMPLETE` and cannot be filled by terminal audit.

## Baseline, target and frozen controls

The first V5 2h binds a passing baseline audit root. Later stages bind the exact
previous V5 stage final and terminal root, frozen cursor tuple and continuation
seed. A predecessor from another artifact/schema/product configuration or the
wrong stage is refused. A fresh live readiness observation at T0 is mandatory.

`baseline-preflight.json` anchors the first baseline: run ID, exact deployment
identity/config binding, configured products, boot ID, migration result,
Catalog authority and quiescence certificate. It grants zero Formal duration.

The normal terminal online sample uniquely anchors `stage-target.json`: stage,
run, T0, required duration, terminal sample ordinal/SHA/clocks, identity,
process/service, readiness, processed and target high-water tuple,
continuation digest, predecessor and online outcome. Its sample BOOTTIME minus
T0 is the entire candidate duration. No ordinary sample or online resume is
allowed after target; later work grants zero duration.

Only an operator may stop/disable Recorder and pause archive mutators. The
auditor checks inactive worker/timer authority, disabled Recorder, zero active
partials and unchanged service invocation/timestamps; it never supervises them.

`quiescence-corpus.json` binds the anchor (target or baseline preflight),
identity, boot/data-root authority, consistent read-only Catalog backup SHA,
schema/high-waters/table counts/registered target aggregate, manifest-freeze
summary and quiescence certificate. A second consistent Catalog backup and
sorted manifest membership pass must agree before publication.

Manifest mappings are sorted canonical NFC root-relative paths and bind chunk
identity, exact length/SHA and `manifest-objects/<sha>.manifest`. Two coherent
source reads and descriptor/path `dev/inode/size/mtime_ns/ctime_ns` guards reject
mutation/replacement/loss. Private objects are exclusive, no-overwrite,
fsynced and independently readback-verified. Symlink traversal is rejected.
Filesystem enumeration/sorting and uniqueness use disk SQLite indexes, not an
N-sized RAM inventory. Audit reads private bytes and frozen Catalog, not live
manifest paths. Raw/archive payload stays in its registered location and is
streamed with stored/decompressed hashes, frame CRC/identity/statistics and
bounded reconnect projections.

Qualification proves the corpus frozen at quiescence and exact Raw/archive
bytes observed by qualification. It does not claim perpetual live-path
immutability or resist a malicious root rewriting all authorities coherently.
Sanctioned mutators must remain stopped through audit publication.

## Streaming audit, independent verification and resume

Record order is manifest path, Catalog chunk primary key, numeric chunk
transition, archive transaction primary key, numeric archive event, numeric
operational sequence and Raw location path, in that fixed family order.
Each record binds exact/canonical authority SHA, prior comparison SHA, findings
and baseline/historical/online/post-target timing classification. Temporary disk
joins reconstruct membership, historical immutability, legal lifecycle,
discontinuity and online omission/causality without retaining the full corpus.

All JSON uses sorted keys, compact separators, UTF-8 without ASCII escaping,
one trailing LF. Shards bind ordinal/anchor/previous-shard SHA and records;
single oversized records fail closed. The chains are exactly ADR-0034's
`BMDR-V5-MANIFEST-FREEZE\0` (Catalog SHA anchor) and
`BMDR-V5-AUDIT-SHARDS\0` (target/preflight anchor), raw SHA bytes and uint64 big
endian ordinal. Roots store the chain digest/count, not a growing hash list.

`audit-root.json` binds stage/run, identity/anchor/predecessor, corpus and
Catalog controls, family counts, shard chain/total, aggregate record SHA,
continuation seed, findings/result, completion and verified byte counts. A
separate exact reconstruction runs before a completed root is published.
That qualification reader streams private controls, Catalog, actual Raw/archive
and shards; it independently reconstructs results and does not trust producer
verdicts. Pre-publication self-check explicitly requires live Raw replay.
Historical predecessor controls, online chains and summaries are reconstructed;
their pinned Raw proofs must match the current full exact Raw verification,
allowing legitimate archive retirement of old local paths.

Completed-stage independent verification uses historical/control replay:
reconstruct the frozen corpus, Catalog ledger, online chain, immutable shards,
hash-bound Raw proof records, root aggregate and final eligibility. It does not
reread terminal-time physical Raw locations or resolve the current live archive
target map. Thus authorized archive retirement after qualification cannot
invalidate an immutable historical stage. CLI `acceptance verify` (including
`--baseline`) has this historical meaning; it is not a new integrity audit of
current production bytes. Evidence tampering still fails closed. Baseline and
finalize commands, including resumed qualification, retain live verification.

Independent terminal replay also reconstructs each online high-water's open
chunk and unfinished archive state, verifies immutable causal companion rows,
and rejects a high-water tuple that splits an atomic archive commit.

Published prefix shards are immutable. Explicit resume verifies the same
target, identity, corpus, Catalog/private objects, prefix bytes/order/hash and
deterministic records. Completed Raw proofs in that prefix can be reused for
production continuation; independent terminal reconstruction still verifies
exact Raw before root/eligibility. Resume starts at the exact next record/shard,
ignores untrusted temporaries and never overwrites controls or creates a T0.
Completed resume returns only an exactly verified existing final/root.

Progress records publish after shards and every 60 seconds of forward bytes.
The production corpus-freeze worker and terminal controller are killable on
900 seconds without forward byte/record progress. The terminal controller has
an isolated process group so cancellation includes its Raw workers. Immutable
interruption evidence records reason/phase/published prefix; no eligible final
exists after interruption. There is no total audit-duration cap.

`stage-final.json` binds start, target and terminal root SHAs, unchanged elapsed
online duration, blockers/result/eligibility. Online gap or unresolved target
delta is `INCOMPLETE`; sticky readiness/safety or terminal integrity failure is
`FAIL`; only duration + valid identity/predecessor + terminal READY + online PASS
+ independently complete terminal PASS yields an eligible `PASS_CANDIDATE`.
No audit duration or historical V4 credit is transferred.

The implemented command surface and validation evidence are recorded in
[M22.9 V5 implementation](milestone_acceptance/M22.9-v5-implementation.md).
