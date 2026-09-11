# BinanceMarketDataRecorder — Project Handoff

This handoff is self-contained for a new development team. It separates
current GitHub engineering authority, the older deployed/qualified artifact,
and the current multi-symbol qualification program. Documentation is not deployment or live
traffic authorization.

## Current stage — MS4-C reviewed complete; MS4-D not started (2026-09-11)

MS4-A local preparation and MS4-B target preflight/stopped-deployment review
remain complete. The owner-authorized MS4-C attempt started the reviewed
four-ProductKey artifact on the Tokyo VPS, reached authoritative `READY` for
all four ProductKeys, and completed the bounded two-hour steady interval. That
first window remains `EXECUTED_PARTIAL_NOT_ACCEPTED` because controlled
recovery was not executed in the approved window; it is not retroactively
reclassified by the later supplement. The MS4-B private bundle's
`PRIVATE_EVIDENCE_BUNDLE_DIRECT_INSPECTION=NOT_RUN_LOCAL` is a historical
MS4-B local-review limitation, not an MS4-C evidence limitation. The MS4-C VPS
evidence was directly inspected by the primary agent; both records are listed
in [`MS4-C evidence`](milestone_evidence/MS4-C-20260910.md).

The separate 2026-09-11 R3 evidence is a passing
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT`. It used fresh official eligibility,
an exact-MainPID socket disconnect, and bounded post-fault observations; all
four ProductKeys remained ready and receiving, 12 core contexts passed sealed
validation, Catalog integrity was clean, and no active partial remained. It
closes the MS4-C recovery gate for review, but grants
`formal_m22_9_credit_seconds=0`.

The service was gracefully stopped and remains stopped. A receiver-only SSH
archive cycle to the explicitly authorized MacBook Downloads internal-folder
test target was verified; it created no remote pending authority, did not
delete or retire the VPS source, and did not produce a formal receipt-bound
Catalog snapshot. `PRODUCTION_READY=NO`; the full MS4 milestone remains
incomplete and MS4-D has not started. The archive timer is explicitly
`disabled`.

MS4_A=REVIEWED_COMPLETE; MS4_B=REVIEWED_COMPLETE;
MS4_C=REVIEWED_COMPLETE; RECOVERY_GATE=REVIEWED_COMPLETE;
MS4_D=NOT_STARTED; NEXT=MS4_D_REVIEW_STAGE_CLOSURE.

## Original MS4-C attempt disposition — 2026-09-10

The attempt ran from `T0=2026-09-10T15:10:26.637211Z` to
`T1=2026-09-10T17:10:33.156829Z`, with recorded monotonic duration
`7206.519615476` seconds. The recovery observation ended at
`2026-09-10T17:25:33.300961Z`; controlled recovery was not executed in the
approved window. The explicit stop returned success at
`2026-09-10T17:26:04.819925Z`, with systemd `inactive/dead`, `MainPID=0`, and
`Result=success`. The receive-only archive source remained present with its
original stored SHA-256 after the stop; its manifest was sealed before T0 and
is not steady-period archive-throughput evidence.

## MS4-C R3 recovery supplement — 2026-09-11

The R3 supplement is recorded at
`/root/MS4-C-RECOVERY-20260911-R3/evidence/recovery-audit.json` on the Tokyo
VPS. Its status is `PASS`, classification
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT`, and Formal M22.9 credit is zero. Fresh
official eligibility was captured at 2026-09-11T01:01:49Z. MainPID 685913 was
`READY` before the exact owned socket was disconnected. The only durable gap
was `um_perpetual:BTCUSDT/book_ticker`, completed in `0.436024210` seconds
with a new connection. Fifteen post-fault observations kept all four expected
ProductKeys ready with receive progress; 12 core contexts passed sealed
validation (`validated_chunks=23`). Catalog integrity was `ok`, with no
malformed, degraded, unclosed, active, or `.partial` artifacts. Final systemd
state was `inactive/dead`, `MainPID=0`, `Result=success`. Historical continuity
is not claimed restored, duration credit is zero, and source retirement was not
authorized.

## Historical stage closeout — MS4-A local preparation

MS3 is merged. MS4-A local preparation and its document review are complete;
PR #58 carries the reviewed closeout. No MS4-B/C/D development, VPS access,
deployment or live qualification is authorized. The owner requested a rest
after merging current completed work and cleaning its branches.
MS4_A=REVIEWED_COMPLETE; NEXT=OWNER_RESUME_REQUIRED.
Historical next-step fields below describe their recorded checkpoint only.

## MS3 merge closeout — 2026-09-10

PR #56 is CLOSED/MERGED through the normal repository rule. Astra approved head
`2a701fe79b78d3c63dd5959efecd20a2369d58e5`, tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`; exact-head CI run `34436773366`
passed every macOS and Ubuntu step, including build and clean-wheel smoke. The
actual merge commit is `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`, tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`. MS3-R1, MS3-CI1 and MS3-CI2 are
closed. The historical CI1/CI2 failures remain recorded in the milestone plan
and MS3 acceptance; they do not describe the current state.

MS3 is not deployed. Formal M22.9 has not started and Production Ready remains
NO. This historical checkpoint recorded `NEXT=MS4_A_LOCAL_PREPARATION`; at that
historical checkpoint the follow-up field was
`OWNER_RESUME_REQUIRED_MS4_C`. It is not the current project next action.

## MS3-CI1 update — 2026-09-10 (historical handoff; now closed)

**MS3-CI1 was IMPLEMENTED / AWAITING REVIEW at handoff and is now
CLOSED/MERGED.**
The Ubuntu failure was a fixture executor-scheduling cycle: fake snapshot SDK
workers synchronously waited for depth writer work queued in the same executor.
Profile D now uses real-persistence async phase events before SDK submission,
thread-safe worker-to-loop signals and separate hang watchdogs. The local
default/six-worker repeat, focused/full offline suites and static/build/wheel
gates pass. No production code or capacity setting changed. The exact candidate
later passed required CI and was merged in PR #56. No deployment or online
qualification is implied.

## Current review update — 2026-09-10 (historical approval, now merged)

Astra approved the offline MS3 candidate at
`66e036a07f422818f5e3f54f0216d4083b443698` (tree
`1413ec705db3fd9f214499c57696bcab2da2e8b9`). MS3-R1 and the original
coverage findings are CLOSED. The approved exact candidate was subsequently
validated by run `34436773366` and merged in PR #56 as
`303e073e25d5ed53d7cf6e26a9c6c6e879013b50` (tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`). No deployment is authorized.
The [multi-symbol execution ledger](milestone_plan.md#multi-symbol-execution-ledger--2026-09-10)
is the current detailed continuation queue; the candidate submission summaries
below retain their evidence context. No MS4 deployment is authorized.

## Start here: the boundary

MS2 implementation/offline acceptance, independent review, and merge are
**CLOSED**. **MS3-A and MS3-B are merged**; MS3-B is merged through PR #56 at
`303e073e25d5ed53d7cf6e26a9c6c6e879013b50` with tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`. MS1 remains merged and closed.
See [MS2 acceptance](milestone_acceptance/MS2.md)
and [MS3 acceptance](milestone_acceptance/MS3.md). Do not restart or
reopen the historical M23 optimization, BBO, Storage Forecast, remote-delete,
single-symbol shared USD-M gate, prior burn-in campaigns, clean 24h campaign,
MS1 migration design, Raw v1, Contracts, or the closed MS1 review findings
unless new MS2+ evidence creates a concrete contradiction.

The following pre-MS2 authority checks were completed before implementation.
They are historical/time-local lineage, not current behavior or deployment
authority:

1. Fetch live GitHub `main`.
2. Verify that `d38180074b5f76ab6b7778eea7fc505160c671ae` remains an ancestor.
3. Inspect every commit after `d38180074b5f76ab6b7778eea7fc505160c671ae`.
4. At that historical checkpoint, if those commits were only post-MS1
   documentation synchronization, retain
   `d38180074b5f76ab6b7778eea7fc505160c671ae` and tree
   `95f16f05b30b7db23e43ebb6439ed0d055081902` as MS1 foundation lineage.
5. If any later commit changes source or behavior, **STOP** and establish the
   new implementation authority for that historical checkpoint before MS2.
6. Read `AGENTS.md`, `docs/CURRENT_PRODUCTION_STATE.md`, this handoff,
   `docs/milestone_plan.md`, and `docs/architecture.md`.
7. Read ADR-0032, retain ADR-0031 as superseded history, and read
   `docs/milestone_acceptance/MS1.md`.
8. Inspect the current single-symbol runtime assembly.
9. Obtain explicit MS2 implementation authorization.

## Current authority split

### A. Live GitHub main and current behavior/deployment source

```text
MS4_C_REVIEW_BASE_MAIN_SHA=efae0135ed5272d18d800af0ac247b70ece07422
MS4_C_REVIEW_BASE_MAIN_TREE=53342ac880cc36d65eba6f5e9b49fa722cc9d56b
MS4_DEPLOYMENT_SOURCE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
MS4_DEPLOYMENT_SOURCE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
CURRENT_BEHAVIOR_DEPLOYMENT_SOURCE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
CURRENT_BEHAVIOR_DEPLOYMENT_SOURCE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
MS1_FOUNDATION_LINEAGE_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
MS1_FOUNDATION_LINEAGE_TREE=95f16f05b30b7db23e43ebb6439ed0d055081902
MS1_MERGE_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
MS1_IMPLEMENTATION_MERGED=YES
MS1_PR=51
MS1_POST_MERGE_CI_RUN=33955915046
MS1_POST_MERGE_CI_PASS=YES
CURRENT_MAIN_DEPLOYED=NO
MS2_PR=54
MS2_MERGED=YES
MS3=CLOSED_MERGED
MS3_A_MERGED=YES
MS3_A_PR=55
MS3_B=CLOSED_MERGED_PR_56
MS3_B_CANDIDATE=feat/ms3b-shared-resource-acceptance
MS3_B_CANDIDATE_BRANCH_STATUS=DELETED_AFTER_MERGE
MS3_B_CANDIDATE_TIP=2a701fe79b78d3c63dd5959efecd20a2369d58e5
MS3_B_MERGED=YES
MS3_R1=CLOSED
MS3_INDEPENDENT_RE_REVIEW=APPROVED
MS3_CI1=CLOSED
MS3_CI2=CLOSED
MS3_R2=CLOSED
MS3_MERGE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
MS3_MERGE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
MS4=IN_PROGRESS_MS4_C_REVIEWED_COMPLETE
MS4_A=REVIEWED_COMPLETE
MS4_B=REVIEWED_COMPLETE
MS4_C=REVIEWED_COMPLETE
RECOVERY_GATE=REVIEWED_COMPLETE
MS4_D=NOT_STARTED
NEXT=MS4_D_REVIEW_STAGE_CLOSURE
```

The MS3-A merge parents are `52bf086dd240556b054821f33bf1e2840fdcf912` and
`ad1e941af3cd2bd3922eac239ba92a47058e9875`. PR #51 is merged, PR #54 is the
merged MS2 behavior authority, and PR #55 is the merged MS3-A authority on
current `main`; none is deployed as current production. The MS3-B implementation
is merged through PR #56 at `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`, tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`. It adds only the shared-resource
acceptance work recorded in `docs/milestone_acceptance/MS3.md`, including the
real production-path Collector/Poller supplement, its minimal in-flight REST
cancellation lifecycle correction, the MS3-R1 waiter barrier correction and
the CI1/CI2 fixture repairs. The former candidate tip
`2a701fe79b78d3c63dd5959efecd20a2369d58e5` is retained as restoration evidence;
the candidate branch was removed after merge and worktree checks.

MS1 merged the durable identity foundation. It did not implement runtime
fan-out, multi-symbol startup, or a new readiness policy. The MS1 merge SHA
above is historical foundation lineage; current behavior/deployment authority
is `303e073e25d5ed53d7cf6e26a9c6c6e879013b50` with tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`. GitHub `main` at MS4-C review
start was `efae0135ed5272d18d800af0ac247b70ece07422` with tree
`53342ac880cc36d65eba6f5e9b49fa722cc9d56b`. It is the PR #60 merge,
documentation-only relative to the deployed source, and not production
deployed; later GitHub merges may change live `main`.

### B. Deployed and clean-24h authority

The last independently qualified deployed artifact before MS1 is:

```text
SOURCE_SHA=c421605e302d2ad46acdb2466627f64644181c9a
SOURCE_TREE=a521dd61f8a090b4930cce5254985383f8893a3f
WHEEL_SHA256=278ee0b0df1e7766e205684ad1e401b12fb98341296164edc1c0de9b6d58c9c6
LOCK_SHA256=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335
CONFIG_SHA256=5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb
SYSTEMD_UNIT_SHA256=d5afc4c2228a78f02ffd7be07775e7c53acda90b8c2b1b3581d64020537188b6
DEPLOYMENT_ID=bdda546432bcaf6d29f281cd2a281b4d684bc447b2fafa382ffd1948f39a107f
```

That pre-MS1 artifact completed the single-symbol current-main non-formal
clean-24h stage with verdict
`A — CLEAN_24H_PASS_CURRENT_NONFORMAL_STAGE_COMPLETE`. Its evidence is
artifact-specific. It is not Formal M22.9 evidence, MS1 live qualification,
or a 14-product result. No 72h or 168h burn-in is required before beginning
multi-symbol development, and no old duration credit transfers to a new
multi-symbol artifact.

Accepted watches for later qualification are capacity cadence tail jitter
(p99 about 16.0819 s; max 27.3311 s; no events above 30 s) and RSS
early-growth-then-plateau (maximum 377339904 bytes, with the final segment
approximately flat). They are not MS2 blockers.

### C. MS2 implemented target

ADR-0032 supersedes ADR-0031. The future target is Binance-specific Spot and
USD-M perpetual capture with an operator-configured finite symbol list for
each market. There is no fixed symbol allowlist, automatic all-symbol
discovery, exchange/plugin framework, or hot runtime topology reload; changed
configuration takes effect on normal process restart. The project is not
becoming a multi-exchange framework.

The implemented `[recorder]` surface is `spot_symbols = [...]` and
`usdm_symbols = [...]`. Only when both fields are absent does legacy
compatibility mode resolve to the historical BTCUSDT/BTCUSDT profile. If either
field is present, explicit product-selection mode uses each supplied list
exactly and resolves an omitted sibling to an empty list; both resolved lists
empty is invalid. `ProductKey = (market, symbol)`; one process owns one durable
Catalog and one existing market Collector per configured ProductKey. The
The GitHub `main` snapshot at MS4-C review start contains that runtime; it was
not deployed as production, and later merges may change live `main`.

An empty resolved USD-M set means zero USD-M Collectors, zero product-specific
USD-M side-data managers, no process-global USD-M side-data owner, and no
USD-M REST or WebSocket traffic. The global owner exists only when a USD-M
ProductKey exists and at least one global kind is enabled. An empty Spot set
similarly creates no Spot Collector or Spot side-data traffic. The legacy
`GLOBAL_SIDE_DATA_SYMBOL="BTCUSDT"` sentinel never creates a ProductKey.

## MS1 accepted architecture

Implemented now:

- durable discontinuity identity `(market, symbol, stream)`, with `gap_id`
  included for lifecycle matching;
- symbol-specific side-data cursor identity `(kind, symbol)`;
- legacy Catalog migration to `BTCUSDT`, atomic, idempotent, restart-safe, and
  fail-closed;
- in-memory `BTCUSDT` normalization for historical symbol-less seal intents,
  without rewriting persisted historical intents;
- explicit-symbol new normal APIs;
- Raw v1 and external Contracts unchanged.

Cross-symbol gap collision is not possible under the accepted matching logic.
Global side-data remains global; the six persisted 5-minute statistic cursor
families are symbol-specific. Runtime fan-out is not part of MS1.

## Roadmap: MS2 → MS3 → MS4

### MS2 — Configurable product runtime

Implemented, independently reviewed, merged through PR #54, and offline-accepted. Includes explicit
finite Spot/USD-M product lists,
ProductKey propagation through existing WS/REST/schema/envelope/spool paths,
dynamic one-process Collector assembly, product-aware service state and
configuration-bound readiness, product-aware hard-reserve discontinuity
evidence, the shared USD-M REST authority, and one process-global USD-M
side-data owner. Preserve product ownership, per-product reconnect/resync and
backpressure isolation, and the MS1 durable identities.

Acceptance is primarily deterministic/offline: actual runtime ProductKeys equal
the configured expected set; identities cannot collide; product failure does
not alter another; product readiness is observable; global readiness is
configuration-bound and fail-closed; shared REST gating is not multiplied;
global side-data is not duplicated; cursors remain independent; and one-process
restart/shutdown remains coherent.

MS2 does not change Raw v1 or Contracts, add another exchange, implement
automatic all-symbol discovery, redesign archive format, optimize unrelated
hot paths, run a long burn-in, or
declare Production Ready.

### MS3 — Shared resources / rotation / observability

**MS3 CLOSED/MERGED.** MS3-A is merged through PR #55 and MS3-B is merged
through PR #56.
The candidate retains the deterministic F1–F10 model evidence and now adds
real `UsdMCollector`/`RestSideDataPoller` gate, pagination/cursor, rate-limit,
cancel/stop, and mixed 14-product/42-core-stream running-path evidence. The
existing sequential storage Profile D test is labeled as storage-layer proof;
the new running-path test proves simultaneous activity and sibling progress
under target backpressure. It adds no generic scheduler, persisted metrics
migration, or speculative optimization. Independent re-review/merge is closed;
see `docs/milestone_acceptance/MS3.md`. MS4-B stopped-deployment review and the
MS4-C recovery-gate review are complete; MS4-D review/stage closure is next.

### MS4 — Configurable-product integration / deployment qualification

MS4-B stopped-deployment review is complete after the exact source/artifact,
configuration, rollback and identity gates. The original owner-authorized
MS4-C window used the representative mixed Spot/USD-M profile and remains
`EXECUTED_PARTIAL_NOT_ACCEPTED` because its controlled recovery was not run in
that window. The separate passing R3
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT` closes the MS4-C recovery gate for review
with zero Formal M22.9 credit; it does not retroactively grant the original
window duration. The receiver-only Mac archive cycle did not authorize source
retirement. MS4-D review/stage closure is next; do not automatically schedule
72h or 168h, and keep Formal M22.9 separate.

## Non-negotiable boundaries

The Recorder captures Binance public market data only. It has no account
endpoints, API keys, credentials, orders, trading, strategies, or external
consumer repository dependency. Raw payload bytes and Raw v1 framing remain
recoverable and unchanged. Do not write production data under the repository
or use an external volume as an active Collector target.

`FORMAL_M22_9_STARTED=NO`, `PRODUCTION_READY=NO`,
`STOPPED_DEPLOYMENT_INSTALLED=YES`, `LIVE_START_AUTHORIZED=NO`,
`DEPLOYMENT_AUTHORIZED=NO` (live start only), `CURRENT_MAIN_DEPLOYED=NO`,
`MS2_IMPLEMENTATION_STARTED=YES`, `MS2=CLOSED`,
`MS3_A_MERGED=YES`, `MS3=CLOSED_MERGED`,
`MS3_B=CLOSED_MERGED_PR_56`,
`MS3_B_MERGED=YES`, `MS3_R1=CLOSED`, `MS3_CI1=CLOSED`, `MS3_CI2=CLOSED`,
`MS3_INDEPENDENT_RE_REVIEW=APPROVED`,
`MS3_MERGE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50`,
`MS3_MERGE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`,
`MS4=IN_PROGRESS_MS4_C_REVIEWED_COMPLETE`,
`MS4_B=REVIEWED_COMPLETE`, `MS4_C=REVIEWED_COMPLETE`,
`RECOVERY_GATE=REVIEWED_COMPLETE`, `MS4_D=NOT_STARTED`,
`NEXT=MS4_D_REVIEW_STAGE_CLOSURE`.

Any MS4-C start requires the reviewed exact installed source/artifact,
immutable Wheel, lock, config, unit, and deployment identities, explicit start
authorization, and a fresh bounded qualification. Historical single-symbol
duration credit does not transfer to the current multi-symbol artifact.
