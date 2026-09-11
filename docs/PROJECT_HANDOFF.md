# BinanceMarketDataRecorder — Project Handoff

This handoff is self-contained for a new development team. It separates
current GitHub engineering authority, the older deployed/qualified artifact,
and the current multi-symbol qualification program. Documentation is not deployment or live
traffic authorization.

## Current stage — Formal M22.9 2h failed closeout reviewed complete (2026-09-11)

The owner-authorized Formal 2-hour attempt on greencloud-tokyo-01 failed
before its first observer sample. T0 was
`2026-09-11T13:41:32.831837Z` / `1789134092831837314` ns with BOOTTIME
`789457702764089`, boot ID
`f2720022-bc39-4e22-bc68-af6bfce92274`, and observer InvocationID
`ee05ad7a73f04dec867629767439bc82`. The observer unit
`binance-market-data-acceptance-2h-20260911T132845Z-5b4fd719.service`
exited 1 before the first sample.

The failed root is
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T132845Z-5b4fd719-646792f2`
with stage root
`.../2h-a8b45b7e1da640c497213b172c235815` and immutable stage-start SHA-256
`1c35f62e0673488c441374971e8e8552212e9693aacfba35820ea613ae4a08f6`.
Stage-start failed with 24 blockers (23 Catalog/manifest disagreements and
one unexplained raw absence), so Formal duration credit is zero. The earlier
pre-T0 aborted setup root
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T130258Z-646792f2`
is preserved as a distinct zero-credit setup record.

After the requested graceful stop and archive drain, exact lifecycle checks
found all 26 unique chunk IDs named by those findings currently complete in
the verified archive state. This supports, but does not prove, a concurrent
AcceptanceObserver filesystem/Catalog snapshot race; the review has no
per-read interleaving trace. The compact forensic JSON, summary, and hashes
are under the failed root's `operator-evidence/closeout-review/` and are
listed in [`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md). The full
installed read-only post-stop audit completed over 112,817 manifests in 115.95
seconds with zero Catalog findings, zero integrity findings, and zero chunks
with scan issues. An additive correction preserves the original closeout
files and supersedes only their mistaken no-result statement.

```text
M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
12H=NOT_STARTED
PRODUCTION_READY=NO
```

No redeploy, retry, configuration, unit, identity, source, or code change is
authorized by this closeout. The next milestone is narrowly the
acceptance-observer/archive-concurrency diagnosis, fix, and offline test;
redeploy/retry authorization is separate.

## P2 deployment basis — reviewed complete (2026-09-11)

P2 executed the exact deployment, archive, and capacity preflight. The review
record is [`M22.9-P2 acceptance`](milestone_acceptance/M22.9-P2.md), with VPS
evidence under
`/srv/recorder-data/recorder-archive/evidence/M22.9-P2-20260911T090741Z`.
P2 deployment source/review base `646792f2e5fc5b7195ea58541d3f1dfda6555b7f`
and tree
`c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c` were installed and deployment
verified with exact artifact hashes. The four canonical ProductKeys reached
readiness with 12 core stream contexts; the Recorder was gracefully stopped
and is now `inactive/dead`, `MainPID=0`, `Result=success`, `NRestarts=0`.

The registered archive target is storage ID
`ef852751-721c-4145-9083-f6fd48718480` at
`/srv/recorder-data/recorder-archive` on ext4 `/dev/vdb1`. Existing verified
ArchiveManager/Catalog transactions drained the backlog to zero, and the full
archive verification reported 112,570 verified files with zero failures or
pending files. `binance-market-data-archive.timer` is enabled and
active/waiting with a future monotonic trigger and successful last service
result. `/dev/vdb1` is archive storage; the active writer root remains
`/var/lib/binance-market-data-recorder` on `/dev/vda1`.

The conservative historical generation rate is `349910.017730 B/s`; the
bounded 278-hour archive projection leaves approximately 1.772 TB of target
free-space margin, while active-root runway above the 10 GiB reserve is only
about 26.46 hours. `P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES` records exact
installed identity verification. After this docs-only merge,
`CURRENT_MAIN_DEPLOYED=NO`; the reason is
`DOCS_ONLY_DESCENDANT_NOT_INSTALLED`. At P2 completion the artifact was the
Formal candidate; after the failed T0 it remains only the installed evidence
basis pending the scoped fix, review, and separately authorized redeploy.
`PRODUCTION_READY=NO`.
The Formal 2-hour attempt subsequently failed at T0 before its first sample;
duration credit remains zero. The separate 12-hour stage is not started.

MS4_A=REVIEWED_COMPLETE; MS4_B=REVIEWED_COMPLETE;
MS4_C=REVIEWED_COMPLETE; RECOVERY_GATE=REVIEWED_COMPLETE;
MS4_D=REVIEWED_COMPLETE; MS4=REVIEWED_COMPLETE;
BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS;
QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE;
M22_9_P1=REVIEWED_COMPLETE;
M22_9_P2=REVIEWED_COMPLETE;
M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE;
FORMAL_M22_9=EXECUTED_FAILED_AT_T0; FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0;
FORMAL_M22_9_CREDIT_SECONDS=0; 12H=NOT_STARTED;
PRODUCTION_READY=NO; P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES;
CURRENT_MAIN_DEPLOYED=NO; RECORDER=STOPPED;
CURRENT_MAIN_DEPLOYMENT_REASON=DOCS_ONLY_DESCENDANT_NOT_INSTALLED;
ARCHIVE_TIMER=ENABLED_ACTIVE;
NEXT=ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST;
REDEPLOY_RETRY_AUTHORIZATION=SEPARATE_AUTHORIZATION.

P2 used only unsigned public Binance market-data eligibility endpoints. No
account, order, key, or credential endpoint was accessed, and no manual Raw
deletion was used. The short live interaction is non-formal and grants no
Formal duration credit. P2 itself created no Formal T0, P1 observer, long soak,
or automatic stage advancement; the later Formal 2-hour attempt did create T0
and failed there.

### Historical MS4 and P1 checkpoints (not current authority)

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
Catalog snapshot. MS4-D reviewed the bounded evidence and closes the
multi-symbol qualification for the exact four-ProductKey profile. The archive
timer is explicitly `disabled`; Formal M22.9 remains separate and unstarted,
and `PRODUCTION_READY=NO`. The current M22.9-P1 package documents an external
systemd-detached observer for one explicitly selected stage and is reviewed
complete; it did not deploy or start the Recorder.

The historical MS4-D review base was `main` commit
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` (tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`). It includes the merged MS4-C
evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d` with the same tree; base CI run
`34551834444` completed successfully. These are historical review authorities
only. P1 was based on main at its historical checkpoint
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`; at that checkpoint the current
main was not deployed. This is historical, not the current P2 state.

The historical MS4-D final read-only VPS check at `2026-09-11T06:39:23Z`
passed: Recorder was
`inactive/dead` with `MainPID=0`, `Result=success`, `NRestarts=0`, and the
archive timer was `disabled/inactive`. It observed `/dev/vdb1` (`ext4`) mounted
at `/srv/recorder-data` with `2163348520960` total bytes,
`19120271360` used, `2122221260800` available and `1%` used. The active writer
root remains `/var/lib/binance-market-data-recorder`; this mounted disk is an
archive target, not the active writer root. No start, mutation or live traffic
occurred during the check.

The P1 current read-only VPS check at `2026-09-11T07:27:40Z` recorded systemd
255; Recorder `inactive/dead` with `MainPID=0`, `Result=success`,
`NRestarts=0`; and `binance-market-data-archive.timer`
`loaded/disabled/inactive`. The active `/dev/vda1` writer filesystem was
approximately 57.1 GB total with 41.2 GB available. The archive `/dev/vdb1`
filesystem was ext4, approximately 2 TB with approximately 1.9 TB available;
Catalog storage ID `ef852751-721c-4145-9083-f6fd48718480` resolved `READY` at
`/srv/recorder-data/recorder-archive`. P1 performed only these read-only checks
and the two harmless transient-unit lifecycle probes: it did not deploy or
start Recorder, enable the timer, archive or delete Raw, or send Binance
traffic. The probes changed only their ephemeral systemd test units.

MS4_A=REVIEWED_COMPLETE; MS4_B=REVIEWED_COMPLETE;
MS4_C=REVIEWED_COMPLETE; RECOVERY_GATE=REVIEWED_COMPLETE;
MS4_D=REVIEWED_COMPLETE; MS4=REVIEWED_COMPLETE;
BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS;
QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE;
M22_9_P1=REVIEWED_COMPLETE;
FORMAL_M22_9=NOT_STARTED; FORMAL_M22_9_CREDIT_SECONDS=0;
PRODUCTION_READY=NO; CURRENT_MAIN_DEPLOYED=NO; RECORDER=STOPPED;
NEXT=M22_9_P2_EXACT_DEPLOYMENT_ARCHIVE_CAPACITY_PREFLIGHT.

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
was the detailed continuation queue at that historical cut; the candidate
submission summaries below retain their evidence context. The current P2
authority is the opening section of this handoff.

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

### P2 current deployment/archive authority

```text
P2_REVIEW_BASE_MAIN_SHA=646792f2e5fc5b7195ea58541d3f1dfda6555b7f
P2_REVIEW_BASE_MAIN_TREE=c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c
EVIDENCE_ROOT=/srv/recorder-data/recorder-archive/evidence/M22.9-P2-20260911T090741Z
STORAGE_ID=ef852751-721c-4145-9083-f6fd48718480
P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES
CURRENT_MAIN_DEPLOYED=NO
CURRENT_MAIN_DEPLOYMENT_REASON=DOCS_ONLY_DESCENDANT_NOT_INSTALLED
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
M22_9_P2=REVIEWED_COMPLETE
M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
12H=NOT_STARTED
PRODUCTION_READY=NO
NEXT=ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST
REDEPLOY_RETRY_AUTHORIZATION=SEPARATE_AUTHORIZATION
```

The exact deployment passed identity/readiness verification for four
ProductKeys and 12 core stream contexts. The existing archive transaction path
drained the registered target to backlog zero; full verification reported
112,570 verified files with zero failures or pending files. The archive timer
is enabled and active/waiting with a future monotonic trigger. The active
writer root remains `/var/lib/binance-market-data-recorder`; the approximately
2 TB `/dev/vdb1` filesystem is archive storage only. Capacity margin and the
active-root runway limitation are recorded in the P2 acceptance file. This
block is the current authority; the following A/B sections preserve historical
lineage.

### A. Historical GitHub main and behavior/deployment source

```text
HISTORICAL_MS4_C_REVIEW_BASE_MAIN_SHA=efae0135ed5272d18d800af0ac247b70ece07422
HISTORICAL_MS4_C_REVIEW_BASE_MAIN_TREE=53342ac880cc36d65eba6f5e9b49fa722cc9d56b
MS4_D_REVIEW_BASE_MAIN_SHA=e11d5cbdf861ab82bb110ead8e98a1f9498f3c55
MS4_D_REVIEW_BASE_MAIN_TREE=9fcf3e4128706938ffd02ef5a6af80c558cb234b
MS4_D_REVIEW_BASE_PR=61
MS4_D_REVIEW_BASE_PR_COMMIT_SHA=013e20d6b911fde2f443aa6c855039599483ef7d
MS4_D_REVIEW_BASE_PR_COMMIT_TREE=9fcf3e4128706938ffd02ef5a6af80c558cb234b
MS4_D_REVIEW_BASE_CI_RUN=34551834444
MS4_D_REVIEW_BASE_CI_STATUS=COMPLETED_SUCCESS
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
MS4=REVIEWED_COMPLETE
MS4_A=REVIEWED_COMPLETE
MS4_B=REVIEWED_COMPLETE
MS4_C=REVIEWED_COMPLETE
RECOVERY_GATE=REVIEWED_COMPLETE
MS4_D=REVIEWED_COMPLETE
BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS
QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE
M22_9_P1=REVIEWED_COMPLETE
NEXT=M22_9_P2_EXACT_DEPLOYMENT_ARCHIVE_CAPACITY_PREFLIGHT
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
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
above is historical foundation lineage; historical MS4-C behavior/deployment
authority was `303e073e25d5ed53d7cf6e26a9c6c6e879013b50` with tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`. The historical GitHub `main` at
MS4-C review start was `efae0135ed5272d18d800af0ac247b70ece07422` with tree
`53342ac880cc36d65eba6f5e9b49fa722cc9d56b`; it is the PR #60 merge,
documentation-only relative to the deployed source, and not production
deployed. The historical MS4-D review base is
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` with tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`; it includes the merged MS4-C
evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d`, and base CI run `34551834444`
completed successfully. The current M22.9-P1 documentation base is main
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`; later GitHub merges may change
live `main`.

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
see `docs/milestone_acceptance/MS3.md`. MS4-B stopped-deployment review, the
MS4-C recovery-gate review and the MS4-D bounded stage closure are complete.

### MS4 — Configurable-product integration / deployment qualification

MS4-B stopped-deployment review is complete after the exact source/artifact,
configuration, rollback and identity gates. The original owner-authorized
MS4-C window used the representative mixed Spot/USD-M profile and remains
`EXECUTED_PARTIAL_NOT_ACCEPTED` because its controlled recovery was not run in
that window. The separate passing R3
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT` closes the MS4-C recovery gate for review
with zero Formal M22.9 credit; it does not retroactively grant the original
window duration. The receiver-only Mac archive cycle did not authorize source
retirement. The resulting qualification is non-formal and bounded to the
four-product core; do not automatically schedule 72h or 168h, and keep Formal
M22.9 separate.

## Non-negotiable boundaries

The Recorder captures Binance public market data only. It has no account
endpoints, API keys, credentials, orders, trading, strategies, or external
consumer repository dependency. Raw payload bytes and Raw v1 framing remain
recoverable and unchanged. Do not write production data under the repository
or use an external volume as an active Collector target.

`FORMAL_M22_9_STARTED=YES`,
`FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0`,
`FORMAL_M22_9=EXECUTED_FAILED_AT_T0`,
`FORMAL_M22_9_CREDIT_SECONDS=0`, `12H=NOT_STARTED`,
`PRODUCTION_READY=NO`,
`STOPPED_DEPLOYMENT_INSTALLED=YES`, `LIVE_START_AUTHORIZED=NO`,
`DEPLOYMENT_AUTHORIZED=NO` (future live start only),
`P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES`,
`CURRENT_MAIN_DEPLOYED=NO`,
`CURRENT_MAIN_DEPLOYMENT_REASON=DOCS_ONLY_DESCENDANT_NOT_INSTALLED`,
`MS2_IMPLEMENTATION_STARTED=YES`, `MS2=CLOSED`,
`MS3_A_MERGED=YES`, `MS3=CLOSED_MERGED`,
`MS3_B=CLOSED_MERGED_PR_56`,
`MS3_B_MERGED=YES`, `MS3_R1=CLOSED`, `MS3_CI1=CLOSED`, `MS3_CI2=CLOSED`,
`MS3_INDEPENDENT_RE_REVIEW=APPROVED`,
`MS3_MERGE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50`,
`MS3_MERGE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`,
`MS4=REVIEWED_COMPLETE`,
`MS4_B=REVIEWED_COMPLETE`, `MS4_C=REVIEWED_COMPLETE`,
`RECOVERY_GATE=REVIEWED_COMPLETE`, `MS4_D=REVIEWED_COMPLETE`,
`BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS`,
`QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE`,
`M22_9_P1=REVIEWED_COMPLETE`, `M22_9_P2=REVIEWED_COMPLETE`,
`FORMAL_M22_9_CREDIT_SECONDS=0`, `RECORDER=STOPPED`,
`ARCHIVE_TIMER=ENABLED_ACTIVE`,
`NEXT=ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST`.

Any further live traffic or Formal stage requires the exact P2 deployment
source/artifact, immutable Wheel, lock, config, unit, and deployment identities,
explicit separate authorization, and a fresh readiness/capacity decision.
Historical single-symbol and non-formal duration credit does not transfer.

M22.9-P1 is reviewed-complete, documentation-only preparation for an external systemd 255
transient `Type=exec` unit around the existing foreground acceptance observer.
It uses `Restart=no`, `SIGINT`/120-second clean stop, non-root `bmdr:bmdr`,
`UMask=0027`, `NoNewPrivileges=yes`, and journal output. The unit is detached
from SSH but never schedules, restarts, or advances stages. The exact
registered archive-subdirectory root, detached probe evidence, same-stage
resume rules, and final-review command are in
[`docs/milestone_acceptance/M22.9-P1.md`](milestone_acceptance/M22.9-P1.md).
P1 did not deploy, start Recorder, enable the archive timer, archive/delete
Raw, or begin Formal M22.9. P2 is reviewed complete: it installed and verified
the exact P2 deployment source/review base, drained the registered archive via
the existing transaction path, and performed bounded non-formal readiness and
interaction before stopping Recorder. After this docs-only merge,
`CURRENT_MAIN_DEPLOYED=NO`; the docs-only merge descendant is not installed.
The installed P2 artifact remains the evidence basis for the failed attempt,
but is not retry-eligible until the scoped observer fix is reviewed and a
later exact artifact is separately authorized and deployed. The Formal 2-hour
attempt then failed at T0 before its first sample; the exact roots and
quiescent forensic review are recorded in
[`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md). The next named
milestone is `ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST`;
redeploy/retry authorization is separate and this handoff does not start it.
