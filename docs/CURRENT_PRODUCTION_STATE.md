# Current Production State

This is the concise current authority. It separates GitHub engineering source,
the independently qualified deployed artifact, and the current multi-symbol
qualification program. Verify live GitHub before acting; this document does not authorize
deployment, live traffic, formal acceptance, or data retirement.

## Current milestone — M22_9 post-merge macOS CI reconnect-layout repair (2026-09-12)

This milestone records a test-only schedule-sensitive assumption repair exposed
by GitHub main push offline-ci run `34663566549`. Only macOS Python 3.12 job
`103470852952` failed; its pytest result was `1 failed, 1651 passed, 24
skipped, 4 deselected` at the old line 794 of the Spot session-restart
post-close timeout test. Ubuntu job `103470853142` passed all gates. PR #68
exact-head run `34663191565` passed on both platforms, and twenty local exact
Spot-node repetitions on macOS arm64 Python 3.12.14 passed, confirming schedule
sensitivity. The legal two-manifest layout is already enforced by
`assert_old_ingress_boundary_layout`; the USD-M session-restart test had the
same fixed one-manifest assertion.

Only the Spot and USD-M ingress backpressure tests changed. Both session-
restart tests now validate the existing complete layout helper with the exact
lifecycle STARTED `original_connection_id`, while retaining the surrounding
boundary, recovery, ordering, no-fabrication, and fail-closed assertions. No
production, contract, source-record, deployment, VPS, or archive change is
included. Independent lead review found P0=0, P1=0, and P2=0; the
production/test delta is independently reviewed complete, while GitHub merge
remains a separate operation.

```text
MILESTONE=M22_9_POST_MERGE_MACOS_CI_RECONNECT_LAYOUT_REPAIR
MILESTONE_STATUS=REVIEWED_COMPLETE
BASE_MAIN_SHA=bed826909e1a53089289981e9ea435c7ddbf07f0
BASE_MAIN_TREE=a85a39f5096f834a95bae11f9d047fac18602bc6
BRANCH=codex/m22-9-main-macos-ci-reconnect-layout
GITHUB_MAIN_PUSH_RUN=34663566549
MACOS_PYTHON312_JOB=103470852952
UBUNTU_PYTHON312_JOB=103470853142
PR_68_EXACT_HEAD_RUN=34663191565
VPS_TOUCHED=NO
CURRENT_MAIN_DEPLOYED=NO
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
FORMAL_M22_9=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
12H=NOT_STARTED
PRODUCTION_READY=NO
NEXT=EXACT_ARTIFACT_REDEPLOY_PREFLIGHT
REDEPLOY_RETRY_AUTHORIZATION=SEPARATE_AUTHORIZATION
```

`RECORDER=STOPPED` and `ARCHIVE_TIMER=ENABLED_ACTIVE` are the last
authoritative VPS values; this local run did not access the VPS. See the
[acceptance record](milestone_acceptance/M22.9-main-macos-ci-reconnect-layout.md).

## Previous local milestone — AcceptanceObserver/archive concurrency fix reviewed complete (2026-09-12)

The narrowly scoped offline diagnosis and fix is complete. AcceptanceObserver
now receives one lifecycle-coherent Catalog boundary before its long manifest
scan, compares membership only within that boundary, defers later rows, and
freshly validates exact archive lifecycle state when local Raw disappears.
The existing ArchiveManager verification and fail-closed safety rules remain
the authority. Deterministic offline coverage includes post-boundary deferral,
`LOCAL_DELETE_PENDING` unlink races, validation-time disappearance,
unauthorized absence, external corruption, and observer read-only behavior.
Independent Luna Max review of exact code-review commit
`31cabe4445ee699ad284aa707d24333c78cf8d21` against base
`e214120a25a5aff28fad4903c9510920a25738d3` found P0=0, P1=0, and P2=0;
the reviewer could not rerun pytest because its read-only sandbox had no usable
temporary directory, which is not a product failure.

```text
ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_FIX=REVIEWED_COMPLETE
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
12H=NOT_STARTED
PRODUCTION_READY=NO
VPS_TOUCHED=NO
CURRENT_MAIN_DEPLOYED=NO
NEXT=EXACT_ARTIFACT_REDEPLOY_PREFLIGHT
REDEPLOY_RETRY_AUTHORIZATION=SEPARATE_AUTHORIZATION
```

The detailed record is [`M22.9 observer/archive concurrency fix`](milestone_acceptance/M22.9-observer-archive-concurrency-fix.md).
The next gate is exact-artifact redeploy preflight; redeploy and any Formal
retry require separate authorization.

## Historical current-stage record — Formal M22.9 2h failed closeout reviewed complete (2026-09-11)

The owner-authorized Formal 2-hour attempt on greencloud-tokyo-01 failed
before the first observer sample. T0 was
`2026-09-11T13:41:32.831837Z` / `1789134092831837314` ns, BOOTTIME
`789457702764089`, boot ID
`f2720022-bc39-4e22-bc68-af6bfce92274`. Observer unit
`binance-market-data-acceptance-2h-20260911T132845Z-5b4fd719.service`
failed with exit 1, InvocationID
`ee05ad7a73f04dec867629767439bc82`; no first sample was recorded.

The failed Formal root is
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T132845Z-5b4fd719-646792f2`
and its stage root is
`.../2h-a8b45b7e1da640c497213b172c235815`. The immutable stage-start
SHA-256 is
`1c35f62e0673488c441374971e8e8552212e9693aacfba35820ea613ae4a08f6`.
Stage-start reported 24 blockers: 23
`catalog_manifest_disagreement:<chunk_id>` findings and one
`unexplained_raw_absence`; the attempt receives zero duration credit. The
earlier pre-T0 aborted setup root
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T130258Z-646792f2`
is preserved separately and is not the failed Formal T0 root.

After graceful Recorder stop and the bounded verified archive drain, a
focused exact-chunk review found all 26 unique affected chunk IDs currently
reconciled as `AUTHORIZED_ARCHIVE_STATE_CURRENTLY_COMPLETE`: Catalog rows
are `LOCAL_DELETED`, internal manifests remain, internal sealed Raw is
absent, and verified archive Raw/manifests are present. All 26 archive
transactions and source retirements occurred after T0. This supports, but
does not prove, a concurrent AcceptanceObserver filesystem/Catalog snapshot
race; no per-read interleaving trace exists. The compact forensic record is
under the failed root's `operator-evidence/closeout-review/`. The full
installed read-only post-stop audit also completed over 112,817 manifests in
115.95 seconds with zero Catalog findings, zero integrity findings, and zero
chunks with scan issues. An additive correction preserves the original files
and supersedes only their mistaken no-result statement. Exact hashes are in
[`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md).

Current status is:

```text
M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
12H=NOT_STARTED
PRODUCTION_READY=NO
```

The installed P2 exact source/review base remains
`646792f2e5fc5b7195ea58541d3f1dfda6555b7f`, with deployment identity
`11029b9434f72fe48912659c050167e6a058e9827cd40401c9a852dff3c019cd`.
`CURRENT_MAIN_DEPLOYED=NO` because this documentation-only descendant is
not installed. The installed artifact is not retry-eligible until the scoped
fix is reviewed and a later exact artifact is separately authorized and
deployed. The offline fix is recorded in the current section above; the next
gate is independent code review plus exact-artifact redeploy preflight, and
redeploy/retry require separate authorization.

## P2 deployment basis — reviewed complete (2026-09-11)

P2 executed the exact deployment, archive, and capacity preflight. The complete
record is [`M22.9-P2 acceptance`](milestone_acceptance/M22.9-P2.md) and the VPS
evidence bundle at
`/srv/recorder-data/recorder-archive/evidence/M22.9-P2-20260911T090741Z`.
The installed deployment verifies the P2 deployment source/review base
`646792f2e5fc5b7195ea58541d3f1dfda6555b7f` and tree
`c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c`. Exact wheel, lock, config, unit,
and identity hashes are recorded in the acceptance file.

The canonical Spot/USD-M four-ProductKey deployment reached readiness with
12 core stream contexts. It was gracefully stopped and is currently
`inactive/dead`, `MainPID=0`, `Result=success`, `NRestarts=0`.
`P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES` records that exact identity
verification. After this docs-only merge,
`CURRENT_MAIN_DEPLOYED=NO`; the reason is
`DOCS_ONLY_DESCENDANT_NOT_INSTALLED`. At P2 completion the artifact was the
Formal candidate; after the failed T0 it remains only the installed evidence
basis pending the scoped fix, review, and separately authorized redeploy. This
does not mean Production Ready.

The registered archive target is storage ID
`ef852751-721c-4145-9083-f6fd48718480` at
`/srv/recorder-data/recorder-archive` on ext4 `/dev/vdb1`. The existing
ArchiveManager/Catalog transaction path drained the bounded and post-stop
backlogs to zero; the full archive verification reported 112,570 verified
files, zero failed, and zero pending. The archive timer is
`binance-market-data-archive.timer`, `enabled/active/waiting` with a future
monotonic trigger and successful last service result. The active writer remains
`/var/lib/binance-market-data-recorder` on `/dev/vda1`; `/dev/vdb1` is an
archive target, not an active writer root.

The conservative existing 24-hour generation rate is `349910.017730 B/s`.
The 278-hour projection is `350189945744` bytes and archive free space leaves
approximately `1771538091120` bytes after that projection (about 6.06x the
projection). The active-root free space above the 10 GiB hard reserve provides
only about 26.46 hours at that rate. The short live interaction's
`43835.914894 B/s` active-root growth is not a long-run rate. Every Formal
stage start and end must independently recheck timer health, backlog, target
margin, and active-root runway.

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

P2 used only unsigned public Binance eligibility endpoints for BTCUSDT and
ETHUSDT on Spot and USD-M; no account, order, key, or credential endpoint was
accessed. Source retirement occurred only through the existing verified
ArchiveManager/Catalog transaction path; no manual deletion was used. The
short live interaction is non-formal evidence and grants no duration credit.
P2 itself created no Formal T0, P1 observer, long soak, or automatic stage
advancement; the later Formal 2-hour attempt did create T0 and failed there.

### Historical MS4 and P1 checkpoints (not current authority)

MS4-A local preparation and MS4-B target preflight/stopped-deployment review
remain complete. The owner-authorized MS4-C attempt started the reviewed
four-ProductKey artifact on the Tokyo VPS, reached authoritative `READY` for
all four ProductKeys, and completed the bounded two-hour steady interval. That
first window remains `EXECUTED_PARTIAL_NOT_ACCEPTED` because its controlled
recovery was not executed in the approved window; it is not retroactively
reclassified by the later supplement. The MS4-B private bundle's
`PRIVATE_EVIDENCE_BUNDLE_DIRECT_INSPECTION=NOT_RUN_LOCAL` is a historical
MS4-B local-review limitation, not an MS4-C evidence limitation. The MS4-C VPS
evidence was directly inspected by the primary agent; both records are listed
in [`MS4-C evidence`](milestone_evidence/MS4-C-20260910.md).

The separate 2026-09-11 R3 evidence is a passing
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT`. It used fresh official eligibility
evidence, a targeted exact-MainPID socket disconnect, and bounded post-fault
observations; all four ProductKeys remained ready and receiving, 12 core
contexts passed sealed validation, Catalog integrity was clean, and no active
partial remained. It closes the MS4-C recovery gate for review, but grants
`formal_m22_9_credit_seconds=0`.

The service was gracefully stopped and remains stopped; the archive timer is
explicitly `disabled`. A receiver-only SSH
archive cycle to the explicitly authorized MacBook Downloads internal-folder
test target was verified; it created no remote pending authority, did not
delete or retire the VPS source, and did not produce a formal receipt-bound
Catalog snapshot. MS4-D reviewed the bounded evidence and closes the
multi-symbol qualification for the exact four-ProductKey profile. The original
window remains partial and the R3 supplement does not transfer duration credit.
`PRODUCTION_READY=NO`; Formal M22.9 remains separate and unstarted. The
current M22.9-P1 package documents an external systemd-detached observer for
one explicitly selected stage and is reviewed complete; it did not deploy or
start the Recorder.

The historical MS4-D review base was `main` commit
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` (tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`). It includes the merged MS4-C
evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d` with the same tree; base CI run
`34551834444` completed successfully. These are historical review authorities
only. P1 was based on main at its historical checkpoint
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`; at that checkpoint
`CURRENT_MAIN_DEPLOYED=NO`. This is historical, not the current P2 state.

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
FORMAL_M22_9=NOT_STARTED; FORMAL_M22_9_CREDIT_SECONDS=0;
PRODUCTION_READY=NO; CURRENT_MAIN_DEPLOYED=NO; RECORDER=STOPPED;
NEXT=M22_9_P2_EXACT_DEPLOYMENT_ARCHIVE_CAPACITY_PREFLIGHT.

M22_9_P1=REVIEWED_COMPLETE;
FORMAL_M22_9=NOT_STARTED; FORMAL_M22_9_CREDIT_SECONDS=0;
PRODUCTION_READY=NO; CURRENT_MAIN_DEPLOYED=NO; RECORDER=STOPPED;
NEXT=M22_9_P2_EXACT_DEPLOYMENT_ARCHIVE_CAPACITY_PREFLIGHT.

P1 is documentation-only and is based on main
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`. It retains the existing
`AcceptanceObserver` as the only measurement/evidence authority and documents
systemd 255 transient `Type=exec` units with `Restart=no`, `SIGINT` stop,
120-second stop timeout, non-root `bmdr:bmdr`, `UMask=0027`,
`NoNewPrivileges=yes`, and journal output. Each unit observes one stage only;
SSH detachment does not authorize automatic restart or stage advancement.

The harmless systemd detached probe used InvocationID
`ec84df57f764445ca2db873eedc97b6c` from `2026-09-11T07:29:02Z` to
`2026-09-11T07:29:22Z`; a second SSH session saw the retained journal and the
successful unit later became `not-found` after garbage collection. The SIGINT
probe used InvocationID `2bc9ce4af8a94d4f867f5842631fab67` and recorded
SIGINT/KeyboardInterrupt followed by clean `systemctl stop`. Neither probe
touched Recorder or Raw.

The current correct archive timer is `binance-market-data-archive.timer`,
loaded/disabled/inactive. Recorder remains `inactive/dead`, `MainPID=0`,
`Result=success`, `NRestarts=0`. The active writer is `/dev/vda1`
(approximately 57.1 GB total and 41.2 GB available); its selected historical
24-hour net growth is `167228.007472 B/s`, forecasting hard-reserve reach at
`2026-09-13T15:11:31.626026Z`, so the approximately 278-hour formal chain
does not have a capacity-complete precondition. The mounted 2 TB `/dev/vdb1`
has approximately 2.122 TB available and registered storage ID
`ef852751-721c-4145-9083-f6fd48718480` resolves `READY` at
`/srv/recorder-data/recorder-archive`; it remains an archive target, not the
active writer root. P1 does not enable the timer, archive, delete source Raw,
or start Formal M22.9.

## Original MS4-C attempt disposition — 2026-09-10

The attempt ran from `T0=2026-09-10T15:10:26.637211Z` to
`T1=2026-09-10T17:10:33.156829Z`, with recorded monotonic duration
`7206.519615476` seconds. The recovery observation ended at
`2026-09-10T17:25:33.300961Z` without a reviewed recovery completion signal;
the controlled recovery was not executed in the approved window. The explicit
stop returned success at `2026-09-10T17:26:04.819925Z`, with systemd
`ActiveState=inactive`, `SubState=dead`, `MainPID=0`, and `Result=success`.

The local receiver verified chunk
`346c856f-86c3-49d1-a7c3-77e98df52431` (`spot`/`ETHUSDT`/`diff_depth`, 38
records, 10,258 stored bytes, SHA-256
`12886aed3a2921b778e0fd3e99d746ab46a7c7dcb55a7791d218d7e6534582be`). The
source remained present with the same hash after the stop. Its manifest was
sealed before T0, so this receive is not steady-period archive-throughput
evidence. No remote authorization, deletion, source retirement, or formal
receipt-bound snapshot was performed. See the detailed
[`MS4-C attempt report`](milestone_evidence/MS4-C-20260910.md).

## MS4-C R3 recovery supplement — 2026-09-11

The independent R3 supplement is recorded at
`/root/MS4-C-RECOVERY-20260911-R3/evidence/recovery-audit.json` on the Tokyo
VPS. It is `PASS` with classification
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT` and zero Formal M22.9 credit. Fresh
official eligibility was captured at 2026-09-11T01:01:49Z; the service reached
`READY` as MainPID 685913, and the exact owned socket was disconnected. The
only durable recovery was `um_perpetual:BTCUSDT/book_ticker`, an
`unexpected_disconnect` gap completed in `0.436024210` seconds with a new
connection. Fifteen post-fault observations kept all four expected ProductKeys
ready with receive progress. Twelve core contexts passed sealed validation
(`validated_chunks=23`); Catalog integrity was `ok`, with no malformed,
degraded, unclosed, active, or `.partial` artifacts. Final systemd state was
`inactive/dead`, `MainPID=0`, `Result=success`. The recovery restored the
bounded runtime path; it does not claim historical continuity was restored,
does not grant duration credit, and did not authorize source retirement.

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
authority is the opening section of this document.

## Historical status at a glance (not current authority)

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
MS1_STATUS=MERGED
MS1_PR=51
MS1_POST_MERGE_CI_RUN=33955915046
MS1_POST_MERGE_CI_PASS=YES
CURRENT_MAIN_DEPLOYED=NO
PRE_MS1_NONFORMAL_VALIDATION_STAGE_COMPLETE=YES
MULTI_SYMBOL_DEVELOPMENT_MAY_BEGIN=YES
MS2_IMPLEMENTATION_STARTED=YES
MS2=CLOSED
MS2_INDEPENDENT_PR_REVIEW=COMPLETE
MS2_MERGED_PR=54
MS3=CLOSED_MERGED
MS3_A=MERGED_PR_55
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
FORMAL_M22_9=NOT_STARTED
FORMAL_M22_9_CREDIT_SECONDS=0
PRODUCTION_READY=NO
CURRENT_MAIN_DEPLOYED=NO
RECORDER=STOPPED
```

## Current MS3-B merged authority

MS2 implementation, offline acceptance, independent review, and merge are
closed on current `main` via PR #54. MS3-A is merged via PR #55 at
`01527037254595267003f886689bb270e08b5e5d`, tree
`eb63b645660a64ac606341ce3fb7f447a6e89457`, with merge parents
`52bf086dd240556b054821f33bf1e2840fdcf912` and
`ad1e941af3cd2bd3922eac239ba92a47058e9875`. MS3-B merged evidence is
recorded via PR #56 at `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`, tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`; it is not deployed. The merged
candidate includes the narrow MS3-R1 waiting-cancellation correction and the
MS3-CI1/CI2 test repairs. The former candidate branch was safely removed after
verifying its merged tip; restoration tip
`2a701fe79b78d3c63dd5959efecd20a2369d58e5` is retained in the ledger.
The update adds real `UsdMCollector`/`RestSideDataPoller` production-path
evidence, finite Catalog pagination/cursor competition, cancellation lifecycle
coverage, a minimal owned-worker fix for in-flight side REST cancellation, and
the corrected waiter enqueue/cancel/successor/post-stop assertions.
See [MS2 acceptance](milestone_acceptance/MS2.md) and
[MS3 acceptance](milestone_acceptance/MS3.md) for the acceptance records.

## A. Historical/time-local pre-MS2 lineage and current source split

| Item | Authority |
| --- | --- |
| Live GitHub `main` at historical MS3-B start | `01527037254595267003f886689bb270e08b5e5d`; tree `eb63b645660a64ac606341ce3fb7f447a6e89457` |
| Historical behavior/deployment source at the pre-P2 cut | `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`; tree `2b30a4dd2b8c694ac2e3abad88d6cb56a75badee` |
| Historical GitHub `main` at MS4-C review start (PR #60 merge; not deployed) | `efae0135ed5272d18d800af0ac247b70ece07422`; tree `53342ac880cc36d65eba6f5e9b49fa722cc9d56b` |
| Historical MS4-D documentation review base (`main`; not deployed) | `e11d5cbdf861ab82bb110ead8e98a1f9498f3c55`; tree `9fcf3e4128706938ffd02ef5a6af80c558cb234b` |
| Merged MS4-C evidence closeout included in historical MS4-D review base (PR #61; not deployed separately) | `013e20d6b911fde2f443aa6c855039599483ef7d`; tree `9fcf3e4128706938ffd02ef5a6af80c558cb234b`; CI `34551834444` completed/success |
| M22.9-P1 documentation base (current `main`; not deployed) | `83a063f5bb9f91508238c9fd86d21aa45d1bd501` |
| MS1 foundation lineage (historical) | `d38180074b5f76ab6b7778eea7fc505160c671ae`; tree `95f16f05b30b7db23e43ebb6439ed0d055081902` |
| MS1 merge (historical) | `d38180074b5f76ab6b7778eea7fc505160c671ae` |
| Merge parents | `c421605e302d2ad46acdb2466627f64644181c9a`, `11e100fbcb974e7d54f0515c99e08ac6042b9204` |
| MS1 | merged via PR #51; reviewed head `11e100fbcb974e7d54f0515c99e08ac6042b9204` |
| Post-merge CI | `offline-ci` run `33955915046`, push event, macOS and Ubuntu Python 3.12 jobs successful |
| Deployment | GitHub `main` at MS4-C review start was not deployed |

MS1 is the merged multi-symbol durable identity foundation. Its three
implementation commits remain provenance, not separate current authorities:
`026e357eb9af9b5b9fd111872dc6dcc30e9c599d`,
`39fbd04172a6b5b27b41d43c57d0e5ff575b95d4`, and
`11e100fbcb974e7d54f0515c99e08ac6042b9204`.

The MS1 merge above is retained as historical foundation lineage from the
pre-MS2 review; it is not the current behavior authority. The historical
behavior/deployment source at that cut was `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`, tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`. The historical GitHub `main`
snapshot at MS4-C review start was `efae0135ed5272d18d800af0ac247b70ece07422`,
tree `53342ac880cc36d65eba6f5e9b49fa722cc9d56b`; it is the PR #60 merge,
documentation-only relative to the deployed source, and is not production
deployed. The historical MS4-D review base is
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` with tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`; it includes the merged MS4-C
evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d`, and base CI run `34551834444`
completed successfully. The current M22.9-P1 documentation base is main
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`; later GitHub merges may change
live `main`.

## B. Deployed and clean-24h authority

The last independently qualified deployed artifact is the pre-MS1,
single-symbol artifact. It is not current `main` and is not MS1 live
qualification.

| Item | Frozen value |
| --- | --- |
| Source SHA | `c421605e302d2ad46acdb2466627f64644181c9a` |
| Source tree | `a521dd61f8a090b4930cce5254985383f8893a3f` |
| Wheel SHA-256 | `278ee0b0df1e7766e205684ad1e401b12fb98341296164edc1c0de9b6d58c9c6` |
| Lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Config SHA-256 | `5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb` |
| systemd unit SHA-256 | `d5afc4c2228a78f02ffd7be07775e7c53acda90b8c2b1b3581d64020537188b6` |
| Deployment identity | `bdda546432bcaf6d29f281cd2a281b4d684bc447b2fafa382ffd1948f39a107f` |

### Clean 24-hour non-formal closure

Final independent verdict:
`A — CLEAN_24H_PASS_CURRENT_NONFORMAL_STAGE_COMPLETE`.

```text
BOOT_ID=f2720022-bc39-4e22-bc68-af6bfce92274
PID=157507
PROC_STARTTIME=7023878
T0=2026-09-03T06:10:44.357173Z
T1=2026-09-04T06:10:51.870564Z
DURATION_SECONDS=86408.67870779098
EVIDENCE_ARCHIVE=CURRENT-MAIN-24H-CLEAN-NONFORMAL-EVIDENCE-20260904T062029Z.tar.gz
EVIDENCE_ARCHIVE_SHA256=1ae0001eec78b6aebff2b45183a4191ac7bfcb99b2b9e10588dc3be635a50946
RESULT_SHA256=c08ba6299390b92f0f2687c14177682c05c34781bc4ae020710e51f7056da9d9
```

Core result: Spot gaps `0`; USD-M gaps `0`; order-book resync `2`; recovered
discontinuities `22`; unresolved discontinuities `0`; stream-attempt terminal
`2`, both subsequently recovered; 22 discontinuity START identities matched
22 COMPLETE identities; Catalog/SQLite errors `0`; backpressure timeouts `0`;
swap `0`; OOM `0`; final readiness `YES`; final books synchronized `YES`.
This is non-formal evidence and must not be labeled Formal M22.9 evidence.

Accepted non-blocking watches for future multi-symbol qualification are
capacity cadence `PASS_WITH_JITTER_WATCH` (full-window p99 about `16.0819 s`,
max `27.3311 s`, zero events over 30 s) and RSS
`EARLY_GROWTH_THEN_PLATEAU` (T0 `270000128`, 12h `335282176`, T1 `368766976`,
maximum `377339904` bytes; final 18–24h approximately flat).

```text
SEVENTY_TWO_HOUR_BURNIN_REQUIRED_BEFORE_MULTI_SYMBOL=NO
ONE_SIXTY_EIGHT_HOUR_BURNIN_REQUIRED_BEFORE_MULTI_SYMBOL=NO
PERFORMANCE_ENGINEERING_REOPENED=NO
```

## C. Implemented configurable-product runtime and next qualification

ADR-0032 supersedes ADR-0031. The future target remains Binance-specific with
Spot and USD-M perpetual markets, but the operator explicitly configures a
finite symbol list independently for each market. There is no fixed symbol
allowlist, automatic all-symbol discovery, exchange/plugin framework, or hot
runtime topology reload. Configuration changes take effect on normal process
restart. See
[`docs/adr/0032-configurable-product-set.md`](adr/0032-configurable-product-set.md).

The implemented surface is `[recorder]` with `spot_symbols = [...]` and
`usdm_symbols = [...]`. Legacy compatibility mode applies only when both fields
are absent, resolving to BTCUSDT in both markets. If either field is present,
explicit product-selection mode applies: supplied lists are exact and an
omitted sibling resolves to an empty list; both resolved lists empty is invalid.
MS2 assembles one Collector per configured ProductKey in one process.
It passed offline acceptance on current main; MS3-B was then merged through PR
#56 with the production-path tests and CI1/CI2 repairs. MS4-B stopped-deployment
review is complete. The original owner-authorized MS4-C window remains
`EXECUTED_PARTIAL_NOT_ACCEPTED` because its recovery gate was not run in that
window. The later R3 `NONFORMAL_MS4_RECOVERY_SUPPLEMENT` passed its bounded
recovery and sealed/Catalog audit, closes the recovery gate for review, and
grants zero Formal M22.9 duration credit; MS4-D records the bounded
four-ProductKey qualification as reviewed complete.

In explicit Spot-only mode, the resolved USD-M set is empty: no USD-M
Collectors, product-specific side-data managers, process-global USD-M
side-data owner, REST polling, or WebSocket collection are instantiated. An
empty Spot set similarly creates no Spot Collector or Spot side-data traffic.
The process-global USD-M side-data owner exists only when a USD-M ProductKey is
configured and at least one global kind is enabled. The legacy
`GLOBAL_SIDE_DATA_SYMBOL="BTCUSDT"` sentinel never creates a core ProductKey.
The accepted sequence is MS2 configurable product runtime, MS3 shared-resource
scaling/rotation/observability, then MS4 configurable-product integration and
bounded live qualification.

## Boundaries that remain frozen

MS1 uses durable discontinuity identity `(market, symbol, stream)` with
`gap_id` for lifecycle matching and `(kind, symbol)` for symbol-specific side
data cursors. Legacy single-symbol Catalog identity is migrated to `BTCUSDT`
atomically, idempotently, restart-safely, and fail-closed; historical
symbol-less seal intents are not rewritten. Global side data remains global.

Raw v1 framing and payload bytes are unchanged. External Contracts are
unchanged. No account endpoints, credentials, orders, trading, other
exchanges, or arbitrary-symbol framework are authorized.

## Formal and deployment status

```text
STOPPED_DEPLOYMENT_INSTALLED=YES
LIVE_START_AUTHORIZED=NO
DEPLOYMENT_AUTHORIZED=NO
FORMAL_M22_9_STARTED=YES
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
12H=NOT_STARTED
PRODUCTION_READY=NO
P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES
CURRENT_MAIN_DEPLOYED=NO
CURRENT_MAIN_DEPLOYMENT_REASON=REVIEWED_FIX_SOURCE_NOT_INSTALLED
M22_9_P2=REVIEWED_COMPLETE
M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE
ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_FIX=REVIEWED_COMPLETE
ARCHIVE_TIMER=ENABLED_ACTIVE
RECORDER=STOPPED
NEXT=EXACT_ARTIFACT_REDEPLOY_PREFLIGHT
REDEPLOY_RETRY_AUTHORIZATION=SEPARATE_AUTHORIZATION
```

Here `DEPLOYMENT_AUTHORIZED=NO` means future live deployment/start
authorization is absent. Any future live traffic or Formal stage requires the
exact P2 deployment source/artifact, immutable Wheel, lock, config, unit, and
deployment identities, followed by explicit separate authorization and a fresh
readiness/capacity decision. Historical single-symbol and non-formal duration
credit does not transfer.

## Next action

NEXT=EXACT_ARTIFACT_REDEPLOY_PREFLIGHT

MS3 is closed/merged, MS4-A is reviewed complete, and MS4-B stopped-deployment
review is complete. MS4-C is reviewed complete only through the separate,
non-formal R3 recovery supplement; the original two-hour window remains
`EXECUTED_PARTIAL_NOT_ACCEPTED` and receives no transferred duration credit.
MS4-D has closed the bounded four-ProductKey qualification. M22.9-P1 is the
reviewed-complete documentation-only systemd-detached observation preparation.
M22.9-P2 is reviewed complete: the exact P2 deployment source/review base was
installed and verified, the registered archive was drained and fully verified,
and bounded non-formal readiness/interaction completed before Recorder was
stopped. `CURRENT_MAIN_DEPLOYED=NO`; the current engineering source is not
installed.
The installed P2 artifact remains the evidence basis for the failed attempt,
but it is not eligible for retry until the reviewed observer fix is built into
a later exact artifact and that artifact is separately authorized and
deployed. The Formal
2-hour stage was attempted once and failed at T0 before the first observer sample; its
stage-start evidence and post-stop forensic review are recorded in
[`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md). The affected
Catalog/manifest findings support, but do not prove, a concurrent archive
snapshot race. The offline fix is complete; no redeploy or retry is authorized
by this document. The next gate is exact-artifact redeploy preflight;
redeploy/retry authorization is separate.
