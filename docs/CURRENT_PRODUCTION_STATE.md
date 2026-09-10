# Current Production State

This is the concise current authority. It separates GitHub engineering source,
the independently qualified deployed artifact, and the future multi-symbol
program. Verify live GitHub before acting; this document does not authorize
deployment, live traffic, formal acceptance, or data retirement.

## Current stage — MS4-B target preflight/stopped deployment

MS4-A local preparation and review remain complete. MS4-B target preflight,
rollback evidence preservation, exact frozen-source Linux artifact build,
complete four-ProductKey configuration, stopped systemd deployment and
deployment identity verification are ready for independent review. The Tokyo
Recorder was gracefully stopped before mutation and remains stopped. No live
capture, Binance qualification traffic, controlled recovery, or Formal M22.9
was started. `PRODUCTION_READY=NO` and MS4-C remains not started.

MS4_A=REVIEWED_COMPLETE; MS4_B=STOPPED_DEPLOYMENT_READY_FOR_REVIEW;
NEXT=MS4_B_TARGET_PREFLIGHT_STOPPED_DEPLOYMENT_REVIEW.

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
NO. NEXT=MS4_A_LOCAL_PREPARATION.

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

## Status at a glance

```text
LIVE_GITHUB_MAIN=bb8c93ba63c23adaf2cb0288b6ea127030b89e58
LIVE_GITHUB_MAIN_TREE=8770f48d458dc4e35d813e8db8dc2009ff78889c
MS4_DEPLOYMENT_SOURCE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
MS4_DEPLOYMENT_SOURCE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
POST_MS1_IMPLEMENTATION_AUTHORITY_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
POST_MS1_IMPLEMENTATION_AUTHORITY_TREE=95f16f05b30b7db23e43ebb6439ed0d055081902
MS1_MERGE_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
MS1_STATUS=MERGED
MS1_PR=51
MS1_POST_MERGE_CI_RUN=33955915046
MS1_POST_MERGE_CI_PASS=YES
CURRENT_MAIN_DEPLOYED=NO
CURRENT_MAIN_SHA=bb8c93ba63c23adaf2cb0288b6ea127030b89e58
CURRENT_MAIN_TREE=8770f48d458dc4e35d813e8db8dc2009ff78889c
CURRENT_NONFORMAL_VALIDATION_STAGE_COMPLETE=YES
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
MS4=IN_PROGRESS_MS4_B_STOPPED_DEPLOYMENT
MS4_A=REVIEWED_COMPLETE
MS4_B=STOPPED_DEPLOYMENT_READY_FOR_REVIEW
MS4_C=NOT_STARTED
NEXT=MS4_B_TARGET_PREFLIGHT_STOPPED_DEPLOYMENT_REVIEW
FORMAL_M22_9=NOT_STARTED
PRODUCTION_READY=NO
```

## Current MS3-B merged authority

MS2 implementation, offline acceptance, independent review, and merge are
closed on current `main` via PR #54. MS3-A is merged via PR #55 at
`01527037254595267003f886689bb270e08b5e5d`, tree
`eb63b645660a64ac606341ce3fb7f447a6e89457`, with merge parents
`52bf086dd240556b054821f33bf1e2840fdcf912` and
`ad1e941af3cd2bd3922eac239ba92a47058e9875`. MS3-B candidate evidence is
merged via PR #56 at `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`, tree
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

## A. Verified pre-MS2 main and post-MS1 implementation/behavior authority

| Item | Authority |
| --- | --- |
| Live GitHub `main` at MS3-B start | `01527037254595267003f886689bb270e08b5e5d`; tree `eb63b645660a64ac606341ce3fb7f447a6e89457` |
| Post-MS1 implementation/behavior authority | `d38180074b5f76ab6b7778eea7fc505160c671ae` |
| Post-MS1 implementation tree | `95f16f05b30b7db23e43ebb6439ed0d055081902` |
| MS1 merge | `d38180074b5f76ab6b7778eea7fc505160c671ae` |
| Merge parents | `c421605e302d2ad46acdb2466627f64644181c9a`, `11e100fbcb974e7d54f0515c99e08ac6042b9204` |
| MS1 | merged via PR #51; reviewed head `11e100fbcb974e7d54f0515c99e08ac6042b9204` |
| Post-merge CI | `offline-ci` run `33955915046`, push event, macOS and Ubuntu Python 3.12 jobs successful |
| Deployment | live GitHub `main` is not deployed |

MS1 is the merged multi-symbol durable identity foundation. Its three
implementation commits remain provenance, not separate current authorities:
`026e357eb9af9b5b9fd111872dc6dcc30e9c599d`,
`39fbd04172a6b5b27b41d43c57d0e5ff575b95d4`, and
`11e100fbcb974e7d54f0515c99e08ac6042b9204`.

The last behavior-changing authority before MS2 was the MS1
merge `d38180074b5f76ab6b7778eea7fc505160c671ae`, with tree
`95f16f05b30b7db23e43ebb6439ed0d055081902`. A documentation-only descendant
may make live GitHub `main` newer without changing that implementation or
behavior authority. At takeover, verify live `main`, confirm this SHA remains
an ancestor, and inspect every later commit. If any later commit changes
source or behavior, stop and establish the new implementation authority before
MS2.

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
#56 with the production-path tests and CI1/CI2 repairs. Live qualification
remains pending, and MS4-A is the next separately authorized milestone.

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
DEPLOYMENT_AUTHORIZED=NO
FORMAL_M22_9_STARTED=NO
PRODUCTION_READY=NO
```

Any MS4 deployment requires a newly frozen source, immutable Wheel, lock,
config, unit, and deployment identities, followed by separate authorization
and a fresh bounded qualification. Historical single-symbol duration credit
does not transfer to a behavior-changing multi-symbol artifact.

## Next action

NEXT=MS4_B_TARGET_PREFLIGHT_STOPPED_DEPLOYMENT_REVIEW

MS3 is closed/merged and MS4-A is reviewed complete. Review the MS4-B target
preflight/stopped deployment evidence as a separate gate. Do not start the
Recorder or run live qualification; MS4-C requires its own pre-start review
and authorization.
