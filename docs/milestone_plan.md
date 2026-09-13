# Milestone Plan

## Current milestone — M22_9 Formal 2-hour quiet-window retry closeout (2026-09-13)

The owner-authorized retry reached a canonical final after
`7677610836692` BOOTTIME ns and 14 samples, but it is not eligible. Full-state
observer samples grew to approximately 223 MB; the observation/audit/publication
cycle exceeded the 600-second cadence and the final retained
`acceptance_observation_gap`. Recorder process, service instance, boot ID, and
deployment identity remained stable with `NRestarts=0`, so this is an
acceptance-evidence scalability defect rather than a demonstrated Recorder
continuity failure.

Controlled closeout stopped and disabled Recorder, drained archive backlog and
pending to zero, verified Catalog `ok` with no active partials, retained the
archive timer enabled/active, and restored normal OS update authority. Formal
credit remains zero and 12 hours is not started.

```text
MILESTONE=M22_9_FORMAL_2H_QUIET_WINDOW_RETRY_CLOSEOUT
MILESTONE_STATUS=REVIEWED_COMPLETE
FORMAL_M22_9_2H_QUIET_WINDOW_RETRY=EXECUTED_INCOMPLETE_OBSERVATION_GAP
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
RECORDER_ENABLED=NO
ARCHIVE_TIMER=ENABLED_ACTIVE
OS_UPDATE_AUTHORITY=RESTORED
12H=NOT_STARTED
PRODUCTION_READY=NO
RETRY_ELIGIBLE=NO
NEXT=M22_9_ACCEPTANCE_OBSERVER_BOUNDED_EVIDENCE_FIX
```

The next milestone is a focused bounded-evidence repair: stop repeating the
full Catalog/reconnect/manifest inventory in every sample and verify completed
chains with bounded memory, without weakening immutable hashes, chain
continuity, lifecycle evidence, or fail-closed eligibility. A new live retry
is not eligible until that artifact is reviewed and deployed. See
[`M22.9 Formal 2-hour quiet-window retry closeout`](milestone_acceptance/M22.9-formal-2h-quiet-window-retry.md).

## Previous milestone — M22_9 host-maintenance quiet-window preflight (2026-09-13)

The restart-from-scratch preflight is complete. It installed the six pending
Ubuntu updates before any T0, established zero pending upgrades with a clean
dpkg audit and no reboot requirement, and tested the bounded runtime-only
maintenance exclusion. Both apt timers, both apt services, and
`unattended-upgrades.service` were inactive and `masked-runtime`; an explicit
start was rejected and the 60-second hold observed no boot change or systemd
reexecution. All runtime masks were then removed and the normal enabled/active
update authorities were restored.

Recorder remained inactive and disabled. No observer or Formal stage was
created. The archive timer remains enabled/active; Catalog integrity is `ok`,
active partial and archive backlog/pending counts are zero, and the registered
target is READY.

```text
MILESTONE=M22_9_HOST_MAINTENANCE_QUIET_WINDOW_PREFLIGHT
MILESTONE_STATUS=COMPLETE
HOST_MAINTENANCE_QUIET_WINDOW_PREFLIGHT=PASS
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
RECORDER_ENABLED=NO
ARCHIVE_TIMER=ENABLED_ACTIVE
OS_UPDATE_AUTHORITY=RESTORED
PENDING_UPGRADES=0
REBOOT_REQUIRED=NO
12H=NOT_STARTED
PRODUCTION_READY=NO
RETRY_ELIGIBLE=YES_CONDITIONAL_ON_FRESH_PRESTART_GATES
NEXT=FORMAL_M22_9_2H_QUIET_WINDOW_RETRY
```

The next milestone is exactly one new Formal 2-hour retry. Before its T0, the
operator repeats the package/lock and runtime-mask gates, explicitly
enables/loads the Recorder unit, verifies the unchanged installed artifact,
and obtains fresh readiness, archive and capacity evidence. It creates one new
acceptance root, never resumes either prior incomplete stage, restores update
authority on controlled exit, and does not start 12 hours automatically. See
[`M22.9 host-maintenance quiet-window preflight`](milestone_acceptance/M22.9-host-maintenance-quiet-window-preflight.md).

## Previous milestone — M22_9 VPS root-home recovery and handoff (2026-09-12)

The first host-maintenance quiet-window preflight attempt is aborted and
unaccepted. An unsafe remote cleanup expanded an unset target to `/root/`,
removed root-home contents and SSH authorization, and invalidated the ongoing
preflight shell/workspace. GreenCloud password reset plus temporary VNC
restored access without a rebuild. The root home and dedicated SSH key are
re-established, the temporary recovery key is removed, and VNC disablement is
operator-confirmed.

The preceding reboot also demonstrated that a stopped-but-enabled Recorder
will auto-start. It ran for approximately six minutes, stopped successfully,
created no observer or Formal stage, and earns zero credit. Recorder is now
inactive and disabled; archive scheduling stays enabled/active. Installed
deployment files, active data, Catalog, and archive survived, while prior
material stored only under `/root` is unavailable.

```text
MILESTONE=M22_9_VPS_ROOT_HOME_RECOVERY_AND_HANDOFF
MILESTONE_STATUS=COMPLETE
HOST_MAINTENANCE_QUIET_WINDOW_PREFLIGHT=ABORTED_UNACCEPTED
FORMAL_M22_9_2H_RETRY=EXECUTED_INCOMPLETE_HOST_MAINTENANCE_INTERRUPTED
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
RECORDER_ENABLED=NO
ARCHIVE_TIMER=ENABLED_ACTIVE
12H=NOT_STARTED
PRODUCTION_READY=NO
RETRY_ELIGIBLE=NO
NEXT=FORMAL_M22_9_HOST_MAINTENANCE_QUIET_WINDOW_PREFLIGHT_RESTART_FROM_SCRATCH
```

The next milestone must begin with a fresh evidence root and a reviewed
bounded procedure. It finishes pending package work before T0, proves the
maintenance quiet window and restoration of normal update authority, and
keeps Recorder stopped/disabled. It must not reuse the two aborted evidence
roots, create a Formal T0, or start 12 hours. See the
[recovery and handoff acceptance record](milestone_acceptance/M22.9-vps-root-home-recovery.md).

## Previous milestone — M22_9 Formal 2-hour retry closeout (2026-09-12)

The exact-artifact retry established a valid T0 and ten samples, then became
incomplete when the VPS unattended glibc/Python upgrade reexecuted systemd and
externally restarted both Recorder and its transient observer at approximately
85 minutes. No finalized 7,200-second evidence chain exists, so the retry earns
zero duration credit and does not unlock 12 hours.

```text
MILESTONE=M22_9_FORMAL_2H_RETRY_CLOSEOUT
MILESTONE_STATUS=REVIEWED_COMPLETE
FORMAL_M22_9_2H_RETRY=EXECUTED_INCOMPLETE_HOST_MAINTENANCE_INTERRUPTED
FORMAL_M22_9_CREDIT_SECONDS=0
DEPLOYED_RUNTIME_SOURCE_SHA=e267ae38bdbb206c8f54dcb5fa338b8f1c54c61d
CURRENT_MAIN_DEPLOYED=NO
CURRENT_MAIN_DEPLOYMENT_REASON=DOCS_ONLY_CLOSEOUT_DESCENDANT_NOT_INSTALLED
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
12H=NOT_STARTED
PRODUCTION_READY=NO
RETRY_ELIGIBLE=NO
NEXT=FORMAL_M22_9_HOST_MAINTENANCE_QUIET_WINDOW_PREFLIGHT
```

The current milestone ends with Recorder stopped and archive/Catalog state
healthy. The next milestone is a narrowly scoped host-maintenance quiet-window
preflight: it must establish how pending unattended package work and systemd
service reexecution are excluded from an authorized measurement window, test
that procedure, preserve security update authority, and stop. It must not start
another Formal T0.

Detailed evidence is in
[`M22.9 Formal 2-hour retry closeout`](milestone_acceptance/M22.9-2h-retry.md).

## Previous milestone — M22_9 exact-artifact stopped redeploy and readiness (2026-09-12)

At that milestone, the exact current main source
`e267ae38bdbb206c8f54dcb5fa338b8f1c54c61d` / tree
`e968ede54d9f110ef7371a4847a54940177a1a19` was installed with wheel SHA-256
`9bb924ad7cc38466d2b79c291f1b864890d5d071413dce1047eaf06d76532fdb`.
Deployment identity
`582bf645dea0c6ad2c409880d44a68b97a55ddbe0c9bfb68d930e12daa0d75a6`
and retained P2 rollback compatibility both verified. Recorder reached READY
for all four ProductKeys and 12 core contexts; the archive timer was active. No
Formal stage child or T0 had been created.

```text
MILESTONE=M22_9_EXACT_ARTIFACT_STOPPED_REDEPLOY_AND_READINESS
MILESTONE_STATUS=COMPLETE
CURRENT_MAIN_DEPLOYED=YES
RECORDER=RUNNING_READY
ARCHIVE_TIMER=ENABLED_ACTIVE
FORMAL_M22_9_2H_RETRY=NOT_STARTED
FORMAL_M22_9_CREDIT_SECONDS=0
12H=NOT_STARTED
PRODUCTION_READY=NO
NEXT=FORMAL_M22_9_2H_RETRY_START
```

The detailed authority is
[`M22.9 exact-artifact stopped redeploy and readiness`](milestone_acceptance/M22.9-exact-artifact-redeploy-readiness.md).
The next milestone starts exactly one new detached Formal 2-hour observer from
the recorded readiness evidence; it must not start 12 hours.

## Previous milestone — M22_9 exact-artifact redeploy preflight (2026-09-12)

This bounded milestone preflights, but does not authorize, redeployment of the
exact GitHub main candidate. The authoritative candidate is commit
`e267ae38bdbb206c8f54dcb5fa338b8f1c54c61d`, tree
`e968ede54d9f110ef7371a4847a54940177a1a19`; existing main CI is green. On the
VPS it was cloned into a fresh clean detached workspace, built once with Python
3.12.3 in a fresh isolated build environment, and staged into a uniquely named
venv outside `/opt`. The exact runtime lock was installed with
`--require-hashes`, the wheel with `--no-deps`, and pip, dependency, RECORD, and
offline CLI checks passed.

The read-only gate and final post-staging snapshot both show Recorder
inactive/dead with PID 0, the archive timer enabled/active/waiting, zero active
partial files, READY registered archive target
`ef852751-721c-4145-9083-f6fd48718480`, zero archive backlog and remote
pending, Catalog integrity OK, current P2 deployment VERIFIED, and zero legacy
reconnect ambiguity/conflict/contradiction/degraded-authority findings. Active
and archive capacity both pass the conservative 2-hour projection plus safety
margin. Only the private evidence root and disposable staging area were
written; the installed deployment and archive timer were not changed.

```text
MILESTONE=M22_9_EXACT_ARTIFACT_REDEPLOY_PREFLIGHT
MILESTONE_STATUS=PREFLIGHT_COMPLETE
GITHUB_MAIN_SHA=e267ae38bdbb206c8f54dcb5fa338b8f1c54c61d
GITHUB_MAIN_TREE=e968ede54d9f110ef7371a4847a54940177a1a19
OLD_P2_ARTIFACT_INSTALLED=YES
CURRENT_MAIN_DEPLOYED=NO
NEW_CANDIDATE_STAGED=YES
NEW_CANDIDATE_DEPLOYED=NO
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
NEW_FORMAL_RUN_STARTED=NO
12H=NOT_STARTED
PRODUCTION_READY=NO
NEXT=EXACT_ARTIFACT_STOPPED_REDEPLOY_AND_READINESS
DEPLOYMENT_AUTHORIZATION=NOT_GRANTED
LIVE_RUN_AUTHORIZATION=NOT_GRANTED
```

The evidence root is
`/srv/recorder-data/recorder-archive/evidence/M22.9-redeploy-preflight-20260912T022735Z`;
its root-controlled `SHA256SUMS` digest is
`44254c9a609e19e8504ca2e57114c1aab81ca7150d710f43382db6f4e0e6dd2f`. The
complete gate and digest record is
[`M22.9 exact-artifact redeploy preflight`](milestone_acceptance/M22.9-exact-artifact-redeploy-preflight.md).

The previous Formal 2-hour failed-at-T0 record remains immutable and has zero
credit. No new Formal run started and Production Ready remains NO. The local
branch is based exactly on the candidate; no GitHub state was mutated and the
six unrelated untracked artifacts remain excluded. The next milestone is
`EXACT_ARTIFACT_STOPPED_REDEPLOY_AND_READINESS`, which still requires its own
authorization boundary.

## Previous local milestone — M22_9 post-merge macOS CI reconnect-layout repair (2026-09-12)

This narrowly scoped milestone records a legal reconnect-boundary layout
assumption in the Spot and USD-M backpressure tests. GitHub main push
offline-ci run `34663566549` failed only macOS Python 3.12 job
`103470852952`; macOS pytest reported `1 failed, 1651 passed, 24 skipped, 4
deselected` at the old fixed `len(old_manifests) == 1` assertion in the Spot
session-restart post-close timeout test. Ubuntu job `103470853142` passed all
gates. PR #68 exact-head run `34663191565` passed both macOS and Ubuntu, and
twenty local exact Spot-node repetitions on macOS arm64 Python 3.12.14 passed.

The diagnosis is schedule sensitivity, not a production regression. The
existing `assert_old_ingress_boundary_layout` helper already specifies the
legal result: zero or one earlier ordinary complete manifest followed by
exactly one incomplete `reconnect_gap` manifest, preserving the original
connection identity and exact source payload prefix. The USD-M session-restart
test contained the same brittle assumption and is repaired symmetrically.
Each test now passes the lifecycle STARTED evidence's exact
`original_connection_id` to that helper. All existing boundary hash,
missing-frame, gap-id, recovery, completion-after-sync, ordering,
no-fabrication, and fail-closed checks remain. No production code or public
contract changes. Independent lead review found P0=0, P1=0, and P2=0; the
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

Validation passed with 20 repetitions of both repaired nodes (40 passed), the
complete Spot/USD-M ingress files (50 passed, 1 deselected), the full offline
suite (1,652 passed, 24 skipped, 4 deselected, 13 existing warnings), Ruff,
strict MyPy, M0 contracts, Go Raw golden, build, and clean-wheel CLI smoke.
The prior observer/archive fix remains a previous reviewed milestone. This
milestone did not access or mutate the VPS; its last authoritative state stays
stopped with the archive timer enabled/active. See the [acceptance record](milestone_acceptance/M22.9-main-macos-ci-reconnect-layout.md).

After GitHub merge, the next milestone remains
`EXACT_ARTIFACT_REDEPLOY_PREFLIGHT`; this record authorizes no deployment or
retry.

## Previous local milestone — AcceptanceObserver/archive concurrency fix reviewed complete (2026-09-12)

The bounded offline diagnosis and fix for the AcceptanceObserver/archive
concurrency race is complete. The implementation freezes a Catalog lifecycle
boundary before the long filesystem scan, performs one authoritative
membership comparison from that boundary, defers later rows, and re-reads
fresh exact-chunk archive lifecycle state when local Raw disappears. Existing
archive verification and fail-closed semantics remain unchanged. Deterministic
offline tests cover post-boundary deferral, ArchiveManager-held
`LOCAL_DELETE_PENDING`, validation-time disappearance, unauthorized absence,
external corruption, and observer read-only behavior.
Independent Luna Max review of exact code-review commit
`31cabe4445ee699ad284aa707d24333c78cf8d21` against base
`e214120a25a5aff28fad4903c9510920a25738d3` found P0=0, P1=0, and P2=0.

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

The new acceptance record is
[`M22.9 observer/archive concurrency fix`](milestone_acceptance/M22.9-observer-archive-concurrency-fix.md).
The next gate is exact-artifact redeploy preflight; any redeploy or Formal
retry requires separate authorization.

## Historical Formal M22.9 2h failed closeout — reviewed complete (2026-09-11)

The owner-authorized Formal 2-hour attempt on greencloud-tokyo-01 failed
before the first observer sample. T0 was
`2026-09-11T13:41:32.831837Z` / `1789134092831837314` ns, with BOOTTIME
`789457702764089` and boot ID
`f2720022-bc39-4e22-bc68-af6bfce92274`. The detached observer unit
`binance-market-data-acceptance-2h-20260911T132845Z-5b4fd719.service`
failed with exit 1, InvocationID
`ee05ad7a73f04dec867629767439bc82`.

Formal failed root:
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T132845Z-5b4fd719-646792f2`.
Stage root:
`.../2h-a8b45b7e1da640c497213b172c235815`. Immutable stage-start SHA-256:
`1c35f62e0673488c441374971e8e8552212e9693aacfba35820ea613ae4a08f6`.
Stage-start result was FAIL with 24 blockers: 23 Catalog/manifest
disagreements and one unexplained raw absence. The separate pre-T0 aborted
setup root
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T130258Z-646792f2`
is preserved and receives no credit; it is not the Formal failed T0.

The quiescent exact-chunk forensic review covered 26 unique IDs from those
findings. Every row currently resolves to verified archive state with Catalog
`LOCAL_DELETED`, retained internal manifest, absent internal sealed Raw, and
present verified archive Raw/manifest. The 26 archive transactions and local
source retirements occurred after T0. This supports, but does not prove, a
concurrent AcceptanceObserver filesystem/Catalog snapshot race; there is no
per-read interleaving trace. The installed read-only post-stop audit completed
over 112,817 manifests in 115.95 seconds with zero Catalog findings, zero
integrity findings, and zero chunks with scan issues. Additive correction
evidence preserves the original closeout and supersedes only its mistaken
no-result statement.

The failed-stage closeout evidence is in the formal root under
`operator-evidence/closeout-review/`; its exact hashes and final VPS state are
recorded in [`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md).

```text
M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
12H=NOT_STARTED
PRODUCTION_READY=NO
```

This historical closeout remains unchanged. The offline fix is recorded in the
current section above; redeploy and Formal retry remain separately authorized
gates.

## Historical CI repair — Spot ingress gap manifest layout (2026-09-12)

The failed push run was `34615091958`, Ubuntu job/node `103314892201`. Only
pytest failed, at
`tests/integration/test_spot_ingress_backpressure.py::test_sustained_saturation_preserves_boundary_and_publishes_gap[agg_trade-agg_trade]`,
where the test incorrectly required every gap manifest to contain
`sequence_gap`; the result was `1 failed, 1636 passed`. The same code passed
PR run `34615041520` and parent push run `34597348895`; merge `#66` changed no
`src`, tests, dependencies or workflow, and macOS passed. A read-only Luna
diagnosis ran the exact node 21 times on macOS arm64 / Python 3.12.9; all
passed.

Root cause: stable-phase 60-second rotation can place the writer deadline at
the boundary. After the old boundary frame has persisted its `sequence_gap`,
the writer may rotate before `close_and_seal`; the pending reconnect seal
intent then creates a valid zero-record `reconnect_gap` marker. The old
manifest-wide assertion rejected that legal layout. The test-only repair
monkeypatches `RawChunkWriter.should_rotate` to make the real deadline
decision exactly once after the first persisted `sequence_gap`, without
sleeping, and validates every gap manifest as either a non-empty exact
`sequence_gap` manifest backed by a flagged Raw frame or an exact empty
`reconnect_gap` marker. Forced cases require the hook and one marker.

Local validation for this repair: the forced boundary proof passed both stream
variants and showed two non-empty `sequence_gap` manifests plus one
zero-record, empty-connection `reconnect_gap` manifest per case; 20 complete
repetitions covered all four normal/forced stream cases (80 passed); the four
focused files passed 80 tests; and the full offline suite passed 1,643 tests,
with 24 skipped, 4 deselected and 13 existing fork warnings. Ruff, strict
MyPy (254 files), M0 contracts, Go Raw golden, `compileall`,
`build --no-isolation`, `git diff --check`, and clean-wheel CLI smoke passed.
No runtime, public contract, dependency, workflow or deployment behavior
changed; no VPS, online, or GitHub operation was performed. The current
production disposition and Formal credit are unchanged.

NEXT=ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST

## P2 deployment basis — reviewed complete (2026-09-11)

M22.9-P2 executed the exact deployment, archive, and capacity preflight. The
installed source is the P2 deployment source/review base
`646792f2e5fc5b7195ea58541d3f1dfda6555b7f` (tree
`c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c`). The canonical four ProductKeys
reached readiness with 12 core stream contexts, then Recorder was gracefully
stopped. The registered 2 TB archive target was drained through the existing
verified ArchiveManager/Catalog path; full verification reported 112,570
verified files, zero failed, and zero pending. The managed archive timer is
enabled and active/waiting with a future monotonic trigger. At P2 completion
Formal M22.9 was unstarted; the later Formal 2-hour attempt is recorded in
the current closeout section above.

The evidence bundle is
`/srv/recorder-data/recorder-archive/evidence/M22.9-P2-20260911T090741Z`;
its `EVIDENCE_SUMMARY.md` and `SHA256SUMS` hashes are recorded in the P2
acceptance file. The conservative 24-hour generation rate is
`349910.017730 B/s`; the 278-hour archive projection leaves about 1.772 TB of
archive margin, while active-root runway above the 10 GiB reserve is about
26.46 hours. P2 was non-formal and is reviewed complete; the subsequent
Formal 2-hour attempt failed at T0 with zero credit, while Production Ready
remains NO.

M22_9_P2=REVIEWED_COMPLETE
M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE
P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES
CURRENT_MAIN_DEPLOYED=NO
CURRENT_MAIN_DEPLOYMENT_REASON=DOCS_ONLY_DESCENDANT_NOT_INSTALLED
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
FORMAL_M22_9=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
12H=NOT_STARTED
PRODUCTION_READY=NO
NEXT=ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST
REDEPLOY_RETRY_AUTHORIZATION=SEPARATE_AUTHORIZATION

### Historical pre-P2 disposition (preserved)

MS3 implementation is merged; MS4-A local preparation and the MS4-B target
preflight/stopped-deployment work package are reviewed complete. The
owner-authorized MS4-C attempt started the reviewed artifact, reached all-four
ProductKey readiness, and completed its bounded two-hour steady interval. That
original window remains `EXECUTED_PARTIAL_NOT_ACCEPTED` because controlled
recovery was not executed in the approved window. The separate passing
2026-09-11 `NONFORMAL_MS4_RECOVERY_SUPPLEMENT` closes the MS4-C recovery gate
for review with zero Formal M22.9 credit; it does not retroactively grant the
original window duration. The Recorder is now stopped, the archive timer is
disabled, and the MS4-D review closes the bounded qualification. The full
Formal M22.9 chain remains separate and unstarted. The current M22.9-P1
package documents systemd-detached execution for one explicitly selected stage
while reusing the existing foreground acceptance observer; it is reviewed
complete and did not deploy Recorder, enable archive, or start Formal M22.9.
PR #57 and PR #58 are historical documentation closeouts. PR #60 is the
historical MS4-C review-start merge through the normal repository rule as
`efae0135ed5272d18d800af0ac247b70ece07422` (tree
`53342ac880cc36d65eba6f5e9b49fa722cc9d56b`); exact-head CI run `34478654689`
passed. The historical MS4-D documentation review was based on `main`
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` (tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`), which includes the merged
MS4-C evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d`; base CI run `34551834444`
completed successfully. Neither review authority is a deployment
authorization. P1 was based on main at its historical checkpoint
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`.

The runbook now consistently references the retained lock/Wheel, avoids moving
an installed venv, separates non-formal MS4 evidence from M22.9, and leaves
machine-specific remote archive commands for the separately authorized MS4-C
target. These are documentation corrections, not production changes.

MS4_A=REVIEWED_COMPLETE
MS4_B=REVIEWED_COMPLETE
MS4_C=REVIEWED_COMPLETE
RECOVERY_GATE=REVIEWED_COMPLETE
MS4_D=REVIEWED_COMPLETE
MS4=REVIEWED_COMPLETE
BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS
QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE
M22_9_P1=REVIEWED_COMPLETE
FORMAL_M22_9=NOT_STARTED
FORMAL_M22_9_CREDIT_SECONDS=0
PRODUCTION_READY=NO
CURRENT_MAIN_DEPLOYED=NO
RECORDER=STOPPED
NEXT=M22_9_P2_EXACT_DEPLOYMENT_ARCHIVE_CAPACITY_PREFLIGHT

## Multi-symbol execution ledger — 2026-09-10

This section implements the owner's Luna-max implementation / Astra review
workflow. It is a planning and MS3 review update, not MS4 deployment authority.
Use this document as the single task queue; acceptance files hold evidence,
CURRENT_PRODUCTION_STATE holds deployed facts, and PROJECT_HANDOFF points here.

### MS3-CI2 — macOS normal rotation assertion repair (closed/merged)

Merged repair prepared by GPT-6 Astra on base `e6dc3a993defa2c6f24af25209090a55deda2381`.
Run `34424559261`: Ubuntu job `102706917778` passed all gates, including
Profile D, build and clean-wheel smoke. macOS job `102706918017` failed a
DIFFERENT test: `test_global_stop_post_close_timeout_does_not_fabricate_reconnect_gap`
in `tests/integration/test_usdm_ingress_backpressure.py:1070`, expecting one
manifest but observing two (1 failed, 1639 passed, 24 skipped, 4 deselected).
Profile D passed in both jobs. Do not repeat the CI1 repair or attribute this
failure to the executor without evidence.

The test uses 60-second stable-phase Raw rotation. The next boundary can be
arbitrarily close to writer creation; a short test may legitimately seal more
than one chunk. Astra reproduced the exact 2 != 1 failure by injecting a near
rotation deadline. The minimal test-only repair validates every manifest as
complete/no-gap, aggregate record counts, the nonempty exact source prefix and
absence of discontinuity events. A parametrized boundary case exercises the
real should_rotate decision at its deadline, without sleeping for a phase.
No production logic, rotation policy, public contracts or CI configuration changes.

MS3-CI2=CLOSED
MS3=CLOSED_MERGED
MS3_R1=CLOSED
MS3_CI1=CLOSED
MS3_CI2_REVIEWED_HEAD=2a701fe79b78d3c63dd5959efecd20a2369d58e5
MS3_CI2_REVIEWED_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
MS3_CI2_CI_RUN=34436773366
MS3_CI2_MERGE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
MS3_CI2_MERGE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
MS3_CURRENT_DISPOSITION=CLOSED_MERGED
MS4=NEXT
NEXT_EXECUTABLE=NONE_OWNER_RESUME_REQUIRED
PR #56 is merged through the normal repository rule. The actual merge commit
is `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`, tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`; the latter is the final candidate
tree, not a GitHub temporary test merge. No deployment or MS4 implementation
was performed.

### MS3-CI1 — Ubuntu Profile D failure (historical handoff; now closed)

New evidence supersedes merge readiness, not the closed R1 test repair.
GitHub run `34421869781`, Ubuntu job `102698845490`, candidate
`54fc7a7bd006d071c628e2f4db824f3fdd43d649`: pytest failed with
1 failed, 1634 passed, 28 skipped, 4 deselected in 298.70s. macOS succeeded.
Ubuntu lint/type/build steps were skipped after pytest, not independently failed.
Failure: `test_profile_d_runs_fourteen_collectors_with_forty_two_active_streams`,
line 1130, waiting up to 2 seconds for all 41 sibling stream counts to reach 2.
This is distinct from MS3-R1. No merge or auto-merge until this new issue is
resolved and the changed test/code delta independently reviewed.

Root cause is NOT yet established. The test mixes 1/2-second coroutine waits,
a 3-second blocked writer, synchronous snapshot waits for depth persistence,
and a shared default executor. Inspect event causality and worker availability
before classifying slow scheduling versus deadlock or production failure.
A reviewer local run with the asyncio default executor bounded to 6 workers
passed (1 passed in 1.03s); this does not reproduce or explain Ubuntu's failure.
Do not label it fixed by rerunning green or by increasing every timeout.

Luna-max execution bundle (complete in one MS3 run, then one review handoff):

1. Preserve CI evidence and exact source identity; instrument only the fixture
   as needed to show missing product/stream counts, task failures, barrier state
   and worker progress. Reproduce the causal condition locally or on an already
   authorized non-production Linux test environment; no VPS access is implied.
2. Correct test synchronization at its cause. Prefer explicit async phase events
   and a terminal-state/deadline guard; bridge worker signals with the owning
   loop's thread-safe API. Keep 14 real collectors, 42 active streams, real writer
   backpressure and 41 sibling progress assertions. Never synthesize counters,
   replace production processing with a model, skip the test or reduce the load.
   A documented generous watchdog is acceptable for hung-test termination; it
   must not be the synchronization mechanism. Ensure the injected writer cannot
   time out before the test phase it intentionally blocks is allowed to finish.
3. Audit the other NEW production-path fixture waits for the same demonstrated
   defect and fix that class together. Do not rewrite unrelated old tests or
   tune production pools without production defect evidence. Ensure cleanup
   releases every injected worker gate before bounded task cleanup, preserving
   the original failure rather than hanging or replacing it with teardown noise.
4. Run a finite repeat batch (e.g. 20 targeted repetitions), include a bounded
   small-executor/scheduling variant if it tests the identified cause, then the
   focused suite and one full offline suite plus applicable static/build/clean
   wheel smoke gates. Record platform/Python and failures honestly. Actual Linux
   CI success is not inferred from macOS emulation. No endless retries or soak.
5. Update this ledger, MS3 acceptance and active status docs with root cause,
   fix, evidence and remaining Linux verification. Prepare the MS4-A coverage
   map and missing operator inputs as planning notes only, using existing tools;
   do not start MS4 implementation or build a final deployment artifact yet.
6. Commit/push the MS3-only repair and update PR #56. Do not manually run/retry/
   cancel/wait for CI. Do not merge or enable auto-merge for the unreviewed delta.
   Return one complete review bundle, not an interim result after each step.

Implementation result — 2026-09-10 (historical; subsequently merged):

- Root cause: the Profile D fake snapshot API waited synchronously for depth
  persistence inside SDK calls already running in the shared asyncio default
  executor. Seven Spot requests, the serialized USD-M request, 42 real writer
  drains and the intentionally blocked target drain therefore competed for the
  same small worker pool. On the Ubuntu schedule, workers could wait for depth
  work still queued behind them, while the 2-second polling assertion expired.
  This was a fixture scheduling cycle, not evidence of a production defect.
- Fix: initial Spot and USD-M snapshot admission now waits asynchronously for an
  aggregate real-depth-persisted phase before submitting SDK worker calls. Real
  writer observers set explicit async phase events; worker callbacks cross to
  the owning loop with `call_soon_threadsafe`. The 41 siblings and target third
  frame are still counted only after real production persistence. A 10-second
  watchdog terminates a hung phase, while the injected target writer has a
  separate 60-second guard and is unconditionally released during cleanup.
- The finite repeat also exposed an independent invalid fixture assumption:
  production's stable-phase rotation can legitimately split two frames across
  two complete chunks. Profile D now requires exact aggregate record counts,
  identity, completeness and no-gap evidence across all manifests, while still
  requiring at least one sealed chunk for every expected product/stream.
- Audit: the other new worker-start waits do not form this cycle: their SDK
  requests are serialized by the shared REST lock and only one blocking worker
  is active. They remain bounded and their existing cancellation/gate assertions
  remain intact.
- Local Darwin arm64 / Python 3.12.9 evidence: 20 repeats of both the normal and
  6-worker variants passed (40 cases); focused five-file suite 45 passed; full
  offline suite 1640 passed, 24 skipped, 4 deselected; Ruff, strict MyPy, M0
  contracts, Go Raw golden, source/wheel build, clean-wheel install, `--version`,
  `doctor` and `status` passed. Linux and required GitHub checks remain pending
  normal push execution; no CI run was manually triggered, rerun, cancelled or
  awaited.

The CI1 handoff was later closed by exact-head run `34436773366`; macOS and
Ubuntu both passed all required steps, including build and clean-wheel smoke.
MS3-CI1=CLOSED
MS3_CURRENT_DISPOSITION=CLOSED_MERGED

### Status and next task

| Work package | Status | Exit evidence / remaining work |
| --- | --- | --- |
| MS1 durable identity | CLOSED / MERGED | PR #51, MS1 acceptance; do not repeat |
| MS2 configurable runtime | CLOSED / MERGED | PR #54, MS2 acceptance; do not repeat |
| MS3-A implementation | MERGED | PR #55, base `01527037254595267003f886689bb270e08b5e5d` |
| MS3-B production-path supplement | CLOSED / MERGED | PR #56; actual merge `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`, tree `2b30a4dd2b8c694ac2e3abad88d6cb56a75badee` |
| MS3-R1 waiting-cancellation evidence | CLOSED / REVIEWED | Actual enqueue, cancellation, retained holder, successor and post-stop evidence verified |
| MS3-CI1 Ubuntu Profile D failure | CLOSED / MERGED | Exact-head run 34436773366 passed Ubuntu and macOS, including Profile D and build/clean-wheel smoke |
| MS3-CI2 normal rotation assertion | CLOSED / MERGED | Deterministic boundary regression and aggregate-manifest repair included in PR #56 |
| MS3-R2 final review / merge handoff | CLOSED / MERGED | PR #56 merged by normal repository rules; actual merge SHA/tree recorded above |
| MS4-A offline qualification preparation | REVIEWED_COMPLETE | Local artifact/profile/coverage ledger and corrected runbook; later target qualification is not authorized |

| MS4-B Tokyo VPS preflight / stopped deployment | REVIEWED_COMPLETE | Owner-supplied Tokyo VPS report, exact artifact/config/unit/identity, rollback and stopped-state evidence; no start/readiness/live traffic |
| MS4-C bounded qualification | REVIEWED_COMPLETE | Original 2026-09-10 two-hour window remains `EXECUTED_PARTIAL_NOT_ACCEPTED`; separate passing R3 non-formal recovery supplement closes the recovery gate with zero Formal M22.9 credit; see `docs/milestone_evidence/MS4-C-20260910.md` |
| MS4-D final review / documentation closure | REVIEWED_COMPLETE | Eleven bounded gates are `PASS` with explicit scope limitations; current claims are aligned without changing source, runtime or deployment behavior |
| M22.9-P1 detached single-stage observation preparation | REVIEWED_COMPLETE | Documentation-only systemd 255 transient-unit procedure around the existing observer; current main `83a063f5bb9f91508238c9fd86d21aa45d1bd501`; no deployment, timer enablement, Recorder start, Formal T0 or duration credit |
| M22.9-P2 exact deployment/archive/capacity preflight | REVIEWED_COMPLETE | P2 deployment source/review base `646792f2e5fc5b7195ea58541d3f1dfda6555b7f` was installed and verified; registered archive drained and fully verified; timer enabled/active, Recorder stopped; see `docs/milestone_acceptance/M22.9-P2.md` |
| M22.9 2h Formal failed closeout | REVIEWED_COMPLETE | Formal result remains `EXECUTED_FAILED_AT_T0`: failed before first observer sample with 24 stage-start blockers; zero credit; Recorder stopped and archive drained; see `docs/milestone_acceptance/M22.9-2h.md` |
| M22.9 Formal 2h retry | EXECUTED_INCOMPLETE_HOST_MAINTENANCE_INTERRUPTED | Valid T0 plus ten passing samples; systemd reexecution at about 85 minutes invalidated the process/service identity; zero credit; see `docs/milestone_acceptance/M22.9-2h-retry.md` |
| M22.9 root-home recovery / stopped handoff | COMPLETE | Access recovered without rebuild; Recorder inactive and disabled; archive timer enabled/active; aborted preflight roots preserved |
| M22.9 host-maintenance quiet-window preflight | COMPLETE / PASS | Six pending packages completed; runtime-only maintenance exclusion tested; update authority restored; no Recorder start or T0; see `docs/milestone_acceptance/M22.9-host-maintenance-quiet-window-preflight.md` |
| M22.9 Formal 2h quiet-window retry | EXECUTED_INCOMPLETE_OBSERVATION_GAP | Stable Recorder identity beyond two hours, but approximately 223 MB observer samples exceeded the 600-second cadence; final `INCOMPLETE`, zero credit; see `docs/milestone_acceptance/M22.9-formal-2h-quiet-window-retry.md` |
| M22.9 AcceptanceObserver bounded evidence | NEXT | Bound repeated evidence and completed-chain memory without weakening immutable integrity or fail-closed acceptance |
| Completed-branch cleanup | CURRENTLY NO REMOTE CANDIDATES | Luna read-only audit on 2026-09-13 found only `main` in Recorder and Contracts, with no open PR or running Action; earlier merged heads are already absent |
| Formal M22.9 | 2H QUIET-WINDOW RETRY INCOMPLETE / LATER STAGES NOT STARTED | Bounded-evidence code fix, exact review/deploy, and a new eligible 2h final are required; zero current duration credit; Production Ready remains NO |

NEXT_AFTER_CI1_REVIEW=MS3-R2-MERGE-HANDOFF
NEXT_AFTER_CI2_REVIEW=MS4-A-LOCAL-PREPARATION

### MS3 final independent review — 2026-09-10

Verdict: **APPROVED for offline MS3 scope; no remaining blocking review findings**.
Reviewer: GPT-6 Astra. Head `66e036a07f422818f5e3f54f0216d4083b443698`,
tree `1413ec705db3fd9f214499c57696bcab2da2e8b9`; incremental parent
`c7d6c904c42b4bc86cd0937d4fca03aebc16a81b`. PR base remains
`01527037254595267003f886689bb270e08b5e5d`; do not confuse the incremental
review parent with the PR base. GitHub head matched local HEAD; PR #56 open.

The R1 test now yields until the real waiter attempts the held lock, verifies
no grant before cancellation, observes cancellation at the lock, preserves the
holder, then completes a successor and excludes post-stop wire calls. Together
with the previously reviewed production-path and Profile D supplement, this
closes the original review findings. No production code changed in R1.
Reviewer reran the same five focused test files (44 tests); see MS3 acceptance
for the result. Full-suite evidence remains the implementer's reported result.

Next local Luna run: commit only these review documentation updates; update
PR #56 body (it still names c7d6c904); preserve the approved code/test content.
Verify any descendant is documentation-only before relying on this review.
Use normal merge gates without administrative bypass, manual CI actions or
waiting. If pending, return MERGE_PENDING; if merged, record actual merge
SHA/tree and MS3=CLOSED, then stop this MS3 run. Start MS4-A in the next MS4
run following this plan. No further code repair, deployment or long test is
requested. These documentation edits do not require a full test/CI rerun.

### MS3 review disposition and R1 repair

Reviewer: GPT-6 Astra, 2026-09-10. Reviewed the supplement against previous
candidate `e2a51dfe32eb15f4873bf027f4199ce8086877bb` and the existing owned-worker
helper. The original model-only coverage finding is substantially addressed by
real Collector/Poller requests and Catalog pagination; its waiting-cancellation
subcase was open at c7d6c904 and is now CLOSED by the final review below. The original sequential-only Profile D finding is CLOSED:
the supplement runs 14 collectors and 42 core stream contexts simultaneously,
observes sibling progress during target backpressure, and verifies shutdown and
persisted identities. These are bounded offline results, not live capacity proof.

R1 [P2]: In `tests/integration/test_ms3b_production_paths.py`,
`test_production_waiting_cancel_and_post_stop_request_have_no_wire_side_effect`
first acquires `_RecordingLock` in the test owner, incrementing request_count
to 1. It then creates a waiter but waits for `request_count == 1`, a condition
already true. `_wait_until` can return without yielding, so the task is cancelled
before entering `_request` or enqueueing on the production lock. A passing test
does not establish waiting-cancellation behavior.

Historical R1 repair instructions (completed; do not repeat):

1. Capture the holder's request/acquire counts, start the real poller waiter,
   and wait for its additional acquire request (or an equivalent explicit
   enqueue event). Assert no additional grant and no SDK call before cancel.
2. Cancel that queued waiter; assert cancellation is observed by the lock,
   the original holder still owns it, and no wire request occurred.
3. Release the holder and prove a subsequent real request can acquire and
   finish with no stale ownership. Preserve the separate post-stop no-wire case.
4. Keep changes narrow: this finding requires a test correction, not another
   scheduler or production refactor. Fix production only if a new deterministic
   failing case actually establishes a defect.
5. Update MS3 acceptance and this row to IMPLEMENTED / AWAITING REVIEW, with
   commands and results. Do not mark independent review or merge complete.

Reviewer validation on c7d6c904: 44 passed in 2.03s for the production-path
supplement, fairness, multi-product storage, archive/capacity, and existing
USD-M shared-gate files. This is a focused re-run, not a reproduction of the
implementer's reported full suite (1639 passed, 24 skipped, 4 deselected).
Online, VPS, external media, stress/soak and full CI were not run by reviewer.

R2 exit: Astra reviews the R1 diff and results against the reviewed source.
Merge state and source identity must be verified independently of test success.
If GitHub checks are pending, report a merge-ready handoff and continue only
independent authorized documentation/preparation in the current milestone;
do not wait, bypass required checks, or begin MS4 implementation on an unmerged
dependency. Do not automatically force-push or rewrite reviewed history.

### MS3-R1 execution result — 2026-09-10

R1 was submitted as **IMPLEMENTED / AWAITING REVIEW** in the appended MS3 test commit. The
change is limited to `tests/integration/test_ms3b_production_paths.py`; no
production scheduler or production-code refactor was added.

The corrected test captures the holder's request/acquire/release counts before
starting the real `RestSideDataPoller._request()` waiter, then waits for the
waiter's additional request-lock acquisition attempt. Before cancellation it
asserts no additional grant, no release, no SDK wire start, and that the lock
is still held by the original holder. After cancellation it asserts the
`_RecordingLock` observed `CancelledError`, the acquire/release counts remain
unchanged, the SDK still has no wire call, and the holder still owns the lock.
It releases the holder and completes a subsequent real poller request through
the same lock, proving no stale ownership. It then clears the SDK-start marker,
sets stop, and independently asserts the post-stop request does not increment
the lock request count or start the SDK.

Post-R1 validation: the focused MS3 suite passed 44 tests in 1.83s; the full
offline suite passed 1639 tests with 24 explicit online/preview skips and 4
stress tests deselected. Ruff, MyPy, compileall, M0 contract verification, and
Raw v1 golden verification also passed. Independent final review, merge, CI,
online traffic, VPS access, external media, stress/soak, and deployment remain
outside this result.

### MS4-A — local preparation, no production access

Status: **REVIEWED_COMPLETE**. Execution was local macOS Codex, Luna-max, on a
new MS4 run after verifying the actual MS3 merge. The preparation uses the
existing configuration, readiness, deployment identity, archive and audit
interfaces; it adds no service, RPC, GUI, scheduler or benchmark framework.

The reviewable deliverables are
[`docs/milestone_acceptance/MS4.md`](milestone_acceptance/MS4.md),
[`docs/runbooks/MS4_qualification.md`](runbooks/MS4_qualification.md), and
[`docs/runbooks/MS4_candidate_profile.toml.example`](runbooks/MS4_candidate_profile.toml.example).
This branch starts from merged main `303e073e25d5ed53d7cf6e26a9c6c6e879013b50`
with tree `2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`; the known MS3
documentation-only closeout PR #57 is not duplicated here.

Completed preparation:

1. The exact merged source/tree, Python/build tooling, macOS Wheel and sdist
   hashes, Linux runtime lock hash and profile-fragment hash are recorded in
   `docs/milestone_acceptance/MS4.md`. The local Wheel is
   `py3-none-any`, but target compatibility is not inferred from that tag.
2. Existing CLI/tools/tests are mapped to configuration, exact ProductKey
   readiness, shared REST, concurrent backpressure/rotation, Raw/manifest/
   Catalog, archive, capacity, systemd and deployment identity checks. MS2/MS3
   green evidence is reused; no Contracts/gRPC rebuild is required.
3. The representative profile is BTCUSDT and ETHUSDT in both Spot and USD-M
   perpetual markets: exactly `(spot, BTCUSDT)`, `(spot, ETHUSDT)`,
   `(um_perpetual, BTCUSDT)` and `(um_perpetual, ETHUSDT)`, with 12 core stream
   contexts. All current auxiliary kinds are explicitly disabled for this
   sample, so no global side-data owner is part of the proposed workload.
   Symbol eligibility and official-source provenance remain a pre-live check.
4. The runbook freezes the existing direct proxy policy, 60-second/128 MiB
   rotation defaults, exact readiness set and a 15-minute absolute outer
   startup envelope, followed by the proposed 2-hour steady-state,
   15-minute recovery, 15-minute shutdown and 15-minute margin envelope. The
   existing readiness observer's 300-second bound is an implementation
   interval inside that outer deadline, not an automatic extension. It does
   not guess the target host, service principal, archive destination or live
   root.
5. The runbook provides concrete existing-CLI install, check, start, sample,
   stop, archive/verify and fail-closed rollback commands. Rollback explicitly
   requires a target identity that understands the MS1+ Catalog and current
   durable remote states; a pre-MS1 binary is not used against new state merely
   because its Wheel is retained.
6. Local preparation checks passed: build, no-index temporary Wheel smoke,
   `pip check`, `--version`, profile `config show`, offline `doctor`, structured
   `status` and `git diff --check`. Exact-source run `34436773366` is reused for
   the merged-code Ubuntu x86_64 lock/build/clean-Wheel evidence.

MS4-B disposition and MS4-C/MS4-D handoff:

- The stopped deployment identity is verified against the actual canonical
  filesystem and effective systemd unit; the exact four-product config is
  frozen without operational environment overrides.
- The owner-authorized cross-machine destination was the current MacBook
  Downloads internal-folder test target. It is not an external-storage
  certification and the receiver-only cycle did not authorize source
  retirement.
- Immediate official Spot/USD-M eligibility evidence was captured before
  start and recorded under the preserved VPS evidence root.

Exit: MS4-B is `REVIEWED_COMPLETE` for the stopped boundary. The original
MS4-C attempt remains an executed partial record; the separate R3 supplement
closes its recovery gate for review, and MS4-D closes the bounded qualification.
The full ledger, evidence template, hashes, coverage map, capacity formula and disposition are in
`docs/milestone_acceptance/MS4.md` and
`docs/milestone_evidence/MS4-C-20260910.md`.

MS4-A=REVIEWED_COMPLETE
MS4-B=REVIEWED_COMPLETE
MS4-C=REVIEWED_COMPLETE
RECOVERY_GATE=REVIEWED_COMPLETE
MS4_D=REVIEWED_COMPLETE
MS4=REVIEWED_COMPLETE
BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS
QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE
CURRENT_MAIN_DEPLOYED=NO
FORMAL_M22_9=NOT_STARTED
FORMAL_M22_9_CREDIT_SECONDS=0
PRODUCTION_READY=NO
RECORDER=STOPPED
NEXT=FORMAL_M22_9_PREPARATION_REQUIRES_SEPARATE_AUTHORIZATION

### MS4-B — Tokyo VPS preflight and stopped deployment (reviewed complete)

Execution: **Tokyo VPS Codex**, Luna-max. The owner-supplied execution summary
and cross-checked Git/runbook evidence establish the stopped boundary; the
private evidence bundle was not directly inspected in this local worktree
(`PRIVATE_EVIDENCE_BUNDLE_DIRECT_INSPECTION=NOT_RUN_LOCAL`). Local archive steps
remain for the explicitly identified archive machine. Every future execution
prompt must name its machine.

1. Verify host OS/architecture, actual deployed identity, service user/unit,
   active root, free bytes, memory headroom and co-resident service boundaries.
   Record observations; do not change unrelated services or inspect credentials.
2. Calculate capacity runway from measured aggregate growth where available.
   Single-symbol historical rates are provisional estimates, not a multi-product
   measurement. Require available bytes above hard reserve to cover the finite
   planned window plus a stated shutdown/recovery margin. Treat unverified
   archive throughput as zero when budgeting. If insufficient, shorten/reduce
   the proposed workload with explicit recording or return an actionable block;
   never lower reserve thresholds or delete unarchived data.
3. Verify rollback compatibility: MS1 migrated Catalog identities may not be
   understood by the old deployed artifact. Do not simply point old binaries at
   new state. Use a proven supported rollback path or retained prior root;
   preserve all new Raw and record any stopped interval explicitly.
4. Verify the approved artifact/config/unit hashes, install via the existing
   native procedure while stopped, and check actual ProductKeys against the
   independently parsed config. Record the final deployment identity and
   stopped service state. Do not start the service, test readiness, create T0,
   or make Binance qualification traffic in MS4-B.

Exit: exact installed identity and four-ProductKey configuration, rollback
procedure verified, sufficient preflight runway, and service `inactive/dead`.
Startup/readiness, live capture, controlled recovery, archive receive/verify,
and shutdown/integrity observation belong to MS4-C.
On a failed gate preserve evidence, mark the failed step, and repair only the
demonstrated issue; do not silently substitute another artifact or workload.

### MS4-C — bounded live evidence (original window; partial record)

The 2026-09-10 owner-authorized attempt used the approved MS4-B artifact/config,
the Tokyo-to-MacBook receiver-only archive target, immediate official
ETHUSDT eligibility evidence and the explicit start authorization. It reached
all-four ProductKey readiness and completed the two-hour steady interval. At
that window's disposition it was `EXECUTED_PARTIAL_NOT_ACCEPTED`: controlled
recovery was not executed in the approved window. The detailed record is
`docs/milestone_evidence/MS4-C-20260910.md`; the separate R3 supplement below
is the current recovery-gate review. Any future continuation
must review that record first; the 15-minute startup envelope remains
absolute and the 300-second readiness observer remains inside it.

| Check | Required evidence / pass condition |
| --- | --- |
| Identity and topology | SHA/tree/Wheel/lock/config/unit/deployment IDs; all expected ProductKeys, no extras; each configured core stream produces correctly attributed Raw |
| Readiness | Independently configuration-bound; no false-ready interval; final products ready |
| Continuity and recovery | Product-specific START/COMPLETE evidence and final unresolved core discontinuities zero; recoverable gaps remain honestly recorded |
| Controlled recovery | One approved targeted reconnect/resync or equivalent observed event, bounded and attributed; siblings progress; no host-wide firewall/proxy fault injection |
| Shared REST | Existing traces/logs plus offline proof; no observed cooldown bypass or duplicated global owner; never deliberately provoke live 418/429 |
| Storage | Verify sealed chunks, hashes, manifest identity/counts and Catalog consistency; do not checksum active partials as sealed artifacts |
| Archive | At least one authorized receive/verify/publish cycle for qualifying identities on the actual chosen archive path; receipt/manifest/hash evidence; source retirement remains separately authorized |
| Resources | CPU/RSS/queue/backpressure/capacity samples, maxima and trend, no OOM or reserve breach; scope conclusions to the measured window |
| Shutdown/restart | Graceful seal and bounded restart if included in approved runbook; no orphan active ownership after stop; preserve process-session gap evidence |

The receiver-only archive destination was available for one verified cycle;
formal receipt-bound Catalog snapshot and source retirement were deliberately
not run. If a future archive destination is unavailable, mark archive
verification NOT RUN and MS4 partial; do not call the entire milestone
complete based on local mocks.
Global auxiliary sentinel BTCUSDT and REST request connection IDs are not extra
core products or automatic core reconnects. Keep known fail-closed auxiliary
manifest classifications visible; do not relabel incomplete artifacts complete.

On integrity failure, false readiness, persistent unresolved recovery beyond
the frozen deadline, or resource exhaustion: stop the qualification, preserve
Raw/evidence, and use the approved rollback/stop path. Normal transient recovery
is observed within its bounded deadline rather than treated as immediate
permanent failure. Any behavior-changing fix gets fresh source/artifact identity
and a fresh affected qualification window; no duration credit transfers.

### MS4-C R3 recovery supplement — 2026-09-11 (reviewed complete)

The R3 evidence is a separate, owner-authorized non-formal supplement, not a
continuation of the original two-hour duration stage and not Formal M22.9.
The historical VPS path
`/root/MS4-C-RECOVERY-20260911-R3/evidence/recovery-audit.json` reported
`PASS`, classification `NONFORMAL_MS4_RECOVERY_SUPPLEMENT`, and
`formal_m22_9_credit_seconds=0`. Fresh official Spot/USD-M eligibility was
captured at `2026-09-11T01:01:49Z`; MainPID `685913` reached `READY`, then the
exact owned socket was disconnected. The only durable recovery was
`um_perpetual:BTCUSDT/book_ticker`, completed in `0.436024210` seconds with a
new connection. Fifteen post-fault observations retained all four expected
ProductKeys ready with receive progress. Twelve core contexts passed sealed
validation (`validated_chunks=23`), Catalog integrity was `ok` with zero
malformed/degraded/unclosed/active chunks, and no `.partial` file remained.
Final systemd state was `inactive/dead`, `MainPID=0`, `Result=success`.
The unique recovered gap remains explicit; historical continuity is not
claimed restored, and source retirement was not authorized. This closes the
MS4-C recovery gate for review; MS4-D closure is recorded below.

### MS4-D — review and stage closure

Luna summarizes MS4-A/B/C evidence in MS4 acceptance and updates each ledger
row. Astra reviews the exact implementation and measured results. Use explicit
PASS / FAIL / NOT RUN / BLOCKED states, with artifact identity and reason.
Keep IMPLEMENTED, REVIEWED, MERGED, DEPLOYED and QUALIFIED distinct.

After approval, align README, AGENTS, CURRENT_PRODUCTION_STATE, PROJECT_HANDOFF,
this plan, ADR status summaries, known limitations, risks and traceability where
their current claims changed. Preserve historical records. MS4 is complete only
when all required checks pass; label this multi-symbol bounded qualification,
not Formal M22.9 or Production Ready. Record the next separately authorized
program without implementing it. Do not require Gateway multi-symbol support,
Contracts publication, a Web UI or speculative optimization for this closure.

#### MS4-D outcome

The eleven MS4-D main gates are recorded as the single primary state `PASS` in
[`docs/milestone_acceptance/MS4.md`](milestone_acceptance/MS4.md), with each
bounded limitation retained in its reason. The review is bound to the exact
four configured ProductKeys and the deployed source/tree and evidence hashes
recorded there. `ROLLBACK_COMPATIBILITY=PASS` refers to the verified
compatibility preflight only; `ACTUAL_ROLLBACK_EXECUTION=NOT RUN` remains
explicit. Queue depth was unavailable, so the resource result is bounded to
the observed CPU/RSS/no-symptom window. Auxiliary live data was disabled, and
the archive result is limited to the authorized Mac internal APFS
receiver-only test path.
The historical MS4-D final read-only VPS check at `2026-09-11T06:39:23Z` also
passed with the
Recorder `inactive/dead` (`MainPID=0`, `Result=success`, `NRestarts=0`) and the
archive timer `disabled/inactive`; `/dev/vdb1` is a mounted `/srv/recorder-data`
archive target, while the active writer root remains
`/var/lib/binance-market-data-recorder`.

The current result is:

```text
MS4_D=REVIEWED_COMPLETE
MS4=REVIEWED_COMPLETE
BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS
QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE
FORMAL_M22_9=NOT_STARTED
FORMAL_M22_9_CREDIT_SECONDS=0
PRODUCTION_READY=NO
CURRENT_MAIN_DEPLOYED=NO
RECORDER=STOPPED
NEXT=FORMAL_M22_9_PREPARATION_REQUIRES_SEPARATE_AUTHORIZATION
```

### Execution, review and CI rules for every continuation

- Recommended implementation model: Luna, max. Astra owns final independent
  review and architecture decisions. Sol-medium is optional only when the owner
  explicitly chooses it; no extra agents or background tasks are implied.
- At run start read this ledger and Git status; choose the first authorized
  unfinished package in the current milestone. Complete routine steps without
  repeated permission questions. At a review boundary deliver one evidence
  bundle; after review the next run reads the revised queue from this document.
- An implementer may mark work IMPLEMENTED / AWAITING REVIEW, never approve its
  own independent review. A reviewer marks CLOSED only when the stated exit is
  met. Record new findings here with file, trigger, impact, minimal change and
  test, so the next Luna run has a concrete task rather than another redesign.
- Keep one milestone per run/commit. This document's future planning is not
  future implementation. Preserve user files and these uncommitted planning
  edits; do not demand a globally clean checkout by deleting unrelated files.
- Documentation-only changes require diff/link/status checks, not test/CI
  reruns. Code/test changes require focused regression and applicable repository
  offline gates; report unrun tests. Do not manually trigger/retry/cancel/wait on
  CI, bypass required checks, or disable workflows. Normal pushes may naturally
  trigger configured CI; the owner supervises it.
- GitHub mutations run only through the user's local Luna execution prompt.
  Do not merge without the review/authorization gate. Branch cleanup is separate
  maintenance: only proven merged tips with no later work, no active worktree,
  no protected/default/current branch; preserve restoration SHAs, use a remote
  expected-tip condition, and retain local branches when `git branch -d` fails.
- Return milestone/package, base/head/tree, changed files, tests, unrun reasons,
  findings resolved/open, compatibility, NEXT and execution machine. Include
  concrete missing inputs only when they actually block the next authorized step.

## Universal gate

Before every milestone, read `AGENTS.md`, this plan, and the milestone's ADRs
and contracts; inspect Git status; verify the previous milestone acceptance;
and preserve unrelated changes. At completion, run the stated gates, record
unrun tests without lowering standards, update risks/compatibility, make one
local commit containing only that milestone, require a clean worktree, report,
and stop.

If official API semantics, macOS permission, or platform behavior cannot meet a
gate, stop implementation, capture evidence, and update the risk register. No
plausible but semantically different substitute is acceptable. Every milestone
inherits the bans on trading/accounts/keys, GUI, filesystem format/repair, and
modifying external consumer repositories. A future named-consumer validation is
read-only and optional unless separately authorized.

Rollback always preserves immutable data. “Revert commit” below means revert
code/config/schema additions; it never means delete or rewrite captured Raw.

## Dependency graph

```text
M0 -> M0.1 -> M0.2 -> M1 -> M2 -> M3 -> M4 -> M5 -> M6 -> M7 -> M8 -> M9 -> M10
                                                   M8 -> M11
                                             M9 -> M12
                                   M4/M5/M6 -> M13 -> M14
                         M3/M6/M8/M10/M13 -> M15 -> M16
                                  all implemented/fault gates -> M17 -> M18
```

Sequential execution remains mandatory even where the graph permits a weaker
technical dependency.

## M0 — Repository audit, boundary freeze, and complete plan

Status: **ACCEPTED** by commit recorded in the final M0 report.

- Scope: read-only audit of Alpha101Crypto's required contracts/config and
  Binance `data`/`exchange`/`store` modules; independent Git repository;
  `AGENTS.md`, README, project/architecture/data/storage/macOS contracts, full
  plan, traceability, source inventory, risks and foundational ADRs; minimal
  offline test.
- Non-scope: Python production package, CLI/service/config model, SDK install,
  updater tool, WebSocket/REST implementation, Collector, GUI, or any change to
  Alpha101Crypto.
- Dependencies: none. Bootstrap exception: the target repository, AGENTS and
  plan did not exist before M0; the research repo had no applicable AGENTS.
- Acceptance: boundaries do not conflict with audited Alpha101Crypto; every
  milestone is independently committable; all unknowns are explicit; all input
  requirements map to contracts/milestones; `pytest` and standalone verifier
  pass; only M0 is committed and worktree is clean.
- Rollback: revert/archive the M0 commit and repository before implementation;
  do not touch the audited research repository.

## M0.1 — Project identity, workspace, and generic consumer boundary correction

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M0.1.md` and reported at handoff.

- Scope: preserve the M0 Git history while moving the same repository to the
  intermediate identity/workspace recorded verbatim in ADR-0006 and the M0.1
  acceptance record; freeze that milestone's identifiers; remove the original
  Recorder identity from then-current surfaces; make Raw, Catalog, normalize,
  replay, archive, and consumer contracts generic; add ADR-0006, traceability
  and M0.1 acceptance; update contract tests.
- Non-scope: M1 packaging/config/CLI implementation, `src` production code,
  Binance connection, Collector/WebSocket/REST implementation, GUI, production
  data migration, or modification of Alpha101Crypto.
- Dependencies: accepted M0 root commit
  `1634b09e57d287eba82ef34f117b4657979cc38b`, clean worktree, and preservation
  of the existing `.git` object database during the directory move.
- Acceptance: Git top-level and frozen identities equal ADR-0006; original M0
  commit remains a commit object; exact legacy identity/path searches contain
  only explicitly classified migration history; every Alpha101 reference is
  classified as historical audit or optional ordinary external consumer; all
  current CLI/data/service paths use the new identity; M0 verifier and pytest
  pass on Python 3.12; no `src`, M1/network/GUI/Collector code, external-repo
  change, or dirty post-commit worktree.
- Rollback: revert the single M0.1 commit and move the same repository directory
  back only before later milestones depend on the new public identifiers; do
  not rewrite M0, create a second history, or move/delete production data.

## M0.2 — Binance-specific project identity correction

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M0.2.md` and reported at handoff.

- Scope: preserve M0/M0.1 history while moving the same repository to
  `/Users/amada/Documents/Development/Crypto/BinanceMarketDataRecorder`; freeze
  Binance-specific display/distribution/import/CLI/application-data identities;
  establish the independent unofficial-project disclaimer and brand/service
  namespace rules; make Spot/USD-M the product modules; remove exchange-neutral
  V1 positioning; add ADR-0007, traceability, M0.2 acceptance, and contract
  verification while preserving M16's generic consumer boundary.
- Non-scope: M1 packaging/config/CLI implementation, `src`, `pyproject.toml`,
  production configuration, Binance connection, Collector/WebSocket/REST
  implementation, GUI, service identifiers, branding assets, other exchanges,
  production data migration, or modification of Alpha101Crypto.
- Dependencies: accepted M0 commit
  `1634b09e57d287eba82ef34f117b4657979cc38b`, accepted M0.1 commit
  `b186c08191392e7454d259d8cfd16d0263e0901f`, clean worktree, and preservation
  of the existing `.git` object database during the directory move.
- Acceptance: Git top-level and all current identities match ADR-0007; README,
  project contract and release plan contain the unofficial/no-affiliation/
  no-sponsorship/no-endorsement rule; active documents do not imply official
  status or use Binance-owned-looking reverse-DNS namespaces; intermediate
  names occur only in allowlisted history/migration/test evidence; active V1 is
  Binance Spot/USD-M rather than multi-exchange; M0/M0.1 objects remain
  reachable; Python 3.12 pytest/verifier/diff checks pass; no M1/production/
  network/GUI code, external-repository change, or dirty post-commit worktree.
- Rollback: revert the single M0.2 commit and move the same repository directory
  back only before M1 publishes identifiers; do not rewrite earlier commits,
  create a second history, or move/delete production data.

## M1 — Python engineering skeleton, configuration, and CLI

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M1.md`.

- Scope: Python 3.12 `src` layout; `pyproject.toml`; installable package; lint,
  type checking and pytest; structured logging; strict Pydantic configuration;
  CLI/version/Git commit injection; default application-support paths;
  `binance-market-recorder doctor`, `config show`, and `status`;
  `binance-market-data-recorder` distribution and
  `binance_market_data_recorder` import package; path/permission guards; empty
  keys/credential surface.
- Non-scope: any Binance connection, collector, external-volume write, raw
  writer, launchd install, or production data.
- Dependencies: M0/M0.1/M0.2 contracts, ADR-0001/ADR-0007, and clean M0.2
  acceptance.
- Acceptance: clean environment install; unit/lint/type tests pass; all CLI
  commands run offline; default/override path and permissions are tested;
  repository/Desktop/Documents/iCloud/persistent `/tmp` are rejected for
  production roots; config schema contains no secret/key fields; no Binance or
  real external disk access.
- Rollback: revert M1; M0 documents remain usable and no production state needs
  migration.

## M2 — Agent Native docs pipeline and SDK capability validation

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M2.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

- Scope: `tools/update_binance_docs.py`; working `llms.txt`; configured selected
  Spot/USD-M pages and URL/time/hash records; allowed-host/redirect/content
  validation; pinned official modular SDK candidates; offline capability probes;
  unsigned public REST depth smoke; SDK versus generic WebSocket evidence; REST
  transport ADR and WebSocket transport ADR.
- Non-scope: long-running capture, spool writer, production Collector, account
  endpoints/keys, deprecated connectors, third-party MCP, or default download of
  all `llms-full.txt`.
- Dependencies: M1 tooling; ADR-0005; source/risk records.
- Acceptance: every semantic conclusion cites official evidence; only
  `developers.binance.com` and `github.com/binance` are downloadable; remote
  content is never executed; deprecated Futures connector absent; public
  no-key smoke works or milestone stops with official/platform evidence;
  default tests offline, online tests explicitly marked; probes establish raw
  payload/timestamp/lifecycle/backpressure/fault capabilities; no long-running
  Collector.
- Rollback: revert tool/dependency/ADR selection, keep the source evidence; no
  captured Raw exists.

## M3 — Event contract, raw chunks, Catalog, and crash recovery

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M3.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

- Scope: executable EventEnvelope v1; ADR-0002 byte-level specification and
  cross-language golden vectors; framed append-only writer; bounded queue;
  60-second/128-MiB configurable rotation; <=1-second configurable durability
  window; flush/fsync; `.partial`, seal, separate Zstd artifact, manifest and
  SHA-256; CRC32C frames; startup scan/tail truncation/quarantine; SQLite
  Catalog and idempotent transitions.
- Non-scope: real network, Binance parsing beyond synthetic envelopes, order
  book, normalization/Parquet, external archive, or service deployment.
- Dependencies: M2 transport/schema evidence and M1 config/logging.
- Acceptance: kill -9 matrix recovers last complete frame and never creates a
  false sealed file; repeat startup is idempotent; corruption/truncation and
  property/fault tests pass; 1,000,000 synthetic events pass within documented
  bounded memory; queue overload is explicit, never silent; format golden
  vectors pass; Catalog never stores full events.
- Rollback: stop writers, preserve/version any test chunks, revert M3. A
  superseding format retains readers/transcoder for any non-test chunks.

## M4 — Binance Spot BTCUSDT real-time Collector

- Scope: Spot diff depth 100 ms, aggTrade, bookTicker and public REST depth
  snapshots; selected transport; ping/pong, serverShutdown, backoff reconnect,
  planned 24-hour rotation; exchange/receive clocks, connection/session/version,
  raw payload and snapshot provenance; graceful stop/network evidence.
- Non-scope: USD-M, book reconstruction, Parquet, archive, strategies, callback
  compression or complex calculation.
- Dependencies: M2 Spot schemas/transport ADR; M3 durable writer.
- Acceptance: official-schema fixtures; local mock WebSocket; disconnect,
  duplicate and out-of-order tests; explicitly marked public live smoke >=15
  minutes; all accepted/malformed evidence reaches Raw; callback only envelopes
  and bounded-enqueues; shutdown seals safely; no key/account call.
- Rollback: disable Spot worker, seal/mark end and gap as applicable, revert M4;
  retain readable raw chunks and version provenance.

## M5 — Binance USD-M BTCUSDT real-time Collector

- Scope: independent USD-M perpetual diff depth 100 ms, aggTrade, bookTicker
  and public REST snapshot; USD-M sequence fields including `U/u/pu`; separate
  connection/failure/metrics state; side-data extension point.
- Non-scope: order-book reconstruction, side-data implementation, archive,
  research or coupling Spot/UM failure.
- Dependencies: accepted M4 pattern, M2 current routed-endpoint/schema evidence,
  M3 writer.
- Acceptance: M4-equivalent fixtures/mock/disconnect/duplicate/out-of-order and
  public live gates; official `U/u/pu` semantics asserted; Spot and USD-M run
  together >=30 minutes; market-specific statistics; injected crash/failure of
  either leaves the other running; exact payloads written Raw.
- Rollback: disable only USD-M, seal/mark its state, revert M5; Spot continues.

## M6 — Spot and USD-M order-book reconstruction and quality audit

- Scope: diff buffer before snapshot; official snapshot bridging; absolute
  price-level apply and zero delete; market-specific gap/resync; checkpoints;
  best bid/ask, crossed/empty checks; bookTicker comparison; explicit unreliable
  intervals and deterministic hashes.
- Non-scope: execution/queue model, strategy, treating checks as exchange
  checksum where none is documented, or hiding gaps.
- Dependencies: M4/M5 Raw and M2 official algorithms; M3 checkpoints/storage.
- Acceptance: official examples; randomized sequence properties; deleted update
  always creates a gap; gap never complete; repeated replay hashes match;
  checkpoint restore equals origin replay; Spot and UM rules cannot be mixed.
- Rollback: remove derived books/checkpoints and revert; Raw remains unchanged
  and can rebuild with a new algorithm version.

## M7 — USD-M auxiliary market data

- Scope: independently configurable current-official mark price,
  index/premium, funding, open interest, liquidation events, exchange info and
  filter snapshots; explicit sparse/polling semantics; source/fetch time and
  REST rate-limit provenance; isolated failure/metrics.
- Non-scope: blocking/pausing core L2, forward-filling missing values, assuming
  an eight-hour funding interval, private endpoints, strategies.
- Dependencies: M5 isolation and M2 refreshed official source pipeline.
- Acceptance: each kind has official schema fixture and failure statistics;
  public/no-key behavior verified; missing values not silently filled;
  observed/official funding cadence preserved; rate limits tested; side-data
  failure leaves both core collectors healthy.
- Rollback: disable individual data kind and remove/rebuild its derived outputs;
  preserve Raw/provenance and core streams.

## M8 — Metrics, daily traffic reports, and status

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M8.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

- Scope: all project-contract input, quality, output and performance counters;
  structured runtime JSON; `binance-market-recorder status`;
  `binance-market-recorder report daily`; UTC
  rollover; persisted idempotent aggregates; per-market/stream audit.
- Non-scope: GUI/metrics web server, full events in SQLite, storage forecast
  algorithm (M11), or external archive mechanics.
- Dependencies: M3 Catalog, M4-M7 event/quality signals.
- Acceptance: fixed fixture yields deterministic JSON/CSV; UTC midnight is
  correct; restart/replay does not double count; each stream reconciles input
  and output; required lag percentiles/queue/write/fsync/CPU/RSS/free-space/age
  fields exist with explicit unavailable semantics.
- Rollback: regenerate versioned reports/aggregates from Catalog/manifests and
  revert; never change Raw.

## M9 — macOS external-volume detection and archive registration

- Scope: Disk Arbitration adapter; startup inventory and appearance/disappearance;
  mountpoint/UUID/name/filesystem/read-only/capacity; folder registration and
  marker; in-folder write/fsync/rename/readback probe; full public storage state
  machine; display-only unregistered volumes.
- Non-scope: copying/deleting spool, formatting/repairing/remounting, volume-root
  marker, fixed-name identity, or making Collector depend on external storage.
- Dependencies: M1 CLI/config, ADR-0003, M8 status.
- Acceptance: available APFS/exFAT environments tested and unavailable physical
  tests explicitly unrun; unplug/replug and UUID recognition after name/path
  change; read-only cannot be READY; zero writes outside registered folder; no
  external disk leaves Collector normal; PyObjC/helper semantics proven or
  milestone stops per R-022.
- Rollback: unregister/disable adapter without deleting markers or archives;
  revert M9; internal capture continues.

## M10 — Archive transaction, verification, and safe local deletion

- Scope: oldest-sealed selection; `.copying`; streaming copy; fsync/reopen/full
  readback; size/SHA-256; atomic final rename; external manifest; Catalog
  transaction; separate local deletion; retries/reconciliation; disappearance,
  collision, residual-temp and crash idempotence.
- Non-scope: any file outside the registered directory, active/unverified
  deletion, multiple backup guarantees, safe eject UI/flow (M12).
- Dependencies: M3 chunks/Catalog and M9 READY identity/state.
- Acceptance: kill -9 during copy, verify and both sides of Catalog commit;
  simulated unplug; checksum mismatch; existing same/mismatched target;
  deletion failure; every failure retains internal source; verified success can
  release local space; operations remain inside registered directory.
- Rollback: stop new archive work, reconcile in-flight transactions, retain all
  internal sources not fully committed, revert; never mass-delete target data.

## M11 — Space alerts and exhaustion forecast

- Scope: internal/per-target space states; 1 h/6 h/24 h/7 d robust growth rates;
  40%/15%/`max(10 GiB,5%)` thresholds and UTC ETAs; backlog/oldest age; alerts;
  emergency policy; `binance-market-recorder storage forecast`.
- Non-scope: silent data deletion, changing filesystems, claiming forecast when
  history is insufficient.
- Dependencies: M8 persisted metrics and M10 archive/delete rates.
- Acceptance: synthetic positive/negative/insufficient/multi-window rates and
  archive-on scenarios; no NaN/infinity; exact `INSUFFICIENT_DATA` and
  `NOT_APPROACHING`; emergency suspends non-core work, prioritizes verified
  archive, seals/stops before hard reserve and opens explicit gap without
  filling disk.
- Rollback: disable forecasts/alerts only after preserving conservative hard
  stop; revert algorithm and rebuild history-derived output.

M11's 40%/15%/percentage-based thresholds remain the accepted historical
local-profile implementation contract. ADR-0028 prospectively selects the
explicit 18/14/12/10 GiB plus ETA policy for the future shared VPS; this does
not rewrite M11 acceptance evidence.

## M12 — macOS safe eject

- Scope: `binance-market-recorder storage eject <id>`; block new allocation;
  wait/cancel work;
  fsync/transaction completion or rollback; close handles; Disk Arbitration
  unmount/eject; safe-to-remove result; busy/refusal/forced-removal recovery.
- Non-scope: forced filesystem manipulation, format/repair, claiming success
  before system confirmation.
- Dependencies: M9 Disk Arbitration and M10 transactions.
- Acceptance: idle, COPY, VERIFY, system refusal, forced unplug, and reinsertion
  tests; internal source never lost; busy immediate request is refused or
  explicitly waits; current transaction reconciles. M12 chooses immediate
  structured `BUSY`; the existing idempotent archive retry completes work
  before a repeated eject request.
- Rollback: disable eject command while preserving passive disappearance
  recovery; users may use macOS eject; revert M12.

## M13 — Blue/green upgrade and gap-free planned deploy

- Scope: versioned instances; independent candidate connection; snapshot/book
  sync and health readiness; old/new overlap; duplicate tagging/dedup support;
  guarded old shutdown; rollback/audit; reuse for 24-hour rotation.
- Non-scope: hiding overlap, stopping old on unready candidate, GUI deploys, or
  unplanned-fault guarantees beyond explicit gap marking.
- Dependencies: M4-M6 health/reconstruction, M8 metrics, M3 raw provenance.
- Acceptance: failed/unready candidate leaves old running; readiness proves
  current connections and persisted events for all core streams, a persisted
  public snapshot, and market-specific book sync; synchronized candidate
  permits cutover only after fresh post-readiness old/new events; overlap
  duplicates are identifiable; deployment transitions are durable; reverse
  rollback and pre-24-hour rotation use the same gate; no unmarked planned gap;
  no GUI.
- Rollback: stop candidate and retain old version; preserve overlap artifacts
  and deployment log; revert supervisor changes after safe state.

## M14 — launchd, logs, and MacBook power risk

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M14.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

- Scope: user LaunchAgent plist; install/uninstall/start/stop/status scripts;
  stdout/stderr; auto-restart; SIGTERM; permissions; multi-instance prevention
  compatible with supervised overlap; sleep-risk detection; optional scoped
  prevent-sleep assertion; documentation of lid-close limit.
- Non-scope: root LaunchDaemon, permanent power-setting changes, or promise of
  closed-lid capture.
- Dependencies: M13 lifecycle, M8 status, M3 safe sealing.
- Acceptance: login auto-start and machine-reboot-then-login recovery; current
  session launchctl bootstrap/crash-restart/bootout proof; CLI status validates
  PID/heartbeat; no root; SIGTERM seals/stops; sleep/wake gap is explicit; a
  kernel service lock permits only in-process managed blue/green overlap;
  scoped power assertion cleans up and no persistent power setting changes.
  If a disruptive reboot window is unavailable, record that gate as unrun with
  no claim of physical reboot evidence; this does not waive the V1 criterion.
- Rollback: unload LaunchAgent, stop/seal service, restore prior manual launch
  path, revert M14; no user power setting remains changed.

## M15 — Compression, normalized Parquet, and rebuildable data

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M15.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

- Scope: operationalize/version sealed Raw compression from ADR-0002 without
  in-place mutation; normalize every Spot/UM stream; Parquet UTC date/hour
  partitions; schema/version/source hashes; deterministic dedup and gap
  propagation; checkpoints; rerunnable output; DuckDB smoke query.
- Non-scope: strategy/factors/backtests, hiding gaps, mutation of Raw, or
  filesystem-location coupling in consumer output.
- Dependencies: M3 format, M4-M7 schemas/quality, M13 overlap semantics.
- Acceptance: repeated same Raw produces logically identical versioned data;
  each partition traces to source hashes; blue/green overlap resolves
  deterministically; gaps remain visible; Raw hashes unchanged; DuckDB query
  succeeds.
- Rollback: delete only versioned derived/compressed copies proven rebuildable,
  retain Raw, revert M15 and regenerate with old version.

## M16 — Replay interface and generic consumer data contract

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M16.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

- Scope: receive/exchange-time replay and event clock; market/stream/time reads;
  checkpoint seek; explicit gap policy; generic manifest query/dataset version;
  generic `docs/consumer_contract.md`; independent example consumer; optional
  read-only validation that Alpha101Crypto can connect as an ordinary consumer.
- Non-scope: Recorder strategy, factor, BacktestRunner change, live trading, or
  modifying any consumer repository. The optional named-consumer validation is
  not a V1 gate and cannot add reverse dependencies or specialized core fields.
- Dependencies: M15 datasets, M6 checkpoints, ADR-0004.
- Acceptance: identical input produces identical total order; any consumer does
  not know archive mountpoint; generic Catalog/manifest APIs resolve locations;
  the independent example uses only published contracts; no named consumer is
  required for V1 completion.
- Rollback: retain prior reader/version, revert new API/example, leave Raw and
  datasets intact.

## M17 — Short-term reliability and fault injection

- Scope: Spot bootstrap boundary correction; Spot snapshot rate-limit
  containment; network/DNS/Binance close/serverShutdown;
  missing/duplicate/out-of-order depth; Collector/Archive kill -9; Catalog
  lock/transaction fault; local space; missing/read-only external disk;
  checksum mismatch; sleep/wake; failed blue/green; physical normal archive,
  safe eject, pull-during-copy and idempotent reinsertion recovery; complete
  offline engineering gates.
- Non-scope: 72-hour/168-hour continuous-operation proof, release packaging, or
  claiming production/trading readiness.
- Dependencies: all M1-M16 acceptance gates and representative macOS hardware.
- Acceptance: complete required short-term fault matrix; Spot bootstrap and
  rate-limit regressions; physical external normal/error paths; no wrong source
  deletion or false archive commit; complete offline pytest, Ruff, strict mypy,
  contract verification, independent Go Raw golden verification and clean
  diff. Record 72-hour/168-hour proof as explicitly unexecuted and deferred.
- Status statement: short-term functionality and fault injection passed;
  continuous 72-hour and 168-hour operation was not executed. This does not
  make the version suitable for real-money trading.
- Rollback: return to last accepted version, preserve all test/run evidence and
  Raw, and repeat affected short-term tests after correction.

## M18 — Mac Developer Preview

- Scope: `0.1.0a1` wheel/sdist and SHA-256 manifest; CLI/version/commit
  provenance; logged-in-user LaunchAgent install/start/stop/status/uninstall;
  uninstall-with-data-retention proof; concise macOS quickstart, architecture,
  data/storage, operations and limitations documentation; clean-environment
  install verification; short public-data Spot/USD-M smoke; focused safety
  review and complete test report.
- Non-scope: Production/Stable/Trading Ready claims, remote publication, PyPI,
  GitHub Release, 72-hour/168-hour run, GUI, strategy, backtest, trading,
  accounts/keys, other exchanges, Ubuntu or Windows implementation.
- Dependencies: accepted M17 short-term gates and every prior
  compatibility/operations contract.
- Acceptance: reproducible wheel/sdist; verified CLI; rootless LaunchAgent
  lifecycle whose uninstall preserves data and whose reinstall reads Catalog;
  5–15-minute independent-data-root Spot/USD-M smoke; focused boundary review;
  complete offline pytest/Ruff/strict-mypy/contracts/Go-golden/build/install
  evidence; all skips listed; Developer Preview and long-run warning in every
  required release surface.
- Rollback: uninstall only the candidate LaunchAgent/code environment, preserve
  the application data root and Catalog, and reinstall the prior package.

Completion of M18 still stops and waits for explicit human publication/merge.

## M19 — Reliability repair and critical market-data completeness

- Scope: market-local depth resync; fail-fast core terminal recovery;
  restartable/visible side data; truthful RSS gauges; Spot exchangeInfo; six
  official USD-M latest-closed 5m statistics; revisioned official historical
  backfill; offline macOS CI and coverage contracts.
- Non-scope: strategies, models, backtests, accounts, orders, trading, API
  credentials, live raw-trade/kline streams, L3 fabrication, other exchanges,
  Ubuntu/RK3588, or long-running acceptance.
- Dependencies: accepted M18 Developer Preview; ADR-0011/0012/0022; current
  official Binance documentation, modular SDK, and public-data README.
- Acceptance: A–G audit recorded; every resync and terminal path tested;
  side-task recovery/status tested; public schemas and rate-limit provenance
  tested; archive checksum/revision/404/timestamp/clock/idempotency tested;
  offline pytest plus explicit stress, Ruff, strict mypy, contracts, Go golden,
  build/install and clean diff pass; branch pushed and PR opened without merge.
- Rollback: preserve immutable Live Raw, historical source revisions, Catalog
  evidence and gaps; disable affected side dataset/readiness, revert only M19
  commits, and never substitute third-party or fabricated data.

### M19.1 — Second-review blockers

**Scope.** Explicit official archive filenames and online URL smoke; immediate
  fail-fast for normally returning core collectors; ordered USD-M side-task
  shutdown; durable 5-minute Cursors and bounded catch-up; streaming Parquet
  normalization; safe HTTP Range recovery; truthful recovered book update ID;
  full backfill lineage verification; and repair of the existing macOS CI
  workflow validation failure.
**Non-scope.** Other symbols, credentials, accounts, orders, trading, strategy,
  models, R-034 production-rule changes, new PRs, or merge.
**Dependencies.** The M19 branch and immutable contracts in ADR-0023/0024.

**Acceptance.** Focused regressions, complete offline/stress/toolchain gates,
  explicit unsigned online archive and six-endpoint smoke, same-branch push,
  and a successful existing-PR CI run.
**Rollback.** Revert only M19.1 commits; preserve Raw, historical revisions,
  Catalog Cursors/events, and the open R-034 evidence.

### M19.2 — Normalized consumer contract for M19 Live data

**Scope.** Market-specific Spot/USD-M exchange-info schemas; six USD-M
5-minute schemas; multi-record REST expansion; timestamp-centered semantic
identity; explicit empty observations; and deterministic duplicate/conflict
handling through Raw-to-Parquet.
**Non-scope.** Collector lifecycle, Historical Importer architecture, R-034
production rules, other symbols, credentials, accounts, trading, strategy,
models, platform ports, new PRs, or merge.
**Dependencies.** The M19/M19.1 Live Raw provenance and the immutable
`normalized-dataset.v1` contract in ADR-0020.

**Acceptance.** Offline real-envelope-shaped parser fixtures and full
Raw-to-Parquet tests cover every M19 Live stream, multi-record expansion,
overlap deduplication, conflicts, empty and malformed observations, plus old
stream regression; complete offline/stress/toolchain/install gates pass; the
same PR branch is pushed and its CI succeeds.
**Rollback.** Revert only the M19.2 commit; preserve immutable Live Raw,
Catalog and existing normalized artifacts, and rebuild compatible datasets
from Raw after a corrected parser is available.

## M20 — Ubuntu ARM64/RK3588 transport and native deployment

- Status: **ACCEPTED and merged through PR #3.**
  Final PR Head: `2ebb981ea956929467b6dc4b0990875cc43e53bf`;
  Merge Commit: `80b8a5745fc64ee4e0ed0db7691c3acf7d2567bc`.
  macOS and Ubuntu CI both passed. M20 is closed.
- Scope: one redaction-safe `direct`/`environment`/`explicit` proxy policy for
  every Spot/USD-M WebSocket and REST exit plus Historical Backfill; Linux XDG
  paths; Ubuntu ARM64 dependency proof; non-root systemd service lifecycle;
  discovery and registration of already-mounted Linux external archive
  directories; RK3588 short public-data deployment and Mihomo-restart fault
  evidence.
- Non-scope: new symbols/markets, account/key/order/trading features, Raw or
  normalized schema changes, automatic mount/unmount/format/repair, firewall
  or routing changes, Docker/Kafka/Kubernetes, Linux blue/green certification,
  72-hour/168-hour soak, Production Ready claims, merge, release, or tag.
- Dependencies: M19.2 merged into `main`; Python `>=3.12,<3.13`; existing Raw,
  Catalog, reconnect/resync/gap and archive safety contracts; user-selected
  Mihomo node left unchanged.
- Acceptance: complete offline/stress/lint/type/contracts/Go/build gates;
  clean-venv final-Wheel ARM64 imports; static and real systemd lifecycle;
  direct and explicit public Spot/USD-M REST/WebSocket plus small Historical
  smoke; 30-minute concurrent service run; graceful stop/recovery/restart;
  Mihomo restart causes visible disconnect/reconnect/depth resync or explicit
  unreliable evidence; no proxy URL/credential/production data committed;
  branch pushed, PR opened, Code Reviewed, and merged through PR #3.
  Post-merge handoff completed. macOS Python 3.12 CI pass; Ubuntu Python 3.12
  CI pass.
- Rollback: stop/seal the service, retain `/var/lib` data and Catalog, install
  the prior Wheel/unit/config, and restart. Revert M20 code only; never delete
  or rewrite Raw. Linux blue/green and long soak remain M21 work.

## M21.2 / M21.3 / M21.4 — long-run evidence and recovery stability

- M21.2 completed its formal 72-hour window but failed because the Recorder
  restarted 639 times. Archive and disk evidence passed; the run isn't eligible
  for 168-hour continuation. See
  `docs/milestone_evidence/M21.2-72h-failure-analysis.md`.
- M21.3 was merged through PR #6 at
  `a9db145718338faa49a4e4c57bea9a821e74d828`. Its 2-hour preflight and closed
  12-hour observation passed, but a later USD-M `book_ticker` ingress overflow
  ended PID continuity before the 24-hour gate could start.
- M21.4 was merged through PR #7 at
  `cf1e749c7a533e916dbfb685212e5549a38c70dd`. The production Wheel
  (SHA-256 `926615b09ef46130f49a87fe8ab20acb7cfa6313daa67af5b718931bd95ff329`)
  was deployed and passed Stage D plus the Canonical Installed Identity Gate.
  Formal 2-hour and 12-hour process-stability windows passed with independent
  evidence reviews. The formal 24-hour window passed on process stability
  (corrective integrity review and Backpressure contract forensic review
  confirmed; the natural gen5 backpressure recovery cycle passed its recovery
  contract, RECOVERY_CONTRACT_PASS). M21.4 does not change public data
  schemas or begin a new soak. See
  `docs/milestone_acceptance/M21.4.md`,
  `docs/milestone_evidence/M21.4-ingress-overflow-analysis.md`,
  `docs/milestone_evidence/M21.4-deployment-and-validation.md`, and
  `docs/milestone_evidence/M21.4-24h-validation-forensics.md`.

  **M21.4.9/10/11 — formal 72h FAIL and reconnect boundary repair.** The
  formal 72-hour window's core process stability PASSED (PID 317289,
  NRestarts=0), but its data-integrity contract FAILED: the
  2026-08-07T14:08:24Z USD-M `book_ticker` unexpected disconnect reconnected
  within the same generation with no gap evidence, and every planned rotation
  seals `gap=false/complete=true`. FORMAL_72H_RESULT=FAIL,
  eligible_for_next_stage=false. The 12h/24h data-integrity acceptance is
  SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING (process stability stands). A
  corrected boundary-local read-only audit found 4,680 unmarked reconnect
  boundaries (11 explicit, 0 ambiguous). The M21.4.11
  repair (`fix/m21-4-reconnect-boundary-integrity`) implements the unified
  Reconnect Boundary state machine, manifest-level `reconnect_gap`, seal
  defense, and the read-only audit tool; the M21.4.11-R1..R5 correction adds
  crash-durable STARTED-before-seal ordering, boundary-local audit
  classification, strictly read-only audit semantics, side-data fail-closed
  terminal restart, and deterministic canonical audit output.   R2/R2.1/R2.2
  further add SEALING seal-intent recovery keyed by exact gap lifecycle,
  Catalog-first zero-record marker durability, exact operational-event
  idempotency, and logical audit transitions across frame-less chunks. It is
  merged to `main` through PR #11, but NOT DEPLOYED. The merged code state is
   distinct from deployed/production-validated state.
  Full record:
  `docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md`.

  **M21.4.11 formal 72h PASS and M21.4.11-R3 orphan extension-intent P1.**
  The deployed artifact (`f659895…`) passed its independent formal 72h
  observational window: process continuity, 27/27 explicit WS transitions,
  +0 unmarked, 0 false-complete, 27/27 first-new Raw `sequence_gap`.
  FORMAL_72H_RESULT=PASS. The same review found a latent P1: a reconnect
  boundary that merely EXTENDS an open pending gap persisted a SEALING seal
  intent with a freshly minted gap_id and no lifecycle; startup recovery
  scans every historical SEALING intent and would materialize a phantom
  `STREAM_DISCONTINUITY_STARTED` on the next service restart (production
  example: um_perpetual `book_ticker` 2026-08-13T08:20:35Z, orphan gap_id
  `33e6420b`, marker `7223d5ba`, parent `70ace625`). The 168h gate
  requires a controlled service restart, so
  ELIGIBLE_FOR_168H=false for `f659895…`. The M21.4.11-R3 correction makes
  extension intents reuse the canonical pending-gap identity (attempt
  metadata under a separate `extension` key) and teaches startup recovery
  to recognize legacy orphan shapes from durable evidence without phantom
  materialization; REQ-103 intent-only crash recovery is preserved. An
  independent exact-head review (PR #11 R2) rejected the R3 closed-parent
  legacy discriminator as P1-unsound because it used UTC wall-clock
  containment plus generation equality as causal proof, which wall-clock
  rollback can defeat for a genuine post-completion boundary.
  M21.4.11-R3.1 corrects it with clock-independent durable identity rules
  (frame-bearing SEALING evidence, boundary connection equal to the
  parent's completing connection, generation identity), fail-closed
  ambiguity, and an explicit operator-reviewed additive classification
  authority; UTC was removed as silent-suppression authority. A further
  independent exact-head review (PR #11 R3.2) found three P1s in R3.1:
  UTC containment still gated whether CLOSED-parent ambiguity engaged
  (an orphan outside the parent's numeric interval could still become a
  phantom STARTED, and inverted-wall pairs were dropped from the
  interval universe); the classification authority was consulted before
  stronger durable proofs and could therefore override them; and no
  deterministic read-only production pre-start inventory existed.
  M21.4.11-R3.2 corrects all three with the exhaustive three-way
  partition (PROVEN_LEGITIMATE / PROVEN_EXTENSION / AMBIGUOUS, no
  fourth default), the strongly-bound authority
  (`legacy-reconnect-classification.v2`: chunk_id + canonical
  seal-intent SHA-256, consulted only for AMBIGUOUS candidates, and
  contradictions fail closed), the shared decision engine, the read-only
  `recovery legacy-reconnect-preflight` command, the two-phase startup
  (global pre-decision before any legacy lifecycle mutation), and the
  mandatory pre-start classification sequence documented in
  `docs/ubuntu_rk3588_operations.md`. UTC never gates classification.
  A third independent exact-head review (PR #11 R3.2) rejected it:
  REV-001 (P1) the "no possible parent → proven_legitimate" proof is
  unsound because malformed/unkeyable historical lifecycle authority can
  disappear from the searched universe; REV-002 (P1) the authority digest
  bound only chunk_id + seal intent, not `verified_frames`, although
  verified_frames drives classification; REV-003 (P1) the documented
  `root:root 0600` authority mode is unreadable by the production
  service (User=orangepi Group=orangepi); REV-004 (P2) the "read-only"
  preflight called `ensure_storage_layout()` and could mkdir/fsync
  missing directories. M21.4.11-R3.3 corrects all four: the legacy
  no-parent absence proof is REMOVED (absence of a parent only widens
  uncertainty; automatic legitimacy for legacy intents requires positive
  proof — trustworthy `verified_frames > 0` or the exact
  completing-connection proof — everything else is AMBIGUOUS); new
  intents emitted by the corrected runtime carry the durable
  `intent_schema: reconnect-seal-intent.v2` provenance (persisted inside
  the immutable SEALING evidence; pure extensions reuse the pending gap
  identity, decision-point-2 uses a fresh genuine gap, so a versioned
  fresh ABSENT intent safely materializes REQ-103 without operator
  classification; unknown future schemas fail closed); malformed
  lifecycle authority is surfaced as explicit degraded-authority
  predecision blockers instead of being silently skipped; the authority
  is bumped to `legacy-reconnect-classification.v3` with
  `classification_evidence_sha256 = sha256(canonical_json({chunk_id,
  seal_intent, verified_frames}))` binding the COMPLETE immutable
  decision evidence; the documented authority installation contract is
  owner=root group=orangepi mode=0640 (service-readable, not
  service-writable) enforced by a deterministic permission-contract
  test; and the preflight is intrinsically read-only (layout derived
  without mutation, exit 0 eligible / exit 2 ineligible with the full
  JSON report). SCHEMA_MIGRATION_REQUIRED=false and
  CATALOG_MUTATION_REQUIRED=false (the intent version field is a forward
  persistent evidence-contract revision, not a SQLite schema migration);
  ADDITIVE_COMPATIBILITY_AUTHORITY_REQUIRED=true and
  PRESTART_LEGACY_CLASSIFICATION_REQUIRED=true for the first corrected
  production start. The R3.3 focused exact-head review accepted the core
  algorithm but returned one P1 and two P2s. M21.4.11-R3.4 closes them
  narrowly without touching the accepted algorithm:
  REV-003-R3.3-001 (P1) — the authority file moved OUT of the
  service-writable data root into the root-controlled configuration
  namespace (`config_file.parent /
  legacy_reconnect_classifications.json`; production
  `/etc/binance-market-data-recorder/…`, parent root:orangepi 0750,
  file root:orangepi 0640), so the service can read but can never
  unlink/rename/replace the authority pathname (file mode 0640 alone
  was insufficient because the service owns the data-root directory);
  the permission-contract test now models both file and parent
  directory; R3.3-SCHEMA-001 (P2) — an explicit `intent_schema: null`
  (or any present-but-noncanonical value) now fails closed instead of
  being treated like a missing legacy key; R3.3-DOC-001 (P2) — the
  Ubuntu operations status now records that the deployed `f659895…`
  PASSED the formal 72h observational gate and became NOT ELIGIBLE FOR
  168H, while the corrected artifact is NOT DEPLOYED with validation
  PENDING. It is
  merged to `main` through PR #11 and remains NOT DEPLOYED. Production validation for the corrected
  artifact is PENDING: after review and separately authorized
  deployment, the NEW artifact must re-execute the full staged chain
  (exact artifact identity → readiness → 2h → 12h → 24h → 72h → 168h).
  See ADR-0027 "Pending-gap extensions and orphan seal-intent prevention /
  Legacy extension-orphan recovery (M21.4.11-R3, corrected R3.1,
  corrected R3.2, corrected R3.3, corrected R3.4)".

  The validation sequence continues: after the repair is reviewed, merged,
  and separately authorized for deployment, the NEW artifact must re-execute
  deployment canonical identity → readiness → 2h → 12h → 24h → 72h → 168h.
  The old artifact's windows are historical reference only. The 168-hour
  window is never started automatically.

## D0/D1 — VPS and remote archive architecture freeze

Status: **DOCUMENTATION-ONLY ARCHITECTURE FREEZE**. Approved by ADR-0028,
ADR-0029, and ADR-0030; implementation and deployment are not included.

- Primary production target: Ubuntu 24.04 LTS x86_64, Python 3.12, systemd,
  non-root Recorder service, 2 vCPU/4 GiB/40 GB-class shared VPS.
- Live VPS role: public acquisition, Raw spool/seal, Catalog, recovery,
  integrity/gap/provenance state, metrics/status, and archive export support.
- Offline role: local Normalize, heavy Replay/analysis, and Historical Backfill
  using the same Recorder distribution and the Offline Workspace.
- Archive direction: local client pulls from VPS over SSH through a replaceable
  `RemoteTransport` seam; durable local verification and receipt authorization
  precede immediate VPS source deletion.
- Archive Set: `archive_set_id` is logical collection identity;
  `storage_id` is one physical medium; chunks remain whole and media metadata
  is self-describing/rebuildable.
- VPS capacity policy: WARNING/CRITICAL/EMERGENCY/HARD RESERVE at 18/14/12/10
  GiB, with ETA triggers at 7 days/72 hours/24 hours.
- Acceptance roles: MacBook development, remote Linux integrated failure
  acceptance, and exact VPS staged acceptance `2h -> 12h -> 24h -> 72h ->
  168h`. The preferred current M22.8 topology is Mac/local archive operator
  -> Internet/SSH -> the Germany Ubuntu VPS -> an isolated disposable M22.8
  test workspace. LAN Linux remains a valid separate Linux validation profile,
  but physical LAN topology is not an M22.8 acceptance invariant.

See `docs/vps_operations.md`, `docs/archive_transfer_contract.md`,
`docs/offline_workspace.md`, and `docs/test_environment_matrix.md`.

## M22 — VPS / remote archive implementation sequence

M22 is the implementation sequence for the approved D0/D1 architecture. It
does not reopen ADR-0028, ADR-0029, or ADR-0030. Exactly one M22 milestone is
executed per run and per local commit; later M22 milestones never begin
automatically.

### M22.0 — Implementation decomposition / acceptance freeze

- **Status:** **ACCEPTED** as documentation-only by the acceptance record
  `docs/milestone_acceptance/M22.0.md`.
- **Scope:** Freeze M22.1 through M22.9, including the accepted durability,
  state-boundary, Catalog-compatibility, M22.4 split, M22.7 split, and
  M21-to-M22 validation corrections.
- **Non-scope:** All production code, remote transport, Archive Set writes,
  receipts, source deletion, schema migrations, deployment, and acceptance
  windows. D0/D1 architecture is not redesigned.
- **Dependencies:** Accepted ADR-0028, ADR-0029, ADR-0030 and the existing
  same-host M10/M11 implementation and contracts.
- **Acceptance:** The authoritative sequence below, its non-scopes and
  gates, the final-artifact validation policy, and the compatibility boundary
  are documented; targeted documentation checks pass; M22.1 remains not
  started.
- **Rollback:** Revert this documentation commit only. Preserve the approved
  D0/D1 ADRs, existing same-host behavior, Raw data, and historical M21
  evidence.

### M22.1 — Read-only remote source identity / export kernel

- **Status:** **IMPLEMENTED**; acceptance evidence is recorded in
  `docs/milestone_acceptance/M22.1.md`.
- **Scope:** Select only immutable sealed Raw; produce a deterministic,
  versioned source descriptor binding chunk, size/hash, manifest identity,
  market/stream, source artifact/path, and required version identity; reject
  partial, unsealed, quarantined, or mismatched sources; provide a
  transport-neutral read-only surface. The implementation validates the
  exact manifest bytes, sealed artifact size and hashes, and a final Catalog
  identity/state snapshot before returning the descriptor/export result.
- **Non-scope:** Source-lifecycle mutation, SSH, Archive Set writes, receipts,
  deletion authorization, and source unlink.
- **Dependencies:** M22.0 acceptance and existing Raw/manifest/Catalog
  contracts.
- **Acceptance:** Deterministic descriptor fixtures and mismatch/eligibility
  tests pass with no source or Catalog lifecycle mutation; the focused,
  related, and full offline validation gates pass.
- **Rollback:** Revert M22.1 implementation while preserving immutable Raw and
  manifests; no source deletion is permitted.

### M22.2 — Archive Set / physical-media durable identity

- **Status:** **IMPLEMENTED**; acceptance evidence is recorded in
  `docs/milestone_acceptance/M22.2.md`.
- **Scope:** Implement `archive_set_id` as logical collection identity and
  `storage_id` as one physical medium; whole-chunk placement, self-identifying
  media-local metadata, rebuildable global index, and explicit Archive !=
  Backup semantics.
- **Non-scope:** Striping, replication, RAID, backup guarantees, VPS source
  deletion, and changes to Raw/public data schemas.
- **Dependencies:** M22.1 read-only source identity and existing storage
  registration boundaries.
- **Acceptance:** Multi-medium identity, collision, detach, rebuild, and
  whole-chunk tests pass; no VPS lifecycle mutation occurs.
- **Rollback:** Revert only additive Archive Set metadata/index changes; media
  artifacts and existing storage identities remain interpretable.

### M22.3 — Local receive / verify / durable publish / receipt

- **Status:** **IMPLEMENTED**; implementation-branch acceptance evidence is
  recorded in `docs/milestone_acceptance/M22.3.md`.
- **Scope:** Initially use fake/in-process transport and source providers.
  The publication gate is: transaction-owned temporary artifact; file
  durability; close/reopen full readback; stored-size and SHA-256 verification;
  Raw manifest/source identity verification; atomic publication; durability
  of the final published artifact namespace, including required parent
  directory metadata durability where supported; durable Archive Set and
  physical-media metadata commit; durable receipt commit; and durability of
  the receipt namespace/containing metadata required for crash survival.
  Only after all gates may a receipt become capable of participating in VPS
  deletion authorization.
- **Non-scope:** VPS source deletion, SSH, deletion authorization, and any
  claim that a pre-rename fsync alone proves final pathname durability.
- **Dependencies:** M22.1 and M22.2 acceptance.
- **Acceptance:** Kill-point and fault tests prove every failed or
  unsupported durability property fails closed and produces no
  deletion-capable receipt; successful artifacts and receipts are
  independently revalidated.
- **Rollback:** Retain VPS sources and local artifacts/metadata; revoke only
  uncommitted or non-authorization-capable local transaction state.

### M22.4A — Remote deletion persistence / Catalog compatibility / recovery model

- **Status:** **IMPLEMENTED** on the dedicated feature branch; acceptance
  evidence is recorded in `docs/milestone_acceptance/M22.4A.md`.
- **Scope:** Non-destructive, additive remote persistence authority; exact
  remote transitions `SEALED -> REMOTE_DELETE_PENDING -> REMOTE_DELETED`;
  pre-M22 Catalog to new-binary compatibility; separate remote recovery
  interpretation and CASE A/B/C/D decision engine; audit explicit
  `ChunkState` consumers; capacity/backlog/source-presence semantics;
  same-host/remote mutual exclusion; and idempotency. A separate remote
  deletion persistence projection owns remote transaction/source/receipt,
  Archive Set, storage, session, state, authorization, and recovery facts.
- **Non-scope:** Source unlink, destructive filesystem mutation, parent
  directory durability, and any reuse or extension of `ArchiveState`,
  `ARCHIVE_CHUNK_STATES`, or `archive_transactions` for remote semantics.
- **Dependencies:** M22.3 receipt authority and M22.0 state/compatibility
  freeze.
- **Acceptance:** Old Catalogs open under the new binary; remote states are
  handled through the separate projection; old binaries are not claimed safe
  after remote states persist; CASE A/B/C/D and mutual-exclusion tests pass;
  remote authority is validated before selection/backlog/reporting effects;
  descriptor-derived report identity is reconstructed from retained manifest
  evidence; initial events bind their own receipt; malformed partial indexes
  and weakened state/timestamp checks fail closed; no filesystem mutation
  occurs. Corrections implemented; independent focused closure review remains.
- **Rollback:** Disable remote persistence/authorization paths and preserve
  existing same-host rows and meanings; do not downgrade by relabeling remote
  states as `LOCAL_DELETED`.

The implementation uses two additive tables in the existing Catalog SQLite
database. `chunks.state` remains `SEALED`; neither `ChunkState`, `ArchiveState`,
`ARCHIVE_CHUNK_STATES`, nor same-host transition meaning changes. M22.4A can
persist only `REMOTE_DELETE_PENDING`. The schema can represent
`REMOTE_DELETED`, but no production terminal write API or source filesystem
mutation exists; those remain exclusively M22.4B scope.

### M22.4B — Remote source deletion / durability / kill-point recovery

- **Status:** **IMPLEMENTED** on the dedicated feature branch; acceptance
  evidence is recorded in `docs/milestone_acceptance/M22.4B.md`.
- **Scope:** First destructive remote milestone: exact immediate source
  revalidation, exact unlink, parent-directory deletion durability, terminal
  remote Catalog transition, and crash/kill recovery for CASE A/B/C/D.
- **Non-scope:** Redesign of M22.4A persistence or state model; transport/SSH;
  deletion without a matching durable pre-delete authorization; and treating
  `REMOTE_DELETE_PENDING` as completed deletion.
- **Dependencies:** Independently accepted M22.4A and M22.3.
- **Acceptance:** Authorized deletion reaches `REMOTE_DELETED` only after the
  held exact Raw file object passes full stored/decompressed validation, the
  exact direct leaf is unlinked relative to an anchored parent descriptor, and
  that same parent descriptor is fsynced with post-fsync absence verified.
  CASE B performs recovery-only absence reconciliation; absent source without
  exact durable authorization fails closed; real K1-K5 process-death and
  same-receipt concurrency tests converge idempotently. The Raw manifest and
  all Catalog/receipt/event evidence remain retained.
- **Rollback:** Stop new remote deletion attempts, reconcile only through
  durable M22.4A evidence, and retain any source not proven deleted.

### M22.5 — RemoteTransport + SSH V1

- **Status:** **IMPLEMENTED** on the dedicated feature branch; acceptance
  evidence is recorded in `docs/milestone_acceptance/M22.5.md`.
- **Scope:** Add the transport-neutral `RemoteTransport` seam, then an
  ordinary CLI SSH adapter that moves bytes/messages only.
- **Non-scope:** SSH as deletion authority; subprocess logic inside hashing,
  receipt correctness, eligibility, durable authorization, or recovery
  decisions; custom SSH protocol, restricted account, or API server.
- **Dependencies:** M22.1 through M22.4B acceptance.
- **Acceptance:** In-process and executable SSH-shim tests exercise the same
  M22.1/M22.3/M22.4A/M22.4B integrity semantics. Exact descriptor, manifest,
  Raw, and receipt bytes cross only fixed hidden CLI verbs; process-aware Raw
  EOF requires child exit zero; ambiguous control responses are reconciled by
  the same receipt ID. The side-effect-free system OpenSSH client probe is not
  real sshd, network-transfer, deployment, or production evidence.
- **Rollback:** Disable the SSH adapter and use the accepted fake transport;
  preserve local artifacts, receipts, and remote evidence.

### M22.6 — Post-session Catalog DR snapshot

- **Status:** **IMPLEMENTED** on the dedicated feature branch; acceptance
  evidence is recorded in `docs/milestone_acceptance/M22.6.md`.
- **Scope:** SQLite-supported consistent backup/snapshot after each successful
  session, local transfer/verification, and retention of at least `latest` and
  `previous` snapshots.
- **Non-scope:** Raw replacement, raw-copying a live WAL database, or undoing
  valid Raw archival/deletion when snapshot creation fails.
- **Dependencies:** M22.5 and the M22 remote Catalog state model.
- **Acceptance:** A live non-immutable read-only Catalog is copied only through
  SQLite Online Backup; the remote staging database and transferred standalone
  database independently validate the exact receipt and its pending/deleted
  lower bound. Process-aware EOF, reopened local size/hash, SQLite/Catalog
  validation, immutable generation publication, and mirrored crash-recoverable
  latest/previous retention all gate success. Snapshot failure reports that the
  archive session is already committed and retries only the same receipt's
  snapshot operation.
- **Rollback:** Retry or disable snapshot transfer while retaining valid Raw,
  manifests, receipts, and deletion evidence.

### M22.7A — Named VPS capacity / ETA profile

- **Status:** **IMPLEMENTED** in the M22.7A acceptance record.
- **Scope:** Select the named VPS profile with 18/14/12/10 GiB states and
  7-day/72-hour/24-hour ETA/action thresholds.
- **Non-scope:** Redesigning generic ADR-0016 `threshold_bytes()` or
  `space_severity()` behavior for macOS, RK3588, or local profiles.
- **Dependencies:** M22.0 freeze and existing forecast/emergency behavior.
- **Acceptance:** VPS-profile thresholds, ETA actions, shared-host semantics,
  and historical generic behavior are independently tested and documented.
- **Rollback:** Disable only the named VPS profile and retain generic/local
  threshold behavior and fail-closed hard-reserve actions.

### M22.7B — VPS systemd / readiness / exact deployable artifact identity

- **Status:** **IMPLEMENTED / REAL-HOST VALIDATED / ACCEPTED**; the offline,
  static, and corrected real Ubuntu host evidence is recorded in
  `docs/milestone_acceptance/M22.7B.md`. M22.7B acceptance is the deployment/
  readiness host gate only; no production deployment or Production Ready claim
  is made.
- **Scope:** Ubuntu 24.04 LTS x86_64, Python 3.12, non-root systemd,
  shared-host requirements, exact deployable artifact identity, readiness,
  deployment, and rollback contract.
- **Non-scope:** Capacity algorithm redesign, VPS provisioning ownership,
  Docker/Kubernetes, and production acceptance windows.
- **Dependencies:** M22.4B, M22.6, and M22.7A acceptance.
- **Acceptance:** Artifact identity, the exact installed lock distribution set,
  protected venv/release control chain, effective systemd properties, closed
  direct-network process environment, readiness gates, service ownership and
  resource boundaries, hard-reserve clean exit, real unit lifecycle,
  journald, restart, and fail-closed rollback compatibility preflight are
  accepted on the recorded Ubuntu host. Actual artifact rollback and all
  staged production windows remain unrun. M22.7B acceptance is not Recorder
  Production Ready.
- **Rollback:** Stop the candidate, restore the prior immutable artifact/unit
  and retain all data and Catalog state.

### M22.8 — Remote Linux Integrated Failure Acceptance

- **Status:** **IMPLEMENTED / REAL CROSS-MACHINE FAILURE VALIDATED / ACCEPTED**;
  the fixed-run evidence and final audit are recorded in
  `docs/milestone_acceptance/M22.8.md`.
- **Scope:** Mac/local archive-operator integration across a real
  SSH/network boundary to a remote Linux source environment: remote source
  identity, archive transfer, receiver storage and verification, durable
  receipt, deletion authorization, authorized remote deletion, response-loss
  recovery, process-crash recovery, network/SSH interruption, retry and
  idempotency, Catalog DR snapshot, and storage-failure evidence.
- **Preferred execution topology:** Mac/local archive operator ->
  Internet/SSH -> the Germany Ubuntu VPS -> an isolated disposable M22.8 test
  workspace. The physical network being a LAN is not an acceptance invariant;
  a real remote Linux environment is.
- **Isolation:** The M22.8 workspace may share the physical VPS with the
  production-target environment only when its filesystem, workspace, config,
  and test identities/state are independently identifiable and disposable.
  Failure injection must not operate on production Raw, production Catalog,
  production receipts, production deletion authorization, production remote
  ownership state, or accepted M22.7B deployment evidence. The test domain
  independently identifies its Raw, Catalog, source descriptor, Archive Set,
  receipt, deletion, and snapshot state.
- **Non-scope:** M22.9 exact VPS staged production acceptance, its duration
  windows, production promotion, LAN bandwidth/latency or home-router
  behavior as standalone requirements, and relabeling historical RK3588/LAN
  evidence as VPS or M22.9 evidence.
- **Dependencies:** M22.1 through M22.7B acceptance.
- **Acceptance:** The integrated cross-machine remote-source/archive failure
  matrix passed in fixed run `m22.8-20260822T041913Z-23f1fcc7` at exact main
  `f699b6dc0e3e9e2d193eb9fd25321c59526995cd`: 9/9 scenarios passed, all
  milestone-wide gates passed, and production remained untouched. Two
  evidence-packaging P2 findings remain nonblocking. M22.8 evidence never
  counts as M22.9 duration evidence, even when the same physical VPS is used.
- **Rollback:** Stop the test deployment and preserve evidence/data; revert
  only test-profile changes.

### M22.9 host-maintenance quiet-window preflight

- **Status:** `COMPLETE`. The 2026-09-13 restart-from-scratch attempt passed;
  Formal credit remains zero and 12 hours is not started.
- **Maintenance:** The six pending Ubuntu packages were installed before any
  T0. The final simulation reports zero pending upgrades, `dpkg --audit` is
  empty, and no reboot is required.
- **Quiet-window gate:** Both apt timers, both apt services, and
  `unattended-upgrades.service` were stopped and `masked-runtime`. An explicit
  start probe was rejected; the bounded hold retained boot/systemd identity
  with no maintenance activation or reexecution. Runtime masks were then
  removed and normal enabled/active update authority was restored.
- **Recorder/archive:** Recorder remained inactive and disabled, no observer
  or stage was created, the archive timer remained enabled/active, Catalog
  integrity is `ok`, backlog/pending/partial counts are zero, and the
  registered archive target is READY.
- **Next:** One `FORMAL_M22_9_2H_QUIET_WINDOW_RETRY` after separately
  authorized fresh identity, readiness, archive, capacity, package/lock, and
  runtime-mask gates. Never resume the prior incomplete roots and never start
  12 hours automatically. See
  [`M22.9 host-maintenance quiet-window preflight`](milestone_acceptance/M22.9-host-maintenance-quiet-window-preflight.md).

### M22.9 root-home recovery and handoff closeout

- **Status:** `COMPLETE`. The first maintenance quiet-window preflight is
  `ABORTED_UNACCEPTED`; neither of its partial evidence roots is resumable or
  acceptable. Formal credit remains zero and 12 hours is not started.
- **Recovery:** GreenCloud password reset and temporary VNC restored root
  access without rebuild. `/root` and the dedicated VPS SSH key were restored;
  the temporary recovery key was removed and VNC was disabled. Production
  authorities under `/etc`, `/opt`, `/var/lib`, and `/srv` survived. Prior
  root-home-only content is unavailable.
- **Reboot behavior:** The enabled Recorder auto-started for approximately six
  minutes after boot, then stopped successfully. It created no observer or
  Formal stage. Recorder is now inactive and disabled; the archive timer is
  still enabled/active.
- **Permanent safety gate:** No remote recursive cleanup may target a variable
  that crosses an SSH expansion boundary. Cleanup is normally omitted; when
  required it uses a fresh exact child under an allowed disposable parent,
  same-shell nonempty/canonical-prefix guards, explicit protected-path refusal,
  and a separate reviewed cleanup step.
- **Next:** Restart
  `FORMAL_M22_9_HOST_MAINTENANCE_QUIET_WINDOW_PREFLIGHT` from scratch with a
  fresh evidence root. Keep Recorder stopped/disabled and do not create T0.
  See
  [`M22.9 VPS root-home recovery`](milestone_acceptance/M22.9-vps-root-home-recovery.md).

### M22.9-P1 — systemd-detached single-stage observation preparation

- **Status:** **REVIEWED_COMPLETE** in
  `docs/milestone_acceptance/M22.9-P1.md`, based on current main
  `83a063f5bb9f91508238c9fd86d21aa45d1bd501`. This is documentation-only;
  `FORMAL_M22_9=NOT_STARTED`, duration credit is zero, and Production Ready is
  NO. `NEXT=M22_9_P2_EXACT_DEPLOYMENT_ARCHIVE_CAPACITY_PREFLIGHT` is named but
  not started.
- **Scope:** Document an external systemd 255 transient `Type=exec` unit for
  one explicitly selected stage, using the existing foreground
  `AcceptanceObserver`/`deployment acceptance stage` command. The frozen unit
  uses `Restart=no`, `KillSignal=SIGINT`, `TimeoutStopSec=120s`,
  `User/Group=bmdr`, `UMask=0027`, `NoNewPrivileges=yes`, and journal output;
  SSH detachment must not affect the observer.
- **Evidence root:** Use only an operator-selected child under the registered
  relative archive path, for example
  `/srv/recorder-data/recorder-archive/acceptance/m22.9/<id>`. Never use the
  volume root or the active writer root. Resume reuses the exact recorded
  stage child and T0; it does not create another root or stage.
- **Acceptance:** The existing observer remains the sole measurement and
  evidence authority; one-stage status/journal/stop/resume/final-review steps
  are reproducible, and no command advances a later stage. Harmless systemd
  probes are recorded in the acceptance file; they did not touch Recorder or
  Raw.
- **Historical MS4-D read-only check:** At `2026-09-11T06:39:23Z`, Recorder
  was `inactive/dead` (`MainPID=0`, `Result=success`, `NRestarts=0`), the
  archive timer was disabled/inactive, and `/dev/vdb1` was mounted as the
  registered archive target. No start, mutation, or live traffic occurred.
- **P1 current read-only check:** At `2026-09-11T07:27:40Z`, systemd was 255;
  Recorder was `inactive/dead` (`MainPID=0`, `Result=success`, `NRestarts=0`);
  `binance-market-data-archive.timer` was `loaded/disabled/inactive`; active
  `/dev/vda1` was approximately 57.1 GB total with 41.2 GB available; and
  archive `/dev/vdb1` was ext4, approximately 2 TB with approximately 1.9 TB
  available. Catalog storage ID
  `ef852751-721c-4145-9083-f6fd48718480` resolved `READY` at
  `/srv/recorder-data/recorder-archive`. No Recorder/Raw mutation or Binance
  traffic occurred; P1 also ran only the two harmless transient-unit probes.
- **Non-scope / blockers:** No Python, test, supervisor, Contract, timer,
  archive, deployment, Recorder start, Formal stage, source deletion, or
  automatic stage advancement. The current `/dev/vda1` active-root runway
  forecast reaches hard reserve at `2026-09-13T15:11:31.626026Z`, so the
  approximately 278-hour complete-chain capacity precondition remains blocked.

### M22.9-P2 — exact deployment, archive, and capacity preflight

- **Status:** **REVIEWED_COMPLETE** in
  `docs/milestone_acceptance/M22.9-P2.md`. This is a bounded, non-formal
  preflight. `P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES` records that the exact
  P2 deployment source/review base was verified in the installed deployment;
  after this docs-only merge,
  `CURRENT_MAIN_DEPLOYED=NO`; the docs-only merge descendant is not installed.
  At P2 completion the artifact was the Formal candidate; after the failed T0
  it remains only the installed evidence basis pending the scoped fix, review,
  and separately authorized redeploy;
  `RECORDER=STOPPED`,
  `ARCHIVE_TIMER=ENABLED_ACTIVE`. At P2 completion Formal M22.9 was
  unstarted; the later 2-hour attempt and current status are recorded above.
  `FORMAL_M22_9_CREDIT_SECONDS=0`, and `PRODUCTION_READY=NO`.
- **Exact identity:** P2 deployment source/review base
  `646792f2e5fc5b7195ea58541d3f1dfda6555b7f`, tree
  `c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c`; the wheel, lock, config, unit,
  and deployment identity hashes are frozen in the acceptance record. The
  four configured ProductKeys reached readiness with 12 core stream contexts.
- **Archive:** storage ID
  `ef852751-721c-4145-9083-f6fd48718480` resolves to the registered
  `/srv/recorder-data/recorder-archive` directory on ext4 `/dev/vdb1`. The
  one-file canary and timer drain used only the existing verified
  ArchiveManager/Catalog transaction. Backlog reached zero; the one full
  verification reported 112,570 verified files, zero failed, and zero pending.
  The timer is enabled/active/waiting with a future monotonic trigger.
- **Capacity:** the conservative existing 24-hour generation rate is
  `349910.017730 B/s`; the 278-hour projection is `350189945744` bytes, below
  the archive free-space margin of approximately `1771538091120` bytes. The
  active writer root has only about 26.46 hours above its 10 GiB hard reserve
  at that rate. The initial backlog-clearance envelope was approximately
  1.51 MB/s wall-clock, and service-active mean was approximately 1.76 MB/s;
  the full-window 347,073 B/s average includes idle cycles and is not saturated
  capacity. Every Formal stage start/end must recheck these values.
- **Live interaction:** direct unsigned official eligibility was captured for
  Spot and USD-M BTCUSDT/ETHUSDT. Readiness and an approximately five-minute
  non-formal interaction were observed, then Recorder was gracefully stopped
  and the sealed post-stop backlog was drained. P2 itself created no Formal T0,
  P1 observer, long soak, or automatic stage advancement; the later Formal
  2-hour attempt did create T0 and failed there.
- **Evidence and limits:** the VPS evidence bundle and checksum are listed in
  the acceptance file. Corrected-before-start orchestration failures remain
  preserved there. Full local pytest/Ruff/MyPy/build and manual CI were not run
  for this docs-only closeout; deployment pip checks, readiness, archive drain,
  full archive verification, and the authorized capacity forecast were run on
  the VPS. P2 does not certify long-run generation, active-root runway, or
  Production Ready status.
- **Next:** `NEXT=ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST`.
  The diagnosis/fix/offline-test milestone must finish before any separately
  authorized redeploy or Formal retry; no retry is started here.

### M22.9-2h — Formal failed attempt closeout

- **Closeout status:** **REVIEWED_COMPLETE**. The immutable Formal result is
  **EXECUTED_FAILED_AT_T0**. The observer failed before its first
  sample at `2026-09-11T13:41:32.831837Z`; `FORMAL_M22_9_CREDIT_SECONDS=0`.
  The distinct earlier pre-T0 aborted setup root remains preserved and is not
  conflated with this Formal failed root. The 12-hour stage is
  `12H=NOT_STARTED` and `PRODUCTION_READY=NO`.
- **Evidence:** Failed root
  `/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T132845Z-5b4fd719-646792f2`;
  stage root `2h-a8b45b7e1da640c497213b172c235815`; immutable stage-start
  SHA-256 `1c35f62e0673488c441374971e8e8552212e9693aacfba35820ea613ae4a08f6`.
- **Forensic disposition:** Stage-start produced 24 blockers (23 Catalog/
  manifest disagreements plus one unexplained raw absence). The exact
  quiescent review covered 26 unique chunk IDs; all currently resolve to
  verified archive state, with 26 post-T0 archive/local-delete transactions.
  This supports, but does not prove, a concurrent observer/archive snapshot
  race. No per-read interleaving trace exists. The installed read-only
  post-stop audit completed over 112,817 manifests in 115.95 seconds with zero
  Catalog findings, zero integrity findings, and zero chunks with scan issues;
  the additive correction authority supersedes only the original mistaken
  no-result statement.
- **Final VPS state:** Recorder is inactive/dead, `MainPID=0`,
  `Result=success`, `NRestarts=0`, with no active `.partial`; the archive timer
  is enabled/active/waiting, target storage ID
  `ef852751-721c-4145-9083-f6fd48718480` is READY, backlog/pending/failed are
  zero, and Catalog integrity is ok.
- **Closeout evidence:** The compact JSON, human summary, and SHA256SUMS are
  under the failed root's
  `operator-evidence/closeout-review/`. Their paths, digests, ownership, and
  permissions are recorded in
  [`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md).
- **Next:** `ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST`.
  Redeploy and retry remain separately authorized actions.

### AcceptanceObserver/archive concurrency fix — reviewed complete

- **Status:** `ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_FIX=REVIEWED_COMPLETE`.
  The Catalog boundary is captured before the long filesystem scan; one audit
  result owns membership comparison; post-boundary rows are deferred; and
  exact missing-Chunk lifecycle reads use the existing verified archive
  protocol without changing archive state transitions.
- **Offline gates:** Deterministic tests cover post-boundary Catalog/manifest
  deferral, real ArchiveManager `LOCAL_DELETE_PENDING` windows including
  validation-time unlink, unauthorized local absence, external corruption,
  and observer read-only sidecar/tree preservation. Full offline pytest,
  Ruff, MyPy, M0 contracts, and diff checks pass.
- **Boundary:** No VPS, deployment, live traffic, production archive, GitHub
  operation, Formal retry, or duration credit was authorized. Existing Formal
  `EXECUTED_FAILED_AT_T0`, zero credit, `12H=NOT_STARTED`, and
  `PRODUCTION_READY=NO` remain unchanged.
- **Independent review:** Exact code-review commit
  `31cabe4445ee699ad284aa707d24333c78cf8d21` was reviewed by Luna Max against
  base `e214120a25a5aff28fad4903c9510920a25738d3`; P0=0, P1=0, and P2=0.
  The review confirmed boundary deferral, resume-subset validation, the bounded
  112,817-`LOCAL_DELETED` fast path, fail-closed external verification for
  active deletion races, observer read-only behavior, and registered
  READY/LOW_SPACE archive-root resolution. The reviewer could not rerun pytest
  because its read-only sandbox had no usable temporary directory; this is not
  a product failure. Implementer/lead focused tests passed 55 tests, and the
  full isolated offline gates had already passed: 1,652 passed, 24 skipped,
  4 deselected; Ruff; strict MyPy for 254 source files; M0 contracts; and
  `git diff --check`.
- **Residual safeguards:** The 112,817-manifest audit is historical
  performance evidence. Terminal `LOCAL_DELETED` classification intentionally
  relies on durable transaction evidence; full external verification remains
  a separate stage safeguard. No per-read interleaving trace proves the
  original race, the reviewed source is not deployed, and no Formal retry or
  retroactive credit is authorized. **Next:** Exact-artifact redeploy
  preflight; any redeploy or Formal retry requires separate authorization. See
  [`M22.9 observer/archive concurrency fix`](milestone_acceptance/M22.9-observer-archive-concurrency-fix.md).

### M22.9 — Exact VPS staged acceptance

- **Status:** The Formal 2-hour stage is
  `EXECUTED_FAILED_AT_T0`; later stages, including 12h, remain not started and
  Production Ready is NO. The failed-stage record is in
  `docs/milestone_acceptance/M22.9-2h.md`; older M22.9 incident and
  incomplete-stage evidence in `docs/milestone_acceptance/M22.9.md` remains
  historical and unchanged. The pre-MS1 precondition-only inspection stopped
  before T0 with `INSUFFICIENT_MEASURED_CAPACITY_RUNWAY` and is a separate
  historical record.
- **Scope:** Only the final integrated M22 artifact runs exact identity,
  readiness, then independent `2h -> 12h -> 24h -> 72h -> 168h` stages.
- **Non-scope:** Automatic stage advancement, transfer of M21 evidence,
  historical f659895 acceptance, or acceptance of an intermediate artifact.
- **Dependencies:** Accepted M22.8, exact M22 artifact, operator-authorized VPS
  deployment, and readiness evidence.
- **Acceptance:** Every stage has its own T0, target, evidence root, and
  independent review; no stage begins automatically. The current candidate
  receives zero formal duration credit from historical or non-formal runs and
  must execute exact identity -> readiness -> 2h -> 12h -> 24h -> 72h -> 168h
  in a capacity-complete environment. The independent chain totals about
  278 hours; capacity must be resolved before T0.
- **Rollback:** Stop the staged run on failure, preserve evidence and Raw, and
  return to the last approved artifact without deleting unarchived data.

### M23 — Recorder Resource & Throughput Hardening (historical)

- **Status:** **HISTORICAL M23.4 COMPLETE / MERGED / DEPLOYED; M22.9 NOT
  STARTED**. This evidence belongs to its recorded pre-MS1/older artifact and
  is not current-main authority. M23.0/M23.0F profiling and independent
  architecture review authorized the execution-order override. Final M23.4
  correctness review recorded P0/P1/P2/P3 all zero; PR #41 merged as
  `e074d41a…`; post-merge CI passed; the two-hour production-equivalent VPS A/B
  proved structural scan removal, runtime integrity, and material seal-latency
  improvement. The subsequent new-host substrate, 30-minute, 2-hour, 4-hour,
  and 12-hour non-formal stages all passed. M23.1/M23.2 were skipped as
  speculative, and M23.3 was not required before M23.4.
- **Planning sequence:** M23.0 baseline profiling; M23.1 low-risk hot-path
  batching/write and instrumentation reduction; M23.2 allocation optimization;
  M23.3 bounded seal pipeline only if needed; M23.4 clean-seal incremental
  stats/hash with differential/fuzz/crash proof; M23.5 narrow C++ Raw-engine
  decision gate only if Python remains the measured bottleneck; M23.6 much
  later Go v2 decision gate. A full C++ rewrite is not recommended.
- **Evidence philosophy:** optimize algorithms/complexity before language
  rewrites; use production-equivalent profiling and require unchanged Raw v1,
  ordering, gap, and crash semantics. Future research may target sustained
  capacity at least 2x maximum observed production rate, but this is not an
  M22.9 acceptance contract.

#### M23.4 — Incremental clean-seal evidence

- **Scope:** A normal live-owned `RawChunkWriter` derives Raw SHA-256, verified
  byte count, `ChunkStatistics`, and connection transitions from the same
  private immutable semantic snapshot used to encode each exact frame. Only a
  successfully written, final-fsynced, closed, non-poisoned writer can transfer
  one-shot memory-only evidence into the shared durable seal implementation.
  Compression consumption count plus decompressed byte count/SHA-256 remains
  the post-close byte-integrity gate.
- **Non-scope:** Raw/schema changes, Catalog migration, persisted clean
  evidence, recovery redesign, asynchronous or concurrent sealing, queue/
  compression tuning, M23.1/M23.2/M23.3, native code, and M22.9 acceptance.
- **Compatibility:** `seal_partial()` always full-scans arbitrary partials;
  startup/crash/recovered/poisoned/unknown partials retain scan authority. The
  zero-record reconnect marker remains on `seal_partial()`. Raw v1, manifest,
  Catalog schema, completeness/gap semantics, and the durable
  ACTIVE/RECOVERED -> SEALING -> SEALED protocol are unchanged.
- **Evidence:** Differential, bounded randomized, alias-mutation, header-parity,
  poison/fault, source-corruption, scan-routing, reconnect, Raw golden, and
  M22.9 regressions plus post-merge two-hour VPS results are recorded in
  `docs/milestone_evidence/M23.4-incremental-clean-seal.md`.
- **Rollback:** Revert the M23.4 implementation commit. Existing Raw partials,
  sealed artifacts, manifests, and Catalog rows remain compatible and
  recoverable by the unchanged scan-based authority.

### Historical progressive non-formal VPS validation (superseded current authority)

- **Classification:** engineering validation/burn-in, not formal M22.9.
- **Completed checkpoints:** 2h PASS, 4h PASS, and 12h PASS on the new host;
  the 30-minute qualification and substrate gates also passed.
- **Final checkpoint:** clean 24h PASS before MS1; this is artifact-specific
  non-formal evidence and is not Formal M22.9 evidence.
- **Accepted watches:** capacity cadence tail jitter and RSS
  early-growth-then-plateau remain watches for later multi-symbol qualification.
- **Next:** MS2 configurable-product runtime after explicit authorization;
  ADR-0032 supersedes the former fixed-seven-symbol target.
- **Between independent runs:** freeze/hash evidence, analyze, optimize only on
  measured need, then use a separately authorized consistency-safe retirement
  or reset for disposable test data if space must be reclaimed. Never delete
  Raw while Catalog/manifests still reference it.
- **Artifact rule:** if CPU optimization changes production code, build a new
  immutable Wheel and deployment identity. The completed 30m/2h/4h/12h
  duration does not transfer; a new candidate starts a separately selected
  short qualification/progressive sequence. Formal M22.9 receives no duration
  credit.
- **Correctness rule:** CPU work must preserve Raw v1 and exact payload bytes,
  receive timestamps, canonical CBOR, CRC32C, SHA-256, bounded ingress,
  durability/fsync, seal/manifest/Catalog ordering, reconnect/discontinuity
  evidence, gap/resync semantics, crash/recovery, deterministic replay, and
  Spot/USD-M sequence semantics. Longer fsync intervals, CRC/SHA removal,
  float substitution, silent metrics/gap deletion, and unreviewed stream
  merging are not authorized.

The approximately 278-hour formal requirement is not the current next test.
M23.5 is **NOT AUTHORIZED / NOT NEXT** and remains an explicit native-code gate
only if Python is still a measured bottleneck after cheaper algorithmic work.

### M21 -> M22 validation policy

At the M21 handoff the merged PR #11 reconnect correction was not deployed.
It was later included in the M22.9 incident artifact but did not prevent the
fatal post-close-handoff defect recorded above. The previously deployed
`f659895…` artifact passed its historical
formal 72-hour observational gate but later became
`ELIGIBLE_FOR_168H=false` because of the restart-only orphan-intent defect;
that evidence remains historical. M22 intermediate artifacts are limited to
development tests, CI, Mac, and LAN Linux as applicable and are **not
Production Ready**. The final integrated M22 artifact, which includes the
reconnect corrections, must independently execute:

```text
exact artifact identity -> readiness -> 2h -> 12h -> 24h -> 72h -> 168h
```

No M21 or failed M22.9 duration evidence transfers to the newly corrected
artifact, and no stage starts automatically. Historical M21 and M22.9 evidence
remain unchanged.

## MS1 — Multi-symbol durable identity foundation

- **Status:** **MERGED / CLOSED** through PR #51 at
  `d38180074b5f76ab6b7778eea7fc505160c671ae`; reviewed head
  `11e100fbcb974e7d54f0515c99e08ac6042b9204`; post-merge `offline-ci` run
  `33955915046` passed on macOS and Ubuntu. Acceptance evidence is recorded in
  `docs/milestone_acceptance/MS1.md`.
- **Scope:** Make discontinuity identity explicit as
  `(market, symbol, stream)` across Catalog, reconnect, recovery, readiness,
  and audit paths; migrate historical single-symbol discontinuities and
  symbol-specific side-data cursors to `BTCUSDT`; make symbol-specific cursor
  operations explicit; and prove cross-symbol isolation for the frozen Spot
  and USD-M target identities.
- **Non-scope:** Runtime fan-out, multi-symbol startup/readiness, new websocket
  orchestration, REST scheduling redesign, Raw v1 bytes/payload schemas,
  Contracts/protobuf changes, generic arbitrary-symbol abstractions, and
  performance or VPS work.
- **Compatibility:** Existing single-symbol production assembly continues to
  pass `BTCUSDT` explicitly. Legacy Catalog rows without symbol identity are
  migrated atomically and idempotently to `BTCUSDT`; genuinely global side
  data retains global semantics.
- **Dependencies:** Closed M23 hardening stage and the existing Catalog,
  reconnect, Raw, and side-data contracts.
- **Acceptance:** Focused migration, rollback, restart, discontinuity
  isolation, cursor isolation, global-side-data, and frozen-target table-driven
  tests pass; full offline pytest, Ruff, mypy, compile, and diff checks pass.
  No online Binance traffic, deployment, VPS access, or Contracts repository
  change is permitted.
- **Rollback:** Perform a separately reviewed revert of the merged MS1 change /
  PR #51 if needed. Preserve Raw bytes, manifests, archive state, Catalog
  evidence, and production data; do not individually delete or rewrite Catalog
  state.

## MS2 — Configurable product runtime

- **Status:** **CLOSED / OFFLINE ACCEPTED; MERGED THROUGH PR #54**. Evidence:
  `docs/milestone_acceptance/MS2.md`. GitHub `main` at MS4-C review start was
  not deployed.
- **Scope:** Implement the explicit finite `[recorder]` `spot_symbols` and
  `usdm_symbols` lists; `ProductKey = (market, symbol)`; symbol propagation
  through current Spot/USD-M WS/REST/schema/envelope/spool paths; dynamic
  configured Collector assembly; product-aware service state; config-bound
  global readiness; product-aware hard-reserve discontinuity evidence; shared
  USD-M REST authority; the process-global USD-M side-data singleton; and the
  backward-compatible BTCUSDT/BTCUSDT default profile. Resolve that profile
  only when both product-selection fields are absent; when either field is
  present, use supplied lists exactly and resolve an omitted sibling to empty.
  Both resolved lists empty is invalid. An empty USD-M set creates no USD-M
  collectors, side-data managers, global owner, REST polling, or WebSocket
  traffic; an empty Spot set creates no Spot collector or side-data traffic.
- **Non-scope:** Raw v1, existing Catalog/MS1 durable identity, external
  Contracts, Projection, archive format, automatic all-symbol discovery, other
  exchanges, hot reload, unrelated optimization, long burn-in, deployment, or
  a Production Ready claim.
- **Dependencies:** Accepted MS1, ADR-0032, existing Spot/USD-M runtime,
  Catalog, side-data, readiness, and REST-gate contracts.
- **Acceptance intent:** Deterministic/offline proof that the actual runtime
  ProductKey set exactly equals the configured expected set; same stream/`gap_id`
  cannot collide across products; product failure does not alter another;
  product readiness is observable; global readiness is config-bound and fail-
  closed; the shared REST authority is not multiplied; global side data is not
  duplicated; symbol-specific cursors are independent; Spot-only configuration
  implies zero USD-M traffic and no global USD-M owner; and one-process
  shutdown/restart is coherent.
- **Rollback/stop:** Stop before implementation if MS1 identity or existing
  product isolation cannot be preserved. Revert only MS2 code/config after
  sealing and retaining any test evidence; never rewrite Raw.

## Historical plan snapshot — MS3 shared-resource scaling / rotation / observability

> The following MS3/MS4 entries preserve the earlier planning snapshot for
> provenance. They are not the active queue; the current authority is the
> execution ledger and status block near the top of this document.

- **Historical status at handoff:** **OFFLINE REVIEW APPROVED / MERGE PENDING** — MS3-A is merged
  through PR #55; the candidate branch is
  `feat/ms3b-shared-resource-acceptance`; independent re-review/merge is
  pending.
- **Historical work package:** MS3-B — shared REST scheduling/fairness and
  cooldown behavior, bounded Profile D load, product attribution/recovery
  isolation, archive/retry, and aggregate capacity interaction. The updated
  evidence adds real `UsdMCollector`/`RestSideDataPoller` gate and pagination/
  cursor paths, cancellation lifecycle coverage, and a finite mixed
  14-product/42-core-stream running path. Evidence is
  `docs/milestone_acceptance/MS3.md`; independent re-review/merge and live
  qualification remain pending.
- **Scope:** Prove REST scheduling/fairness under multiple configured products;
  shared cooldown behavior; product-aware writer rotation phase; product task,
  log, reconnect, resync, and backpressure attribution; bounded synthetic
  multi-product load; archive/capacity interaction; and optional side-data
  isolation. Prefer runtime/state/log product attribution; persisted metrics
  schema migration is not automatic unless a concrete acceptance need requires
  it.
- **Non-scope:** Speculative optimization without evidence, Raw v1 or
  Contracts changes, exchange/plugin framework, hot reload, deployment, or long
  burn-in.
- **Dependencies:** Independently accepted MS2 and ADR-0032.
- **Acceptance intent:** Model tests establish fairness traces and edge-case
  oracles; production-path tests establish the real Collector/Poller shared
  gate, finite eligible-request termination, page release/reacquisition,
  Catalog cursor isolation, 429/418, cancel/stop, and sibling progress under
  backpressure. The sequential storage Profile D test proves storage-layer
  identity/seal/recovery only; the separate running-path test proves
  simultaneous 14-product activity. No physical host, live Binance, CPU/RSS
  benchmark, or long soak is implied.
- **Rollback/stop:** Stop on fairness, capacity, or isolation regressions;
  revert only MS3 changes while retaining Raw and manifests.

## Historical plan snapshot — MS4 configurable-product integration / bounded live qualification

- **Historical status at handoff:** **NEXT / PLANNED after MS3-B merge**.
- **Scope:** Freeze exact main, run full offline CI, build one new immutable
  Wheel and record source/Wheel/lock/config/unit/deployment identities; after
  separate deployment authorization, run a bounded live qualification using a
  representative mixed Spot/USD-M configured profile with non-BTC products.
  All configured products must be READY, with shared-resource integrity,
  Raw/Catalog/manifest/archive checks, and CPU/RSS/queue/backpressure/capacity
  observation.
- **Non-scope:** Automatic 72h/168h scheduling, inherited single-symbol
  duration credit, Formal M22.9 acceptance, or Production Ready declaration.
- **Dependencies:** Accepted MS2 and MS3, fresh artifact identity, and
  separate deployment authorization.
- **Acceptance intent:** Every identity and live result is artifact-specific;
  a fixed qualification workload is evidence only, not a supported-symbol
  allowlist; a bounded steady-state window may begin after all products become
  ready; any later burn-in duration is independently justified from MS4
  evidence.
- **Rollback/stop:** Stop on any unresolved gap, false readiness, shared-gate
  bypass, integrity failure, or resource exhaustion; preserve evidence and Raw,
  return to the last approved artifact, and do not delete unarchived data.

## Future Work

M21 historical work contains the Ubuntu ARM64/RK3588 soak evidence and recovery
hardening. Future work includes the separately authorized VPS deployment and
staged acceptance, the portable macOS/Linux/Windows archive client, repeated
connection rotations, notifications, and a separately reviewed Web UI. Future
work has no production implementation in this documentation milestone and
does not include strategy, backtest, or trading implementation without a new
human-approved project scope.
