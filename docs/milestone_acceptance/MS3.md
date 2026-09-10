# MS3 — Shared-resource scaling / rotation / observability acceptance

- Date: 2026-09-09; evidence update: 2026-09-10
- Status: **CI REPAIR IMPLEMENTED — AWAITING INDEPENDENT REVIEW**
- Candidate branch: `feat/ms3b-shared-resource-acceptance`
- Candidate PR: `#56` (not merged)
- NEXT=INDEPENDENT_MS3_CI_REPAIR_REVIEW
- MS4: **NEXT**, only after independent review/merge and separate authorization
- FORMAL_M22_9=NOT_STARTED
- PRODUCTION_READY=NO
- CURRENT_MAIN_DEPLOYED=NO

## MS3-CI2 final local validation

Implemented and checked by GPT-6 Astra on macOS / Python 3.12.9, based on
`e6dc3a993defa2c6f24af25209090a55deda2381`. Only one test file and current
review documents changed; production code is unchanged. Exact CI failure was
reproduced before repair with a near-deadline rotation injection (2 manifests
versus assumed 1). The permanent regression uses the actual deadline decision
at the boundary, with no wall-clock wait required to cause rotation.

- New normal/boundary test: 2 passed; ten repeated runs: 20 passed.
- USD-M ingress backpressure plus MS3 production-path files: 46 passed,
  1 stress case deselected.
- Full offline pytest: 1641 passed, 24 skipped, 4 deselected, 13 existing fork
  warnings in 136.41s.
- Ruff, mypy (254 files), compileall, M0 contracts, Go Raw v1 golden and
  git diff --check passed.
- Build/clean-wheel checks were not repeated for this test/document-only delta;
  Ubuntu's successful e6dc3a9 CI includes those checks on unchanged production.
- No new remote CI result is claimed. Online, VPS, deployment, external media,
  stress/soak and Formal M22.9 were not run.

NEXT=LOCAL_LUNA_MS3_CI2_SUBMIT. Commit/push and verify normal required checks
through the local execution workflow. No production repair or architecture
expansion is requested; merge only the exact submitted delta under normal gates.

## Latest CI disposition — MS3-CI2

On `e6dc3a993defa2c6f24af25209090a55deda2381`, run `34424559261`:
Ubuntu passed all gates; macOS failed the older global-stop backpressure test's
single-manifest assertion (actual 2). Profile D passed on both platforms.
Astra reproduced this failure with an injected normal rotation boundary and
implemented a test-only aggregate-manifest correction plus a boundary case.
Local validation passed (1641 passed, 24 skipped, 4 deselected); PR remains unmerged and no deployment is
implied. See the MS3-CI2 entry in milestone_plan.md for current execution scope.

## MS3-CI1 implementation — 2026-09-10 (current disposition)

**IMPLEMENTED / AWAITING REVIEW.** The Ubuntu failure was caused by the Profile
D fixture synchronously waiting for depth persistence inside fake SDK calls
already occupying the shared default executor. Those waits competed with the
42 real stream-writer drains and the one intentionally blocked writer. On a
small-worker schedule, depth work needed to satisfy the fake SDK could remain
queued behind waiting workers until the 2-second sibling poll expired.

The fixture now gates Spot rate-limit admission and the shared USD-M request lock
asynchronously until all 14 real depth streams report persisted data. Snapshot
SDK calls then use their unchanged production paths without holding workers that
wait on writer progress. Real writer observers set explicit events for all 41
sibling second frames and the target third frame; snapshot worker observations
use the owning loop's thread-safe callback. Phase deadlines are 10-second hang
watchdogs rather than the synchronization mechanism. The injected writer's
60-second safety guard exceeds the phase window and cleanup always releases it.

The required 14 products, 42 streams, capacity-one target queues, actual target
backpressure, all 41 sibling advances, target recovery, exact Raw payload counts,
complete/no-gap manifests, Catalog closure and graceful task shutdown remain.
Manifest validation now aggregates exact records across complete chunks because
the production stable-phase rotation may legitimately split the two finite
frames; a local pre-fix repeat observed that valid split once. No production
source, scheduler, thread pool, capacity threshold, Contracts or Projection was
changed.

Validation on Darwin arm64 with Python 3.12.9:

- target default + six-worker variants: 20 repetitions, 40 passed;
- five focused MS3 files: 45 passed in 2.13s;
- full offline suite: 1640 passed, 24 skipped, 4 deselected in 130.32s;
- Ruff, strict MyPy, M0 contracts, Go Raw golden and `git diff --check`: pass;
- sdist/wheel build and clean-wheel `--version`/`doctor`/`status`: pass.

Ubuntu required checks and independent review of this repair delta remain open.
No online/VPS/external-media/stress/soak test ran, and no GitHub CI operation was
manually triggered, rerun, cancelled or awaited. The earlier Astra approval at
`66e036a0` remains historical and does not approve this new test delta.
NEXT=INDEPENDENT_MS3_CI_REPAIR_REVIEW.

## CI failure update — 2026-09-10 (historical trigger)

**MS3-CI1 OPEN; merge readiness suspended.** Ubuntu job `102698845490` in
run `34421869781` failed the Profile D sibling-progress wait at line 1130
(2-second deadline); 1 failed, 1634 passed, 28 skipped, 4 deselected.
macOS passed. The root cause is under investigation; this is not proof of a
production integrity defect. Earlier code review and R1 closure remain historical
facts, but do not authorize merging this failing candidate. Execute
[MS3-CI1](../milestone_plan.md#ms3-ci1--ubuntu-profile-d-failure-implemented-awaiting-review), then independently review the repair delta.
No merge/auto-merge, deployment or manual CI operation is authorized for the
unreviewed repair. NEXT=MS3_CI1_REPAIR.

## Final independent review — 2026-09-10

**APPROVED for the bounded offline MS3 scope. MS3-R1 CLOSED.**
Reviewed by GPT-6 Astra at `66e036a07f422818f5e3f54f0216d4083b443698`, tree
`1413ec705db3fd9f214499c57696bcab2da2e8b9`. The actual waiter enqueue,
no premature grant/release, cancellation observation, successor completion and
post-stop no-wire assertions satisfy R1. No new blocking findings were found
in this incremental review; the earlier production-path and Profile D findings
are closed. Historical reviews below retain their time-local disposition.

Independent validation: 44 passed in the five focused files listed in the prior
review below. Full offline suite, Ruff/MyPy and other gates remain implementer
reported evidence; this review did not rerun full CI, online, VPS, external media
or stress/soak. PR #56 is still open/unmerged. Approval is not deployment or live
qualification. Local Luna must update the stale PR description and complete the
normal merge handoff before MS3 is marked CLOSED. Any later code/test changes
require review of their delta; documentation-only review records do not invalidate
this source review.

## Independent re-review — 2026-09-10

Reviewed head `c7d6c904c42b4bc86cd0937d4fca03aebc16a81b`, tree
`b573c09ae958f81905ace58ca0c3612c81666ae4`; PR #56 still open/unmerged.
Reviewer: GPT-6 Astra. The real Collector/Poller and concurrent Profile D
supplements substantially resolve the original findings. One P2 test-evidence
correction remains: the waiting-cancellation test waits for request_count == 1
after the holder has already made count 1, so it can cancel before the waiter
executes. The F8 waiting-cancellation claim below was the implementer's
submitted claim, not independently accepted evidence until MS3-R1 was fixed.

See [MS3-R1 in the execution plan](../milestone_plan.md#ms3-review-disposition-and-r1-repair)
for the minimal correction and exit checks. The concurrent Profile D finding
is closed; no production redesign is requested. Final MS3 approval is pending.

Independent focused run: 44 passed in 2.03s using `.venv-ms2/bin/python -m
pytest -q` on `tests/integration/test_ms3b_production_paths.py`,
`tests/unit/test_ms3b_rest_fairness.py`,
`tests/integration/test_ms3b_multi_product_load.py`,
`tests/integration/test_ms3b_archive_capacity.py`, and
`tests/unit/test_usdm_shared_rest_gate.py`. Full-suite results below remain
implementer evidence; reviewer did not rerun full suite, online, VPS, external
media, stress/soak or CI.

## MS3-R1 implementation — 2026-09-10

The waiting-cancellation barrier was corrected in
`tests/integration/test_ms3b_production_paths.py`. The test now records the
holder's request/acquire/release counts, starts the real
`RestSideDataPoller._request()` waiter, and waits for the additional
request-lock attempt (`request_count == holder_request_count + 1`). Before
cancellation it proves there was no additional grant or release, no SDK wire
start, and the original holder still owns the lock.

After cancellation, the `_RecordingLock` event trace must contain the waiter's
`cancelled` observation while acquire/release counts remain at the holder
baseline, the SDK remains untouched, and the holder remains locked. The holder
is then released; a subsequent real poller request acquires the same lock,
reaches the SDK, and completes, proving there is no stale ownership. The test
clears the SDK-start marker and separately verifies that a request created after
`stop` does not increment the lock request count or start the SDK.

R1 is test-only; no scheduler or production-code refactor was added. The local
focused regression passed, and the updated evidence is awaiting Astra's
independent MS3-R2 final review. This record does not mark MS3 approved or PR
#56 merged.

## Frozen authority and scope

The exact MS3 base was verified before implementation:

```text
BASE_SHA=01527037254595267003f886689bb270e08b5e5d
BASE_TREE=eb63b645660a64ac606341ce3fb7f447a6e89457
BASE_MERGE_PARENTS=52bf086dd240556b054821f33bf1e2840fdcf912,ad1e941af3cd2bd3922eac239ba92a47058e9875
MS3_A_MERGED_REVIEWED_HEAD=ad1e941af3cd2bd3922eac239ba92a47058e9875
BASE_MAIN_MERGE_PR=55
```

The candidate identity is the branch above plus the single milestone commit
that contains this acceptance record and the MS3-B implementation. The
candidate is not deployed and is not current `main` until independently
reviewed and merged.

MS3-B followed the frozen decision tree. No generic scheduler, priority queue,
weighted arbiter, per-product request worker, or dispatcher was introduced.
The existing USD-M pair remains runtime-owned and exact:

```text
outer cooldown wait
→ shared asyncio.Lock
→ inner cooldown recheck
→ one HTTP request
→ release
```

The USD-M five-minute catch-up contract remains bounded at a maximum batch of
500 records and two pages per poller attempt. Cursor persistence and page
progress occur outside the shared lock; each page releases and reacquires it.
Spot keeps one weak loop-scoped shared `SpotIpRateLimiter`; depth snapshots and
Spot exchangeInfo use its same request slot.

The deterministic harness uses real `asyncio.Lock` behavior, controlled
`asyncio.Event` barriers, scripted outcomes, and fake clocks. Its starvation
oracle records `eligible_seq`, `lock_enqueue_seq`, `waiters_ahead`,
`grant_seq`, wire boundaries, result, cooldown deadline, and shared gate
identity. The tests assume Python 3.12 `asyncio.Lock` FIFO ordering for
non-cancelled lock waiters. No network, wall-clock timing, or sleep-based
fairness was used to establish the model result. The production-path supplement
below is separate evidence: it retains the real Collector/Poller, gate,
pagination, Catalog, spool, and seal paths, replacing only local SDK responses,
clock values, and deterministic wait boundaries.

## F1–F10 result matrix

| Scenario | Result | Evidence |
| --- | --- | --- |
| F1 simultaneous seven-product USD-M core bootstrap | PASS — model; production cohort supplement | One lock/cooldown pair, maximum one wire request, all eligible requests terminal in the finite production cohort |
| F2 core queued behind side work | PASS — model; production queue supplement | Model starvation oracle plus real Collector/Poller waiters and later page reacquisition |
| F3 global plus product | PASS — model; production competition supplement | Real core, product-side, and global-side requests shared one exact gate pair |
| F4 five-minute catch-up | PASS — model; production pagination supplement | Real two-page cursor path released/reacquired the lock and advanced isolated Catalog cursors |
| F5 repeated core retry | PASS — model evidence | 5xx → transport error → success lock release remains covered by the model; no new production retry behavior was needed for this review finding |
| F6 USD-M 429 | PASS — model; production cooldown supplement | Actual side poller 429 installed shared cooldown and blocked actual core snapshot wire start |
| F7 USD-M 418 | PASS — model; production cooldown supplement | Actual side poller 418 installed shared cooldown and blocked actual core snapshot wire start |
| F8 cancellation | PASS — model; R1 production lifecycle correction | Waiting enqueue/cancellation, in-flight SDK cancellation, successor reacquisition, and post-stop request are covered on the real path; independent final review approved at 66e036a |
| F9 stop | PASS — model; production lifecycle supplement | Stop converged the actual side/core path with no post-stop wire start |
| F10 Spot contention | PASS after authorized correction | Both active-block and rejection-installation races are covered |

The focused deterministic evidence is in
`tests/unit/test_ms3b_rest_fairness.py` (14 passed). The required aggregate
oracle results are:

```text
STARVED_ELIGIBLE_REQUESTS=0
FIVE_MINUTE_OLDER_WAITER_OVERTAKES=0
SIDE_CURSOR_CROSSOVER=0
USDM_MAX_CONCURRENT_WIRE_REQUESTS=1
```

## Spot F10 decision and correction

The pre-fix race was reproduced against the frozen MS3-A main before changing
production code. The deterministic trace was:

```text
A acquire(weight) → A request_slot → A wire start
B acquire(weight) → B request_slot queued
A receives HTTP 429 → A releases request_slot
B wire start
A observes rejection → shared block installed too late
```

The authorized narrow correction is now applied:

1. `SpotIpRateLimiter.request_slot()` acquires the existing request lock and
   performs a block-only recheck. It loops until the block deadline, charges no
   additional weight, and releases the lock on cancellation.
2. `SpotSnapshotRequester` observes a 418/429 while still owning that slot.
3. `SpotExchangeInfoPoller` observes typed 418/429 while still owning that
   slot; the outer handler preserves existing classification, statistics, and
   logging without a second installation.

The post-fix trace is:

```text
A wire start → A receives 429 → block installed while slot owned → A releases
B acquires slot → block-only recheck/synthetic wait → B wire start
```

```text
SPOT_PRE_FIX_F10_REPRODUCED=YES
SPOT_IN_SLOT_BLOCK_ONLY_RECHECK=YES
SPOT_REJECTION_INSTALLED_BEFORE_SLOT_RELEASE=YES
SPOT_WEIGHT_DOUBLE_CHARGED=NO
SPOT_ACTIVE_BLOCK_BYPASS_COUNT=0
SPOT_FINITE_COHORT_UNADMITTED=0
```

No Spot scheduler or queue architecture was added. The focused Spot and USD-M
rate-limit regressions, existing shared-gate tests, and the full suite pass.

## MS3-B finding 1 — production Collector/Poller gate supplement

The original F1–F9 harness is retained as model evidence; it is not used alone
to claim production multi-product gating. The new
`tests/integration/test_ms3b_production_paths.py` tests construct actual
`UsdMCollector` instances and obtain actual `RestSideDataPoller` instances from
the production `UsdMSideDataManager`. They inject the exact same
`asyncio.Lock` and `UsdMRestCooldown` into product managers and a global manager,
and assert object identity before starting work.

The finite production cohort starts two real core snapshot calls, two real
product-side `open_interest` calls, two real product-side
`global_long_short_ratio` catch-up calls, and one real global `funding_info`
call. The catch-up pollers use the production `_catch_up_five_minute` loop,
`Catalog.side_data_cursor`, page request bounds, `_persist`/`sync`, and cursor
advance. Each cursor receives two pages; page one must release the shared lock
before page two can enqueue and reacquire it. All seven top-level tasks finish,
all nine eligible HTTP calls have exact terminal outcomes, the lock has nine
matching request/acquire/release records, and the fake SDK observes
`max_active=1`. Final BTCUSDT and ETHUSDT Catalog cursors are independently at
the expected period; no cursor crosses symbols. The multi-page REST manifests
remain fail-closed (`complete=false`, `gap=true`, `reconnect_gap`) because each
page has a distinct REST provenance connection ID; this is expected generic
seal behavior, not an exchange sequence-gap claim, and M22.9 reconnect auditing
does not classify REST request IDs as reconnects.

The rate-limit parameterization runs the real side poller's typed 429 and 418
handling, observes the shared cooldown, then starts the real Collector snapshot
path and proves that its SDK call cannot start until stop/release. Existing
shared-gate tests retain the converse core-error-to-side-route coverage.

Cancellation is split into three states. A waiter cancelled before lock grant
produces no SDK call and leaves the lock owned by the holder. An in-flight
`RestSideDataPoller._request` cancellation is held open while its actual SDK
worker thread is blocked; the subsequent real Collector snapshot cannot enter
the gate, and the lock release occurs only after the worker finishes. A request
created after `stop` is set produces no SDK call. The first case was a
deterministic reproduction of a real bug: bare `asyncio.to_thread` cancellation
released the `asyncio.Lock` while its worker could still be executing. The
minimal fix reuses the existing `run_owned_blocking_call` helper, already used
by the WebSocket storage lifecycle; it adds no scheduler, queue, or new gate.

Replacement boundary for these tests: local fake SDK response objects and
blocking outcomes, fixed poller clock values, temporary filesystem roots, and
explicit events/timeouts for wait edges. The production request lock,
cooldown/recheck order, Collector snapshot method, side-data manager wiring,
pagination/cursor control, Raw spool, Catalog, manifest, and seal logic remain
under test.

## Profile D bounded load and recovery

The synthetic Profile D contains seven Spot and seven USD-M products:

```text
PROFILE_D_PRODUCTS=14
PROFILE_D_CORE_STREAM_IDENTITIES=42
PROFILE_D_INJECTED_CORE_EVENTS=4032
```

`tests/integration/test_ms3b_multi_product_load.py` uses the real
`StreamSpool`, Raw v1 writer, seal, manifest, Catalog, and recovery paths.
This is a sequential storage-layer test: it proves identity, Raw, seal,
Catalog, manifest, archive eligibility, queue-boundary, and recovery behavior,
but it does not prove simultaneous Collector activity. All 42 homogeneous
identities receive exactly 96 finite events and seal. A
separate tiny-queue test exercises the existing explicit `IngressQueueFull`
boundary for one product while a sibling seals normally. A truncated partial
for one USD-M product is recovered only for that identity; a sibling sealed
manifest remains byte-for-byte unchanged.

```text
SILENT_DROPS=0
IDENTITY_MISMATCHES=0
CROSS_PRODUCT_EVENT_CROSSOVER=0
QUEUE_CAPACITY_VIOLATIONS=0
RAW_MARKET_SYMBOL_STREAM_MISMATCH=0
UNEXPECTED_WRITER_IDENTITY_REJECTION=0
UNRESOLVED_TEST_GAPS=0
EXPECTED_WRITERS_UNSEALED=0
BACKPRESSURE_ISOLATION=PASS
RECOVERY_ISOLATION=PASS
```

The production-running supplement in
`tests/integration/test_ms3b_production_paths.py` separately starts seven real
Spot and seven real USD-M `Collector.run` paths. A deterministic opener barrier
holds all 14 products and their three core streams (42 stream contexts) active
before the finite input waves are released. The target USD-M aggregate-trade
writer is held at its real `drain_all` boundary until its receipt queue is full;
the other 41 active stream paths each persist their second event while the
target is blocked. Releasing the target produces an observed queue wait,
recovery, and third target event, after which all products reach snapshot
progress and are stopped together.

The test then inspects actual Raw envelopes, payload bytes, market/symbol/stream
identity, collector instance IDs, per-stream counts, stable connection IDs,
56 SEALED Catalog rows, 56 manifests, and the absence of ACTIVE/SEALING rows or
`.partial` files. The finite run contains 14 products × 3 core streams × 2
waves, one additional target event, and 14 snapshots: 99 Raw records in total.
The Spot limiter is the real limiter with only a high deterministic weight
budget injected to avoid pacing the offline test; USD-M keeps its real shared
lock/cooldown. This is production-path offline evidence, not a CPU/RSS
benchmark, physical-host test, live Binance test, or soak.

## Archive and capacity interaction

`tests/integration/test_ms3b_archive_capacity.py` creates one finite SEALED
candidate per Profile D core identity, retains an ACTIVE `.partial`, and uses
only a registered test directory under `tmp_path`. The bounded drain finishes
all eligible candidates; the active partial is never allocated for archive.
Archive failure injection retains the internal source and retries the same
chunk idempotently. An absent or LOW_SPACE target retains the internal SEALED
source and creates no archive transaction.

```text
ARCHIVE_EXPECTED_ELIGIBLE_IDENTITIES=42
ARCHIVE_INELIGIBLE_COPIES=0
ARCHIVE_IDENTITY_MISMATCHES=0
ARCHIVE_HASH_MISMATCHES=0
ARCHIVE_FINAL_ELIGIBLE_BACKLOG=0
ARCHIVE_FAULT_RETRY_IDEMPOTENT=PASS
```

Capacity tests preserve the existing aggregate-only profile and thresholds:

```text
WARNING=18 GiB
CRITICAL=14 GiB
EMERGENCY=12 GiB
HARD_RESERVE=10 GiB
CAPACITY_AGGREGATE_GROWTH=PASS
CAPACITY_TOPOLOGY_DECOMPOSITION_INVARIANT=PASS
CAPACITY_STATE_GLOBAL=PASS
PRODUCT_COUNT_THRESHOLD_COUPLING=NO
```

The hard-reserve coordinator seals the exact 42 configured core identities,
records one global `DISK_EMERGENCY_STOP`, stops collectors, and opens one
identity-complete test gap callback. Duplicate and missing identity counts are
zero. No per-product disk threshold or product-count-scaled archive cadence
was introduced.

```text
HARD_RESERVE_EXPECTED_IDENTITIES=42
HARD_RESERVE_MISSING_IDENTITIES=0
HARD_RESERVE_DUPLICATE_IDENTITIES=0
GLOBAL_DISK_EMERGENCY_STOP_EVENTS=1
```

## Narrow side-data isolation

Existing USD-M shared-gate and side-data cursor suites remain green. The new
production trace covers global/product/core finite progress, page-level cursor
locality, and the in-flight side-request lifecycle;
the production USD-M request lock/cooldown injection is unchanged. The Spot
exchangeInfo rejection test confirms the exact weighted rejection is installed
inside the existing shared request slot while preserving its failure stats and
retry loop.

```text
USDM_RUNTIME_SCHEDULER_CHANGED=NO
USDM_SHARED_LOCK_COUNT=1 when USD-M is enabled
USDM_SHARED_COOLDOWN_COUNT=1 when USD-M is enabled
GLOBAL_USDM_OWNER_COUNT_WHEN_ENABLED=1
GLOBAL_USDM_OWNER_COUNT_ZERO_USDM=0
```

## Offline validation

The offline repository gates were run with the repository's available Python
3.12 environment:

| Gate | Result |
| --- | --- |
| MS3-R1 waiting-cancellation/post-stop regression | PASS: 1 passed in 0.93s |
| Targeted MS3 production/shared-gate/Profile D/archive suites | PASS: 44 passed in 1.83s after R1 |
| `python -m pytest -q` | PASS: 1639 passed, 24 explicit online/preview skips, 4 stress deselected; 13 existing fork warnings |
| `python -m ruff check .` | PASS |
| `python -m mypy` | PASS: 254 source files |
| `python -m compileall -q src tests tools` | PASS |
| `python tests/verify_m0_contracts.py` | PASS |
| `go run tools/verify_raw_chunk_golden.go` | PASS: Raw v1 unchanged |
| `binance-market-recorder --version` | PASS: `0.1.0` / candidate git identity |
| `git diff --check` | PASS |

Online Binance tests, VPS access, external production storage, systemd/
launchd changes, long stress/soak/burn-in, formal M22.9, production capacity
proof, and live qualification were not run. CPU/RSS was not used as a
developer-machine acceptance threshold. The branch itself is not deployed.

## Compatibility and final status

```text
NEW_GENERIC_SCHEDULER_ADDED=NO
CORE_PRIORITY_ADDED=NO
RAW_V1_CHANGED=NO
MANIFEST_SCHEMA_CHANGED=NO
CATALOG_SCHEMA_CHANGED=NO
PERSISTED_METRICS_SCHEMA_CHANGED=NO
CURSOR_IDENTITY_CHANGED=NO
GAP_IDENTITY_CHANGED=NO
CAPACITY_THRESHOLDS_CHANGED=NO
ARCHIVE_TRANSACTION_PROTOCOL_CHANGED=NO
CONTRACTS_CHANGED=NO
PROJECTION_CHANGED=NO
MS3=CI_REPAIR_IMPLEMENTED_AWAITING_REVIEW
MS3_INDEPENDENT_RE_REVIEW=APPROVED
MS3_B_MERGED=NO
MS4=NEXT_AFTER_INDEPENDENT_MS3_RE_REVIEW_AND_MERGE
NEXT=INDEPENDENT_MS3_CI_REPAIR_REVIEW
FORMAL_M22_9=NOT_STARTED
PRODUCTION_READY=NO
CURRENT_MAIN_DEPLOYED=NO
```
