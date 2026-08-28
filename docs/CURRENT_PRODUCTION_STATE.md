# Current Production State

This is the consolidated current-state handoff for M22.9. It records the
historical production incident, the current source correction and review,
infrastructure planning, VPS state, capacity evidence, and the exact path to a
future acceptance run. It is
documentation authority only; it does not authorize deployment, acceptance,
or data deletion.

## Status at a glance

```text
M22_9_24H_RESULT=INCOMPLETE
ELIGIBLE_FOR_72H=NO
PRODUCTION_READY=NO
SERVICE=STOPPED / NOT CAPTURING
FORMAL_ACCEPTANCE=NOT_RESTARTED
```

The reviewed executable/test candidate is commit
`9c1df2333c911eb830a0f9a698ed5e42a1b78740`, titled
`fix: prevent startup promotion after stop`, on branch
`fix/m22-9-startup-recovery-liveness`. This technical candidate is followed by
documentation-only alignment and does not change the reviewed executable/test
candidate. A fresh independent targeted re-review approved PR creation with
`FINAL_VERDICT=APPROVED_FOR_PR_CREATION` (`P0=0`, `P1=0`, `P2=1`, `P3=0`).
No new artifact has been built, no readiness has been run on this candidate,
and no deployment or acceptance window has started.

## Historical first targeted review

The following review authority and findings are historical and are not the
current branch or review state.

Review authority:

```text
MAIN_SHA=553cb345466bdb2e2444d7162dea337a480b1b17
FEATURE_BRANCH_HEAD=7f1e71fdf1f6f73a8b8ca35d202216745f90d02c
IMPLEMENTATION_COMMIT=2e8525f5df27a1b017890c172eae80db340cb901
PR_EXISTS=NO
P0_COUNT=0
P1_COUNT=1
P2_COUNT=1
P3_COUNT=0
FINAL_VERDICT=REQUEST_CHANGES
SAFE_TO_CREATE_PR=NO
SAFE_TO_MERGE_WITHOUT_CHANGES=NO
```

The review positively established:

- `SINGLE_HEARTBEAT_OWNER_PROVEN=YES`;
- `STABLE_SEALED_FAST_PATH_SAFE=YES`;
- `STABLE_SEALED_DURABLE_AUTHORITY_SUFFICIENT=YES`;
- `CRASH_UNSTABLE_FULL_VALIDATION_PRESERVED=YES`;
- `CANCEL_RESUME_IDEMPOTENCE_PROVEN=YES`;
- `NEW_CROSS_THREAD_CATALOG_RACE=NO`;
- `READINESS_SEMANTICS_PRESERVED=YES`;
- `STARTUP_COST_NO_LONGER_O_TOTAL_PAYLOAD=YES`;
- `300S_ARCHITECTURALLY_PLAUSIBLE=YES`, but
  `300S_PRODUCTION_TIMING_PROVEN=NO`.

The review did not reject the stable-sealed fast-path architecture. The merge
blocker is the following pre-existing-in-`553cb345` P1:

```text
recover_storage completes
 -> startup_recovery_complete=true
 -> await VPS capacity observation in asyncio.to_thread()
 -> SIGTERM/request_stop sets STOPPING and stop authorities
 -> capacity observation returns without a stop re-check
 -> collector_factory and supervisor creation
 -> status can become RUNNING and SERVICE_STARTED can be emitted
 -> Collector tasks can be created before eventual stop
```

This violates both `STOPPING must not transition back to RUNNING` and
`Collectors must not start after an operator stop request during STARTING`.
It was not introduced by `2e8525f`. The cooperative stop contract during
`recover_storage()` itself remains implemented and tested, but the full
startup-stop contract is not closed until this post-recovery capacity-window
race is corrected and re-reviewed.

The review also recorded one P2, nonblocking for the current startup-liveness
architecture: a missing or size-mismatched already-stable local `SEALED`
artifact becomes a `reconcile_failed` RecoveryAction rather than forcing
startup failure. This is pre-existing, concerns post-commit external loss or
filesystem corruption, and is not part of the crash-recovery authority fix. It
is not promoted to P1 here.

## Current technical candidate and fresh targeted re-review

The latest technical correction is:

```text
TECHNICAL_CANDIDATE_HEAD=9c1df2333c911eb830a0f9a698ed5e42a1b78740
LATEST_TECHNICAL_CORRECTION=fix: prevent startup promotion after stop
P0_COUNT=0
P1_COUNT=0
P2_COUNT=1
P3_COUNT=0
TARGETED_P1_CLOSED=YES
SAFE_TO_CREATE_PR=YES
FINAL_VERDICT=APPROVED_FOR_PR_CREATION
```

The fresh review confirmed the post-capacity startup-promotion barrier,
preserved the single heartbeat owner, recovery fast path, crash-unstable full
validation, normal startup, and startup hard-reserve behavior. The known P2
about missing or size-mismatched stable local `SEALED` artifacts remains
nonblocking and intentionally deferred.

The acceptance chain remains independent and has no historical duration credit:

```text
new exact source/artifact
 -> deployment
 -> exact deployment identity
 -> operational readiness
 -> formal acceptance identity
 -> formal acceptance readiness
 -> 2h -> 12h -> 24h -> 72h -> 168h
```

If all stages run independently, the staged duration total is **278 hours**.
The historical M22.9 attempt does not restart or partially satisfy this chain.

## Historical source and artifact freeze

The final merged source before the newly discovered startup-liveness correction
is historical incident evidence:

| Item | Value |
| --- | --- |
| Source Git SHA | `553cb345466bdb2e2444d7162dea337a480b1b17` |
| Source tree SHA | `de131c02680be1fe1db04ef6a160b2ea1e62c593` |
| CI run | `33059152748` — completed/success |
| Frozen Wheel | `binance_market_data_recorder-0.1.0a1-py3-none-any.whl` |
| Wheel SHA-256 | `e55dd1ac387390102b81adb0f24d16d225e6697bf5ecdf6edceac46e26a30f9c` |
| Production lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Freeze record SHA-256 | `5671e21dd8ac263abb0b51766863f74e13ab6e182c855b04d663118cda2c16aa` |

This Wheel and source remain valid historical evidence, but they are not the
final production candidate for M22.9 because the startup-liveness defect was
proven and corrected in new local source. Do not deploy `e55dd1ac…` to begin
final M22.9 acceptance.

The initial source correction was commit
`2e8525f5df27a1b017890c172eae80db340cb901`, titled
`fix: bound startup recovery liveness`. No new Wheel, freeze record,
or production artifact has been created from it yet.

## Historical VPS deployment incident and rollback

The controlled stopped upgrade targeted:

```text
VPS_HOST=vps-b5bfe3f8
OS=Ubuntu 24.04.4 LTS
ARCH=x86_64
Python=3.12.3
Service principal=binance-recorder:binance-recorder
```

The historical candidate used source `553cb345…`. Wheel transfer and retained
VPS hash chains were exact: Wheel `e55dd1ac…` and production lock
`44cd3733…` matched from local source through temporary and final retained VPS
paths. Fresh venv creation, `--require-hashes` dependency installation,
Wheel `--no-deps` installation, `pip check`, `direct_url.json` checks, and
systemd installation/effective-property verification passed. The candidate
deployment identity was:

```text
SOURCE_SHA=553cb345466bdb2e2444d7162dea337a480b1b17
IDENTITY_SHA256=e47efa85719badefacc5658811243d6664a71888862d651dcb56d342a14ad7e5
STATIC_VERIFY=PASS
PID=328413
InvocationID=59829e72ffab41bf85047b9a9de12b15
```

Operational readiness then failed: `service_heartbeat_stale`, status remained
`STARTING`, `startup_recovery_complete=false`, and capacity and markets were
unavailable. This was a readiness failure, not a successful production
deployment.

Rollback restored the old deployment authority and passed its checks:

```text
OLD_SOURCE_GIT_SHA=650bc8f81446af5255d1eee8cfb6ab8b2ade5ccb
OLD_WHEEL_SHA256=ba6811097dbe008fd0c4c6a2aded47f48d192f1b942aa5dd11606df2deec9179
OLD_LOCK_SHA256=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335
rollback-check=PASS
old static deployment verify=PASS
```

The old runtime encountered the same operational-readiness failure. Therefore
the failure was not specific to the new `553cb345…` artifact. The final host
state after the incident was service failed/stopped, `MainPID=0`, zero
production writers, zero active partials, and `OLD_RESTORED` deployment
authority.

## Confirmed startup root cause

The independent forensic verdict is
`ROOT_CAUSE_CONFIRMED_LONG_RECOVERY_HEARTBEAT_GAP`. Both old and candidate
source shared the relevant startup implementation. The production corpus had:

| Forensic measure | Observed value |
| --- | ---: |
| Sealed chunks | 66,354 |
| Other chunk states | 0 |
| Unarchived chunks | 66,354 |
| Archived/remote-deleted chunks | 0 |
| Compressed retained Raw | 12,331,419,711 bytes |
| Logical uncompressed Raw | 196,899,977,168 bytes |
| Manifest content | 98,722,391 bytes |
| Catalog | 85,217,280 bytes |
| Catalog WAL | 0 |

Historically, startup opened the Catalog, published one `STARTING` heartbeat,
ran synchronous `recover_storage()` through `asyncio.to_thread()`, fully
revalidated all normal historical `SEALED` Raw, then checked capacity,
started collectors, entered `RUNNING`, and only then resumed the periodic
heartbeat. Recovery took approximately 17 minutes 50 seconds and produced
64,174 recovery actions. With a 5-second heartbeat interval and a stale
threshold of `max(30 seconds, heartbeat * 3) = 30 seconds`, a healthy long
recovery became a stale-heartbeat failure. Confirmed minimum old startup
processing was `>=221,661,538,981` bytes because stable sealed payloads were
read, decompressed, and re-hashed unnecessarily on every restart.

No pathological artifact, Catalog corruption, filesystem fault, OOM, clock
fault, or recovery hang was identified.

### Shutdown-liveness aspect

When automation stopped the `STARTING` process after readiness failed, SIGTERM
was received while `recover_storage()` was still running in
`asyncio.to_thread()`. The recovery worker did not cooperatively stop, so the
process exceeded canonical `TimeoutStopSec=90s` and systemd used SIGKILL. This
was application-level inability to finish while a recovery worker remained
outstanding; it was not Linux D-state or kernel-uninterruptible I/O.

## Startup-liveness correction status

The implementation commit claims/results remain:

- one periodic heartbeat remains active during `STARTING` recovery;
- `STARTING` remains not-ready and collectors do not start before recovery;
- stable exact `SEALED` Catalog+manifest identity uses metadata-only startup
  reconciliation, while crash-unstable states retain full validation;
- recovery shutdown during `recover_storage()` is cooperative and does not mark
  incomplete recovery as complete or require SIGKILL;
- cancel/resume, long-recovery heartbeat, stable-sealed fast-path, and
  unstable-validation tests pass.

Local validation for the original implementation was focused `97 passed`; full
suite `1361 passed, 24 skipped, 4 deselected`; Ruff, MyPy, M0 contracts, and Go
Raw golden verification passed. The latest P1 correction adds focused evidence
of `40 passed`, Ruff PASS, MyPy PASS, and `git diff --check` PASS. The fresh
targeted re-review closed the capacity-observation stop race with no P1
findings. These are implementation/review results, not production acceptance.

The technical candidate has no new artifact, deployment, readiness result, or
duration credit. The next sequence is PR creation, exact-head CI, merge,
post-merge source authority, a new artifact/freeze, controlled VPS deployment,
readiness, and then the M22.9 staged acceptance chain.

No Catalog schema, capacity policy, readiness threshold, or systemd timeout
changed.

## Current capacity and deletion authority

The following are production forensic observations and planning estimates, not
new hardcoded Recorder policy:

```text
FREE_BYTES=17,091,108,864
HARD_RESERVE=10 GiB = 10,737,418,240 bytes
usable headroom above reserve ≈ 6.35 GB
observed selected 6h net growth = 146,864.466862 bytes/s
observed 24h rate = 145,413.612664 bytes/s
capacity state = EMERGENCY (ETA to reserve <24h)
approximate runtime at current rate before reserve ≈ 12.02h
```

The Recorder is stopped, so the estimate applies once capture resumes. At the
observed 6-hour rate, a single uninterrupted 168-hour stage needs about
82.47 GB additional usable capacity to end just above the 10 GiB reserve, or
about 91.06 GB to end above the repository's 18 GiB NORMAL threshold. For the
independent full 278-hour chain, the corresponding estimates are about
140.63 GB and 149.22 GB.

Consequently, +50 GB is insufficient, +100 GB is insufficient for the full
278-hour chain, and +150 GB is only approximately the mathematical minimum
with almost no growth-rate or host-usage margin. Prefer approximately +200 GB
usable capacity if economically reasonable. This is operational planning, not
a code requirement or threshold change.

All 66,354 Raw chunks are unarchived. No Raw is currently in the verified
archive/authorized-delete class. Do not manually delete Raw to solve capacity;
preserve Raw, manifests, Catalog history, historical M22 evidence, and incident
evidence. Obsolete staging or failed-venv scratch may be reviewed separately,
but it is not the main capacity solution.

## Execution-role architecture

The live VPS path owns Binance public acquisition, active Raw framing/spooling,
seal and Zstandard compression, Raw manifests, SQLite Catalog metadata/state,
recovery, gap/provenance evidence, order-book/checkpoint live derived state,
metrics/status/capacity, and archive-export support. VPS Raw `.bmdr.zst` is
already compressed; it is not merely unprocessed or uncompressed Raw storage.

`binance-market-recorder normalize run` is an explicit offline/non-core
operation. It verifies Raw and produces content-addressed Zstandard Parquet;
Collector callbacks do not run it. Heavy Replay/analytical scans and Historical
Backfill are likewise assigned to local/offline execution profiles. DuckDB is
only a development interoperability verifier for published Parquet/Hive
partitions, not Recorder's persistent database. SQLite Catalog is the Recorder
metadata authority and does not store Raw market-event payloads.

## Recorder infrastructure planning

The preferred future direction is a dedicated Recorder VPS. Recorder is a
durable system-of-record/data-capture service and does not need the same latency
profile as Gateway. Separating the services reduces workload coupling and
allows a cheaper Recorder-optimized host, while Gateway/Projection can later
use a separate realtime-latency profile.

```text
Host A: BinanceMarketDataRecorder
Host B: BinanceMarketDataGateway
        -> Projection embedded
        -> gRPC
```

Recorder and Gateway remain independent services and failure domains. This is
infrastructure planning only and creates no runtime dependency or repository
boundary change.

Current candidate configurations are not production-certified:

| Candidate | Current planning status |
| --- | --- |
| 4 vCPU / 8 GB RAM / 200 GB total NVMe | Preferred benchmark/deployment candidate; provides a more comfortable envelope if measurements support it |
| 2 vCPU / 4 GB RAM / 200 GB total NVMe | Lower-cost conditional target; suitability must be proven by profiling, benchmarking, optimization, and long-running acceptance |

The goal is to make `2c/4GB` potentially viable with sufficient headroom while
keeping `4c/8GB` as the more comfortable option when evidence supports it. No
CPU/RAM conclusion is stronger than measured evidence.

The existing OVH VPS has approximately one month already paid. The near-term
plan is therefore to use it for controlled deployment/testing work rather than
waste the paid period, while evaluating lower-cost or better Recorder-only
providers. Compare price, CPU quality, RAM, NVMe capacity/performance,
network, reliability, and upgrade flexibility. A later migration, if chosen,
must use a separately controlled procedure. If no materially better option is
found, retain OVH and upgrade/expand it. No provider migration has been
selected, and no OVH upgrade has occurred.

### Storage sizing clarification

`200 GB total NVMe` is not equivalent to the earlier planning recommendation
of `approximately +200 GB additional usable capacity`. Exact usable capacity
after migration or resize must be measured, and the capacity forecast must be
rerun before any formal acceptance T0. Formal acceptance must not start unless
the measured runway is sufficient.

The existing forensic observations remain `FREE_BYTES=17,091,108,864`, a 10 GiB
hard reserve, 146,864.466862 bytes/second over the selected six-hour sample,
145,413.612664 bytes/second over 24 hours, and `EMERGENCY`, with approximately
12 hours before reserve after capture resumes. The independent chain remains
278 hours. Prior estimates remain approximately 140.63 GB additional usable
capacity above reserve, 149.22 GB above the 18 GiB NORMAL threshold, and a
conservative preference of approximately +200 GB additional usable capacity.

## Future resource optimization

M23.4 clean-seal evidence is implemented locally under the separately
authorized M23.0/M23.0F execution-order override. Independent review approved
the core architecture with `P0=0`, `P1=0`, `P2=2`, and `P3=0`; the two narrow
P2 corrections (Raw header 64-KiB parity and retained active-source convergence
after same-host archive advance) were confirmed closed by targeted rereview.
That rereview found one new ordinary-`SEALED` last-copy deletion P1 introduced
by the first correction; the narrow fail-closed correction is ready for
targeted rereview. It is not deployed and is not part of formal M22.9
acceptance. Other
resource optimization remains future work; do not rewrite the Recorder
speculatively. The evidence-driven order is:

1. Correct algorithms and complexity.
2. Production-equivalent profiling.
3. Batch Raw encoding and writes.
4. Reduce per-event Python allocation/object overhead.
5. Measure and reduce unnecessary per-event instrumentation overhead.
6. Investigate queue handoff only if profiling proves it significant.
7. Add a bounded seal pipeline only if simpler hot-path improvements are insufficient.
8. Consider clean-seal incremental stats/hash only with differential, fuzz, and
   crash proof.
9. Evaluate a narrow C++ native Raw data plane only if Python remains the
   measured dominant bottleneck.
10. Consider a full Go Recorder v2 only at a much later evidence gate.
11. A full C++ Recorder rewrite is not recommended.

Historical production evidence included approximately 338–675 USD-M
`book_ticker` events/second, 195–261 Raw-writer events/second, approximately
108% Recorder CPU, and significant receipt-queue accumulation. This shows
insufficient headroom under at least one condition, not that Python is
inherently incapable. The startup incident demonstrated that algorithmic
complexity can dominate language choice: at least 221 GB of old startup
processing over 66,354 chunks versus metadata-scale stable-`SEALED` recovery
after the fast path. Optimize algorithms before considering a language rewrite.

### M23 — Recorder Resource & Throughput Hardening

`M23.4` is **IMPLEMENTED LOCALLY / RETAINED-RAW P1 CORRECTION READY FOR
TARGETED REREVIEW** under separate authorization. Both prior P2 corrections
were confirmed closed; ordinary `SEALED` cleanup now retains Raw unless the
actual sealed artifact passes full validation. M23.1/M23.2 were skipped and
M23.3 was not required first. The remaining M23 sequence ordinarily follows
M22.9 unless separately authorized:

- `M23.0` Baseline Profiling.
- `M23.1` Low-risk hot-path optimization (`append_many`, batch encoding,
  `writev`/bounded aggregate writes, duplicate metrics/timing reduction).
- `M23.2` Allocation optimization only where profiling demonstrates value.
- `M23.3` Bounded seal pipeline only if prior work lacks headroom, with heavier
  crash/recovery review.
- `M23.4` Implemented: clean-seal incremental writer stats/hash; crash-recovered
  partials still receive full scan, with differential/bounded-random/crash
  proof. VPS A/B remains a separate future task.
- `M23.5` Native Raw engine decision gate; narrow C++ data plane only if the
  measured Python bottleneck remains, preserving Raw v1 compatibility.
- `M23.6` Go v2 decision only if Python orchestration remains materially
  limiting and the hybrid design no longer justifies its complexity.

Future M23 research targets are sustained capacity at least 2x the maximum
observed production rate, non-monotonic receipt backlog, burst drain,
zero-silent-loss and unchanged ordering/Raw/gap/crash semantics, meaningful
CPU headroom, bounded memory, and no persistent ingest deficit during sealing.
These are research targets, not M22.9 acceptance contracts.

## Exact next engineering sequence

Resolve capacity before starting the formal duration chain. The immediate
engineering sequence is:

```text
PR
 -> exact-head CI
 -> merge
 -> post-merge authority
 -> NEW source/artifact freeze
 -> NEW exact Wheel
 -> controlled VPS deployment
 -> static deployment verify
 -> operational readiness
 -> formal acceptance identity/readiness
 -> 2h -> 12h -> 24h -> 72h -> 168h
```

Do not restart acceptance on the historical `e55dd1ac…` Wheel. Do not infer
live capture from static verification or from historical PASS evidence.
