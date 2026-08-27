# Current Production State

This is the consolidated current-state handoff for M22.9. It records the
historical production incident, the current local source correction, the VPS
state, capacity evidence, and the exact path to a future acceptance run. It is
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

The current implementation head is `2e8525f5df27a1b017890c172eae80db340cb901`
on branch `fix/m22-9-startup-recovery-liveness`, pushed to
`origin/fix/m22-9-startup-recovery-liveness`. No PR exists yet; the
implementation is not independently reviewed, not merged, not built into a new
artifact, and not deployed.

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

The new source correction is commit `2e8525f5df27a1b017890c172eae80db340cb901`,
 titled `fix: bound startup recovery liveness`. No new Wheel, freeze record,
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

## Local startup-liveness correction

The local commit claims/results are:

- one periodic heartbeat remains active during `STARTING` recovery;
- `STARTING` remains not-ready and collectors do not start before recovery;
- stable exact `SEALED` Catalog+manifest identity uses metadata-only startup
  reconciliation, while crash-unstable states retain full validation;
- recovery shutdown is cooperative and does not mark incomplete recovery as
  complete or require SIGKILL;
- cancel/resume, long-recovery heartbeat, stable-sealed fast-path, and
  unstable-validation tests pass.

Local validation recorded for this correction is focused `97 passed`; full
suite `1361 passed, 24 skipped, 4 deselected`; Ruff, MyPy, M0 contracts, and
Go Raw golden verification passed. These are local implementation evidence,
not production acceptance. The commit has not received independent review,
has not been merged, and has no deployment or duration credit.

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

The operator is considering, as planning only, a larger shared VPS such as
4 vCores/8 GB or 6 vCores/12 GB with expanded storage. Such a host could
technically execute explicit normalization/replay/backfill, but co-running
heavy offline work with the integrity-critical live path requires a separate
topology/resource-isolation decision. This does not change the accepted
deployment profile or ADRs. A future Gateway may coexist as a separate
service/process, but must not be embedded into Recorder; it is not currently
deployed or production-ready. During formal Recorder acceptance, unrelated
co-resident workload should not be changed mid-chain because it would make the
operational evidence harder to interpret.

## Exact next engineering sequence

Resolve capacity before starting the formal duration chain. Then follow:

```text
2e8525f local correction
 -> independent targeted review
 -> corrections if needed
 -> push / PR
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
