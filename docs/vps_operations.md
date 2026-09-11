# VPS Operations

Status: `M22_9_P2=REVIEWED_COMPLETE`. The exact P2 deployment source/review
base `646792f2e5fc5b7195ea58541d3f1dfda6555b7f` (tree
`c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c`) was installed and deployment
verified. The four canonical ProductKeys reached readiness with 12 core stream
contexts; Recorder was gracefully stopped and is currently `inactive/dead`,
`MainPID=0`, `Result=success`, `NRestarts=0`.

The registered archive target is storage ID
`ef852751-721c-4145-9083-f6fd48718480` at
`/srv/recorder-data/recorder-archive` on ext4 `/dev/vdb1`. Existing verified
ArchiveManager/Catalog transactions drained the backlog to zero; full archive
verification reported 112,570 verified files, zero failed, and zero pending.
`binance-market-data-archive.timer` is enabled and active/waiting with a future
monotonic trigger and successful last service result. The active writer root
remains `/var/lib/binance-market-data-recorder` on `/dev/vda1`; the 2 TB
`/dev/vdb1` filesystem is an archive target, not an active writer root.

`P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES` records exact installed identity
verification. After this docs-only merge,
`CURRENT_MAIN_DEPLOYED=NO`; the docs-only merge descendant is not installed.
The installed P2 artifact remains
the Formal candidate until a later authorized redeploy.
`PRODUCTION_READY=NO`, `FORMAL_M22_9=NOT_STARTED`, and
`FORMAL_M22_9_CREDIT_SECONDS=0`. The conservative 278-hour archive projection
leaves approximately 1.772 TB of target margin, while active-root runway above
the 10 GiB hard reserve is only about 26.46 hours. Every future Formal stage
must recheck timer, backlog, target capacity, and active-root runway. See
[`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md),
[`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md), and
[`M22.9-P2 acceptance`](milestone_acceptance/M22.9-P2.md).

NEXT=FORMAL_M22_9_2H_START_REQUIRES_SEPARATE_AUTHORIZATION

The bounded MS4 qualification remains reviewed complete, and P1 remains
reviewed complete as a documentation-only systemd-detached preparation. The
original MS4-C window remains partial and the R3 supplement grants no Formal
duration credit. No Formal T0, P1 observer, long soak, or automatic stage
advancement occurred in P2.

### Historical MS4-D and P1 checkpoints (not current authority)

The historical MS4-D review base was main
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` (tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`) and includes the merged MS4-C
evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d` with the same tree; base CI run
`34551834444` completed successfully. These are historical review authorities
only. The P1 package was based on main at its historical checkpoint
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`. The earlier PR #60 /
`efae0135…` snapshot above is historical MS4-C review-start authority, not
current.

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
`/srv/recorder-data/recorder-archive`.

P1 changes are documentation-only. P1 performed these VPS read-only checks and
the two harmless transient-unit lifecycle probes; it did not deploy or start
Recorder, enable the timer, archive or delete Raw, or send Binance traffic.
The probes changed only their ephemeral systemd test units. One detached probe
used InvocationID
`ec84df57f764445ca2db873eedc97b6c` from `2026-09-11T07:29:02Z` to
`2026-09-11T07:29:22Z`; a second SSH session saw the persistent journal and
the successful unit later became `not-found` after garbage collection. A
separate SIGINT probe used InvocationID
`2bc9ce4af8a94d4f867f5842631fab67`, recorded SIGINT/KeyboardInterrupt, and
stopped cleanly. Neither probe touched Recorder or Raw. The correct archive
timer is `binance-market-data-archive.timer`, loaded/disabled/inactive. The
active writer filesystem is `/dev/vda1` (approximately 57.1 GB total and
41.2 GB available); historical selected 24-hour net growth is
`167228.007472 B/s`, forecasting hard-reserve reach at
`2026-09-13T15:11:31.626026Z`, so the approximately 278-hour formal chain is
not capacity-complete. The mounted 2 TB `/dev/vdb1` has approximately 2.122 TB
available and registered storage ID
`ef852751-721c-4145-9083-f6fd48718480` resolves `READY` at
`/srv/recorder-data/recorder-archive`; it is an archive target, not the active
writer root. P1 does not enable the timer, archive, delete source Raw, or
start Formal M22.9.

This document describes the intended Ubuntu 24.04 LTS x86_64 profile. The P2
host observed 4 vCPU, 6,163,615,744 bytes RAM, a 61,285,326,848-byte active
`/dev/vda1`, and a 2,163,348,520,960-byte archive `/dev/vdb1`. Ubuntu 22.04
x86_64 is a compatibility target. macOS is a development/local profile and
RK3588 remains a separate LAN Linux validation and historical evidence profile.

## Responsibility boundary

The VPS runs only the integrity-critical live path:

- Binance public Spot/USD-M acquisition;
- Raw active/spool, sealing, and compression;
- already-compressed Zstandard Raw (`.bmdr.zst`) and Raw manifests;
- one live Catalog and startup/crash recovery;
- gap, completeness, and provenance state;
- order-book/checkpoint live derived state, metrics, capacity, and structured
  status; and
- support for local-client archive export.

Full Normalize jobs, heavy Replay/analytical scans, and Historical Backfill run
locally through offline execution profiles. They remain Recorder-owned
capabilities in the same distribution. A co-resident unrelated service is not
managed or inspected by Recorder.

`normalize run` is explicit and offline/non-core; Collector callbacks do not
run it. DuckDB is only a development interoperability verifier for published
Parquet/Hive partitions, not the persistent Recorder database. SQLite Catalog
is the Recorder metadata authority and does not store Raw market-event
payloads. A larger VPS could technically host explicit offline work, but
co-running it with the live path requires a separate topology/resource-
isolation decision and does not change the accepted profile or ADRs. A future
Gateway may coexist only as an independent service/process; it is not embedded,
deployed, or production-ready.

## Recorder-only VPS planning

The preferred future direction is to run Recorder on its own VPS. Recorder is
a durable system-of-record/data-capture service and does not require Gateway's
latency profile. Separate hosts reduce workload coupling and permit a cheaper
Recorder-optimized profile, while Gateway/Projection can use a separate
realtime-latency profile:

```text
Host A: BinanceMarketDataRecorder
Host B: BinanceMarketDataGateway -> embedded Projection -> gRPC
```

Recorder and Gateway remain independent services and failure domains; this is
planning only and creates no runtime dependency. Current un-certified
Recorder-only candidates are:

- **4 vCPU / 8 GB RAM / 200 GB total NVMe:** preferred benchmark/deployment
  candidate, if measurements support the envelope.
- **2 vCPU / 4 GB RAM / 200 GB total NVMe:** lower-cost conditional target;
  suitability requires profiling, optimization, benchmarking, and long-running
  acceptance. It is not certified.

The existing OVH VPS has approximately one month already paid. Use it for
near-term controlled deployment/testing work while evaluating lower-cost
dedicated Recorder providers. Compare price, CPU quality, RAM, NVMe
capacity/performance, network, reliability, and upgrade flexibility. No
provider migration is selected and no OVH upgrade has occurred; if no
materially better option is found, retaining and expanding OVH remains the
fallback. Any migration is a separate controlled procedure.

`200 GB total NVMe` must not be confused with the earlier `approximately +200
GB additional usable capacity` recommendation. Measure exact usable capacity
after migration or resize and rerun the capacity forecast before formal
acceptance T0. Do not start acceptance without sufficient measured runway.

## Network profile

Direct Binance connectivity is mandatory for the current Tokyo VPS
`vps-production-v1` profile. `environment` and `explicit` proxy modes remain
available only outside `vps-production-v1` for local development, fault
injection, and LAN testing.
Proxy URLs and credentials do not enter Raw, manifests, Catalog event bodies,
status, or logs.

## Capacity states

Forecasting uses observed filesystem free bytes and measured ingest/net-growth
rates. It does not assume that Recorder owns the filesystem or the host.
M22.7A names this derived internal-only policy `vps-production-v1`. Selection
is explicit only when the loaded `recorder.toml` contains
`capacity_profile = "vps-production-v1"`. No hostname, platform,
filesystem-size, cloud-metadata, systemd, environment variable, CLI flag, or
runtime inference selects it. Omission retains generic M11 behavior and an
unknown literal fails configuration loading.

| State | Entry condition | Action |
| --- | --- | --- |
| NORMAL | free > 18 GiB and ETA to hard reserve > 7 days | Continue live capture |
| WARNING | free <= 18 GiB or ETA <= 7 days | Continue capture; plan archive soon |
| CRITICAL | free <= 14 GiB or ETA <= 72 hours | Continue integrity-critical work; strongly request archive |
| EMERGENCY | free <= 12 GiB or ETA <= 24 hours | Prioritize capture/seal/Catalog/recovery and archive space recovery |
| HARD RESERVE | free <= 10 GiB | Do not intentionally consume protected reserve; drain/seal, record stop/gap, stop accepting new capture |

All ETA evidence targets the fixed 10 GiB reserve; there are no separate ETA
targets at 18, 14, or 12 GiB. The 10 GiB reserve protects the OS and co-resident
services. At or near the
reserve, the fail-closed rule remains: never delete unarchived Raw, safely
drain and seal what can be proven, persist `DISK_EMERGENCY_STOP` and the gap
start, and stop before writing the filesystem to zero. Forecast ETAs continue
to use observed 1h, 6h, 24h, and 7d evidence where available. ETA alone may
trigger the emergency pre-actions but never a Collector stop; only an actual
observation at or below 10 GiB authorizes the hard stop.

These are initial VPS-profile thresholds. Changes require another explicit
architecture decision and measured evidence.

## Archive workflow

The local archive client pulls from the VPS over SSH in V1. It verifies local
durability, readback, size, Raw hash, manifest identity, Archive Set identity,
and receipt durability before asking the VPS to delete the source. The VPS
revalidates the source and receipt before authorization. SSH success alone is
never sufficient. After successful authorization, VPS Raw may be deleted
immediately; no grace period is required.

After each successful session, the VPS creates a consistent post-session
Catalog snapshot through a SQLite-supported backup mechanism. The local client
verifies it and retains `latest` and `previous`. A snapshot failure is surfaced
and retried independently; it does not undo Raw integrity.

See [`archive_transfer_contract.md`](archive_transfer_contract.md) and
[`offline_workspace.md`](offline_workspace.md).

## Certified service and filesystem boundary

The certified unit is
`/etc/systemd/system/binance-market-data-recorder.service`. It runs an
operator-supplied, pre-existing dedicated `User=` and `Group=` and never
provisions an account. Its fixed invocation is:

```text
/opt/binance-market-data-recorder/venv/bin/python
-m binance_market_data_recorder
--config /etc/binance-market-data-recorder/recorder.toml
_service run
```

The unit wants and starts after only `network-online.target`; it has no Mihomo
dependency. It freezes `Restart=on-failure`, `RestartSec=10s`,
`TimeoutStopSec=90s`, `UMask=0027`, `NoNewPrivileges=true`, and journald output.
It has no `EnvironmentFile`, `PassEnvironment`, or
`BINANCE_MARKET_RECORDER_*` setting. It explicitly sets the upper- and
lowercase standard proxy variables to empty so manager defaults cannot alter
direct routing. Effective verification queries the fragment, drop-ins, exact
structured argv, principal, working directory, restart/delay/stop timeout,
UMask, NoNewPrivileges, Wants/Requires/After, environment authorities, service
type, signal, and journal outputs after daemon reload. It requires the intended
`network-online.target` relationship and rejects an effective Mihomo dependency
even when introduced outside the unit fragment. Readiness also reads the
bounded live `/proc/<MainPID>/environ` evidence and rejects nonempty proxy or
Recorder operational variables.

The operator establishes and audits this ownership boundary before install:

| Path | Required authority |
| --- | --- |
| `/opt/binance-market-data-recorder/` | root/operator owned; service principal cannot write artifacts or executable environment |
| `/etc/binance-market-data-recorder/` | root-controlled and not group/other writable |
| `recorder.toml` and `deployment-identity.json` | `root:<service-group>` mode `0640`; service read-only |
| optional `legacy_reconnect_classifications.json` | root-controlled startup authority, `root:<service-group>` mode `0640` |
| `/var/lib/binance-market-data-recorder/` | `<service-user>:<service-group>` mode `0750` |
| installed systemd unit | root-owned system unit bytes |

The service user may mutate only its data root. M22.7B does not own the host,
other files on the filesystem, OS account provisioning, mount management, or
filesystem repair.

## Exact identity and static gate

`deployment-identity.json` is canonical `deployment-identity.v1` evidence. It
binds the full source Git SHA, retained Wheel path/SHA-256, distribution
version, exact Python executable and `sys.version`, retained hashed-lock
path/SHA-256, config path/SHA-256, installed unit path/SHA-256, selected
profile, effective systemd identity, and the actually consumed legacy
classification sidecar as either `PRESENT` with SHA-256 or explicit `ABSENT`.

Static verification rejects a missing or changed retained file; any missing,
wrong-version, or unexpected installed runtime distribution compared with the
normalized lock set; editable runtime content; Recorder non-Wheel installation;
`direct_url.json`, module/dist-info, package/Python/venv, or installed `RECORD`
disagreement; noncanonical JSON; operational environment override;
effective-property mismatch; or drop-in. Recorder Wheel authority remains
separate from the third-party lock. The `/opt/...` release and venv namespace,
installed package roots, executable, Wheel, and lock must be root-owned,
symlink-free at controlling directories, and non-writable by the actual service
principal. Git `HEAD` and `--version` are display evidence, not deployed
identity.

## Initial stopped deployment

Use a clean source checkout at one frozen full SHA. Build exactly one final
Wheel, calculate its SHA-256, and copy that same immutable Wheel plus
`requirements/linux-x86_64-python312.lock` into a root-controlled release
directory beneath `/opt/binance-market-data-recorder/`. Never rebuild after
acceptance and assume equivalence.

With `SERVICE_USER` and `SERVICE_GROUP` set to already-existing dedicated
principals, the operator performs this stopped sequence:

1. Confirm the Recorder is stopped and there is no running/active mutation.
2. Create `/var/lib/binance-market-data-recorder` as
   `$SERVICE_USER:$SERVICE_GROUP` mode `0750`; install the config directory and
   artifact tree root-controlled.
3. Install `recorder.toml` as `root:$SERVICE_GROUP` mode `0640`. It must contain
   `data_root = "/var/lib/binance-market-data-recorder"`,
   `capacity_profile = "vps-production-v1"`, and the frozen `direct` network
   policy. Do not export any `BINANCE_MARKET_RECORDER_*` variable.
4. Create a fresh staging venv with Python 3.12 using `python -m venv --copies`
   so the certified interpreter is an ordinary file in the venv. Install the retained lock with
   `pip install --require-hashes -r <exact-lock>`, then install the retained
   Wheel non-editably with `pip install --no-deps <exact-wheel>`. Run
   `pip check`. Only `pip` may remain as the explicit non-runtime bootstrap
   distribution; no other extra distribution is accepted. Do not incrementally
   mutate an old or running venv.
5. Publish the completed root-controlled venv at the fixed `venv` path while
   stopped. Preserve the exact Wheel, lock, config, unit, and eventual identity
   in the release bundle.
6. Render/install the unit, verify its exact bytes and effective properties,
   then create the root-controlled identity:

   ```bash
   sudo /opt/binance-market-data-recorder/venv/bin/python \
     -m binance_market_data_recorder \
     --config /etc/binance-market-data-recorder/recorder.toml \
     systemd install --user "$SERVICE_USER" --group "$SERVICE_GROUP"
   sudo /opt/binance-market-data-recorder/venv/bin/python \
     -m binance_market_data_recorder \
     --config /etc/binance-market-data-recorder/recorder.toml \
     deployment identity-create --source-git-sha "$SOURCE_SHA" \
     --wheel "$EXACT_WHEEL" --dependency-lock "$EXACT_LOCK"
   sudo /opt/binance-market-data-recorder/venv/bin/python \
     -m binance_market_data_recorder \
     --config /etc/binance-market-data-recorder/recorder.toml \
     deployment verify
   ```

7. Explicitly start. Startup opens the Catalog, publishes `STARTING`, and keeps
   one heartbeat active while recovery runs. Ordinary local `SEALED` chunks
   whose manifest and Catalog immutable identity already match take the
   metadata-only startup path; crash-unstable states still require full payload
   validation before lifecycle advancement. Collectors remain gated until
   recovery and capacity observation complete. After capacity observation
   returns, startup rechecks the existing stop/shutdown authority. A stop that
   became authoritative while observation was outstanding must not be
   overwritten by Collector construction or a later `RUNNING`/READY promotion.
   If free space is <=10 GiB it records stop/gap evidence and exits cleanly
   without starting collectors. A stop requested during `recover_storage()` is
   honored between atomic recovery units and does not claim recovery completion.
8. Run `deployment readiness`; its fixed external deadline is 300 seconds.
   Preserve the JSON identity, verification, systemd-show, status, readiness,
   ownership, and journal evidence. A non-READY result rejects deployment.

Readiness requires active systemd, matching live MainPID/state PID and fresh
heartbeat, exact artifact/config/unit/profile/installed-dependency identity,
the protected venv/release control chain, the effective direct process
environment, an open valid Catalog, completed recovery, the existing full Spot
and USD-M core readiness (all three streams persisted and connected plus
snapshot and order-book sync), a current internal capacity observation, and
actual free bytes above 10 GiB. Capacity
WARNING/CRITICAL/EMERGENCY above 10 GiB and `INSUFFICIENT_DATA` with a current
safe observation may remain READY while exposing the degraded evidence.
Process existence or `systemctl is-active` alone is never READY.

The historical returning-observation startup-liveness race is fixed in the
recorded deployed artifact. Deployment and non-formal duration do not complete
M22.9 or transfer duration credit to current main or a current stopped multi-symbol
artifact.

Capacity planning for formal acceptance is separate from the hardcoded policy
and from the current non-formal campaign. A later point-in-time precondition
measurement found approximately 32.523 hours of runway, insufficient for the
independent 2h+12h+24h+72h+168h formal chain (about 278 hours), so formal T0
did not start.
This is not the next development action. MS4-B stopped-deployment review,
MS4-C bounded recovery review and MS4-D stage closure are complete. P2 exact
deployment/archive/capacity execution is complete and awaits primary review;
Formal M22.9 preparation remains a separately authorized program and must
verify the exact configured ProductKey set and all configured products. Any
later non-formal run remains separately authorized. Between independent runs,
disposable test data may be retired only by a separately authorized
consistency-safe procedure after evidence is frozen. No referenced or
unarchived Raw may be manually deleted.

## Stopped upgrade

Before mutation, preserve the old exact release bundle and identity. Verify
the new source SHA/Wheel/lock/config/unit inputs and perform compatibility
preflight. Gracefully stop the unit, require service state `STOPPED`, and verify
that active Raw has sealed. Preserve Raw, Catalog, receipts, remote lifecycle,
and service-state evidence unchanged.

Recreate a clean staging venv and repeat the exact dependency/Wheel, config,
unit, identity, static verification, start, recovery, capacity, and readiness
steps above. The upgrade is accepted only after READY. There is no blue/green
or mutation of a running environment.

## Compatible rollback

Rollback never rolls data back. First stop the failed candidate and run the
fail-closed compatibility check against the preserved canonical old identity:

```bash
sudo /opt/binance-market-data-recorder/venv/bin/python \
  -m binance_market_data_recorder \
  --config /etc/binance-market-data-recorder/recorder.toml \
  deployment rollback-check --target-identity "$OLD_IDENTITY"
```

The target must be an exact preserved M22 identity and its Wheel and lock must
still match. It must declare every durable remote lifecycle state present in
the current Catalog. A pre-M22 target or any unproved state compatibility is
refused. If compatible, recreate the fixed venv from the old exact lock/Wheel,
restore the old exact config/unit/identity and any matching startup authority,
daemon-reload, statically verify, explicitly start, and require recovery,
capacity, and readiness again. Never delete/modify Raw, Catalog rows, receipts,
or relabel remote states to make rollback pass.

## Safety termination and later restart

Actual observed free space <=10 GiB produces intentional
`HARD_RESERVE_SAFETY_STOP`: recovery completes first, active collectors drain
and seal, `DISK_EMERGENCY_STOP` and core-stream gap evidence persist, and the
process exits zero. Therefore `Restart=on-failure` does not make a low-space
restart loop. A crash/core failure remains nonzero and restart-eligible.

Space consumed or released by any co-resident process affects later actual
filesystem observations; Recorder makes no cause attribution. Space release
after a safety stop never auto-starts Recorder. The operator verifies free
space is above 10 GiB and explicitly starts it, causing the complete recovery,
capacity, and readiness sequence to run again.

## Acceptance boundary

M22.7B's exact host gate passed the recorded artifact identity and readiness
validation, including real systemd lifecycle, ownership, journald, restart,
and rollback compatibility preflight. Actual artifact rollback was not
exercised. Recorder production acceptance still follows:

```text
exact artifact identity -> readiness -> 2h -> 12h -> 24h -> 72h -> 168h
```

M22.7B starts none of those windows. Each future stage has an independent T0,
target, and evidence root. LAN Linux evidence does not substitute for VPS
evidence, and no stage starts automatically. M22.8 is accepted only as
isolated cross-machine failure evidence for fixed run
`m22.8-20260822T041913Z-23f1fcc7` on `vps-b5bfe3f8`; its nine scenarios passed
without touching production. Historical M22.9 incident evidence includes an
incomplete 24h attempt on an older artifact. For current main, formal M22.9 has
not started; the precondition-only capacity check belonged to the older
deployed artifact and stopped before T0, created no acceptance root, and
assigned no acceptance ID. Historical and non-formal runs provide no formal
duration credit.

The operator sequence is explicit and ordered:

```text
exact artifact identity -> readiness -> explicit duration stage
                         -> next stage only after eligible predecessor
```

The corrected future artifact must therefore run `2h -> 12h -> 24h -> 72h ->
168h`; no command promotes or starts the next stage. `--resume <stage-root>`
continues the same stage authority and reuses that stage's original T0 and
published evidence chain; it does not create a new duration window.

## Operations and recovery

### M22.9 repository-owned observer

The installed `binance-market-recorder deployment acceptance` commands are
read-only observers. Operators provide an operator-owned evidence directory
outside
`/var/lib/binance-market-data-recorder`, `/opt/binance-market-data-recorder`,
and `/etc/binance-market-data-recorder`; the tool rejects lexical and symlink
escapes into those trees. Identity and readiness are explicit gates. A stage
must be invoked explicitly with its eligible predecessor and has an independent
T0; `--resume` preserves the original T0 and published sample chain. The
observer never starts, stops, restarts, deploys, promotes, or claims Production
Ready, and it does not persist capacity observations.

### M22.9-P1 detached single-stage observation

P1 is reviewed complete and does not add a worker or scheduler. It documents
running the existing foreground observer under one external systemd 255
transient unit so an SSH
disconnect does not terminate it. The unit is `Type=exec`, runs as non-root
`bmdr:bmdr`, writes stdout/stderr to the journal, sets `UMask=0027` and
`NoNewPrivileges=yes`, uses `KillSignal=SIGINT` and `TimeoutStopSec=120s`, and
sets `Restart=no`. It must not use `--wait`, `--pipe`, or `--pty`; the launch
command waits only for systemd to accept the start job and for the `Type=exec`
process launch to succeed, not for the long task to finish. The SSH command
then returns while systemd continues to supervise the unit, and a separate SSH
session checks status and journal.

The operator first verifies the exact artifact, readiness, current capacity,
and registered archive target under the separately authorized Formal M22.9
preconditions. The evidence parent is a stable subdirectory of the registered
relative path, never the volume root:

```bash
CONFIG=/etc/binance-market-data-recorder/recorder.toml
ACCEPTANCE_ROOT=/srv/recorder-data/recorder-archive/acceptance/m22.9/<acceptance-id>
STAGE=2h
PREVIOUS_EVIDENCE="$ACCEPTANCE_ROOT/readiness-result.json"  # 2h only
UNIT=binance-market-data-acceptance-${STAGE}-<run-id>.service

sudo systemd-run --unit="$UNIT" \
  --property=Type=exec \
  --property=User=bmdr \
  --property=Group=bmdr \
  --property=UMask=0027 \
  --property=NoNewPrivileges=yes \
  --property=KillSignal=SIGINT \
  --property=TimeoutStopSec=120s \
  --property=Restart=no \
  --property=StandardOutput=journal \
  --property=StandardError=journal \
  -- /opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config "$CONFIG" deployment acceptance stage \
  --stage "$STAGE" \
  --previous-evidence "$PREVIOUS_EVIDENCE" \
  --evidence-root "$ACCEPTANCE_ROOT"
```

The existing command creates one immutable child `<stage>-<uuid>` under
`ACCEPTANCE_ROOT`; retain the exact child containing `stage-start.json` as
`STAGE_ROOT`. For later stages, `PREVIOUS_EVIDENCE` is the independently
verified immediate predecessor `stage-final.json` under the same parent. Never
rerun the new-stage form after an interruption.

Before leaving SSH, record the unit's InvocationID and lifecycle, then inspect
the journal from the same or a second SSH session:

```bash
sudo systemctl show "$UNIT" \
  -p Id -p ActiveState -p SubState -p MainPID -p InvocationID \
  -p Result -p ExecMainCode -p ExecMainStatus -p ActiveEnterTimestamp
sudo journalctl -u "$UNIT" --no-pager -o short-iso-precise
```

The failed unit state is intentionally left available for inspection. A
successful transient unit may still be garbage-collected by systemd, so
`not-found` is then possible and the immutable stage-final record remains
authoritative. To stop cleanly, use the unit only; systemd sends SIGINT and
waits up to 120 seconds. The incomplete evidence remains for review:

```bash
sudo systemctl stop "$UNIT"
sudo systemctl show "$UNIT" -p ActiveState -p SubState -p MainPID -p Result
sudo journalctl -u "$UNIT" --no-pager -o short-iso-precise
```

Resume only the exact `STAGE_ROOT` when it has no `stage-final.json`, the boot
ID/process incarnation/service instance/deployment identity are unchanged,
and no sample gap exceeds 600 seconds. The first command intentionally omits a
collection flag, so its failed `$UNIT` may still be loaded. Choose a fresh
unique supervisor unit before resuming; this changes only external process
custody and preserves the exact `STAGE_ROOT`, original T0, original `run_id`,
and immutable chain. It must not create a second T0. The existing observer's
`--resume` path reuses that original T0 and chain:

```bash
RESUME_UNIT=binance-market-data-acceptance-${STAGE}-resume-<resume-id>.service

sudo systemd-run --unit="$RESUME_UNIT" \
  --property=Type=exec \
  --property=User=bmdr \
  --property=Group=bmdr \
  --property=UMask=0027 \
  --property=NoNewPrivileges=yes \
  --property=KillSignal=SIGINT \
  --property=TimeoutStopSec=120s \
  --property=Restart=no \
  --property=StandardOutput=journal \
  --property=StandardError=journal \
  -- /opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config "$CONFIG" deployment acceptance stage --resume "$STAGE_ROOT"
```

Inspect the resume custody unit with the new unique name, not the possibly
still-loaded first-run unit:

```bash
sudo systemctl show "$RESUME_UNIT" \
  -p Id -p ActiveState -p SubState -p MainPID -p InvocationID \
  -p Result -p ExecMainCode -p ExecMainStatus -p ActiveEnterTimestamp
sudo journalctl -u "$RESUME_UNIT" --no-pager -o short-iso-precise
```

If any path, predecessor digest, boot/process/service/identity, or chain check
fails, preserve the evidence and stop. Normal `FAIL`/`INCOMPLETE` does not
auto-restart or start a new stage. Final review runs the exact installed
package's existing `verify_completed_stage` against `STAGE_ROOT`, confirms the
eligible predecessor, and only then explicitly authorizes the next stage. No
command here starts a later stage.

The properties above follow the official primary systemd references:
[`systemd.service.xml`](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml),
[`systemd.unit.xml`](https://github.com/systemd/systemd/blob/main/man/systemd.unit.xml),
[`systemd.exec.xml`](https://github.com/systemd/systemd/blob/main/man/systemd.exec.xml),
[`systemd.kill.xml`](https://github.com/systemd/systemd/blob/main/man/systemd.kill.xml),
and [`systemd-run.xml`](https://github.com/systemd/systemd/blob/main/man/systemd-run.xml).
`Type=`, `Restart=`, and `TimeoutStopSec=` are covered by
`systemd.service.xml`; transient lifecycle and possible unit GC by
`systemd.unit.xml`; `User=`, `Group=`, `UMask=`, `NoNewPrivileges=`, and journal
output by `systemd.exec.xml`; `KillSignal=` and the kill procedure by
`systemd.kill.xml`; and the default start-job wait plus optional
wait-for-completion/collection semantics by `systemd-run.xml`. P1 intentionally
uses neither a wait-for-completion mode nor a collection flag.

The future service must remain non-root, bounded, and recovery-first:

1. start with Raw/Catalog recovery before network capture;
2. preserve exact bytes and explicit gap evidence across reconnects;
3. give live capture/seal/Catalog priority over offline work;
4. surface archive absence or transfer failure without stopping capture until
   the hard reserve requires fail-closed stop;
5. retry archive and Catalog snapshot failures idempotently;
6. never claim `COMPLETE` for an unknown interval.

Future notification hooks may report archive pressure, degradation, stop, and
transfer outcomes through Firebase or another future messaging transport, but
notification failure must never control data integrity. Future Web UI work
remains separately authorized and broader than a status screen: it may include
professional market-data visualization, TradingView-like charting concepts,
order flow, depth, trades/volume, host state, Recorder state, completeness and
integrity, archive state, and future controlled operational actions. View,
Health, and Control concerns must remain separable from Raw/Recorder core.
