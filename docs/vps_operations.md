# VPS Operations

Status: M22.7B deployable substrate implemented and real-host validated on the
recorded Ubuntu 24.04.4 LTS x86_64/systemd 255.4 host. Production deployment
and staged production validation are not claimed.

This document describes the intended Ubuntu 24.04 LTS x86_64 profile for a
shared 2 vCPU, 4 GiB RAM, 40 GB-class VPS. Ubuntu 22.04 x86_64 is a
compatibility target. macOS is a development/local profile and RK3588 remains
a separate LAN Linux validation and historical evidence profile.

## Responsibility boundary

The VPS runs only the integrity-critical live path:

- Binance public Spot/USD-M acquisition;
- Raw active/spool, sealing, and compression;
- one live Catalog and startup/crash recovery;
- gap, completeness, and provenance state;
- metrics and structured status; and
- support for local-client archive export.

Full Normalize jobs, heavy Replay/analytical scans, and Historical Backfill run
locally through offline execution profiles. They remain Recorder-owned
capabilities in the same distribution. A co-resident unrelated service is not
managed or inspected by Recorder.

## Network profile

Direct Binance connectivity is mandatory for the certified Germany VPS
profile. `environment` and `explicit` proxy modes remain available only outside
`vps-production-v1` for local development, fault injection, and LAN testing.
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

7. Explicitly start. Startup opens the Catalog, completes recovery, and takes
   an immediate actual capacity observation before Collector construction. If
   free space is <=10 GiB it records stop/gap evidence and exits cleanly without
   starting collectors.
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
without touching production. M22.9 later began on the exact production domain,
but its 24h result is INCOMPLETE after the R-054 Raw continuity defect; it is
not eligible for 72h. The local correction is not deployed and receives no
duration credit.

## Operations and recovery

### M22.9 repository-owned observer

The installed `binance-market-recorder deployment acceptance` commands are
read-only observers. Operators provide an evidence directory outside
`/var/lib/binance-market-data-recorder`, `/opt/binance-market-data-recorder`,
and `/etc/binance-market-data-recorder`; the tool rejects lexical and symlink
escapes into those trees. Identity and readiness are explicit gates. A stage
must be invoked explicitly with its eligible predecessor and has an independent
T0; `--resume` preserves the original T0 and published sample chain. The
observer never starts, stops, restarts, deploys, promotes, or claims Production
Ready, and it does not persist capacity observations.

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
