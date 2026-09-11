# MS4 qualification runbook

Status: **MS4-B target preflight/stopped deployment REVIEWED_COMPLETE; the
original MS4-C window is EXECUTED_PARTIAL_NOT_ACCEPTED; the separate R3
non-formal recovery supplement and MS4-D bounded stage closure are
REVIEWED_COMPLETE; overall MS4 is REVIEWED_COMPLETE for the non-formal bounded
four-ProductKey core**.
This is a preparation reference, not a copy-and-run deployment script. Target
inputs and target-specific publication/remote-archive commands remain subject
to review before any continuation. No command authorizes VPS access, deployment
or Binance traffic by itself. See
[`MS4-C-20260910`](../milestone_evidence/MS4-C-20260910.md) for the original
window and R3 supplement disposition; it is the bounded MS4 record, not
Formal M22.9 acceptance.

The runbook uses the existing CLI, systemd manager, deployment identity and
existing read-only status/report interfaces. It does not introduce a scheduler, a second
service, a GUI, a benchmark framework, or a new data path.

## 1. Freeze the inputs (MS4-B completed/report-derived)

Use a clean checkout at the exact code authority below. The merge commit is a
Git identity; the second parent is the reviewed implementation candidate. Do
not rebuild from an unrecorded working tree.

```bash
SOURCE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
SOURCE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
EXPECTED_LOCK_SHA256=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335

git fetch origin
git show -s --format='%H %T %s' "$SOURCE_SHA"
test "$(git rev-parse "$SOURCE_SHA^{tree}")" = "$SOURCE_TREE"
sha256sum requirements/linux-x86_64-python312.lock
```

The proposed profile fragment is
`docs/runbooks/MS4_candidate_profile.toml.example`. Its exact ProductKeys
are `(spot,BTCUSDT)`, `(spot,ETHUSDT)`, `(um_perpetual,BTCUSDT)` and
`(um_perpetual,ETHUSDT)`. The final config must be parsed independently and
must contain exactly those four keys before the service is started. Confirm
`ETHUSDT` eligibility from an allowed official source immediately before live
execution and record URL, UTC retrieval time, content SHA-256 and conclusion;
this local run intentionally did not make that source request.

The implementation-enforced `vps-production-v1` paths are:

```text
ARTIFACT_ROOT=/opt/binance-market-data-recorder
VENV=/opt/binance-market-data-recorder/venv
PYTHON=/opt/binance-market-data-recorder/venv/bin/python
CONFIG=/etc/binance-market-data-recorder/recorder.toml
DATA_ROOT=/var/lib/binance-market-data-recorder
UNIT=/etc/systemd/system/binance-market-data-recorder.service
SERVICE=binance-market-data-recorder.service
```

Confirm these paths on the named target before use. Do not substitute a
hostname, data root, archive destination, service user or group from this
document. The service principal must be an existing dedicated non-root user
and group. Production uses `network_proxy_mode = "direct"`; do not place a
proxy URL or credential in the config, environment, logs or evidence.

Set the canonical path variables listed above in one controlled shell before
using later snippets. Required operator variables, intentionally unset here,
are:

```bash
TARGET_HOST=<operator-supplied exact host>
SERVICE_USER=<operator-supplied existing non-root user>
SERVICE_GROUP=<operator-supplied existing non-root group>
LOCAL_CONFIG=<operator-supplied complete configuration file>
ARCHIVE_STORAGE_ID=<operator-supplied registered storage id>
ARCHIVE_DESTINATION=<operator-supplied archive-machine project directory>
OLD_IDENTITY=<operator-supplied preserved compatible deployment identity>
EVIDENCE_ROOT=<operator-supplied evidence directory outside Recorder roots>
```

## 2. Build and prove the target-compatible artifact (MS4-B completed/report-derived)

Run the following on an Ubuntu 24.04 LTS x86_64 preparation environment (not
the production service host unless explicitly authorized). Use the repository
lock exactly; the runtime lock and CI tool lock are separate.

```bash
python3.12 --version
python3.12 -m pip install --require-hashes \
  -r requirements/linux-x86_64-python312.lock
python3.12 -m pip install -r requirements/ci-linux-python312.lock
python3.12 -m build --no-isolation

# Use a fresh build output directory; never select an arbitrary old wheel.
# Set EXACT_WHEEL to the one artifact just built, then verify its hash.
: "${EXACT_WHEEL:?set the exact newly built wheel path}"
EXACT_WHEEL_BASENAME="$(basename "$EXACT_WHEEL")"
sha256sum "$EXACT_WHEEL" requirements/linux-x86_64-python312.lock

python3.12 -m venv --copies /var/tmp/bmdr-ms4-production-venv
/var/tmp/bmdr-ms4-production-venv/bin/pip install \
  --require-hashes -r requirements/linux-x86_64-python312.lock
/var/tmp/bmdr-ms4-production-venv/bin/pip install --no-deps "$EXACT_WHEEL"
/var/tmp/bmdr-ms4-production-venv/bin/pip check
/var/tmp/bmdr-ms4-production-venv/bin/python -c \
  'from pathlib import Path; from binance_market_data_recorder.service.deployment_identity import verify_installed_dependencies; e = verify_installed_dependencies(Path("requirements/linux-x86_64-python312.lock")); assert e["exact_match"] is True and e["recorder_distribution_separate"] is True; print(e)'
```

This target step was **PENDING in the historical MS4-A snapshot**. The
owner-supplied Tokyo VPS report records it complete for the reviewed MS4-B
stopped deployment: exact wheel SHA
`cfce08f747bf53372e4619d37bdfdbab9a6b3bd39c7f09337ddf86c9286b5602`, lock SHA
`44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335`, and
package version `0.1.0a1`. The merged source also passed the same Ubuntu x86_64
lock/build/clean-wheel path in CI run `34436773366`. Direct inspection of the
private VPS evidence bundle was not performed locally;
`PRIVATE_EVIDENCE_BUNDLE_DIRECT_INSPECTION=NOT_RUN_LOCAL`.

Copy the one verified Wheel and the exact lock into a root-controlled release
directory beneath `ARTIFACT_ROOT` while stopped. Never rebuild after hashing.
Create a clean copied venv; do not incrementally mutate a running venv:

```bash
sudo install -d -o root -g root -m 0755 "$ARTIFACT_ROOT"
sudo install -d -o root -g root -m 0755 "$ARTIFACT_ROOT/releases/$SOURCE_SHA"
sudo install -o root -g root -m 0644 "$EXACT_WHEEL" \
  "$ARTIFACT_ROOT/releases/$SOURCE_SHA/"
sudo install -o root -g root -m 0644 requirements/linux-x86_64-python312.lock \
  "$ARTIFACT_ROOT/releases/$SOURCE_SHA/"
```

The final venv must be published at the code-enforced `VENV` path and remain
root-controlled, symlink-free and not writable by `SERVICE_USER`. The target
operator must preserve the exact Wheel, lock, source SHA, config, unit and
deployment identity as one release bundle.

## 3. Prepare the complete configuration while stopped (MS4-B completed/report-derived)

Create `CONFIG` as `root:SERVICE_GROUP` mode `0640`. Start from the fragment,
then add the confirmed canonical `DATA_ROOT`; do not leave a placeholder in a
deployable file. The selection and initial auxiliary policy are:

```toml
[recorder]
data_root = "/var/lib/binance-market-data-recorder" # use only after host confirmation
capacity_profile = "vps-production-v1"
network_proxy_mode = "direct"
spot_symbols = ["BTCUSDT", "ETHUSDT"]
usdm_symbols = ["BTCUSDT", "ETHUSDT"]
log_level = "INFO"
rotation_seconds = 60.0
rotation_bytes = 134217728
durability_interval_seconds = 1.0
ingress_queue_capacity = 8192
max_frame_bytes = 16777216
heartbeat_seconds = 5.0
sleep_gap_threshold_seconds = 30.0
prevent_sleep = false

side_mark_price_enabled = false
side_liquidation_enabled = false
side_premium_index_enabled = false
side_funding_history_enabled = false
side_funding_info_enabled = false
side_open_interest_enabled = false
side_exchange_info_enabled = false
spot_exchange_info_enabled = false
side_open_interest_statistics_enabled = false
side_taker_buy_sell_volume_enabled = false
side_global_long_short_ratio_enabled = false
side_top_long_short_account_ratio_enabled = false
side_top_long_short_position_ratio_enabled = false
side_basis_enabled = false
```

The path above is the existing implementation's canonical VPS path, not a
claim about a host. The service code rejects a different path for
`vps-production-v1`; an operator must still confirm ownership and intent.
Hash the complete final file and record `CONFIG_SHA256`. Do not set any
`BINANCE_MARKET_RECORDER_*` environment variable for this profile.

Before installing the unit, use only non-mutating checks:

```bash
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" config show
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" doctor
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" status
sha256sum "$CONFIG"
```

`doctor` may report its existing generic x86_64 informational warning. It does
not replace exact installed identity/readiness verification.

## 4. Install, identify and verify the stopped service (MS4-B completed/report-derived)

The following is the retained MS4-B target procedure and is recorded as
completed in the owner-supplied Tokyo VPS report. It changes the named host;
future operators must confirm the service is stopped and the data root is the
intended project-owned subdirectory before any mutating command. Recorder does
not provision accounts or own the host.

```bash
sudo install -d -o "$SERVICE_USER" -g "$SERVICE_GROUP" -m 0750 "$DATA_ROOT"
sudo install -d -o root -g "$SERVICE_GROUP" -m 0750 "$(dirname "$CONFIG")"
# Install the complete CONFIG as root:SERVICE_GROUP mode 0640 here.
sudo install -o root -g "$SERVICE_GROUP" -m 0640 "$LOCAL_CONFIG" "$CONFIG"

# After preserving the old release and while STOPPED, create a fresh venv
# directly at its final canonical path. Do not move a populated venv: installed
# entry-point shebangs contain absolute paths. The exact old-venv preservation
# commands are target-specific and were recorded in the MS4-B evidence; do not
# infer or rerun them as part of this documentation closure.
sudo python3.12 -m venv --copies "$VENV"
sudo "$PYTHON" -m pip install --require-hashes \
  -r "$ARTIFACT_ROOT/releases/$SOURCE_SHA/linux-x86_64-python312.lock"
sudo "$PYTHON" -m pip install --no-deps \
  "$ARTIFACT_ROOT/releases/$SOURCE_SHA/$EXACT_WHEEL_BASENAME"
sudo "$PYTHON" -m pip check

sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" \
  systemd install --user "$SERVICE_USER" --group "$SERVICE_GROUP"
sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" \
  deployment identity-create --source-git-sha "$SOURCE_SHA" \
  --wheel "$ARTIFACT_ROOT/releases/$SOURCE_SHA/$EXACT_WHEEL_BASENAME" \
  --dependency-lock "$ARTIFACT_ROOT/releases/$SOURCE_SHA/linux-x86_64-python312.lock"
sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" deployment verify
```

`systemd install` renders the existing managed unit and enables it; it does
not accept an unmanaged unit. `identity-create` writes the root-controlled
`deployment-identity.json` next to `CONFIG` and binds the actual effective
systemd properties, package/venv files, lock, config, unit and legacy
classification sidecar state. Record the resulting identity SHA-256 and all
file hashes. Never hand-edit that JSON.

Before any start, run the read-only legacy reconnect preflight if a prior
classification authority is present:

```bash
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" \
  recovery legacy-reconnect-preflight
```

An ineligible or missing required preflight is a stop condition, not a repair
request to this preparation work.

## 5. MS4-C start and validate readiness (original window; R3 recovery reviewed)

The 2026-09-10 attempt started only after separate authorization and the
owner-selected archive target were supplied. Any continuation must be
explicitly reviewed first; no service manager action is automatic in this
runbook.
Startup must complete recovery and current capacity observation before it can
construct collectors.

Immediately before the explicit start, capture ETHUSDT eligibility from an
allowed official source and record its URL, UTC retrieval time, content SHA-256
and conclusion. If the archive machine/SSH/destination/workflow authority is
absent, or explicit start authorization is absent, remain stopped.

```bash
sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" systemd start
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" systemd status
sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" deployment verify
sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" deployment readiness
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" status
```

The outer MS4 startup qualification envelope is an absolute 15 minutes after
the explicit start request. The existing deployment-readiness observer has a
single bounded implementation observation interval of 300 seconds. That
interval is not an automatic extension of the 15-minute stage envelope; do
not invoke it so early or interpret its bound as permission to extend the
stage deadline. Use existing systemd/status/service-state read-only
observation as appropriate, and require the final authoritative deployment
readiness result to return `READY` within the outer deadline. Do not count
`systemctl is-active`, a process PID, or a single boolean as readiness. Record
the configuration-bound expected set, actual set, per-product readiness,
connected/persisted core streams, snapshot/order-book sync, recovery result,
capacity state, process incarnation and exact identity. All four ProductKeys
must be ready with no extras before T0.

For an authorized observation window, preserve structured JSON and journal
evidence at a fixed cadence without doing an expensive full Catalog scan for
every sample:

```bash
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" status
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" report daily --date YYYY-MM-DD
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" storage forecast
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" archive status
journalctl -u "$SERVICE" --since '<fixed UTC start>' --until '<fixed UTC end>'
```

### Non-formal MS4-C evidence boundary

Do not invoke `deployment acceptance identity/readiness/stage` for this
non-formal MS4 window. Those commands implement the separate M22.9 acceptance
chain. Reusing a 2-hour duration does not authorize that chain.

On future MS4-C authorization, freeze a dedicated MS4 evidence directory,
record the deployment identity/hash, boot ID, process incarnation, UTC and
monotonic T0 after all products are ready, and UTC/monotonic T1. Preserve the
existing status, forecast, readiness and bounded journal outputs at a fixed
cadence (proposed 60 seconds), plus the final integrity and archive results.
Use the host's monotonic elapsed time for the duration, not sample counts.
The exact target sampling commands are a required MS4-C runbook execution
item. No new observer implementation is requested. The resulting
record must say NONFORMAL_MS4 and grants zero Formal M22.9 duration credit.

The separate 2026-09-11 R3 supplement passed its bounded recovery gate. Its
original VPS evidence is `/root/MS4-C-RECOVERY-20260911-R3/evidence/recovery-audit.json`;
the local review mirror is under
`/Users/amada/Downloads/BinanceMarketDataRecorder-MS4-C-20260910/recovery-r3-evidence/`;
the classification is `NONFORMAL_MS4_RECOVERY_SUPPLEMENT` and formal M22.9
credit is zero. It used fresh official Spot/USD-M eligibility, an exact
MainPID-owned socket disconnect, and existing status/journal/Catalog reads.
All four expected ProductKeys stayed ready with receive progress across 15
post-fault observations; 12 core contexts passed sealed validation, Catalog was
clean, and no active `.partial` remained. The original two-hour window remains
`EXECUTED_PARTIAL_NOT_ACCEPTED`; this supplement does not transfer duration
credit or start Formal M22.9.

## 6. MS4-C capacity and archive gate

Use the existing `vps-production-v1` reserve policy without lowering it:

```text
WARNING  <= 18 GiB or ETA <= 7 days
CRITICAL <= 14 GiB or ETA <= 72 hours
EMERGENCY <= 12 GiB or ETA <= 24 hours
HARD RESERVE <= 10 GiB
```

For free bytes `F0`, `F1` and monotonic interval `dt`, calculate
`g_net=max(0,(F0-F1)/dt)` and `T_runway=(F1-10 GiB)/g_net` when growth is
positive. Use the latest measured remaining free space `F1` as the runway
authority. Use the existing 1h/6h/24h/7d observations where present, do not
attribute shared-host space changes, and treat unverified archive release as
zero. The proposed startup + 2h steady + 15m recovery + 15m shutdown + 15m
margin envelope is 3 hours. Refuse T0 if free space is at/below reserve or
the measured runway does not cover that envelope. A hard-reserve stop seals,
records `DISK_EMERGENCY_STOP` and gap evidence, and exits; it never deletes
unarchived Raw.

An archive destination and the machine hosting it are operator inputs.
`storage status`, `archive retry --storage-id ...` and `archive verify ...`
operate on storage registered on the machine executing those commands; they
are not a VPS-to-local remote receive/receipt procedure. Do not run them on the
VPS as a substitute for the remote archive boundary.

The attempt selected the existing remote source/transport/receive/verify/receipt
workflow for `greencloud-tokyo-01` and the owner-authorized MacBook Downloads
internal-folder test target. One receiver-only cycle is verified in the
attempt report; it created no remote authority and did not retire the source.
The archive timer is explicitly `disabled` after the run. A host reboot does
not automatically schedule the timer; manual archive remains unauthorized.
The transfer protocol/library exists, but this selected test-path result does
not certify the production archive workflow or a receipt-bound Catalog
snapshot. Preserve
verified receive/readback/size/SHA-256/manifest/receipt evidence and keep
source retirement separately authorized. An unavailable destination means
archive acceptance NOT RUN, not PASS.

## 7. MS4-C stop and verify

Stop explicitly at the frozen observation end or on any stop condition:

```bash
sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" systemd stop
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" systemd status
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" status
"$PYTHON" -m binance_market_data_recorder --config "$CONFIG" archive status
journalctl -u "$SERVICE" --since '<fixed UTC start>' --until '<fixed UTC end>'
```

Require graceful `STOPPED`, sealed active tails, no orphan active ownership,
and preserved process-session/gap evidence. Keep the archive timer
`disabled`. Do not delete Raw, Catalog,
manifests, receipts or evidence as part of stopping.

## 8. MS4-C fail-closed rollback

Rollback never rolls data backward. Before stopping a candidate, retain the
prior exact identity, Wheel, lock, config, unit, startup authority and a
separate evidence copy. A pre-MS1 binary is not automatically compatible with
the MS1+ Catalog or later durable remote lifecycle state.

First run the existing compatibility check against the preserved identity:

```bash
sudo "$PYTHON" -m binance_market_data_recorder --config "$CONFIG" \
  deployment rollback-check --target-identity "$OLD_IDENTITY"
```

This check must return `COMPATIBLE` and the target must independently be
verified to understand the current Catalog schema, MS1 product identities and
every durable remote state present. A target that only has a retained Wheel,
or any target with unproved MS1+ Catalog support, is **not** a rollback target.
If the check fails, leave the candidate stopped, preserve its Raw/evidence and
escalate; do not point the old program at the new state. Use a separately
retained old data root only if its compatibility and ownership were already
proven, never as an inferred workaround.

When compatibility is proven, perform the controlled sequence:

1. Stop the candidate and verify `STOPPED` plus sealed active Raw.
2. Preserve all candidate Raw, Catalog, manifests, receipts, identities and
   gap evidence; do not rewrite or delete them.
3. Recreate a clean venv from the old exact lock and install the old exact
   Wheel non-editably. Restore the old exact config, unit, identity and any
   matching startup authority; never edit identity JSON.
4. Run `deployment verify`, `systemd install` if the old unit is the managed
   one, and `deployment rollback-check` again before `systemd start`.
5. Start explicitly and require recovery, capacity and configuration-bound
   readiness. Record the stopped interval as a gap if collection did not run.

Any failed identity, Catalog, reserve, recovery or readiness gate leaves the
service stopped and the data intact. A rollback does not grant duration credit
to a different artifact and does not close MS4.

## 9. MS4-C stop conditions and scope boundary

Stop the qualification and preserve evidence on false readiness, unexpected
ProductKeys, unresolved integrity/gap evidence, shared-gate bypass,
unbounded/backpressured queues, resource exhaustion, reserve breach, failed
archive verification, identity drift, or an unbounded recovery. Repair only a
demonstrated issue in a new reviewed artifact/config identity. Do not lower
thresholds, skip products/streams, deliberately provoke live 418/429, run
heavy Normalize/Replay/Backfill on the live host, access credentials, or start
Formal M22.9 from this runbook.

MS4-A local artifact/document preparation and MS4-B target preflight/stopped
deployment review are complete. The original owner-authorized MS4-C attempt
remains an executed-partial record because controlled recovery was not
executed in its approved window. The separate passing R3 non-formal supplement
closes the MS4-C recovery gate for review with zero Formal M22.9 credit. Any
additional live traffic or source retirement requires a fresh explicit review;
MS4-D bounded stage closure is complete. Formal M22.9 preparation is the next
separately authorized program and is not started from this runbook.
