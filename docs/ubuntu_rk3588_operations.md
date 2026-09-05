# Ubuntu ARM64 / RK3588 operations

Status: M20 Ubuntu ARM64/RK3588 Developer Preview / Soak Candidate. All
artifact and validation statements in this profile are historical. Current
GitHub main is not deployed; consult `CURRENT_PRODUCTION_STATE.md` for current
authority. This profile is
not the primary production authority and is not Production Ready. The approved
future production profile is Ubuntu 24.04 LTS x86_64 on a shared VPS; see
`vps_operations.md` and `test_environment_matrix.md`.
The deployed artifact `f659895…` completed the later FORMAL 72h observational
gate (PASS: 27/27 explicit WS transitions, +0 unmarked, 0 false-complete,
27/27 first-new Raw `sequence_gap`) but became NOT ELIGIBLE FOR 168H when the
restart-only orphan-intent defect was discovered; the 168-hour soak has not
run. The corrected M21.4.11-R3.x artifact is merged to `main` through PR #11
but is NOT DEPLOYED:
production validation is PENDING and the full staged validation chain must
reset after a separately authorized deployment.

This document remains specific to RK3588/LAN Linux. It must not be relabelled
as VPS operations, and its historical production evidence must not be promoted
to VPS evidence.

## Fixed layout

```text
source checkout  /home/orangepi/BinanceMarketDataRecorder
production venv  /opt/binance-market-data-recorder/venv
configuration    /etc/binance-market-data-recorder/recorder.toml
internal data    /var/lib/binance-market-data-recorder
unit             binance-market-data-recorder.service
logs             journald
```

The checkout is not a data directory. The Collector runs as the User/Group
selected at install time, never root. Do not replace Ubuntu's
`/usr/bin/python3`; provide a separate Python `>=3.12,<3.13`.

## Build and first installation

Build in the existing checkout and install the final Wheel, not an editable
checkout:

```bash
python3.12 -m build --no-isolation
sudo install -d -m 0755 /opt/binance-market-data-recorder
sudo python3.12 -m venv /opt/binance-market-data-recorder/venv
sudo /opt/binance-market-data-recorder/venv/bin/python -m pip install \
  dist/binance_market_data_recorder-0.1.0a1-py3-none-any.whl
sudo install -d -o orangepi -g orangepi -m 0750 \
  /var/lib/binance-market-data-recorder
sudo install -d -o root -g orangepi -m 0750 \
  /etc/binance-market-data-recorder
```

Create `/etc/binance-market-data-recorder/recorder.toml` as root, group-readable
by the service group, mode `0640`:

```toml
[recorder]
data_root = "/var/lib/binance-market-data-recorder"
network_proxy_mode = "explicit"
network_proxy_url = "http://127.0.0.1:7890"
ingress_queue_capacity = 262144
log_level = "INFO"
```

This loopback URL contains no credentials. Never add proxy username/password,
API keys, account fields, controller secrets, node authentication, or
subscription URLs.

The RK3588 acceptance host uses the bounded `262144` capacity for both the
WebSocket receipt queues and Raw spool ingress queues. The generic default
remains `8192`. Raw time rotation is deterministically phase-staggered by
market/stream so that eMMC compression and fsync work does not start for every
stream in the same instant. Queue depth, resident memory, seal latency, and
overflow faults remain M21 soak observations; increasing the bound is not a
claim of unlimited buffering or zero interruption.

Validate before installation:

```bash
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml doctor
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml config show
```

Install and start:

```bash
sudo /opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml \
  systemd install --user orangepi --group orangepi
sudo /opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml systemd start
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml systemd status
```

The unit has `After` and `Wants` for `network-online.target` and
`mihomo.service`, not `Requires`. Proxy process restarts therefore become
visible reconnect/resync events rather than coupling service liveness to
Mihomo's unit state.

## Proxy operation

Three modes are exact:

| Mode | Behavior |
| --- | --- |
| `direct` | WebSocket, urllib, SDK, and Historical all ignore shell proxy variables |
| `environment` | standard proxy variables and `no_proxy`; intended for interactive use |
| `explicit` | one validated unauthenticated HTTP(S) URL for every production exit |

Production systemd uses `explicit`; it does not inherit SSH shell proxy
variables. `status`, `doctor`, and `config show` reveal only mode, scheme,
loopback, and port. Do not automate `mihomo-select`; online acceptance uses the
operator's already-selected fixed node. Do not enable TUN, redirect/TProxy,
iptables, nftables, or policy routing for this service.

## Status, logs, stop, and restart

```bash
systemctl status binance-market-data-recorder.service
journalctl -u binance-market-data-recorder.service --since today
/opt/binance-market-data-recorder/venv/bin/binance-market-recorder \
  --config /etc/binance-market-data-recorder/recorder.toml status
sudo systemctl stop binance-market-data-recorder.service
sudo systemctl start binance-market-data-recorder.service
sudo systemctl restart binance-market-data-recorder.service
```

SIGTERM must reach `STOPPED` after draining and sealing within
`TimeoutStopSec=90s`. A stale PID/heartbeat is never reported as healthy.

## Safe update and rollback

Linux blue/green deployment is not certified in M20. Use a controlled,
gap-explicit update:

1. Save the current Wheel and configuration version.
2. Stop the unit; verify `STOPPED` and sealed manifests.
3. Install the new final Wheel into the production venv.
4. Run `doctor`, `config show`, and import smoke.
5. Run `systemd install` again (idempotent unit refresh), then start.
6. Require both markets READY and verify new Raw/Catalog evidence.

Rollback repeats the stop/seal sequence, installs the saved prior Wheel,
refreshes the unit, and starts. Never delete or edit
`/var/lib/binance-market-data-recorder`.

### First corrected restart: mandatory legacy reconnect preflight (M21.4.11-R3.3)

The first deployment of the M21.4.11-R3.3-corrected artifact adds a
MANDATORY pre-start compatibility sequence BEFORE the first controlled
service restart. The OLD production service keeps running while the
sequence executes; nothing below touches production data or the
production classification state until the final explicitly authorized
deployment step. SCHEMA_MIGRATION_REQUIRED=false and
CATALOG_MUTATION_REQUIRED=false, but
ADDITIVE_COMPATIBILITY_AUTHORITY_REQUIRED=true and
PRESTART_LEGACY_CLASSIFICATION_REQUIRED=true: this is a compatibility
pre-start action, not a schema migration. R3.3 removed the legacy
"no possible parent" absence proof, so every AMBIGUOUS legacy candidate
must receive an explicit authority classification; new intents emitted
by the corrected runtime are versioned (`intent_schema:
reconnect-seal-intent.v2`) and materialize REQ-103 automatically.

1. **Keep the OLD service running.** Do not stop
   `binance-market-data-recorder.service` during the exploratory
   preflight.
2. **Build/review the corrected artifact separately** (no production
   venv modification): canonical Wheel identity verified per
   "Artifact identity" below.
3. **Run the READ-ONLY exploratory preflight against current production
   data** with the corrected artifact:

   ```bash
   binance-market-recorder \
     --config /etc/binance-market-data-recorder/recorder.toml \
     recovery legacy-reconnect-preflight
   ```

   The command derives the layout without creating anything, opens the
   Catalog read-only, mutates no Catalog/Raw/manifest/authority state,
   and prints the deterministic inventory (schema
   `legacy-reconnect-preflight.v1`). It never repairs a missing layout
   directory; a missing data root or Catalog is an error. Exit status
   `0` means eligible; exit `2` means ineligible or error — the JSON
   Boolean `first_corrected_startup_eligible` is printed in both cases
   and automation must key on the exit code. The authority is resolved
   NEXT TO the loaded config file
   (`/etc/binance-market-data-recorder/legacy_reconnect_classifications.json`),
   never inside the data root; preflight and startup use the exact same
   rule (M21.4.11-R3.4).
4. **Export the deterministic candidate inventory** to a recorded
   evidence file outside the data root; verify it byte-identical on a
   second run. Review every `degraded_authority` blocker (malformed
   lifecycle rows) with its event identity and reason: these block the
   first corrected start until a separately documented operator
   decision is applied; they are never silently skipped.
5. **Independently review each AMBIGUOUS candidate** against
   `docs/adr/0027-reconnect-boundary-integrity.md` (R3.3 rules): the
   production orphan shape (um_perpetual `book_ticker`, parent
   `70ace625…`, orphan `33e6420b…`, marker `7223d5ba…`) is an
   `extension_orphan`; only durable identity proofs may classify.
   "No parent found" is NOT a classification reason.
6. **Create the classification authority OFFLINE** (not while the
   recorder runs): schema `legacy-reconnect-classification.v3`, entries
   bound to `(gap_id, market, stream, chunk_id,
   classification_evidence_sha256)` where
   `classification_evidence_sha256 = sha256(canonical_json({"chunk_id",
   "seal_intent", "verified_frames"}))` computed from the exact
   persisted SEALING evidence (see `operations.md` for the digest
   definition; the preflight output prints the digest for every
   candidate). Never hard-code production UUIDs anywhere in the
   repository.
7. **Validate the authority with the read-only preflight**: every
   AMBIGUOUS candidate must be classified; stale/unmatched/contradictory
   and degraded counts must be zero.
8. **Require `first_corrected_startup_eligible=true`** (exit code 0)
   before any further step.
9. **Atomically install the authority file** into the ROOT-CONTROLLED
   configuration namespace (M21.4.11-R3.4 trust boundary), NOT into the
   service-writable data root:
   `/etc/binance-market-data-recorder/legacy_reconnect_classifications.json`.
   The parent directory is already owner=root group=orangepi mode=0750
   (the same directory that holds `recorder.toml`); the file itself is
   owner=root group=orangepi mode=0640 — root/operator writes AND
   replaces the pathname, the Recorder service group can only read it,
   everyone else has no access, and because the service principal does
   NOT own the containing directory it cannot unlink/rename/replace the
   authority pathname. The recorder itself never writes the file. Write
   the temporary file (`…json.partial`) in the SAME directory/filesystem
   WITH the final owner/group/mode already applied, fsync it, `mv` it
   over the final path, fsync `/etc/binance-market-data-recorder`.
   Startup and preflight resolve the authority as `config_file.parent /
   legacy_reconnect_classifications.json`, so both read only this final
   path. This step may run while the old service is still running. Do
   not move or edit `recorder.toml`.
10. **STOP the old service** (`systemd stop`), as part of the separately
    authorized deployment.
11. **Run the FINAL read-only preflight against the frozen Catalog plus
    the installed authority** (same command as step 3). This final
    post-stop coverage validation is MANDATORY: it must exit `0` with
    `first_corrected_startup_eligible=true`. Do not proceed on a
    nonzero exit.
12. **Install the corrected Wheel, refresh the systemd unit, and start**
    only after step 11 passed. Corrected startup re-executes the ENTIRE
    global predecision pass itself before any collector starts
    (Phase A read-only pre-decision, then Phase B mutations only if the
    decision set is safe), so no candidate-set race between the final
    preflight and service start can bypass the gate.
13. **Verify recovery actions** in the startup logs: only
    `pending_discontinuity_materialized` for PROVEN/classified
    legitimate candidates and `extension_orphan_ignored` for classified
    orphans; no `RECOVERY_LEGACY_PREDECISION_INELIGIBLE`.
14. **Proceed to readiness** and reset the staged validation chain:
    exact artifact identity → readiness → 2h → 12h → 24h → 72h → 168h.

Do not execute this deployment as part of the code correction; it
requires separate authorization. The 168-hour window is never started
automatically.

## Already-mounted external archive directory

The OS/operator mounts the filesystem first. Recorder performs no mount,
unmount, format, repair, repartition, or udev action.

```bash
findmnt --json
lsblk --json
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  storage list
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  storage inspect /media/orangepi/archive/Recorder
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  storage register /media/orangepi/archive/Recorder
```

Registration requires a candidate directory on an already-mounted filesystem
that meets all of the following conditions:

- the filesystem is already mounted by the OS (Recorder never mounts);
- its source is a resolvable block device identifiable through
  `/proc/self/mountinfo`, `findmnt --json`, and `lsblk --json`;
- the device is not part of the root backing-device lineage (i.e., does not
  share a parent block device with the root filesystem);
- the mountinfo, findmnt, and lsblk evidence is internally consistent;
- the filesystem has a reliable filesystem UUID;
- the registered subdirectory already exists, is writable, and the user
  explicitly selects and registers it;
- the marker/storage_id and write/fsync/rename/readback probe succeed.

RM (removable), HOTPLUG, and TRAN fields are recorded as auxiliary observations
only. A USB-SATA or USB-NVMe bridge device that reports `RM=false` and
`HOTPLUG=false` is still eligible for discovery and registration when the
conditions above are met.

Recorder never automatically registers, mounts, unmounts, formats, repairs,
partitions, or creates udev rules. Real physical external disk validation
remains unexecuted.

If the filesystem disappears, collection continues, the archive attempt is
reported failed/absent, and the internal source is retained. M20 has no trusted
udisks eject backend: `storage eject` reports `MANUAL_ACTION_REQUIRED` and does
not claim safe removal. Stop/finish archive work and use trusted OS tooling.

## Soak plan (not executed in M20)

M21 owns both runs:

- 7-day/168-hour: repeated connection rotation, proxy restart, service restart,
  resource trends, gaps/resyncs, seal/catalog consistency, and archive backlog.
- 30-day operational observation: disk growth/forecast, journal retention,
  external disappearance/reinsertion, update/rollback drills, and alert review.

The separate 72-hour gate must also be recorded. The M21.4 validation
history: the original M21.4 production validation had a 72h integrity
failure (see `docs/milestone_evidence/M21.4-72h-failure-and-reconnect-
integrity.md`), which is preserved as history. The subsequent corrected
deployed artifact `f659895…` completed the later FORMAL 72h observational
gate with PASS (27/27 explicit transitions, +0 unmarked, 0 false-complete,
27/27 first-new Raw `sequence_gap`), but became NOT ELIGIBLE FOR 168H
because the restart-only orphan-intent defect was discovered before the
required 168h restart exercise. M21.4 2h and 12h windows
passed with independent evidence reviews; the formal 24-hour window passed
with corrective and contract forensic confirmation, and the natural gen5
backpressure recovery contract passed inside the formal window.
The 168-hour window remains pending and is never started
automatically. The corrected M21.4.11-R3.x artifact is NOT DEPLOYED and
must re-execute the full staged validation chain after a separately
authorized deployment. Until all long-run gates are completed the platform
remains a Soak Candidate and must not be described as Production Ready or
zero-interruption.

### Formal evidence rules for 72h/168h (24h corrective lessons)

The 24h window produced binding evidence rules for all later formal windows:

- **Journal boundaries derive from the formal T0/Target UTC nanoseconds, but
  journald filtering is not nanosecond-exact.** `journalctl`/systemd time
  parsing and the journal `__REALTIME_TIMESTAMP` field have **microsecond**
  resolution. Derive explicit UTC timestamps from the nanosecond T0/Target
  and pass explicit RFC3339 UTC timestamps with microsecond precision, e.g.
  `journalctl --since "2026-08-05 15:09:30.200566Z"`, or the `@<Unix-seconds>`
  syntax that the project has verified read-only on the target systemd
  version; do not assume fractional `@` forms are accepted without that
  read-only test. Never bare local-time strings. The original 24h export
  used `--since "2026-08-05 15:09:30"` (no `Z`), which journalctl
  interpreted as local time and shifted the formal boundary by 8 hours
  (FORMAL_JOURNAL_ARTIFACT_CONTAMINATION=true). Do not claim journal
  filtering itself is nanosecond-exact.
- **Export structured formats**: use `--output=json` / `--output=json-seq` /
  `--output=export` so every record carries `__REALTIME_TIMESTAMP` (UTC
  microseconds); verify first/last records' `__REALTIME_TIMESTAMP` against
  the derived UTC microsecond bounds (T0/Target nanoseconds truncated to
  microseconds) after every export. Boundary enforcement happens at
  journald's documented microsecond resolution; a record that cannot be
  classified inside/outside at that resolution is recorded as
  `BOUNDARY_PRECISION_AMBIGUOUS`, never guessed.
- **Formal samples strictly filtered by T0/Target nanoseconds**; post-Target
  data is reported separately as post-window current state and never mixed
  into formal statistics.
- **Catalog events are queried from Catalog**: `STREAM_DISCONTINUITY_*`
  operational events are Catalog-only writes and never appear in the
  journal; journal string searches cannot count them (the first corrective
  review's 0/0 count was invalid; Catalog holds 7 complete pairs gen0–gen6).
- **Raw gap evidence is read from Raw** (`capture_flags=sequence_gap`);
  **manifest status is read from manifests** (`gap`/`complete`).
- **Save both raw command output and parsed JSON** every round; generation
  failure must not discard raw evidence.
- **Never overwrite original evidence**: corrective/forensic reviews go to
  their own independent directories under the run root.
- **Recovery wording**: `queue_backpressure_recovered` (below low_watermark)
  is not stream recovery completion; the completion boundary is new
  connection + first-new `sequence_gap` persisted + Raw sync + Catalog
  COMPLETED. Internal zero-drop does not prove exchange-side completeness;
  a reconnect boundary stays `gap=true`/`complete=false`.
- **Saturation timing**: the 30 s budget is accumulated saturation time above
  low_watermark; timeout raises only when a later put again meets a full
  queue, so started→timeout spans are not continuous full-queue spans.

## M21.4 deployment notes

### Artifact identity

Production identity is the immutable deployed Wheel SHA-256 plus
`direct_url.json` match, non-editable install state, and Canonical Gate
static verification. CLI `--version` Git suffix is for display only and
may be affected by the runtime working directory. Run production CLI from
`/tmp` or another non-repository directory.

### Observation collection

When collecting structured observations from `systemctl show`, do not
blindly parse all values as numbers or timestamps. Fields such as
`NextElapseUSecRealtime` and `NextElapseUSecMonotonic` are timer properties
that should be preserved as raw strings. Save both raw command output and
parsed JSON; JSON generation failure must not discard the raw evidence.

### Backpressure

Do not actively trigger backpressure in production. The M21.4 repair
provides stream-level recovery when it occurs naturally, but the
recovery path should not be exercised through artificial load. The formal
24h window did observe one natural gen5 `book_ticker` cycle that passed its
recovery contract (RECOVERY_CONTRACT_PASS) and one gen6 cycle that started
inside the window and completed after Target (POST_WINDOW); post-window
recovery is valid for current health judgment only.
