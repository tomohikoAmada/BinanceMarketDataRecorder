# Operations

Current implementation status and approved future architecture are distinct.
macOS LaunchAgent and Ubuntu ARM64/RK3588 systemd procedures below describe
implemented/local validation profiles. The primary production deployment
profile is Ubuntu 24.04 LTS x86_64 on a shared 2 vCPU/4 GiB/40 GB-class VPS; see
[`vps_operations.md`](vps_operations.md). The M22.7B deployment/readiness host
gate passed on the recorded Ubuntu host, but that VPS profile, remote archive
client, and Catalog snapshot transfer are not production deployed or
Production Ready.

The current M22.9 state is consolidated in
[`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md): the historical
24h result is INCOMPLETE, the service is STOPPED / NOT CAPTURING, and
Production Ready is NO. The local startup-liveness correction is not reviewed,
merged, built into a new artifact, or deployed.

Ubuntu ARM64/RK3588 systemd, explicit proxy, update/rollback, mounted external
directory, and M21 soak procedures are in
[`ubuntu_rk3588_operations.md`](ubuntu_rk3588_operations.md). Ubuntu is an M20
Developer Preview / Soak Candidate, not the primary production authority.

## Proxy status

`config show`, `doctor`, and `status` expose only proxy mode, scheme, loopback,
and port. They never expose the configured URL. `direct` ignores the shell,
`environment` honors standard variables plus `no_proxy`, and `explicit` uses
one credential-free HTTP(S) proxy for all Recorder network exits when selected.
The RK3588
validation systemd profile uses TOML `explicit`, never an SSH environment. The
certified Germany VPS profile requires `direct` mode. Its unit neutralizes the
upper- and lowercase standard proxy variables, and readiness rejects any
nonempty proxy authority in the live service process. Local and LAN proxy modes
remain testable outside `vps-production-v1`.

## Side-data and backfill status

Runtime market state includes each side task's `status`, `enabled`, `running`,
`attempts`, `accepted`, `failures`, `consecutive_failures`,
`last_success_at_utc_ns`, `last_error_type`, and `next_retry_at_utc_ns`.
Retrying/stale enabled tasks make network status `DEGRADED`; core collection
continues.

Always run `backfill plan` before `backfill run` and review estimated bytes and
URLs. Imports use concurrency one, `.partial` files, official checksums and
atomic revision commits. Funding-rate archives are planned monthly because that
is the verified official layout; other partial months use daily files.
Normalization streams fixed-size Arrow batches instead of retaining an entire
CSV in memory. `backfill verify` rereads immutable ZIP hashes and verifies
Parquet readability and lineage metadata. A 404 is a recorded gap, not empty
data.

Each limited-retention USD-M 5-minute dataset has an independent durable
Cursor. Restart catches up from the next unpersisted period in bounded pages.
`EMPTY_RESPONSE` and request/fsync failures keep the Cursor stationary. Monitor
`consecutive_failures` and explicit unrecoverable-gap events; no process can
recover a period after Binance removes it from the public retention window.

## Status and reports

All commands return structured JSON:

```bash
binance-market-recorder doctor
binance-market-recorder status
binance-market-recorder report daily --date YYYY-MM-DD
binance-market-recorder storage forecast
binance-market-recorder archive status
```

`status` is evidence-based. A missing, dead, future-dated, or stale heartbeat
cannot produce `RUNNING`.

## LaunchAgent lifecycle

```bash
binance-market-recorder launchd install \
  --label "$AUTHOR_CONTROLLED_LABEL" \
  --author-controls-namespace
binance-market-recorder launchd start
binance-market-recorder launchd status
binance-market-recorder launchd stop
binance-market-recorder launchd uninstall
```

The service runs only in the logged-in user session and needs no root access.
SIGTERM drains queues and seals active Raw. launchd restarts unsuccessful
exits. The single-process lock prevents two service owners from sharing one
data root.

Uninstall never removes market data or the Catalog. Confirm the configured
data root separately before removing any code environment.

## systemd lifecycle

Use an explicit config on every management command:

```bash
sudo binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd install --user orangepi --group orangepi
sudo binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd start
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd status
```

Stop/restart/uninstall are idempotent. SIGTERM drains/seals; uninstall retains
configuration and all data. Logs are in journald.

Startup publishes `STARTING` after the Catalog opens and keeps one periodic
heartbeat active through recovery and the later `RUNNING` phase. Collectors are
still constructed only after recovery completes. Recovery performs full
payload validation for crash-unstable lifecycle states before advancing them,
but a retained manifest whose exact immutable identity already matches an
ordinary local `SEALED` Catalog row uses metadata reconciliation instead of
rehashing and decompressing all historical Raw on every restart. SIGTERM during
startup is cooperatively observed between recovery units; the current unit
finishes atomically, recovery remains incomplete, and shutdown finalizes as
`STOPPED` without starting collectors.

This startup-liveness correction does not close M22.9 acceptance. Merge must be
followed by a new exact artifact build and controlled deployment before any new
acceptance window can begin; historical failed-deployment evidence remains
historical.

## External storage

Only an explicit project subdirectory may be registered:

```bash
binance-market-recorder storage list
binance-market-recorder storage inspect /Volumes/Disk/Chosen/Recorder
binance-market-recorder storage register /Volumes/Disk/Chosen/Recorder
binance-market-recorder storage status
binance-market-recorder archive retry
binance-market-recorder archive verify <storage-id>
```

To remove a disk, stop new allocation and request non-forced macOS
unmount/eject:

```bash
binance-market-recorder storage eject <storage-id>
```

Do not unplug until the command confirms both system operations. `BUSY`,
refusal, timeout, or disappearance is not safe-to-remove confirmation.
External absence does not stop internal capture.
Linux M20 performs no automatic eject; it reports manual action without
`SAFE_TO_REMOVE`.

The future archive workflow is local-client pull over SSH. The local client
must verify durability, readback, size, SHA-256, Raw manifest identity,
Archive Set/storage identity, and a durable receipt before the VPS can authorize
deletion. A transport success, file name, or size match alone is never enough.
After each successful session the VPS creates a consistent SQLite-supported
post-session Catalog snapshot; the local client retains at least `latest` and
`previous`. See [`archive_transfer_contract.md`](archive_transfer_contract.md).

## Sleep and resource operation

Laptop sleep is a known gap source. `prevent_sleep=true` uses a service-scoped
`caffeinate` assertion and never changes permanent power settings. It does not
promise closed-lid capture. Review daily reconnect, sequence-gap, resync,
oldest-unarchived, queue, file-handle, memory, and disk/backlog metrics.

For the explicitly selected future VPS profile `vps-production-v1`, capacity
states are WARNING at 18 GiB (or ETA <= 7
days), CRITICAL at 14 GiB (or ETA <= 72 hours), EMERGENCY at 12 GiB (or ETA <=
24 hours), and HARD RESERVE at 10 GiB. All ETA calculations target only the
10 GiB reserve. The VPS preserves approximately 10 GiB for the OS and
co-resident services. Never delete unarchived Raw. M22.7B selects the profile
only through the literal `capacity_profile = "vps-production-v1"` in the
explicitly loaded TOML file. There is intentionally no CLI or environment
profile selector, and the certified VPS rejects Recorder operational
environment overrides. Omission retains existing local M11 percentage
behavior. At actual free space <=10 GiB, the VPS service drains/seals, records
stop and gap evidence, and exits zero so `Restart=on-failure` does not loop.
Free space later released by a co-resident process is visible to the next
observation but never restarts Recorder; an operator must explicitly start it
after verifying free space is above 10 GiB.

Current production forensic evidence places the stopped host in EMERGENCY:
free bytes `17,091,108,864`, approximately 6.35 GB above the 10 GiB reserve,
and observed net growth of roughly 145–147 kB/s, implying about 12.02 hours to
the reserve if capture resumes. These are observations/planning estimates, not
threshold or policy changes. The full 278-hour staged chain needs about
140.63 GB additional usable capacity to finish just above reserve, or about
149.22 GB to finish above the 18 GiB NORMAL threshold; +50 GB and +100 GB are
insufficient, +150 GB is a near mathematical minimum, and approximately
+200 GB usable is the preferred planning recommendation. See the consolidated
state document for derivation and deletion authority.

Exact VPS static verification and the 300-second recovery-first readiness gate
are exposed as:

```bash
sudo /opt/binance-market-data-recorder/venv/bin/python \
  -m binance_market_data_recorder \
  --config /etc/binance-market-data-recorder/recorder.toml \
  deployment verify
sudo /opt/binance-market-data-recorder/venv/bin/python \
  -m binance_market_data_recorder \
  --config /etc/binance-market-data-recorder/recorder.toml \
  deployment readiness
```

`systemctl is-active` and process existence are not readiness. See the stopped
deploy, upgrade, and rollback procedures in `vps_operations.md`.

## Recovery

- A killed Collector is restarted by launchd; startup scans `.partial` files
  and truncates only to the last complete frame.
- A gap makes the local book unreliable until a public snapshot resynchronizes
  it; the incomplete interval remains visible.
- Archive copy/checksum/Catalog failures are retryable and never authorize
  deletion of an unverified source.
- A reinserted registered disk is resolved by volume UUID and marker, not by a
  fixed `/Volumes/<name>` assumption.
- **Legacy reconnect classification (M21.4.11-R3.3).** UTC wall-clock
  timestamps never prove causal order, so legacy reconnect decisions use
  an exhaustive three-way partition: PROVEN_LEGITIMATE (materialize
  REQ-103), PROVEN_EXTENSION (ignore lifecycle creation), and AMBIGUOUS
  (fail closed). No UTC condition gates any of it. "No parent found" is
  NEVER positive legitimacy proof for unversioned pre-R3 intents; only
  sound positive proof (trustworthy `verified_frames > 0` or the exact
  completing-connection proof) may do that. Intents generated by the
  R3.3+ runtime carry `intent_schema: reconnect-seal-intent.v2` inside
  the immutable SEALING evidence; a versioned fresh intent safely
  materializes REQ-103 automatically. The read-only command

  ```sh
  binance-market-recorder recovery legacy-reconnect-preflight
  ```

  inventories every historical SEALING reconnect intent against the same
  decision engine startup uses and emits deterministic counts plus
  `first_corrected_startup_eligible`. The command is intrinsically
  read-only: it never creates storage directories, and a missing data
  root or Catalog is an error, never a repair. Exit status is a gate:
  `0` = eligible; `2` = ineligible (full JSON report still printed) or
  runtime error; automation must not ignore the exit code. Only
  AMBIGUOUS candidates are resolved by the operator-reviewed additive
  file `legacy_reconnect_classifications.json` (schema
  `legacy-reconnect-classification.v3`, located next to the loaded
  config file — see the authority location rule below): entries bind
  the exact
  persisted record (`gap_id`/`market`/`stream` + `chunk_id` +
  `classification_evidence_sha256`, the SHA-256 of the canonical JSON of
  `{"chunk_id", "seal_intent", "verified_frames"}`) with classification
  `extension_orphan` or `legitimate_req103`. Changing any immutable
  candidate-side decision fact (chunk, intent identity, intent schema,
  verified_frames) invalidates the binding. The authority is consulted
  ONLY for AMBIGUOUS candidates; an entry contradicting durable proofs
  fails closed (`RECOVERY_LEGACY_AUTHORITY_CONTRADICTION`), and stale,
  duplicate, unmatched, or v1/v2-schema authorities fail closed.
  Malformed lifecycle authority (unkeyable STARTED/COMPLETED rows or
  identity-degraded CLOSED pairs) is surfaced as explicit
  `degraded_authority` blockers and makes the report ineligible; it is
  never silently skipped. The recorder never writes or edits the file.

  **Authority location (M21.4.11-R3.4 trust boundary).** The authority
  file resolves NEXT TO the loaded Recorder configuration file
  (`config_file.parent / legacy_reconnect_classifications.json`), never
  inside the service-writable data root: file mode 0640 denies content
  writes but the service principal owns the data-root directory and
  could otherwise unlink/rename/replace the authority pathname. Ubuntu
  system service: `/etc/binance-market-data-recorder/
  legacy_reconnect_classifications.json` (parent root:orangepi 0750,
  the same directory that holds `recorder.toml`). Preflight and startup
  use this exact same rule. Only a config-less interactive/test
  operation falls back to the data root, whose owner is the same
  interactive principal in that mode.

  **Authority installation contract.** Install the authority atomically
  with the exact documented owner/group/mode. Ubuntu system service
  (production service `User=orangepi Group=orangepi`): parent directory
  owner `root`, group `orangepi`, mode `0750` (root replaces the
  pathname; the service group can only traverse — no directory write,
  so the service can never unlink/rename/replace the authority); file
  owner `root`, group `orangepi`, mode `0640` — root/operator writes,
  the service group can read, everyone else cannot. macOS interactive
  (service and CLI share the interactive account): authority next to
  the interactive config file, owner `user`, group `staff`, mode
  `0600`. Write a temporary file in the SAME directory/filesystem
  (`legacy_reconnect_classifications.json.partial`) with the FINAL
  owner/group/mode, fsync it, `mv`/rename it over the final path, then
  fsync the parent directory. Never leave a post-rename window with
  unsafe or unreadable permissions. Startup reads the final path only,
  so a partial JSON can never be observed. The recorder never creates
  or edits the file. The Ubuntu pre-start sequence is mandatory for the
  first corrected restart: see `ubuntu_rk3588_operations.md`.

See [data and storage](data_and_storage.md) for artifact guarantees and
[known limitations](known_limitations.md) before operating the preview.

Future notifications may report archive pressure, degradation, stop, integrity,
and transfer outcomes, but notification failure is never Recorder data
authority. A future Web UI is separately authorized and must keep View, Health,
and Control concerns separable from Raw and Recorder integrity decisions.

## M21.4 production deployment experience

M21.4 deployment established several operational practices that supplement
`ubuntu_rk3588_operations.md`:

### Wheel identity verification

The production artifact identity is determined by immutable Wheel SHA-256,
`direct_url.json` matching, non-editable install confirmation, and static
file verification inside the installed `dist-info`. The CLI `--version`
output contains a Git suffix that may be affected by the runtime working
directory; it is a display convenience, not the authoritative identity.
Always run production CLI checks from `/tmp` to avoid CWD contamination.

### Deployed RECORD SHA

The SHA-256 of the entire installed `RECORD` file is environment-specific
and cannot serve as a cross-machine fixed identity gate. It is preserved as
installation evidence only.

### Stop/seal/offline Wheel install

The production venv was updated with the service stopped and all active
Raw sealed. The new Wheel was installed with pip, the systemd unit refreshed,
and the service restarted. Both markets were confirmed READY with orderbooks
synchronized before the deployment was considered complete.

### Rollback Wheel

The prior Wheel was preserved. Rollback follows the same stop/seal/offline
install sequence but reinstalls the saved prior artifact.

### Canonical Installed Identity Gate

After installation, a static audit verified: Wheel file SHA, direct_url.json,
non-editable state, module_file path, dist-info path, RECORD file hashes,
and production Python/CLI resolution. This gate confirms the deployed artifact
is the intended build before any production validation window starts.

### Production code revision vs documentation revision

The production code commit (`cf1e749c...` for M21.4) and the repository
documentation revision are tracked separately. Documentation-only commits
merged to `main` after the production deployment do not change the production
Wheel, collector version, or running code. Do not conflate a documentation
merge commit with a production code change.

### M22.8 remote failure acceptance boundary

The fixed M22.8 run `m22.8-20260822T041913Z-23f1fcc7` was accepted at exact
main `f699b6dc0e3e9e2d193eb9fd25321c59526995cd` over the real MacBook ->
OpenSSH -> Germany VPS boundary. All nine isolated scenarios passed, including
same-receipt delete-response-loss reconciliation, snapshot-only retry, local
receiver storage failure, and remote source read failure. The accepted result
proves the remote archive lifecycle and failure handling in a disposable test
workspace; it is not a production deployment or long-run qualification.

The final audit retained safe test Raw with authority `ABSENT` for M8-02,
M8-03, and M8-08, and retained the run workspace and evidence for forensics.
Cleanup must not authorize or delete these objects merely for cosmetic
cleanup. M22.9 remained a separate exact-VPS staged acceptance. It later began,
but the 24h result is INCOMPLETE after startup readiness failed; 72h is not
eligible, and any artifact built from the local startup-liveness correction has
runtime credit zero.

### 2h/12h/24h/72h/168h T0 independence

Each validation window has its own T0, Target, and evidence root. A prior
window's PASS does not automatically start the next window. Each window
must be explicitly created with its own T0 anchor and continuous observation.
The 24h window passed on process stability with corrective and contract
forensic confirmation; **the formal 72h window FAILED on data integrity
(reconnect boundaries seal without gap evidence; see
`docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md`)**.
The 168h window remains pending and is never started automatically. A new
artifact must restart the full chain (2h→12h→24h→72h→168h) after review and
deployment.

### Formal evidence collection rules (24h corrective lessons, mandatory for 72h/168h)

The repository-owned installed M22.9 observer is the authority for future
production acceptance records. It writes only immutable canonical
`m22.9-acceptance-evidence.v1` files under an explicit operator-owned evidence
root, uses the existing deployment/readiness/Catalog/Raw/manifest authorities,
and records both UTC nanoseconds and Linux CLOCK_BOOTTIME nanoseconds with the
boot ID. Baseline manifest membership is frozen before T0; BOOTTIME is the
elapsed-duration authority; and each stage requires exact, transitively
verified predecessor lineage. One shared reconnect engine supplies reconnect
analysis, and expensive Raw audit work is incremental. It is not a service
controller or automatic stage runner: production observation is read-only and
no stage advances automatically. Historical M21/M22 evidence was collected
before this observer and remains unchanged.

The 24h corrective review and Backpressure contract forensic review
established binding rules for every future formal window:

1. **Journal boundaries derive from the formal T0/Target UTC nanoseconds, but
   journald filtering is not nanosecond-exact.** `journalctl`/systemd time
   parsing and the journal `__REALTIME_TIMESTAMP` field have **microsecond**
   resolution, not nanosecond. Derive explicit UTC timestamps from the
   nanosecond T0/Target and pass them as:
   - explicit RFC3339 UTC timestamps with microsecond precision, e.g.
     `2026-08-05T15:09:30.200566Z`
     (`journalctl --since "2026-08-05 15:09:30.200566Z"`), or
   - the `@<Unix-seconds>` syntax that the project has verified read-only on
     the actual target systemd version; do not assume fractional `@` forms
     are accepted without that read-only test.
   Never use bare local-time strings such as `--since "2026-08-05 15:09:30"`
   without a timezone suffix; journalctl interprets them in local time and
   the formal boundary shifts (the 24h export shifted 8 hours). Do not claim
   journal filtering itself is nanosecond-exact.
2. **Forbidden**: timezone-ambiguous since/until strings in any formal
   export.
3. **Export structured formats and verify bounds independently**: export with
   `--output=json` / `--output=json-seq` / `--output=export` so every record
   carries `__REALTIME_TIMESTAMP` (UTC microseconds); after each export,
   verify the first and last records' `__REALTIME_TIMESTAMP` against the
   derived UTC microsecond bounds (T0/Target nanoseconds truncated to
   microseconds). Boundary enforcement happens at journald's documented
   microsecond resolution.
4. **Boundary ambiguity**: if a record's timestamp falls in the boundary
   microsecond and its inside/outside classification cannot be decided at
   microsecond resolution, record `BOUNDARY_PRECISION_AMBIGUOUS` instead of
   guessing. Soak samples and observations carry their own nanosecond fields
   and remain strictly filtered by the nanosecond T0/Target.
5. **Formal samples/observations** must be strictly filtered by the T0/Target
   nanosecond bounds; post-Target data must never be mixed into formal
   statistics (it may be reported separately as post-window current state).
6. **Catalog events must be queried from Catalog**: `STREAM_DISCONTINUITY_*`
   and other operational events are written only to Catalog, never to the
   journal. Journal string counts are invalid evidence for them.
7. **Raw gaps must be read from Raw** (EventEnvelope `capture_flags`) and
   **manifest status from manifests** (`gap`/`complete`). Each layer has its
   own evidence responsibility.
8. **Save both raw text and parsed JSON** for every observation round; a
   serialization failure must not discard the raw command output
   (12h timer-parsing lesson).
9. **Never overwrite original evidence**: corrective reviews and forensic
   reviews must create their own independent directories under the run root
   and leave all original files byte-identical.
10. **Distinguish recovery events precisely**: `queue_backpressure_recovered`
    (queue below low_watermark) is not stream recovery completion; the
    completion boundary is new connection + first-new `sequence_gap` persisted
    + Raw sync + Catalog COMPLETED.
11. **Saturation semantics**: the 30 s backpressure budget is accumulated
    saturation time above low_watermark, and the timeout raises only when a
    later put re-encounters a full queue. A long started→timeout span is not
    a continuous full-queue span; record it as accumulated saturation.
