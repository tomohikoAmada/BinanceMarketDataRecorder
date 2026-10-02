# Q2 cloud execution procedure — through eligible 2h only

Prepared October 2, 2026 for the owner's instruction to continue until the next
step is cloud 12h. This procedure is not a completed Q2/Q3/Q4 record. Execute
sequentially after independent release review; stop on a failed prerequisite.
The current [plan](../milestone_plan.md) and
[V5 contract](../acceptance_evidence_v5.md) remain authoritative.

## Exact starting facts

Read-only SSH inspection confirmed:

- Host `greencloud-tokyo-01`, Ubuntu 24.04 x86_64, Python 3.12.3, boot ID
  `b0c05e42-299e-414a-b7c3-b8863711d680`.
- Recorder `binance-market-data-recorder.service` is inactive, disabled,
  MainPID 0; actual service user/group is `bmdr:bmdr`.
- Installed source is `89bc6b41c0cb7d270ca6e4d2dec9a375137c028e`.
- Writer is `/var/lib/binance-market-data-recorder`; all 143,362 Catalog
  chunks and archive transactions are `LOCAL_DELETED`; active partials are 0.
- Old registered archive is `/srv/recorder-data/recorder-archive`, storage ID
  `ef852751-721c-4145-9083-f6fd48718480`, ext4 volume UUID
  `95A2CE20-BF7A-4FAE-98E8-D208517AE318`, marker nonce
  `d6b44820f5f24991b57797ca068967aa`.
- Correct archive units are `binance-market-data-archive.timer` and
  `binance-market-data-archive.service`. Timer is enabled/active; worker is
  inactive. Its command uses the old storage ID, 50s runtime and 1000-file cap.
- Root free bytes: 36,410,413,056; archive free bytes: 2,102,856,585,216.
  These snapshots do not pass live capacity or growth gates.
- Config remains four ProductKeys, twelve core streams, REST snapshots,
  60s/128 MiB rotation, 1s durability, queue 8192, direct network policy.
  **Every auxiliary enable flag is false.** This is the existing selected
  workload, not an auxiliary-completeness claim or a new tuning change.
- dpkg audit is clean; no current reboot-required marker. Eight upgrades
  are pending, including kernel packages. Maintenance must be rechecked and
  settled before any identity-sensitive baseline or Formal T0.

Q2 tests use `/var/tmp/binance-recorder-q2-9tb0agg2`, a private 0700 development
directory with a separate Python environment. Synthetic/test data stays there;
installed runtime, real Catalog, config and services are unchanged. Do not
promote this development environment or relocate it into the live path.

## Q3 custody and new corpus

1. Recheck inactive **and** disabled Recorder, Catalog integrity, zero partials,
   zero unarchived/pending/failed work and ready old archive. Retain the incomplete
   October 1 baseline unchanged. Drain through the existing installed command if
   necessary; inspect its result, not just exit status.
2. Stop only the project archive timer/worker. Recheck no active mutator. Create a
   new private evidence child beneath the existing project evidence parent using
   remote `mktemp -d`; retain it. Preserve config, identity and both managed unit
   pairs. Record exact hashes, owners/modes, volume registration and path binding.
3. Inventory and stream-hash the old writer's regular metadata files. Reject
   symlinks, unexpected mounted children and any undisposed Raw/partial. Retain
   inventory as JSONL; hash/read back it and the preserved metadata. Do not scan
   all old archived Raw merely to retain custody, or call its unfinished audit
   PASS. Retain the entire old archive/evidence root and its marker unchanged.
4. With all writers stopped, move the exact project-owned writer directory to a
   uniquely named **sibling** under `/var/lib`, then recreate the canonical
   writer with `bmdr:bmdr`, mode 0750. Print and validate source/destination,
   require same filesystem, non-mount/non-symlink source and absent destination.
   Read back the metadata inventory from custody. No recursive deletion.
5. Create a uniquely named archive **sibling** of `recorder-archive` under the
   already-mounted `/srv/recorder-data`, mode 0750, owned by `bmdr:bmdr`. It must
   be outside the old registered relative scope. Initialize the new Catalog and
   register that directory through existing `storage register`; retain actual
   UUID, relative path, marker and storage ID. Do not copy old Catalog rows.

Rollback never overlays the old Catalog onto newly captured data. Stop/disable,
drain/retain the new corpus, pause the archive worker, preserve its root/config/
registration/evidence, then restore the exact old writer/config/unit binding.
Never delete either sole archive copy. Physical cleanup is unnecessary for this
switch; retained custody satisfies recoverability without reclaiming uncertain
objects.

## Q3 exact deployment and NONFORMAL warm-up

1. Freeze the independently reviewed source/tree, **one** wheel, hashed Linux
   runtime lock and current interpreter. Retain the same wheel/lock under a new
   root-controlled release child of `/opt/binance-market-data-recorder`.
2. Pause archive mutation while replacing the runtime. Preserve the old venv as
   an inert rollback snapshot. Create/install the new venv at the **canonical**
   `/opt/binance-market-data-recorder/venv` path using `python3.12 -m venv --copies`,
   hashed-lock install, then `pip install --no-deps <exact-wheel>` and `pip check`.
   Do not run a relocated venv as if its interpreter/shebang identity matched.
3. Keep the existing selected workload config bytes unless a reviewed change is
   needed. Install the managed Recorder unit and immediately recheck its disabled
   stopped handoff (install enables it). Rebind the managed archive timer with
   `archive timer install --user bmdr --group bmdr --storage-id <actual-new-id>`;
   retain both unit hashes/effective settings. No manual alternative archive path.
4. `deployment identity-create` binds the exact source, retained wheel/lock,
   unchanged or explicitly reviewed config, unit/profile and actual Python.
   `deployment verify` must pass. Keep a stopped unit loaded with the already
   reviewed DBus reference-holder procedure when required; do not enable
   auto-start just to avoid unit garbage collection.
5. Explicitly enable/start for warm-up; all four products must reach READY.
   Run archive normally. After a long stopped/rebind interval, verify the timer
   has a finite future deadline and actually fires automatically; if necessary,
   explicitly prime the existing verified archive service once, then recheck
   recurrence. An active timer alone is insufficient evidence. Warm-up and diagnostic observation evidence live in a
   distinct, explicitly labelled **NONFORMAL** root and earn zero credit. Reuse
   the real V5 observer/qualification path; do not substitute a six-chunk timing
   or inject fake ready state. If an engineering baseline/start is used solely
   to exercise that path, it is separate from the authoritative post-warm-up
   baseline and is never an eligible Formal predecessor.
6. Exercise complete observations at normal cadence and one skipped cadence,
   keeping observed start-to-start gaps safely below 600s. Verify finite pending
   deltas drain while capture/archive continue, all actual document byte counts,
   complete observation cost below 240s with headroom, real sealing/size-rotation
   and any natural reconnect bursts. Do not inject destructive faults or change
   selected workload merely to make the gate pass.
7. Sample Recorder and observer/children separately and combined: process CPU,
   RSS, cgroup peak (label cache separately), `/proc/stat` steal/iowait,
   `/proc/pressure/*`, swap and aggregate host load. Keep bounded JSONL samples;
   never materialize multiple large historical observer documents in one command.
   Record core events/bytes, enabled auxiliary count 0, backlog and root/archive
   free space with exact observation durations. Unrelated services stay untouched.

## Forecast and maintenance decision before Formal T0

Use measured, per-stream growth and actual observed peaks. For cumulative target
hours `H = 2, 14, 38`, forecast Raw stored/uncompressed bytes, frames, manifests,
Catalog/ledger rows, logs and evidence as warm-up + failed/interstage capture +
`H * measured hourly growth`, retaining observed peak-load headroom explicitly.
Estimate full audit wall time from **both** producer and independent LIVE passes,
plus measured freeze/SQL/shard/control replay/publication. CLI completed `verify`
is historical control/proof replay, not a third LIVE Raw scan. Report estimates
as forecasts, not measured 38h results or a fixed speedup guarantee.

Compare root free space against 18/14/12/10 GiB policy, reserve ETA, measured
maximum unarchived backlog and log/evidence growth; archive capacity is separate.
Review practical 2h/14h/38h wall-time estimates before T0. A failing performance
gate returns to measured code optimization/review; no automatic language rewrite
or server upgrade.

Finish pending packages and any required reboot with Recorder inactive/disabled
and no observer. Recheck package locks, dpkg audit, reboot marker, boot ID,
interpreter/dependency hashes, archive readiness and capacity. A changed frozen
identity returns to release review. The runtime-only five-unit maintenance mask
must cover authoritative baseline, timed 2h, quiescence, terminal audit and final
publication within a **finite** schedule derived from those measurements. Record
the end time and exact restore command before applying it; prove masked activation
fails. Use the existing procedure in [VPS operations](../vps_operations.md).

## Authoritative baseline, Q4 and stop point

The [actual Q3 warm-up](../milestone_acceptance/Q3-fresh-corpus-and-cloud-warmup.md)
failed its byte/causal-cap gate. The following steps remain unexecuted; first
review/refreeze an actual correction and repeat the cloud gate. Preserve the
new growing corpus and all failed-attempt evidence.

After warm-up, stop/disable Recorder, drain verified archive, pause timer/worker,
prove quiescence and freeze identity. Under the fresh maintenance gate:

```text
deployment acceptance baseline --evidence-root <new-authoritative-baseline>
deployment acceptance verify --baseline --evidence-root <same-baseline>
```

Both must pass with completed immutable evidence and no blocker. Restore the
archive timer, explicitly enable/load/start Recorder, verify exact identity and
fresh four-product READY; bind identity/readiness evidence. Start exactly one
operator-authorized stage:

```text
deployment acceptance stage --schema-version v5 --stage 2h
  --previous-evidence <authoritative-baseline/audit-root.json>
  --evidence-root <new-formal-parent>
```

Use an external systemd `Type=exec`, `Restart=no` observer, no restart coupling
to Recorder. Monitor compact actual stage evidence and process identity. At
target, stop time credit, stop/disable Recorder, drain archive, pause mutation,
prove frozen quiescence, then:

```text
deployment acceptance finalize --evidence-root <actual-2h-stage-root>
deployment acceptance verify --evidence-root <same-2h-stage-root>
```

Review exact final/root/control hashes, eligible predecessor lineage, unchanged
artifact/profile/corpus, target duration >=7200s, no blockers,
`result=PASS_CANDIDATE`, `eligible_for_next_stage=true`, completed verification,
core/gap/resource evidence and recorded limits. Only then record Q4 PASS.
Restore archive and normal update authorities, verify Recorder inactive/disabled,
Catalog ok, zero active partials and zero backlog. Keep all new data/evidence.
Publish milestone-pure acceptance records and handoff. **Stop here: next is Q5
cloud 12h; do not start it.** Broader `PRODUCTION_READY=NO` remains unchanged.
