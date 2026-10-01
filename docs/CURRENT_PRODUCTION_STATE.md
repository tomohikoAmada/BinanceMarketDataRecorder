# Current Production State

Verified October 1, 2026 following the owner's instruction to stop qualification.

## Source and installed artifact

| Item | Value |
|---|---|
| Code merge | PR #78 |
| Deployed source | `89bc6b41c0cb7d270ca6e4d2dec9a375137c028e` |
| Deployed source tree | `039f8fefaeefe116a3a464b7d2bffae8d05e7d9c` |
| Wheel SHA-256 | `ddc4d631cf773e16955d00b0843f8993c50a629881f497373e892340f55586a8` |
| Linux production lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Deployment identity | `ae58d4a2cdc7ca1407f897cae7abf0f7804912f729691ec599bfa2de5adb8b80` |
| Host | `greencloud-tokyo-01`, Ubuntu 24.04 x86_64 |
| Environment | `/opt/binance-market-data-recorder/venv` |
| Configuration | `/etc/binance-market-data-recorder/recorder.toml` |
| Identity file | `/etc/binance-market-data-recorder/deployment-identity.json` |
| Writer root | `/var/lib/binance-market-data-recorder` |
| Archive root | `/srv/recorder-data/recorder-archive` |
| Archive storage ID | `ef852751-721c-4145-9083-f6fd48718480` |

The documentation commit following this code merge is not deployed. Exact
installed identity is the table above, not the moving GitHub main reference.

## Owner-stopped qualification

The V5 baseline started at `2026-10-01T00:03:01Z` and was stopped by the owner
before publication. At the stop snapshot (`2026-10-01T14:20:34Z`,
22:20:34 Asia/Shanghai), its published prefix contained 87,423 audit records
in 344 shards. Freeze captured 143,362 manifests in 281 shards, corpus SHA-256
`7b6c95aff9a83135154102cfd6f4b68fe10ae50c8d56daf014ac2f73f1587666`.
There is no `audit-root.json`. This is an incomplete operator-stopped operation,
not a baseline PASS and not an integrity failure verdict.

No Recorder start, Formal T0, online sample, target, or final was created by this
V5 workflow. The monitor `v5-tokyo-qualification-continuation` is PAUSED.
No automatic continuation, resume, reset, or new run is scheduled.

## Verified closeout

- Recorder: inactive/dead, disabled, `MainPID=0`, `NRestarts=0`, `Result=success`.
- Baseline and its resource sampler: inactive/dead, `MainPID=0`.
- Active `.partial` files under `data/active`: 0.
- Catalog `PRAGMA integrity_check`: `ok`.
- Archive target: READY, 2,102,856,589,312 bytes free at closeout.
- Archive: 143,362 transactions in `LOCAL_DELETED`; backlog and remote pending 0.
- Archive timer: enabled, active/waiting; worker completed successfully.
- The five apt/unattended-upgrades runtime masks were removed.
- Both apt timers: enabled, active/waiting. Unattended upgrades: enabled, active/running.

The earlier six pending updates were installed before the baseline. Maintenance
is restored and may subsequently run normally; package/boot state must be freshly
checked before any future qualification.

```text
V5_IMPLEMENTED=YES
V5_DEPLOYED=YES
BASELINE=OWNER_INTERRUPTED_NOT_PUBLISHED
FORMAL_V5_STARTED=NO
FORMAL_V5_CREDIT_SECONDS=0
12H_STARTED=NO
PRODUCTION_READY=NO
NEXT=OWNER_DECISION_ON_QUALIFICATION_APPROACH
```

## Evidence and rollback

Operator evidence root:
`/srv/recorder-data/recorder-archive/evidence/M22.9-v5-89bc6b41-20261001-5MZRRCPc`.

Acceptance parent:
`/srv/recorder-data/recorder-archive/acceptance/m22.9/v5-89bc6b41-20261001-LmzlTmzl`.
The interrupted baseline is its `baseline/` child; identity evidence is under
`identity/`. Evidence and production data were retained.

Coherent V4 rollback material remains at
`/opt/binance-market-data-recorder/rollback-v4-354f5199-mLEv28OB`.
Its relocated environment is an inert snapshot; rollback requires rebuilding at
the canonical environment path from the retained wheel/lock, with Catalog
compatibility checks. No rollback was executed.

See the [owner-stop record](milestone_acceptance/M22.9-v5-deployment-owner-stop.md)
and [development handoff](PROJECT_HANDOFF.md).
[Older production snapshots](CURRENT_PRODUCTION_STATE_HISTORY.md) preserve
artifact-specific history and do not describe the current deployment.
