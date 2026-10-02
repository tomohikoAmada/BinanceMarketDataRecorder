# Current Production State

Verified October 2, 2026: controlled Q3 closeout snapshot at 02:37:20 UTC,
followed by exact stopped deployment verification. This is the current
operational authority; [history](CURRENT_PRODUCTION_STATE_HISTORY.md) preserves
the previous October 1/2 installed-artifact checkpoint.

## Current qualification

[Q2](milestone_acceptance/Q2-reviewed-release-and-cloud-design.md) is complete.
PR #79 is merged at `8c9310554a559369c8708a30e6a79fdcb379d8e2`; local and remote
branches contain only main and no PR is open. Historical detached worktrees,
the original stash and untracked review bundles remain preserved.

[Q3](milestone_acceptance/Q3-fresh-corpus-and-cloud-warmup.md) completed old-data
custody, fresh Catalog/archive registration, exact deployment and initial
four-product readiness. Its **NONFORMAL** warm-up failed the online
delta-capacity gate with `pending causal reference cap exceeded`. The
authoritative post-warm-up baseline has not run. Q3 is PARTIAL_NOT_ACCEPTED;
no Formal V5 T0, target or final exists. Do not repeat the unchanged live run
or start 2h/12h until optimization/review and cloud gates pass.

The latest owner instruction resumes Q3 with all fourteen auxiliary capture flags
enabled in the replacement profile, then eligible Formal 2h and 12h, stopping
before Formal 24h. Local fixes and review are in progress; installed config still
has auxiliaries disabled. This instruction has not changed installed artifacts
or created duration credit.

## Installed artifact and data scope

| Item | Value |
|---|---|
| Host | `greencloud-tokyo-01`, Ubuntu 24.04 x86_64 |
| Kernel / Python | `6.8.0-146-generic` / 3.12.3 |
| Boot ID | `11b990f3-68bb-43fa-ac82-03e94ea6962a` |
| Deployed source | `d0f455c1a417cc1a184c47b6ff766a60f3dc0159` |
| Source tree | `cf3854d7fc032de79096dced8e002a6cd55e65c3` |
| Wheel SHA-256 | `b286923d3dc777bf2e3d63ea661effd7cf389137aaf81883519c3a071d449921` |
| Linux lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Deployment identity | `6ab685ed3bc51c9787a52e461607aafe99c35312d2f6e3a489ac1ae4ab79f40e` |
| Environment | `/opt/binance-market-data-recorder/venv` |
| Configuration | `/etc/binance-market-data-recorder/recorder.toml` |
| Writer root | `/var/lib/binance-market-data-recorder` |
| New archive root | `/srv/recorder-data/recorder-archive-qualification-q3-d0f455c-lg427ebp` |
| New storage ID | `57512e1d-59cc-4aff-bf3b-49ae84a7a44e` |

The unchanged config selects Spot BTCUSDT/ETHUSDT and USD-M BTCUSDT/ETHUSDT,
twelve core WebSocket contexts and public depth snapshots. All auxiliary kinds
are disabled. The canonical venv contains the noneditable frozen Q2 wheel and
28/28 locked runtime distributions. Documentation descendants of its source
are not installed wheels.

## Verified stopped handoff

- Recorder: inactive/dead **and disabled**, MainPID 0, Result success,
  NRestarts 0; exact installed identity/dependencies/effective unit VERIFIED.
- Active partials: 0. Catalog integrity: `ok`.
- New corpus: 448 chunks/transactions, all `LOCAL_DELETED`; archive backlog,
  pending and failed counts 0. Captured data is retained, zero Formal credit.
- Archive timer: enabled/active with a finite future deadline; worker inactive
  after verified drain. Actual automatic recurrence was checked during warm-up.
- Both apt timers and unattended upgrades: enabled/active. Normal update
  authority is restored; no quiet-window runtime masks were applied to this
  failed warm-up. Repeat fresh maintenance gates before a future baseline/T0.
- Diagnostic resource sampler: stopped. Failed nonformal observer units and
  all original logs/documents remain available. Historical automation remains
  paused; no automatic continuation was created.

## Retained custody and evidence

Old writer metadata/Catalog is at
`/var/lib/binance-market-data-recorder-custody-q3-20261002-ccf48ee63f5d`;
full metadata hash/owner/mode readback passed. Old archive/evidence remains at
`/srv/recorder-data/recorder-archive`, with its original storage ID. The
interrupted 143,362-chunk old baseline remains unpublished and R-078 open.
There was no physical data deletion or old-row import into the new Catalog.

Old runtime is inert custody at
`/opt/binance-market-data-recorder/venv-custody-q3-20261002-6ec8c7bda454`;
the retained V4 rollback material also remains. Any rollback requires canonical
reinstallation and compatibility review with new capture preserved.

Q3 evidence root:
`/srv/recorder-data/recorder-archive/evidence/Q3-fresh-d0f455c-20261002-YgstkmRP`.
Attempt-2 closeout SHA-256:
`b6bb1fac567403044f7c38f74c565fded374efb139fa11a1f8ff389796f56eb9`.
See [Q3 record](milestone_acceptance/Q3-fresh-corpus-and-cloud-warmup.md) and
[compact evidence](reviews/2026-10-02-q3-warmup-closeout.json).

```text
Q3=PARTIAL_NOT_ACCEPTED
FORMAL_V5_STARTED=NO
FORMAL_V5_CREDIT_SECONDS=0
12H_STARTED=NO
PRODUCTION_READY=NO
NEXT=Q3_OBSERVER_BYTE_AND_CAUSAL_CAP_OPTIMIZATION_REVIEW
```
