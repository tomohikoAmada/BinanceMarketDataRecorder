# Current Production State

Verified October 2, 2026: all-enabled stopped redeploy and exact deployment
verification; engineering baseline and completed verification passed, and
all-enabled NONFORMAL warm-up is running. This is the current
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

The owner resumes Q3 with all fourteen auxiliary flags enabled, then eligible
Formal 2h and 12h, stopping before 24h. The exact independently accepted
replacement c87d580 is installed and VERIFIED with all14 flags true. Its
local1934-test suite, dual-platform CI36969020155, clean locked wheel,
cloud252-test changed-path suite and21-test public smoke PASS. Actual all-enabled
warm-up, forecasts and authoritative post-warm-up baseline remain pending.
See [candidate record](milestone_acceptance/Q3-all-enabled-candidate.md).

## Installed artifact and data scope

| Item | Value |
|---|---|
| Host | `greencloud-tokyo-01`, Ubuntu 24.04 x86_64 |
| Kernel / Python | `6.8.0-146-generic` / 3.12.3 |
| Boot ID | `11b990f3-68bb-43fa-ac82-03e94ea6962a` |
| Deployed source | `c87d58072e309b13dd9979d8fbff1b33b041fe0c` |
| Source tree | `d610184647da8ba0ddea2bec623a787564597323` |
| Wheel SHA-256 | `6fa0491363a146f4d4b1af59f65b81c0724be77390721964a942aa47dd32342a` |
| Linux lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Deployment identity | `ffe30115f438ab0dc6c82af15c6c95070251a175a37ec1d6e71890d5c404e15c` |
| Environment | `/opt/binance-market-data-recorder/venv` |
| Configuration | `/etc/binance-market-data-recorder/recorder.toml` |
| Writer root | `/var/lib/binance-market-data-recorder` |
| New archive root | `/srv/recorder-data/recorder-archive-qualification-q3-d0f455c-lg427ebp` |
| New storage ID | `57512e1d-59cc-4aff-bf3b-49ae84a7a44e` |

Config SHA-256 is `5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419`.
Spot BTCUSDT/ETHUSDT and USD-M BTCUSDT/ETHUSDT are unchanged. All fourteen
auxiliary flags are true; twelve poll intervals and900s grace are explicit.
42 Raw contexts/16WS/26REST is the configured topology, not measured live coverage.
The canonical venv contains the noneditable frozen c87d580 wheel and
28/28 locked runtime distributions. Documentation descendants of its source
are not installed wheels.

## Current nonformal engineering preparation

- Recorder: active/running and enabled solely for NONFORMAL warm-up,
  MainPID16736, InvocationID `39d89a726ce041d98310fddf95801186`, NRestarts0.
  Exact installed identity/dependencies/effective unit VERIFIED before start.
- At stopped deployment: active partials0 and Catalog `ok`. New warm-up capture
  now grows this corpus; those stopped counts are not a live claim.
- Pre-start retained corpus:448 chunks/transactions, all `LOCAL_DELETED`,
  backlog/pending/failed0. These rows and all new warm-up capture are retained,
  with zero Formal credit.
- Archive timer: enabled/active. The existing verified worker was primed once;
  a later automatic invocation `c2a17786df044a76a007ea6daa70172a` succeeded
  with a finite next deadline before warm-up.
- Both apt timers and unattended upgrades: enabled/active. Normal update
  authority is restored; no quiet-window runtime masks are applied to the
  current NONFORMAL warm-up. Repeat fresh maintenance gates before a future baseline/T0.
- Lightweight all-enabled diagnostic resource sampler: RUNNING as bmdr,
  bounded RuntimeMaxSec10800. Engineering full baseline over448 archived chunks
  is COMPLETE/PASS_CANDIDATE with completed verification, no blocking findings.
  Both LIVE passes took approximately530s together, control verification10s.
  All26 auxiliary owners passed after cold catch-up. NONFORMAL observer
  `7c27a0c18406435e8d2206d208562a5a` runs with RuntimeMaxSec7200, Restartno,
  SIGINT and TimeoutStop120s. Its measured normal/missed/recovery gate is pending.
  Failed nonformal observer units and
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
NEXT=Q3_ALL_ENABLED_MEASURED_WARMUP_THEN_AUTHORITATIVE_BASELINE
```

Current additive evidence: `/srv/recorder-data/recorder-archive/evidence/Q3-all-enabled-20261002-xyeeHH0o`.
Frozen release SHA-256 `2bd68c4d394f76c66b039fec8c19c3643fce61a55e25e018b2ad8f7b01555353`.
Root-controlled release: `/opt/binance-market-data-recorder/release-c87d580-O0wAuYCt`.
Replaced d0f455c venv retained at `/opt/binance-market-data-recorder/venv-custody-q3-all-enabled-20261002-56d4ede49e54`.
No Raw/Catalog reset, physical deletion or new Formal T0 occurred.
