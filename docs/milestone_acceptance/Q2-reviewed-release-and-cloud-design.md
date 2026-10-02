# Q2 — Reviewed release candidate and cloud execution design

Date: October 2, 2026. **Implementation review ACCEPT; revised release gates in progress.**
Predecessor: [Q1 offline acceptance](Q1-correctness-and-bounded-diagnostics.md).
The owner authorized continuation through reviewed eligible cloud 2h, stopping
before 12h, and explicitly authorized a GPT-6.1 Sol / xhigh independent read-only
reviewer. No Q3 corpus switch, deployment, Recorder start or Formal T0 has occurred.

## Retained preliminary candidate — superseded by reviewed correction

| Authority | Value |
|---|---|
| Source | `9e08c1d1f6434d4e9687d12163f47924456e8c09` |
| Source tree | `ed839e92fb8663bf26d684d7e9ffc04269c38435` |
| Wheel | `binance_market_data_recorder-0.1.0a1-py3-none-any.whl` |
| Wheel SHA-256 | `de7c488576425c6cbdb910aaa71adca61f199316c53ebb66f30bc7cb2e0dcf6b` |
| Linux runtime lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Builder | Cloud x86_64 Python 3.12.3, setuptools 75.8.0, `build --no-isolation --wheel` |
| Existing exact-source CI | [36945763984](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/36945763984), both platforms PASS |

The preliminary wheel was built from a clean Git archive of that exact source,
separate from the modified Q2 test tree. A clean installed-wheel environment
matches all **28/28** locked runtime distributions, with only bootstrap pip;
Recorder is a separate noneditable wheel. `pip check`, CLI version, doctor and
status pass. Doctor retains its existing x86_64 developer-preview warning;
this is not production certification or a deployment-readiness verdict.

The wheel, original source archive, lock and test/build logs are readback-verified
in private root-controlled staging:
`/srv/recorder-data/recorder-archive/evidence/Q2-release-9e08c1d-20261002-4t0kqp5k`.
Staging record SHA-256:
`d6b5726a0cfc096cd2688fc6c94fad73ca6087a3c9b396489178630f517e230d`.
Its immutable `independent_review=PENDING` describes staging-time status. The
subsequent runtime correction supersedes it as a final release candidate; retain
it and never deploy it for Q3. Staging is not installation. See
[staged hashes](../reviews/2026-10-02-q2-staged-candidate.json).

## Independent review and corrected implementation

[Independent review](../reviews/2026-10-02-q2-independent-release-review.md)
accepts Q1, the performance candidate and the Q2 correction with **no open
P0/P1/P2/P3 findings**. Reviewer ran 125 focused tests, 27 affected tests, 128
complete Raw proof-differential cases and an actual STOPPED-publication failure
cleanup probe. One original P3 was corrected: an over-budget pending descriptor
slightly exceeded the literal 7 MiB delta limit; the hard 8 MiB total and
eligibility protections had remained effective.

Declared-policy production now omits over-budget entries without cursor advance;
frozen high-waters retain pending work, and independent replay enforces the cap.
Legacy missing-policy producer/reader behavior stays unchanged. The four-product
catch-up fixture now measures 7,337,041 / 7,338,460 bytes, below 7,340,032, and
drains during subsequent windows. Before correction, the new assertion reproduced
the measured overflow. An oversized companion fixture checks omission, reader
rejection and legacy tolerance without changing SQLite's smaller per-row bound.

Reviewed runtime patch SHA-256:
`b689392a86358ce193a89f68a7bf9569eaf392e310fa34d919272217180df500`;
corrected source file SHA-256:
`74057d5df680a27ccfdbfc2ab8569df24c8311b4a8b414987ebd844ce205fa56`.
The source/test diff hash is in the independent report. The Q2 implementation
commit pins these inputs; additive release evidence must bind its actual Git
SHA/tree, one revised wheel and exact-source CI before Q3 begins.

## Additional release checks

Initial preparation reused Q1's 1881 passing offline checks and exact-source CI.
The subsequent runtime change requires revised full offline, lint/type/M0/Go and
CI/build gates. Unaffected stress/public-endpoint checks remain valid.

| Check | Actual result |
|---|---|
| Explicit cloud public smoke: public depth, USD-M side REST/WebSocket, M19 statistics/backfill | 21 PASS, 28.98s |
| Cloud stress million frames and 1000 disconnect/rotation cycles | Both PASS in initial 453.53s suite; a third test failed as below |
| Cloud 100 owned-worker cancellation races | 1 PASS, 6.13s |
| Corrected cloud 100 backpressure cycles, normal + forced phase boundary | 2 PASS, 15.39s |
| Local deterministic old-assertion reproduction | Forced phase rotation FAIL: 102 gap manifests versus old expected 101 |
| Local corrected normal + forced boundary | 2 PASS, 7.73s |
| Local V5 throughput/legacy resume/Raw/policy/SQL focused suite | 33 PASS, 6.12s |
| Ruff / strict mypy for both changed test files; diff/link checks | PASS |
| Revised full default suite | 1883 PASS, 24 online skipped, 5 stress deselected, 13 existing fork warnings, 153.40s |
| Revised Ruff / strict mypy / M0 / Go golden | PASS; mypy 280 source files |
| Cloud corrected V5 online/delta/throughput/finalization | 27 PASS, 38.10s |

[Cloud report](../reviews/2026-10-02-q2-cloud-release-preparation.json) binds actual
logs, hashes, host/interpreter/boot and terminal test unit status. The initial
stress failure is retained, not relabelled PASS. The million-frame and 1000-cycle
checks are unchanged and not needlessly rerun after a test-only correction.

The initial cloud failure was a positional/count assumption in the old stress
test: stream-phase rotation sealed one boundary's data first, and the existing
protocol preserved a zero-record reconnect marker with exact SEALING intent.
All 101 Raw generations and all 100 STARTED/COMPLETED pairs were present; the
extra marker is valid incomplete gap evidence. Forced real rotation reproduces
the old failure. The correction requires every generation's sequence-gap
evidence, ordered unique Raw update IDs, exact paired gap IDs/generation/connection
and boundary payload hashes, markers bound to the known intent, and incomplete
gap manifests. Queue, descriptor/thread and historical-continuity assertions
remain. Production Collector/spool code and seal protocol are unchanged.

The separate cloud test environment initially lacked Recorder distribution metadata: its
first focused run had 16 setup failures, then a mistyped filename selected no tests.
Both logs remain retained. Installing the project into that disposable environment
(as CI does) and using the correct test path produced the 27-test PASS above.
No installed production environment was modified by this correction.

The V5 throughput regression now directly resumes an old start lacking
`delta_policy`, continues sampling and replays that chain with the original
256-row cap; old policy is not widened. Those regressions affect tests only.
The separate byte-admission correction requires a revised wheel; Raw,
normalized and Catalog formats remain unchanged.

## Execution design and remaining gates

[Q3/Q4 procedure](../reviews/2026-10-02-q2-cloud-execution-procedure.md) binds
actual starting paths, service principals, archive identities, safe retained
custody, canonical-root/venv replacement, rollback, fresh archive sibling,
NONFORMAL warm-up, per-process pressure measurements, cumulative 2/14/38h
forecasts, finite maintenance window, both LIVE audit passes and the exact 2h
closeout/12h stop point. Actual live forecasts and identities remain Q3 gates;
no stopped synthetic timing is represented as live capacity proof.

The selected frozen workload has four ProductKeys, twelve core streams and
REST snapshots; **all auxiliary enable flags are false** in the existing
configuration. No enabled auxiliary kind is silently removed to pass testing,
and this scope grants no auxiliary completeness claim.

Cloud Recorder remains inactive/disabled, MainPID 0; old Catalog/Raw/archive and
the paused historical monitor are unchanged. Archive timer remains enabled and
active. All new tests used a separate private development environment; no
synthetic Raw was written into the production writer or external archive.
Eight pending host packages and any resulting reboot must be resolved/rechecked
under the Q3 maintenance procedure before baseline/T0. Online 15m/30m historical
collector acceptance and 5m developer-preview tests were not rerun; the relevant
public endpoint smoke ran, and selected-scope real service warm-up is a separate
mandatory Q3 gate. Physical-media certification and broader soak remain outside
this release review. Formal/12h/24h and live resource gates are **not run** yet.

Independent implementation review is complete. Revised exact-source CI, final
wheel freeze and additive release evidence remain required before Q2 closes or
Q3 mutates the production scope. `PRODUCTION_READY=NO`;
`FORMAL_V5_CREDIT_SECONDS=0`; `12H_STARTED=NO`.
