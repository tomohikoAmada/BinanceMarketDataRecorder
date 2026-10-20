# R2 report isolation repair and fresh24h qualification

Current state: **OFFLINE_REPAIR_INDEPENDENTLY_ACCEPTED / EXACT_RELEASE_CI_ACCEPTED / CLOUD_RELEASE_ACCEPTED / STOPPED_BTC_DEPLOYMENT_ACCEPTED**.
Candidatec368/wheel68460be4 is deployed with BTC confige2/newidentitydb52730c.
Recorder remains stopped-disabled; no new capture/window/T0 or24h result exists.
Source branch `codex/r2-report-isolation-20261020` starts from deployedca760;
main's unrelated/offline R3 changes are not part of this production release.

- [x] Full current failed-run metadata/journal diagnosis independently complete:
  report16434e9a/result7392479d/manifest6f5b57af. Original2ae5 failed at
  October20 08:01:21 Shanghai after10h35m30.113s; zero24h credit.
- [x] Actual failed-management restoration independently acceptedfcc832fc:
  OS/archiveRESTORED1792459694698677165 beforecancel1792459694727642258.
  Recorder stopped-disabled/PID0/NRestarts0; old guardCLOSED and data retained.
- [x] Controlled ca760 lock-coupling reproduction: deliberately slow report
  blocks lifecycle/heartbeat. Candidate removes report generation from rollover
  and all product/global critical shutdown paths; durable batch flush remains.
- [x] One owned report child, separate live-read-only Catalog, retryable closed-day
  JSON/CSV generation and cumulative input receipts implemented. No new service,
  queue/recovery/stop-limit increase or authority-check removal.
- [x] Current focused30 cases PASS, Ruff and mypy309 source files PASS. First broad
  LOCAL run2132PASS/2FAIL retained: the old eager-shutdown-report assertion was
  updated to verify independent reporting, and M0 location checks were rerun in
  the supported local CI-root mode. Corrected affected cases and the final full suite/exact CI below PASS;
  the initial broad run is not labelled final-head PASS.
- [x] Bounded latent review found and repaired all-history intermediate allocation
  (short ordered pages), fixed bad-day starvation (one retry cursor), and same-PID
  partial-file retry obstruction (unique owned scratch cleanup). Synthetic30000
  metric rows preserve all rows: list peak57,757,741 tracedB vs paged627,553B;
  not VPS performance evidence. Total report CPU/history scaling remains an
  explicit capacity boundary, not a new readiness claim.
- [x] Final nine-file source/fault/latent review ACCEPT at source d5a4bae:
  report1264023f/result702be13d/manifest5a8d1e94. Independent full source125
  members, paging output equivalence, late insertion, publication, fairness,
  scratch cleanup and real child isolation/reaping are accepted LOCAL/OFFLINE.
  See [frozen independent review](../reviews/2026-10-20-r2-report-isolation-latent-independent-review.md).
- [x] Final full LOCAL2136PASS/24SKIP/5DESELECTED and exact dualCI39356789510
  sourcec3686dd BOTH SUCCESS: mac2136PASS/24SKIP, Ubuntu2132PASS/28SKIP, both5
  deselected; all lint/type/M0/Go/build/CLI steps successful. Actual checkout logs
  prove c368 on both platforms. Initial PR CI39356196424 merge-tree success remains
  integration evidence only. PR84 merged06fd84a; release excludes main R3.
- [x] Actual isolated Linux QA229PASS/1DESELECTED and first new locked wheel
  68460be4/cleanreceipt3afde832 complete; native28607f26 normally succeeded
  02:22:31.685798UTC. Independent actual supply-chain ACCEPTab69ecad verifies
  all649 source files,124 source/wheel/installed Python files and28 locked deps.
- [x] Fresh current-failure metadata inventory583f307c under the existing exclusive
  read-only service lock: partials0, nonretired chunks0/archive transactions0.
  No Raw opened or recovery/custody mutation needed; original data preserved.
- [x] Actual stopped BTC deployment60134a37/newidentitydb52730c and fresh
  canonicalVERIFY complete; native27ba9652 normally succeeded06:51:46.620547UTC,
  all20 commands returned0. Installed124 Python files and locked28 dependencies
  independently ACCEPT960c82ad. Old venv/config/identity retained; Stop90s unchanged.
- [x] Fresh prospective N1 preflight565aee1c: exact BTC14 contexts/22 spools,
  zero backlogs, projected writer17,127,127,282B>10GiB; required158656.632s<165600s.
  This is a forecast, not an armed window, measured capacity or24h qualification.
- [ ] Fresh capacity/time/strict warm/resource qualification/new finite window/
  own86400T0 for BTCUSDT on both markets, then progressively larger cohorts under
  the [owner-approved capacity plan](R2-vps-symbol-capacity-20261020.md).
  No duration transfer from any prior run, source or configuration.
- [ ] Complete new selected dualreleasedLIVE/completedimmutableverify and saved
  frames denominator strictly>99.9%, independent final controls reconstruction,
  configured strict contexts (BTC N1=14)/core and actual plannedrotation/recovery evidence, actualrestoration
  beforeguardcancel, naturalarchive/futurefiniteCLOCK_MONOTONIC/freshcanonicalVERIFY/
  stopped-disabledsameidentity finalhandoff/owner24receipt/main docs. Pause only
  after the supported capacity boundary and headroom recommendation are recorded.

Automatic reporting is now scheduled outside capture; current-day/manual reports
remain available through the unchanged CLI. No promise of error-free indefinite
operation: expected external interruptions must recover, and long-running tests
must cover boundary conditions such as midnight, growth, reconnect and shutdown.
The source defect is confirmed and reproduced; no incident stack establishes it
as the unique contributor to the field stall.48hPENDING/R078OPEN; no broader
ProductionReady, canonicalV5 or every-exchange-event claim.

Exact continuation:
`/private/var/tmp/bmdr-r2-report-isolation-20261020-yf61hyp0/CONTINUATION.md`.
See [ADR0049](../adr/0049-isolate-daily-reports-from-capture.md).
