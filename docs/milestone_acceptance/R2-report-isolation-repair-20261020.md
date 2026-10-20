# R2 report isolation repair and fresh24h qualification

Current state: **OFFLINE_REPAIR_INDEPENDENTLY_ACCEPTED / EXACT_RELEASE_CI_ACCEPTED / CLOUD_QA_RUNNING**.
This candidate is not deployed. No new capture/window/T0 or24h result exists.
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
  the supported local CI-root mode. Corrected affected cases PASS; final exact CI
  remains pending, and the initial broad run is not labelled final-head PASS.
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
- [x] Independent release copy/build design14abb55e accepted, actual software-only
  cloud copy509064c5 complete; actual physical/exactCI/pendingnative ACCEPT12b13159.
  Single native28607f26 launched02:19:41UTC/Runtime3600/Stop120, actual complete
  QA/wheel result pending (checkpoint02:26UTC). No Recorder start or production change.
- [ ] Preserve failed partial bytes; frozen source/new locked wheel/affected Linux
  QA/independent full supply chain/stopped deployment/new identity.
- [ ] Fresh capacity/time/strict warm/resource qualification/new finite window/
  own86400T0 for BTCUSDT on both markets, then progressively larger cohorts under
  the [owner-approved capacity plan](R2-vps-symbol-capacity-20261020.md).
  No duration transfer from any prior run, source or configuration.
- [ ] Complete new selected dualreleasedLIVE/completedimmutableverify and saved
  frames denominator strictly>99.9%, independent final controls reconstruction,
  strict26/core and actual plannedrotation/recovery evidence, actualrestoration
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
