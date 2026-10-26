# R3-b / G3-b scoped independent code review

2026-10-19 — **APPROVED**, no remaining blocking findings.

A new review subagent examined the Recorder and Gateway `codex/shared-api-budget`
branches against their respective main baselines (`59641b6` / `0c8d33d`). This was
a focused code/proof review requested by the owner, not a historical-data audit.
No VPS, production API, capture, deployed configuration or automation was accessed.

The review covered all public REST/WS admission paths, role-share composition,
endpoint costs, rolling boundaries, mutex/event-loop ownership, budget timer
generations and cancellation, deduplication, metadata startup bounds,429/418
source diagnostics, and the five-scenario two-process policy proof/evidence.

Concrete findings resolved:

- Metadata cannot synchronously wait an entire shared418 ban: it now fails
  RuntimeAdmission before DNS when unavailable and checks again at HTTP write.
- Gateway WS handshake responses now retain429/418/Retry-After and install the
  process-shared market gate before notifying independent recovery owners.
- Recorder shutdown now cancels queued shielded Spot admission, while already
  started blocking workers remain owned and single-flight semantics survive.
- The proof/report distinguishes policy composition from network transport
  recovery. Recorder REST rejection keeps its REST-only reaction; WS handshake
  rejection blocks that market's WS+REST. Static attempt shares remain independent.

One process per role, no overlapping replacement and300s restart quiet interval
(with any longer known ban preserved), actual metadata-limit checks and explicit
third-party IP headroom are requirements for future deployment. Those assumptions
are recorded in the runbook; production capacity/ordinary live observation remains
outside this offline acceptance.

The reviewer checked implementation, new tests and saved arithmetic; full suite
and sanitizer runs are parent validation, not independently rerun claims. See
[acceptance and current validation](../milestone_acceptance/R3-b-G3-b-shared-api-20261019.md).


Final parent validation/delivery: Recorder2160 macOS/2156 Ubuntu full CI;
Gateway seven ordinary jobs/Linux production25/25; local Release27/27,
ASan27/27, focusedTSan4/4 andUBSan4/4. PR#82/#39 merged asd98821c/8d171c1,
with tested/merge trees identical. This does not expand the review to live
production or repeat the independent code review.
