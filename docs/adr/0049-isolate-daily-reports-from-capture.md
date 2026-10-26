# ADR-0049: Keep daily reporting off the capture and shutdown paths

Latest actual October22 00:45UTC: **N1 BTC OWN24H COMPLETE_OWNER_NEW_DATA_ONLY / N2 BTC+ETH PREPARING**.

- [x] Sole N1 run3f127f83 own86400.234416692s and strict14/core actual endpoint PASS.
- [x] Both complete released LIVE passes and completed immutable verification audit415ae088:
  independent45893093/45893093 saved frames100%,12998chunks; monitor2876/2880=99.861111% separately.
  Each satisfies the latest inclusive >=99.5% owner criterion; original999 BLOCK retained.
- [x] Complete independent866893-check reconstruction ACCEPT8ac5cea3/result8afbe521/manifest199244a0;
  all27158 exported controls and14150 review members hash/size checked. No extra Raw audit or duration.
- [x] Actual OS/archive RESTORED before guard cancellation; both old windows CLOSED forever.
  Fresh canonical0c621729/handoff9297a05f independently accepted, sameboot/config/identity,
  RecorderinactiveANDdisabled/PID0/NRestarts0, backlogs/partials0, natural archive success and finite
  future CLOCK_MONOTONIC deadline at handoff collection.
- [x] Final owner24h N1 receipt **a7938d4ae2ab564945f2dd0434132bf4978f69756103f66838b5fb75eacc29ac**
  plus three independent reports actually published/fsynced/root0:988/0640/SHA-readback on VPS.
- [x] Future995 continuity policy reviewed5e1938cb:30 focused cases,13 complete86400 fake-clock cases,
  14 actual sample cases; no production wheel/source change. Recoverable faults log/retry;
  actual write/verification/trusted-evidence failure pauses affected capture and notifies.
- [ ] N2 BTC+ETH new config/identity/forecast/runtime/finite window/strict warm and fresh own86400 T0.
  Public Spot+USD-M eligibility confirmed; no N2 T0 or final capacity boundary yet.

Retained limitations:2657 gap/incomplete manifests include10 zero-frame objects, overlap0;
7 original stop-tail manifests extend <=0.920193276s after actual target (nominal UTC tail1.154609350s),
zero extra duration. Complete monitor rows/projections and4 failed full-state snapshots reconstruct;
full service_state source bytes were not kept for every row. Oct20 daily report is aggregate, not
this run's frame denominator. Four failed checks show USD-M book_ticker temporarily disconnected;
3 recovered queue-pressure episodes and6 planned rotations remain visible, no deeper origin assumed.
Only tested BTC traffic/VPS conditions accepted. Capacity/headroom matrix pending;48h PENDING,
R078 OPEN/no every-event/canonicalV5/predecessor/production-ready claim. Earlier checkpoints are history.

Date: 2026-10-20. Updated October21. Status: implementation, independent
LOCAL/OFFLINE review, exact CI39356789510, Linux229 and locked wheel68460be4
accepted; stopped BTC deployment60134a37 independently accepted. N1 own86400
strict target is complete; full saved-data/independent/restored24h acceptance
is pending. The owner
authorized a minimal stability repair and progressive VPS capacity qualification,
starting BTCUSDT on both markets with all14 functions. See the
[capacity acceptance plan](../milestone_acceptance/R2-vps-symbol-capacity-20261020.md).

Run2ae5 failed after38130.113168833s. At UTC midnight, book_ticker queues filled,
writer/observer calls took tens of seconds, heartbeat stopped advancing, and
the original60s recovery bound expired. Recorder later exceeded its90s stop
limit. The independent current-evidence diagnosis is
`16434e9a67a2c452c4713c0ea53bc584c0d54d180ede18004bd06141be08ab7d`.
There is no incident thread stack, so the field stall's unique initiating cause
is not established. However, the exact deployed source contains a reproducible
blocking defect: metric rollover synchronously generates a historical report
while holding a lock also acquired by capture lifecycle/quality callbacks.

Controlled old-source reproduction holds report generation at a barrier. A
writer holds the metrics lock; the event loop's lifecycle observation then
blocks and heartbeat stalls. The candidate no longer invokes report generation
from that path. This is an implementation/test coverage defect, not evidence
that longer queues, recovery deadlines or stop limits are needed.

## Decision

- Metric rollover and product/global shutdown retain idempotent durable batch
  flushes. They do not render reports or wait for historical report reconstruction.
- The existing ServiceRuntime owns one report child at a time, with its own live
  read-only Catalog connection. No additional service or general job queue is
  introduced. Capture does not share its metrics/Catalog locks with that process.
- Every60s while Recorder runs, the child checks committed closed-day report
  inputs and renders at most one due day. A300s background report budget bounds
  one attempt; failures remain visible and retry with the original inputs.
  Shutdown terminates/reaps the owned child, with5s then kill/5s bounds, rather
  than waiting for a report. Recorder's90s stop limit is unchanged.
- A small report receipt binds the cumulative metric/remote-authority row and
  event revisions through the selected day and the actual JSON/CSV hashes.
  Late prior-day inputs invalidate that day and affected later reports. A receipt
  is published only after both outputs are durable and the input revision is
  unchanged. The receipt schedules work; it replaces no authority validation.
- A single durable last-attempt-day cursor rotates retries, so a broken report
  day cannot indefinitely prevent healthy days from being produced. Each child
  still attempts at most one day; there is no general job queue.
- Historical metrics and remote authority are read in short keyset pages with
  the original unique sort order. All per-row authority checks remain. This
  bounds intermediate memory and avoids holding a report read transaction across
  the full history; it does not promise constant total work as history grows.
- Manual and automatic publication use the same cross-process file lock so a
  mixed JSON/CSV generation cannot be recorded as complete. This race was
  independently reproduced during review and corrected.
- Report contents, UTC timestamp meaning and existing archive authority checks
  remain unchanged. The explicit `report daily` CLI remains available, including
  current-day reports and operation while Recorder is stopped. Automatic reports
  concern closed days and run while Recorder is running; stopping no longer
  synchronously produces the current partial-day report.

The change is built from deployedca760, without activating main's offline R3
runtime. All Raw durability/backpressure, owned-byte budgets, strict endpoint,
recovery60s and capture duration gates remain. Failed data and original FAILED
receipts remain unchanged; actual restored-beforecancel cleanup closed that
window. It cannot be reused.

## Verification and release gates

Required focused regressions cover controlled slow reporting versus heartbeat,
midnight/late metrics, idempotent batches, report failure/retry without new market
events, partial JSON/CSV publication, input changes during building, remote-only
days/authority failures, concurrent publication and child termination/reaping.
Run the existing broader suite and exact CI. Release a new locked wheel and
identity only after independent code/supply-chain review and affected Linux QA.
Fresh exclusive-lock metadata inventory found zero current failed partials and
zero nonretired chunks/transactions; no recovery or byte copy is needed. Recheck
these stopped preconditions before deployment. Then use
fresh capacity/time admission, an independent finite window and a new86400s T0.
Previousca760 own2h/12h receipts remain evidence for their original source only;
they supply no duration for the repaired24h.

The full new saved-data dual LIVE/immutable verify, independent final review,
strict endpoints, planned rotation evidence, actual OS/archive restoration and
owner24 receipt remain mandatory for every accepted capacity stage. No48h
expansion or ProductionReady claim.
R078 remains OPEN.

The isolation follows Python's documented event-loop constraint: blocking work
delays other asyncio tasks. Separate database readers still need SQLite locking;
the worker uses the existing live-read-only API, not immutable access to a live DB.
Sources: [Python asyncio](https://docs.python.org/3.12/library/asyncio-dev.html#running-blocking-code),
[SQLite WAL concurrency](https://www.sqlite.org/wal.html#concurrency).
