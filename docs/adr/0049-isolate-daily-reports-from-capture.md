# ADR-0049: Keep daily reporting off the capture and shutdown paths

Current authoritative October21 14:14UTC checkpoint: **N1 OWN24H CAPTURE COMPLETE / SELECTED AUDIT FAILED / CAUSAL METADATA REPAIR IN PROGRESS / FINAL PENDING**.

The failed audit is a private selection defect: two completions in this attempt
lost their pre-boundary START authority rows. Frozen source pairs are valid;
retain exact original sequence4256/4258 as metadata only in a new private copy.
No production source change, old Raw selection or24h recapture is needed.
Independent diagnosis ACCEPT1b089065/22195e1f/019f1bfc (1714 checks).
Actual OS/archive RESTORED13:41:50.058491266UTC before guard cancellation;
original window CLOSED, never extend/reuse/rearm. Original failed evidence stays.
A fresh, bounded audit-only continuation, full dualLIVE/verify, final independent
rebuild/handoff/receipt are still pending. N2 has not started.

Latest human policy supersedes earlier thresholds: **monitor PASS ratio and
saved-frame contract conformance each >=99.5%, with separate denominators**.
Log and retry recoverable transient faults; a rare sample failure alone does not
stop collection. Missing trusted evidence, disk full, sustained write failure or
inability to verify cannot be called a percentage PASS: the owner explicitly
selected **pause affected capture and notify**. Preserve all error records and
fix deterministic defects. Existing source/config/duration evidence is unchanged.
Earlier checkpoint text below is retained history, not a claim of a healthy audit.


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
