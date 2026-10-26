# Project Handoff

Read the [production plan](production_milestone_plan.md), [P0 result](milestones/P0/README.md),
[coverage inventory](milestones/P0/coverage.md), and [ADR-0051](adr/0051-production-continuity-and-incremental-verification.md).
PR #87 review accepts R1/R3/R4, with one R2 classifier fix remaining in P0; see the
[reproduced case](milestones/P0/review-20261026.md#pr-87-复审2026-10-26) and
[v3 baseline](milestones/P0/corrections-20261026/README.md). A temporary backlog
decrease must not hide overall growth. Add that focused regression and reclassify
retained boundaries; unchanged instrumentation needs no new VPS measurement.
Physical order is
verified before payload comparison; each source has timed write/fsync and queue
accounting; output conflicts preserve old evidence; D11 U/C sampling semantics
are disclosed separately. USD-M latest/largest remains unresolved, so no such
assumption may govern deduplication, completeness or derived totals. The new
unprofiled run's recorded label is STEADY; its separate profile is NON_STEADY and
diagnostic only. Finish the remaining R2 correction before P1. Preserve old results; compare matched per-source window work,
tail and background work in P1 A/B, with the same v3 instrumentation on both sides.

P0 preserved the original local changes and research in `d81e7d2`, then merged the
accepted `origin/main=01d7421` into `113cdb3`. Existing accepted fixes include USD-M
E/T time semantics, bounded owned ingress, stop priority, sparse OI evaluated windows,
shared-IP API budgets, independent daily-report process and WebSocket diagnostics.
Do not repair the pre-merge version again. Installed production source `20e57f3`
remains distinct; the isolated P0 benchmark is not a production installation.

For P1, retain Raw v1/SQLite/read compatibility, exact payload/time/provenance,
periodic fsync and normal drain. Current writer already batches thread handoff;
measure actual syscall batching, avoid per-message task churn, and detach expensive
derived computation. Keep queue/persistence accounting honest and bounded. Repeat
P0 fixed input/configuration for A/B and compare exact bytes/order/counts. No new
market adapter, generic fault framework, historical data migration or production run
is required by P1. Product/raw HTTP changes belong to P2; per-source monitoring and
background verifier integration belong to P3.

Use [developer guide](developer_guide.md) for the code map and local checks.
Use a temporary test root; leave production data, evidence and old venvs intact.
No inherited run duration or prerequisite private OS protection window, repeated
whole-history scan, helper receipt chain or model signature applies to P0–P6.
Archive destination readback/deletion authorization remains required.

Historical handoffs and failed attempts remain in
[pre-P0 history](history/pre-P0-PROJECT_HANDOFF.md) and
[earlier history](PROJECT_HANDOFF_HISTORY.md); their next-action instructions are superseded.
