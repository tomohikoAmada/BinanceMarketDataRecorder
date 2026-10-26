# Project Handoff

Read the [production plan](production_milestone_plan.md), [P0 result](milestones/P0/README.md),
[coverage inventory](milestones/P0/coverage.md), and [ADR-0051](adr/0051-production-continuity-and-incremental-verification.md).
Current task: complete P0 only. Next implementation: **P1, shorten Raw capture path**.

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
