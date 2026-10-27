# Project Handoff

Next work is **P1-R1** in the [production plan](production_milestone_plan.md).
[Independent review of 5218d9d](milestones/P1/review-20261027.md) reproduces a
connection-retirement stall in Spot/USD-M: retirement during `sleep(0)` can be
followed by a fresh idle recv, leaving rotation and later stop waiting for a new
message. Fix the handoff and add focused regressions before accepting P1.
The remaining reviewed P1 work and original measurements retain their credit.
[P1](milestones/P1/README.md) is implemented in `ce1826b8a8fa9057783dfe035596332c8c6d67d8`;
its runtime src tree is `0a173540835c6eb78de26eab9d06d165cc4902aa`. Later result,
test and documentation commits do not change that runtime identity. Follow
[ADR-0051](adr/0051-production-continuity-and-incremental-verification.md) and
[ADR-0052](adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md).

P1 preserves Raw v1/SQLite/read compatibility, exact payload/time/provenance,
periodic fsync and normal drain. The existing batch thread handoff now reaches
bounded vectored writes. Each connection owns one receive loop; full book
reconstruction no longer gates capture. Readiness declares sequence-only health
and `full_orderbook_evaluated=false`; `orderbook_synchronized` is a compatibility
alias for the snapshot-backed sequence bridge. Written and synchronized progress
are distinct. Malformed received bytes remain Raw; malformed depth invalidates
sequence health after saving. Storage failures remain fatal and retain evidence.

Local A/B/B/A uses unchanged P0 fixtures/instrumentation/classifier. All four
all-feature rounds pass physical Raw/order/fsync reconciliation, but A4 and B2
are NON_STEADY and background work differs; no all-feature CPU gain is established.
Ten-asset / twenty-product / sixty-core-stream fixtures pass, with bounded queues
and normal drain. The separate identical-byte writer comparison shows an 11.82%
median wall reduction locally, not a VPS throughput or long-run capacity claim.
See P1's single result record for raw measurements, checks, limitations and rerun commands.

P2 implements explicit market/contract identities and the
[P0 coverage inventory](milestones/P0/coverage.md), including COIN-M/delivery and
the single REST representation decision in
[ADR-0053](adr/0053-single-rest-representation-and-required-information.md): body bytes
or a sufficient SDK model, declared honestly, without model/Base64 duplication or a
universal exact-HTTP-body gate. Preserve required precision, clocks, identity, sequence
and gap information; P2 must implement and verify the representation/read compatibility.
Research consumers do not determine capture scope. P2 prioritizes distinct information
and declares resource-driven reductions before a run, preserving the minimum facts
across all selected markets; every enabled source remains in continuity accounting.
Keep the full inventory as a target and disclose uncovered observations. Evaluate bounded
recent-trade overlap removal with explicit polling evidence; do not silently deduplicate
WS or rewrite history. P3 removes unused hashes and repeated processing, merges compatible
independent read passes, and keeps normalization an on-demand consumer. Meaningful file
corruption/archive checks remain; no speculative consumer or scheduling framework is needed.
Do not treat ten current Spot/USD-M products configured
by symbol as completion of that expanded coverage. D11 USD-M latest/largest
sampling semantics remain unresolved; preserve the disclosed conflict rather than
assuming completeness, deduplication or derived liquidation totals. P3 owns
per-source availability, durable new-file verification and background recovery.

P0 R1/R3/R4 and the R2 v4 classifier correction are complete; original measurements
and the NON_STEADY diagnostic profile remain preserved. See [P0](milestones/P0/README.md)
and its [review closure](milestones/P0/review-20261026.md#r2补充修正2026-10-27).
The installed-source historical observation remains `20e57f3`, distinct from this
local candidate. P1 made no deployment, VPS service change or production-data access.
No P4–P6 duration, expanded-market production credit or new power-loss guarantee
has been earned. Production readiness remains NO.

Use [developer guide](developer_guide.md) for the code map/checks and a temporary
test root. Preserve production data, evidence and old environments. Archive
readback/deletion authorization remains required. Historical handoffs remain in
[pre-P0 history](history/pre-P0-PROJECT_HANDOFF.md) and
[earlier history](PROJECT_HANDOFF_HISTORY.md); their operations are superseded.
