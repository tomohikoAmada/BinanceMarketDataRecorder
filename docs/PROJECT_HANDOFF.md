# Current P2 independent re-review — 2026-10-28

Reviewed commit `195762c`; P2 remains NOT ACCEPTED. R1's measured fixed-cadence profile,
nominal budget and independent daily supplement pass this review. Existing 143 focused
tests and exact-commit dual-platform CI pass. Retained 17 daily ZIPs, 8 imported files
(123,773 CSV rows) and 147 Raw observations were independently checked; limits remain
explicit. See [review](milestones/P2/review-20261028.md#review-195762c) and updated plan.

R4's duration calculation passes, but its admission ordering does not: poll_once awaits
persist_rest before blocking the pool. Six new offline cases send a fresh Spot or shared
UM/CM request after 429/418 is known, while persistence waits. Enable the existing cooldown
before that first await, retain failure persistence/storage-error propagation/normal stop,
and add those regressions. Do not add a limiter framework or cancel in-flight requests.
R2 U/C dated-premium applicability remains unresolved; retain failures without substitution.
The next development work stays in P2. Keep Draft PR #88 and the working branch until
R2/R4 and final-candidate verification/re-review pass. Main is unchanged, P3 not started;
no deployment or production operation was performed. Owner's option-one scope is approved.

## Prior correction-candidate handoff (retained)

# P2 correction candidate — 2026-10-28

R4 now uses existing Retry-After/date/ban/fallback semantics and remaining receive-based
cooldown. R1's owner-approved live-plus-daily direction is implemented: explicit measured
2s USDT / 3s USDC / 30s other derivative polling, nominal steady admission bound, and a
small selected-daily-trades profile using the existing separate HistoricalImporter.
See [current P2 result](milestones/P2/README.md) for measurements, loss, exact samples,
file reader/merge contract and final checks/CI. No approval of the same direction is needed.
R2 remains unresolved: this check also found all four USD-M delivery premium symbols fail,
in addition to COIN-M delivery. Official semantics still do not establish applicability.
P2 is NOT ACCEPTED pending R2 and independent re-review. P3 has not started. No deployment
or production data change; the generic 1s default and historical evidence remain intact.

## Prior review/handoff (retained)

# Project Handoff

Current work is **P2**, independently reviewed and **not accepted** at e73068b.
The [2026-10-28 review](milestones/P2/review-20261028.md) adds R4: REST 429/418
ignores Retry-After and always blocks 120 seconds. Six offline reproductions fail
despite 2,314 full-suite and 314 focused tests passing. Fix R4 using existing
cooldown semantics, then re-review the final candidate with dual-platform CI.
Completed implementation items are checked in the production plan. The
[D06 study](milestones/P2/d06-budget-research-20261028.md) now records the owner's
October28 selection of option one: explicit per-product live polling plus a
separate official daily-file supplement. The direction is approved. Verify actual
product/date/field coverage, then implement the small independent importer and
measured frequency allocation under ADR-0054; no repeat direction approval is needed.
See the [P2 result](milestones/P2/README.md), source mapping, fixtures and ADR-0054.
The full current BTC/ETH directory requires ~5,829 derivative weight/min with D06 at 1s,
above the Recorder share and exchange pool. No unapproved cadence reduction was made;
the existing 1s default and unselected 6s proposal remain unchanged until the measured
configuration is implemented. R1 remains open for that work, not for the direction
decision. C dated premium applicability also remains
unresolved; failures are retained without a perpetual substitution. P2 is not marked
passed, P3 has not started, and production readiness remains NO.

Retained P1 closure follows.
[Independent P1 re-review](milestones/P1/review-20261027.md#r1独立复审闭合2026-10-27) accepts fix
`b292d0d892c8f53812749f1aeac7f637ba1a3b13`, src tree
`d8194460b4dcc5bd45dc52209d483c9243a98306`. Both receive loops now check retirement
immediately before receive admission, including after their scheduling yield.
The reviewer reran the original three reproductions and 196 focused tests, including
nine new regressions, and verified dual-platform CI run 40465389449 at the exact fix
commit. No additional actionable issue was found; P1 is accepted and R1 is closed.
At the P1 closeout GitHub had only main and no open PRs; no branch/PR deletion was needed.

Original P1 implementation `ce1826b` and its measurements retain src tree
`0a173540835c6eb78de26eab9d06d165cc4902aa`; the R1 fix does not rerun or relabel them.
Follow
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
and gap information; P2 now implements and verifies representation/read compatibility under ADR-0054;
its outstanding scope/source blockers remain in the P2 result.
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
