# Current P2 R5 test correction candidate — 2026-10-29

The test fixture now controls only the writer's rotation-time predicate via the existing
writer factory. Shutdown and TCP-reset/server-close cases cover both active-tail and
already-rotated layouts, asserting exact Raw bytes/order, connection identities, two data
chunks, one/two empty markers, completeness/gaps and Catalog discontinuity identity.
Other same-fixture layout assumptions now run with controlled writer timing. Runtime,
configuration, protocol and historical failure evidence are unchanged.

87 affected capture/reconnect/startup checks and both3/4-manifest shutdown cases pass.
See [candidate and evidence](milestones/P2/README.md) and [checks](milestones/P2/r5-20261029/checks.json).
Final correction CI and independent R5 review remain. Keep Draft PR #88 and its branch;
do not reopen accepted R1/R4 or scope/history audits. P3/production have not started.

## Prior P2 independent review — 2026-10-29 (retained)

The [independent review](milestones/P2/review-20261029.md) accepts the ADR-0055
initial-release implementation, exact source-set/budget comparison and historical-read
boundary at runtime 13602c8 / documentation HEAD 58a3ca2. The 71 affected checks pass;
R1/R4 stay accepted and the runtime commit's dual-platform CI/logs are verified.

P2 is still **NOT ACCEPTED** because the later docs-only HEAD's macOS CI failed
`test_server_shutdown_frame_is_persisted_before_reconnect` (`4 != 3` manifests).
P2-R5 has a deterministic two-case reproduction: normal spool rotation can seal the
shutdown frame before reconnect, requiring a separate zero-record boundary marker under
ADR-0027. Both payloads are retained; the fixed-count/position test rejects a legal layout.
This is an existing test assumption, not a newly demonstrated source-selection defect.

Team next: control test rotation timing and verify both allowed layouts, preserving
payload/order/connection/gap assertions; check the same fixture's related assumptions.
Run affected checks and final-commit CI, then review R5 only. Do not change runtime
integrity semantics or rerun accepted history merely to make the test green. Keep Draft
PR #88 and its branch until closure. Official applicability stays UNKNOWN, issue #585
remains nonblocking follow-up. P3, production runs and deployment have not started.

## Prior P2 initial-release implementation candidate — 2026-10-29 (retained)

The [ADR-0055 candidate](milestones/P2/initial-production-20261029/README.md) implements
the approved dated-premium omission before source creation/admission/scheduling. Its
explicit configuration sets capture_dated_premium_klines=false; old/default configurations
retain the full target. Perpetual premium, dated mark/index and all other source descriptions
are unchanged. New dated identities use the same boundary. No production operation occurred.

Coverage now declares uncaptured/UNKNOWN products and intervals, actual counts and the
same nominal budget used by admission; the new-instance effective UTC/identity and frozen
configuration are retained in existing directory events. Old failed Raw and old directory
evidence remain readable and unchanged. Retained directories produce 4,661 selected sources
plus 3 directories, 2,154 WS streams and 765.328366/min nominal derivative weight. This scope
change is not a CPU or VPS capacity measurement. R1/R4 acceptance remains unchanged.

Ten new regressions and 71 affected checks pass. Exact 13602c8 [CI and logs](milestones/P2/initial-production-20261029/ci.json)
are verified: macOS 2,366 passed/24 skipped, Ubuntu 2,362 passed/28 skipped; both complete
build/install/CLI checks. Independent P2 review remains; keep Draft PR #88 and its branch.
P3 and production have not started. Official
applicability stays UNKNOWN with issue#585 as future follow-up, not a first-release gate.

## Prior P2 owner decision and team handoff — 2026-10-29 (retained)

The owner approved an initial release without **only USD-M/COIN-M dated premium-index
klines**. Perpetual premium continues and all other selected scope stays unchanged.
Prioritize reliable basic facts and as much other distinct public data as resources
allow. [ADR-0055](adr/0055-initial-production-dated-premium-exception.md) and the
[P2 checklist](production_milestone_plan.md#p2补齐产品和数据范围) record the decision.

The prepared clarification was submitted with explicit owner authorization as
[Binance issue #585](https://github.com/binance/binance-connector-python/issues/585).
The issue was read back and its body matches the retained submission. Official dated
eligibility stays unknown; waiting for that answer no longer blocks initial-release P2.

Next, implement the declared omission before source scheduling/budget admission, including
new dated products; preserve perpetual premium, dated mark/index bars and other families.
Keep complete-target versus selected/uncaptured reporting explicit, retain old failures
and denominators, recalculate source/request budgets, and run focused boundary regressions
and final-code CI. Then request independent P2 review. Do not reopen R1/R4 or repeat their
historical audits. These implementation steps are authorized; no repeat scope approval.

This update changes plans/contracts only. Runtime/configuration still need the P2 work;
P2 is not yet accepted, Draft PR #88 and its branch remain, P3/production have not started.
Historical notes below describe the state before this owner decision.

## Prior P2 evidence review of 066c2c3 — 2026-10-29 (retained)

R2 investigation evidence and the unsent clarification draft pass independent review.
The runtime tree is unchanged from f9aaeee; R1/R4 remain accepted and no new blocking
defect was found. Fourteen saved diagnostic responses, two full directory bodies and
eight source digests match; eight dated products remain TRADING in those observations.
Current official premiumIndexKlines sections still do not establish dated applicability.
See [review and practical impact](milestones/P2/review-20261028.md#review-066c2c3).

The missing series is dated premium-index bars, not all market data for those products.
The existing failure path can consume up to 120 weight/min once 120 configured sources
reach 60-second retry backoff. No new CPU/capacity conclusion follows. A narrowly declared
initial-production coverage exception is described for owner consideration only; it has
not been approved or implemented. Do not change the scope, suppress failures or post the
public issue on the strength of this review. R2 and overall P2 remain open; keep Draft
PR #88 and its branch, main unchanged, P3 not started. No deployment was performed.

## Prior R2 investigation handoff (retained)

R1/R4 remain accepted. R2 is still OPEN: the new bounded native-HTTP diagnostic
observed eight dated premium failures, four successful perpetual controls and two
failed explicit closed-range variants. A fresh directory check confirms all eight
dated identities remain TRADING. Current official Python methods expose no extra
dated-contract parameter; official documentation still supplies no applicability rule.
See [evidence and next actions](milestones/P2/r2-20261028/README.md).

Follow the official-source stop condition: no runtime change, inferred exclusion,
series substitution or removal of failed source/retry costs. A concrete
[official clarification draft](milestones/P2/r2-20261028/binance-clarification-draft.md)
is prepared but unsent; external submission requires explicit owner authorization.
P2 remains NOT ACCEPTED, PR #88 remains Draft, P3 has not started, no deployment.
No unchanged runtime/history audit was repeated. Prior independent reviews follow.

## P2 independent re-review of f9aaeee — 2026-10-28 (retained)

R4 is accepted and closed. The existing cooldown is applied synchronously before the
persistence await; malformed directives reuse existing fallback/ban semantics while
retaining failure headers. Storage errors still propagate, pools remain correctly shared
or independent, and waiting-admission cancellation and in-flight drain ownership remain.
Both six-case reproductions, 33 cooldown tests and 42 related tests pass independently
(one online smoke skipped). Exact f9aaeee CI and logs are verified: macOS 2,356 passed,
Linux 2,352 passed. No new blocking issue was found. See the
[closure](milestones/P2/review-20261028.md#review-f9aaeee) and updated production plan.

R1 remains accepted without a new history audit. R2 U/C dated-premium applicability
remains open; keep failed sources and their costs, with no substitute or unsupported
not-offered inference. Continue R2 in P2 and associate any supported conclusion with
the coverage inventory. Overall P2 is NOT ACCEPTED; retain Draft PR #88 and its branch.
Main is unchanged, P3 has not started, and no deployment or production operation occurred.

## Prior R4 admission-order correction handoff (retained)

The R4 correction candidate moves the existing shared-pool cooldown immediately after
capture returns a known rejection and before the first persistence await. Duration
semantics remain those accepted in the 195762c review. Failure persistence, fatal storage
errors, waiting-admission cancellation and existing in-flight drain ownership remain.
Eight new order/storage regressions fail on the previous runtime and pass after correction.
Four further invalid-header cases reuse existing fallback semantics while retaining the
failure record, so moving the gate cannot make malformed directives skip persistence. Both
six-case reproductions pass. See [current P2 result](milestones/P2/README.md) and
[checks](milestones/P2/closeout-20261028/cooldown-order-checks.json). New exact-commit CI
is recorded in Draft PR #88; independent re-review is still required.

R1 remains accepted from the independent 195762c review. No R1 configuration, import
contract, measurement or historical samples changed. R2 U/C dated-premium applicability
remains unresolved and failed sources stay retained without substitution. P2 is NOT
ACCEPTED; keep Draft PR #88 and the development branch, main unchanged, P3 not started.
No Binance probe, deployment or production-data operation was performed in this correction.

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
