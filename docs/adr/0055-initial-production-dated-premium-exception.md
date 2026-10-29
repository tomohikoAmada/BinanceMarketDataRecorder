# ADR-0055: Initial-production exception for dated premium-index bars

Status: owner-approved on 2026-10-29; implementation and focused validation independently
accepted at 13602c8/58a3ca2. Overall P2 remains pending the separate R5 test correction;
see the [review](../milestones/P2/review-20261029.md).

## Decision

The owner accepts an initial release without USD-M and COIN-M **dated-contract
premium-index klines**. Perpetual premium-index klines continue to be captured;
all other selected products, data families and required fields remain in scope.
Design still supports at least ten underlying assets, with BTC/ETH the initial
production target. Prioritize reliable basic market facts and as much additional
distinct public information as resources allow, independently of research consumers.
This is a specific disclosed scope decision, not permission for silent shedding.

The official dated applicability remains UNKNOWN. An owner-approved omission must
not be represented as exchange-not-offered, successful coverage or equivalent data.
Retain the full target inventory and historical failure observations. Do not replace
the missing series with perpetual, mark/index-price bars or locally computed basis.
Existing Raw/readers, required precision/time/identity/sequence information, 99.5%
continuity for the frozen eligible source set and saved-data correctness are unchanged.

## P2 implementation boundary

- Declare this exception in the initial-production source selection before scheduling
  or budget admission. Match the actual `um_delivery`/`cm_delivery` product identity
  and premium-kline family, not a fixed list of eight symbols or the whole D23 family.
  New dated products for the configured assets follow the same declared policy.
- Do not schedule these excluded polls or their retries. Keep dated mark-price and
  index-price klines, ordinary market data and all perpetual premium sources selected.
  Reuse existing source/configuration paths; no generic exclusion framework, new
  limiter, per-event hashing or independent polling service is required.
- In the coverage inventory/report, distinguish the full target, the frozen selected
  sources and this declared uncaptured exception with its reason. Preserve official
  applicability as unknown and old failures/gaps as observed. The exception is outside
  the future eligible source set from the declared configuration's start; never alter
  old denominators or turn an already-running source's failure into an exclusion.
- Recompute the affected source/request budget. The old eight-product/15-interval
  selection contains 120 affected sources; removing failed work is a scope change,
  not measured CPU optimization or increased VPS capacity.
- Verify dated/perpetual boundaries, new-product reconciliation, coverage/budget
  reporting and old failure readability with focused existing tests plus necessary
  regressions. Compare source sets to ensure no other family is removed. Qualify the
  final code with affected checks and CI, then obtain the final independent P2 review.

## Official clarification and acceptance

The [implementation candidate](../milestones/P2/initial-production-20261029/README.md)
uses the existing CoverageConfig field `capture_dated_premium_klines` (default true).
The explicit initial-release configuration selects false; default/old configurations
retain their complete target. The condition applies during source generation, before
admission or scheduling. The additive `source_selection` coverage report discloses the
uncaptured products/intervals, unknown official applicability, selected/full-target counts
and the same nominal budget used by admission. Its effective UTC start and collector
instance are recorded with the configuration in existing directory observations. It
does not mutate old observations or qualify continuity. See the candidate for validation
and the final review gate.

The owner separately authorized submitting the prepared clarification. It was posted
as [binance/binance-connector-python#585](https://github.com/binance/binance-connector-python/issues/585)
on 2026-10-29. The question and evidence remain available; an official answer can later
establish a supported dated query or a documented exclusion. No automated polling for
that answer, synthetic replacement series or repeated failing market probes is needed.

Waiting for this official answer is **not an initial-release P2 acceptance gate**.
The approved exception implementation and its independent review have passed. R1/R4
stay accepted; overall P2 remains pending the separate R5 deterministic-test correction
and final validation. Draft PR #88 is retained, and P3/production have not started.

## Superseded boundary

For this one initial-release omission, this decision qualifies the complete-target
interpretation in ADR-0053/0054, the P0 coverage inventory, the data contract and prior
P2 R2 notes requiring an official answer before acceptance. Those notes remain historical
evidence. It does not change Binance semantics, rewrite accepted reviews or authorize
other source omissions. The [P2 plan](../production_milestone_plan.md) owns the remaining
implementation checklist.
