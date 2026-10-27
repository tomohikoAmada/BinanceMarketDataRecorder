# Data coverage

## Current implementation — P2, 2026-10-27

Selected-underlying discovery and public capture now distinguish Spot, USD-M
perpetual/delivery and COIN-M perpetual/delivery products. The
[P2 mapping](milestones/P0/coverage.md#p2-implementation-and-field-mapping--2026-10-27)
links all 53 rows to actual adapters, required fields, representation and sample
evidence. New combined WS observations retain exact application bytes; REST has
one declared primary representation. The
[P2 result](milestones/P2/README.md) records unresolved request-budget and CM
dated-premium applicability blockers. Implementation is not P2 acceptance or
production qualification. Historical coverage and deployed versions below are
preserved as their original observations.

## Historical implemented boundary — P0, 2026-10-26

Use the [first-release coverage inventory](milestones/P0/coverage.md) for the current
52-family mapping (53 rows after the U/C liquidation split) and [ADR-0051](adr/0051-production-continuity-and-incremental-verification.md)
for per-source continuity/new-file correctness. The inventory explicitly distinguishes
planned families, shared/equivalent sources, authentication limitations, not-offered
interfaces and historical alternatives. Tables below describe implemented/historical
scope only. COIN-M and dated futures, original live kline/ticker families and additional
public metadata are P2 work; existing SDK model re-encoding is not raw HTTP coverage.

| Market/data | Live | Historical official archive | Time/retention | State and intended use |
|---|---|---|---|---|
| Spot diff depth 100ms | Raw + replay, resync-gated | unavailable | exchange + receive clocks; must capture continuously | L2 reconstruction, liquidity monitoring; gaps explicit |
| Spot aggTrade/bookTicker | Raw + replay | aggTrades optional profile; no bookTicker | exchange + receive clocks live; archive source clock historical | trade bars, top-of-book checks |
| Spot exchangeInfo | Raw + normalized; startup/hourly, configurable | not imported | receive time plus serverTime when supplied | market-specific filters/status/order-types/permissions schema |
| USD-M diff depth 100ms | Raw + replay, `U/u/pu` resync-gated | unavailable | must capture continuously | L2 reconstruction |
| USD-M aggTrade/bookTicker | Raw + replay | aggTrades optional profile | live receive/exchange vs archive-source clocks | trade bars/top-of-book |
| USD-M mark price/liquidation/funding/OI/exchange info | Raw + normalized; configurable independent tasks | fundingRate baseline | liquidation is event-sparse; no forward fill | derivatives monitoring/research; USD-M exchange-info schema remains compatible |
| USD-M OI history, taker volume, global/top ratios, basis | Raw + normalized per exchange-period record; latest closed 5m, configurable | not in baseline archive | exchange-period timestamp identity; official latest month/30-day retention; capture continuously | crowding, flow, leverage and basis features; overlapping polls deduplicate deterministically |
| Spot/USD-M 1m bars; USD-M mark/index/premium bars | derived/live inputs; no live kline stream | baseline-bars | archive source clock, UTC ns | benchmarks and model inputs that do not require receive time |
| Spot/USD-M raw trades | deferred live | explicit microstructure-trades | archive source clock | trade-level studies |
| L3 queue position | unavailable | unavailable | public L2 does not provide it | unsupported |

## Verified coverage gaps — October 4, 2026

The [read-only coverage review](reviews/2026-10-04-data-coverage-gap-review.md)
matches the deployed four-product/all14-auxiliary configuration to current
official sources. Within the configured Spot/USD-M products, live individual
executions, USD-M depth including RPI, Spot block trades/reference prices/
calculation settings/execution rules and USD-M index constituents/symbol-level
ADL/insurance snapshots are not collected. Other symbols, delivery contracts,
COIN-M and options have no continuous capture. Global exchangeInfo still retains
market metadata; this does not provide those markets' live tapes.

USD-M aggregate Raw already preserves `nq`; the normalized aggregate schema
does not expose it. Mark-price capture already includes index price. Missing
individual executions cannot be inverted from aggTrade, but official historical
trade files may supply exchange-time records later; original live receive timing
cannot be recovered. The implemented microstructure historical planner currently
targets BTCUSDT and does not prove that an import has actually occurred.

The current official USD-M inventory does not establish a public `@trade` stream;
recent REST trades are bounded and key-dependent historical lookup is outside
this project's no-key scope. Klines/tickers can be derived only under matching
coverage/window semantics, without reproducing their original stream updates.
The review separately records two actual Q5 OI polling misses and their later
successful retries; it does not grant Q5 completion or every-event completeness.

Historical and Live data are never silently mixed: receive-time replay rejects
archive-only rows. Missing results are absent/GAP, never zero or forward-filled.
An empty 5-minute REST model is an explicit observation keyed by its requested
range and model hash, with no fabricated exchange-period timestamp.
No data class here authorizes strategy, model, account, order, or trading code.

## P0 R4 liquidation interpretation

USD-M's current reference describes the latest snapshot per1000ms, while its
2026-04-10 changelog (effective April14) describes the largest. The actual
selection rule is unresolved; preserve published payloads without using either
assumption for completeness, deduplication or liquidation totals. COIN-M's own
reference is recorded separately; a USD-M-only change does not establish a C
change. Both are representative feeds, not an exhaustive liquidation ledger.
See [D11 source conflict and policy](milestones/P0/coverage.md#d11-的官方语义冲突r42026-10-26).
