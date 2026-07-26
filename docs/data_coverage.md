# Data coverage

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

Historical and Live data are never silently mixed: receive-time replay rejects
archive-only rows. Missing results are absent/GAP, never zero or forward-filled.
An empty 5-minute REST model is an explicit observation keyed by its requested
range and model hash, with no fabricated exchange-period timestamp.
No data class here authorizes strategy, model, account, order, or trading code.
