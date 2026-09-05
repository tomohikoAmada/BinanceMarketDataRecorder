# ADR-0031 — Fixed seven-symbol multi-symbol Recorder expansion

- **Status:** Accepted target; MS2–MS4 not implemented
- **Date:** 2026-09-05
- **Decision owners:** Recorder project authority

## Context

MS1 is merged and provides durable symbol-aware Catalog, discontinuity, and
cursor identity. The current runtime assembly is still a single-process,
single-symbol BTCUSDT profile. The next program must expand that assembly
without changing Raw v1 or collapsing the distinction between engineering
source, deployed evidence, and future design.

## Decision

Freeze the following target for MS2–MS4:

- Symbols: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `XRPUSDT`, `DOGEUSDT`, `SUIUSDT`,
  `LINKUSDT`.
- Markets: Binance Spot and Binance USD-M perpetual.
- Core products: 14, identified by `(market, symbol)`.
- Process model: one Recorder process and one durable Catalog authority; do
  not create one operating-system process per symbol.
- Configuration: an explicit fixed allowlist, not arbitrary-symbol discovery or
  a generic exchange/plugin framework.
- Product isolation: each product owns its continuity, reconnect, depth
  resync, queue/backpressure, and side-data failure state.
- Readiness: product readiness is independently observable; global `READY` is
  fail-closed until all 14 core products satisfy the core readiness contract.
- REST authority: one process-wide shared public REST cooldown/rate-limit
  authority per relevant Binance market domain. USD-M core snapshots and
  relevant side-data calls share the existing USD-M gate; it is not multiplied
  seven ways. Preserve equivalent Spot containment.
- Side data: genuinely global records, including USD-M `funding_info` and
  `exchange_info`, are not duplicated per symbol. The six persisted 5-minute
  statistic cursor families remain symbol-specific by `(kind, symbol)`;
  `global_long_short_ratio_5m` is still symbol-scoped despite its name.
- Operations: writer rotations are phased across products, and metrics,
  queue/high-watermark, backpressure, reconnect, and recovery evidence is
  symbol-aware while genuinely process-global metrics remain global.
- Compatibility: reuse MS1 `(market, symbol, stream)` discontinuity identity
  and `gap_id` lifecycle matching. Raw v1 framing/payload bytes and external
  Contracts remain frozen absent a concrete blocker.

## Phasing

1. **MS2 — Fixed 7-symbol / 14-core-product runtime fan-out:** expand the
   existing Spot and USD-M runtime assembly and prove deterministic isolation.
2. **MS3 — Shared resources / rotation / observability:** prove shared gates,
   scheduling fairness, phased rotation, capacity behavior, and attribution.
3. **MS4 — Multi-symbol integration / deployment qualification:** build a new
   immutable artifact, deploy only after separate authorization, and run a
   bounded live qualification for all 14 products.

MS2–MS4 are accepted targets, not implementation claims. This ADR does not
authorize deployment, long burn-in, Formal M22.9, or a Production Ready claim.
No old single-symbol duration credit transfers to the behavior-changing
multi-symbol artifact. Do not automatically schedule 72h or 168h burn-in.

## Consequences

The fixed scope keeps product identity and operational behavior reviewable, and
the one-process model preserves a single Catalog authority. Fan-out must prove
that shared REST and global side-data semantics do not create cross-product
coupling. Any request for another symbol, exchange, Raw v1 change, or Contracts
change requires a new architecture decision or a concrete blocker review.
