# ADR-0032 — Operator-configured Binance product set

- **Status:** Accepted; MS2 implemented/offline-accepted and merged through PR #54; MS3-A merged through PR #55; MS3-B offline evidence is merged through PR #56; MS4-B stopped-deployment review is complete, while MS4-C/live qualification remains blocked/not started and Formal M22.9 is not started
- **Date:** 2026-09-09
- **Decision owners:** Recorder project authority
- **Supersedes:** ADR-0031, fixed seven-symbol multi-symbol Recorder expansion

## Original pre-MS2 context

MS1 merged the durable symbol-aware identity foundation. The current runtime
assembly remains one process with the historical BTCUSDT Spot and USD-M
perpetual profile. ADR-0031 then proposed a fixed seven-symbol target. The
project owner has replaced that future target with an operator-configured finite
Binance product set. This is an architecture decision for future MS2
implementation; it does not change the current runtime, Raw v1, Catalog/MS1
durable identity, Contracts production code/schema, or Projection production
code.

## Decision

The Recorder remains Binance-specific and supports these current markets:

- Binance Spot;
- Binance USD-M perpetual.

The operator explicitly configures the finite set of products to record. There
is no fixed symbol allowlist and no automatic all-symbol discovery. This is not
an exchange/plugin framework. Runtime topology does not hot-reload: a changed
configuration takes effect on a normal process restart.

The durable and runtime identity is:

```text
ProductKey = (market, symbol)
Mapping[ProductKey, RuntimeCollector]
```

There is one Recorder process and one durable Catalog authority. MS2 assembles
one existing `SpotCollector` for every configured Spot ProductKey and one
existing `UsdMCollector` for every configured USD-M ProductKey. Each product
owns its three core WebSocket streams, depth snapshot/bootstrap,
continuity/reconnect, order-book resync, bounded ingress queues, product
readiness, and product-local side-data state. A recoverable product-local
transport, resync, or readiness failure must not mutate another product.

A true terminal core integrity, owner, or storage failure remains
process-fatal: stop all collectors, fail closed, and let the service manager
restart the process. The design does not create one operating-system process per
symbol.

## Configuration surface

MS2 must freeze this intended TOML surface under `[recorder]`:

```toml
[recorder]
spot_symbols = ["BTCUSDT", "ETHUSDT"]
usdm_symbols = ["BTCUSDT", "ETHUSDT"]
```

The two fields are independent explicit finite symbol lists. The same symbol in
both lists creates two distinct ProductKeys, and either resolved list may be
empty. Both resolved lists empty is invalid. Configuration has two modes:

- **Legacy compatibility mode:** if neither product-selection field is present,
  resolve both fields to the historical profile:

```text
spot_symbols = ["BTCUSDT"]
usdm_symbols = ["BTCUSDT"]
```

- **Explicit product-selection mode:** if either field is present, a supplied
  field uses exactly its canonicalized list and an omitted sibling resolves to
  an empty list. No implicit BTC product is added.

For example, only `spot_symbols = ["ETHUSDT"]` resolves to Spot ETHUSDT and
an empty USD-M set; only `usdm_symbols = ["SOLUSDT"]` resolves to USD-M
SOLUSDT and an empty Spot set. Explicit empty lists are valid when the sibling
contains at least one symbol, while both explicit lists empty is invalid.

Symbols are canonicalized once at the configuration boundary to uppercase.
Empty, control-character, or whitespace-invalid symbols are rejected, and
duplicates after canonicalization within one market are rejected. Configuration
parsing does not query Binance. This architecture has no symbol environment
variables, no CLI product DSL, and no hard-coded maximum product count in this
ADR. Actual operational capacity is established later by MS3/MS4 evidence.

These fields are implemented by MS2; see `../milestone_acceptance/MS2.md`.
The original architecture-only acceptance preceded this implementation.

## Shared resources

Spot reuses the existing process/event-loop shared Spot IP limiter. MS2 does
not introduce a second Spot limiter architecture.

USD-M owns one process-wide asyncio request lock and one process-wide
`UsdMRestCooldown`. Both are injected into every configured USD-M product
Collector. The same shared REST authority is used by the process-global USD-M
side-data owner. A shared SDK client is not required unless current client
thread-safety is separately established; shared rate authority is required,
not a shared transport object.

If the resolved configured USD-M ProductKey set is empty, instantiate zero
`UsdMCollector` instances, zero product-specific USD-M side-data managers, no
process-global USD-M side-data owner, and perform no USD-M REST polling or
WebSocket collection. The process-global owner exists only when at least one
USD-M ProductKey is configured and at least one global USD-M side-data kind is
enabled. When it exists, it runs the global `funding_info` and
`exchange_info` kinds at most once per process and shares the same request
lock/cooldown as configured USD-M products. An empty Spot product set similarly
creates no Spot Collector or Spot side-data traffic.

## USD-M side data

The configurable topology must not depend on BTCUSDT being a configured core
product. USD-M therefore has one process-global side-data execution owner.

The global REST kinds are:

- `funding_info`;
- `exchange_info`.

The product-specific kinds are:

- `mark_price`;
- `liquidation`;
- `premium_index_snapshot`;
- `funding_history`;
- `open_interest`;
- `open_interest_statistics_5m`;
- `taker_buy_sell_volume_5m`;
- `global_long_short_ratio_5m`;
- `top_long_short_account_ratio_5m`;
- `top_long_short_position_ratio_5m`;
- `basis_5m`.

The six persisted five-minute cursor families remain keyed by `(kind, symbol)`.
The current durable identity `GLOBAL_SIDE_DATA_SYMBOL="BTCUSDT"` remains
unchanged for compatibility. It is a legacy global-side-data sentinel, not an
implicitly configured BTC core product. Cleaning up that durable identity
requires a separate data-contract and compatibility decision.

## Readiness and service state

Global core readiness is configuration-bound and requires all of the following:

1. the configured ProductKey set is non-empty;
2. the actual runtime ProductKey set exactly equals the configured expected
   ProductKey set; and
3. every configured product satisfies the existing core readiness contract.

Missing or unexpected products fail closed. Readiness is not defined as “all
observed collectors are ready.” The service state must expose the exact
topology, for example:

```text
products:
  spot: <SYMBOL>: <product readiness>
  um_perpetual: <SYMBOL>: <product readiness>
expected_product_count: <count>
ready_product_count: <count>
core_ready: <bool>
```

Existing market-level status may remain as a derived compatibility summary, but
it is not the topology authority. VPS readiness must independently receive or
derive the expected configured ProductKey set and verify exact state topology;
it must not trust a runtime boolean alone.

## Continuity and hard reserve

Hard-reserve safety-stop handling iterates over configured ProductKeys crossed
with the three core streams. It is never hard-coded to BTCUSDT or a fixed
14-product set. Every active configured product receives the existing MS1
durable discontinuity identity `(market, symbol, stream)`. No Catalog migration
is required by this decision.

Historical data for removed products is retained. An interval in which a
product is not configured is outside that product’s active collection scope and
must not be described as continuity-complete. Adding or re-adding a product
starts a normal current capture session through the existing bootstrap and
recovery mechanisms.

## Compatibility and non-scope

Raw v1 remains unchanged. Existing Catalog/MS1 durable identity remains
unchanged. Contracts production code/schema and Projection production code
remain unchanged. Historical BTCUSDT data and acceptance records remain valid
time-local evidence and are not rewritten by this decision.

This decision does not authorize another exchange, automatic discovery, hot
reload, one-process-per-symbol deployment, deployment, Formal M22.9, a 72-hour
or 168-hour campaign, or a Production Ready claim.

## Phasing

### MS2 — Configurable product runtime

Implement the configuration product lists; ProductKey; symbol propagation
through Spot/USD-M WebSocket, REST, schema, envelope, and spool paths; dynamic
configured Collector assembly; product-aware service state; configuration-bound
global readiness; product-aware hard-reserve discontinuity evidence; shared
USD-M REST authority; the process-global USD-M side-data singleton; and the
backward-compatible BTCUSDT/BTCUSDT default profile.

### MS3 — Shared-resource scaling, rotation, and observability

Prove REST scheduling/fairness across configured products, shared cooldown
behavior, product-aware writer rotation phase, product task/log/reconnect/
resync/backpressure attribution, bounded synthetic multi-product load, and
archive/capacity interaction. Persisted metrics schema migration is not
automatically required; prefer runtime/state/log product attribution unless a
concrete acceptance need requires durable schema change. No speculative
optimization is authorized without evidence.

### MS4 — Configurable-product integration and bounded live qualification

Freeze exact implementation authority, complete offline acceptance, produce an
immutable artifact, and obtain separate deployment authorization. Qualify a
representative mixed Spot/USD-M profile containing non-BTC products on the
Tokyo VPS. All configured products must become READY, with shared-resource
integrity, Raw/Catalog/manifest/archive checks, and CPU/RSS/queue/backpressure/
capacity observation.

A fixed qualification workload is evidence only; it is not a supported-symbol
allowlist. A bounded steady-state window may begin after all products become
ready. Do not authorize Formal M22.9, automatically schedule 72-hour or
168-hour validation, or claim Production Ready.
