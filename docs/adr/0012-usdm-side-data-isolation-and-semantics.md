# ADR-0012: USD-M Side-Data Isolation and Semantics

- Status: Accepted
- Date: 2026-07-22
- Milestone: M7

## Context

USD-M L2 capture is the core service and must not inherit the availability,
polling cadence or rate-limit risks of auxiliary data. Binance currently
publishes mark/index/funding state through a one-second market WebSocket stream,
liquidation snapshots through an event-sparse market stream, and public REST
methods for mark/premium state, funding history and adjustments, open interest,
and exchange rules/filters.

The funding history and funding-info endpoints share a documented 500 requests
per five minutes per IP limit. Funding-info returns only symbols whose caps,
floors or interval were adjusted, so absence of BTCUSDT is meaningful absence,
not permission to assume eight hours. The liquidation stream emits at most the
latest liquidation snapshot for a symbol within 1000 ms and emits nothing when
there is no liquidation; it is not an exhaustive order feed.

The new developer catalog pages are interactively readable but the project
updater receives an official WAF HTTP 202 challenge. The selected official SDK
14.0.0 generated sources expose the same method, schema, cadence and route
semantics and are downloadable through the allowlisted official GitHub
repository with content hashes. R-028 records this evidence boundary.

## Decision

Implement seven independently switchable side-data kinds:

| Kind | Transport | Current route/method | Semantics |
| --- | --- | --- | --- |
| `mark_price` | exact-byte WebSocket | `/market/ws/btcusdt@markPrice@1s` | periodic 1 s mark, index, estimated settle, funding rate and next funding time |
| `liquidation` | exact-byte WebSocket | `/market/ws/btcusdt@forceOrder` | event-sparse latest snapshot within 1000 ms |
| `premium_index_snapshot` | official SDK REST | `GET /fapi/v1/premiumIndex?symbol=BTCUSDT` | polling snapshot, IP weight 1 |
| `funding_history` | official SDK REST | `GET /fapi/v1/fundingRate` | ascending event history; shared 500/5 min/IP budget |
| `funding_info` | official SDK REST | `GET /fapi/v1/fundingInfo` | sparse interval/cap/floor adjustments; shared budget |
| `open_interest` | official SDK REST | `GET /fapi/v1/openInterest?symbol=BTCUSDT` | polling snapshot, IP weight 1 |
| `exchange_info` | official SDK REST | `GET /fapi/v1/exchangeInfo` | periodic rules, rate limits and filters snapshot, IP weight 1 |

Each enabled kind owns its own internal Raw spool. WebSocket callbacks retain
the exact payload bytes and reuse the M5 bounded receive/write lifecycle. REST
records request/fetch clocks, public parameters, safe rate-limit headers,
documented weight/budget, canonical SDK model and the explicit absence of a raw
HTTP body. No account method, credential or key is present.

REST calls share a local serialization lock to avoid an initial request burst.
Each interval is configurable and positive. A transient failure increments
inspectable per-kind attempts/failures and retries after that kind's interval.
A terminal side-task failure is contained by the side-data supervisor and does
not set the Spot or USD-M core stop event. M8 will persist these counters; M7
keeps them inspectable in memory without pre-implementing daily aggregation.

Never forward-fill side data. Empty funding history, missing BTCUSDT funding
adjustment metadata, and a silent liquidation stream remain explicit absence.
The mark stream's `T` means next funding time, not transaction time. Funding
cadence is preserved from observed `fundingTime`, `nextFundingTime` and
`fundingIntervalHours`; no fixed eight-hour rule is encoded.

## Consequences

Auxiliary failure cannot block core L2. Raw duplicates from repeated REST
history polls are permitted and later normalization owns deterministic dedup.
Exchange information may contain the full public response because the endpoint
has no symbol parameter; the BTCUSDT filters are validated before acceptance.

The liquidation dataset represents Binance's documented sparse snapshots, not
all liquidations. A lack of events cannot distinguish quiet market activity
from a disconnected transport without connection-quality evidence, so the
collector records disconnections separately and never synthesizes zero events.

## Rollback

Disable any or all side-data settings and revert M7. Preserve already written
Raw side-data chunks and provenance. Core M4/M5 streams, M6 order books and
their schemas remain unchanged.
