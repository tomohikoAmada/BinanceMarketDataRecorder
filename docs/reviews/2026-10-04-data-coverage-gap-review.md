# Data coverage gap review — 2026-10-04

This review answers the owner's request to verify the claims in the referenced
“归档流程说明” conversation. It reviews public market-data coverage and separately
identifies observed capture misses. It does not change production configuration,
the frozen wheel, acceptance policy, or Q5 controls.

## Evidence and scope

Read-only VPS inspection at **2026-10-04 05:06:56 UTC / 13:06:56 Shanghai**:
Recorder RUNNING, PID86133, NRestarts0, enabled/active, core_ready=true,
four ready products. Config SHA
`5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419`;
source23ea5c4/wheelbb7e357e/identity0ea93b2c match the current handoff.

Actual products are Spot BTCUSDT/ETHUSDT and USD-M perpetual BTCUSDT/ETHUSDT.
All14 auxiliary flags are enabled. Runtime, stream declarations and archived
stream inventory agree: core diff_depth/agg_trade/book_ticker; depth snapshots;
Spot exchange_info; USD-M mark_price/liquidation/premium_index_snapshot,
funding_history/funding_info/open_interest/exchange_info and six 5m statistics
(OI, taker volume, global ratio, top-account ratio, top-position ratio, basis).

Periodic captures have their configured resolution: mark/index prices1s;
premiumIndex and OI60s; six statistics5m; exchangeInfo/fundingInfo1h.
Hourly global exchangeInfo can contain metadata about unconfigured symbols.
That does not supply their live order books, trades or OI.

Local evidence cache:
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/data-coverage-review-20261004`.
`live-snapshot.json` SHA
`cd40a9f5a8c06f303bcbb56b2d805b2304757b28a62831a43aee35f5213e60e1`.
This is a point-in-time inspection, not continuous health or a full corpus audit.

## Missing independent data within the four configured products

| Missing information | Official availability | Loss relative to current capture |
| --- | --- | --- |
| Spot individual executions | `<symbol>@trade`; recent REST trades; official historical trade files | Exact per-execution sizes/times/IDs are not recoverable from aggregated totals. Historical exchange-time records may be imported later; original WebSocket receive timing cannot. |
| USD-M individual executions and per-trade RPI classification | `GET /fapi/v1/trades` returns `isRPITrade`; older lookup also exists but requires an API-key header | Current official USD-M stream inventory does **not** establish a public `@trade` stream. Recent REST is bounded; polling is not automatically a complete live tape. No key-dependent lookup was called or proposed for this project. |
| USD-M depth including RPI | `GET /fapi/v1/rpiDepth`, `<symbol>@rpiDepth@500ms` | Normal depth excludes RPI. Existing depth/trades cannot reconstruct this missing historical liquidity. The RPI depth response is an aggregated book including RPI, with crossed levels hidden; it is not a complete order-by-order RPI ledger. |
| Spot block executions | `<symbol>@blockTrade`, `GET /api/v3/historicalBlockTrades` | No corresponding collector/import dataset. Existing normal trade aggregation is not evidence of this separate stream's execution identities. |
| Spot reference price, calculation configuration, execution rules | `@referencePrice`, `/api/v3/referencePrice`, `/api/v3/referencePrice/calculation`, `/api/v3/executionRules` | No corresponding collector. Exchange filter snapshots do not replace these separately published values and rules. Exact historical engine reference values are not established by current inputs. |
| USD-M price-index constituents | `GET /fapi/v1/constituents` | Index level is already captured, but constituent exchange/symbol/price/weight histories are absent. A scalar index cannot determine its component history. |
| USD-M symbol-level ADL rating | `GET /fapi/v1/symbolAdlRisk` | Internal risk rating is absent; current public books/trades do not supply all its inputs. This is distinct from private account `adlQuantile`. |
| USD-M insurance balance | `GET /fapi/v1/insuranceBalance` | Exchange risk-state history is absent; it cannot be inferred exactly from price/trade data. |

Availability is established from the
[current Agent Native index](https://developers.binance.com/en/docs/llms.txt),
[Spot stream reference](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~),
[USD-M market reference](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)
and the source inventory below. Reconstructability is an inference from those
schemas and the locally inspected captured inputs.

The newly checked Spot reference/rule gap applies to **both actual symbols**:
six unsigned public GETs at05:10:37–05:14:58UTC returned HTTP200.
Both symbols had non-null reference prices, ARITHMETIC_MEAN calculation,
bucketCount80/bucketWidthMs3750 and PRICE_RANGE rules. Responses and hashes are
in `reference-prices.json` and `execution-rules.json`; this snapshot says nothing
about previous rule values. The
[official rule description](https://developers.binance.com/en/docs/products/spot/faqs/price_range_execution_rules)
explains why referencePrice is distinct from an ordinary latest price or VWAP.

## Additional uncollected feeds and markets

| Coverage boundary | Current result | Reconstruction limit |
| --- | --- | --- |
| Official live klines, mini/24h/rolling tickers, average-price updates | Not subscribed | Derived OHLCV/statistics can be calculated under matching coverage/window rules. Current aggTrade is not proof of exact official bar counts, boundary timing or the original stream's provisional updates/receive times. Official historical bars are separately importable. |
| USD-M `!contractInfo` change events | Not subscribed; hourly exchangeInfo is collected | Metadata snapshots do not prove the precise time/history of changes between polls. |
| USD-M multi-assets collateral indexes `assetIndex` / `!assetIndex@arr` | Not collected | Separate collateral valuation information, not the already captured BTC/ETH price index. |
| Other symbols and USD-M delivery contracts | No continuous capture | Separate books/trades/OI cannot be derived from these two perpetuals. Existing hourly exchange metadata is partial coverage, not an independent tape. Delivery-price records are not a configured dataset. |
| COIN-M | No collector or normalized market contract | Independent market data cannot be recreated by changing units on USD-M data. |
| BTC/ETH options | No collector or normalized market contract | Books, executions, OI, marks/IV/Greeks, block trades and settlement/exercise records are absent. Current underlying data cannot establish option-market values. |
| Event Contracts | No implementation | No matching public unsigned Event Contract market-data feed was established by this review. The Web3 Prediction Trading catalog is not proof of that product's feed. Do not describe this as an already verified, readily collectable API gap. |

The [official options market reference](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/market-data)
and current Agent Native inventory support the options/API-family boundary.
This is a review of the conversation's relevant market-data scope, not an
exhaustive audit of every Binance product or authenticated data service.

## Information already preserved that the conversation understated

1. USD-M aggregate Raw **contains `nq`**. Four recent archived chunks were
   verified against their manifests; the first decoded frame in each had
   `q` and `nq`, for both BTCUSDT and ETHUSDT. For example, BTC chunk
   `c2990ec7-5845-4b72-b21e-b94a2a373ed5`, manifest SHA
   `cda88acfc5cbddf12468b6411972318b10357467770754452847ec0755eb017d`,
   stored SHA
   `60ea5968e144dca843d35a92c6e2bef2f34dcc8cb8862667cde02e5d3ceb68d6`.
   The current normalization parser/table does not expose `nq`.
   This is a **derived-schema omission**, not deletion from Raw. The official
   definition permits aggregate RPI quantity analysis from `q` and `nq`
   where valid; it still does not identify each individual RPI execution.
   All four sampled first frames had q=nq; this sampling does not establish
   absence of RPI trades elsewhere.
2. Mark-price payloads and normalized rows already include index_price.
   “Index price is missing” would be incorrect; its constituents are missing.
3. Spot/USD-M Raw already retain aggregate first/last trade IDs `f/l`.
   These expose ID ranges and can support counts when the required continuity
   assumptions hold. The conversation's blanket “aggTrade tells you nothing
   about trade count” is too strong. Per-trade sizes/times still cannot be
   recovered from one aggregate.
4. “Cannot invert aggTrade” is different from “cannot backfill historical
   trades.” The [official public-data repository](https://github.com/binance/binance-public-data)
   documents Spot and futures daily/monthly trade files. The local
   `microstructure-trades` profile already plans both, currently hardcoded to
   BTCUSDT. An implemented importer does not establish that those files have
   actually been imported. Its documented futures CSV columns do not promise
   `isRPITrade`; do not promise recovered RPI flags without checking actual
   published files. Historical files cannot supply the original live receive
   clock, connection identity or exact WebSocket bytes.

Code anchors: `binance/spot/schema.py:30`, `binance/usdm/schema.py:29`,
`binance/usdm/side_data_schema.py:26`, `binance/usdm/side_data_rest.py:101`,
`normalize/parser.py:157`, `normalize/model.py:108`,
`backfill/planner.py:45` and `domain/event.py:16`.

## Confirmed Q5 polling misses

The journal since the sole Q5 T0 shows two `NetworkError` events for OI.
Four surrounding immutable archived OI chunks were checksum/manifest verified
and decoded to locate the next successful captures.

| Product | Failed attempt, Shanghai October4 | Next captured response | Delay from failure journal |
| --- | --- | --- | --- |
| USD-M BTCUSDT OI |10:45:38.232400|10:45:43.241508|5.009107958s|
| USD-M ETHUSDT OI |12:54:57.224281|12:55:02.232296|5.008014998s|

Each product's cumulative failure counter remains1; at the inspected heartbeat,
both were RUNNING with consecutive_failures0 and fresh later successes.
These are **two missed instantaneous polling observations**, not two lost trade
events. A successful retry observes a later OI; it cannot recreate the original
instant's value. The5m statistics likewise do not reconstruct that exact value.
Evidence `oi-misses.json` SHA
`9d4f5af572bbb275c02e8f0538739ea615a56abc0fdaa3947e1bb10668d8dd37`;
journal summary SHA
`66e4ae56bb4aeb6f99237a295455d4cac97d6424114de8f95ae7a0ab5ccd6ea8`.

Q4's1269 gap/incomplete manifests and64 zero-frame manifests remain historical
facts from its full audit. They are not current lost-message counts. This review
does not recensus all current manifests or count unobservable exchange events.
No complete liquidation ledger or L3 order/queue feed was established by the
reviewed public market interfaces. In particular, forceOrder is a sparse
representative feed; the2026-04-10 changelog specifies the largest order within
1000ms. The frozen source's “latest snapshot” wording is older terminology and
does not make the recorded feed exhaustive.

At05:06:56UTC, Catalog showed10109 LOCAL_DELETED chunks and13 SEALED chunks under
ongoing capture, plus34 ACTIVE rows. LOCAL_DELETED is a local-copy lifecycle
state after verified archive custody, **not evidence of data loss**. This review
verified eight selected archived chunks; it did not re-run the full archive audit.

## Result and next scope

For preserving additional independent information in the existing products, a
future owner-selected scope could evaluate Spot trade, USD-M RPI depth and a
separately designed USD-M individual-trade acquisition path first, followed by
Spot block/reference/rules and the small derivatives risk-state feeds. Exposing
already captured `nq` is a separate normalized-contract change requiring
compatibility review. Broader markets require their own scope/architecture work.

These are review conclusions, not implementation or restart authorization.
Q5 continues under its existing source/configuration/T0; no acceptance credit or
production-ready claim is added.
