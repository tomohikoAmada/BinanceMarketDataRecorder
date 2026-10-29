# Clarify dated-contract support for premiumIndexKlines (USD-M and COIN-M)

Submitted with owner authorization as
[binance/binance-connector-python#585](https://github.com/binance/binance-connector-python/issues/585)
at 2026-10-29T05:39:33Z by tomohikoAmada. The published body was read back and matched
the [retained submission](binance-clarification-issue-body.md); it adds the complete
error JSON and a direct-HTTP environment note to the original draft below.
This asks for an API documentation clarification, not trading assistance.

## Question

Does `GET /fapi/v1/premiumIndexKlines` or `GET /dapi/v1/premiumIndexKlines`
support dated USD-M / COIN-M futures, including CURRENT_QUARTER and NEXT_QUARTER?
Please clarify the supported contract types in the official endpoint documentation.

The current [USD-M market-data page](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#premium-index-kline-data),
[COIN-M market-data page](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data#premium-index-kline-data)
and generated Python methods describe symbol/interval and optional time/limit
parameters without a perpetual/dated applicability rule. Section C.3 of the
[CM/UM integration notice](https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice.md)
lists both premiumIndexKlines paths among endpoints accepting CM/UM symbols;
it does not distinguish perpetual and dated contract eligibility.

## Public, unsigned reproduction

Observed on 2026-10-28, 15:30:04–15:30:10 UTC, using direct HTTP GET to the
native host with `symbol=<native symbol>&interval=1m&limit=2`. No key, account,
order endpoint or authenticated request is involved.

| Host / path | Native symbols | Result |
| --- | --- | --- |
| fapi.binance.com /fapi/v1/premiumIndexKlines | BTCUSDT_261225, BTCUSDT_270326, ETHUSDT_261225, ETHUSDT_270326 | Each HTTP 400, code -1121, Invalid symbol |
| dapi.binance.com /dapi/v1/premiumIndexKlines | BTCUSD_261225, BTCUSD_270326, ETHUSD_261225, ETHUSD_270326 | Each HTTP 400, code -1121, Invalid symbol |
| fapi.binance.com /fapi/v1/premiumIndexKlines | BTCUSDT, ETHUSDT | Each HTTP 200, two kline rows |
| dapi.binance.com /dapi/v1/premiumIndexKlines | BTCUSD_PERP, ETHUSD_PERP | Each HTTP 200, two kline rows |

Examples:

```sh
curl 'https://fapi.binance.com/fapi/v1/premiumIndexKlines?symbol=BTCUSDT_261225&interval=1m&limit=2'
curl 'https://dapi.binance.com/dapi/v1/premiumIndexKlines?symbol=BTCUSD_261225&interval=1m&limit=2'
```

For these two dated symbols, adding
`startTime=1793201280000&endTime=1793201399999` (15:28:00–15:29:59.999 UTC,
two already closed minutes) also returned 400/-1121.

A subsequent fresh native exchangeInfo check found all eight dated symbols
with their exact native symbols and CURRENT_QUARTER/NEXT_QUARTER identities,
all TRADING (`status` for USD-M, `contractStatus` for COIN-M). These observations
do not establish that the premium endpoint permanently excludes dated contracts.

The current generated Python `premium_index_kline_data` methods use the same
native paths, with no additional `pair` or `contractType` parameter. The direct
HTTP result above does not depend on SDK response-model decoding.

## Requested clarification

1. If premiumIndexKlines is intentionally perpetual-only, please document that
   separately for USD-M and COIN-M, including migration behavior and the expected
   response for a listed dated symbol.
2. If dated contracts are supported, what exact symbol/parameters, path and time
   availability rules should a client use? Please provide a successful dated
   example or identify a service defect.
3. Does any published premium series have pair/shared scope rather than native
   dated-product scope? If so, please identify its official identity and semantics.
   We cannot assign a perpetual premium, mark-price bar, index-price bar or a
   locally computed basis to a dated-product premium series without that contract.

We are preserving these failures as unknown coverage until an official rule is
available. A documented exclusion and a supported dated query require different
client behavior; an Invalid symbol response alone cannot distinguish them.
