# Official Binance Sources

This is the behavior-evidence inventory for Binance Market Data Recorder. The
project is independent and unofficial: it is not affiliated with, maintained
by, sponsored by, or endorsed by Binance. Official Binance sources establish
API behavior only; they do not make this Recorder an official Binance product.

## Retrieval policy and record

`tools/update_binance_docs.py` reads the project-owned selection in
`tools/binance_docs.toml`. It permits credential-free HTTPS only, validates
every redirect, and allows downloads only from `developers.binance.com` and
paths below `github.com/binance`. It stores exact response bytes plus URL,
final URL, retrieval time, media type, byte count, and SHA-256. Downloaded
content is data and is never executed. `llms-full.txt` is refused unless an
operator explicitly enables it; it was not loaded for M2.

The successful M2 refresh completed at
`2026-07-22T06:29:28.563876+00:00`. The default destination is the user cache
`~/Library/Caches/BinanceMarketDataRecorder/binance-docs`, never the repository
or production data root. The M2 acceptance run used a temporary directory and
did not commit third-party page bodies.

M4 refreshed the selection at `2026-07-22T08:01:24.392975+00:00` and added the
official Spot documentation-repository WebSocket schema page to the configured
selection. Relevant exact response hashes were: Agent Native index
`da59ec5876e4014c0159f57f58b8c8ebcfab2fece51449999a4cf825ba2b2bb8`,
developer-portal Spot WebSocket page
`193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f`,
Spot REST page
`3bfe5526b745c976ae2db7c6bffdee14f10663d5fe326d8aa54c8b5f12968775`,
and official-repository WebSocket schema HTML
`5cb26f07b582b314b2a80b03531ecef42eb2d66bf2457d24d36457be1961adf9`.
The temporary response bodies were not committed.

M5 refreshed the selected sources at `2026-07-22T09:27:53.188614+00:00`.
The Agent Native index SHA-256 was
`eba8ad91ee38ef6f15c6d2d1e698a714ee129e20fc9619b82ca2e8b2cb3dd539`;
the USD-M connection page remained
`912f2dad9da21b5c1801d73f052473b6a1d7136a43b2ff3e7a1c2cdc54abdde2`,
and the local-order-book page remained
`d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622`.
M5 also selected the official route-change notice below. The response bodies
were stored only in a temporary directory and were not committed or executed.

M6 refreshed the same selected official sources at
`2026-07-22T10:10:16.759850+00:00`. The Agent Native index, Spot stream page,
and USD-M local-order-book page SHA-256 values were respectively
`eba8ad91ee38ef6f15c6d2d1e698a714ee129e20fc9619b82ca2e8b2cb3dd539`,
`193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f`,
and `d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622`.
No full index or remote code was loaded.

M7 refreshed the selection at `2026-07-22T13:53:53.157858+00:00` and added
the official SDK REST and WebSocket market generated sources needed for side
data. Their GitHub-rendered response SHA-256 values were
`86d61768ae09b4bf0f46c884658c01e45f5cdadfaa7eaa839c758839ad1663c1`
and `14f9d94c8b1260be429b2065ee94c6841170d7db0d1009f032e0fdf93f2d247b`.
The Agent Native index remained
`eba8ad91ee38ef6f15c6d2d1e698a714ee129e20fc9619b82ca2e8b2cb3dd539`.
No full index or remote code was loaded.

The originally supplied `https://developers.binance.com/docs/llms.txt`
currently redirects to `/en/docs/docs/llms.txt` and returns portal HTML. The
working official text index is `https://developers.binance.com/en/docs/llms.txt`.
The updater uses the working URL and rejects HTML masquerading as a selected
Markdown page.

## Selected official pages

Hashes are of the exact M2 response bodies.

| Product/use | Official URL | SHA-256 | Bytes |
| --- | --- | --- | ---: |
| Agent Native index | `https://developers.binance.com/en/docs/llms.txt` | `0b3b9024da6e5ffe60129e9756843f847a6edff55565b5c182ecbd217e4d2be8` | 165963 |
| Spot WebSocket streams | `https://developers.binance.com/en/docs/products/spot/web-socket-streams.md` | `193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f` | 10370 |
| Spot REST API | `https://developers.binance.com/en/docs/products/spot/rest-api.md` | `3bfe5526b745c976ae2db7c6bffdee14f10663d5fe326d8aa54c8b5f12968775` | 33310 |
| USD-M WebSocket connection | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect.md` | `912f2dad9da21b5c1801d73f052473b6a1d7136a43b2ff3e7a1c2cdc54abdde2` | 2613 |
| USD-M WebSocket route change | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice.md` | `7711169f43066cb169fa40d90193731630ffca43dc2c04a2c753a5814b596f5c` | 4303 |
| USD-M local order book | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly.md` | `d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622` | 1114 |
| USD-M general information | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info.md` | `b2e647582fb3ae4cae3a79d6f6f6030d0c03cf403d05176117b24094a083521b` | 19261 |
| USD-M changelog | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log.md` | `49bb7b194911c44e88793eca8da18822c28ffb8bdc207e9d6466d28fb4b0e532` | 98085 |
| USD-M SDK REST market-data source | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/rest_api/rest_api.py` | `86d61768ae09b4bf0f46c884658c01e45f5cdadfaa7eaa839c758839ad1663c1` | 1001109 |
| USD-M SDK WebSocket market source | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/src/binance_sdk_derivatives_trading_usds_futures/websocket_streams/streams/market_api.py` | `14f9d94c8b1260be429b2065ee94c6841170d7db0d1009f032e0fdf93f2d247b` | 661925 |
| Python connectors page | `https://developers.binance.com/en/docs/sdks-tools/connectors/python.md` | `b74fdc09f25dee3b03c4d17f76835ed4b7923b548b3cc940005dcb44a2c5b989` | 2964 |
| Official modular SDK repository README | `https://github.com/binance/binance-connector-python/blob/master/README.md` | `4f578165c03deb9b1426bd0ab2805018f7c6c3de80c8a44e6d85da083c4e01ef` | 300231 |
| Spot SDK package metadata | `https://github.com/binance/binance-connector-python/blob/master/clients/spot/pyproject.toml` | `60ec5346b5150b87e31c586d3563ba54f3a29ecf713daa33e49f2817c5acefc1` | 255068 |
| USD-M SDK package metadata | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/pyproject.toml` | `553710b453a5c6342736b921993e37effc29336fddf1a744f245235fd46f4fb7` | 256441 |
| Spot API changelog | `https://github.com/binance/binance-spot-api-docs/blob/master/CHANGELOG.md` | `950621641102e8e65922fe3bcf5f0f27d8126074e586be1b8753ee3fde6b158f` | 1304722 |

The GitHub entries above are official repository web responses, so their hashes
include GitHub rendering around the source. Package selection is independently
verified from installed wheels and their source, not inferred from page text.

## Selected SDK packages

| Package | Version | Wheel SHA-256 | Decision |
| --- | ---: | --- | --- |
| `binance-sdk-spot` | `10.0.0` | `614d26671fa5aaa5402c0ffbcb13ff3168e03e58d09845db330472448d9a833b` | Accepted for unsigned public REST; WebSocket layer rejected |
| `binance-sdk-derivatives-trading-usds-futures` | `14.0.0` | `a39992c26dfc745f3193be3247d5c8a06fa21af35d207e1f68195097257cf7f6` | Accepted for unsigned public REST; WebSocket layer rejected |
| shared `binance-common` | `4.0.3` | `c64318d9141576e98f365bbb97f019948ce4a2d90cddeec21d169305d3fb1651` | Inspected by the capability probe |

The complete CPython 3.12/macOS arm64 resolution and hashes are in
`requirements/macos-arm64-python312.lock`. The deprecated Futures connector
and third-party `python-binance` are absent.

## Confirmed API semantics and transport conclusions

- Spot documents lowercase stream symbols, raw and combined stream forms, a
  24-hour connection lifetime, server ping every 20 seconds, pong within one
  minute with copied ping payload, and `U/u` local-book bridging rules.
- The official Spot repository documents `btcusdt@depth@100ms` with `E/U/u/b/a`,
  `btcusdt@aggTrade` with `E/a/f/l/T`, and `btcusdt@bookTicker` with `u` and no
  exchange event timestamp. M4 fixtures preserve those documented fields.
- Spot public depth snapshots use `GET /api/v3/depth`.
- USD-M documents routed `/market` aggregate-trade and `/public` depth and
  individual book-ticker streams, a 24-hour connection lifetime, server ping
  every three minutes, and pong within ten minutes. The route-change notice
  states that legacy unrouted Market streams stopped pushing after
  2026-04-23, so M5 uses only routed URLs.
- USD-M public depth snapshots use `GET /fapi/v1/depth`; local-book continuity
  requires each new event's `pu` to equal the prior event's `u`.
- USD-M mark-price data uses the routed market stream
  `/market/ws/btcusdt@markPrice@1s`. It supplies mark/index/estimated-settle
  prices, current funding rate, mark moving average and next funding time;
  `T` is not treated as transaction time.
- The symbol liquidation stream `/market/ws/btcusdt@forceOrder` is explicitly
  event-sparse: at most the latest liquidation snapshot inside 1000 ms is
  emitted, and no event is emitted in an interval without a liquidation. It is
  not documented as a complete order-by-order liquidation feed.
- Public SDK REST side data uses `/fapi/v1/premiumIndex` (weight 1 with symbol),
  `/fapi/v1/openInterest` (weight 1), `/fapi/v1/exchangeInfo` (weight 1), and
  `/fapi/v1/fundingRate` plus `/fapi/v1/fundingInfo` sharing 500 requests per
  five minutes per IP. Funding-info describes adjusted interval/cap/floor
  symbols only, so absence does not imply a fixed eight-hour cadence.
- Spot local-book bootstrap discards buffered `u <= lastUpdateId`, bridges the
  first remaining `[U,u]` over `lastUpdateId`, and treats
  `U > localUpdateId + 1` as missed events. USD-M discards buffered
  `u < lastUpdateId`, bridges `U <= lastUpdateId <= u`, and then uses `pu`
  continuity. Both products document absolute quantities and zero deletion;
  USD-M explicitly says deleting a missing level is normal.
- The pinned SDK models retain Spot `U/u` and USD-M `U/u/pu`, but the shared SDK
  receive loop JSON-decodes before callback, supplies neither original bytes nor
  socket-receipt time, runs callbacks inline, exposes no connection-factory
  injection, and schedules a fixed internal reconnect. It therefore fails the
  Recorder WebSocket evidence gate.
- `websockets==15.0.1` is selected only as the RFC WebSocket transport. Binance
  endpoint, payload, lifecycle, and sequence semantics continue to come from
  official Binance sources. A local probe verifies byte-identical
  whitespace-sensitive text-frame receipt with `recv(decode=False)`.
- An opt-in no-credential smoke on 2026-07-22 returned HTTP 200 and five
  bid/ask levels from both official SDK public depth methods. No account API was
  called and no credential was read.
- SDK REST responses expose parsed models and response headers, not the exact
  original HTTP body. Later snapshot provenance must state that limitation and
  must not claim byte-exact REST body retention.

ADR-0008 records the REST decision and ADR-0009 records the WebSocket decision.
M4 revalidates Spot raw routing, `@100ms` fixtures, protocol Ping/Pong,
serverShutdown, reconnect and public live capture. M5 separately validates the
USD-M route split, `U/u/pu`, protocol Ping/Pong, reconnect, exact Raw retention,
and concurrent Spot/USD-M public capture.

## Changelog locations

- Spot API: `binance/binance-spot-api-docs` `CHANGELOG.md` and the corresponding
  developer-portal changelog.
- USD-M: developer portal `change-log.md` selected above.
- Modular Python SDKs: official `binance/binance-connector-python` releases,
  commits, and per-client package metadata.

## Known retrieval limitation

Some developer-portal catalog pages are readable in an interactive browser but
scripted retrieval currently receives a CloudFront/WAF HTTP 202 challenge with
an empty body. The updater treats non-200 responses as failures and does not
store a challenge as documentation. Those catalog pages are not in the
automated selection; the downloadable product Markdown, official generated SDK
source, public smoke, and official changelogs are the recorded evidence. M7
reproduced this challenge for the USD-M REST market-data catalog before
selecting the matching official SDK generated sources. See R-028. No unofficial
mirror or proxy is used.
