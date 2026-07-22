# Official Binance Sources

## Policy and retrieval record

Only Binance official sources listed in `AGENTS.md` may establish API behavior.
Hashes below are SHA-256 of exact response bodies retrieved on
2026-07-22 around 04:48–04:55 UTC. Dynamic pages must be refreshed in M2 before
transport selection; M0 does not vendor or execute their content.

The originally specified `https://developers.binance.com/docs/llms.txt`
returned HTTP 302 to `/en/docs/docs/llms.txt`, which was portal HTML rather than
the text index at audit time. The working official index was
`https://developers.binance.com/en/docs/llms.txt`. This routing observation is
recorded as R-002 and must be handled deliberately by the M2 updater.

## Source inventory

| Product/use | Official URL | SHA-256 | M0 use |
| --- | --- | --- | --- |
| Agent Native document index | `https://developers.binance.com/en/docs/llms.txt` | `0b3b9024da6e5ffe60129e9756843f847a6edff55565b5c182ecbd217e4d2be8` | Select only relevant pages; do not default-load full index content |
| Spot WebSocket streams | `https://developers.binance.com/en/docs/products/spot/web-socket-streams.md` | `193aa07cd537b2ccc94662474fb3dda3cb774d550b1e117825919d99f91b725f` | Connection lifecycle and local-book algorithm inventory |
| Spot REST API | `https://developers.binance.com/en/docs/products/spot/rest-api.md` | `3bfe5526b745c976ae2db7c6bffdee14f10663d5fe326d8aa54c8b5f12968775` | Public depth snapshot endpoint inventory |
| Spot changelog | `https://github.com/binance/binance-spot-api-docs/blob/master/CHANGELOG.md` | `fe345417d817bb7f64f087d87f11204a7311a9e97c13af5a5ed2a8bef26ba172` | Current Spot behavior change tracking |
| Spot official docs repository README | `https://github.com/binance/binance-spot-api-docs/blob/master/README.md` | `65cb85d617dde0e9fd41cc18f7276e7ca870b41e0b821b3eaae053b6853d619a` | Confirms official/supported repository and changelog location |
| USD-M WebSocket connection | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect.md` | `912f2dad9da21b5c1801d73f052473b6a1d7136a43b2ff3e7a1c2cdc54abdde2` | Lifecycle and routed endpoint inventory |
| USD-M local order book | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly.md` | `d6a94d17fb32450c67ad598c0f923bf9df12ecdc43ced4928798a9fa56d62622` | `U/u/pu`, snapshot, absolute quantity and resync inventory |
| USD-M general information | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info.md` | `b2e647582fb3ae4cae3a79d6f6f6030d0c03cf403d05176117b24094a083521b` | Public endpoint/rate-limit policy inventory |
| USD-M changelog | `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log.md` | `49bb7b194911c44e88793eca8da18822c28ffb8bdc207e9d6466d28fb4b0e532` | Current USD-M change tracking |
| Official modular Python SDK repository README | `https://github.com/binance/binance-connector-python/blob/master/README.md` | `eed80dcafe5327153915109fa38a951e52a52d6e8ad969ca3d78753a739bdd5d` | Identifies modular SDK family/migration |
| Spot SDK package metadata | `https://github.com/binance/binance-connector-python/blob/master/clients/spot/pyproject.toml` | `69fbe9487d329088b7a70f5b573d8f41289933602ab21adc11c905d1ee465a1a` | Candidate name/version only |
| USD-M SDK package metadata | `https://github.com/binance/binance-connector-python/blob/master/clients/derivatives_trading_usds_futures/pyproject.toml` | `91250abb12371b82b0ba6be88e022d91ffe856fc217b0c913d80da173b4d2e92` | Candidate name/version only |

Official repository commits observed during the audit:

- `binance/binance-connector-python` master/HEAD:
  `15c2bfcbb9e9654d7186680a0dd32287a3285e11`.
- `binance/binance-spot-api-docs` master/HEAD:
  `a5e0bc3ddc0fd7e6bb696849323b74423fa3a54d`.

## SDK candidates as observed

| Package | Repository-declared version on retrieved master | M0 status |
| --- | --- | --- |
| `binance-sdk-spot` | `10.0.0` | candidate only; not installed or pinned |
| `binance-sdk-derivatives-trading-usds-futures` | `14.0.0` | candidate only; not installed or pinned |

M2 must confirm published versions, lock hashes/dependencies, run offline
capability probes, and record the actual selected package/version. A repository
version observation is not an installation decision.

## Semantics confirmed only for planning

These facts are sufficient to design later acceptance tests, not to claim an
implemented transport:

- Spot documents raw and combined streams, lowercase stream symbols, a
  24-hour connection lifetime, server shutdown events, ping/pong behavior, and
  local order-book snapshot/buffer/update rules.
- Spot local-book rules use `U/u`, discard old buffered events, require snapshot
  bridging, treat quantities as absolute, and remove zero-quantity levels.
- USD-M currently documents routed `/public`, `/market`, and `/private`
  endpoints. Its local-book page currently uses `/public` for depth, public
  `/fapi/v1/depth`, `U/u`, and requires each new `pu` to equal previous `u` or
  resynchronize.
- USD-M connection documentation currently states a 24-hour lifetime and its
  own ping/pong timings. Spot and USD-M lifecycle values must not be conflated.
- The official index lists public REST order-book, aggTrades, bookTicker,
  funding, mark/index/premium, open-interest, exchange-information, and
  liquidation-related market data to be selected/validated in M2/M7.

Exact diff-depth `@100ms`, aggTrade/bookTicker payload field schemas, rate
limits, REST weights, and raw/combined byte boundaries remain M2 verification
items. No M0 code relies on these planning notes.

## Changelog locations

- Spot: official repository `CHANGELOG.md` and portal product `CHANGELOG` page.
- USD-M: portal product `change-log.md` listed above.
- Modular SDK: official repository commits/releases and each client package
  metadata/changelog if present; M2 records the exact pinned revision/release.

## Agent Native updater boundary (M2)

`tools/update_binance_docs.py` will be created only in M2. It must fetch the
working `llms.txt`, select configured project pages, validate redirects and
content types, allow only `developers.binance.com` and `github.com/binance`,
save URL/retrieval time/SHA-256, avoid `llms-full.txt` by default, and never
execute downloaded code. No official Binance MCP installation procedure was
established in M0, so no MCP or Codex skill was created.
