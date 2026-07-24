# ADR-0008: Use official modular SDKs for public REST snapshots

- Status: Partially superseded by ADR-0022 for Spot depth snapshots
- Date: 2026-07-22
- Supersedes: REST portion of ADR-0005

## Context

M2 evaluated `binance-sdk-spot==10.0.0` and
`binance-sdk-derivatives-trading-usds-futures==14.0.0`, the modular packages
named by Binance's Python connector page and official repository. Their exact
wheels and the full macOS arm64/Python 3.12 dependency resolution are hashed in
`requirements/macos-arm64-python312.lock`.

The only V1 bootstrap calls in scope are unsigned public BTCUSDT depth
snapshots. The official index identifies `GET /api/v3/depth` and
`GET /fapi/v1/depth`; the SDKs expose `Spot().rest_api.depth(...)` and
`DerivativesTradingUsdsFutures().rest_api.order_book(...)` respectively. An
opt-in live smoke on 2026-07-22 returned HTTP 200, integer `lastUpdateId`, and
five bid/ask levels for both markets without credentials or account calls.

## Decision

Use the two exact-pinned official modular SDKs for Spot and USD-M public REST
depth snapshots and later public exchange metadata covered by their generated
interfaces. Recorder-owned wrappers in later milestones must:

- instantiate clients without API key, secret, private key, or account state;
- call only the explicitly approved public market-data methods;
- record request market/symbol/limit, UTC and monotonic receive timing, HTTP
  status/headers/rate-limit evidence, SDK/package versions, and returned model;
- treat schema, network, HTTP, or model-validation failures as visible errors;
- never fall through to an account, user-data, order, or signed method.

The SDK returns parsed typed data and headers rather than the exact REST
response body. M4/M5 snapshot provenance must not claim byte-exact REST body
retention. This does not weaken the separate invariant that WebSocket payload
bytes are exact and recoverable.

## Alternatives

- Direct `requests`/stdlib REST calls would make raw response access easier but
  would duplicate official endpoint/model handling before a demonstrated need.
- The deprecated Futures connector and third-party `python-binance` are
  prohibited.
- WebSocket API request/response methods are not the market-stream transport and
  are not selected for capture.

## Consequences

The runtime gains the generated SDKs and their transitive dependencies. Exact
SDK versions are intentional because generated model/API changes require a new
capability review. Online tests remain opt-in. M4/M5 must add fixture and live
snapshot provenance tests, not reinterpret this M2 smoke as a Collector.

M17 found one demonstrated exception to this decision: the pinned common SDK
raises 418/429 exception objects without retaining the HTTP response headers,
so a caller cannot strictly honor or preserve `Retry-After`. ADR-0022 replaces
only the Spot depth-snapshot transport with a minimal credential-free HTTPS
wrapper. USD-M and other approved public SDK calls remain governed by this ADR.

## Rollback

Revert the SDK dependencies, tool, and this ADR. No production snapshot or Raw
data exists in M2. A replacement REST transport requires a superseding ADR with
equivalent public/no-credential and provenance evidence.
