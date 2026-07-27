# ADR-0025: One proxy transport policy for every production network exit

Status: accepted for M20.

## Context

The pre-M20 Spot and USD-M WebSocket openers forced `proxy=None`, while
`urllib`, the official Binance SDK, and Historical Backfill could each make a
different environment-proxy decision. On a host without policy routing this
split could make a market partly reachable, obscure the path used by a request,
or bypass an operator's explicit decision.

The policy must remain credential-free and cannot add transport details to Raw
or other data contracts. The RK3588 deployment has a local user-managed mixed
HTTP proxy, but the project cannot control its node, configuration, routing, or
credentials.

## Decision

`network_proxy_mode` is mandatory in the effective configuration and defaults
to `direct`. `network_proxy_url` is allowed only for `explicit`.

- `direct`: websockets receives `proxy=None`; `urllib` receives an empty
  `ProxyHandler`; official SDK configuration receives `proxy=None`. Shell proxy
  variables cannot silently change this mode.
- `environment`: websockets receives `proxy=True`, retaining websockets 15's
  standard `getproxies()` and `no_proxy` behavior. REST selection prefers
  `wss_proxy`, `https_proxy`, then `http_proxy` as applicable, honors
  lower-case precedence and `no_proxy`, and passes the resolved unauthenticated
  HTTP(S) host/port/protocol dictionary to the official SDK.
- `explicit`: a validated HTTP or HTTPS URL is passed consistently to
  websockets, an explicit `ProxyHandler`, the SDK host/port/protocol dictionary,
  and Historical Backfill.

SOCKS, missing/invalid ports, paths, queries, fragments, and any username,
password, or userinfo are rejected. Exceptions do not echo the supplied value.
Public configuration, doctor, service state, and status expose only
`proxy_mode`, `proxy_scheme`, `proxy_loopback`, and `proxy_port`. The URL is
never placed in Raw, manifests, Catalog event bodies, ordinary logs, or test
snapshots.

CONNECT failure, timeout, and proxy restart are ordinary visible transport
failures. Existing bounded reconnect, market-local depth resync, gap, and
unreliable-interval rules remain authoritative; the proxy layer never retries
or discards market data independently.

The documentation updater retains its separate host/content/redirect
allowlist. This ADR does not weaken that boundary.

## Consequences

The macOS default remains explicit direct transport. Operators that need a
proxy must choose `environment` or `explicit`; a configured proxy URL cannot
silently survive a change back to another mode. Status is useful for operations
without becoming a credential-exposure surface. HTTP(S)-only explicit support
is intentionally narrower than every scheme supported by third-party
libraries.

## Rollback

Set `network_proxy_mode = "direct"` and remove `network_proxy_url`, stop and
seal, restore the prior Wheel, and restart. Preserve all Raw and gap evidence.
