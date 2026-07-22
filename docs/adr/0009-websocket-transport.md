# ADR-0009: Use a generic WebSocket client for Binance market streams

- Status: Accepted
- Date: 2026-07-22
- Supersedes: WebSocket portion of ADR-0005

## Context

Recorder must preserve the exact received WebSocket payload bytes, timestamp at
the receive boundary, control connection/reconnect/24-hour rotation, expose all
depth IDs, support fault injection, and guarantee that a blocking callback does
not silently discard messages.

The pinned official SDKs expose Spot and USD-M WebSocket Stream models and keep
Spot `U/u` and USD-M `U/u/pu`. However, `binance-common==4.0.3` implements the
shared receive loop by calling `json.loads(msg.data)`, optionally serializing
again for Pydantic validation, then invoking synchronous `callback(parsed)` in
that same receive loop. The callback never receives `msg.data` or a socket
receipt timestamp. There is no Recorder-owned bounded handoff or injectable
connection factory, and connection creation schedules an internal fixed
23-hour reconnect task. These are hard failures of the ADR-0005 evidence gate,
even though update IDs remain visible.

`websockets==15.0.1` exposes caller-owned `recv(decode=False)`, explicit
`max_queue` high/low watermarks, connection context/close/cancellation,
reconnect iteration, ping settings, and `create_connection`. An offline local
server probe returned a deliberately whitespace-sensitive JSON text frame as
identical bytes. The library does not interpret Binance schemas; all endpoint,
ping/pong, sequence, and payload semantics remain sourced from official Binance
documentation.

## Decision

Use `websockets==15.0.1` directly against the officially documented Binance
WebSocket Market Streams in M4/M5. The future transport adapter must:

- call `recv(decode=False)` and timestamp immediately after it returns;
- enqueue the exact bytes into a Recorder-owned bounded queue before parsing;
- configure finite transport flow-control limits and make saturation a visible
  fault/gap, never a silent drop;
- own reconnect, backoff, graceful close, fault injection, and planned
  connection rotation;
- disable library behavior that conflicts with product-specific Binance
  ping/pong requirements and test Spot and USD-M separately;
- preserve raw versus combined wrapper semantics explicitly; M4/M5 must not
  discard a combined stream wrapper if that endpoint form is selected.

Current routing evidence requires Spot and USD-M separation. USD-M's official
connection page routes aggregate trades under `/market` and depth under
`/public`; the SDK's generated interfaces also route book ticker under
`/public`. M5 must validate live routing again and must not infer a single
shared route.

## Alternatives

- Official SDK WebSocket Streams fail raw-byte, timestamp, callback
  backpressure, deterministic lifecycle, and fault-injection requirements.
- `websocket-client` is transitively installed by the SDKs but adds a second
  callback-oriented model without an advantage over the selected asyncio API.
- A custom WebSocket protocol implementation would add unnecessary risk.

Transport-library mechanics were checked against the upstream 15.0.1 asyncio
client reference at
`https://websockets.readthedocs.io/en/15.0.1/reference/asyncio/client.html`.
That non-Binance source is used only for the generic library API; no Binance
endpoint, schema, timing, or sequence claim comes from it.

## Consequences

Binance schemas and endpoint behavior still come only from official sources;
the generic library owns RFC WebSocket transport mechanics. M2 adds no
long-running connection or Collector. M4/M5 must build local mock/fault tests
and their required live smoke before production capture is accepted.

## Rollback

Revert the direct `websockets` dependency, probes, and this ADR. No production
connection or data exists in M2. Any replacement must pass every ADR-0005
capability rather than merely produce parsed events.
