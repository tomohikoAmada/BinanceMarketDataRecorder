# ADR-0005: Defer Binance transports until capability evidence

- Status: Superseded by ADR-0008 and ADR-0009
- Date: 2026-07-22

M0.2 scope note: this ADR selects transport evidence for the Binance-specific
project. Official documentation and SDKs establish API semantics but do not
make the independent Recorder an official or endorsed Binance product.

## Context

Official modular Python packages currently identify themselves as
`binance-sdk-spot` and `binance-sdk-derivatives-trading-usds-futures`. Their
presence does not by itself prove raw WebSocket payload fidelity, callback
backpressure, receive timestamp control, connection rotation, or fault
injection. The official portal also changed document routing by the M0 audit,
including new USD-M `/public` and `/market` stream paths.

## Decision

M0 selects no runtime transport and implements no network client. M2 must:

- refresh only official selected documents and record hashes;
- pin and probe both official modular SDKs;
- validate unsigned public REST depth snapshot behavior;
- test WebSocket raw payload, timing boundary, lifecycle, reconnect/24-hour
  rotation, update-ID visibility, backpressure, and injected failures;
- compare a mature generic WebSocket client connected only to officially
  documented endpoints;
- issue separate REST and WebSocket transport ADRs.

Use official modular SDKs for REST when their semantics pass. Use them for
WebSocket only if every recorder requirement passes; otherwise select the
generic client without changing the official endpoint/schema semantics.

Deprecated `binance-futures-connector-python`, third-party `python-binance` as
a core dependency, unverified SDKs, MCPs, account endpoints, keys, and remote
code execution are prohibited.

## Consequences

M1 remains offline and M2 is an evidence milestone rather than premature
long-running capture. If official semantics cannot be established, M2 stops and
updates the risk register instead of building a plausible substitute.

M2 completed this gate. ADR-0008 accepts the official modular SDKs for public
REST snapshots; ADR-0009 rejects their WebSocket stream layer and accepts a
generic WebSocket transport against official Binance endpoints.

## Rollback

Supersede with M2 transport ADRs backed by recorded evidence. This decision
intentionally expires only through those accepted ADRs.
