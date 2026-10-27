# ADR-0054: Selected-asset capture and versioned public observations

Status: implemented for P2, 2026-10-27. Implements ADR-0053 within the approved
P2 scope; this is not production or expanded-market continuity qualification.

## Identity and scope

`ProductKey` remains `(market, symbol)`. Product markets are `spot`,
`um_perpetual`, `um_delivery`, `cm_perpetual`, `cm_delivery`. A directory Product
also records venue market, pair, base/quote/margin assets, contract type, size,
onboard/delivery times and status. Linear quantity uses base units (multiplier
1); inverse quantity uses the directory's contractSize in quote units. Shared
sources use `um_shared` / `cm_shared` and their actual pair/contract-role/global
scope, rather than pretending to be a specific perpetual or dated contract.
These are additive identities, not relabels of historical `um_perpetual` data.

`recorder.coverage` selects underlying assets and optional Spot quote filters,
with explicit product, stream, connection and request-concurrency bounds. It is
mutually exclusive with legacy explicit-symbol configuration. Each market's
exchangeInfo is requested once per process refresh, saved once, and then filtered
by selected base assets. Missing directory entries remain unknown; initial
non-trading entries, authoritative expiry, terminal states, and resource-blocked
new products have separate states. Failed sources are not deleted. Derivative
REST weight uses the existing shared UM/CM IP pool, including statistics/funding
sub-budgets; requests are charged before every wire attempt. Source errors do
not become successful observations or bypass the budget.

## WebSocket representation

The established bounded receiver, batch writer, retirement checks, periodic sync,
normal drain and Catalog discontinuity state machine are reused. Combined Binance
subscriptions reduce the number of sockets; raw_payload is the **entire exact
received combined application message**, including its stream wrapper. It is never
JSON-reencoded and presented as original bytes. The new payload_encoding is
`utf-8-json-combined.v1`, module `binance.public.websocket.v1`.

A physical stream is `ws_public_<membership-id>` or
`ws_market_<membership-id>` within its product/shared scope. One SHA-256 prefix
per immutable group binds the exact ordered subscription members to its Raw and
Catalog gap owner. This is a real identity consumer, not a per-event hash or a
dependency-tree audit. Membership changes cannot inherit another group's gap.
`source_sequence.sourceStream`, `sourceFamily`, `wireStream` are routing labels;
they are explicitly **not** exchange sequence counters. Native U/u/pu, trade IDs
and supplied clocks remain separate. `logical_envelope` exposes the logical
stream without changing payload bytes. Normal and RPI depth have independent
snapshot-backed sequence bridges; neither reconstructs a full book in capture.
Known U/u/pu jumps are marked before Raw append so the enclosing file cannot
claim sequence completeness. Duplicate/stale events do not rewind the cursor;
connection changes start a new cursor. Bootstrap overflow requests a new
snapshot and clears the exhausted health buffer without losing persisted Raw.

## REST representation

New expanded REST records use `binance-public-rest-provenance.v1`. The installed
official SDK common unsigned transport supplies the HTTP session, timeout,
proxy policy and response; a receive hook records completed-body wall and
monotonic times, status and allowlisted headers before SDK parsing. No second
HTTP implementation is introduced. The generated Spot 10.0.0 generic wrapper
incorrectly subscripts a function, so the adapter calls its common implementation
directly. Generated endpoint model exports can erase explicit nulls; the
adapter therefore selects the common parsed-JSON result, preserving decimal
strings, integers, null versus missing, arrays, identity and request bounds.
This is `sdk-parsed-json.v1`, not original HTTP. SDK/common versions are recorded.
Environment proxy support does not enable netrc authentication or redirects.

D31 has JSON **numeric** delivery prices. A binary float is insufficient for
its required precision, so its already-received SDK response body is the sole
primary `http-body-base64.v1` representation. No complete model is saved beside
it. The reader exposes fractional JSON numbers as exact decimal text. Other
model sources do not promise unknown fields or original HTTP formatting.

Legacy Spot snapshots now write provenance v3: one Base64 body when available,
otherwise one injected model. Historical model-only and model+body v1/v2
snapshots remain readable. Snapshot, sequence-bridge and existing normalized
snapshot paths use the common representation reader; no historical conversion.

D06 defaults to measured `recent-trades-compact.v1`: a bounded 2,000-ID cache
compares native IDs and retained fields directly, never a maximum-ID filter,
per-trade SHA, database lookup or historical scan. New and changed rows retain
first-observed and current observation times. Every successful poll retains its
count, empty flag, ID/time bounds and window-overflow evidence; failed/invalid
polls retain the standard failure record and do not refresh success. Eviction or
restart can repeat rows. Known non-overlapping ID windows also mark the Raw
interval with `sequence_gap`. Original per-poll response lists cannot be reconstructed.
`recent_trades_compact=false` explicitly restores one full parsed response.

REST observations share a `rest_observations` spool per actual scope. The logical
source remains in the envelope labels and the provenance source_stream/family.
This avoids thousands of mostly idle files, while retaining every selected poll.
`connection_id` identifies the logical collector/venue polling session, not a
TCP connection. Each request has its own `request_id`. The SDK does not expose a
reliable per-response TCP identity; generating a connection UUID for every poll
would incorrectly manufacture WS reconnect gaps when sealing healthy polling.
Collector restarts remain distinct sessions and retain conservative boundaries.

## Reading and compatibility

EventEnvelope and Raw chunk outer versions, framing, CRC, file hashes, archive
readback and retirement rules remain unchanged. Current readers still read all
old records. New market values and public-observation payloads require this
version-matched reader; old binaries are not claimed to understand new domains.
New normalized public-observation schemas have source_stream, source_family,
representation and observation_json. They retain complete saved observations,
including compact polling evidence and failure records; existing normalized
schemas are not renamed or rewritten. The existing ManifestCatalog/replay API
reads these schemas. Unsupported valid payload versions fail rather than guess.
Malformed WS bytes remain recoverable and normalize to explicitly invalid rows.
Specialized per-family analytical datasets are not a capture prerequisite.

The P0 inventory and P2 result disclose restricted, equivalent, unobserved and
unresolved sources separately. USD-M liquidation selection semantics remain
unresolved; no latest/largest assumption or liquidation-total derivation is added.
P3 still owns durable per-source interval accounting, new-file verification and
background/recovery qualification; no 99.5% or VPS capacity credit is inferred.
