# ADR-0038: Persist each capture owner's initial stream boundary

- Status: Implemented; independent source ACCEPT; new release/cloud gates pending
- Date: 2026-10-02
- Scope: Q3 sparse WebSocket startup correction; Raw/manifest/V5/SQLite schemas unchanged

## Evidence and decision

The retained 605bc16 nonformal warm-up failed on ETHUSDT liquidation. Its
initial connection opened before T0, but the first payload arrived afterwards.
The V5 baseline contained a prior-process tail. With no intervening durable
boundary, the correct strict classifier reported an unmarked connection change.
This was separate from later real network drops.

Each Spot/USD-M stream capture owner now seals one ordinary zero-frame Raw
chunk with `reconnect_gap` before creating its writer task or opening a socket.
It preserves the current ProductKey and collector header, contains no payload
or connection IDs, and has `gap=true`, `complete=false`. It does not invent a
network-discontinuity identity or STARTED event. It runs once per capture owner,
not once per retry/generation. Existing network-gap lifecycle remains required.
An initial seal failure/cancellation prevents connection startup.

The existing zero-record replay logic supplies this boundary only to the next
nonempty chunk of the same stream/ProductKey, then consumes it. A later real
unmarked connection change still fails. The V5 classifier, thresholds, proof
requirements and auxiliary coverage policy are unchanged.

## Crash recovery and compatibility

The initial ACTIVE/RECOVERED-to-SEALING transition now optionally records sorted,
unique nonempty `seal_forced_flags`. Both recovery and direct seal retries retain
these durable flags; malformed authority blocks while retaining the partial.
Existing network seal intents and open gaps remain separate authorities. Missing
flags retain the original ordinary-seal behavior, including TEST-106. Flags first
introduced on a later retry still require their existing durable intent/open-gap
authority; this field describes the initial SEALING request.

If failure precedes SEALING, the ordinary clean header remains ACTIVE under
REQ-108. No socket opened and no data was captured. A later process records its
own initial boundary; it does not silently delete that failed header. Such an
attempt cannot pass zero-partials/quiescence acceptance. Once SEALING commits,
crashes before manifest or before SEALED recover the same incomplete semantics.

Raw v1, manifests and completed evidence remain readable by existing consumers.
Older recovery implementations cannot reconstruct the new optional flags from an
unfinished seal. A rollback must use a compatible recovery artifact; do not claim
unfinished-state recovery compatibility with an older installed wheel. No DDL,
special startup schema, fake exchange event or automatic repair is introduced.

## Verification

Focused real-storage tests cover both transports, startup-before-socket, once-only
startup, failed/cancelled sealing, idle rotation, both SEALING crash windows,
direct retries, retained pre-SEALING headers, malformed flags, delayed liquidation,
ProductKey isolation, later strict reconnect rejection and preserved pending-gap
identity/completion. Existing network fault tests explicitly isolate already
completed startup so their faults still target their original lifecycle phases.
Full offline checks, exact review/release identity and actual cloud qualification
remain required; this correction grants no Formal duration credit.
