# ADR-0038: Persist each capture owner's initial stream boundary

- Status: Superseded in part by M2.2 (abolished 0-record startup chunks; startup boundary conveyed via Catalog + first real chunk manifest); independent source ACCEPT
- Date: 2026-10-02 (Amended 2026-11-02)
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

## M2.2 Amendment: Abolishing 0-record startup chunks (2026-11-02)

Under the pragmatic Python optimization and qualification plan (Milestone M2.2,
governed by the `AGENTS.md` pragmatic engineering principles):

1. **Zero Empty `.zst` Chunks on Startup**:
   The collector no longer writes or seals 0-record `.zst` files or empty manifests
   upon process startup (`StreamSpool.seal_capture_startup()` is now a no-op that
   creates 0 files on disk). This eliminates the burst of dozens of empty chunks,
   disk syncs, and Catalog mutations on every service restart.

2. **Durable Capture Startup Conveyance**:
   - The process startup boundary is captured globally once in SQLite `Catalog` as an
     operational event (`SERVICE_STARTED`) during runtime initialization.
   - Each stream maintains an in-process pending flag (`_process_boundary_pending = True`).
   - When the stream receives its first real frame and seals its first non-empty Raw
     chunk, the chunk manifest sets `"process_boundary_before_first_frame": true`.
     Subsequent chunks omit this field.
   - The boundary information is preserved across crash recovery (`Catalog` transition
     evidence and manifest validation).

3. **Replay, Normalization, and Strict Classification**:
   - The inter-chunk reconnect classifier (`_classify_inter_chunk` in
     `src/binance_market_data_recorder/audit/reconnect_boundaries.py`) inspects
     `process_boundary_before_first_frame`: if set on the new chunk, an inter-chunk
     connection change across the process restart is proven and classified as
     `EXPLICIT_SEQUENCE_GAP` without requiring an intervening 0-record chunk.
   - All subsequent unmarked connection changes within the same process execution
     continue to be caught and reported as `UNMARKED_RECONNECT`.
   - Raw v1 binary framing, manifest schemas (`MANIFEST_OPTIONAL_FIELDS`), and
     downstream replay and normalization contracts remain 100% compatible.
