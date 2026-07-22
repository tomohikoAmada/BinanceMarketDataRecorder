# ADR-0011: Market-Specific Order-Book Reconstruction and Checkpoints

- Status: Accepted
- Date: 2026-07-22
- Milestone: M6

## Context

Raw diff-depth events and public REST snapshots are useful only if the derived
layer applies Binance's current market-specific sequence rules without hiding
missing events. Spot supplies `U/u`; USD-M supplies `U/u/pu`. The two algorithms
are similar at snapshot bootstrap but have different live continuity tests.
Raw duplicates and malformed evidence must remain unchanged, and a derived
checkpoint must never turn an unreliable interval into a complete one.

The official Spot procedure buffers diff events, obtains `/api/v3/depth`,
discards buffered events whose `u <= lastUpdateId`, requires the first remaining
event to contain `lastUpdateId` in `[U,u]`, and declares a gap when a live
event's `U` is greater than local update ID plus one. The official USD-M
procedure buffers diff events, obtains `/fapi/v1/depth`, discards events whose
`u < lastUpdateId`, requires `U <= lastUpdateId <= u`, then requires each new
event's `pu` to equal the preceding applied `u`.

## Decision

Create a derived `orderbook` package with:

- strict `BookSnapshot`, `DepthUpdate`, and `BookTicker` inputs;
- decimal, absolute price-level state where zero quantity deletes a level;
- separate Spot and USD-M sequence branches in one explicitly market-bound
  reconstructor;
- states `BUFFERING`, `SYNCHRONIZED`, and `RESYNC_REQUIRED`;
- immutable gap intervals that always retain `complete=false`, including after
  a later snapshot closes the interval;
- best bid/ask, empty-side and crossed-book audits;
- bookTicker comparison only when its update ID equals the local book update
  ID; stale/ahead observations are not called mismatches;
- SHA-256 over a canonical logical book mapping with normalized decimal text,
  descending bids, ascending asks, market, symbol and update ID;
- atomic JSON checkpoints under `data/checkpoints/`, with algorithm/schema
  version, logical hash, source Raw chunk hashes, collector version and gap
  history; Catalog stores only checkpoint metadata.

An update already covered by the local `u` is ignored as duplicate/stale. Since
updates are absolute quantities, this is logically idempotent. It does not
relax the next unseen event's market-specific continuity test.

On a sequence gap, the last book becomes unavailable to reliable consumers,
the offending event and later events are buffered, and only a new official
snapshot bridge can restore `SYNCHRONIZED`. Checkpoints are refused while the
book is unreliable. Restoring a checkpoint verifies its logical hash and then
uses the same live rules as an origin replay.

bookTicker comparison is a quality cross-check, not an exchange checksum.
Binance does not document a checksum for these V1 streams, so M6 does not claim
one. A same-update-ID mismatch, crossed book or empty side is explicit audit
evidence but does not invent missing sequence IDs.

## Consequences

Spot and USD-M inputs cannot be mixed. A deleted sequential event forces a gap
under both algorithms, but for different reasons. Repeated origin replay and
checkpoint continuation produce the same logical hash. Snapshot depth limits
mean levels outside the snapshot are unknown until updated; the local book is
not represented as a guaranteed full exchange book.

Checkpoints are derived and removable. They do not modify Raw chunks or their
manifests. A new algorithm or checkpoint representation requires a new version;
old Raw remains the rebuild source.

## Rollback

Stop derived reconstruction, remove M6 checkpoint/Catalog metadata only after
confirming it is derived, and revert M6. Retain all Raw chunks unchanged so a
corrected algorithm can rebuild them. Never rewrite or delete Raw as part of
this rollback.
