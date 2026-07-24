# ADR-0011: Market-Specific Order-Book Reconstruction and Checkpoints

- Status: Accepted; Spot algorithm amended by M17 on 2026-07-24
- Date: 2026-07-22
- Milestone: M6

## Context

Raw diff-depth events and public REST snapshots are useful only if the derived
layer applies Binance's current market-specific sequence rules without hiding
missing events. Spot supplies `U/u`; USD-M supplies `U/u/pu`. The two algorithms
are similar at snapshot bootstrap but have different live continuity tests.
Raw duplicates and malformed evidence must remain unchanged, and a derived
checkpoint must never turn an unreliable interval into a complete one.

The official Global Spot procedure buffers diff events, obtains
`/api/v3/depth`, discards buffered events whose `u <= lastUpdateId`, requires
the first remaining event to contain `lastUpdateId` in `[U,u]`, and declares a
gap when a live event's `U` is greater than local update ID plus one. The
official USD-M
procedure buffers diff events, obtains `/fapi/v1/depth`, discards events whose
`u < lastUpdateId`, requires `U <= lastUpdateId <= u`, then requires each new
event's `pu` to equal the preceding applied `u`.

M17 established a Spot conflict that must remain explicit:

- **A — official Global documentation:** still says the first remaining event
  contains `lastUpdateId`, not `lastUpdateId + 1`.
- **B — official example-code behavior:** Binance's
  `binance-toolbox-python/manage_local_order_book.py` at commit
  `51547845a9e3725b98e5a1bc55d4895c69ca0ca2` accepts
  `U <= last_update_id + 1 <= u`.
- **C — immutable Raw evidence:** one public snapshot ended at
  `97799318619`; the first remaining diff was
  `[97799318620, 97799318630]`.
- **D — engineering inference:** after a snapshot covering through `L`, the
  next required sequence is `L + 1`; accepting an event that contains that
  target is consistent with the official example and the documented live-gap
  rule. This inference is not represented as a correction to the Global page.

Binance maintainers have not directly confirmed the discrepancy. R-034 remains
open, and the issue text is retained locally as an unpublished draft.

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

For Spot, `binance-local-orderbook.v2` freezes this state machine:

1. set `bootstrap_target = snapshot.lastUpdateId + 1`;
2. discard buffered events where `u < bootstrap_target`;
3. accept the first remaining event only when
   `U <= bootstrap_target <= u`; `U > bootstrap_target` is a bootstrap gap;
4. after synchronization set each next target to `local_last_update_id + 1`;
5. treat `u < target` as stale/duplicate, accept
   `U <= target <= u`, and treat `U > target` as a live gap;
6. after applying an event set `local_last_update_id = u`.

USD-M bootstrap and `pu` continuity are unchanged.

The pre-snapshot diff buffer is bounded. It emits a near-capacity audit before
exhaustion. At capacity it clears only the invalid derived bootstrap state,
marks the cause, stops that Spot capture session, waits bounded full-jitter
backoff, and starts fresh connections plus a new paced snapshot. Raw events
already written remain immutable.

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

Spot v1 checkpoints are not loaded as v2 checkpoints. They are derived and can
be rebuilt from immutable Raw. Spot and USD-M inputs cannot be mixed. A deleted sequential event forces a gap
under both algorithms, but for different reasons. Repeated origin replay and
checkpoint continuation produce the same logical hash. Snapshot depth limits
mean levels outside the snapshot are unknown until updated; the local book is
not represented as a guaranteed full exchange book.

Checkpoints are derived and removable. They do not modify Raw chunks or their
manifests. A new algorithm or checkpoint representation requires a new version;
old Raw remains the rebuild source.

## Rollback

If Binance publishes a corrected normative procedure or a maintainer confirms
different semantics, stop Spot readiness, version (do not silently mutate) the
algorithm again, invalidate only derived checkpoints/normalized products, and
rebuild from Raw. Operational anomalies such as crossed books, unexplained
gaps, or repeatable divergence from bookTicker also trigger that review.
Rollback may select the prior code only with readiness disabled; it must not
label the known `[L+1, ...]` boundary complete. Never rewrite or delete Raw.
