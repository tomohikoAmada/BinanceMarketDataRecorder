# ADR-0004: Clock and deterministic replay semantics

- Status: Accepted
- Date: 2026-07-22

## Decision

Capture both UTC wall-clock nanoseconds and monotonic nanoseconds immediately at
the raw message boundary, before queueing or parsing work. Preserve every
exchange-provided event, transaction, and trade timestamp without choosing one
as universally authoritative.

Monotonic values are meaningful only within the recorded process boot/clock
domain and are paired with collector instance/boot identity. They measure local
latency and ordering but are never compared across boots.

Replay exposes two explicit clocks:

- receive-time: primary UTC receive time;
- exchange-time: stream-specific documented exchange time.

Each uses a versioned total-order tie-break including market, stream,
connection/session, source sequence IDs where defined, source chunk hash, and
record ordinal. Exact ordering/dedup details are finalized with fixtures in M16.
Filesystem enumeration order is never semantic. Gap policy is explicit and
propagates incomplete intervals.

## Consequences

- Network-arrival studies and exchange-time research are both possible.
- Equal/non-monotonic exchange timestamps do not make replay nondeterministic.
- Sleep/reboot requires explicit discontinuity/gap handling.
- Clock synchronization quality must be operationally visible; UTC receive time
  is evidence, not proof of exchange latency by itself.

## Alternatives rejected

- Exchange timestamp only: discards arrival evidence and may be non-unique.
- Wall clock only: cannot robustly measure within-process durations if the clock
  adjusts.
- Monotonic global replay: invalid across process/boot domains.

## Rollback

Ordering-policy changes create a new dataset/replay version and retain old
behavior for existing manifests. Raw records remain unchanged.
