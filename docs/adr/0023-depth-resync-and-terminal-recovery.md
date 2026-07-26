# ADR-0023: Depth resync and terminal core recovery

- Status: Accepted for M19
- Date: 2026-07-26

## Context

M19 independently reproduced three reliability failures: a synchronized depth
collector did not request a new snapshot after a depth disconnect/rotation or
gap; USD-M did not restart its whole capture session after bootstrap overflow;
and one terminal core collector could remain dead while the service appeared
to run.

## Decision

Each market owns a bounded `DepthResyncCoordinator`. The coordinator is
triggered by `unexpected_disconnect`, `planned_rotation`, `server_shutdown`,
`sequence_gap`, and `bootstrap_buffer_overflow` for `diff_depth` only. It marks
readiness failed immediately, stops that market's capture session, resets only
derived readiness/book state, applies capped jittered backoff, opens fresh
streams, obtains a fresh public snapshot, and returns READY only after the
market-specific bridge succeeds. Spot and USD-M coordinators are isolated.

Catalog evidence records reason, gap UTC time, prior/new connection IDs,
snapshot payload hash/source sequence, recovered update ID, and failure count.
The recovered update ID is the reconstructor's read-only reliable local-book
ID after snapshot bridging and buffered diff application; the snapshot
`lastUpdateId` is not a substitute.
Raw events, lifecycle evidence, and gaps are never rewritten or fabricated.
Spot continues to use ADR-0011's open-risk `L+1` rule; USD-M continues to use
official `U/u/pu` continuity.

Any core market task that terminates before the global service stop—whether it
returns normally or raises—records
`CORE_MARKET_TERMINAL_FAILURE`, sets both child stop events, lets the healthy
collector seal, and exits the service nonzero. `launchd`, not an in-process
worker pool, owns process restart and startup Raw recovery.

Side-data tasks use independently recreated attempts with unbounded attempt
count, capped exponential full jitter, stop-interruptible waits, and visible
per-task status. They never stop core Raw capture. USD-M shutdown sets the
shared side stop and awaits the side-data task before sealing snapshot Raw,
flushing metrics, and closing the Catalog, so a core exception cannot orphan a
side task or create Catalog use-after-close.

## Consequences

A 23h50m depth rotation intentionally requires a snapshot resync. Auxiliary
tasks can be DEGRADED while core markets continue. Process restart is broader
than a market-local worker restart, but is simpler and fail-closed for V1.

## Rollback

Disable readiness and stop the service rather than continuing an unreliable
book. Revert coordinator code only together with these failure gates; preserve
Catalog and immutable Raw evidence.
