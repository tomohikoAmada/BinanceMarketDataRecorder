# Known Limitations

Updated October 27, 2026; current P1 boundary below. Older measurements retain
their original code/run scope.

## Current production milestones

- [P1](milestones/P1/README.md) separates sequence-only capture health from full
  offline book reconstruction and preserves Raw v1. The compatibility field
  `orderbook_synchronized` now denotes the snapshot/sequence bridge; public
  status explicitly declares `full_orderbook_evaluated=false`.
- Ten-asset offline current-product fixtures do not establish expanded COIN-M /
  delivery coverage, real-network continuity, VPS capacity or long-run memory
  behavior. P2–P6 remain necessary.
- Each stream's queue/batch is bounded, but aggregate budgets scale with product
  and stream count. Simultaneous maximum saturation is not a host memory promise.
- P3's durable independent new-file verification and source-specific availability
  remain pending. Existing independent audit and archive verification are retained.

## Qualification and performance

- V5 is implemented and deployed, but its first production baseline was stopped
  before publication. No V5 Formal 2h stage started; production qualification
  remains incomplete.
- Historical V5 full Raw/archive audit cost remains a limitation of that tool,
  not the current P2 market-coverage milestone. ADR-0051 governs new qualification;
  its P3 independent new-file workflow is pending. The stopped run published 87,423
  first-pass records from 143,362 frozen manifests over more than 14 hours.
  These are partial measurements, not a complete integrity or throughput result.
- Recorder memory growth was previously a watch item, not a demonstrated leak.
  V5 baseline process samples are available; terminal scaling has not been measured.
- Long-run evidence belongs to the tested artifact and profile. Earlier V4 and
  single-product runs do not establish V5 multi-product qualification.

See [current deployment state](CURRENT_PRODUCTION_STATE.md),
[owner-stop measurements](milestone_acceptance/M22.9-v5-deployment-owner-stop.md),
and [next work](PROJECT_HANDOFF.md).

## Data coverage

- Historical imports have archive-source clocks and no local receive timestamps
  or historical L2 reconstruction.
- USD-M period statistics have a limited source window. Data older than that
  window cannot be recovered by catch-up and is recorded as a gap.
- Disconnected WebSocket intervals may contain unrecoverable exchange events;
  reconnect and gap records describe that boundary.
- Spot bootstrap uses the ADR-0011 `lastUpdateId + 1` interpretation. The recorded
  conflict between official wording and examples remains open (R-034).
- Live raw trades, live klines, L3 queues and other exchanges are outside the
  implemented capture set. See [coverage](data_coverage.md) for the exact streams.

## Operations and platforms

- Public endpoint availability, rate limits and regional access are external
  dependencies. The shared USD-M cooldown is process-local; it resets on restart.
- macOS sleep interrupts collection. Linux deployment uses systemd; macOS uses
  a logged-in-user LaunchAgent. Windows Recorder deployment is not implemented.
- Portable archive-client rollout and external-media production certification
  remain unfinished. Linux has no automatic eject backend.
- Heavy normalization, replay and backfill use the offline profile. Their working
  space must be sized independently of the live writer.
- Archive source retirement can leave the registered archive as the only copy;
  redundant backups are a separate operational concern.

[Historical limitations](known_limitations_history.md) retain earlier incidents
and artifact-specific findings. Their old next-action statements do not define
current development work.
