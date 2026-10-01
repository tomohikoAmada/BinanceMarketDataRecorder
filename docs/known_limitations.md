# Known Limitations

Updated October 1, 2026 for the code merged in PR #78.

## Qualification and performance

- V5 is implemented and deployed, but its first production baseline was stopped
  before publication. No V5 Formal 2h stage started; production qualification
  remains incomplete.
- Full Raw/archive audit cost is the open P2. The stopped run published 87,423
  first-pass records from 143,362 frozen manifests over more than 14 hours.
  These are partial measurements, not a complete integrity or throughput result.
- Recorder memory growth was previously a watch item, not a demonstrated leak.
  V5 baseline process samples are available; terminal scaling has not been measured.
- Long-run evidence belongs to the tested artifact and profile. Earlier V4 and
  single-product runs do not establish V5 multi-product qualification.

See [current deployment state](CURRENT_PRODUCTION_STATE.md),
[owner-stop measurements](milestone_acceptance/M22.9-v5-deployment-owner-stop.md),
and [next work](PROJECT_HANDOFF.md#remaining-work).

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
