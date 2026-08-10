# Known Limitations

连续72小时和168小时长期运行验收尚未执行。
静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。
当前版本为Mac Developer Preview;Ubuntu ARM64/RK3588为Developer Preview / Soak Candidate;不得用于真实资金交易。

> **72小时验收结果为 FAIL（数据完整性合同失败）**。72小时窗口的核心进程
> 稳定性 PASS，但 2026-08-07T14:08:24Z USD-M `book_ticker` 意外断线及所有
> planned rotation 边界均无 gap 证据（gap=false/complete=true）。修复
> (M21.4.11) 已实现并提交 PR 审查，未部署。完整记录：
> `docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md`。

M21.4 was deployed and passed 2h, 12h, and 24h formal process-stability
windows. **The formal 72h window FAILED on data integrity**: the
2026-08-07T14:08:24Z USD-M `book_ticker` unexpected disconnect and every
planned rotation sealed their reconnect boundaries without gap evidence
(gap=false/complete=true). The M21.4.11 reconnect-boundary repair is
implemented and under review; it is NOT deployed. Static review, unit
tests, fault injection, and short online tests cannot substitute for
long-running proof.

## M21.4 validation status

- 2h preflight: PASS (short-window process stability; independent review)
- 12h observation: PROCESS-STABILITY PASS; data-integrity
  SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING
- 24h formal window: PROCESS-STABILITY PASS (corrective + contract forensic
  review confirmed); data-integrity
  SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING
- **72h formal window: FAIL** (core process stability PASS; data integrity
  FAIL; eligible_for_next_stage=false)
- 168h window: PENDING (not started)
- Planned rotation: **FAIL on Raw gap evidence** — all five observed
  rotations (12h/24h/72h windows and post-window) seal with
  gap=false/complete=true and no Catalog gap
- Reconnect boundary integrity: corrected boundary-local read-only audit
  found 4,680 historical unmarked transitions (11 explicit, 0 ambiguous);
  forward fix implemented, NOT DEPLOYED
- Backpressure natural exercise: gen5 PASS in formal 24h window
  (RECOVERY_CONTRACT_PASS); gen6 started in window, completed POST_WINDOW;
  Spot backpressure repair still absent
- Production Ready: NOT CLAIMED

## M20 Ubuntu ARM64

- RK3588 short validation found that side-data staleness must include the
  task's scheduled poll interval plus its failure grace; the final code does
  so, while `RETRYING` remains immediately visible.
- The 72-hour and 168-hour soaks, repeated 24-hour rotations, and 7/30-day
  operational observations have not run; they are M21.
- Linux blue/green upgrade is not certified. Use stop/seal/update/readiness and
  the documented rollback.
- Linux discovers only already-mounted external block filesystems with reliable
  identity. Physical external-media registration/disappearance was not
  exercised during M20.
- Linux has no trusted automatic eject backend in M20 and never claims
  `SAFE_TO_REMOVE`.
- Proxy availability and the operator-selected Mihomo node remain external
  dependencies. Interruptions must be represented by reconnect/resync/gap
  evidence and are not claimed complete.

## M19

- R-034 remains open: official Global Spot bootstrap wording and the official
  toolbox/observed adjacent boundary disagree. The offline evaluator compares
  both targets; it is evidence, not an official resolution.
- Historical archives provide no local receive clock and no historical L2.
- USD-M statistics have only the official latest-month/latest-30-day window.
  Durable Cursors and bounded catch-up recover in-window downtime, but periods
  older than the source window remain unrecoverable and are recorded as gaps.
- Live raw trades, live klines, L3 queue data, accounts, orders and trading are
  not implemented.

- Repeated 24-hour Binance connection rotation has been verified in the
  M21.4 12h window (06:52-06:53 UTC) and 24h window (06:43:11–06:43:23Z).
  **However, rotation is process/orderbook-stable only: every observed
  rotation seals its reconnect boundary without Raw gap evidence
  (gap=false/complete=true). Rotation Raw-data integrity is
  SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING until the M21.4.11 fix
  deploys.**
- Long-term memory, file-descriptor, bounded-queue, archive-backlog, growth
  forecast, and resource-leak behavior has been validated in 2h, 12h, and 24h
  windows only; 72h/168h remain pending.
- macOS sleep and closed lid interrupt user-session networking. The Recorder
  marks detected gaps but cannot recover events Binance no longer provides.
- macOS Apple Silicon retains its logged-in-user LaunchAgent behavior. Ubuntu
  ARM64 adds M20 systemd support at Soak Candidate level. Windows is not
  implemented.
- Capture scope is BTCUSDT Spot and BTCUSDT USD-M perpetual. Other symbols and
  exchanges are not part of this preview.
- The current Binance Global Spot bootstrap wording conflicts with the
  official toolbox example and observed adjacent Raw boundary. ADR-0011 uses
  `lastUpdateId + 1`, records the conflict, and awaits maintainer confirmation.
- Binance public endpoints can throttle, ban, change, or be unavailable in a
  region. Snapshot logic observes rate headers, Retry-After, and ban windows;
  public access is still an external dependency.
- Optional archive media can become the only Raw copy after verified local
  deletion. Recorder is not a backup system.
- No GUI, account API, API-key storage, order submission, strategy, factor,
  backtest, or trading engine exists.

Before simulated or live trading work, complete a frozen-commit 168-hour run
whose first 72 hours pass the stability gate, including repeated connection
rotation and resource/backlog evidence. Such work is Future Work and is not
part of M20; it is the M21 acceptance scope.

### Additional M21.4 known limitations

- **Ordinary reconnect and planned rotation seal without gap evidence
  (72h FAIL root cause)**: the deployed artifact reconnects
  unexpected_disconnect/planned_rotation/server_shutdown/session-restart
  boundaries in the same generation with no Catalog STARTED/COMPLETED, no
  `sequence_gap`, and manifest `gap=false/complete=true`. The formal 72h
  window FAILED on this contract (2026-08-07T14:08:24Z book_ticker
  disconnect; receive gap ~1.73 s; u jump 56,294,564). The M21.4.11 forward
  fix (unified Reconnect Boundary state machine, manifest-level
  `reconnect_gap`, seal defense) is implemented and under review; **until it
  is deployed, any interval crossing a reconnect boundary produced by the
  running artifact must not be trusted as complete.**
- **Historical silent gaps are immutable**: the corrected boundary-local
  read-only audit (M21.4.11-R2/R3/R5, cutoff `1786349202047196027`,
  inventory 161,817 manifests) found 4,680 unmarked reconnect boundaries
  (11 explicit backpressure gaps, 0 ambiguous) across all WebSocket streams
  since capture began. The earlier 4,680/11 figures from the
  manifest-flag-classified scanner are retained only as
  `SUPERSEDED_BY_CORRECTED_BOUNDARY_LOCAL_AUDIT`; the corrected rerun
  confirms the same totals with boundary-local evidence. Sealed
  Raw/Manifest evidence cannot be rewritten; additive remediation is
  designed but not executed (POST_MERGE_MIGRATION_REQUIRED=true).
- **Planned rotation is not an integrity exemption**: all five observed
  rotations (12h, 24h, 72h x3) and the post-window rotation carry no Raw gap
  evidence; only the forward fix changes this.
- **Observation collector timer parsing**: The 12h observation loop failed
  to save JSON observations because a timer-field parser mishandled
  `systemctl show` output ordering. Core continuity was independently
  verified. The 24h collector preserved raw text and used safe parsing;
  later windows must do the same.
- **Original 24h evidence engineering errors**: the original `run/journals`
  were exported with local-time bounds (8 h shift), and the original
  `run/report.md` cited three PRE_WINDOW backpressure events (gen2/3/4) as
  formal-window coverage. The first corrective review counted
  `STREAM_DISCONTINUITY` via journal string search (0/0), which is invalid
  because those events are Catalog-only (Catalog holds 7 complete
  STARTED/COMPLETED pairs). Re-export with epoch-derived UTC bounds
  (journald microsecond resolution; journal filtering is not
  nanosecond-exact) recovers the formal conclusions; original artifacts were
  preserved and the contract forensic review is the correcting record.
  "Formal 24h PASS" does not mean every original artifact was correct.
- **gen6 cross-window recovery**: gen6 backpressure started inside the
  formal 24h window but its timeout and complete recovery occurred after
  Target (POST_WINDOW). The formal window claims only the gen6 started
  event; post-window recovery is valid for current health judgment only.
- **Saturation budget semantics**: the 30 s backpressure budget is
  accumulated saturation time while the queue stays above low_watermark, and
  `IngressBackpressureTimeout` raises only when a later put again encounters
  a full queue. The gen6 started→timeout span (1358.9 s) is not a continuous
  full-queue span and the 30 s budget is not a strict 30 s wall-clock
  recovery ceiling. This remains a 72h/168h monitoring item.
- **queue recovered ≠ stream recovery completed**:
  `usdm_ingress_backpressure_recovered` means the queue fell below
  low_watermark; stream recovery completes only at new connection +
  first-new `sequence_gap` persisted + Raw sync + Catalog COMPLETED.
- **Internal zero-drop ≠ exchange-side completeness**: no Recorder-internal
  queue drop and CRC-verified persisted frames do not prove that events
  between WebSocket close and the first new connection frame were not
  missed. Reconnect boundaries therefore stay `sequence_gap`/`gap=true`/
  `complete=false`/`historical_continuity_restored=false`. Do not claim
  "zero data loss" or "historical continuity restored".
- **taker_buy_sell_volume_5m**: continues to fail (RuntimeError), not
  recovered, lifetime cumulative counter keeps increasing. Proven 24h-window
  traceability (lifetime cumulative value, not a window delta):
  `RUN_ROOT/corrective-integrity-review-20260806T152159Z/journals/recorder.log`
  holds a contiguous `usdm_side_rest_failed` series with
  `fields.failures` 88→232 (145 events, no gaps); first observed 88 at
  2026-08-05T15:10:37.615037Z (61 s after T0), last observed 232 at
  2026-08-06T15:02:28.961437Z (last failure event inside the window); delta
  within the exported span = 144. The exact value at T0 is not captured by
  that export (12h boundary value was 56). Corroborated by corrective review
  `report.md` and `review.json` ("cumulative 232 at window end").
- **Spot backpressure**: Spot streams have not received the same
  backpressure repair as USD-M. Their existing `put_nowait` overflow
  behavior (visible collector fault → `CoreMarketTerminalFailure`) remains.
- **Permanently hung kernel I/O**: Remains an uncancellable risk even
  under the M21.4.2 owned-worker cancellation protection.
- **CLI --version CWD contamination**: The Git commit suffix in
  `--version` output may change when the CLI is invoked from a repository
  directory. Production identity must use immutable artifact properties.
