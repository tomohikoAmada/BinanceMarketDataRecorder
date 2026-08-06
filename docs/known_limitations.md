# Known Limitations

连续72小时和168小时长期运行验收尚未执行。
静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。
当前版本为Mac Developer Preview;Ubuntu ARM64/RK3588为Developer Preview / Soak Candidate;不得用于真实资金交易。

M21.4 was deployed and passed 2h, 12h, and 24h formal stability windows.
The 24h PASS was confirmed by a corrective evidence review and a Backpressure
contract forensic review; the natural gen5 backpressure recovery contract
passed inside the formal window. The 72h and 168h windows remain pending
and have not started. Static review, unit tests, fault injection, and short
online tests cannot substitute for long-running proof.

## M21.4 validation status

- 2h preflight: PASS (independent evidence review complete)
- 12h observation: PASS (EVIDENCE_INTEGRITY_PASS_WITH_LIMITATIONS)
- 24h formal window: PASS (corrective + contract forensic review confirmed;
  eligible_for_72h=true)
- 72h window: PENDING (not started)
- 168h window: PENDING (not started)
- Planned rotation: verified in 12h and 24h windows; repeated cycles not proven
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
  Multiple repeated rotation cycles over 72h/168h have not been demonstrated.
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
  STARTED/COMPLETED pairs). Epoch-bounded re-export recovers the formal
  conclusions; original artifacts were preserved and the contract forensic
  review is the correcting record. "Formal 24h PASS" does not mean every
  original artifact was correct.
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
- **taker_buy_sell_volume_5m**: continues to fail (RuntimeError,
  cumulative failures grew past 60 during the 24h window; corrective review
  recorded cumulative 232 by window end), not recovered. This side-data
  stream continues to accumulate errors and requires monitoring.
- **Spot backpressure**: Spot streams have not received the same
  backpressure repair as USD-M. Their existing `put_nowait` overflow
  behavior (visible collector fault → `CoreMarketTerminalFailure`) remains.
- **Permanently hung kernel I/O**: Remains an uncancellable risk even
  under the M21.4.2 owned-worker cancellation protection.
- **CLI --version CWD contamination**: The Git commit suffix in
  `--version` output may change when the CLI is invoked from a repository
  directory. Production identity must use immutable artifact properties.
