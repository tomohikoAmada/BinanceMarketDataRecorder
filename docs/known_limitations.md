# Known Limitations

M21.4 was deployed and passed 2h and 12h formal stability windows with
independent evidence reviews. The 24h, 72h, and 168h windows remain
pending. Static review, unit tests, fault injection, and short online
tests cannot substitute for long-running proof.
当前版本为Mac Developer Preview;Ubuntu ARM64/RK3588为Developer Preview / Soak Candidate;不得用于真实资金交易。

## M21.4 validation status

- 2h preflight: PASS (independent evidence review complete)
- 12h observation: PASS (EVIDENCE_INTEGRITY_PASS_WITH_LIMITATIONS)
- 24h window: PENDING
- 72h window: PENDING
- 168h window: PENDING
- Planned rotation: verified once in 12h window; repeated cycles not proven
- Backpressure recovery: not naturally exercised in production
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

- Repeated 24-hour Binance connection rotation has been verified once in the
  M21.4 12h window (at 06:52-06:53 UTC). Multiple repeated rotation cycles
  over 72h/168h have not been demonstrated.
- Long-term memory, file-descriptor, bounded-queue, archive-backlog, growth
  forecast, and resource-leak behavior has been validated in 2h and 12h
  windows only; 24h/72h/168h remain pending.
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
  verified. The 24h collector must preserve raw text and use safe parsing.
- **taker_buy_sell_volume_5m**: Cumulative failures=56 (RuntimeError),
  not recovered. This side-data stream continues to accumulate errors
  across process lifetime and requires monitoring.
- **Spot backpressure**: Spot streams have not received the same
  backpressure repair as USD-M. Their existing `put_nowait` overflow
  behavior (visible collector fault → `CoreMarketTerminalFailure`) remains.
- **Permanently hung kernel I/O**: Remains an uncancellable risk even
  under the M21.4.2 owned-worker cancellation protection.
- **CLI --version CWD contamination**: The Git commit suffix in
  `--version` output may change when the CLI is invoked from a repository
  directory. Production identity must use immutable artifact properties.
