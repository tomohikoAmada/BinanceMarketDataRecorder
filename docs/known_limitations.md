# Known Limitations

连续72小时和168小时长期运行验收尚未执行。
静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。
当前版本仅为Mac Developer Preview，不得用于真实资金交易。

## M19

- R-034 remains open: official Global Spot bootstrap wording and the official
  toolbox/observed adjacent boundary disagree. The offline evaluator compares
  both targets; it is evidence, not an official resolution.
- Historical archives provide no local receive clock and no historical L2.
- USD-M statistics have only the official latest-month/latest-30-day window;
  missed polling periods may be unrecoverable.
- Live raw trades, live klines, L3 queue data, accounts, orders and trading are
  not implemented.

- Repeated 24-hour Binance connection rotation has not been demonstrated in a
  long-running acceptance window.
- Long-term memory, file-descriptor, bounded-queue, archive-backlog, growth
  forecast, and resource-leak behavior remains unvalidated.
- macOS sleep and closed lid interrupt user-session networking. The Recorder
  marks detected gaps but cannot recover events Binance no longer provides.
- The certified platform is macOS Apple Silicon with a logged-in-user
  LaunchAgent. Ubuntu and Windows are not implemented or certified.
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
part of the M0–M18 Developer Preview plan.
