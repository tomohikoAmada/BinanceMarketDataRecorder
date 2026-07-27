# Binance Market Data Recorder 0.1.0a1

Status: Mac Developer Preview / Ubuntu ARM64 RK3588 Soak Candidate

Binance Market Data Recorder is an independent, unofficial project. It is not
affiliated with, maintained by, sponsored by, or endorsed by Binance. It uses
only unsigned public market-data endpoints and has no API-key, account, order,
strategy, backtest, or trading interface.

连续72小时和168小时长期运行验收尚未执行。
静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。
当前版本为Mac Developer Preview;Ubuntu ARM64/RK3588为Developer Preview / Soak Candidate;不得用于真实资金交易。

## Included

- Python 3.12 wheel and source distribution with SHA-256 manifest
- `binance-market-recorder` CLI and source-revision reporting
- BTCUSDT Spot and USD-M public Raw capture and depth bootstrap
- immutable Raw, manifests, Catalog, crash recovery, normalization, and replay
- registered-directory external archive with full readback verification
- rootless logged-in-user macOS LaunchAgent lifecycle
- non-root Ubuntu ARM64 systemd lifecycle and unified proxy transport policy
- structured status, daily reports, space alerts, and storage forecasts

## Acceptance boundary

M17 short-term functionality, physical external-disk interruption/recovery,
fault injection, complete offline tests, strict typing, lint, contracts, and
the independent Go Raw golden verifier passed. M18 adds clean-wheel install,
LaunchAgent uninstall/data-retention and reinstall/Catalog-read verification,
plus a 5–15-minute public Spot/USD-M smoke.

The Ubuntu M20 port has only short-term evidence; 72-hour/168-hour and Linux
blue/green certification remain M21. This release makes no
production-readiness, stability, or trading-readiness
claim. No remote release or package publication is performed by this
milestone.

## Artifacts

Exact artifact names, sizes, and SHA-256 values are recorded in
`manifest.json`. Build inputs are the source tree at the M18 release commit;
the final local commit SHA is reported in the M18 acceptance record.
