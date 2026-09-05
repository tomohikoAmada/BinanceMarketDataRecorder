# Known Limitations

Current GitHub `main` is `d38180074b5f76ab6b7778eea7fc505160c671ae` (tree
`95f16f05b30b7db23e43ebb6439ed0d055081902`), with MS1 merged through PR #51 and
post-merge CI run `33955915046` passed on macOS and Ubuntu. It is not deployed.
The last independently qualified deployed artifact is pre-MS1 source
`c421605e302d2ad46acdb2466627f64644181c9a`; its clean 24-hour non-formal stage
is complete and remains artifact-specific. No duration credit transfers to
current main. Formal M22.9 has not started and Production Ready is NO.
Current authority is in
[`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md) and
[`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md).

### Current new-host limitations and decision boundary

The current GreenCloud ordinary KVM service has shared CPU cores except VDS;
the official [Terms of Service](https://greencloudvps.com/terms-of-service.php),
checked 2026-08-31, states that average CPU usage should not exceed 30% and
permits bursts to 100% for 10 minutes every 24 hours. The operator observed
approximately 20% in the provider panel during recent Recorder operation.
This is an **OPERATOR / PROVIDER-PANEL OBSERVATION**, not a normalized Recorder
CPU benchmark. This historical capacity watch does not authorize or schedule
another burn-in.

The current next phase is MS2 fixed seven-symbol runtime fan-out, not profiling.
After verifying live main, receive explicit authorization before implementation.
Provider-panel CPU% alone does not select an optimization. Any future CPU
changes must preserve Raw v1, exact
payload bytes, receive timestamps, canonical CBOR, CRC32C, SHA-256, bounded
ingress, durability/fsync, seal/manifest/Catalog ordering, reconnect/
discontinuity and gap/resync evidence, crash/recovery, deterministic replay,
and Spot/USD-M sequence semantics. Longer fsync intervals, CRC/SHA removal,
float substitution, silent metrics/gap deletion, and unreviewed stream merging
are not authorized.

The RSS trend remains **WATCH / NOT YET PROVEN LEAK**. Frozen maxima were
approximately `244400128` bytes at 2h, `260460544` at 4h, and `286740480` at
12h, with no swap, OOM, systemd restart, or clear resource exhaustion. Keep
observing RSS during profiling and new-artifact validation.

The clean-24h deployed artifact identity is recorded in
[`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md). It is pre-MS1 and
must not be represented as current main or as MS1 live qualification.

The M21/M22.9 narrative below is historical incident evidence. Claims within
that narrative that a correction was local-only or undeployed describe that
historical cut, not the current candidate. Static review, unit tests, fault
injection, and short online tests still do not substitute for long-running
proof.

### Frozen historical long-run notice

The following notice is preserved verbatim because the Developer Preview
documentation contract requires the same historical cut on all living
surfaces. Its words such as “当前”, “尚未部署”, and its old staged-validation
requirements describe that historical cut only. They are not current project or
runtime authority.

```text
原始M21.4正式72小时窗口的进程稳定性PASS，但reconnect-boundary数据完整性合同FAIL；随后部署的M21.4.11工件`f659895…`已通过独立正式72小时观测门。
该工件随后因restart-only orphan-intent缺陷被判定`ELIGIBLE_FOR_168H=false`，因此168小时验收未运行。
PR #11的进一步修复后来进入M22.9 incident artifact；当前本地continuity修复
尚未部署，新的修复工件必须从2h→12h→24h→72h→168h重新开始验收。
M22.9 exact-VPS 24小时阶段结果为INCOMPLETE；已确认 fatal post-close
handoff 路径会遗漏持久 gap 证据。修复仅在本地完成、尚未部署；72小时不具备资格。
静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。
当前版本为Mac Developer Preview;Ubuntu ARM64/RK3588为Developer Preview / Soak Candidate;不得用于真实资金交易。
```

The frozen notice above does not override the current authority in
[`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md) and
[`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md).

> **原始 M21.4 正式72小时窗口结果为 FAIL（数据完整性合同失败）**：
> 该历史窗口的核心进程稳定性 PASS，但
> 2026-08-07T14:08:24Z USD-M `book_ticker` 意外断线及 planned rotation
> 边界缺少 gap 证据。
> 随后部署的 M21.4.11 工件 `f659895…` 已通过独立正式72小时观测门
> （PASS），但之后发现 restart-only orphan-intent 缺陷，
> `ELIGIBLE_FOR_168H=false`，因此168小时验收未运行。
> PR #11 的进一步修复已合并到 `main`，但尚未部署。
> 原始72小时失败记录见
> `docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md`。

The original M21.4 formal 72h window FAILED on data integrity while core
process stability passed: the 2026-08-07T14:08:24Z USD-M `book_ticker`
unexpected disconnect and planned rotations lacked required gap evidence.

A later deployed M21.4.11 artifact (`f659895…`) passed its independent formal
72h observational gate, but a restart-only orphan-intent defect was
subsequently discovered, making `ELIGIBLE_FOR_168H=false`; the 168h window did
not run.

The correction merged through PR #11 was later included in the historical
M22.9 incident artifact. The separate startup-liveness technical candidate at
`9c1df233…` is NOT DEPLOYED, and a newly built/deployed artifact must restart
the full staged validation chain.

## Historical M22.9 exact-VPS continuity incident status

- `M22_9_24H_RESULT=INCOMPLETE`
- `ELIGIBLE_FOR_72H=NO`
- `PRODUCTION_READY=NO`
- Historical incident final state: STOPPED / NOT CAPTURING; `MainPID=0`, zero production
  writers, and zero active partials after rollback.
- Root cause: a healthy approximately 17m50s startup recovery had only a
  30-second stale-heartbeat allowance; SIGTERM also left the `to_thread`
  recovery worker outstanding until systemd SIGKILL.
- Technical candidate `9c1df233…` keeps one heartbeat active, preserves
  not-ready STARTING, retains full validation for unstable states, adds
  cooperative stop during `recover_storage()`, and prevents startup promotion
  after a stop during the later capacity observation. Its fresh targeted
  re-review closed the P1 with no P1 findings. It is not built, deployed, or
  acceptance-tested.
- P2 (nonblocking): a missing or size-mismatched already-stable local `SEALED`
  artifact becomes `reconcile_failed` rather than forcing startup failure.
  This is pre-existing post-commit external-loss/filesystem-corruption
  behavior, not a crash-recovery-authority defect, and is not promoted to P1.
- The incident artifact allowed fatal USD-M post-close handoff timeout to
  escape before durable gap intent, producing false-complete historical tails
  and an unmarked first post-restart frame.
- The local corrections preserve queue bounds and fail-fast behavior, mark
  the unpersisted boundary honestly, distinguish session restart from true
  global stop, reset ephemeral boundary state per generation, and restore the
  same gap lifecycle on startup. They are not deployed and have no duration
  credit.
- A future corrected artifact must restart exact identity -> readiness -> 2h
  -> 12h -> 24h -> 72h -> 168h. No stage starts automatically.

Static review, unit tests, fault injection, and short online tests cannot
substitute for long-running proof.

## M21.4 validation status

- 2h preflight: PASS (short-window process stability; independent review)
- 12h observation: PROCESS-STABILITY PASS; data-integrity
  SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING
- 24h formal window: PROCESS-STABILITY PASS (corrective + contract forensic
  review confirmed); data-integrity
  SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING
- **Original M21.4 72h formal window: FAIL** (core process stability PASS;
  data integrity FAIL; eligible_for_next_stage=false) — historical epoch
- **Later deployed M21.4.11 artifact `f659895…`: independent formal 72h
  observational gate PASS** (27/27 explicit WS transitions, +0 unmarked,
  0 false-complete, 27/27 first-new Raw `sequence_gap`); then found to contain
  a restart-only orphan-intent defect → ELIGIBLE_FOR_168H=false
- 168h window: NOT RUN (blocked by the restart-only orphan-intent defect on
  `f659895…`, not by the historical reconnect-boundary failure)
- Planned rotation: **FAIL on Raw gap evidence** — all five observed
  rotations (12h/24h/72h windows and post-window) seal with
  gap=false/complete=true and no Catalog gap
- Reconnect boundary integrity: corrected boundary-local read-only audit
  found 4,680 historical unmarked transitions (11 explicit, 0 ambiguous);
  forward fix merged to `main`, NOT DEPLOYED
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
  windows only; the later deployed artifact's 72h observational gate did not
  cover these resource/backlog metrics; 168h has not run.
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

Formal M22.9 and any later use decision still require the repository-owned full
staged chain on a capacity-complete environment. The next development action is
MS2 fixed seven-symbol fan-out after explicit authorization; deployment,
profiling, and burn-in remain separately gated.

### Current GitHub main and multi-symbol limitations

- Current main contains the MS1 durable identity foundation but is not deployed
  and has no duration credit from the pre-MS1 clean-24h artifact.
- The current runtime implementation is still single-symbol BTCUSDT assembly;
  MS2, MS3, and MS4 are not implemented.
- The frozen multi-symbol target is seven symbols across Spot and USD-M, or 14
  core product identities. The new team starts at MS2 after explicit
  authorization; no 72h/168h campaign is automatically scheduled.
- Production Ready remains NO and formal M22.9 is NOT_STARTED. Do not
  automatically authorize deployment, long burn-in, M23.5, or a C++/Go rewrite.
- The remaining nonblocking backlog is the repeated-cancellation supplemental
  test-strength P3, process-memory-only cooldown across restart, conservative
  typed/no-header 418 fallback, any genuinely unresolved documentation/source
  provenance item, RSS WATCH/not proven leak, open R-034, and unresolved formal
  capacity runway. These do not reopen the closed shared-gate P2 or remote-delete
  P1.

### Additional M21.4 known limitations

- **Ordinary reconnect and planned rotation seal without gap evidence
  (original M21.4 72h FAIL root cause; historical)**: the original deployed
  artifact reconnects unexpected_disconnect/planned_rotation/
  server_shutdown/session-restart boundaries in the same generation with no
  Catalog STARTED/COMPLETED, no `sequence_gap`, and manifest
  `gap=false/complete=true`. The formal 72h window FAILED on this contract
  (2026-08-07T14:08:24Z book_ticker disconnect; receive gap ~1.73 s; u jump
  56,294,564). The M21.4.11 forward fix (unified Reconnect Boundary state
  machine, manifest-level `reconnect_gap`, seal defense) was later deployed
  as `f659895…`, which passed its own 72h observational gate; the further
  orphan-intent correction later entered the M22.9 incident artifact. The
  additional R-054 continuity correction is included in the recorded older
  deployed source `e074d41a…`; longer non-formal and formal proof remains
  pending.
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

## Approved future architecture not yet implemented

- The primary production target is Ubuntu 24.04 LTS x86_64 on a shared 2 vCPU,
  4 GiB, 40 GB-class VPS. At the historical M22.9 incident cut, that candidate
  had been rolled back after readiness failure and the service was stopped.
  This is incident provenance, not current runtime authority; see
  [`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md) and
  [`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md) for current project state.
- The VPS live path and local Offline Workspace execution-role split is
  approved, but heavy offline profiles are not yet separated operationally.
- The local-client pull archive workflow, SSH transport seam, durable receipt,
  source-deletion authorization, Archive Set, and Catalog snapshot transfer are
  not implemented.
- The future archive client targets macOS, Linux, and Windows; platform client
  implementations/certifications do not yet exist.
- Notifications and Web UI remain future, separately authorized extensions.
