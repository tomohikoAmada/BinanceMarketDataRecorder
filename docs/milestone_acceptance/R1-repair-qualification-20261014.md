# R1 repair and new release qualification

Owner authorization October14,2026: finish R1; freeze/review/CI/wheel/deploy;
then qualify this release's own VPS 2h and 12h. Stop before24h/48h.

Candidate: `codex/recorder-r1`, base `9e8ec37`, isolated worktree
`/Users/amada/.codex/worktrees/recorder-r1/BinanceMarketDataRecorder`.
Current production remains old49b4a2d/c7d354be/7072575b/config5b73,
Recorder inactive AND disabled. No new wheel/deployment/window/T0 exists.
Read-only VPS preflight October14 confirms original boot/config, natural
archive timer enabled-active and both disks available; no production mutation.

- [x] Correct USD-M core stream-specific E/T extraction; persist exact Raw.
- [x] Version Normalize as dataset v2; rederive valid legacy core times from
  payloads, preserve v1 builds and explicit v1 Replay compatibility.
- [x] Retain invalid legacy oversized timestamps as invalid/null-time rows,
  rather than aborting the entire build (independent review finding addressed).
- [x] Cap reconnect and snapshot FullJitter without evaluating an overflowing
  exponent;1025 and much larger failure counts tested.
- [x] Bound receipt-owned payload bytes across queue and in-flight writer
  batches; preserve count limits, stop/failure handling and durable Gap paths.
- [x] Parent full offline run2108PASS/24onlineSKIP/5stressdeselected; sole M0
  failure was supported worktree-root configuration, then1PASS with
  CI=true/M0_CONTRACT_ROOT. After the legacy fix, affected48testsPASS.
- [x] Ruff/mypy305files/M0 script/Go golden checks PASS. Real fragmented16MiB
  loopback transport and both-market byte-bounded slowwriter suite6PASS.
- [x] Independent final source correctness ACCEPT: report35cfcc1b/result03a9c275,
  source manifest55972918/input326dab05; guarded140/56/3/18PASS phases overlap.
  No network/production/history reads; no CI/deploy/live credit.
  [Frozen review](../reviews/2026-10-14-r1-source-review.md).
- [ ] Exact frozen source double-platform CI PASS; new Linux locked wheel and
  affected cloud tests/install/source correspondence PASS.
- [ ] Canonical stopped deployment and fresh identity verification accepted;
  old venv/config/data/evidence retained.
- [ ] Fresh2h own7200s, selected doubleLIVE/completed immutable verification,
  saved-frame conformance strictly>99.9%, independent restored closeout.
- [ ] Same release fresh12h own43200s and complete selected saved-data/
  independent restored closeout; update plan/state/handoff and pause hourly job.

Implementation and compatibility: [ADR-0044](../adr/0044-r1-times-backoff-owned-byte-budget.md).
Saved-frame conformance is separate from monitor availability and does not
prove every exchange event was captured. Retain all disclosed gaps and failures.
All14 auxiliary features/four products/growth/history/venvs are preserved.
Old Q7/Q8 acceptance grants no new-source duration. Q9/Q10 remain NOT_STARTED;
production-ready remains NO/R-078 OPEN. Native tests own their timing/audit/
restoration; healthy work must survive model quota/SSH interruptions and must
not be duplicated. Each new finite protection window is separately admitted
and never extended.

Release controls draft (not operational authority):
`/private/var/tmp/bmdr-r1-release-20261014-rioikh96`.
Hourly `vps-2h` is ACTIVE at the existing hourly frequency for this new request.
