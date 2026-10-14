# R1 repair and new release qualification

Owner authorization October14,2026: finish R1; freeze/review/CI/wheel/deploy;
then qualify this release's own VPS 2h and 12h. Stop before24h/48h.

Candidate: `codex/recorder-r1`, base `9e8ec37`, isolated worktree
`/Users/amada/.codex/worktrees/recorder-r1/BinanceMarketDataRecorder`.
Current production is new8fa54d18/26c6c5c7/d804a02f/config5b73; old49b4/c7d/707 is retained.
Recorder inactive AND disabled. R1 wheel/cloud and stopped canonical deployment are independently accepted. First new2h native warm failed before T0 on whole-host swap counter change; its finite window was actually restored and closed. A fresh exclusive retry with Recorder-owned current/peak swap admission is prepared for independent review. New2h and new12h remain pending.
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
- [x] Exact frozen source8fa54d18 dualCI38502452866 BOTH SUCCESS.
- [x] Build helper independentDESIGN ACCEPTaabf9b3c/3a734882; actual exclusive
  dirs/archive/helper copy readback verified and unique cloud native9c8b9877
  is advancing affected tests. Its controller timeout did not stop the native;
  do not repeat. Result checkpointOctober14 03:10UTC.
- [x] Actual421cloudtestsPASS/1stressdeselected; new wheel26c6c5c7 and
  receipt64026b12 qualify122sourcefiles/28exactlocked dependencies. Native
 9c8b9877 SUCCESS03:02:22UTC; actual cloud independent ACCEPT4d8a40e5/87112839.
  Post-GC systemctl default properties are not original unit evidence.
- [x] Stopped deployment helper17176648 design ACCEPT27974421/da1cfe5f;
  nested command groups settle before rollback, unresolved cleanup blocks it.
  Actual Linux child-ready v3 cec2e7b5 tests3PASS; native canonical deploy
  c98ce736/429343 is running, actual result/independent verificationPENDING.
- [x] Actual stopped canonical deploymentc98ce736 SUCCESS/result6e98b14d;
  freshidentityd804a02f VERIFIED, old venv/config/data/evidence retained,
  archive restored and Recorder stopped-disabled.
- [x] Independent actual stopped canonical deployment ACCEPTc478ca38/result1146baba; physical helper/release/oldvenv and prior707 identity custody verified. [Frozen actual review](../reviews/2026-10-14-r1-actual-deployment-review.md).
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

New2h control/design qualification ACCEPT3cbcb9ad/result9a5aecbd: [frozen review](../reviews/2026-10-14-r1-owner2h-design-review.md). Exact v2 outer armStart2400/Stop1500, own7200/fixed28800, current selected-only full audits. Linux installed-wheel38mock+4exactentry+actual nestedspawn twoRawpasses succeeded. Fresh forecast reserve10795451347B>10GiB; only the completed R1 cloud generatedpipcache112files/71815559B was fully retained on archive after copy/source readback, sourcepath kept as symlink. No production Raw/manifest/venv moved. Actual singleton armc2341568 started07:29:07UTC; this grants no window/T0/duration/data credit. Current scopeR1-owner2h-20261014-e56ed32114ca; subsequent actual arm/admission and capture warm/T0 remain pending.

Actual preparation correction: firstc2341568 stopped before anywindow/T0 because a kernel-only reboot notice had appeared. Its successful07:29 restoration/failure are retained. Pending147+linux-base remains a notice; currentboot59bb/kernel146 unchanged. No OS reboot/marker deletion/settings/production release change. Exact f9cc/fa216 entry and LOCAL+Linux22 boundary cases independentlyACCEPT93847549/bf8c6c0b ([entry review](../reviews/2026-10-14-r1-kernel-notice-entry-review.md)).

Newactualarm19b4e58d SUCCESS07:50:09UTC: fixed28800 expiry15:50:03UTC, quiet5a98770c/canonicalde1e1c37/post8068411e. Actualremaining28793.4969>required20849.6328s/margin7943.8641; writerpeak10793247699B>10GiB. Independent actualarm/window/admission ACCEPTdcb7c0cf/040166a1 ([arm review](../reviews/2026-10-14-r1-actual-arm-review.md)); old07:29 restore is not this window's closure. Newcapture63352f72/PID443263 launched08:01:57UTC; full ownmetadata boundary/warm/currentstrict/prestart runs natively. Startup is not T0; actualown7200/data/finalhandoff still pending. Reasonablewarm/startcheckpoint08:15UTC. Do not duplicatearm/capture or restart healthyRecorder.


October14 08:15UTC actual warm closeout: original63352f72 failed08:11:35UTC, no owner-start/T0; blanket hostswap delta57pages veto was the immediate cause.19 observed Recorder current-swap values0/strictPASS7, lifetime peak unknown, no oldwarm PASS. Native actual restoration-before-cancel succeeded08:12:57; window CLOSED,178 newchunks preserved. Proposed retry2 exclusive8ae2482a37c4 preserves release/all14/fourproducts, uses mandatory Recorder current+peak0 with path/PID binding and original strict/resource/time/data bounds. Parent15+23+17 fixtures PASS; exact independent qualification/Linux affectedfixtures/copy/native current admission still pending. No newT0 or 12h started.
