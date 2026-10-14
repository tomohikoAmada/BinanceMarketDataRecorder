# R1 repair and new release qualification

Owner authorization October14,2026: finish R1; freeze/review/CI/wheel/deploy;
then qualify this release's own VPS 2h and 12h. Stop before24h/48h.

Candidate: `codex/recorder-r1`, base `9e8ec37`, isolated worktree
`/Users/amada/.codex/worktrees/recorder-r1/BinanceMarketDataRecorder`.
Current production is new8fa54d18/26c6c5c7/d804a02f/config5b73; old49b4/c7d/707 is retained.
Current checkpoint October14 18:51UTC: **R1_STOP_FIX_REVIEW_AND_EXACT_CI_ACCEPTED; AFFECTED_CLOUD_AND_SCOPED_RECOVERY_PENDING**. Production remains8fa54d18/wheel26c6c5c7/identityd804a02f/config5b73, RecorderinactiveANDdisabled/PID0. The failed source8fa run c1989145 reached7201.027916640s/strict26PASS but exceeded90s stop and was SIGKILLed, so it has no complete2h receipt or full saved-frame correctness denominator. Monitor238/240PASS remains separate. Its original targetb90a73d7/failuredd120e8b/restorationFAILED2a873bfa are preserved. Actual narrow cleanupbefc997b restored OS/archive16:11:32.412UTC/errors[] before guard cancellation16:11:32.470UTC; its window is CLOSED, never reused or extended.

The minimal two-market stop-after-successful-accept fix9744842 preserves the ready receipt, then honors graceful stop/session restart. Exact-source independent reviewa34f5c8d/89bb7cbd ACCEPT; eight real collector/owned-byte/Raw race cases and affected137testsPASS, fullLOCAL2119PASS/24onlineSKIP/5stressdeselected/Ruff/mypy306/M0/GoPASS, dualCI38597255154 BOTH SUCCESS. Subsequent source8f4e5dc70668207e07ed3d906b4f891eeba4085f changes only one positive worker-correctness test's10s cold-spawn budget to60s; all122 production Python files remain identical to974. Deterministic deadline/watchdog tests and production budgets are unchanged. Focused17PASS/Ruff and independent19744b65/74d1a32a ACCEPT; exact dualCI38615679477 BOTH SUCCESS18:36UTC, full log059ebce2. No new wheel/deployment/runtime credit yet.

Prior cloud48d8cac4 on974 FAILED with428PASS/one10s-budget expiration in test_raw_stream_statistics_and_worker; full log126bfb6b is retained. The worker returned the documented unfinished result; no data-corruption/source-algorithm failure is established, and exact cold-spawn scheduling cause is unproven. Frozen source8f archive98522724 and build-continuation805d4b31 are prepared for independent review and fresh exact physical bindings: rerun the affected17-case module, carry412 unchanged passing cases by complete source/log hashes, then build/install/verify a new locked wheel. This is429 distinct composite cases, not an assertion that the original failed run passed.

Exactly six failed-run partials/14,904,530B have independently accepted stable-byte custody2c3a0f3b/manifest5e5c630c; original bytes remain unchanged at this checkpoint. Two private sealing preflights failed safely before mutation (first at authentic multi-REST IDs, second at existing SEALING state), and both failed children/controls are preserved. Current catalog has four ACTIVE and two ordinary durable SEALING rows (523192/523193,14833/2158 verified frames); no staging files exist for those two, independently exported a3515be7. V3 helperac96c596/eight real-spool fixtures independently ACCEPT6e609ff9/b65d0d5b: preserve both ordinary SEALING authorities unchanged and use released sealing for the four ACTIVE tails with recovered_tail meaning operator-forced shutdown-tail uncertainty, not demonstrated truncation/recovery/network gaps. No fabricated RECOVERED event or zero-partial certificate. Exact child FAILED-PARTIAL-SEAL-V3-20261015-p8jvf2dc/copy0878301f/argv2cf26ee1 is prepared but UNRUN pending binding review.

Exact continuation is /private/var/tmp/bmdr-r1-stop-release-20261014-n7x57tez/CONTINUATION.md; see docs/reviews/2026-10-15-r1-stop-starvation-review.md. Next: accepted V3 singleton recovery/natural archive drain, accepted affected cloud/locked wheel, canonical stopped new release deployment/freshVERIFY, then that release's OWN new2h full selected LIVE/verification/independent restored receipt and separate OWN43200s12h. No8fa/old49b4 duration or identity transfer, no historical qualification/24h/48h/production-ready credit. Preserve all14/four products/Raw/archive/oldderived/failed evidence/venvs/worktrees/unrelated changes. Hourly continuationACTIVE.
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
  dirs/archive/helper copy readback verified; cloud native9c8b9877 completed
  its affected tests. Initial controller timeout did not stop native execution;
  final result is accepted in the following item.
- [x] Actual421cloudtestsPASS/1stressdeselected; new wheel26c6c5c7 and
  receipt64026b12 qualify122sourcefiles/28exactlocked dependencies. Native
 9c8b9877 SUCCESS03:02:22UTC; actual cloud independent ACCEPT4d8a40e5/87112839.
  Post-GC systemctl default properties are not original unit evidence.
- [x] Stopped deployment helper17176648 design ACCEPT27974421/da1cfe5f;
  nested command groups settle before rollback, unresolved cleanup blocks it.
  Actual Linux child-ready v3 cec2e7b5 tests3PASS; final native deploy and
  independent actual qualification are accepted in the following items.
- [x] Actual stopped canonical deploymentc98ce736 SUCCESS/result6e98b14d;
  freshidentityd804a02f VERIFIED, old venv/config/data/evidence retained,
  archive restored and Recorder stopped-disabled.
- [x] Independent actual stopped canonical deployment ACCEPTc478ca38/result1146baba; physical helper/release/oldvenv and prior707 identity custody verified. [Frozen actual review](../reviews/2026-10-14-r1-actual-deployment-review.md).
- [x] Fresh actual warm/strict-start/current admission and independent run-T0/native binding ACCEPTce939aa6/fc4dcf56. Final Catalog contents/membership and duration/data/restoration remain pending.
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


Retry2 DESIGN [ACCEPT5ebf8f9f](../reviews/2026-10-14-r1-owned-swap-retry2-design-review.md), independent24 exact boundary cases. Physical18members/canonicalLinux15+23+17 [ACCEPTe8986b6a](../reviews/2026-10-14-r1-owned-swap-retry2-copy-linux-review.md). Actualarmc98d1c54 success08:33:41UTC/newquiet542617c4/fixeddeadlineOct14 16:33:37UTC; post6f5da66b capacity/time pass/current62878counts and strict preserved. Actualbinding qualification pending, no capture/T0 yet; newown2h and12h remain pending.


Oct14 12:44UTC quota-resume actuale42d797d confirms retry2 never launched capture/T0 and remaining13729s<required20849.633. Actual closeout627240c2 restored OS/archive12:45:25UTC/errors[] before guard cancellation; oldwindow CLOSED. Fresh exclusive retry3ecaadc0d8dca is only scope/unit rebinding, counts62878 unchanged, production/controller unchanged. Current preflight9f40f0f3 capacity/time PASS; new exactbinding/copy/nativearm/warm/T0 qualification pending. No new2h/12h credit.


Current retry3 exact path/unit binding [ACCEPTd82acb62](../reviews/2026-10-14-r1-retry3-binding-review.md), actual18copy/arm/currentwindow/canonical/reserves [ACCEPTe62b6fad](../reviews/2026-10-14-r1-retry3-actual-arm-review.md). Fixed window20:52:20UTC, neverextended. Unique nativecapture92ed25ba launched12:57:30UTC; actual0f892f23 shows Typeexec/Runtime10080/Stop1500, launch-admissioncdc99136 and capture-launch479d6573. It owns newmetadata boundary/warm/actualstrict and then own7200T0; launch is expresslyNOT_YET_T0. Reasonablewarm/startcheckpoint13:12:30UTC; new2h actualduration/data/finalindependent-restored receipt and subsequent new12h remainpending. No extraVPS checks/duplicate healthywork.


Actual owner-startaa8cf54a establishes only this8fa/26c6/d804 release ownT0, native92ed25ba/RECfdbef6c9 sameincarnation. Fullmetadata4d88d15f: warm684.547s/strict26/332.6506s endpoints/currentandpeakswap0, postnativeprestart1a3a6a85 currenttime/capacity PASS. Formal95/95PASSprefix at13:56 is partialavailability, notdata correctness orfull2h. Complete target/fullselecteddualLIVE/completedverify/independentrestored receipt and new12h remainpending. Native self progresses, no extraVPS checks beforeknown15:09:12UTC target.


[Limited actual startup independent ACCEPTce939aa6/fc4dcf56](../reviews/2026-10-14-r1-retry3-actual-start-review.md): all8 closed control text SHA and95 immutable prefix rows rebuilt;119 warm/end/start/prefix observations keep sameinv/PID/cgroup/currentandpeakswap0, no clock violation.95 fixedslots/no skips onlypartialavailability. Exact24 warm/end/start RSSmax285253632B/availablemin4723032064B;95prefix RSSmax326103040B/availablemin4498997248B. OriginalstartCatalog3a209c1c onlydeclared remote metadata atthisphase; final fullmetadata export binds its bytes/selection membership. Actualown7200 target15:09:12.641778065UTC, finalduration/data/restored/12h remainpending.


## Shutdown failure and affected correction, October14

- [x] Original8fa target7201.027916640s/strict26PASS retained;238/240 monitor is separate. Automatic stopping failed after90s; terminal/full selected audit never started. No saved-data denominator/complete2h acceptance.
- [x] Exact frozen-source stop-starvation diagnosis and minimal correction DESIGN ACCEPT (reportee7c198a/result902e273e); actual implementation independently accepted, final frozen report pending.
- [x] Eight real collector/Raw stop races plus affected137 tests PASS; Ruff/M0/Go PASS, mypy306PASS after a test bytes annotation; full LOCAL2119PASS/24onlineSKIP/5stressdeselected181.44s.
- [x] Scope-only failed bookkeeping settled without starting Recorder/data changes; actualRESTORED16:11:32.412UTC before guardcancel16:11:32.470UTC, original failures retained. Cleanup is not full restored qualification.
- [x] Exact six current partials actual custody14904530B/2c3a0f3b/manifest5e5c630c completed, independent reconstruction accepts physical proof; originals retained.
- [x] Minimal source9744842 independentlyACCEPT a34f5c8d/89bb7cbd; exactdualCI38597255154 BOTH SUCCESS.
- [ ] Execute only reviewed six-tail incomplete sealing, natural archive drain; new source cloud/wheel/stopped deployment and own2h then12h remain pending.
- [ ] Freeze new correction source/independent review/exact dualCI/new locked wheel/affected cloud/stopped deployment, then OWN new2h and12h full acceptance; no source/time transfer.
