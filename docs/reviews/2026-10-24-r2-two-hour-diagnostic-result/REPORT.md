# Independent completed new two-hour diagnostic reconstruction

Decision: **ACCEPT_NEW_TWO_HOUR_DIAGNOSTIC_RECONSTRUCTION_ONLY**. No blocking discrepancy in the supplied original evidence or normal completion. The result establishes this finite diagnostic's publications, policy classifications, resources, event history and stopped handoff. It proves no transport root cause or runtime repair and grants zero formal duration, saved-frame correctness, capacity,24h or production-ready credit.

Review is LOCAL/OFFLINE only. No SSH, current VPS read, operator/candidate execution, source/design/CI/QA repetition, Raw inspection or saved-data audit occurred. Existing new-source/release/design/initial-binding acceptances are reused through exact references. Original metadata is the authority; parent conclusions and derived summaries are not substituted for it. All times below are UTC on October24.

## Exact evidence and trust reconstruction

Original `actual-diagnostic-read-1792814703709257000.json`:65,762,036B, SHA256 `e805efd95e84a01d63fd1552a4db1d4bb93fd55009c7c6ae9f97c390a6d99d05`. All9 original members match their reported SHA256; the8 base64 members also match the original text bytes. The62,713,141B observations file SHA256 is `0fa29c058cdcae89281b2a0593356ab22f1c3a9dfba0d006582b9b2ac0df7160`. All1,413 embedded publication base64 payloads match their per-row SHA256 and1MiB limit, parse successfully and have nondecreasing fresh heartbeat values (maximum age5.430292965s).

Every sample binds sole Recorder invocation `0c6e489acbc449968de4d7ea42ad0e80`, PID818054, active/running, disabled and NRestarts0, with the same process start timestamp/service instance. All publications bind source `20e57f30682e88b4090cc2f0da34aa26fca4de2e`, wheel `dd2cc06fd182001588b00e03f3407b94bbd648e44b14436d8933dd957e0bcb2c`, identity `9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715` and config `5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419`. These preserve the accepted all14-feature/four BTC/ETH Spot+USD-M configuration. This is exported controller/publication trust evidence, not an independent later remote configfile rehash.

All26 expected auxiliary contexts in every sample equal their original publication values. The official and strict gate context copies are identical:36,738 raw contexts/73,476 stored paired entries verified, including partial empty startup values. Context equality, counters and policy differences were reconstructed without rerunning source qualification tests.

## Readiness and paired classifications

Official gate:1,350 PASS/63 BLOCK. Strict gate:1,328 PASS/85 BLOCK. Paired counts:1,328 PASS/PASS,63 BLOCK/BLOCK,22 official-PASS/strict-BLOCK. No paired result is omitted or substituted.

| Rows | Samples | Original state and classification |
|---|---:|---|
| 1–59 | 59 | STARTING, empty product maps, write_safety_verified=false; both BLOCK |
| 60–61 | 2 | RUNNING, initial core not ready; both BLOCK |
| 62–120 | 59 | Core ready; both PASS |
| 121–140 | 20 | Official PASS/strict BLOCK for taker-volume empty-response recovery; both symbols rows121–139, BTC only row140 |
| 141–198 | 58 | Both PASS |
| 199–200 | 2 | Official PASS/strict BLOCK: ETH then BTC taker-volume empty-response recovery |
| 201–636 | 436 | Both PASS, including a BTC premium REST warning recovered between publications |
| 637 | 1 | Both BLOCK: ETH premium_index_snapshot RETRYING |
| 638–1065 | 428 | Both PASS |
| 1066 | 1 | Both BLOCK: ETH open_interest RETRYING |
| 1067–1413 | 347 | Both PASS through final sample |

First sample01:16:33.091884622; last03:16:21.809849971. Initial core readiness first appears at row62,01:21:43.653444756, and remains true in all1,352 subsequent samples; four products remain ready with the expected core streams connected. Rows1–58 have startup_recovery_complete=false. Row59 already has that flag true while still STARTING with empty product maps and write-safety false; it remains an honest BLOCK. No formal T0 is invented at first readiness, and the whole2h is not called healthy ready operation.

After core readiness, the stored official count is1,350 PASS/2 BLOCK and strict1,328 PASS/24 BLOCK. These are diagnostic5s-loop sample counts, not the formal N2 grid, full24h availability denominator, exact downtime or saved-frame correctness.1,413 actual rows are not described as1,440 fixed5s slots with missing frames; sampling includes work/fsync and sleeps.

## Complete event and recovery evidence

The owned journal has64 events, all bound to the same boot59bb/Recorder invocation/PID:16 initial WebSocket connections (6Spot/10USD-M),5 initial snapshot-bridge warnings (3Spot/2USD-M),42 auxiliary REST warnings,1 SIGTERM stop request.16 distinct connection IDs cover each expected market/symbol/stream tuple once, between01:21:33.521376 and01:21:34.936521.

There is **zero unexpected WebSocket disconnect/reconnect/ConnectionClosed/WebSocket error-close event in this supplied bounded owned journal**. This is not proof of every Binance or network event, future stability or absence of all possible transport faults. No real unexpected WebSocket close exercised the added diagnostic fields. Its root cause remains unreproduced/unobserved.

REST warnings total42, not3:39 `EmptySideDataResponse` warnings on taker_buy_sell_volume_5m (BTC20/ETH19), plus3 `NetworkError`. Each warning group's cumulative failure sequence and final publication counter match; no counters are reset or hidden. All affected contexts are RUNNING with consecutive_failures0 by the final publication.

The39 empty-response warnings occur01:26:37.554762–01:33:20.418464. They produce four sampled retry episodes across two symbols: BTC rows121–140 and200; ETH121–139 and199. Reported recovery successes are BTC01:28:18.674694954/01:33:25.644211982 and ETH01:28:16.996413097/01:33:22.999242124. There are22 distinct discordant sampled rows because symbol episodes overlap;39 warnings, four sampled context episodes and22 blocked strict rows are different counts. Accepted/cursor state remains present during recovery. These classifications are an existing explicit allow-empty-recovery policy difference: same context values, official allow flag true, strict false. They do not prove a production source defect, validate any response payload, or authorize changing criteria after the fact.

| Network warning | Captured effect | Reported later success |
|---|---|---|
| BTC premium_index_snapshot02:06:34.168590 | No captured RETRYING row/gate BLOCK; failure counter first seen at row592, already RUNNING |02:06:39.192723134 |
| ETH premium_index_snapshot02:10:25.857789 | Row637 BLOCK/BLOCK; row638 recovered PASS/PASS |02:10:30.877145591 |
| ETH open_interest02:46:45.381019 | Row1066 BLOCK/BLOCK; row1067 recovered PASS/PASS |02:46:50.591122429 |

Warning-to-later-success timestamp differences are5.024133134/5.019356591/5.210103429s. They are not full failed-request or outage durations: request initiation and between-sample path are absent. The latter success timestamps precede the captured RETRYING read because publications are asynchronous. The BTC warning with no sampled block directly demonstrates why event counts and sampled availability differ. Error labels carry no HTTP status, lower transport cause, request/response bytes or proven network origin. None identifies the earlier WebSocket closure cause or proves that a runtime repair is needed in a particular source path.

## Resources and timing

All1,413 samples have owned cgroup current/peak swap0. Maximum Recorder RSS390,922,240B (after readiness363,081,728B), minimum host available4,621,594,624B, writer free24,335,216,640B, archive free2,051,254,026,240B. All observed hard memory/free-space bounds pass. Existing host swap30,437,376B remains constant and is separate from owned swap. Preserved PSI maximum avg10 values: CPU-some10.21%, IO-some19.10%/full13.71%, memory-some/full0.29%. Cgroup CPU grows7,797.204102s across the sampled interval; it is aggregate CPU accounting, not a saturation diagnosis.

Around all3 NetworkErrors, owned swap stays0 and no sampled hard resource bound crosses. For the ETH premium blocked row, RSS349,757,440B, available4,894,498,816B, CPU PSI-some3.19%, IO-some1.95%/full1.54%, memory PSI0. For the ETH OI blocked row, RSS357,400,576B, available4,750,589,952B, CPU-some5.67%, IO-some1.17%/full0.40%, memory PSI0. These limited sampled facts do not establish or rule out a network/host timing cause; full resource neighborhoods are frozen.

Start request01:16:22.614558469 binds BOOTTIME1869476795087410ns and absolute cleanup deadline1880276795087410ns: exactly10,800s, with duration7,200s. Independent BOOTTIME differences: request→first sample10.483402063s; request→last sample7,199.198491977s; sample span7,188.715089914s; post-readiness sample span6,878.150955438s; adjacent gaps5.019152371–6.400555912s. The last sample has3,600.801508023s remaining to the fixed deadline.

The controller reports7,204.608260326s from its private BOOTTIME observation start counter. The raw initial private counter is not separately exported, so the exact reported elapsed value cannot be independently recomputed from the earlier start-request counter. The request counter, process/start binding, sample interval, termination and normal completion independently support the finite completed run, while the original reported value remains honestly labeled. Cleanup records7,226.249029977s on MONOTONIC since request; its exact BOOTTIME is not separately exported. Same-boot lease admission and these consistent values support timely cleanup without an extension.

Original initial native binding27270710 preserves executed oneshot/Start8400s/Stop900s/UMask0027 and exact pin/canonical observer/cleanup commands. The later not-found transient's90s/UMask0022 defaults do not characterize that executed native invocation.

## Normal completion and current stopped handoff

Native invocation `e33cf116c1e843b5b5e9d7988e71cdf8` is bound to the original exact launch and owned native messages. Diagnostic result `81918b6429ef313c731f5b9051c953fd0f1b4683a8a58063cc9ad2dbaaebd4ff` equals its journal message. Recorder receives SIGTERM03:16:29.501303. Cleanup `357cd17050a4b7835b7e2ae0587081c5184c064f99deec1825a5be69750f96d5` at03:16:48.863586663 records Recorder inactive/dead AND disabled/PID0/NRestarts0, backlog[0,0], enabled-active timer and natural archive worker `25b584bbb691490f9b361224222fcb1c` inactive/dead/PID0/Resultsuccess. Its next finite monotonic timer deadline is41.491381323s later.

ExecStopPost records CLEANUP_ALREADY_COMPLETE_NO_ACTION and preserves exactly the same completed marker. Owned manager messages record Deactivated successfully03:16:49.856346, then Finished/JOB_RESULTdone03:16:49.859572. Normal native completion is established by those invocation-bound execution records. Post-GC not-found/default fields and a later missing transient file message are retained, not mislabeled as execution failure or effective settings.

The one actual metadata read at04:05:06.890837589 finds Recorder inactive/disabled/PID0/NRestarts0. A DIFFERENT normal archive invocation `495ba4f802524a59ae3bee43bd06d7ff` is also inactive/PID0/Resultsuccess; its exported ExecStart records codeexited/status0 and stop04:05:03. It is a later completed normal worker, not the cleanup worker and not the active worker from another earlier scope. Timer is active/waiting/enabled. Current backlog is not exported; backlog0 is specifically the cleanup observation. No new read or operational action was performed by this review.

## Recommended next decision

Close this finite diagnostic as **WS_NOT_REPRODUCED**, preserving all42 REST warnings and both policy classifications. No blocking source defect is demonstrated; no repair should be invented merely to satisfy sequence labels. No source behavior or threshold change follows from these exception labels. If proof of a runtime repair is a mandatory prerequisite elsewhere, this diagnostic does not supply it.

Avoid an indefinite chain of short observations waiting for a rare close. Given the separately established owner objective to measure N2 on the qualified instrumented20e57 release, **one fresh full N2 can instead be a reasoned measurement**: ask whether this exact new source/identity meets unchanged predeclared availability and saved-data criteria, and preserve the richer original evidence if a close occurs. This is a measurement with an honestly possible failed outcome, not an expectation of improved stability from logging changes or acceptance of a completed24h stage.

Before such an operation: independently review its unique source/identity/scope binding, finite window/cleanup, actual fresh admission and capacity/audit/restoration controls; preserve the existing governing monitor policy, full saved-frame correctness/audit requirement and paired strict diagnostics. Grant no transferred duration, previous-source pass or repair credit. Do not change thresholds after the result, auto-repeat a failed N2 or automatically advance N4/N8. This completed2h review approves no launch or new eligibility; it supports preparation of that bounded measurement design rather than more open-ended2h diagnostics.

All original9 members, complete export and journals, per-publication/context/resource hashes, recovery neighborhoods, counters and executable reconstruction assertions are frozen. The local reconstruction assertion was corrected for the actual row59 transition (recovery flag true while still STARTING); no original evidence, candidate source or policy was changed. Prior source/QA/design and old diagnostics were not rerun or reanalyzed.
