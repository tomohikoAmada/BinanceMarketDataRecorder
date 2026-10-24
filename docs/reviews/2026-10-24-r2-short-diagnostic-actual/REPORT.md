# Independent actual 30-minute diagnostic reconstruction

Decision: **ACCEPT_BOUNDED_DIAGNOSTIC_RECONSTRUCTION_ONLY**. No blocking discrepancy was found in the supplied actual result, original publications, owned journals, or normal cleanup binding. This accepts a completed diagnostic observation and its preserved evidence. It grants zero formal duration, capacity, saved-frame correctness, stability-repair, 24-hour readiness, or production-ready credit.

Review was LOCAL/OFFLINE only. No SSH, production read, Raw, candidate control execution, source QA, build, copy, native launch, or historical audit was performed. Prior source/design/supply-chain/deployment acceptances are reused, not retested. All times below are UTC.

## Original bytes and identity

Original export `actual-diagnostic-read-1792803677663471000.json` is 14,662,799 bytes, SHA256 `8248c7dc897a9959a5544d120b58ebb9316ed977caf40899de3d6984127d93c5`. All nine embedded files match their original reported SHA256; eight original base64 members also equal their preserved UTF-8 text. The 13,942,825-byte observations file SHA256 is `d2e179dd7b90e956dacbcb48e6a1eb9f70cdedb4f886e1196839b985dc67cb1e`. All 352 publication base64 payloads match their per-row SHA256, are within the 1MiB cap, parse successfully, and have fresh, nondecreasing heartbeat values. Maximum observed heartbeat age is 5.388309443s.

All 352 runtime records bind Recorder invocation `8948116f38a8483c8438d42645299e72`, PID815493, active/running, NRestarts0, unit disabled, and the same process start timestamp. Every publication binds source `20e57f30682e88b4090cc2f0da34aa26fca4de2e`, wheel `dd2cc06fd182001588b00e03f3407b94bbd648e44b14436d8933dd957e0bcb2c`, identity `9b8ce8219105fd7962cbf4831a99c9764c3c2889088bda7e73c834ff6bffc715`, and config `5b73db1b6ba6ac8cc7ab84cbcf0c7b688187b316644bef3df44bbb42f9283419`. Copy/admission/request identity/config and boot agree; all supplied journals have the same boot59bb. These are exported admission/publication/controller trust facts, not a new independent rehash of the current remote configfile. Four BTC/ETH Spot/USD-M product maps appear after startup; all14 configuration features are reused from the accepted deployment, without a new config write.

## Complete sample segmentation

Both captured official and strict gates have identical outcomes and reasons: 264 PASS and 88 BLOCK. Counting these stored gates is reconstruction, not a repeat of qualification policy tests.

| Rows | Count | UTC first → last | Preserved meaning |
|---|---:|---|---|
| 1–79 | 79 BLOCK | Oct23 23:52:53.317281968 →23:59:28.870904875 | STARTING, startup recovery incomplete; empty product maps and write_safety_verified=false |
| 80–87 | 8 BLOCK | Oct23 23:59:33.946542283 →Oct24 00:00:09.727345173 | RUNNING, startup recovery complete but core not ready; initial auxiliary/cursor/snapshot readiness |
| 88–349 | 262 PASS | Oct24 00:00:14.835126895 →00:22:26.570425166 | RUNNING/core ready, gates PASS |
| 350 | 1 BLOCK | Oct24 00:22:31.630011908 | BTC USD-M premium_index_snapshot status RETRYING |
| 351–352 | 2 PASS | Oct24 00:22:36.790262817 →00:22:41.846673932 | Auxiliary recovered, core remains ready |

The first PASS is 451.937045122s after the start request. The post-readiness sample span is 1347.011547037s, with 265 samples (264 PASS/1 BLOCK). This segmentation creates no formal T0 or formal availability denominator. The first79 partial/unqualified publications remain present; the entire30min is not represented as healthy ready operation.

## Connection and REST evidence

The 29-event owned Recorder journal is bound to the same invocation/PID/boot: ten USD-M and six Spot initial WebSocket connections, nine Spot and two USD-M snapshot-bridge warnings, one REST warning, and one SIGTERM service-stop request. The16 initial connections cover each expected market/symbol/stream tuple exactly once with16 distinct connection IDs, between23:59:34.703361 and23:59:36.804385. Bridge warnings end00:00:11.751970, before first core-ready PASS.

No WebSocket disconnect, reconnect, ConnectionClosed, or WebSocket failure/close event appears in this bounded supplied owned journal. This is an absence in that evidence, not proof of every exchange/transport event or future stability. No actual unexpected WebSocket close exercised the new structured close fields; their production explanatory value remains unobserved.

At00:22:24.579580, `usdm_side_rest_failed` records BTCUSDT/um_perpetual/premium_index_snapshot, `error_type=NetworkError`, failures1. No HTTP status, lower-level exception cause, or network origin is available. Row350 retains RETRYING, failures1/consecutive_failures1/last_error_typeNetworkError. Row351 retains RUNNING, accepted23, failures1/consecutive_failures0/last_error_type null. Its successful REST timestamp00:22:29.883651146 precedes the row350 read, illustrating asynchronous heartbeat publication. One blocked sample or5s cadence is not an exact outage duration. The same process recovered; this isolated REST event is not a WebSocket-close root cause and does not establish a maximum failure count/rate.

## Resources and timing limits

Across352 samples, owned cgroup current and peak swap are both0; RSS maximum390,201,344B, host-available minimum4,718,088,192B, writer-free minimum24,363,888,640B, archive-free minimum2,051,522,383,872B. Existing host swap is30,437,376B throughout; it is disclosed separately from owned cgroup swap. All sampled hard memory/free-space bounds pass. PSI is preserved: maximum avg10 CPU-some12.00%, IO-some22.65%/full21.23%, memory-some0.59%/full0.44%. These measurements include startup and normal archive activity and are not a causal attribution.

At row350 the sampled RSS is344,772,608B, available4,906,369,024B, current/peak swap0; CPU PSI avg10-some7.62%, IO-some2.16%/full1.27%, memory PSI0. No sampled hard resource-bound crossing explains the REST warning. The export does not establish the warning's resource/network cause.

The controller reports1803.909538731s from its private BOOTTIME start counter; the raw initial counter itself is not exported, so that exact duration cannot be independently recomputed. The independently preserved sample BOOTTIME span is1788.530388308s; adjacent gaps are5.019586753–5.968438109s. Startup waiting and terminal sleep are included in the controller's elapsed interval. The immutable start request records duration1800s and cleanup lease4800s. The last sample is3001.048827595s before its absolute BOOTTIME deadline. Cleanup records1850.578718354s on MONOTONIC since the request; cleanup's exact BOOTTIME is not separately exported. Successful cleanup under the accepted same-boot lease check is consistent with these observed times. No extension or new authority is inferred.

The request asks Typeoneshot/Start3000s/Stop900s and the definition pin. Actual effective timeout properties were not exported; no effective-timeout claim is made from the post-GC defaults.

## Actual normal stop and archive handoff

Native invocation `c344d0061f5848bab847bdf9a72d8866` is bound by the original request/initial PID815480; initial activating/start is not called active-running. The recorder SIGTERM request occurs00:22:48.530322. Cleanup marker `992549f974e2096612f486ab404b8928d383a6dc83a65cdee23123c442700465`, at00:23:33.476783910, records Recorder inactive/dead, disabled, PID0/NRestarts0, backlog[0,0], enabled-active archive timer, and naturally completed archive worker invocation `36d24906810b4b76bc062c5e501088c9` inactive/dead/PID0/Resultsuccess. The timer has a finite next monotonic deadline44.172650240s later. This is the cleanup handoff for this observation.

The diagnostic file equals its owned native journal message. ExecStopPost's original marker equals that cleanup object and records `CLEANUP_ALREADY_COMPLETE_NO_ACTION`. Owned manager events bind the same native invocation: Deactivated successfully at00:23:34.119930, followed by Finished/JOB_RESULTdone. Thus normal completion is established by execution-time records. Later transient-unit not-found/default properties and a missing transient definition message reflect collection after that completed invocation; they do not establish native failure or effective settings.

At the later read Recorder remains inactive/disabled/PID0. A DIFFERENT normal archive worker, invocation `4e521f6505124b9fa10cad0c07766ebf`, PID817416, is activating/start and its timer active/running. Its completion is unobserved; initial Resultsuccess does not prove it finished. It is not mislabeled as the earlier drained worker or a failure.

## Bounded next inference

This observation is useful proof that the temporary diagnostic completed and cleaned up under normal owners, preserving startup and one recoverable REST warning. It contains no actual WebSocket closure to explain. A separately reviewed, fresh-scope, finite longer diagnostic can reasonably collect such evidence while keeping zero formal/capacity credit. These data do not justify a blind24h acceptance run, a root-cause-fixed claim, stability repair, or a new formal eligibility decision. No saved-frame audit was performed.

Artifacts include the exact original export, all nine original members, original journals, per-publication summary/hash map, event map, reconstruction assertions and detailed CHECKS/BINDING-CHECKS. The reconstruction script was corrected for the actual export schema during analysis (startup empty maps/false write-safety, REST scope field, outer metadata-result string); no original evidence or candidate control was changed.
