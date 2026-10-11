# Q8 failure diagnosis and minimal retry correction

The first Q8 run failed before its twelve-hour target. Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE reconstruction accepts this diagnosis, not the run: last persisted BOOTTIME41460.100922028s, wall interval to failure41490.06892014s, no43200s target or full saved-data audit. The old run and all saved data remain preserved.

The private controller asserted core readiness/heartbeat before writing its observation. The triggering publication and row were lost, so the available evidence cannot distinguish core-not-ready, stale/future heartbeat or combined conditions. The earlier ETH open-interest NetworkError recovered5370s before the fatal assertion and does not explain it. Recorder SIGTERM followed the controller failure during automatic cleanup; the parent checked hours later. OS/archive restoration preceded expiry cancellation.

The completed failure reviewer replayed all1383 persisted metadata observations:1382PASS/1BLOCK. This incomplete-period availability metric is not data correctness or accepted12h. Persisted rows do not establish a Recorder crash or resource violation. Production source49b4a2d/wheelc7d354be/identity7072575b therefore remain unchanged.

| Failure review control | SHA256 |
| --- | --- |
| Independent report | `79e3aa434dd0fad7fa631df2852d7207d8b0a9dd371280c8e30660de3618f442` |
| Review identity | `3ba38092558faf2d37b23283e1628ba2b6a3020ef98ccf317f0bd22482ed3d53` |
| Independent result | `4d2eb2da6c4552dfd22747690278c31fbefa3e2cfdc12feec4ab412a090fc388` |

Private failure review: `/private/var/tmp/bmdr-q8-failure-review-u9x4fazc`. No Raw, network or production connections were used by the reviewer.

## Affected correction

The owner's October11 instruction explicitly authorizes diagnosis, repair and a fresh VPS12h. The retry repairs the private test controller: persist assembled rows before terminating checks, retain the exact already-read bounded publication/partial context on failure, use named diagnostics, and preserve the original failure if diagnostic writing fails. Exclusive outputs preserve existing evidence.

Only sampled core/heartbeat degradation can receive at most60s consecutive and60s cumulative recovery under the same process/incarnation. Strict all-auxiliary/core/heartbeat PASS must close an episode. Hard runtime/config/boot/clock/resources still fail immediately, including during recovery. A degraded strict target fails without extending T0 or the target. All BLOCKs remain in monitoring; saved-data correctness remains strictly>99.9% over the complete audited frames, with full dualLIVE/completedverify/independent restored closeout. This changes an explicit availability rule for the new attempt and does not relabel the old failure or prove it would have recovered.

23 new focused diagnostic/recovery regressions and14 integration/resource/restoration cases pass locally. The concrete correction, updated retained-corpus capacity and corrected native argv are independently DESIGN ACCEPT;18 additional reviewer boundary cases also pass. Actual copy/readback, arm, warm-up, unique T0 and full execution remain distinct gates; no new12h credit is claimed here.

## Concrete retry design acceptance

Report `0790e2bb66d0556bc59d0d14a659de1433231d63b8a58d06a5d246c2ae4daae8`, review identity `fd50a82abf6ba8293fdcfe6afa203d27a5827a084fd563977ead89373a31a767`, result `606b625c786f7c03985aa69c1dc478df5d955c5875e5d4fc69b79b6e3c100133` bind candidate manifest72b8e2a5, controller6e5ebae8 and corrected outer-arm argva54db531. The independent reviewer found the original proposed arm cleanup Stop150 too short; Stop1500 now covers the existing restoration boundary. This accepts private orchestration design, not actual startup or future12h/data credit.

Fresh formula recomputation accepts new finite129600s, required117419.972546823s/margin12180.027453177s, projected writer11885676731B/archive2082547242529B and distinct writer/archive temp devices. Private report: `/private/var/tmp/bmdr-q8-retry-operator-review-gloynfxg`. Actual upload/TMP/canonical/admission/arm/start are subsequent evidence; all final saved-data and restored qualification gates remain pending.

## Actual copy, finite arm and release binding

Actual metadata controls independently ACCEPT: report `99398b020f6ad7c34f591e7a3fc3023ca3f2867dd73782ab744b5da119790710`, identity `67867276520dd7b8a2f59964cb8c575217dbbf1daf6aa9480fb983b2db79f3fb`, result `e6d4514108ca56a4a7e7e2706603d12087d62ffd82bb1cf5ec14a1a73e2ecefa`. Private review `/private/var/tmp/bmdr-q8-retry-actual-arm-review-v3bd5834` binds all10 complete controls: copied helpers and archive bmdr TMP/SQLite device64785, canonicalVERIFY87233928, exact fresh census/budget, fixed window06345f3e and actual owned arm8f5822c0.

First arm9b11f4b8 rejected the archive state before verification/window/T0 and actual RESTORED/errors[] followed; its controls remain preserved. The independent packet certifies that rejection and the retry's natural inactive/PID0/success snapshot; the separately inspected normal archive busy journal is parent-reported to that review. Retry2 changes only the owned arm unit name, not its safe recipe or fixed budget. Actual deadlineOctober12 17:58:48UTC is immutable. New capture/warm/T0 and final saved-data qualification are outside this actual-arm acceptance.

## Actual all-enabled warm-up and new T0

Startup report `fdeffef60d6ed0a9abb34663259fb8a9446d69f39f059b1e0681e80c68526701`, review identity `b206bc55ce95cbddece3a8e398d596cf4aed4bbd05e44dadda08702725ea6d54`, result `63060cdfeae48f2bc5ae2657e39734002cbae29be293130dfc3f0d91f051ea28` independently ACCEPT actual warm/readiness/resources/capacity/new43200s start. Private review `/private/var/tmp/bmdr-q8-retry-actual-start-review-hl3qe91d` preserves all10 controls and manifestd0bd190d; all19warm+2end/start gates independently replay. Strict endpoints316.383s, four products READY/synchronized/snapshot persisted, stable Recorderinv488/PID353359/NRestarts0/boot59bb, actual captureinve9c/PID353247 and sole run2b406e07/T0October11 06:09:14.345832865UTC match. TargetOctober11 18:09:14.345832865UTC (ShanghaiOctober12 02:09:14), no old time transfer.

19warm rows11PASS/8BLOCK are retained outside capture denominator; two early startup-not-READY and six typed-empty observations are not relabeled. Including endpoints RSSmax265883648B, hostavailablemin4843638784B/RecordercgSwap0, hostswap18472960B with unchangedpswpin531-out4887. The diagnostic readiness capacityCRITICAL/actual_hard_reserve_reached=false is retained; fresh actual reserve/window admission is independently recomputed PASS (margin13801.462s/writer11872068173B>10GiB). Acceptance binds supplied RPC snapshots/contextual plaintext journal, not absent structured per-entry owned-invocation fields or original full state-publication bytes. Complete12h/saved-data/dualLIVE/verify/restored qualification remains pending.
