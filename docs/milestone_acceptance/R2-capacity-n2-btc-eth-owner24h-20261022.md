# R2 N2 BTC+ETH own24h capacity acceptance

Current cause/budget review: [report](../reviews/2026-10-23-n2-monitor-budget-causal-report.md), [result](../reviews/2026-10-23-n2-monitor-budget-causal-result.json), [manifest](../reviews/2026-10-23-n2-monitor-budget-causal-manifest.json). Independent ACCEPT concerns evidence interpretation only; N2 monitor acceptance is unreachable.

Latest actual October23 01:51:52UTC (Shanghai09:51:52): **N2 MONITOR BUDGET EXCEEDED / CAPTURE STILL RUNNING / SAVED-DATA RESULT PENDING**.

- [x] Single explicit-owner read `5c20f433` (61 original controls, all parent SHA/size checked) confirms the same run `344008dae022452c9bee26c1bda90ea8`, CAP067b/PID748970 and Recorder4409fbb6/PID749037 active-running, NRestarts0. Elapsed69,032.083795026s (19h10m32s); actual own86400 minimum target remains **October23 06:41:19.929649179UTC / Shanghai14:41:19.929649179**. Latest core/official/strict observation is PASS; no target or terminal result yet.
- [x] Current nominal2302 checks:2285 officialPASS/17BLOCK, skipped0. **The final fixed2880-slot >=99.5% criterion permits at most14 bad slots. With17 already bad, the best possible final result is2863/2880=99.4097222%; this run cannot pass its monitor criterion.** Recovery probes do not dilute the denominator. Strict2264PASS/38BLOCK is separate; neither percentage supplies saved-frame correctness.
- [x] Sixteen failed nominal checks saw a disconnected book_ticker across the four configured products; one saw BTC USD-M open_interest unavailable with a NetworkError. Subsequent successful observations show recovery (core probes about5–15.53s; auxiliary-only OI next nominal29.814968s). All formal and19 recovery-probe observations retain complete trusted publication/write-safety evidence. Owned current AND peak swap0; RSSmax444,948,480B; host available minimum4,414,652,416B.
- [x] Journal1173 rows retain338 disconnects (333 ConnectionClosedError/5TimeoutError) and333 stream reconnect events. The frozen source records exception class but omits close code/reason, so the initiating transport cause is unresolved. Host contention and long write/seal timings remain diagnostics; there is no observed queue-full recovery cause and no basis to declare the VPS hardware maximum is one symbol.
- [ ] Preserve this healthy original capture through its existing target and automatic full new-data double LIVE/completed immutable verification, then actual OS/archive restoration before guard cancellation. The controller explicitly completes saved-data auditing even when monitor-rate is BLOCK. **Do not issue an accepted N2 capacity receipt or advance to N4 from this monitor result.** No restart, threshold change, counter reset, running-helper edit, duplicate audit or unchanged blind24h retry.
- [x] Focused independent LOCAL/OFFLINE evidence review ACCEPT `68118521` / `d7384d4b` / `50976676`:21,874 assertions; all91 manifest-listed members read back by SHA/size, plus the manifest SHA verified. This accepts the causal evidence interpretation and unreachable monitor budget, **not N2 final acceptance**. No SSH, Raw access, candidate execution or source changes.
- [ ] Choose the smallest evidence-based diagnostic/repair for a future independently qualified attempt. No stability fix or new source deployment is claimed at this checkpoint. N1 BTC remains the accepted tested lower bound; the maximum/headroom remain unresolved,48h PENDING/R078 OPEN.

Guard5dd636b6 expiry1792880481 (October24 22:21:21UTC / ShanghaiOctober25 06:21:21) is unchanged and must never be extended/reused. At/after the actual target, read genuine owner-target/monitor-rate/stop/terminal/selected-data progress once and set the audit checkpoint from actual output. Earlier startup and prefix entries below are retained history, not the current acceptance decision.

Actual-start review: [report](../reviews/2026-10-22-n2-actual-startup-current-report.md), [result](../reviews/2026-10-22-n2-actual-startup-current-result.json), [manifest](../reviews/2026-10-22-n2-actual-startup-current-manifest.json), [unchanged original owner-start](R2-capacity-n2-btc-eth-owner24h-20261022/owner-start.json). Final24h/data/handoff/receipt are still pending.


Latest actual October22 10:32:15UTC (Shanghai18:32:15): **N2 BTC+ETH FORMAL_RUNNING / OWN24H AND FINAL DATA ACCEPTANCE PENDING**.

- [x] Actual owner-start `bf17515f`, sole run `344008dae022452c9bee26c1bda90ea8`: T0 **October22 06:41:19.929649179UTC / Shanghai14:41:19.929649179**, BOOTTIME1716174110184531. Own86400 minimum capture target **October23 06:41:19.929649179UTC / Shanghai14:41:19.929649179**. No inherited N1 duration.
- [x] Single post-checkpoint metadata read `820dfdca` verifies CAP067b/PID748970 and Recorder4409fbb6/PID749037 actual active-running, NRestarts0; no failure/target/terminal result. Warm883.171574s/26 rows (15 strictBLOCK/11PASS retained, outside formal denominator), strict26 end/start PASS. Prestart229201.230694s>required218342.645680s, margin10858.585014s; projected writer21,131,644,856B>10GiB.
- [x] Current462 nominal checks: official459PASS/3BLOCK=99.350649%, strict445PASS/17BLOCK, skipped0. Three core-not-ready samples lost book_ticker connection (SpotBTC, USD-MBTC, SpotETH), each followed by strictPASS recovery around5s later. All failed rows and three diagnostics retained. Full2880-slot >=99.5% acceptance permits at most14 failed nominal slots; this prefix is not final availability or saved-frame correctness.
- [x] Formal+recovery465 observations retain complete trusted same-read publication bytes. Across493 warm/end/start/formal/probe observations, owned current AND lifetime peak swap0, RSSmax387,940,352B and host available minimum4,686,319,616B. Legitimate pre-T0 STARTING may lack initial write-safety publication; formal observations are trusted.
- [x] Current owned journal retains74 REST warnings (3NetworkError,71EmptySideDataResponse on taker queries); no ERROR/CRITICAL entry in this exported journal. Auxiliary strict-only waits and transport recovery remain separate diagnostics; their deeper initiating cause is not established. Latest sample core/official/strict PASS.
- [x] Independent actual startup/current-prefix ACCEPT `2609228c`/`12d29a59`/`a89b8d47`:39002 assertions,29 original controls,493 same-read publication bytes/serialization and986 paired maps/25636 entries reconstructed; all533 review members read back by hash/size. No blocker found. Full startCatalog membership and final Catalog/selection binding remain final-control tasks; no future24h/data acceptance credit.
- [ ] Own86400 target, complete new-data double LIVE/completed immutable verify, final independent/restored handoff and N2 owner receipt. The target above is capture minimum, not a promised final-audit completion time.
- [ ] After full N2 acceptance continue nested4/8… under fresh config/identity/space/time/scope/window/T0. Capacity/headroom boundary pending;48h PENDING/R078 OPEN.

Fixed guard expiry1792880481 (October24 22:21:21UTC / ShanghaiOctober25 06:21:21) remains unchanged. Healthy capture before actual target needs no repeated VPS reads/restart. Preserve all data and error evidence. Earlier NOT_YET_T0/warm-checkpoint paragraphs are retained history.


Actual single launch binding independently ACCEPT `7cbf1bd2` / `f5225d69` / `7943d771` (15 checks,5 members read back). This accepts exact dispatch/effective native binding only, with no strict-warm/T0 or24h credit. [Report](../reviews/2026-10-22-n2-actual-capture-launch-report.md).


Latest actual October22 06:26:14UTC (Shanghai14:26:14): **N1 FINAL ACCEPTED / N2 SINGLE CAPTURE STARTUP ACTIVE / FORMAL T0 PENDING**.

- [x] N1 BTC full own24h/new saved data/independent final/restored handoff and receipt `a7938d4a` accepted; saved frames100% and monitor99.861111% separately meet >=99.5%.
- [x] N2 BTC+ETH config-only deployment `9571c469`/identity `976dfc55`/config `5b73db1b` verified, all12 commands0; frozen source `c3686dd` and wheel `68460be4` unchanged.
- [x] Actual Linux QA18 cases and normal native completion independently ACCEPT `cf5d7748`; exact runtime14/operator `78026179`, physical24-payload copy `48a5b215`, fresh admission, actual ARM and pending launch independently accepted. No source, wheel or old test repetition.
- [x] New guard `5dd636b6` ACTIVE_VERIFIED, fixed expiry October24 22:21:21UTC / ShanghaiOctober25 06:21:21 (epoch1792880481). Never extend/reuse it;230400s covers24h plus audit/restoration and grants no64h capture credit.
- [x] Sole capture native `067b71b2fc804475a3548b57311011f8`/PID748970 launched once, actual active-running/Typeexec/Runtime89280/Stop1500. Dispatch `90460606` binds current boot/window/controller and effective parameters. Initial stopped Recorder is the pre-start boundary snapshot, not a failed capture.
- [ ] Actual strict warm/start/owner-start T0 and independent actual-start reconstruction. Reasonable warm/start checkpoint October22 06:51:15UTC / Shanghai14:51:15. Before that healthy native requires no duplicate VPS reads or restart.
- [ ] Own86400/full new saved-data double LIVE/completed verify/final independent/restored receipt. No N2 accepted duration or actual end time yet; derive minimum target only from real owner-start T0.
- [ ] Continue nested4/8… after complete N2 receipt; measured capacity/headroom matrix pending,48h PENDING/R078 OPEN.

Monitor and saved-frame conformance keep separate complete >=99.5% denominators. Recoverable faults log/retry; disk/sustained-writing/untrusted-evidence failures pause affected capture and notify. Preserve all data and earlier evidence. Prior checkpoints below are history.


Actual October22 06:18:47UTC: Linux QA independent ACCEPT `cf5d7748` (105 checks;55 review members read back). New N2 exclusive24-payload copy `48a5b215` complete; current census/backlogs checked, fresh time/space ADMITTED230400s>221626.192s, projected writer21,164,583,226B>10GiB. These are admission forecasts, not measured N2 capacity or real duration. Actual physical/pending ARM independent review remains pending; no guard/capture/T0 yet.


Actual October22 06:14:43UTC: isolated Linux QA completed normally (12 clean imports +6 fixture processes, all return0), qa-result `c5ed0133`, full44-file export `7d0eadd3`; independent actual-result review pending. No capture or T0. First software-copy attempt failed before SSH authentication and executed nothing remotely; the existing host key was then loaded from the macOS keychain and its host-specific loading configured, preserving the original config. The single successful copy and QA native completed; no retry of the test itself.


Latest actual October22 05:36:33UTC (Shanghai13:36:33): **N1 FINAL ACCEPTED / N2 STOPPED CONFIGURATION VERIFIED / N2 QA AND STARTUP PENDING**.

- [x] N1 BTC own24h/full double LIVE/completed verify/independent final/restored handoff and receipt `a7938d4a` complete; main `02fe214` actually pushed. Saved frames45893093/45893093=100%; monitor2876/2880=99.861111%, separately >=99.5%.
- [x] BTC+ETH config-only deployment `9571c469` complete: all12 commands returned0; current identity `976dfc55`, config `5b73db1b`, same frozen source `c3686dd`/wheel `68460be4`. Old config/identity/venvs retained. Fresh canonical VERIFY accepted; Recorder inactive AND disabled/PID0/NRestarts0, normal archive restored.
- [x] Actual deployment independently ACCEPT `bd3a0cf1`/`04c5fe7f`/`ed1a70a5`; all27 review members hash/size checked. Native2b8c50c1 ended normally; GC defaults are not effective execution settings.
- [x] Future owner995 helpers exact dual CI `39728230486`/head `b23279a` BOTH SUCCESS after test-only type annotation correction; no production rebuild.
- [ ] N2 exact bound Linux QA, exclusive runtime copy, fresh capacity/time admission, new finite guard and strict warm/start.
- [ ] N2 own86400/full new saved data/final independent/restored receipt; no N2 capture or T0 exists yet. The planned230400s guard covers24h capture plus audit/restoration; it is not64h capture or an extension of any old window.
- [ ] Then nested4/8… candidates and measured capacity/headroom matrix.48h remains PENDING; R078 OPEN.

Recoverable faults are logged/retried; monitor and saved frames use separate complete >=99.5% denominators. Untrusted evidence, sustained writing failure or disk failure pauses affected capture and notifies. See [N2 current acceptance](R2-capacity-n2-btc-eth-owner24h-20261022.md). Earlier checkpoints below are retained history.

Scope: `/srv/recorder-data/recorder-archive/evidence/R2-capacity-n2-btc-eth-20261022-yf61hyp0`.
Each of BTCUSDT and ETHUSDT on both Spot and USD-M; all14 features,4 products,26 auxiliary contexts and42 spools including both global contexts.
N1 receipt is a previous capacity result, with zero inherited duration. Every accepted candidate requires its own actual86400s and complete new-data qualification.

Deployment review: [report](../reviews/2026-10-22-n2-actual-stopped-deploy-report.md), [result](../reviews/2026-10-22-n2-actual-stopped-deploy-result.json), [manifest](../reviews/2026-10-22-n2-actual-stopped-deploy-manifest.json).
Design review and Linux QA design are accepted local preparation; they grant no actual Linux run, capture duration or final frame denominator. Exact guard expiry and capture target can be stated only after actual ARM and owner-start JSON respectively.
All production Raw/archive/failed objects and prior configurations remain preserved. No every-exchange-event, universal symbol maximum, permanent error-free or production-ready claim.
