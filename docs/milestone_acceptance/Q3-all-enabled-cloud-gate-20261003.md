# Q3 — Actual all-enabled VPS preparation, October3

Frozen runtime remains cf3909e9/tree2ac585d0, wheel7e15bbd9… and identityeacada17….
All14 auxiliary flags and Spot/USD-M BTCUSDT/ETHUSDT remain unchanged.
Q3 is COMPLETE for current2h admission. Formal2h is RUNNING from10:49:23 Shanghai;
no target/final/duration credit;12H_STARTED=NO;PRODUCTION_READY=NO.

## Completed real cloud gates

The1182-chunk engineering baseline, both LIVE passes and completed verification
PASS remain the predecessor only. Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE
frozen reconstruction ACCEPT:20118 records/42 shards, seven completed gap pairs,
exact source/control/seed/hash checks and two rejected tamper cases; no Raw,
network or production access. Review SHA27431f8df9b46cb0d600a314275d801563f3e563c80371d59d29f9bbe163388f,
identity SHA4c58b6a7f0dbfeebd9c676cb0137da008569f68a728b4f98d3b794e2a421f398.

The actual NONFORMAL warm-up ran01:41:33–02:01:55 UTC and exited successfully.
No target was created. Complete observations include producer and independent
chain replay:

| Phase | Seconds | Pending causal references after phase |
|---|---:|---:|
| Engineering start |20.096|0|
| Immediate |16.150|0|
| Normal300s |25.511|0|
| Missed550s |32.775|140|
| Catch-up300s |30.171|0|

All complete work is below240s. Catch-up converged under ongoing input,
blocking findings/recovering contexts/backlog are empty. Strict pre-start26
contexts and all ten before/after observation auxiliary gates PASS. Four products
remain ready. Durable SIDE_DATA_EMPTY_RESPONSE events26/27 retain two cumulative
failures; actual success follows5.034/5.036s later with correct cursor advancement.
No sampled RETRYING state was captured, so snapshots alone do not prove that
transient. Independent local replay reconstructs five documents and3326 chunk/
2040 archive/3 operational acknowledgments in0.676s, with matching hashes,
continuations and findings. All41 RUNNING resource auxiliary gates PASS;
six subsequent STOPPED/BLOCK snapshots are retained outside active observation.

Warm-up stop disabled Recorder. Post-stop Catalog integrity isok,0partials,
1640 chunks/transactions allLOCAL_DELETED,5218930 frames,264215358 stored bytes,
3980726667 uncompressed bytes. No corpus/evidence reset or unique deletion.
Actual archive recurrence and verified drain PASS; pause stops the timer first,
then waits for its worker's natural success. Archive is now paused for baseline.

## Measured resources and cumulative forecasts

The real capture interval1407.314s adds458 chunks/456373 frames/25777898 stored
bytes/356003392 uncompressed bytes. Approximate mean ingest324.29 frames/s and
18.32KB/s stored; seal-minute peak scenario693.08 frames/s and31.19KB/s stored.
These observed values are estimates, not a future load bound.

Live monitoring has41 RUNNING samples over1202.464s. Host busy17.385%, steal0.886%
(max sampled interval2.100%), iowait0.508%; minimum available RAM4935061504B,
swap usage/in/out0. Maximum summed process RSS629321728B includes shared pages;
Recorder258797568B, observer group371445760B, sampled archive129855488B.
Cgroup peaks include cache and differ from summed process RSS. Archive CPU samples
span19 invocations:5.852464s is a counter range, never cumulative CPU.
Host maximum one-minute load1.48 on4 logical CPUs. Kernel block averages:
vda0.765ms/request/1.940%busy, vdb1.933ms/request/0.606%busy; no application tail
latency claim. CPU/memory/I/O PSI raw samples remain available.

Forecast all current1640 chunks, including failed/warm-up captures, plus15min
readiness capture per stage. Stopped audits add wall time but no ingestion.
Use the largest frame/chunk/uncompressed scaling factor against the actual
1182-chunk engineering full wall1100s, including both LIVE passes, freeze/SQL/
shards and completed verification. Future unexpected interstage capture requires
recalculation. Mean and sustained observed seal-minute peak scenarios:

| Accepted cumulative target | One complete audit mean / peak scenario | Root free after peak + sealed/staging scenario | Archive extra Raw/evidence peak scenario |
|---|---:|---:|---:|
|2h|66 /130min|14.77GiB|2.38GiB|
|14h|289 /700min|12.59GiB|13.11GiB|
|38h|730 /1829min|8.26GiB — below reserve|34.35GiB|

Root budget includes simultaneous42 owners'128MiB rotation plus16MiB frame
allowance, growing metadata, four Catalog/scratch copy allowances and128MiB logs.
V3 additionally budgets one sealed-awaiting generation5.906GiB and42 concurrent
compression staging outputs at current historical maximum chunk plus1MiB each,
1.299GiB. This is an observed-size scenario, not the hard upper bound of all128MiB
chunks compressing simultaneously. The38h sustained peak/staging scenario falls
below10GiB and cannot qualify that future gate; current2h admission still passes.
Hard reserve10GiB remains unchanged; archive currently has~1.91TiB free. Old
custody consumes measured current free space and is retained, outside this new
qualification lineage. Existing predictive EMERGENCY ETA remains visible; actual
hard reserve is not reached. Forecast is assessed together with actual automatic
archive recurrence/zero backlog, without changing thresholds.

Natural size-trigger rotation was not observed: warm-up maximum chunk19.15MB,
retained maximum32.17MB versus128MiB. Time rotation dominates this workload;
exact CI's existing forced-size regression supplies that boundary evidence.
No synthetic burst is injected into Formal or false live size-trigger claim made.
Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE gate review ACCEPTs current admission
and these limits. Report SHAda13ded114752f430a72fdf74c43e733e71ecb6859b25a96d206a9518ecf69e5;
identity SHAc566e40b219f58ccaa144fb8aa1cb6a0e33cc38227f41d5f8a9cda42eb4d6e83.
Historical operator-helper-inputs installed_source_unchanged=4e1cf32 is retained
as preparation provenance, never current identity; actual V5/prewarm-up/deployment
authority consistently binds cf3909e9/eacada17.
Long14/38h audit schedules are not certified here: recalculate from actual2h
terminal and review performance before the next stage. R-078 remainsOPEN; no
speculative language rewrite or server upgrade is introduced.

## Finite quiet window and current operation

Fresh apt index update PASS; upgrade simulation0 pending, dpkg audit empty,
no package locks, no reboot required. Actual stopped deployment verify again
matches eacada17… source/wheel/config/unit/28 dependencies.

The reviewed expiry restore was armed BEFORE masking the five approved OS units.
Actual runtime-only masks, inactivity, negative apt service activation and empty
locks PASS. The23400s window starts02:22:47 UTC, expires08:52:47 UTC
(10:22:47–16:52:47 Shanghai). Calculation:25.44min baseline plus conservative
130.13min terminal, both×1.25, plus120min target/15min readiness/15min endpoint
and drain/30min publication-review =374.46min, rounded up to390min.
No automatic extension or Recorder restart. Controlled exit must prove RESTORED
before stopping the expiry timer.

Authoritative post-warm-up baseline is now running in
`binance-recorder-q3-post-warmup-baseline.service`, invocation
1a401460138b49c19ada13c96a09c67e, start02:22:47 UTC. It has no total audit cap;
native900s no-progress watchdog remains. Exact canonical baseline then completed
verification publish under `Formal2h/baseline`; engineering root is not reused.
Recorder remains inactive AND disabled; archive paused; economical read-only
monitor watches native progress. No Formal T0 or credit yet.

## Retained evidence

Cloud root:
`/srv/recorder-data/recorder-archive/evidence/Q3-discontinuity-array-20261003-ik5UgdqY`.
Local cache:
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/q3-startup-boundary-20261002`.

Warm-up controls bundle1760579B SHAd7eb49a1ad12ebb8fda9fe5711af2be251a37422d34f955a0e03c691cbe3a53b;
post-warm-up census SHAbb794840ff0461743355346b4606372b1c6d2bdfc33cbe552d9ee625c86cc711.
Forecast v1 SHA08b0b9c1567dd36287170877b899fcb5c5dc44b902161dcd9313ce76552ff541
is retained; v2 SHA9f9afcf4bb91033f7a91dde58e783e5adceec0d503b87fc94e631df785b37e20
corrects only archive counter labeling/adds measured block/load summaries.
V3 SHA10125fb081cce4fbe0d65874ee77c8b323082af30106fb722ef24550d2324b56
adds the explicit sealed/staging scenarios and preserves original forecasts.
Quiet plan SHA60c8404276be0f16a87cb40403527441df73d9ddd443fa25b93d3e57a181f3e7.
Reviewed baseline helper SHAb1f64c2001766c9faa50fe47f2abb01e014c69646356eac670957defad93c1e7.

No runtime/test/config changes; prior exact-source release checks reused.
Next: actual post-warm-up baseline/both LIVE/completed verify, strict fresh
core4/aux26 readiness and one Formal2h. Target stop/disable, verified drain,
terminal both LIVE/completed verification and independent review remain required.
Stop before12h.

## Authoritative baseline completion and Formal start

Actual baseline/both LIVE/completed verify PASS, root564beacd02c3b22488644ed5724b61bfa6ff97883060efe76f1b811b0ed1ebff,
27908records/59shards. Native start02:22:47.860167/end02:43:55.742026UTC,
wall1267.881859s/CPU1266.877s/cgroup457.3MiB/swap0; completedverify18.179007s.
Independent frozen reconstruction2.272s ACCEPT/zeroRaw/network/production access,
report SHAd6d084fd752feccb396d668057e1fc8bb659ed6c93f257a98204af9c6c67ed23,
identity SHA86988e936ad28ad4bed3077ad65ac11c9675dcb1923f6fbeb9d5c921c7664cd0.
Control bundle7514136B SHA225ab17ec68ad02eb72b22b031712b8026b08b7235fe68a4c8a72acdb41bf495.
Fresh strictcore4/aux26/deployment PASS; archive actual recurrence restored.

Formal stage `Formal2h/2h-9575ca86a96f44c6a2ffd7d5be73b571`, start SHA28a7e70291b72eb5d9f15fdd7cc9522db178964b2c1aa5a002da78e6fc5d110a,
T0 utc_ns1790995763451078019, UTC02:49:23.451/Shanghai10:49:23.451;
minimumtarget04:49:23UTC/12:49:23Shanghai. RecorderPID43133/invocationf832d492593041bb83a8f8ce4e2c5c1a,
observerinvocationf333ae73277543258ce1400d5bf18380, no restarts at pre-start.
NativeRuntimeMax9000/TimeoutStop150, reviewed strict ExecStopPost stops/disables
Recorder on every exit;30s sampler and5min read-only Luna monitor active.
No target/final/credit yet. No12h.

Pre-start enable/daemon-reload shifted the relative OnActive expiry from its
original deadline. Original receipt is retained. At02:52UTC an absolute calendar
expiry08:52:46UTC was armed/verified BEFORE cancelling the relative timer,
preserving original08:52:47 cutoff without changing Recorder/observer/T0.
Five OS units stayed masked/inactive with no maintenance activation. Actual
`quiet-window-absolute-expiry-correction.json` binds timer/process/start/boot values.
Final independent eligibility review must assess this operator correction; no
eligibility is inferred here. Controlled RESTORED precedes cancellation of
`binance-recorder-q3-formal-quiet-absolute-expiry.timer`. Future finite windows must
use absolute cutoff scheduling, so daemon-reload cannot silently extend them.
