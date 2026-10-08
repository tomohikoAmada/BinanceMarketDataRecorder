# Q6 optimization and release preparation

Started October 5, 2026 (Shanghai), after independently accepted Q5 closeout
and main b9ffa5c push. Q6 is IN_PROGRESS; Q7 has no T0. Existing source23ea5c4,
wheelbb7e357e and accepted14h remain original evidence. Recorder stays inactive
AND disabled while isolated comparisons run. Existing hourly vps-2h is ACTIVE.
No Q8/12h,24h,48h start is authorized in this continuation.

## Predetermined comparison protocol

Record this protocol before measurements. Compare frozen source23ea5c4 with
the smallest candidate on identical inputs, three alternating repetitions,
first observed run plus warm subsequent runs; no OS cache dropping or unrelated
service control. Use a private local directory and a newly created child of
approved VPS disposable parent /var/tmp/binance-recorder. Retain evidence.
Report source hashes, installed dependency/lock identity, fixture identity,
machine, wall/CPU, process peak RSS, I/O, descriptors and temporary bytes.
These measurements earn no Formal credit.

* Complete normalization (Raw validation, parse, scratch sort/dedup, partition
  publication, independent Parquet readback) and verified replay: current four
  products and isolated twenty ProductKeys across BTC/ETH/BNB/SOL/XRP/DOGE/
  ADA/AVAX/LINK/LTC. This selected study mix is an explicit capacity assumption,
  not an operator production selection or twenty-product certification. Compare
  exact build/partition bytes, hashes, provenance and replay order. At least
  20% and 0.5 seconds median improvement on the 40k-frame complete workflow,
  with no meaningful CPU/memory regression, admits scratch buffering. Report
  normal and burst workloads separately. Repeated variant sort removal requires
  a separately attributable 5% and 0.2 seconds complete-workflow improvement;
  otherwise retain old sorting even if a tiny isolated function is faster.
* Bounded normalization merge: removal of an actual >64-descriptor failure on
  at least128 runs admits the32-reader bound without faster wall time. Exact
  stable oracle output, malformed input, cancellation, flush failures and
  closure of all readers are required. Extra merge passes/temp bytes must be
  measured; no unexplained >10%/1-second regression on ordinary small workloads.
* Catalog selection: 200k retained metadata rows plus50 eligible chunks, tied
  times and ownership exclusions. Require same selection and at least90%
  VM-step reduction and 10ms per selection saved on VPS. Record query plans,
  writable index migration, database size and insertion/update cost. Migration
  must finish within30 seconds and added fixture index footprint below20MiB;
  read-only historical Catalogs must not change.
* Checkpoint: complete save/fsync/Catalog registration and restore, current4
  and study20 books with1000 and5000 levels per side. Require at least15% and
  20ms per20-book batch saved. File and Catalog must bind one snapshot even if
  the mutable book changes during persistence. Manual derived checkpoint cost
  is not live capture throughput.

## Five requested direction decisions (pending evidence)

| Direction | Admission criterion / smallest evaluation | Status |
|---|---|---|
| Fused stored-hash/decompression read | Profile stored-read cost versus complete validation on identical fixture chunks. Require >=10% and >=0.5s complete batch gain before introducing a new stream adapter; retain independent target readback and source retirement revalidation. | PENDING |
| Small-chunk reduction | Replay measured current rotation/chunk-size/rate trace against a longer rotation, quantify chunk reduction, additional active/reserve and publication delay. Require >=20% full archival/audit cost gain with unchanged durability/recovery/freshness budgets; never reconfigure live capture for this study. | PENDING |
| Bounded merge readers | Demonstrated descriptor failure removed, exact stable outputs, fault/cancel closure, measured extra I/O. | PENDING |
| Archive cadence/duty | Measure current ingest versus actual single-worker drain and natural recurrence. Change only for sustained backlog or >=20% useful headroom with no capture lag/reserve regression; extra parallel owners are out of scope. | PENDING |
| REST queue scheduling | Inspect actual current waits/ages/missed/catch-up and controlled20-product queue fixture. Change only if waiting causes actual freshness/starvation failure and materially improves complete catch-up; retain cooldown and public request budgets. | PENDING |

Justified REJECTED/DEFERRED decisions close evaluation and ship no experimental
change. Unmeasured twenty-product capture, auxiliary or archive behavior stays
explicitly unqualified; no speculative technology or hardware change.

## Release and next stage

Pending: comparisons/decisions, independent exact-source review,48h ADR and
compatibility tests, full engineering/dual CI/clean locked wheel/cloud checks,
stopped canonical deployment VERIFY, all-enabled warm-up/resource admission,
fresh full authoritative baseline and independent eligibility, practical
growing-corpus capacity/audit window and strict pre-start. Only then create
one Q7 identity-bound7200-second stage. Monitoring >=999/1000 is sampled
qualification, not exchange-event completeness. Both full LIVE audits,
completed verification and independent actual restored closeout remain required.

## Actual intermediate results (release remains pending)

Full local engineering on initial candidate:2024 passed/24 explicit online skips/
5 deselected,171.80s;13 pre-existing multiprocessing fork warnings. Ruff/Mypy293
files/M0/Go PASS. Durable complete48-chain regression added after independent
private validation; new8-stage tests PASS. After noise-level repeat-sort
measurements, `_winner` was restored to original sorting; affected normalization/
replay checks and final exact-source engineering remain tracked separately.

Initial independent source-correctness-only ACCEPT reportccab69e5…941519/
identity2afcb257…f4682 binds the pre-revert11 files, not a frozen release.
See ../reviews/2026-10-05-q6-initial-source-correctness-review.md. It is not
performance/deployment acceptance.

LOCAL native full-workflow4/40000 first three pairs: old174.082/174.196/174.917s,
buffered+bounded9.269/9.064/9.131s, original-winner variant9.049/8.925/9.530s;
all persisted bytes/build IDs/provenance/replay equal. Removing winner sorting
has no attributable material complete-workflow gain: REJECTED and reverted.
VPS first4/40000 pair old720.181s versus candidate51.961s/original-winner52.533s,
exact results equal; subsequent repeats and20-product/burst cases are still
running. Initial4-case representative data alone does not close Q6.

LOCAL private bounds evaluation da2f39ee…b8451: three baseline128-run trials
under64 FD limit fail errno24; three candidate trials finish exact stable1024
rows at33 owned handles, all handles closed. This deliberately constrained
process fixture is not a production outage claim. Intermediate merge writes
one additional complete scratch generation; measured peak space is in the
retained report. VPS bounds validation is pending.
LOCAL stored-only hash pass on4fixture chunks/1,319,186storedB/23,875,836decodedB
uses0.000624–0.000697s versus full artifact validation0.016330–0.016929s.
This bounds avoidable hash-read work below the predetermined10%/0.5s gate even
before frame parse cost; it does not demonstrate a safe fused stream adapter.
VPS corresponding evaluation is pending; no integrity-read reduction is included.

Frozen Q5 manifest trace independently reconfirms19451 chunks/3762 incomplete/
80zero-frame manifests. Trace is whole retained corpus, not solely12h; it lacks
all per-frame arrival, pause/stop and recovery transitions. Longer rotations
cannot be accepted from an aggregate file-count estimate. Rotation candidate
DEFERRED;60s/128MiB/durability/current boundaries unchanged.
Archive cadence/duty and expanded REST queue scheduling remain PENDING evaluation
of actual controls and limits; no experimental scheduler/parallel owner is shipped.
Current four-product capture had1435/1436 sampled monitorPASS/strict endpoints
and0terminal backlog;20-product live capture is not certified by derived fixtures.

Exact current controls: LOCAL /private/var/tmp/bmdr-q6-bench-controls-jkgugp82;
full LOCAL result childq6-derived-1jddv2ce. VPS controls
/var/tmp/binance-recorder/q6-derived-controls-j6wkiz6a, native comparison service
binance-recorder-q6-derived-bench-20261005.service/invocation
b8f5dff770c442bc968797ee5f96d5b9, childq6-derived-jl1_fbqk. Source bundle
48d8e5db727448935b7667058967dbfec1afad13b8bc739f0d82ddf170f75750 and initial
protocol aef5ededc17f40e8347238e0008842ace6ed3a4bea3ddd8b71b509a3e0b371cf
frozen before comparisons. Executed original tool813cb961…8552f4 is retained
onVPS; repository tool subsequently received typing-only fixes. Source bundle
keeps both initial candidate and original-winner runtime comparison. Final
selected source needs its own exact byte binding and final validation.
Each platform creates its own frozen seed, then clones identical bytes for
all variants/repetitions; cross-platform fixture manifests/UUIDs differ, so
absolute platform timings are not an identical-Raw cross-machine comparison.
The original tool uses ordinary ephemeral Replay TemporaryDirectory (OS temp)
inside this isolated derived workflow; production data roots are never used.
No Formal T0/deployment/old credit transfer. No live Collector changes.

Measurement clarification before checkpoint runs: the frozen original comparison
also includes constructing/synchronizing the input books in its outer timing.
The predetermined checkpoint gate concerns an already-ready book's complete
save/fsync/Catalog registration/restore, not unrelated bootstrap work. Record
both outer totals and a supplemental timing of that complete affected workflow
on identical preconstructed books; keep the original15%/20ms threshold. Do not
substitute canonical-mapping-only timings. The input freeze precedes each run.
Full normalization includes Raw validation, parse and publication; Replay opens
and validates every selected build/partition and includes every selected symbol.
Fixture payloads are official-schema aggregate trades with20% repeated logical
identities; this is an isolated derived/recovery workload, not an all-stream
online20-product capture/REST/durability certification. Expanded auxiliary, actual
arrival/capture lag, resync and outage-drain evidence remains separately bounded
or explicitly deferred; no unsupported production capacity claim is made.

Frozen-control scheduling evaluation: archive adaptive cadence/duty DEFERRED
and expanded REST scheduling DEFERRED. Existing single-owner60s/50s budget had
natural successful workers and0actual terminal backlog. No sustained-outage
mu/lambda, modified-cadence comparison or twenty-product queue-wait telemetry
was captured; those unrun studies cannot be replaced by scaling aggregate counts.
Frozen1436-sample actual26-owner contexts, strict endpoints and completed cursor
history were examined;1435PASS/1 originalNetworkError sample retained. Published
closed-period cursor lag reaches one300000ms period. Success age/cursor lag and
published-state receive age are not HTTP latency/queue-wait percentiles or
every-event completeness; ages use each recorded gate observation after state
read, not earlier sampler-begin UTC. No measured scheduler-attributable failure
justifies a new priority/concurrency mechanism for this four-product release.
Evidence /private/var/tmp/bmdr-q6-bench-controls-jkgugp82/
q5-existing-queue-cadence-analysis-v2.json; keep its prior timestamp-analysis
attempt as unselected evidence. Current limits and future twenty-product
certification remain explicit; deferral does not certify expanded capacity.

## Frozen local comparison completion and independent follow-up

LOCAL original full comparison is COMPLETE, result12a2906b6d4e890a7909aec77005ae95034806aca700699465be15991b02a8da. All51 complete-workflow/checkpoint records agree exactly. These isolated synthetic derived fixtures are not live capture, twenty-product certification or an arrival burst;120k is **larger-volume derived stress**. Original CLI option `--burst-frames` names that volume input only. Timings and scratch-file counters do not establish V5 Raw-audit acceleration.

| Products / frames | Old workflow median(s) | Buffered candidate with original winner median(s) | Improvement |
|---|---:|---:|---:|
| 4/40000 | 174.196448 | 9.049161 | 94.81% |
| 20/40000 | 177.359070 | 12.678402 | 92.85% |
| 20/120000 | 536.361885 | 38.062703 | 92.90% |

Final current candidate full engineering:2024 PASS/24 explicit online skips/5 deselected/13 pre-existing fork warnings,176.10s; Ruff/mypy293/M0/Go PASS. No source candidate is merged or deployed yet.

Independent exact-source/protocol supplement ACCEPT for code/compatibility and justified deferral rationale, performance/release remains PENDING. Report7e9815c9108c4423f78f7180f11945edc5b811f26164ea6ab268c9ad822d67e4 / identityeab032fc054006f5d5034a70baededf50a25d49af9602345056ce5c3eea30500. See ../reviews/2026-10-05-q6-source-protocol-decision-review.md. It reconstructs original exact premeasurement protocol aef5eded…b371cf; original protocol/decision-table bytes above remain frozen historical PENDING states. Current decisions and release state are recorded in subsequent evidence.

Required representation supplements use selected exact-byte source bundle1ec10219efc9415f3a96d540b0535dab21fddc1fe091c6c542ae6fe98cb9d14c. Fresh-process baseline/final RSS for each4/40k,20/40k,20/120k workflow avoids cumulative-process `ru_maxrss` comparisons. Normalizer and Replay scratch are counted after flush at every retirement boundary; original retained-directory/published-file totals exclude removed scratch and are not transient peaks. Full INSERT+COMMIT and stateUPDATE+COMMIT cost plus exact actual SQL trace/query plan include remote ownership exclusion. Ready-book complete save/fsync/Catalog registration/restore keeps original15%/20ms gate; no bootstrap-only or hash-only substitution.

LOCAL ready20/1000 mediansold0.189392375s/new0.144416208s; ready20/5000 old1.010819083s/new0.750493208s. Complete1000-row insert+commit old0.001583667s/new0.001981708s; state-update+commit old0.001506291s/new0.002386542s. Receipt5c177c415c916507fd134bc91b43f04185250b2033145f5b434d6de08789f16d. VPS supplements are still required; these local values alone do not admit the index/checkpoint release.

Corrected local scratch smoke final4/40k matches every original run: fresh RSS376946688B, Normalizer254350272B/Replay37522668B transient scratch, all scratch retired. Receipt under isolated-final-_qmlpmdo. Prior helper attempt omitted normalized dataset-version directory in its observer path, failed its observation assertion after matching product output; retained isolated-final-tltgzvm0, no product-code failure. Full local supplemental root selected-supplement-maiqcwsc is running. Preserve initial executed helper72f3f8ad…30b9b and metadata-only follow-up03f22f8e…b28845; worker logic is unchanged. No Formal T0 or production-data work.

## Actual native supplementary dispatch checkpoint

October5 actual dispatch: frozen helpercf8ae04077a7d3746f2e015d11af769f0f27dee1ff5a1198d12f6134a96c6f97/source bundle1ec10219…b9d14c/wrapper48523aa8522f762278a43c621d68d0fa6497c3749481a8c024af49a0ec29c729 were copied/read back in the same approved VPS controls. Measurement/dispatch design independently ACCEPT; actual representative outcomes PENDING. Runtime wrapper invokes old installed Python only for isolated source-loader/fixture work; no installed source/wheel replacement.

Sole supplemental unit binance-recorder-q6-supplement-20261005.service, invocatione1d5801e2965409bb74f903462738c08, PID135715/active-running/ExecMainStatus0. It only waits for the sole original benchmark's complete utc_finish_ns, then sequentially executes selected-source supplements and the FD/hash-read bounds fixture. Original main still active with increasing CPU; latest full20/40k old737.499s versus new70.402s in one pair. No competing benchmark, OS cache dropping, production mutation or Formal T0. Unit-scoped5h maximum is separate from production OS-window authority; parent waiting deadline10800s, no extension. If the original report is mid-write/incomplete JSON, only the waiting loop retries within that same deadline; completed-worker inputs/errors remain strict.

Read actual supplement.log under the controls for selected-supplement-* and bounds-evaluation-* result paths after completion; do not restart a healthy job. No startup or pending receipt counts as performance acceptance. Original main report remains q6-derived-jl1_fbqk/results.json. Expected remaining work is further20/40k and120k repeats, then selected supplements; native jobs survive model quota interruption. Hourly vps-2h remains ACTIVE, same frequency and meaningful-change-only notifications, ending after new Q7 full closeout. Q7 still NOT_STARTED/no inherited T0.

LOCAL fresh-process intermediate RSS4/40k old355532800B/new368525312B;20/40k old381583360B/new424673280B. Scratch bytes are exact equal254350272B and254052960B Normalizer, with Replay37522668B and7495868B respectively. Actual memory increase is retained; further larger-volume and VPS evidence/independent interpretation are pending. Do not use cumulative original-process high-water marks to assert per-variant no regression.

Final independent supplemental measurement-design/binding ACCEPT: report0fdf3e82d95bd9b747674b74c3c7c9013bd3adba89b107935e26b89865a83f61/identity03f79ff06569b6ca32b144e95de11a3048d37e61dd1290f84531741ec81ebd08. See ../reviews/2026-10-05-q6-measurement-supplement-design-review.md. It binds final helpercf8ae040…c6f97, all four exact source modules, six completed-report/reference predicates and three finite parent-wait publication-race predicates. It does not claim actual VPS results or final performance/release qualification.

## Local supplementary completion and next release helper

LOCAL supplementary result6471eeee5cbbf6c5eb3757f143b6838e0930b72268ec25a4c89a2e6d5dad12a1 COMPLETE; six fresh-process baseline/final workloads all match every original complete result.20/120k RSSold517193728B/new545816576B, Normalizer scratch763376160B/Replay22538268B equal. Earlier4/40k and20/40k memory increases remain visible above; no representative VPS-memory PASS claimed. Parent executed retained72f3 helper with later metadata/assert-only worker revisions retained; the complete measured worker algorithms remained identical. Final VPS helpercf8ae040…c6f97 stays frozen.

Next isolated cloud build helper is prepared but UNRUN:
/private/var/tmp/bmdr-q6-bench-controls-jkgugp82/build-q6-release.sh
SHA8e04e612eda2936ab4010b3d54f0f66bc01d8577cd1c2459dfd87799fbccf5e5.
Independent DRAFT DESIGN/BINDING ACCEPT report78282dbc7ca2e0c581b07a65d80fd737291a69c8c6749f0cc2002ddaed804d37/identity16125a4035949869c4d9f421fbf216c646fe40964f3b4af2350cd472d4be3672. See ../reviews/2026-10-05-q6-cloud-build-draft-review.md. Two initial draft blockers (archive/source provenance and reuse/truncation of prior outputs) were corrected before execution. It requires a new unique canonical q6-release child, exact accepted source/CI commit and git-archive-without-prefix PAX commit+SHA, full archive/source inventory before tests and revalidation after build, exclusive logs/receipt, own child TMPDIR/cache, no inherited PYTHONPATH, clean locked wheel/no-deps smoke/121 package Python bytes and exact28 dependency environment. No installation/deployment or service control. Execute only AFTER representative benefit/final independent implementation/contract review, selected source commit and its exact dual CI succeed. Earlier checkpoint e03877a exact workflow37257552586 SUCCESS is documentation-only, not that candidate CI gate.

After those gates: preserve old artifact, stopped canonical deployment VERIFY, actual all-enabled warm-up/resource admission, fresh full authoritative baseline/independent eligibility, actual growing-corpus reserve/temp/staging and realistic two-audit window, then strict prestart and sole new Q7 own7200s T0. Do not repeat unchanged old wheel, old baseline, historical Q5 closeout or idle engineering live scans. Native existing comparison/supplement must finish before dependent performance work. Q7 NOT_STARTED/PRODUCTION_READYNO/R078OPEN.

## Representative original completion and retained supplement failure

October5 original native comparison completed05:30:41UTC/success with all51
workflow records; actual result43efc0344814706d465531327fa5809b93ea4a08a742f62531b436bc0bc822c4.
Exact persisted identities/builds/row counts/all provenance/replay orders equal
across all nine runs for each input. Current selected code retains original
winner sorting (`candidate_sorted`); rejected sorting removal is not shipped.

| Same-input complete Normalizer + verified Replay | Baseline median wall/CPU s | Selected median wall/CPU s | Wall reduction |
|---|---|---|---|
|4 products/40k frames|726.054723/726.176416|52.532921/52.862335|92.764606%|
|20 products/40k frames|753.104803/753.897028|70.402352/71.321314|90.651719%|
|20 products/120k frames|2300.205698/2302.717743|207.340620/210.205528|90.985997%|

Complete derived-work benefit/CPU thresholds PASS; representative per-variant
memory/temp/capture/release remain pending. This study is not expanded live or
arrival-burst certification. Catalog identical selection: median27.589989ms saved
per call, VM601300→<100 (100-step measurement granularity), migration0.315746s,
added8331264B. Indexed selection/migration/size thresholds PASS; full commit and
actual remote-aware query plan supplementary measurements remain pending.

Original supplement failed05:31:03UTC/exit1 before resource work: actual frozen
VPS tool813cb961 has inline two markets, while helpercf8 used a later LOCAL
MARKETS constant. Retain supplement.log, native failure and child
selected-supplement-qtat3cjs/result9f1584b7535021688959566b40b27c3c29a6a52eb27e0531d59c806e3c9e9354.
This is helper binding failure, not product failure or Formal evidence.

New supplement-q6-v5.py SHA4f02f8891df798ddc6816ac02db18416e63b26efd8525bb5251933da15d6a52f
changes only that loop to the original same-order ('spot','um_perpetual') tuple.
New wrapper6ebf4e2c4929dea30f70ef298af95af220272516b5da11d6b0385e5be2ba275f
uses new helper/log+noclobber, preserves old bytes. Independent exact-tool helper
compatibility/dispatch ACCEPT; LOCAL actual frozen813cb961 tool:24 ready-checkpoint
and6 Catalog rows COMPLETE41940f857d088f64242515839e2a0949a752ede719bacae7ca0a51cc962b5b5a.

Sole new native binance-recorder-q6-supplement-v5-20261005.service/invocation
d91d20b3999e4390aa85248aabeee207/PID141937 started06:27:16UTC; fixed18000s
isolated budget, same controls, supplement-v5.log. Original job/result is not
restarted. Before launch actual original successful/MainPID0, old supplementPID0,
Recorder inactiveANDdisabled, no existing new unit/log and exact helper/wrapper/
original report digests verified. Actual resource/performance release remains
PENDING; no wheel/deployment/baseline/newT0. c94c83b dual CI37258717201 SUCCESS
is doc-only. All production/old evidence/corpus retained.

[Independent v5 compatibility review](../reviews/2026-10-05-q6-supplement-compatibility-review.md)
ACCEPT, reportfe330b2c…15a26/identity1fefbd6f…5c6c4d; actual resources/release pending.

## Actual second helper failure and minimal resource continuation

October5 v5 native FAILED06:41:45UTC, partial result96beaa3c8285986c5690d5f6f9a996cedc8fddea292effdf312ba014bc1f51da, selected-supplement-s0326anw.24 ready checkpoints and6 Catalog commits/SQL records completed, normal[]/no finish; this is not a full supplement PASS. Failed baseline derived files retain the same normalized/build/count identities; independently replaying retained output yields32000 rows/SHA91b3b47b…eb522, matching all original nine references. Actual focused40-frame reproduction observed normalizer1/replay0. VPSPython3.12.3 uses bound tempfile._rmtree, while original helper patched only shutil.rmtree. No product failure is established; original missing stderr is not reconstructed as a historical traceback.

Minimal supplement-q6-v6.py8f68da559140751726b4736e5c1c84f88b5749982b13a334210b997cbb41c17c hooks/restores that same existing bound alias. Actual tiny baseline/final both exit0/normalizer1/replay2/match all9: helper-v6-binding-smoke-e13y5x1g/result551200e2afe1119b3759046830493c9616a0343e36ddaeffbca2910219c4d0b1. Parent now retains per-case stdout/stderr/returncode before raising. New wrapperc9762f19d74ad326b0ec86ec7c770352e84815c04739b4b924381c1fecf3c647 uses exclusive supplement-v6.log and --normal-only; only six fresh-process resource cases and unchanged bounds run, no main/ready/Catalog rerun. Native binance-recorder-q6-supplement-v6-20261005.service inv192e43ddbfba4360b211e38b22569775/PID144897 started07:25:20UTC, fixed18000s isolated budget. Actual dispatch digest/oldPID0/newnot-found/RecorderinactiveANDdisabled gates passed; v6-dispatch-request.json retained. Expected checkpoint08:50UTC; actual finish/full output required.

Ready20/1000 VPS old1.040638/new1.310539s and20/5000 old5.606299/new4.086449s. Original checkpoint performance gate NOT_ACCEPTED; retain only necessary File/Catalog same-frozen-snapshot correctness, no speed qualification. Complete1000-row INSERT+COMMIT medians old0.007793772/new0.008164371s; stateUPDATE+COMMIT old0.007565053/new0.009715822s. Added writes are explicit small absolute costs; final representative resource/benefit interpretation pending. Actual remote-aware selection trace retains local+remote NOT EXISTS and uses additive index.

Independent scoped helper/design ACCEPT: ../reviews/2026-10-05-q6-resource-helper-and-q7-preparation-review.md; report5664b088…98b5e46/identity21b699df…01dce8/18 isolated checks. Representative release remains PENDING.

Prepared next-phase helpers (private controls, all UNRUN) are deploy-q6-release-v3.pyfc2871ac…effc34aa, q7-light-warmup-v2.py39f72b80…4b36e80, q7-stop-recorder.pyd924fc97…555400f, q7-nonformal-cost.pyca820a52…129d443, build-q6-release.sh8e04e612…ccf5e5. Deployment draft review reportdb760348…329844f/identityf79faedf…dc94b3b accepts frozen-copy binding/old-venv custody/failure restoration including attempted timer stop. Phase-sequence review reportf35b79a2…27bcf0/identity2659c44f…5607c6 accepts the minimal sequence: actual all-enabled light warm/resources → stop/disable/drain/freeze → sole full authoritative baseline (both LIVE+verify+independent) → healthy restart/fresh publication → separately labelled NONFORMAL start/immediate/normal/missed/catch-up cost using same baseline → fresh strict/current healthy-incarnation dispatch/own unique Formal7200s T0. No duplicate engineering full Raw scan or second unchanged baseline. NONFORMAL path/run earns0 credit; cannot become predecessor or transfer T0. Actual launcher, profile/current-corpus forecast/finite-window, artifact and phase qualification still require review/readback.

## October8 actual completed representative resource readback

The sole corrected supplement completed October5 08:41:21UTC, native journal
success/CPU4571.418s/565.3M cgroup peak/0 swap. October8 readback shows inactive,
not an advancing job. Six child exit0/all original nine-reference identities equal.
Resource resulta5cffbcf151507c24d3b273270fd9fd275e85f65166ce7b0e61fa93c8db00410,
selected-supplement-hirhc6s4/result.json; finish1791189666002515478UTCns.
FD/hash result9cda0b2a39ed2f072953d7aea8945946eceaf1a598f8d869089746a398585911,
bounds-evaluation-3qs95wo0/result.json. Original parent/sources/tool unchanged.
Private exact copies and native readback are under
/private/var/tmp/bmdr-q6-bench-controls-jkgugp82.

|Fresh-process case|Baseline RSS B|Final RSS B|Normalizer scratch B|Replay scratch B|
|---|---|---|---|---|
|4/40k|343105536|338690048|254350272|37522668|
|20/40k|315797504|312463360|254052960|7495868|
|20/120k|356216832|363884544|763376160|22538268|

Scratch is measured after flush at all actual retirement boundaries, not continuous
filesystem sampling. Sizes equal for baseline/final.20/120k final RSS increases
7667712B/2.15%; includes fresh imports and fixture clone, with substantial absolute
headroom on the5.7GiB VPS. Supplement instrumentation timing is not substituted
for original three-repeat benefit timing. All reference results remain exact.

F4: three original128-run/64FD failures errno24, three candidate exact1024-row
completions/33 owned handles/all closed, peak tree913704→1027961B; one extra
complete scratch generation. Bounds source AST proofcdee2557…7c57 binds current
selected merge and helper original-candidate code; only winner differs and merge
does not call it. Original source/Raw writers/archive/full validators untouched.
Hash fixture median stored-only0.006870s versus full validation0.114333s on four
chunks1.319MB stored/23.876MB decoded; this scoped upper-bound work does not
meet the predeclared10%/0.5s complete-batch fusion gate. No fusion shipped.

Proposed final decisions: buffering ACCEPTED for complete derived benefit;
Catalog index ACCEPTED for selection benefit with explicit small commit costs;
F4 ACCEPTED for descriptor bound; checkpoint performance NOT_ACCEPTED, necessary
snapshot correctness retained; variant-sort removal REJECTED/reverted. Of the
five directions, fusion/rotation/archive cadence/REST scheduling DEFERRED for
the already independently reviewed measured limitations and missing expanded
live/outage traces, not falsely labelled passing comparisons. R078 stays OPEN.
Final independent benefit/source release review is advancing. Exact candidate
CI/locked wheel/cloud/deployment/new Q7 qualification remain pending.

Independent final review requested one narrowly affected F4 RSS measurement.
New helperc4a8a4f92ff66c389b2b37d3eb32fc2f710f3818d2418b4bd6a617e332c39c65
is byte-equivalent to original bounds helper after removing the single fresh-child
ru_maxrss field; independently accepted design. New six FD child cases only,
no main/normal/hash repetition: binance-recorder-q6-f4-rss-20261008.service
inv2c93acc0f8964ee68575996e2df3a326/300s finite budget. Complete result
8d525724cf7adbb570a90df1eb7ff9c45601c2a26119bc28bfa44f19bf72fcd4.
Baseline RSS74350592/74350592/74223616B (original errno24 failures), candidate
74608640/74743808/74493952B (three exact stable outputs,33 handles/all closed).
Adds ~0.4MiB at the measured collapse fixture; ordinary workflows remain covered
by the six full fresh-process measurements. Explicit helper/bundle/tool/selected
merge AST binding is retained in private vps-bounds-rss-binding.json.
Benchmark shared environment is original canonical identity0ea93b2c and lock
44cd3733…6f335, exact28 dependencies per final Q5 handoff VERIFYc19c1e6e…a8164;
new clean locked wheel qualification remains separate and pending.

Final independent SOURCE/48h/benefit/resources/qualified decisions ACCEPT, October8.
Reporteb711260…ce3efb/identity02bc80ca…818f1/20 focused PASS; see
../reviews/2026-10-08-q6-final-benefit-release-review.md. No necessary source change.
The decisions above are now accepted for candidate freeze. Source commit/exact
dual CI/new locked wheel/cloud/deployment/Q7 still require actual evidence.
Prepared sole Q7 root /srv/recorder-data/recorder-archive/evidence/Q7-optimized2h-20261008-iu99yb8b;
no T0. Prior canonical VERIFYc19c1e6e…a8164 PASS;19451 retained chunks/0backlog,
28645597184 root-free B/~2.10TB archive-free, Recorder inactive AND disabled.
