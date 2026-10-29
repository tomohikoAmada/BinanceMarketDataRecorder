# Risk Register

## Current P2 scope exception — 2026-10-29

- The owner approved an initial-release omission of only U/C dated premium-index bars;
  perpetual premium and all other selected scope remain. Official applicability stays
  unknown and the history is preserved. [ADR-0055](adr/0055-initial-production-dated-premium-exception.md)
  supersedes earlier notes that made an official answer a first-release acceptance gate.
- [Binance issue #585](https://github.com/binance/binance-connector-python/issues/585)
  is submitted with owner authorization. A future official answer is separate follow-up.
  Missing this series can limit future research; exact later recovery is not promised.
- The [implementation candidate](milestones/P2/initial-production-20261029/README.md)
  explicitly selects `capture_dated_premium_klines=false` before admission/scheduling.
  New dated products follow the same rule. The report discloses 120 uncaptured sources
  with unknown applicability, a new-instance effective boundary and unchanged historical
  evidence. The retained-directory nominal budget becomes 765.328366/min; WS and other
  source descriptions are unchanged. This scope reduction establishes no CPU/VPS gain.
- Independent review accepts the source selection/reporting/historical-read boundary;
  71 affected checks and exact 13602c8 dual-platform CI/logs are verified. Generic/old
  configuration defaults still capture the full target.
  Do not apply the exception to an already-running window or silently drop other sources.
- **P2-R5 CLOSED:** independent review accepts controlled writer timing and strict
  semantic assertions for both legal shutdown/reset/close layouts. 87 affected checks
  and both shutdown cases pass independently; 1c1785c and final HEAD 464ed56 dual-platform
  CI/logs are verified. Exact payload/order, connection/marker layout, completeness
  and Catalog gap assertions remain; production code and old failure evidence are
  unchanged. See [closure](milestones/P2/review-20261029.md#r5-independent-closure).
- R1/R4 and the initial-release implementation stay accepted. P2 is ACCEPTED with no new
  blocking correction. P3/production have not started, and capacity/99.5% qualification
  remain future work. Historical unapproved/unsent and mandatory-official-reply statements
  below describe earlier states.

## P0 current risks — 2026-10-27

- [PR #87 review](milestones/P0/review-20261026.md#pr-87-复审2026-10-26) accepts
  R1/R3/R4; the remaining R2 defect is now corrected with 26 focused tests.
  The classifier retains each source's new highs through partial drains and
  distinguishes the review negative from bounded oscillation/recovery.
  [V4 interpretation](milestones/P0/corrections-20261026/r2-reclassification.json)
  of saved boundaries is appended without recapture; originals are unchanged.
  Five sparse observations remain a comparison screen, not a long-run capacity
  or boundedness proof; P1 still needs matched work/tail and interleaved A/B.
  USD-M latest/largest selection semantics remain unresolved: disclosure is fixed,
  not the exchange conflict. Do not guess either rule or transfer U behavior to C.
  The unprofiled run's recorded label is STEADY; its profile is NON_STEADY and cannot serve
  as a performance comparison. Preserve representative raw messages and compare
  matched per-source window work/tail/background workload for P1.
- R-078 remains OPEN: no new-plan BTC/ETH full-scope production qualification exists.
  Original core baseline and the isolated full-current-feature steady baseline are
  separate model workloads; neither proves the VPS can sustain all P2 sources.
- The [coverage inventory](milestones/P0/coverage.md) records current official USD-M
  top-ratio key restrictions with no equivalent public replacement. Historical no-key
  implementation cannot establish current coverage. P2 must retain the explicit
  restriction and must not add API keys or relabel global ratios as top ratios.
- P1–P3 have not yet implemented the new raw-path, market-identity, source-monitoring
  and incremental verifier contracts of ADR-0051. P4 cannot substitute legacy sampled
  pass rates/full-history helpers for those missing capabilities.
- Source publication, routing and permission semantics may change before P2; refresh
  only affected official pages and record exact differences. HTML fallback shells
  are not successful endpoint-document retrievals.

Owner direction October26, 2026: new work follows the
[production milestone plan](production_milestone_plan.md). Current priorities are
coverage/identity gaps(P0/P2), Raw-path cost(P1), recovery and background workload
coupling(P3), and actual shared-VPS headroom(P4–P6). No risk is closed merely by
publishing this plan. Historical rows and plan links below preserve their original
evidence; their old execution/approval sequences are superseded.

Updated October4,2026 (Shanghai). Retained Q4 checkpoint: corrected source23ea5c4 actual reviewed
eligible2h PASS, both full LIVE/completed verification and independent eligibility/
actual restored-authority handoff ACCEPT. All6718 chunks archived/Catalogok/FKok/
0partials/backlog; RecorderinactiveANDdisabled, normal archive/OSauthority restored.
All14 flags/four products/growing corpus and old evidence preserved. Actual full
terminal50m05.516s; three in-period typed-empty retries/two missed snapshots and
sampling limits remain explicit. Latest recorded Q5 is RUNNING; its full closeout
is pending. Latest owner plan schedules Q6 optimization then new optimized-artifact
VPS Q7/2h, Q8/12h, Q9/24h, Q10/48h (86h), all NOT_STARTED. This does not deploy
new code or grant capacity/duration credit; R078 staysOPEN. See the
[current plan](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b/docs/milestone_plan.md#q6--performance-optimization-and-optimized-release-preparation).
See [current2h record](milestone_acceptance/Q4-formal2h-spot-idle-20261004.md).
R079's minimal correction has passed its own exact release/cloud/baseline/warm-up/
Formal2h/terminal/review gates; the old cf3909e9 PASS remains historical only.

## Current follow-up

| Risk | Current control and evidence | Remaining work |
|---|---|---|
| R-079: Spot snapshot idle cleanup starves the event loop | Deterministic old-method regression and CI diagnostic establish the bug; source23ea5c4 reclaims exact awaited workers. Local1998/exact CI37130186751 bothPASS/independent source ACCEPT; exact wheel/cloud233/canonical deploy/engineering+authoritative baseline/warm-up/own Formal2h/bothLIVE/verify/independent eligibility+handoff ACCEPT | CLOSED for this correction and selected2h scope; preserve exact regressions/old evidence, later stages remain separate |
| R-076: ordinary observations grow with historical corpus | Bounded deltas/offline review and actual reviewed2h online chain pass | Finish current Q5; after Q6, measure and qualify the new Q7–Q10 2h/12h/24h/48h chain without transferred credit |
| Sparse initial WS boundary |605bc16 failed nonformal catch-up; ADR-0038 authentic initial-boundary correction retained in the independently accepted corrected artifact | Actual corrected warm-up/2h/terminal ACCEPT; preserve original failure/strict classifier and repeat later-stage gates |
| Very long consecutive WS failure backoff | Offline unlimited-failure fixture exposes existing exponent OverflowError at1025 failures before the configured cap can apply; separate from the observed startup failure | Proportionate bounded-exponent follow-up; no claim that these attempts occurred on the VPS |
| R-077: incomplete or inconsistent terminal authority | Corrected4434 baseline/6718 terminal both fullLIVE/completed verification/independent review PASS | Retain exact authority and repeat gates for later stages |
| R-078: full Raw/archive audit takes too long | Historical143362 scope remains unfinished/retained; corrected6718 scope complete bothLIVE+verify wall50m05.516s | OPEN; Q6 measures reduced work while retaining both LIVE passes. Before every Q7–Q10 T0, reforecast the actual retained/growing corpus, capacity/temp/staging/controls and practical full-audit windows through the new86h chain; prior38h peak reserve failure unresolved |
| R-034: Spot bootstrap source conflict | ADR-0011 retains exact Raw and the `lastUpdateId + 1` interpretation; offline evaluator compares boundaries | Resolve normative-source conflict before changing that behavior |
| Long-run resources and capacity | Corrected2h sampled host execution15.78%/minavailable4.43GiB/wholehostswap268KiB with0 pagesinout; native full audit cgroup1182.3MiB/swap0 | Selected2h ACCEPT only; later stages require current reserve/temp/staging/backlog/control and schedule admission; unresolved38h peak scenario remains explicit |
| Auxiliary REST availability | All-enabled reviewed2h strict/sampled checks pass; two missed polling snapshots and actual recovery retained | Cooldown remains process-local; inter-sample failures and snapshot market-time coverage limits remain explicit |
| Portable archive rollout | Transfer, receipt binding and verified source retirement are implemented | Complete chosen client/platform and external-media certification |

The owner selected safe cleanup and a fresh independent qualification corpus.
[Q3](milestone_acceptance/Q3-fresh-corpus-and-cloud-warmup.md) has retained old
custody, installed the exact release and exercised a new corpus. Its observer
failed with `pending causal reference cap exceeded`; this blocks Formal T0.
Previous cloud documents approached 7 MiB delta admission while archive cursors
lagged and pending references grew. Repeated lifecycle bundles are a measured
optimization opportunity; ADR-0037's bounded shared representation is now
implemented, independently accepted and installed as c87d580.
No RAM/swap exhaustion supports a hardware-upgrade diagnosis. Both
byte starvation and transient family-replay cap behavior were reproduced and
corrected; repeat actual concurrent Recorder/archive gates. Do not repeat
the unchanged live run. Existing archive/evidence and historical R-078 remain;
no old baseline integrity PASS or FAIL was published. See the
[cloud diagnosis](reviews/2026-10-02-cloud-performance-and-38h.md) and [38h plan](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b/docs/qualification_to_24h_plan.md).

## October 1 architecture-review findings

These are independently reproduced review findings against main
`d6f37576f5b044bf578501cba291fe889e45a919`, not claims of observed V5 Formal
failure. Q2 independently accepts F1 and Q1 F2/F3/F5/F6/F7 controls and freezes the
exact corrected wheel. Real selected-scope qualification remains Q3/Q4.
See [Q2 evidence](milestone_acceptance/Q2-reviewed-release-and-cloud-design.md).
See [Q1 evidence](milestone_acceptance/Q1-correctness-and-bounded-diagnostics.md),
[details and probe results](reviews/2026-10-01-architecture-review.md)
and the [sequenced remediation plan](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b/docs/qualification_to_24h_plan.md).

| Finding | Priority / evidence | Planned control |
|---|---|---|
| F1: V5 online service capacity below four-product row arrival | P1; original Q3 byte starvation/causal error preserved; ADR-0037 exact shared v2 representation and strict boundaries independently accepted at c87d580 | Actual cf3909e9 all-enabled normal/missed/catch-up independent ACCEPT; Formal qualification remains |
| F2: failed heartbeat leaves collectors running and normal shutdown status | P1; Q1 guard/wait/drain regressions pass across startup/recovery/running/drain, including real store failure | Q2 review ACCEPT; exact reviewed artifact deployed in Q3 |
| F3: normalized keys omit product/stream identity in some event kinds | P1; Q1 namespaces all candidates and versions new build identity; old/new replay compatibility passes | ADR-0036 review ACCEPT; explicit rebuild needed for corrected derived results |
| F4: normalization merge opens every run concurrently | P2; 40 runs retain 40 readers | Evaluate in Q6 after current Q5 full closeout: bounded hierarchical fan-in, exact ordering/dedup equivalence and measured fd/RSS/temp/I/O tradeoff |
| F5: reconstructor retains every quality audit in memory | P2; cloud synthetic 100,000 audits remain 256 with Q1; full observer calls and gap/book facts agree | Q2 review ACCEPT; Q3 actual selected-scope live RSS; no whole-process/cloud capacity verdict yet |
| F6: inaccessible repository-discovery cwd crashes CLI | P2; Q1 skips optional discovery failures; denied/removed-cwd installed-entry and data-permission regressions pass | Q2 exact-wheel review/CI PASS; exact reviewed artifact deployed in Q3 |

October 2 [plan recheck](reviews/2026-10-02-plan-recheck.md) adds **F7 (P2)**:
enabled auxiliary `FAILED` is visible in detail but omitted from aggregate
DEGRADED health while cores are READY. An offline state-builder reproduction
confirms this summary inconsistency. Q1 now corrects aggregate/stream/owner
health without stopping cores; terminal owner isolation regressions pass.
Q2 independent review is complete. The recheck also requires live F5 memory
measurement (the finite component control has cloud synthetic evidence), warm-up before the
authoritative baseline, cumulative audit/capacity forecasts and quiet-window
protection through identity-sensitive audit publication. These are current
[plan](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/blob/9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b/docs/milestone_plan.md) gates; none grants Formal credit.

## Existing operational controls

Exact Raw bytes, gap evidence, checksums, verified archive retirement and
artifact identity remain implemented data contracts. Test changes against those
contracts. Keep resource measurements distinct: per-process VmHWM is a sampled
process high-water mark; cgroup memory also includes page cache. Qualification
results describe the artifact and corpus actually measured.

The [historical register](risk_register_history.md) preserves the full risk IDs,
mitigations and incident chronology. Closed or superseded historical findings
are not new task prerequisites. Update this register when a current finding or
control changes.

## Q3 all-enabled supplement — October2

The supplementary independent configuration review reproduced fourP1 findings
(limiter-stop wait, invisible auxiliary termination, unowned Raw mutations,
idle snapshot/metadata durability) and oneP2 (sparse/terminal health). Narrow
fixes and42-context offline coverage pass. The final review additionally closed
enabled STOPPED aggregate-health P2 and table-newline P3. Exact c87d580 final
independent ACCEPT has zero open source findings;1934 local tests, dual-platform
CI, clean locked wheel,252 cloud changed-path tests and21 public-smoke tests PASS.
See the all-enabled review/candidate records. Actual all-enabled cloud
throughput/catch-up remains OPEN and Formal credit remains zero.

Official USD-M detail `.md` routes have moved to the catalog. Current browser-
downloaded official REST/WS schemas establish required unchanged public semantics
with exact hashes in binance_sources.md. The updater correctly rejects landing
HTML; updating its selection is a separate documentation-tool follow-up, not
permission to accept HTML as endpoint documentation. No blocked normative
conflict was found in the health behavior changed here.

## Q3 open-descriptor source retirement correction

The exact original VPS interleaving is unproved. Local real ArchiveManager
source-unlink during Raw scan reproduces the failure. A minimal independently
accepted correction preserves every content check and frozen authority, deferring
local absence without advancing its cursor. Local1942 and cloud112 V5 tests PASS; exact replacement release is installed/
VERIFIED. Actual cloud normal/missed/catch-up remain gates. See
[correction record](milestone_acceptance/Q3-raw-retirement-correction.md).
Owner resumes only through completed Formal2h, stopping before12h.


Latest Q3 checkpoint: engineering637-chunk baseline and completed verification
PASS; auxiliary pre-start blocked repeated empty-response event identity conflicts.
Recorder stopped/disabled,697 chunks/transactions LOCAL_DELETED,0partials/Catalogok.
The [minimal empty-response correction](milestone_acceptance/Q3-empty-recovery-correction.md)
and precise typed-empty observation recovery policy are independently ACCEPTed;
replacement release and actual cloud gates remain. Pre-start/target are strict,
no flag is disabled, no Formal T0/credit. Continue through eligible2h, before12h.


Q3 empty-response identity and the independently reproduced retention-gap
identity P2 are closed in exact605bc16. Precise typed-empty recovery is explicit,
with unchanged actual-data freshness/cursor limits and strict endpoints; generic
errors/conflicts stay blocked. Final56 independent tests and1970 local tests PASS;
CI/clean locked wheel/cloud98/deployment PASS. Actual cloud throughput/catch-up
and cumulative full-audit practicality remain OPEN, no Formal credit.


## Q3 sparse initial capture boundary — OPEN

Actual normal/missed observations fit240s, but catch-up fails unmarked_reconnect
for ETH liquidation: its initial socket was connected before T0 with no messages,
then its first current-process message was compared to the prior-process tail.
Independent replay proves this cause, distinct from later network drops. A minimal
capture startup-boundary correction must retain true unmarked-reconnect detection,
crash consistency and legacy replay. No Formal start or credit; Recorder stopped/
disabled,1182 chunks archived, Catalogok/0partials, archive/OS authority restored.
Measured live resource records do not establish a hardware bottleneck: Recorder
RSS peak269643776B, minMemAvailable4869464064B, swap0, aggregate steal1.715%,
iowait0.516% over1443.562s. These are this nonformal window, not a38h certificate.

## October3 current cloud disposition

ADR-0038 corrected sparse initial startup boundary passes actual all-enabled
normal/missed/catch-up; earlier OPEN section records its time-local failure.
Private discontinuity-array error is minimally corrected and exact release plus
engineering both LIVE/verify PASS. Current Q3 resource/forecast admission ACCEPT,
no Formal credit. Two durable empty responses recover~5s with correct cursors;
all26 owners pass strict/phase gates, failures2 retained. No arbitrary native
rewrite or hardware upgrade is supported by current17.4%busy/~600MiB summedRSS/
swap0 measurements. The2h forecast includes all retained new corpus, both LIVE
passes and explicit sealed/compression staging; peak root14.77GiB>10GiB.
The38h sustained observed-peak/staging scenario leaves8.26GiB<10GiB; it cannot
qualify future capacity. Audit scenarios14h289/700min and38h730/1829min may be
impractical; R-078 stays OPEN. Recompute from actual2h terminal/time/backlog/
staging and resolve the relevant later-stage gate before12h/24h, preserving Raw
and reserve-stop behavior. Current finite quiet window is not automatically extended.

Relative OnActive quiet-window timer shifted after pre-start daemon-reload.
Correction creates/verifies absolute08:52:46UTC expiry before cancelling oldtimer,
preserving original bound/Recorder/observer/T0 and five masked inactive units.
Actual receipt retained; final independent eligibility review must assess it.
Use absolute fixed cutoff for future windows, no silent extension.

## P0 R4 disposition — 2026-10-26

The coverage-disclosure defect is corrected: D11 distinguishes U/C, records the
reference/changelog conflict, and forbids latest/largest assumptions in
completeness, deduplication and derived totals. Exchange selection semantics
remain unresolved; no implementation is authorized to guess them. This does
not invalidate original Raw content/count checks and requires no recapture.

## P2 current source and resource blockers — 2026-10-27

Independent review updated 2026-10-28; see [review](milestones/P2/review-20261028.md).

- **P2-R4 OPEN (new, high):** selected-asset REST polling ignores retained
  Retry-After and always blocks the pool for 120 seconds. Six offline cases
  (Spot/UM/CM × 429/418, server asks 600 seconds) all resume too early. Reuse the
  existing retry-boundary parser, preserve response evidence and shared UM/CM
  blocking, and add focused regressions before P2 acceptance.
- **P2-R1 OPEN:** BTC/ETH current full discovery includes 19 derivative products.
  D06 1s × request weight 5 requires 5,700/min; full nominal steady requests are
  ~5,829/min, exceeding Recorder's 1,200/min reserved share and the official
  2,400/min combined UM/CM pool. The limiter waits honestly; no silent shedding
  or 99.5% credit. On October28 the owner selected option one: explicit per-product
  polling plus a separate official daily-file supplement. The direction is approved;
  R1 remains open for product/date/field verification, measured cadence allocation
  and implementation, not another direction decision. See the
  [updated study](milestones/P2/d06-budget-research-20261028.md).
  Uniform 6s is not automatically accepted and the current default is unchanged.
  Bulk data must not fabricate receipt times or live credit; actual target coverage,
  unavailable fields and possible sampling loss must be measured and disclosed.
- **P2-R2 OPEN:** C dated premiumIndexKlines returns -1121 for listed tested dated
  contracts on both documented DAPI/FAPI transports, while perpetual samples
  succeed; the catalog's BTCUSD example also fails. Current documentation does
  not establish a universal dated applicability rule. Preserve these failures;
  stop semantic inference for that source, do not replace it with a perpetual
  series or retrospectively remove it from qualification. See fixtures and
  [P2 result](milestones/P2/README.md).
- **P2-R3 disclosed:** U dated ticker.ps in the sample equals the dated symbol,
  while depth/bookTicker/RPI ps is the directory pair. Preserve actual payloads;
  directory identity is authoritative. Do not generalize one stream's ps field.
- Full-interval short capture configured 197 native-bar streams but observed 130.
  Missing observations and sparse streams remain unproven, not passed. P3 still
  owns durable source intervals/new-file verification/background qualification;
  no P4+ production run has started. P0/P1 historical evidence is unchanged.

## P2 correction candidate — 2026-10-28 (retained)

- P2-R4: fixed in the correction candidate using existing numeric/date/ban/fallback
  Retry-After parsing and actual remaining time since receive.21 regressions and
  the original6 cases pass. Await independent re-review; historical failures retained.
- P2-R1: measured2s/3s/30s configuration and separate selected-daily-trades importer
  delivered. Nominal778.687/min versus Recorder1200/min, but initial/retry work is extra.
  Ideal daily simulation loses1.297% of supplied rows to finite live windows; it is not
  live completeness. Two CM next-quarter files404 on the selected date, native ID holes,
  missing isRPITrade/live timing and source revisions remain explicit. No live gaps erased.
- P2-R2 remains OPEN and is broader than the first sample: all four tested UM dated
  and four CM dated premium symbols return400/-1121; CM perpetual controls succeed.
  Current official catalog still specifies symbol without a delivery applicability rule.
  No substitute or permanent-not-offered inference. Keep failures and their retry demand
  in the source plan. Up to120 failing interval sources retry at60s; nominal costs must
  not hide this extra demand. See the [actual recheck](milestones/P2/closeout-20261028/premium-applicability.json).

## P2 independent re-review of 195762c — 2026-10-28 (retained)

- **P2-R1 CLOSED for the measured implementation:** fixed per-product cadences, nominal
  admission budget and independent daily supplement pass. 17 retained ZIPs/window results,
  8 imports/123,773 rows and 147 Raw observations independently match their evidence.
  Missing files/fields, file ID holes, finite polling windows and non-live acquisition times
  remain disclosed. This does not qualify full-feature capacity or 99.5% live continuity.
- **P2-R4 OPEN (high, ordering remains):** Retry-After duration/date/ban handling passes
  21 regressions and the original 6 cases. However, poll_once waits for persistence before
  blocking the pool. All 6 new Spot→Spot/UM→CM/CM→UM cases issue a newly admitted request
  after a known 429/418 while persistence waits. Set the existing cooldown before that await;
  retain failure records, storage-error propagation and normal stop. No new limiter needed.
- **P2-R2 OPEN:** U/C dated-premium applicability is still not established. Retain all
  8 failing product observations and their source/retry costs; no permanent-not-offered
  inference or substitute prices.
- Exact-commit dual-platform CI passed, but does not cover the new R4 boundary. P2 remains
  NOT ACCEPTED; PR #88 stays Draft, main/working branches are preserved, P3 has not begun.
  See [review and follow-up](milestones/P2/review-20261028.md#review-195762c).

### P2-R4 admission-order correction candidate (2026-10-28)

The original duration correction and its review evidence remain unchanged. The new
candidate moves the existing rejection gate before the first persistence await, so
storage waiting cannot leave a known rejection's shared pool open to new admission.
Both six-case reproductions now pass; eight new regressions fail on the prior runtime
and pass after correction. Failure bytes still persist, storage errors propagate and
unrelated pools remain independent. Await independent re-review; R2 stays OPEN.
No new network evidence, cadence change, production operation or R1 audit was needed.

Four malformed Retry-After regressions additionally verify that invalid dates or
non-finite numeric directives use the existing missing-header/ban fallback before
persistence, with the original failure header retained. Without this guard, moving
parsing earlier could skip the failure record. The queued73ec0c8 CI was superseded
by the final correction; no old green run is credited to the new source.

## P2 independent re-review of f9aaeee — 2026-10-28 (current)

- **P2-R4 CLOSED:** the existing cooldown now precedes the persistence await. Both
  six-case reproductions, 33 cooldown tests and 42 related tests pass independently;
  one online smoke is skipped. Malformed headers retain their original evidence and
  use existing fallback/ban semantics; storage errors propagate. Exact-commit dual-platform
  CI is verified. No new blocking issue was found in this correction.
- **P2-R1 CLOSED for the measured implementation:** prior acceptance and its limits
  remain unchanged; no cadence/import change or repeated history audit.
- **P2-R2 OPEN:** no new evidence establishes U/C dated-premium applicability. Preserve
  the eight failed product observations, source/retry costs and UNKNOWN coverage.
  Do not infer permanent unavailability from -1121 or substitute another price series.
- Overall P2 remains NOT ACCEPTED. Keep Draft PR #88 and its branch until R2 and final
  acceptance close; main unchanged, P3 not started, no deployment.
  See [independent closure](milestones/P2/review-20261028.md#review-f9aaeee).

## P2-R2 bounded follow-up — 2026-10-29

**OPEN, official semantics required.** [New evidence](milestones/P2/r2-20261028/README.md)
contains 14 native public HTTP requests: eight listed dated-symbol failures, four
successful perpetual controls and two failed dated closed-range variants. Two fresh
directory requests confirm all eight dated identities remain TRADING. Direct HTTP
shows these failures also occur without SDK model decoding; it does not establish
permanent exclusion. Current generated SDK parameters, integration notice and shared
changelog do not resolve endpoint eligibility. A delivery-specific empty funding field
on premiumIndex is not a premiumIndexKlines exclusion rule.

The official-source stop condition applies to this implementation. Preserve UNKNOWN,
the eight product failures and all 120 interval sources/retry costs. Do not substitute
perpetual, mark, index or locally computed basis. An [official clarification draft](milestones/P2/r2-20261028/binance-clarification-draft.md)
is ready but unsent; external submission needs explicit owner authorization. Once an
official rule is available, implement its actual boundary and obtain final P2 review.
R1/R4 stay accepted; no runtime or production change in this follow-up.

## P2 independent evidence review of 066c2c3 — 2026-10-29 (current)

The R2 evidence and clarification draft pass review; no new runtime change or blocking
defect was found. R1/R4 remain closed. Current official endpoint sections still do not
provide dated eligibility, so R2 and P2 remain OPEN/NOT ACCEPTED. The draft is unsent.

Missing dated premium bars limit research that needs that official series; they do not
make other captured market data invalid. The current 8 products × 15 intervals can spend
up to 120 weight/min on repeated failures at the 60-second backoff cap. This is a source
plan estimate, not a CPU or production measurement. Preserve UNKNOWN and failed evidence.

A narrow, explicitly declared initial-production coverage exception is a possible owner
scope choice, not a discovery that Binance never offers the data. It is described in the
[review](milestones/P2/review-20261028.md#review-066c2c3) but is not approved or implemented.
Do not waive acceptance, disable sources or send a public issue based on this note.
