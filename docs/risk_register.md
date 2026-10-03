# Risk Register

Updated October3,2026. Q4 actual reviewedeligible2h PASS, both LIVE/completed
verification and independent eligibility/restored-authority handoff ACCEPT.
3868chunks archived/Catalogok/0partials/backlog; RecorderinactiveANDdisabled,
normalarchive/OSauthorityrestored. All14flags/fourproducts/corpus preserved.
Actualfullaudit34m22s; generic snapshot misses, typed-empty retries, marked WS gap
and sampling limits remain explicit. Q5/Q6 notstarted; historical R078 staysOPEN.
See [actual2h record](milestone_acceptance/Q4-formal2h-20261003.md).
Post-closeout R-079 has a reviewed local source correction; corrected deployment
and its own reviewed Formal2h remain gates before12h. See the
[correction and rerun decision](milestone_acceptance/Q4-spot-idle-correction.md).

## Current follow-up

| Risk | Current control and evidence | Remaining work |
|---|---|---|
| R-079: Spot snapshot idle cleanup starves the event loop | P1; latest macOS CI diagnostic and deterministic old-method regression prove completed workers can remain queued for removal; source23ea5c4 explicitly reclaims exact awaited workers, local1998 PASS/independent ACCEPT/exact CI37130186751 BOTH PASS | Source fixed; complete replacement artifact/cloud/own reviewed2h gates before12h. Installed cf3909e9 is not replaced; original Q4 remains authentic for that identity |
| R-076: ordinary observations grow with historical corpus | Bounded deltas/offline review and actual reviewed2h online chain pass | Complete later12h/24h qualification |
| Sparse initial WS boundary |605bc16 failed nonformal catch-up on a pre-T0-connected sparse liquidation stream's first post-T0 payload; ADR-0038 records an authentic initial boundary without relaxing the classifier | Exact cf3909e9 review/release/cloud and actual warm-up ACCEPT; preserve failures, finish Formal qualification |
| Very long consecutive WS failure backoff | Offline unlimited-failure fixture exposes existing exponent OverflowError at1025 failures before the configured cap can apply; separate from the observed startup failure | Proportionate bounded-exponent follow-up; no claim that these attempts occurred on the VPS |
| R-077: incomplete or inconsistent terminal authority | Actual1640 baseline and3868 terminal both LIVE/completed verification/independent review pass | Retain exact authority and repeat gates for later stages |
| R-078: full Raw/archive audit takes too long | Historical143362 scope remains unfinished/retained; new3868 selected-scope complete both LIVE+verify wall34m22s | OPEN; actual2h scaling gives14h152/361min,38h386/1007min mean/peak scenarios; review later full-audit practicality/capacity before those stages |
| R-034: Spot bootstrap source conflict | ADR-0011 retains exact Raw and the `lastUpdateId + 1` interpretation; offline evaluator compares boundaries | Resolve normative-source conflict before changing that behavior |
| Long-run resources and capacity | Historical bounded measurements and current partial audit samples are retained | Actual2h resources/eligibility ACCEPT; prior38h peak/staging scenario below10GiB reserve remains unresolved; reforecast actual temp/staging/backlog/controls before later stages |
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
[cloud diagnosis](reviews/2026-10-02-cloud-performance-and-38h.md) and [38h plan](qualification_to_24h_plan.md).

## October 1 architecture-review findings

These are independently reproduced review findings against main
`d6f37576f5b044bf578501cba291fe889e45a919`, not claims of observed V5 Formal
failure. Q2 independently accepts F1 and Q1 F2/F3/F5/F6/F7 controls and freezes the
exact corrected wheel. Real selected-scope qualification remains Q3/Q4.
See [Q2 evidence](milestone_acceptance/Q2-reviewed-release-and-cloud-design.md).
See [Q1 evidence](milestone_acceptance/Q1-correctness-and-bounded-diagnostics.md),
[details and probe results](reviews/2026-10-01-architecture-review.md)
and the [sequenced remediation plan](qualification_to_24h_plan.md).

| Finding | Priority / evidence | Planned control |
|---|---|---|
| F1: V5 online service capacity below four-product row arrival | P1; original Q3 byte starvation/causal error preserved; ADR-0037 exact shared v2 representation and strict boundaries independently accepted at c87d580 | Actual cf3909e9 all-enabled normal/missed/catch-up independent ACCEPT; Formal qualification remains |
| F2: failed heartbeat leaves collectors running and normal shutdown status | P1; Q1 guard/wait/drain regressions pass across startup/recovery/running/drain, including real store failure | Q2 review ACCEPT; exact reviewed artifact deployed in Q3 |
| F3: normalized keys omit product/stream identity in some event kinds | P1; Q1 namespaces all candidates and versions new build identity; old/new replay compatibility passes | ADR-0036 review ACCEPT; explicit rebuild needed for corrected derived results |
| F4: normalization merge opens every run concurrently | P2; 40 runs retain 40 readers | Offline follow-up after 38h qualification: bounded hierarchical fan-in |
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
[plan](milestone_plan.md) gates; none grants Formal credit.

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
