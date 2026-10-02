# Risk Register

Updated October2,2026. Restored SSH after the owner's local VPN/proxy correction.
Exact independently accepted48ca920 is installed/verified;1942 local tests,
exact-source dual-platform CI, clean locked wheel and112 cloud V5 tests PASS.
637 retained new chunks are archived; engineering full baseline/completed verification PASS.
Actual all-enabled throughput/catch-up and cumulative forecasts remain OPEN.
Original nonformal failures and historical R-078 retain their dispositions.
Owner resumes through completed Formal2h, stopping before12h.

## Current follow-up

| Risk | Current control and evidence | Remaining work |
|---|---|---|
| R-076: ordinary observations grow with historical corpus | V5 uses bounded durable deltas; implementation review and offline gates passed | Complete production qualification |
| R-077: incomplete or inconsistent terminal authority | Private manifest freeze, Catalog snapshot, causal replay and independent verification are implemented and reviewed | Complete baseline and terminal production verification |
| R-078: full Raw/archive audit takes too long | Production baseline stopped after more than 14 hours, with 87,423 of 143,362 first-pass records published | Assess full-audit practicality and qualification scope; P2 remains open pending complete measurements and independent review |
| R-034: Spot bootstrap source conflict | ADR-0011 retains exact Raw and the `lastUpdateId + 1` interpretation; offline evaluator compares boundaries | Resolve normative-source conflict before changing that behavior |
| Long-run resources and capacity | Historical bounded measurements and current partial audit samples are retained | Measure memory, queues, archive backlog and capacity for the selected artifact/profile |
| Auxiliary REST availability | Shared stop-aware USD-M cooldown and explicit side-data gaps | Cooldown is process-local; live auxiliary coverage remains incomplete |
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
| F1: V5 online service capacity below four-product row arrival | P1; original Q3 byte starvation/causal error preserved; ADR-0037 exact shared v2 representation and strict boundaries independently accepted at c87d580 | Actual all-enabled cloud throughput/catch-up gate OPEN; measure before T0 |
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
