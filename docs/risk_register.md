# Risk Register

Updated October 2, 2026. Merged/deployed base is PR #78; a local optimization
candidate is implemented and not deployed. Current operations
are recorded in [production state](CURRENT_PRODUCTION_STATE.md).

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

The owner stopped live qualification and paused its monitor. Next development
work should use the [handoff](PROJECT_HANDOFF.md) and retained
[partial production measurements](milestone_acceptance/M22.9-v5-deployment-owner-stop.md).
The owner selected safe cleanup and a fresh independent qualification corpus.
Execution has not occurred; existing archive/evidence and historical R-078 remain.
No old baseline integrity PASS or FAIL was published. See the
[cloud diagnosis](reviews/2026-10-02-cloud-performance-and-38h.md) and [38h plan](qualification_to_24h_plan.md).

## October 1 architecture-review findings

These are independently reproduced review findings against main
`d6f37576f5b044bf578501cba291fe889e45a919`, not claims of observed V5 Formal
failure. F1 has an implemented candidate pending review/cloud qualification;
Q1 implements F2/F3/F5/F6/F7 controls pending independent release review.
See [Q1 evidence](milestone_acceptance/Q1-correctness-and-bounded-diagnostics.md),
[details and probe results](reviews/2026-10-01-architecture-review.md)
and the [sequenced remediation plan](qualification_to_24h_plan.md).

| Finding | Priority / evidence | Planned control |
|---|---|---|
| F1: V5 single-page service rate below normal four-product row arrival | P1; initial reproduction remains historical; ADR-0035 candidate consumes 480/300 rows and drains a missed cadence in real seal/archive fixtures | Independent review and complete exact-artifact cloud observations before T0; no live closure claimed |
| F2: failed heartbeat leaves collectors running and normal shutdown status | P1; Q1 guard/wait/drain regressions pass across startup/recovery/running/drain, including real store failure | Q2 independent review; installed artifact unchanged |
| F3: normalized keys omit product/stream identity in some event kinds | P1; Q1 namespaces all candidates and versions new build identity; old/new replay compatibility passes | Q2 review of ADR-0036; explicit rebuild needed for corrected derived results |
| F4: normalization merge opens every run concurrently | P2; 40 runs retain 40 readers | Offline follow-up after 38h qualification: bounded hierarchical fan-in |
| F5: reconstructor retains every quality audit in memory | P2; cloud synthetic 100,000 audits remain 256 with Q1; full observer calls and gap/book facts agree | Q2 review and Q3 actual selected-scope live RSS; no whole-process/cloud capacity verdict yet |
| F6: inaccessible repository-discovery cwd crashes CLI | P2; Q1 skips optional discovery failures; denied/removed-cwd installed-entry and data-permission regressions pass | Q2 exact-wheel review/CI; installed artifact unchanged |

October 2 [plan recheck](reviews/2026-10-02-plan-recheck.md) adds **F7 (P2)**:
enabled auxiliary `FAILED` is visible in detail but omitted from aggregate
DEGRADED health while cores are READY. An offline state-builder reproduction
confirms this summary inconsistency. Q1 now corrects aggregate/stream/owner
health without stopping cores; terminal owner isolation regressions pass.
Independent review remains Q2. The recheck also requires live F5 memory
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
