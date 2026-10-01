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
others remain open. See [details and probe results](reviews/2026-10-01-architecture-review.md)
and the [sequenced remediation plan](qualification_to_24h_plan.md).

| Finding | Priority / evidence | Planned control |
|---|---|---|
| F1: V5 single-page service rate below normal four-product row arrival | P1; initial reproduction remains historical; ADR-0035 candidate consumes 480/300 rows and drains a missed cadence in real seal/archive fixtures | Independent review and complete exact-artifact cloud observations before T0; no live closure claimed |
| F2: failed heartbeat leaves collectors running and normal shutdown status | P1; injected state-write failure through ServiceRuntime | Q1: supervise required task failure and unexpected return; preserve orderly stop and FAILED reason |
| F3: normalized keys omit product/stream identity in some event kinds | P1; two distinct-symbol snapshots collapse to one output | Q1: consistent namespaced dedup identity and versioned immutable builds |
| F4: normalization merge opens every run concurrently | P2; 40 runs retain 40 readers | Offline follow-up after 38h qualification: bounded hierarchical fan-in |
| F5: reconstructor retains every quality audit in memory | P2; 10,000 duplicate updates retain 10,000 audits | Measure stable-session growth; bounded production history must preserve durable gap/checkpoint facts |
| F6: inaccessible repository-discovery cwd crashes CLI | P2; PermissionError reproduction and owner-stop operator incident | Q1: skip inaccessible optional Git candidates, retain data-root permission checks |

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
