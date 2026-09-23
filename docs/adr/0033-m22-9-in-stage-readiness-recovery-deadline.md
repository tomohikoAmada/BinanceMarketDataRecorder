# ADR-0033: M22.9 In-Stage Readiness Recovery Deadline

- **Status:** Accepted policy authority; implementation pending
- **Date:** 2026-09-23
- **Scope:** M22.9 Formal acceptance evidence only

## Context

The independent M22.9 V4 semantics review is accepted against the verified
GitHub `main` policy base
`6e5dd91575d53e226a20d1873eafa3e06ca64329` / tree
`3262aa4ea46156631358a1002b7833d37325eb87`. The review found no Recorder
reconnect defect, readiness-evaluator defect, or readiness-snapshot defect. It
did find an acceptance-policy design defect (`P0=0`, `P1=1`, `P2=1`, `P3=0`):
the Formal outcome was sampling-phase dependent because the historical V3
policy made an intermediate `readiness_not_ready` finding sticky. The review therefore
requires a new schema, a code change, and a fresh Formal chain after the fix.

The accepted review result is:

```text
RECORDER_RECONNECT_DEFECT=NO
READINESS_EVALUATOR_DEFECT=NO
READINESS_SNAPSHOT_DEFECT=NO
ACCEPTANCE_POLICY_DESIGN_DEFECT=YES
FORMAL_OUTCOME_READINESS_SAMPLING_PHASE_DEPENDENT=YES
INTERMEDIATE_TRANSIENT_NOT_READY_SHOULD_BLOCK=NO
INTERMEDIATE_TRANSIENT_NOT_READY_SHOULD_BE_STICKY=NO
READINESS_FAILED_SHOULD_BE_STICKY=YES
TERMINAL_NOT_READY_SHOULD_FAIL=YES
SPEC_REQUIRES_EVERY_SAMPLE_READY=NO
SPEC_REQUIRES_FINAL_PRODUCTS_READY=YES
NEW_SCHEMA_VERSION_REQUIRED=YES
CODE_CHANGE_REQUIRED=YES
FRESH_FORMAL_CHAIN_REQUIRED_AFTER_FIX=YES
P0=0
P1=1
P2=1
P3=0
```

The policy gap is specifically the absence of an exact runtime deadline for a
bounded, truthful, in-stage readiness recovery. A deployment readiness timeout
is a startup authority; it is not a runtime recovery authority. The ordinary
acceptance sample-start target remains 300 seconds and `MAX_EVIDENCE_GAP`
remains 600 seconds. This ADR does not change either cadence value.

## Decision

Freeze the following new M22.9 V4 policy authority:

```text
M22_9_V4_IN_STAGE_READINESS_RECOVERY_DEADLINE_SECONDS=900
M22_9_V4_IN_STAGE_READINESS_RECOVERY_DEADLINE_HUMAN=15 minutes
M22_9_V4_IN_STAGE_READINESS_RECOVERY_CLOCK=CLOCK_BOOTTIME
```

The 900-second value is not derived from the 300-second deployment readiness
timeout. It is an explicit qualification-policy choice. The closest
repository-owned precedent is the MS4 qualification envelope's separately
bounded, separately attributed `recovery observation: 15 minutes` in
`docs/milestone_acceptance/MS4.md`. That was not old M22.9 Formal authority;
this ADR deliberately promotes the bounded recovery envelope into explicit V4
runtime acceptance policy.

The choice is also grounded in historical runtime evidence. M21.4 gen6
recorded `STREAM_DISCONTINUITY_STARTED` at approximately
`2026-08-06T15:11:12.822Z` and
`STREAM_DISCONTINUITY_COMPLETED` at approximately
`2026-08-06T15:18:10.684Z`, a recovery duration of approximately 417.861
seconds. Therefore 300 seconds is too short as a generic runtime recovery
bound for a structurally valid repository recovery. This does not claim that
900 seconds is mathematically optimal.

## Deadline authority

The V4 deadline is an **acceptance-observed recovery episode deadline**. It is
not an assertion of the exact physical transport-outage duration between
exchange, transport, Collector, and readiness transitions. It is evaluated
only from immutable acceptance samples and uses integer BOOTTIME nanoseconds.
Wall-clock time is not used for elapsed duration.

The minimum V4 episode state is one active **global** readiness recovery
episode. It is global because the Formal pass condition is that every
configured ProductKey is ready.

## Episode start

After a stage has established valid READY/T0 authority, an episode starts at
the `observed_at_boottime_ns` of the first V4 acceptance sample that reports:

```text
DeploymentReadinessResult.state == NOT_READY
AND
the readiness reason is exactly <market>:<symbol>_core_not_ready
```

The immutable sample also supplies `observed_at_utc_ns`. The implementation
must store or reconstruct, without inference from outside the samples:

```text
episode_start_boottime_ns
episode_start_utc_ns
```

Do not infer an earlier unobserved start time. The deadline begins at the first
observed recoverable sample, not at a transport log, a Catalog event, or an
assumed physical outage boundary. A pre-T0 condition is not an episode.

## Episode close

The episode closes at the first later acceptance sample whose authoritative
readiness result is `READY`. Its observed age is:

```text
episode_age_ns =
    ready_sample.observed_at_boottime_ns - episode_start_boottime_ns
```

Recovery is within the deadline exactly when:

```text
episode_age_ns <= 900_000_000_000
```

The boundary is inclusive. Use integer nanosecond comparison; do not use
floating-point seconds. If `READY` is first observed only after the age is
greater than 900 seconds, the deadline was exceeded. Do not retroactively
pretend that recovery happened between samples.

## Recoverable classification

The bounded episode category is deliberately narrow. The only current
recoverable in-stage readiness class is:

```text
DeploymentReadinessResult.state == NOT_READY
AND
reason == <market>:<symbol>_core_not_ready
```

This represents truthful ProductKey-local core rebuilding such as WebSocket
reconnect, depth resynchronization, snapshot/bootstrap reconstruction, or
another equivalent ProductKey-local readiness rebuild. It does not require a
matching Catalog `OPEN` lifecycle. Catalog, Raw, manifest, process, identity,
and capacity integrity remain independent acceptance planes.

While an active episode remains in this class and has not exceeded the
deadline, an acceptance sample may truthfully contain:

```text
readiness.state=NOT_READY
readiness.reasons=["<ProductKey>_core_not_ready"]
blocking_findings=[]
result=PASS_CANDIDATE
```

The stored readiness snapshot remains `NOT_READY`. The sample is causal
qualification state, not false `READY`.

If a ProductKey/reason changes while overall readiness remains continuously in
the recoverable `NOT_READY` class, the same global episode continues. A
ProductKey change does not reset the deadline. This prevents rotating failures
from extending qualification indefinitely.

This policy does not accept false `READY`. Existing evaluator validation of
connected and persisted streams, snapshots, order-book synchronization,
topology, identity, and all other readiness authorities remains unchanged.

Multiple independent episodes are permitted. Each
`READY -> recoverable NOT_READY -> READY` sequence starts a fresh 900-second
episode. V4 defines no cumulative downtime budget, maximum episode count, or
per-ProductKey daily budget.

## Deadline expiry and non-recoverable states

If a sample remains in the recoverable class and:

```text
current_sample.observed_at_boottime_ns - episode_start_boottime_ns
    > 900_000_000_000
```

the episode is overdue. Freeze the new semantic blocker
`readiness_recovery_deadline_exceeded`. It is immediate, sticky, fatal, and
stage-ineligible. A subsequent `READY` must not clear a deadline failure that
was already demonstrated by an observed sample.

The following are not recoverable episodes and retain immediate fail-closed
semantics:

```text
systemd_service_not_active
service_state_not_published
runtime_status_<non-running-state>
startup_recovery_incomplete
product_topology_mismatch
hard_reserve_safety_stop
any other NOT_READY reason not explicitly classified as ProductKey core-not-ready
```

Do not convert startup or pre-T0 conditions into runtime recovery episodes.

## Sticky failure and pre-T0 authority

An authoritative `readiness.state == FAILED` immediately yields the sticky
blocker `readiness_failed`. No 900-second grace applies. This includes
existing identity, process, environment, Catalog, product, and capacity
invalidity authorities.

Before Formal T0, all required ProductKeys must be `READY`. A stage must not
begin T0 in `NOT_READY`, `CONNECTING`, `STARTING`, or `FAILED`. The in-stage
episode policy never weakens startup readiness.

Within a valid stage, recovery of
`READY -> recoverable NOT_READY -> READY` within an observed age of at most
900 seconds produces no sticky `readiness_not_ready` and no
`readiness_recovery_deadline_exceeded`. All other Raw/Catalog/manifest,
process, identity, and capacity invariants must independently remain valid.

If readiness changes during an active episode to `FAILED`, apply immediate
sticky `readiness_failed`. If it changes to a non-recoverable `NOT_READY`
reason, apply the normal non-recoverable `NOT_READY` failure semantics; do not
reset or start another episode.

## Terminal semantics

At the stage terminal sample, `readiness.state != READY` always makes the
final ineligible, regardless of the active episode's age. Freeze the terminal
blocker `readiness_not_ready`, publish `result=FAIL`, and set
`eligible_for_next_stage=false`. A terminal sample in a recoverable
`NOT_READY` episode is still a failed final because M22.9 requires final
products to be ready. No second readiness query is made after the terminal
sample.

This is separate from deadline expiry:

```text
during stage: recoverable episode age > 900s
    -> readiness_recovery_deadline_exceeded -> sticky FAIL

at terminal: any readiness.state != READY
    -> readiness_not_ready -> FAIL
```

A stage cannot pass merely because an active episode has not yet reached 900
seconds when the duration boundary arrives.

## Sampling and cadence limitation

The ordinary acceptance cadence remains:

```text
sample-start target = 300s
MAX_EVIDENCE_GAP = 600s
```

V4 does not change the cadence and does not add a polling daemon. The deadline
is evaluated only from immutable acceptance sample BOOTTIME authority. It is
therefore an observed qualification deadline, not a sub-sample physical
outage SLA. A short outage that starts and ends between samples may be
unobserved; an episode is never assigned a start earlier than its first
observed recoverable sample.

The production incident regression remains fail-closed with truthful state:

```text
STARTED = T
NOT_READY observed = T + 0.008800790s
COMPLETED = T + 1.210056629s
later readiness = READY
```

The intermediate sample is `NOT_READY` with `blockers=[]` and
`PASS_CANDIDATE`; the later `READY` closes the episode. This incident alone
does not fail the stage.

The deterministic long-recovery regressions are:

| First recoverable `NOT_READY` | First later `READY` / observation | Result |
| --- | --- | --- |
| `T` | `T+899s` | within deadline; no deadline blocker |
| `T` | `T+900s` | within deadline; no deadline blocker |
| `T` | still `NOT_READY` at `T+901s` | `readiness_recovery_deadline_exceeded`, sticky `FAIL` |
| `T` | first `READY` at `T+901s` | deadline exceeded; sticky `FAIL`; later `READY` cannot salvage |

## Historical compatibility

The schema lineage is explicitly:

```text
v1 = historical v1 semantics
v2 = historical v2 semantics
v3 = historical v3 semantics
v4 = bounded readiness-episode semantics defined by this ADR

SCHEMA_VERSION=m22.9-acceptance-evidence.v4
PREVIOUS_SCHEMA_VERSION=m22.9-acceptance-evidence.v3
```

The exact implementation constant names may be designed later, but the
semantic names and lineage are frozen here. V3's historical rule that an
intermediate `readiness_not_ready` finding is sticky remains historical
authority for V3 evidence. V1, V2, and V3 evidence are not reinterpreted,
rewritten, upgraded, or assigned V4 meaning. Historical V1 and V2 readers and
verifiers remain explicitly reachable; mixed historical/V4 chains are not
implicitly upgraded.

The historical V3 chain remains immutable: its accepted 2-hour result belongs
to the old artifact; V3 12-hour attempt #1 was aborted by the operator
monitor with zero credit; and V3 12-hour attempt #2 failed on sticky
`readiness_not_ready` with zero credit. The current status is therefore:

```text
V3_2H_HISTORICAL_ACCEPT=YES
V3_12H_ELIGIBLE=NO
24H_STARTED=NO
```

## Formal-chain consequence

V4 implementation changes source, Wheel, and deployment identity. No old V3
2-hour credit transfers. After implementation, independent review, exact
artifact deployment/readiness, and separate authorization, the fresh chain
must be:

```text
V4 identity -> readiness -> 2h -> 12h -> 24h -> 72h -> 168h
```

No stage starts automatically. This documentation-only freeze does not
implement V4, deploy it, start a Formal V4 stage, or authorize VPS work.

## Rejected alternatives

- **300-second deployment-readiness timeout:** rejected because it is startup
  authority rather than runtime recovery authority, and the M21.4 gen6
  recovery took approximately 417.861 seconds.
- **Terminal-only unlimited recovery:** rejected because it would allow a
  stage to carry an unbounded unresolved runtime episode until terminal time.
- **Any-`NOT_READY`-sticky V3 behavior:** rejected because it creates
  sampling-phase-dependent false rejection of truthful transient recovery.
- **Catalog-gap-only recovery classification:** rejected because readiness
  recovery is a ProductKey-local runtime state and Catalog/Raw/manifest
  integrity is an independent acceptance plane.
- **Per-ProductKey timer reset loophole:** rejected because rotating failures
  would evade one bounded qualification deadline; V4 uses one global episode.
