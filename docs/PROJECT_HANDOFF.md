# Project Handoff

Updated October 1, 2026. This is the entry point for the next development team.

## Current code

PR #78 is merged at `89bc6b41c0cb7d270ca6e4d2dec9a375137c028e`,
tree `039f8fefaeefe116a3a464b7d2bffae8d05e7d9c`. This documentation update
is a descendant of that code; it changes no runtime implementation.

The Recorder implements finite configurable Spot/USD-M products, immutable Raw,
Catalog lifecycle/recovery, gap evidence, order-book reconstruction, verified
archive, normalization, replay, historical imports, and native service management.
MS1–MS3 and the bounded MS4 review are complete. See the
[developer guide](developer_guide.md) for source locations and commands.

V5 implements bounded online delta observations, cross-cursor causal replay,
a stopped full baseline, immutable online target, private frozen control corpus,
streamed terminal Raw/archive audit, resume paths, and independent verification.
The final implementation review accepted head
`227b09aa3e328f890e8178603bf493013d8d91f5` with P0=0, P1=0, P2=1, P3=0.
P2 is `TERMINAL_FULL_RAW_AUDIT_SCALABILITY` and remains open.

## Current operations

The merged V5 artifact is installed on `greencloud-tokyo-01`. On October 1 the
owner stopped all qualification work during the baseline audit. Recorder is
inactive and disabled; the baseline and its resource sampler are stopped.
The continuation automation is paused. Archive and OS update timers have been
restored. No V5 Formal T0, 2-hour stage, or 12-hour stage was created.

The baseline has no published `audit-root.json`, so it is incomplete and cannot
be used as a passing predecessor. Existing data and partial audit evidence are
retained. No data reset or empty-directory test was performed.

[Current production state](CURRENT_PRODUCTION_STATE.md) contains exact artifacts,
paths and verified closeout state. The
[deployment and owner-stop record](milestone_acceptance/M22.9-v5-deployment-owner-stop.md)
contains gates and audit progress.

## Development setup

Use a clean checkout of current `main`, Python 3.12, and a virtual environment.
Install `.[dev]`; run the offline checks listed in the [developer guide](developer_guide.md).
Use temporary roots for tests. The original workspace's WIP branch, stash,
backup branch, and untracked review bundles remain preserved.

Follow the current task and implemented contracts. Old one-milestone-per-run,
fixed-model assignments and mandatory README disclaimer text are retired.
Routine fixes need proportionate checks; architecture/data-contract changes need
compatibility analysis. Historical evidence stays available for diagnosis.

Read these documents in order:

1. [Developer guide](developer_guide.md).
2. [Project contract](project_contract.md) and [architecture](architecture.md).
3. The relevant subsystem contract and ADR.
4. [V5 contract](acceptance_evidence_v5.md) for acceptance work.
5. [Milestone plan](milestone_plan.md) and [risk register](risk_register.md).

## Remaining work

- Review the practical cost of full baseline/terminal audits using retained
  production measurements. The stopped audit gives partial measurements only;
  it establishes neither a full integrity PASS nor completed throughput.
- Agree with the owner on the next qualification approach. An empty-data test
  was discussed but not approved or executed. Do not restart this audit or
  launch a stage from the paused automation.
- If qualification is restarted, verify the exact artifact and data scope,
  complete its baseline, obtain fresh readiness for every configured product,
  then execute the selected stage and terminal audit. V5 online observation
  does not stop Recorder; an operator owns stop/drain/quiescence/finalize.
- Keep the production-corpus P2 open until real measurements and independent
  review support closure. No V5 duration has been accepted.
- Complete the portable archive-client rollout and remaining platform/long-run
  qualification as separately scoped work.

## Compatibility to preserve

Raw v1, manifest and Catalog lifecycle semantics, the ProductKey boundary, and
consumer contracts are unchanged by V5. V1–V4 evidence readers remain available.
Online cadence is 300 seconds, maximum evidence gap 600 seconds, delta budget
240 seconds, and global recoverable-readiness episode 900 seconds. Full audit
runs outside online cadence; it retains a distinct 900-second no-progress
watchdog. Prepublication verification reads LIVE Raw; completed historical
verification reconstructs frozen controls/proofs without requiring retired live
Raw to remain in its original location.

Historical V4 2h acceptance and incomplete V4 12h remain unchanged; they grant
no V5 credit. `12H_STARTED=NO`; `PRODUCTION_READY=NO`.

## Historical records

Earlier handoff snapshots are preserved in [handoff history](PROJECT_HANDOFF_HISTORY.md).
Previous operations are in [production state history](CURRENT_PRODUCTION_STATE_HISTORY.md)
and `milestone_acceptance/`. Their status and next-action statements apply to
those checkpoints, not to the current paused workflow.
