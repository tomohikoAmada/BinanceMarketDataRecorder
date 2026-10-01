# Project Handoff

Updated October 2, 2026. This is the entry point for the next development team.

## Current code

PR #78 is merged at `89bc6b41c0cb7d270ca6e4d2dec9a375137c028e`,
tree `039f8fefaeefe116a3a464b7d2bffae8d05e7d9c`. This is still the deployed base.
The current branch adds a local optimization candidate, not an installed release.

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

## Current development plan

The October 1 owner-requested [architecture review](reviews/2026-10-01-architecture-review.md)
reproduces three P1 issues: inadequate per-observation delta capacity, unsupervised
heartbeat failure and cross-product normalized deduplication. Three P2 findings
cover normalization fan-in, retained quality audits and inaccessible-cwd CLI
discovery. F1 now has a local implemented candidate; F2–F6 remain open fixes or
follow-up resource work. The [October 2 recheck](reviews/2026-10-02-plan-recheck.md)
adds F7, an enabled auxiliary FAILED omitted from aggregate degraded health.
No new independent implementation review is claimed.

The owner selected safe old-data cleanup followed by an independent fresh corpus.
The single current [milestone plan](milestone_plan.md) requires 2h + 12h + 24h all PASS
with complete terminal/independent verification. Next is Q1 correctness fixes,
release review and Q3 safe cleanup/new canonical Catalog; no old archive is
implicitly destroyed or imported into the new corpus.

[Cloud performance analysis](reviews/2026-10-02-cloud-performance-and-38h.md) records
~21% scanner time reduction and compact status output with identical proofs/totals.
[ADR-0035](adr/0035-v5-bounded-delta-batches.md) freezes finite new-start batches
and retains old one-page replay/resume. The default CLI archive status omits full
transaction details; use `--details --limit 100 --offset 0` for a page. Code,
dependency set, Raw and Catalog format remain Python/compatible; no native engine
was added. Installed source and stopped operational checkpoint are unchanged.

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

1. The current section of [milestone plan](milestone_plan.md) and [risk register](risk_register.md).
2. [Developer guide](developer_guide.md).
3. [Project contract](project_contract.md) and [architecture](architecture.md).
4. The relevant subsystem contract and ADR.
5. [V5 contract](acceptance_evidence_v5.md) for acceptance work.

## Remaining work

- Review the practical cost of full baseline/terminal audits using retained
  production measurements. The stopped audit gives partial measurements only;
  it establishes neither a full integrity PASS nor completed throughput.
- Implement Q1, independently review the optimization/ADR candidate, and pass
  exact-artifact release/cloud gates. The owner has selected safe cleanup/new
  corpus; it has not been executed. Warm-up precedes the authoritative stopped
  baseline. Do not resume the unchanged interrupted audit.
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
New-start batch limits are frozen by ADR-0035; missing policy keeps the original
one-page V5 on replay/resume. Online cadence is 300 seconds, maximum evidence gap 600 seconds, delta budget
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
