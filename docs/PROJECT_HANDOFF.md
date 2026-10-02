# Project Handoff

Updated October 2, 2026. This is the entry point for the next development team.

## Current code

PR #78 is merged at `89bc6b41c0cb7d270ca6e4d2dec9a375137c028e`,
tree `039f8fefaeefe116a3a464b7d2bffae8d05e7d9c`. This is still the deployed base.
The reviewed release is frozen at `d0f455c1a417cc1a184c47b6ff766a60f3dc0159`,
tree `cf3854d7fc032de79096dced8e002a6cd55e65c3`, wheel SHA-256
`b286923d3dc777bf2e3d63ea661effd7cf389137aaf81883519c3a071d449921`.
[Q2 acceptance](milestone_acceptance/Q2-reviewed-release-and-cloud-design.md)
records independent ACCEPT, exact-source dual-platform CI and cloud release checks.
It is staged, not installed. Subsequent documentation is not a replacement wheel.

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
discovery. F1 now has a local implemented candidate. Q1 implements F2/F3/F6,
bounded F5 retention and F7 health; independent Q2 review accepts those changes and F4
stays a separate offline follow-up. The [October 2 recheck](reviews/2026-10-02-plan-recheck.md)
adds F7, an enabled auxiliary FAILED omitted from aggregate degraded health.
The independent review has no open P0/P1/P2/P3 findings; its original P3 byte-bound finding is corrected. The
[Q1 acceptance record](milestone_acceptance/Q1-correctness-and-bounded-diagnostics.md)
records 1881 passing offline tests and a read-only cloud synthetic memory probe.
New normalized builds use [ADR-0036](adr/0036-product-scoped-normalized-dedup.md)
and v2 build identity; legacy immutable v1 builds remain readable.

The owner selected safe old-data cleanup followed by an independent fresh corpus.
The single current [milestone plan](milestone_plan.md) requires 2h + 12h + 24h all PASS
with complete terminal/independent verification. Next is Q3 safe custody/new
canonical Catalog, exact deployment and measured cloud warm-up/baseline/readiness; no old archive is
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
- Q2 release gates are complete. Execute the reviewed Q3 procedure for retained
  old-data custody, a fresh corpus and exact frozen wheel deployment. Measure live
  capacity/RSS/complete observations and cumulative audit forecasts. Warm-up
  precedes the authoritative stopped baseline. Do not resume the unchanged old audit.
- The owner authorizes continuation through Q4 eligible 2h, then stop before Q5
  cloud 12h. Do not silently start 12h or count engineering warm-up as Formal.
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
