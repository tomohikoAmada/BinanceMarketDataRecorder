# R2 VPS symbol capacity qualification

Current state: **AUTHORIZED / RELEASE_ACCEPTED / STOPPED_BTC_DEPLOYMENT_PENDING / NO_CAPACITY_RUN_STARTED**.
The owner's October20 request and explicit choice authorize the sequence below.
This supersedes the earlier instruction to stop after the first new24h receipt.
It does not authorize a48h capture or a wider exchange/service framework.

## Counting and scope

One symbol means the same USDT symbol collected on both Spot and USD-M: N symbols
are2N configured ProductKeys. Keep all existing14 data functions, intervals,
durability, queue/byte limits and recovery/stop rules. First N=1 is BTCUSDT;
N=2 is BTCUSDT+ETHUSDT. Preserve the original four-product configuration and all
its data/identities. The reduced test configuration is a new explicit scope,
not retroactive acceptance of the old four-product run.

Use the existing configurable product-set mechanism (ADR0032), not a new load
generator or automatic all-symbol discovery. Before adding a symbol, verify
current public Spot and USD-M eligibility. Freeze a named, ordered, nested
symbol list in the run record. Subsequent symbols should represent active
markets; record measured event/byte rates because symbol count alone does not
describe load. No order/account endpoints or credentials are needed.

## Sequence and decision

1. Finish the report-isolation repair's exact-source CI, affected Linux QA,
   locked wheel and independent review (completed), then stopped deployment/new
   identity. Fresh583f307c inventory found no remaining partial or nonretired
   object; no old Raw recovery/copy is needed. Recheck before deployment.
2. Start BTCUSDT only after fresh capacity/time/strict warm/resource admission;
   run its own86400s and complete saved-data/restored acceptance.
3. After full acceptance, try the nested N=2,4,8… cohorts. Each configuration has
   its own config/identity binding, new exclusive scope, finite non-extendable
   window and independent T0. Reuse the frozen wheel and valid source proofs;
   do not rebuild identical source or transfer duration between configurations.
4. Stop increasing at a demonstrated capacity constraint or capacity-related
   failure. Narrow the gap between the last accepted and first capacity-limited
   cohort with intermediate counts. A transport outage or software defect is
   diagnosed separately; it does not by itself establish a hardware maximum.
   Repair an actual defect before retrying, rather than repeating unchanged
   failed runs. A source change requires qualification for that new source.
5. Every candidate called stable must complete its own24h, covering UTC midnight
   reporting and the configured planned connection rotation. A short warm/smoke
   can reject an unsafe candidate, but cannot award a24h capacity result.

For each stage, record cohort/config/source/wheel/identity, actual T0/endpoint,
event and stored-byte rates, CPU, owned RSS/current+peak swap, host memory,
writer/heartbeat latency, queue occupancy/backlog trend, REST cooldown/recovery,
report completion/retries, archive throughput/backlog and actual storage
forecast. Retain existing gates; do not raise queue/stop/recovery limits to
force a pass. Stop an unsafe experiment through the existing reviewed controls,
preserving data and restoring OS/archive owners. Do not change unrelated
services or delete unique production data/retained venvs to make a stage fit.

The complete selected new-data dual released LIVE passes, completed immutable
verification, saved-frame denominator strictly>99.9%, configured-product strict
start/finish, planned rotation/recovery evidence, independent LOCAL/OFFLINE
control reconstruction and actual restored-beforecancel/fresh stopped handoff/
owner receipt remain mandatory for every accepted24h stage. Expected contexts
must come from that exact config; an absent unconfigured symbol is not a
failure, and a configured symbol must never be silently omitted. Monitor
availability is reported separately from saved-frame conformance.

## Result and stopping rule

Report the largest **accepted cohort on this VPS under recorded conditions**,
the nearest measured capacity constraint/failed cohort and any untested range.
Do not claim a universal maximum for arbitrary symbols, constant future traffic,
unlimited retention or error-free permanent operation. If resource safety stops
testing before a failing count is reached, state the accepted lower bound and
the untested upper bound. A production operating recommendation must retain
measured headroom rather than simply select the last barely passing count.

After the supported boundary is established, publish the stage matrix and
recommendation, update repo/external plans and handoff, push main, then pause
the hourly task. Keep the hourly frequency unchanged and stay quiet while a
healthy native stage progresses.48h remains PENDING; R078 remains OPEN.

| Stage | Symbols on each market | State | Full24h receipt |
| --- | --- | --- | --- |
| N=1 | BTCUSDT | NOT_STARTED; repaired release pending | None |
| N=2 | BTCUSDT, ETHUSDT | PENDING N=1 acceptance | None |
| N=4+ | Explicit nested eligible cohort, not selected yet | PENDING | None |

## Completed and pending

- [x] Owner approved both-market counting, all14 functions and at least24h per
  accepted candidate; progressive N=1→2→4→8 and smaller steps near the limit.
- [x] Existing config supports explicit per-market symbols; no topology rewrite
  is needed. Report isolation and bounded latent fixes are independently accepted
  locally; see [repair acceptance](R2-report-isolation-repair-20261020.md).
- [x] Exact source CI39356789510/PR84 merged; BTC-only candidate changes just both
  symbol lists. Draft auxiliary/monitor context gates and topology independently
  ACCEPTfcf40d5f (N1/2/4 contexts14/26/50, spools22/42/82), not VPS capacity credit.
- [x] Linux229PASS/locked68460be4 wheel and independent actual supply chain
  ACCEPTab69ecad; current failed-tail metadata583f307c confirms zero partials
  and zero nonretired chunks/transactions, with no Raw or data mutation.
- [ ] Stopped BTC deployment/newidentity; common/controller/terminal/forecast
  and actual stage admission remain unbound.
- [ ] N=1 own24h/full data/independent/restored receipt, then successive stages.
- [ ] Measured capacity boundary/headroom recommendation and final main closeout.
