# R2 VPS symbol capacity qualification

Current state: **AUTHORIZED / RELEASE_ACCEPTED / STOPPED_BTC_DEPLOYMENT_ACCEPTED / N1_NATIVE_WARMING_NOT_YET_T0**.
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
| N=1 | BTCUSDT | NATIVE_WARMING; formal T0 pending | None |
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
- [x] N1 config-aware common/controller/terminal/policy/forecast draft independently
  ACCEPTed3eaee5: exact BTC products/14 contexts/22 spools including global feeds;
  original four-product peak rates retained conservatively without halving.
  Full12×86400 fake-clock cases preserve2880 nominal slots;7 topology and19
  independent negative/semantic checks PASS. These grant no actual duration.
- [x] Stopped BTC deployment60134a37/newidentitydb52730c independently
  ACCEPT960c82ad, all20 commands0 and unchanged90s stop contract; old venv/config/
  identity preserved. Fresh prospective forecast565aee1c admits N1 with22spools,
  projected writer17,127,127,282B>10GiB and158656.632s required<165600s finite plan.
- [x] N1 final actual identity/config/policy binding, Linux complete-control import
  and12×86400 fake-clock cases acceptedb1113e04/16b6f103 (zero real-time credit).
  A setup interpreter error before admission was diagnosed and fixed1bfd3ccd:
  use canonical Python for package-dependent controls; existing copied files were
  revalidated without rewriting. Actual physical3420039c independently ACCEPTaa337a3f.
- [x] Actual ARM6f6d2b75/canonicalVERIFY/windowf02f4e96 and pre/post admission
  independently ACCEPTce20e40f. Fixed expiry **October22 05:29:34UTC**,
  epoch1792646974; never extend, reuse or rearm this window.
- [x] Sole native captureb1e30644/PID667176 launched **October20 07:37:05UTC**,
  observedactive/NRestarts0. Requested Typeexec/Runtime89280/Stop1500/archiveTMP.
  It is warming/NOT_YET_T0; no actual24h target, saved-frame denominator or receipt.
- [ ] At/after **October20 08:02:05UTC** warm/start checkpoint, read actual
  owner-start/warm/failure/native output once. Independently bind real T0 and its
  own86400 target; before checkpoint let healthy native work proceed.
- [ ] N=1 own24h/full data/independent/restored receipt, then successive stages.
- [ ] Measured capacity boundary/headroom recommendation and final main closeout.

The current attempt's exact status, checkpoint and full pending closeout are in
[N1 BTC own24h acceptance](R2-capacity-n1-btc-owner24h-20261020.md). N2 and later
cohorts require their own actual configuration/rates/admission and prior-stage
full receipt; do not blindly reuse N1 constants or transfer its duration.
