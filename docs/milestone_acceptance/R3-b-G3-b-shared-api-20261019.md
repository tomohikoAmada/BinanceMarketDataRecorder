# R3-b + G3-b / BMD-RL-01 offline shared-IP admission

2026-10-19 — **R3-b/G3-b offline COMPLETE / REVIEWED / CI PASSED / MERGED**.
Actual production observation remains a separate G3-c/BMD-4 gate.
Scope: one Recorder + one Gateway, 10 Spot + 10 USD-M research products, all14
Recorder features. No VPS or Binance API calls, deployment, R2 service/config,
frozen gates, data/evidence or automation changes.

## Problem and minimal change

The old inventory's two Spot recovery rounds plus metadata cost6220>6000;
eight forty-connection Spot rounds cost320>300. Backoff counters reset after
Live, and successful USD-M catch-up requests had no total-window allowance.
Serializing a request or applying per-product retry delays did not close this.

Both processes now enforce fixed, independent rolling-window shares at their
production request admission points. Product recovery cannot reset the counters.
No coordinator, IPC, database, additional service or configurable policy framework
was added. Existing snapshot depth, polling/catch-up semantics, features, Raw,
Gap, fsync, archive/readback and consumer contracts are preserved.

| Window | Recorder | Gateway | Combined ceiling | Reference / reserved headroom |
| --- | ---: | ---: | ---: | --- |
| Spot weight /60s | 2000 | 3000 | 5000 | 6000 reference;1000 reserved |
| USD-M weight /60s | 1200 | 600 | 1800 | 2400 reference;600 reserved |
| FundingRate + FundingInfo /300s | 400 | 0 | 400 | official500;100 reserved |
| All six USD-M statistics routes /300s | 800 | 0 | 800 | conservative combined cap below documented1000;200 reserved |
| Spot WS attempts /300s | 180 | 60 | 240 | official300;60 reserved |
| USD-M WS attempts /300s | 180 | 60 | 240 | conservative local policy;not a claim of an official300 limit |

The two minute capacities are reference metadata values, not permanent exchange
constants. The [inventory/runbook](../runbooks/R3-shared-ip-budget.md) retains
endpoint weights and the deployment admission requirements.

## Actual integration and cancellation

Recorder routes Spot snapshot and metadata wire slots, USD-M depth and every
side-data page, and both real WS openers through its shared counters. USD-M
statistics waits release the shared request lock so depth has its own allowance.
Old REST429/418 cooldowns remain mandatory; a WS HTTP rejection installs a
market block observed by WS and REST. Recorder REST rejection retains its
REST-only reaction; independent WS admission still bounds connection attempts.

Gateway checks weights immediately before each HTTP write and connections
before WS DNS/TCP. All product threads share a mutex-protected budget; HTTP and
WS429/418 responses install the shared market deadline, never shorten it, and
retain status/Retry-After diagnostics. Metadata fetch fails explicitly with
RuntimeAdmission when unavailable, preserving finite synchronous startup work.
Budget timers reuse the existing clean cancellation/drain path.

Recorder shutdown cancels a queued shielded Spot request before wire admission;
already-started workers still finish under their owner. Deduplication is retained.
Tests exercise the actual requester/openers and Gateway transport stop/metadata
path with mock or loopback endpoints only.

## Joint proof and its precise scope

[Reproducible tool](../../tools/verify_shared_api_budget.py) starts independent
Python and C++ subprocesses with virtual clocks. They execute the production
budget classes, route weights and existing cooldown interpretation. This is a
composition proof of admission policies; it is separate from actual socket or
collector recovery. Existing real runtime/20-product recovery fixtures and new
production entry/cancellation tests cover those bindings separately.

Five scenarios pass: cold start,100 single-key recoveries,12 full-set short-Live
recoveries,2400 statistics/600 funding catch-up requests, and queued429/418.
Every operation is accounted for, original scheduling bounds and cooldowns are
checked, and every rolling window is recomputed after merging both traces.
[Results](../milestone_evidence/shared-api-budget-20261019.json) reach combined
5000 Spot/1800 USD-M weight,240 connection attempts per market,400 funding and800
statistics; extra work waits rather than exceeding the share. Cold start is3220
Spot weight /422 USD-M weight /40 Spot+60 USD-M attempts. The fixture's longest
last admission is901s under repeated artificial failures, not a production SLA.

Reproduce from Recorder with its development Python and `PYTHONPATH=src`:

```text
python tools/verify_shared_api_budget.py --gateway-probe <Gateway-build>/bmd_gateway_public_api_budget_tests --output <new-result.json>
```

## Deployment boundary and remaining work

This proof requires both new role-budget implementations (`45866af`/`1c93304`),
not an older unlimited-role version, and one process of each role with no
overlapping generations.
Rolling counters are in memory: after stopping/replacing a role, wait a full300s
quiet interval before its replacement sends requests; preserve any longer known
server ban. Set the future service restart delay accordingly. A third process
must use the explicit residual allowance, not another whole role share.
A duplicate instance, process-restart loop or unknown IP user is outside this proof.
No deployed restart policy is changed by this offline work.

Before G3-c/BMD-4, validate current exchangeInfo limits/response used-weight,
final symbols, the one-instance/restart policy and all known same-IP consumers.
The proof does not grant live20-product resource capacity, R2 duration/quality,
R-078 closure or Production Ready. Authorized ordinary live observation remains
pending; never deliberately trigger a real429/418.

## Validation and delivery

- [x] Inventory, negative budgets, fixed-share implementation and five two-process
  production-policy scenarios.
- [x] Initial Recorder84 existing limiter/cooldown tests; new opener/requester
  shutdown tests plus Spot single-flight regressions27 PASS; mypy314 PASS.
- [x] Gateway complete production Release27/27; actual loopback budget wait/stop
  and metadata-bound checks; full format/diff checks PASS.
- [x] Recorder local full suite:2159 PASS /24 opt-in skips /5 stress deselected;
  the sole initial M0 failure was the worktree path assumption. Re-running that
  unchanged verifier with its existing CI workspace-root override passed1/1.
  Ruff/mypy314/Go Raw golden/diff PASS. Initial log is retained; no code was
  changed to suppress the verifier. Final GitHub full-suite result follows below.
- [x] New [independent scoped review](../reviews/2026-10-19-shared-api-budget-review.md)
  APPROVED, all concrete findings closed; Gateway full ASan27/27 and focused
  TSan4/4 and UBSan4/4 PASS.
- [x] Final Recorder `45866af` [CI39250190008](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/39250190008)
  passed both complete jobs:macOS2160/Ubuntu2156, including M0 contracts,
  Ruff/mypy314/Go golden/build/locked-dependency and clean-wheel/CLI checks.
- [x] Gateway `1c93304` passed all seven ordinary jobs on push/PR
  [39250188012](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39250188012)/
  [39250228864](https://github.com/tomohikoAmada/BinanceMarketDataGateway/actions/runs/39250228864);
  Linux production25/25. Manual dependency preparation was correctly skipped.
- [x] [Recorder PR #82](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/pull/82)
  merged as `d98821c20ea45e8821e7c6dac1b1b7be56d5d3eb` at20:18:48 Shanghai;
  [Gateway PR #39](https://github.com/tomohikoAmada/BinanceMarketDataGateway/pull/39)
  merged as `8d171c1304d1f02d7f02365307dad94de4dc9f84` at20:18:53 Shanghai.
  Each merge tree exactly equals its tested technical head. Plans and current
  handoffs mark the offline scope complete.
