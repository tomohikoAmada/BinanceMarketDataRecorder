# Q1: Correctness fixes and bounded live diagnostics

Date: 2026-10-02. Scope: Q1 of the current [38h plan](../milestone_plan.md).
Predecessor: `4d44902b882623c7a88025c6d222d688403583ba`, whose dual-platform
CI/build passed in [run 36902512051](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/actions/runs/36902512051).
This is implementation/self-verification, not the independent Q2 review or
deployment/qualification acceptance. The review worktree and PR #79 preserve
the original checkout's WIP and untracked bundles.

## Implemented behavior

| Finding | Q1 change and evidence |
|---|---|
| F2 required tasks and shutdown | Guard the existing heartbeat, cooperatively interrupt startup recovery on termination, include it in required-task waiting, and propagate failure through drain. Exceptions/early return in startup, recovery and steady operation, real state-store failure, and failure while draining publish FAILED instead of STOPPED. Capacity unexpected return also fails and drains. Collector drain/seal failure now propagates instead of becoming STOPPED; peer drain failure cannot replace an earlier primary cause. Failure task/type and heartbeat stop reason are retained; healthy stop and hard-reserve regressions remain. Final publication errors cannot bypass Catalog/lock release. |
| F3 dedup | One candidate boundary namespaces every semantic key by venue/market/symbol/stream. Shutdown, snapshot and malformed multi-product fixtures retain independent rows, genuine duplicates merge, and conflict handling retains its existing tests. [ADR-0036](../adr/0036-product-scoped-normalized-dedup.md) versions new writes/build identity as `normalized-dedup.v2`; a generated legacy-format fixture proves old files remain unchanged and both versions replay through the public reader. |
| F6 CLI cwd | Optional Git discovery skips unusable cwd/candidates. Real data-root permission failures still propagate. Existing installed console entry is exercised from denied and removed cwd with imports pointed at candidate source; clean-wheel validation is also a release/CI gate. |
| F7 auxiliary health | Enabled FAILED enters aggregate DEGRADED. Unexpected global owner return, exception or cancellation is explicit owner/stream failure with an operational event; ready core collection continues until normal stop. Disabled kinds remain disabled. |
| F5 audit history | Live `CollectorReadiness` constructs a 256-item diagnostic deque both initially and after restart. Every audit still reaches the existing observer/counters; unreliable intervals and checkpoint rules are unchanged. Offline reconstructor default remains full history. Cloud synthetic evidence below establishes a finite component bound; total live RSS remains a Q3 gate. |

No new supervisor framework, configuration system, transport, dependency,
native engine, service, Catalog/Raw format or consumer coupling was added.
The additional service-state fields are compatible with existing readers.
An old replay reader cannot verify a new v2 build; retain a v1 build or upgrade
the reader. Existing v1 builds retain their original semantics, including the
old multi-product defect; correction requires rebuilding from intact Raw.

## Cloud diagnostic and limits

[Probe](../reviews/2026-10-02-q1-cloud-memory-probe.py) and
[raw results](../reviews/2026-10-02-q1-cloud-memory.json), retrieved
`2026-10-02T00:09:39Z`–`00:09:42Z` from greencloud-tokyo-01, Ubuntu x86_64,
installed Python 3.12.3. Two independent interpreters compare installed code
with candidate reconstructor source loaded only into process memory. Bytecode
writes are disabled; no production data, service control or deployment writes.

| Same synthetic duplicate burst | Installed unbounded | Candidate 256 history |
|---|---:|---:|
| Retained audits at 10,000 / 100,000 events | 10,000 / 100,000 | 256 / 256 |
| Traced allocations at 100,000 events | 10,404,913 bytes | 27,737 bytes |
| Process RSS at 10,000 / 100,000 events | 33,779,712 / 61,239,296 bytes | 31,711,232 / 31,715,328 bytes |
| Full observer calls including later gap | 100,001 | 100,001 |
| Later gap intervals | 1 | 1 |

Both runs retain the same logical book hash. Recorder was inactive **and**
disabled before and after each diagnostic. Tracemalloc and synthetic duplicate
rates make elapsed time unsuitable as production throughput evidence. These
figures isolate retained diagnostic history, not all live Collector memory or
38h feasibility. Q3 must measure actual selected-scope Recorder/Observer
combined RSS, queues, capacity and audit costs before T0. F4 remains the
explicit offline fan-in follow-up after this endpoint.

## Checks

Local macOS Apple Silicon, Python 3.12.9, original `.venv-ms2` reused without
installation/mutation, candidate imports via `PYTHONPATH=src:.`.

| Command | Result |
|---|---|
| `CI=true M0_CONTRACT_ROOT="$PWD" PYTHONPATH=src:. <python> -m pytest -q` | 1881 PASS, 24 skipped, 4 stress deselected, 154.61s; 13 pre-existing fork warnings |
| `<python> -m ruff check .` | PASS |
| `PYTHONPATH=src <python> -m mypy` | PASS, 280 checked files |
| `CI=true M0_CONTRACT_ROOT="$PWD" python3.12 tests/verify_m0_contracts.py` | PASS |
| `go run tools/verify_raw_chunk_golden.go` | PASS |
| `git diff --check`, document links, cloud probe JSON/source binding | PASS |

Final candidate GitHub CI/build/fresh-wheel/dependency results are tracked on
[PR #79](https://github.com/tomohikoAmada/BinanceMarketDataRecorder/pull/79).
Online tests were not run: this milestone changes no Binance endpoint semantics;
their explicit release gates remain Q2/Q3. Stress tests were not run: Q1 changes
no Raw framing/ingress algorithm, and selected existing stress gates remain Q2.
No external-volume tests, cleanup, redeploy, warm-up or Formal duration ran.
Independent implementation review is unrun and mandatory in Q2; self-review
and CI do not substitute for it.

```text
Q1=IMPLEMENTED_OFFLINE_VERIFIED
Q2_INDEPENDENT_REVIEW=PENDING
DEPLOYED_SOURCE=89bc6b41c0cb7d270ca6e4d2dec9a375137c028e
RECORDER=STOPPED_DISABLED
FORMAL_V5_CREDIT_SECONDS=0
12H_STARTED=NO
PRODUCTION_READY=NO
NEXT=Q2_REVIEW_AND_FREEZE_RELEASE_CANDIDATE
```
