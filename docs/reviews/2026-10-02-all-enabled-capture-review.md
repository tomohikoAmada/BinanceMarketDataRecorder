# All-enabled configuration and lifecycle review

The owner requires all fourteen public auxiliary capture switches enabled for
the new cloud qualification chain, continuing through eligible 2h and 12h,
stopping before 24h. This supplements unchanged Q2 coverage rather than rerunning
an unrelated architecture review.

## Original candidate

Independent reviewer: previously owner-authorized GPT-6.1 Sol xhigh, read-only
local/offline; no SSH, network, edits or further agents. Base main
`2cccf698f86cf01a6e5c04f3c2682fa2566b6c82`, tree
`b465656cf4768eb9ddbea293aa266e0ce190a209`. Original verdict **BLOCK**:
P0=0, P1=4, P2=1, P3=0. Independently executed 178 focused tests PASS.

| Finding | Candidate correction; independent rereview pending |
|---|---|
| P1 Spot auxiliary waits through limiter ban/held request slot after stop | Stop-aware admission; retain ownership of started SDK worker and coincident rate error |
| P1 factory failure/unexpected child cancellation invisible; canceled owner can strand children | Factory inside handler; terminal FAILED/error evidence; owner-local stop and child drain without stopping core |
| P1 auxiliary and both core snapshot drain/seal threads outlive canceled waiters | Existing owned blocking helper for storage drain/sync/seal; no new worker framework |
| P1 idle snapshot/Spot metadata frame misses one-second durability window | Explicit owned sync before persisted-readiness/accepted success |
| P2 quiet liquidation falsely STALE; aged terminal FAILED overwritten | Existing connection lifecycle exposed; event-sparse health exemption; preserve terminal statuses |

Capture/config/runtime/normalization files were unchanged between earlier
reviewed `9e08c1d` and this main. Prior tests already cover path/proxy/redaction,
finite products, interval/flag wiring, public schemas, shared REST limits,
independent cursors, dedup and failure isolation. No miswired flag/interval found.
The uncovered real-runtime shutdown test had disabled every auxiliary flag.
The new regression assembles all four selected products, captures/seals all42
contexts, verifies singleton global owners, shared USD-M admission/cooldown,
SDK/Raw product identity and explicit post-frame sync at durability=1.

## Workload and supplementary gate

42 steady Raw contexts: 12 core WebSockets, 4 snapshots, 26 auxiliaries.
There are16 WS connections and26 REST contexts. Auxiliaries comprise22 REST
contexts,2 mark-price streams and2 liquidation streams. Two global USD-M
REST contexts are singleton, not duplicated per symbol. The four-product
open-row bound128 admits42 steady/84 singly-overlapping contexts; reconnect
bounds admit16 WS contexts. Static arithmetic is not measured cloud headroom.

Core readiness stays core-only. The read-only operator gate checks all26
auxiliary owners and12 symbol-bound five-minute cursors before T0 and throughout
qualification. Failed supplementary coverage prevents reviewed qualification.
Zero connected liquidation messages and successfully captured empty fundingInfo
remain valid. Failures/catch-up gaps remain explicit; no complete liquidation
history or per-product normalized global-metadata completeness is claimed.
The existing normalized global-metadata projection uses its legacy BTCUSDT
sentinel while Raw retains full response models; changing that derived contract
is outside this Raw qualification correction.

## Frozen rereview input

Runtime/test/tool manifest SHA-256:
`688a6583b399f8d6fba257719bb7e768349829380f6afb5a8182b5a6cc316b98`.
This is canonical sorted JSON of changed/untracked src/tests/tools paths and
exact file SHA-256 values. It also includes the separately reviewed pending
V5 v2 codec/delta/online/throughput changes in ADR-0037. Independent final
verdict is pending; this report is not an ACCEPT or deployment authorization.

Owner-run full default suite:1929 PASS,24 online SKIP,5 stress deselected,
159.63s. Ruff/strict mypy286 files/M0/Go golden/diff-check PASS. Repeated-cancel
blocked drain/sync, storage-error priority, no early cursor/readiness advance,
ban/slot/in-flight stop, empty/sparse coverage and failure-health regressions pass.
Prior unchanged million-frame/transport stress results are retained; changed
cloud capture/observer checks and long duration tests remain pending.
