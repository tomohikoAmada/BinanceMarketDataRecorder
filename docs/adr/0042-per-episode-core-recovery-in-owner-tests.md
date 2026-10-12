# ADR-0042: Per-episode recovery in owner-policy test controls

Date: 2026-10-12
Status: Accepted implementation; fresh actual twelve-hour qualification pending

## Problem and evidence

Q8 run `2b406e075f9543d7a0fc6490e21236dd` stopped at its own41490.050143783s,
before43200s, although the fatal observation was already coreREADY/strict26PASS.
The private controller enforced both a60s continuous and60s run-wide cumulative
observed recovery budget. Its30s polling charged three recovered episodes
29.970699755+30.005922428+29.993793229s. The cumulative89.970415412s comparison
terminated the third recovered observation before closing that episode.

Same-read publications identify transient book_ticker disconnections for Spot
BTC, Spot ETH and USD-M BTC. Their nearby disconnected-to-connected log intervals
are0.609251s,1.625364s and4.280392s. Neither those log intervals nor coarse polling
measure exact readiness outage or lost exchange events. ConnectionClosedError
alone cannot attribute closure to Binance, routing or the local client. No close
code/reason sufficient for that attribution was retained. Recorded memory, swap,
disk, identity and clock remained within existing bounds.

## Decision

Use one small `CoreRecovery` helper in the private qualification controller.
Keep a60s bound on each continuously observed core/heartbeat episode under the
same Recorder incarnation; remove the cumulative termination condition.
Cumulative observed intervals and episode count remain diagnostics. The owner's
saved-frame criterion and full audit decide data correctness; repeated short
recoveries alone must not prevent collecting that denominator.

Normally keep the original fixed30s monitor schedule. During an open episode,
reuse the existing publication/strict gate at most every5s, with those extra
observations in a separate durable stream. They never add PASS rows to the
official monitoring denominator. Closing an episode requires a fresh strict26
PASS publication whose heartbeat is no earlier than the first bad observation.
Include collection/validation latency in the60s check. A still-bad observation at
60s fails; a completed strict recovery at60s may close; later recovery fails.

The native T0+43200s deadline never moves. Persist and reject a recovery-bearing
observation that crosses it, including a regular observation closing an open
episode. Slow regular sampling advances to the first future T0-bound slot and
discloses skipped slots, without bursts of catch-up samples. Missing/invalid
monitor evidence remains INVALID/BLOCK with no inferred percentage; an actual
strict own43200s target still dispatches the complete saved-data auditor.

Keep immediate hard runtime/boot/config/deployment/clock/resource checks, strict
target readiness, natural stop/drain/freeze, full dualLIVE/completed verification,
independent reconstruction and actual restored handoff. Saved-frame conformance
strictly>99.9% is distinct from availability and every-exchange-event completeness.
No failed historical duration or closed window transfers to a retry.

## Scope and alternatives

This corrects private test policy/accounting, not the production transport.
Production source49b4a2d/wheelc7d354be/identity7072575b/config5b73 remain unchanged.
Increasing the cumulative threshold only moves the same false-stop condition.
Continuous rapid polling adds unnecessary work; diagnostic-only5s checks are
enough to match the existing heartbeat cadence. Language/server changes do not
address this control failure. No client ping change is justified by these logs.

The published [Binance Spot stream contract](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
explicitly requires handling connection lifecycle/pings. The pinned
[websockets15.0.1 exceptions documentation](https://websockets.readthedocs.io/en/15.0.1/reference/exceptions.html)
distinguishes abnormal closure from its available close details; its
[keepalive guidance](https://websockets.readthedocs.io/en/15.0.1/topics/keepalive.html)
describes temporary network failures. These support reconnect-aware operation,
not attribution of this particular fault. The
[Google SRE monitoring chapter](https://sre.google/sre-book/monitoring-distributed-systems/)
supports simple monitoring rules with suitable measurement granularity. Applying
those principles here is an engineering inference, validated by actual replay
and timing fixtures rather than a claim that a source prescribes this exact60s rule.

## Validation

The independent GPT-6.1 Sol xhigh LOCAL/OFFLINE review accepts controller
`0e5fa1e8a1046059a87fa224d16108ac4f001f3d9fc7656ccbd2647022b58043`
and helper
`dfb252a63fcb5e511e1cfb967bee6609f97c8b1f2936c91eee78d3c03db4f844`.
All50 historical failure-export hashes and the fatal replay match. Twelve pure
helper tests,23 diagnostic fixtures,15 integration fixtures and7 independent
timing/evidence cases pass. The exact23+15 fixtures also pass on VPS Ubuntu
Python3.12.3 in a mock-only isolated directory; they do not start production.
Focused repo28 tests/Ruff/mypy pass. Actual new scope/window/capacity/start and
the complete twelve-hour closeout remain separate qualifications.
