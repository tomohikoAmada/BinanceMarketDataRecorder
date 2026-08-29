# Current Production State

This file is the concise operational authority for the current repository and
VPS candidate. For project history, evidence boundaries, and takeover guidance,
see [`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md). Historical acceptance and
incident records remain unchanged in `docs/milestone_acceptance/` and
`docs/milestone_evidence/`.

## Status at a glance

```text
MAIN=e074d41a979af92b50bee880d6d55295ca65413d
M23_4=COMPLETE
M23_4_2H_POST_MERGE=PASS
CURRENT_PHASE=PROGRESSIVE_NON_FORMAL_VPS_VALIDATION
CURRENT_COMPLETED_DURATION=2h
NEXT=4h_NON_FORMAL_BURN_IN
FORMAL_M22_9=NOT_STARTED
PRODUCTION_READY=NO
M23_5=NOT_AUTHORIZED
```

This state was consolidated on 2026-08-29. It is documentation authority only:
it does not authorize deployment, service restart, formal acceptance, profiling,
or data retirement.

## Source, review, and deployment authority

| Item | Current authority |
| --- | --- |
| GitHub `main` | `e074d41a979af92b50bee880d6d55295ca65413d` |
| M23.4 merge | PR #41, merge commit above |
| Post-merge CI | `offline-ci` run `33174563501`, completed/success |
| M23.4 independent final review | approved; `P0=0`, `P1=0`, `P2=0`, `P3=0` |
| Deployed source | `e074d41a979af92b50bee880d6d55295ca65413d` |
| Deployed Wheel SHA-256 | `7dfef238514dbb3fc1ceb56e1b395eefbb8a85516bd4ffcf784daaf1260634a1` |
| Production lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Production config SHA-256 | `5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb` |
| systemd unit SHA-256 | `69f3c4c2c77a3e6fc4ee397d26ecb2927a741a6165024b2f6656727d0b398b83` |
| Deployment identity | `ed1b108c33cf31ad8faceddbd66814804e1411cdbc6250eef1526365c52dece2` |
| Service at close of the M23.4 window | ACTIVE / READY, zero restarts |

The service observation is historical evidence at the end of the validated
window, not a durable claim about a PID or future live state. Verify service,
artifact, host, boot, and process-incarnation authority before every new run.
The deployed candidate is not thereby Production Ready.

## M23.4 status

M23.4, Incremental Clean-Seal Evidence, is implemented, independently approved,
merged, deployed, post-merge CI-clean, and validated by a two-hour non-formal
production-equivalent VPS A/B run.

Normal live-owned clean rotation now seals with writer-owned incremental
verified evidence and no redundant second full semantic scan. General
`seal_partial()`, startup/recovery, unknown/recovered/poisoned partials, and the
zero-record reconnect-marker path where applicable retain full `scan_chunk`
authority. Raw v1 and the durable storage protocol are unchanged.

The two-hour result was:

- runtime integrity: **PASS**;
- structural performance: **PASS** — normal clean-seal `scan_chunk` was not
  observed;
- measured performance: **PASS** — conservative maximum seal latency fell from
  `11.532059644 s` to `2.043359872 s` (82.2811% reduction), with zero
  backpressure episodes/timeouts in B;
- no restart, SIGKILL, OOM, resync, or terminal failure in B.

Raw average CPU was higher in B (`85.6172%` versus `41.8757%`) under a workload
that sealed about 3.1404 times as many records. It is not a like-for-like CPU
regression. Frozen evidence gives CPU seconds per million sealed records of
`1000.389` for A and `651.306` for B, a 34.8947% improvement.

Detailed implementation and validation authority is in
[`milestone_evidence/M23.4-incremental-clean-seal.md`](milestone_evidence/M23.4-incremental-clean-seal.md).

## Current validation phase

The current phase is progressive **NON-FORMAL** engineering validation on the
temporary approximately one-month VPS rental:

```text
2h COMPLETE/PASS -> 4h NEXT -> approximately 12h -> 24h -> 72h
```

These are independent engineering burn-ins, not M22.9 stages. After each run:

1. freeze and hash evidence;
2. analyze integrity, performance, memory, backpressure, reconnect behavior,
   and operations;
3. optimize only if measurements justify it;
4. perform a separately authorized, consistency-safe retirement/reset of
   disposable non-formal test data if space must be reclaimed;
5. proceed to the next duration only when the evidence justifies it.

Safe retirement must occur only after evidence is frozen. It must never be an
arbitrary recursive deletion, Catalog surgery, or deletion of Raw still
referenced by Catalog/manifests.

The exact later sequence can change in response to measurements. M23.1/M23.2
remain measurement-gated, M23.3 remains a bounded-pipeline option only if
cheaper work is insufficient, M23.5 native C++ remains unauthorized unless
Python is again proven to be the bottleneck, and M23.6 is a later explicit
redesign gate.

## Formal M22.9 status

Formal M22.9 **has not started** for this candidate. A read-only precondition
inspection found ACTIVE/READY service continuity, zero systemd restarts,
Catalog integrity PASS, zero missing files, zero size mismatches, no open local
or remote archive transactions, and no open stream discontinuities. It did not
create an acceptance root, assign an acceptance ID, or start T0.

The same inspection measured only approximately `32.523064 h` of free-space
runway. Repository-owned formal M22.9 authority requires capacity for the full
independent `2h -> 12h -> 24h -> 72h -> 168h` chain, approximately 278 hours.
The precondition therefore correctly stopped at
`INSUFFICIENT_MEASURED_CAPACITY_RUNWAY` before T0.

This means:

- M22.9 did not fail after execution; it did not start;
- 278 hours is a later formal capacity gate, not the current next test;
- Production Ready is not authorized;
- the capacity blocker does not prevent independent non-formal stages because
  their disposable data may be retired safely between runs.

## Disconnect and operator context

The M23.4 B window observed 17 external disconnect intervals: 17 STARTED,
17 COMPLETED, and 0 OPEN. The subsequent precondition inspection classified
them as `CLOSED_HISTORICAL_BASELINE`. They are neither zero disconnects nor
17 unresolved gaps, and they did not require a new formal starting cut.

Two disclosed operator incidents occurred around that work: `/dev/null` was
briefly replaced during an operator check and immediately restored; after the
completed measurement window, an optional unbounded full-history audit used
excessive RAM and was terminated. Neither restarted, SIGKILLed, or OOM-killed
the Recorder, changed its durable state, or invalidated the completed window.
The unbounded audit must not be repeated on this small VPS; use only bounded,
project-owned validation mechanisms.

## Evidence locations

Repository evidence:

- [`milestone_evidence/M23.4-incremental-clean-seal.md`](milestone_evidence/M23.4-incremental-clean-seal.md)
- [`milestone_acceptance/M22.9.md`](milestone_acceptance/M22.9.md) — historical
  incident/acceptance authority, not a current pass
- [`milestone_plan.md`](milestone_plan.md)

VPS non-formal evidence (not committed to GitHub):

```text
/opt/binance-market-data-recorder/m23-profiling/M23.4-POST-MERGE-20260828T143255Z
/opt/binance-market-data-recorder/m23-profiling/M23.4-POST-MERGE-20260828T143255Z.tar.gz
bundle SHA-256: cb3c2998090545778dd5a0dc2d1cec333d08729120fb8a097de053a63fddb1a2
```

Do not alter or overwrite the evidence. The M23.4 package is non-formal support
evidence and does not substitute for a future M22.9 chain.

## Next action

Prepare and execute the next **4-hour NON-FORMAL burn-in** on the existing
validated candidate, unless live health or identity authority has changed.
Freeze evidence before any separately authorized test-data retirement. Do not
start formal M22.9, a 278-hour chain, or speculative M23.5 work as the next
action.
