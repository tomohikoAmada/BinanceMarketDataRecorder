# Current Production State

This file is the concise operational authority for the current repository and
VPS candidate. For project history, evidence boundaries, and takeover guidance,
see [`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md). Historical acceptance and
incident records remain unchanged in `docs/milestone_acceptance/` and
`docs/milestone_evidence/`.

## Status at a glance

```text
PRE_DOC_MAIN=697e9900b6017844cbfcba01d9815308523a053e
MAIN=VERIFY_LIVE_GITHUB_MAIN; DOCUMENTATION_COMMIT_NEWER_THAN_PRE_DOC_MAIN
M23_4=COMPLETE
NEW_HOST_SUBSTRATE=PASS
NEW_HOST_30MIN=PASS
NEW_HOST_2H=PASS
NEW_HOST_4H=PASS
NEW_HOST_12H=PASS
NEW_HOST_24H=PAUSED_NOT_STARTED
NEW_HOST_72H=NOT_STARTED
NEW_HOST_168H=NOT_STARTED
CURRENT_PHASE=CPU_HARDENING_AFTER_12H_NEW_HOST_VALIDATION
LONG_BURNIN_STATUS=PAUSED
NEXT=EXACT_HEAD_CPU_PROFILING
FORMAL_M22_9=NOT_STARTED
PRODUCTION_READY=NO
M23_5=NOT_AUTHORIZED
```

This state was consolidated on 2026-08-31. Verify live GitHub `main`; this
documentation commit is newer than pre-documentation `main`
`697e9900b6017844cbfcba01d9815308523a053e`. It is documentation authority only:
it does not authorize deployment, service restart, formal acceptance, profiling,
or data retirement.

## Source, review, and deployment authority

| Item | Current authority |
| --- | --- |
| GitHub `main` | Verify live; this documentation commit is newer than pre-doc `main` `697e9900b6017844cbfcba01d9815308523a053e` |
| M23.4 merge | PR #41, merge commit `e074d41a…`; this documentation commit follows it |
| Post-merge CI | `offline-ci` run `33174563501`, completed/success |
| M23.4 independent final review | approved; `P0=0`, `P1=0`, `P2=0`, `P3=0` |
| Deployed source | `e074d41a979af92b50bee880d6d55295ca65413d` |
| Retained deployed Wheel | `/opt/binance-market-data-recorder/release-e074d41/binance_market_data_recorder-0.1.0a1-py3-none-any.whl` |
| Current deployed Wheel SHA-256 | `48784824f9d7501ddb5f56a210fcf6b846ae1dd8b46e3cc3dd71f96652c53d0c` |
| Historical unavailable Wheel SHA-256 | `7dfef238514dbb3fc1ceb56e1b395eefbb8a85516bd4ffcf784daaf1260634a1` (historical only) |
| Production lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Production config SHA-256 | `5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb` |
| systemd unit SHA-256 | `69f3c4c2c77a3e6fc4ee397d26ecb2927a741a6165024b2f6656727d0b398b83` |
| Current new-host deployment identity | `6856d07f54bb27f2b375443f4e96abf8f551babd530bafda94c167242aaaac24` |
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

## New-host non-formal validation freeze

The new GreenCloud Tokyo host is an Ubuntu x86_64 ordinary KVM engineering
host. The Recorder service principal is `bmdr:bmdr`; internal active data is
under `/var/lib/binance-market-data-recorder` on `/dev/vda1`. The registered
archive target is `/srv/recorder-data/recorder-archive` on `/dev/vdb1`, with
storage ID `ef852751-721c-4145-9083-f6fd48718480` and vdb UUID
`95a2ce20-bf7a-4fae-98e8-d208517ae318`.

The following non-formal evidence is frozen outside GitHub. The substrate and
30-minute values are the evidence-manifest SHA-256; burn-in values are the
frozen evidence archive SHA-256:

| Stage | Result | Canonical duration | Evidence root | Evidence SHA-256 |
| --- | --- | ---: | --- | --- |
| Substrate | PASS | — | `/root/NEW-HOST-REBUILD-AND-SUBSTRATE-20260829T124039Z` | `c47a18ff2a0d636fb4fa1e18822a78bc8312c7c853ebd592d89f04bd6b0ef85b` |
| 30-minute qualification | PASS | `1800 s` | `/root/NEW-HOST-30MIN-QUALIFICATION-20260829T124656Z` | `2df8e1e6e87aa1dd4c8596a34e0d2bef1e0742c3e139deb51a9f0de1ad4ea7f0` |
| 2-hour independent burn-in | PASS | `7283.70763542 s` | `/root/NEW-HOST-2H-NONFORMAL-20260829T145116Z` | `a71e40e3896d5ba5e4290a4e4c7240e424cc9a7a2195a6dd2b65a8bddd503538` |
| 4-hour independent burn-in | PASS | `14400 s` | `/root/NEW-HOST-4H-NONFORMAL-20260829T235721Z` | `5fce74178445dd3945a197fbef064031ece25775057917f433e2491c47d8bbd0` |
| 12-hour independent burn-in | PASS | `43200 s` | `/root/NEW-HOST-12H-NONFORMAL-20260830T055931Z` | `da424835ad2e697ece123f6979c18454856978683a816b3e039978d3ef5c691b` |

The 12-hour interval began at `2026-08-30T06:09:09.842465860Z`, used Recorder
PID `129391`, and had zero systemd restarts. Spot core, USD-M core, order-book
sync, Catalog, storage, and archive-worker results passed; 634 archive timer
runs completed with zero worker failures and final eligible backlog zero. One
recoverable USD-M sequence-gap event was explicitly marked unreliable without
Catalog corruption. Optional taker-side data remained isolated: the report
records 72 accepted and 72 recoverable `RuntimeError` failures for
`taker_buy_sell_volume_5m`, with no core impact.

RSS is a watch item, not a proven leak: the observed maxima were approximately
`244400128` bytes at 2h, `260460544` bytes at 4h, and `286740480` bytes at 12h.
The 12-hour report classifies the trend as upward but inconclusive; there was
no swap, OOM, systemd restart, or clear resource exhaustion.

## Current validation phase

The current phase is **CPU HARDENING AFTER 12H NEW-HOST VALIDATION**. The
completed 30-minute, 2-hour, 4-hour, and 12-hour windows are frozen as
non-formal engineering evidence. The 24-hour window is paused and not started;
72-hour and 168-hour windows are not started:

```text
30m PASS -> 2h PASS -> 4h PASS -> 12h PASS -> 24h PAUSED/NOT_STARTED
```

The pause is narrow and operational. GreenCloud's current
[Terms of Service](https://greencloudvps.com/terms-of-service.php), checked
2026-08-31, states that ordinary KVM VPS CPU cores are shared except VDS, that
average CPU usage should not exceed 30%, and that bursts to 100% are allowed
for 10 minutes every 24 hours. The operator's recent provider-panel reading
was approximately 20% during Recorder operation. This is an
**OPERATOR / PROVIDER-PANEL OBSERVATION**, not a scientific Recorder CPU
benchmark. The current deployment therefore has limited CPU headroom under the
provider policy, and longer burn-ins are paused pending exact-head profiling.

The next engineering sequence is deliberately narrow:

```text
exact-head profiling -> select one measured hot path
                     -> correctness-preserving Python optimization
                     -> deterministic same-workload A/B
                     -> live production-equivalent A/B if justified
                     -> review -> select another optimization only then
```

Provider-panel CPU% alone is not an optimization benchmark. Future evidence
should prefer process CPU seconds, CPU seconds per million Raw events or per
payload byte/GB where useful, event rate, payload volume, queue/high-watermark,
backpressure, writer/fsync/seal latency, RSS, FD/thread behavior,
reconnect/resync, and Raw/manifest/Catalog correctness. An approximately 20%
normalized process-CPU reduction may be used as an engineering decision
heuristic; it is not a correctness contract or provider guarantee.

CPU work must preserve Raw v1, exact payload bytes, receive timestamps,
canonical CBOR, CRC32C, SHA-256, bounded ingress, durability/fsync, seal/
manifest/Catalog ordering, reconnect/discontinuity evidence, gap/resync,
crash/recovery, deterministic replay, and Spot/USD-M sequence semantics. Do
not trade these for CPU savings through longer fsync intervals, removal of
CRC/SHA, float substitution, silent metrics/gap deletion, or unreviewed stream
merging.

No profiling or optimization has started in this documentation update. M23.1
and M23.2 remain fresh-profiling decisions, M23.5 remains unauthorized unless
Python remains a measured bottleneck after cheaper algorithmic work, and a full
C++ or Go rewrite remains unauthorized.

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

If CPU optimization changes production code, the result must use a new
immutable Wheel and deployment identity. The prior 30m/2h/4h/12h duration does
not transfer; the old evidence remains baseline/historical evidence only. A
new candidate must complete a separately selected short qualification and
progressive validation sequence before any longer burn-in. Formal M22.9 is
separate and receives no duration credit.

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

The frozen new-host 12-hour evidence is also preserved at
`/root/NEW-HOST-12H-NONFORMAL-20260830T055931Z` with archive SHA-256
`da424835ad2e697ece123f6979c18454856978683a816b3e039978d3ef5c691b`.

Do not alter or overwrite the evidence. The M23.4 package is non-formal support
evidence and does not substitute for a future M22.9 chain.

## Next action

Design and execute **EXACT-HEAD CPU PROFILING** against the deployed source
`e074d41a979af92b50bee880d6d55295ca65413d` only after separate authorization.
Do not start the 24-hour burn-in, formal M22.9, a 278-hour chain, or M23.5 as
the next action. If a measured optimization is later selected, build a new
immutable artifact and restart validation without transferring duration credit.
