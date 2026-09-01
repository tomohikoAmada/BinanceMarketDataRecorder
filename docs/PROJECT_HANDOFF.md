# BinanceMarketDataRecorder — Project Handoff

This document lets a new researcher, reviewer, or developer resume work without
chat history. It separates current GitHub engineering source, the older
deployed VPS artifact, frozen historical evidence, and future plans. Verify live
authority before acting: documentation does not replace checking GitHub, the
installed artifact, service identity, or durable state.

## Start here — takeover checklist

1. Verify live GitHub `main`; the pre-documentation authority for this refresh
   is `c7d7900830e39d17470059f670ab2e5005e57103`.
2. Treat current GitHub `main` and the deployed source as different identities:
   `main` is newer; the VPS artifact remains
   `e074d41a979af92b50bee880d6d55295ca65413d`.
3. Confirm PR #45, PR #46, and PR #47 are merged and that PR #47 post-merge
   CI run `33456832048` succeeded on macOS and Ubuntu.
4. Keep `PRODUCTION_READY=NO`, formal M22.9 `NOT_STARTED`, and the non-formal
   24h/72h/168h status as paused/not started below.
5. Before profiling or validation, obtain separate authorization and choose
   current-main development profiling or a new immutable artifact plus fresh
   short deployment qualification. Do not inherit old duration credit.

## 1. Current authoritative state

| Area | Status | Authority |
| --- | --- | --- |
| GitHub main | Verify live; pre-doc authority `c7d7900830e39d17470059f670ab2e5005e57103`; this documentation commit is newer | current engineering source |
| Recent closed engineering work | PR #45 BBO/taker/side-data hardening; PR #46 remote-delete SQLite snapshot; PR #47 shared USD-M public REST gate | all integrated into main |
| Hardening phase | CLOSED | PR #45/46/47 integrated |
| PR #47 post-merge CI | Passed | `offline-ci` run `33456832048`, macOS and Ubuntu |
| VPS candidate | Older source deployed and non-formally validated through 12h | identities and frozen evidence below; not current main |
| Current main deployed | NO | no duration credit transfers from the VPS candidate |
| Current phase | Handoff / next phase requires separate authorization | 24h paused/not started; 72h/168h not started |
| Formal M22.9 | Not started | precondition stopped before T0 on capacity |
| Production Ready | No | formal terminal chain not complete |
| M23.5 | Deferred/evidence-gated | not authorized and not next |

At the close of the 2026-08-30 12-hour non-formal window, the candidate was
ACTIVE and READY with zero restarts. This dated observation is not a durable
PID guarantee. Recheck the live service and incarnation before a new operation.

## 2. What is complete

The Recorder is a long-running Python 3.12 service for Binance public market
data. It captures BTCUSDT Spot and USD-M perpetual public streams/snapshots,
preserves exact Raw payload bytes with integrity/provenance metadata, records
explicit reconnect/gap state, and supports deterministic normalization, replay,
historical import, archive lifecycle, operational reporting, and non-root
systemd/LaunchAgent profiles. It has no accounts, API keys, orders, strategies,
or trading capability.

Meaningful closed workstreams recorded by the milestone plan and evidence are:

- M0–M18: project/storage contracts, source verification, Raw framing and crash
  recovery, Spot/USD-M capture, order-book quality, side data, metrics, capacity,
  storage/archive protocols, supervision, normalization, replay, operations,
  packaging, and documentation foundations;
- M19/M19.1: historical backfill and bounded retained-window catch-up;
- M20: Ubuntu ARM64/systemd and unified proxy support;
- M21-era work: bounded backpressure, reconnect/gap evidence, recovery, and
  long-window incident evidence, preserving each historical pass/fail result;
- M22.0–M22.8: remote/archive-set protocol, receipts, remote lifecycle and
  recovery, snapshot/transfer, VPS capacity/deployment gates, and accepted
  fixed cross-machine M22.8 evidence;
- M23.0/M23.0F: production-equivalent profiling that identified the redundant
  synchronous seal scan;
- M23.4: incremental clean-seal evidence, independently approved, merged,
  deployed, and passed in a two-hour non-formal VPS validation on its older
  candidate; this remains historical artifact-specific evidence.

Historical milestone results remain historical. Old M21 and M22.9 incident
records are not rewritten as if the current candidate existed at their T0.

## 3. Current deployed candidate

| Identity | Value |
| --- | --- |
| Source SHA | `e074d41a979af92b50bee880d6d55295ca65413d` |
| Wheel | `binance_market_data_recorder-0.1.0a1-py3-none-any.whl` |
| Retained Wheel path | `/opt/binance-market-data-recorder/release-e074d41/binance_market_data_recorder-0.1.0a1-py3-none-any.whl` |
| Current Wheel SHA-256 | `48784824f9d7501ddb5f56a210fcf6b846ae1dd8b46e3cc3dd71f96652c53d0c` |
| Historical unavailable Wheel SHA-256 | `7dfef238514dbb3fc1ceb56e1b395eefbb8a85516bd4ffcf784daaf1260634a1` (historical only) |
| Production lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Production config SHA-256 | `5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb` |
| systemd unit SHA-256 | `69f3c4c2c77a3e6fc4ee397d26ecb2927a741a6165024b2f6656727d0b398b83` |
| Current deployment identity | `6856d07f54bb27f2b375443f4e96abf8f551babd530bafda94c167242aaaac24` |
| Service principal | `bmdr:bmdr` |
| Internal data root | `/var/lib/binance-market-data-recorder` on `/dev/vda1` |
| Registered archive target | `/srv/recorder-data/recorder-archive` on `/dev/vdb1` |
| Storage ID | `ef852751-721c-4145-9083-f6fd48718480` |
| vdb UUID | `95a2ce20-bf7a-4fae-98e8-d208517ae318` |
| VPS role | temporary engineering validation host |

This is the validated deployed candidate, not formal M22.9 acceptance or a
Production Ready declaration.

## 4. Latest evidence

### M23.0/M23.0F baseline

M23.0/M23.0F profiling found the synchronous path `StreamSpool.drain_all ->
drain_one -> _seal_current -> seal_partial -> scan_chunk`. The 7200-second
M23.0F baseline had average/peak CPU of `41.8757%`/`92.9835%`, three
backpressure episodes totaling `22.3055 s`, queue depth `8192`, maximum seal
latency `11.532059644 s`, and `scan_chunk` at `9.2728%` of global samples and
`91.8529%` of seal samples. Its own accounting recorded no gaps, resyncs,
restarts, or terminal failures.

### M23.4 correctness and merge

Do not collapse these identities:

| Step | SHA / result |
| --- | --- |
| Source before M23.4 | `86e056cbcaebb6ec2a84c00e31e6854b7898aa7e` |
| Initial implementation | `5d89e467eba50e2758b7187d937b3ca62dcbc808` |
| First correction | `05a04a45ca7d8132e41e89ee200cf83617ced51a` |
| Final P1 correction | `3ba1c217e5d1a68c4ee6d27439ad60a8d7ec93a0` |
| Final independent review | approved; P0/P1/P2/P3 all zero |
| PR / merge-main | PR #41 / `e074d41a979af92b50bee880d6d55295ca65413d` |
| Post-merge CI | run `33174563501`, success |

Normal clean rotation now uses writer-owned incremental evidence from a private
immutable semantic snapshot. Exact Raw SHA/bytes and semantics commit only
after complete writes; ambiguous writer/fsync/close failures poison one-shot
memory-only evidence. Full compressor/decompression validation remains, Raw-v1
64-KiB header encode/decode authority stays aligned, retained sources converge
without archive lifecycle reversal, and ordinary `SEALED` retained Raw cannot
be deleted without a valid sealed artifact. Raw v1 is unchanged.

General `seal_partial()`, startup/recovery, unknown/recovered/poisoned partials,
and the zero-record reconnect-marker path where applicable retain full scans.

### M23.4 post-merge two-hour result

The non-formal production-equivalent B run passed runtime integrity, structural
performance, and measured performance:

| Metric | Result |
| --- | ---: |
| Duration | `7200.001773005002 s` |
| CPU average / peak | `85.617193%` / `103.899556%` |
| RSS start / end / peak | `219152384` / `255397888` / `263421952` bytes |
| Seal count | `1137` |
| Seal latency min / p50 / p95 / p99 | `0.005654295` / `0.116858659` / `0.835251814` / `1.191445053 s` |
| Conservative maximum seal latency | `2.043359872 s` |
| Maximum latency reduction vs A | `82.281050%` |
| Backpressure episodes / timeouts | `0` / `0` |
| Maximum observed queue depth | `7325 / 8192` |
| Restart / SIGKILL / OOM / resync / terminal | all `0` |
| Normal clean-seal `scan_chunk` | not observed |
| `scan_chunk` global / seal path global | `0.0%` / `1.532027%` |

B sealed 9,464,735 records versus A's 3,013,877, about 3.1404 times the
workload. Raw CPU percentages are not directly comparable. Frozen evidence
gives `1000.389` versus `651.306` CPU seconds per million sealed records, a
34.8947% normalized improvement.

Repository detail is in
[`milestone_evidence/M23.4-incremental-clean-seal.md`](milestone_evidence/M23.4-incremental-clean-seal.md).
VPS evidence remains outside GitHub:

```text
/opt/binance-market-data-recorder/m23-profiling/M23.4-POST-MERGE-20260828T143255Z
/opt/binance-market-data-recorder/m23-profiling/M23.4-POST-MERGE-20260828T143255Z.tar.gz
SHA-256 cb3c2998090545778dd5a0dc2d1cec333d08729120fb8a097de053a63fddb1a2
```

The B run had 17 external disconnect intervals: 17 STARTED, 17 COMPLETED,
0 OPEN. They were later classified `CLOSED_HISTORICAL_BASELINE`; they are not
17 unresolved gaps, and the project must not claim zero network disconnects.

## 5. Current project phase

The project is in **CPU HARDENING AFTER 12H NEW-HOST VALIDATION**. The frozen
non-formal sequence is:

```text
30m PASS -> 2h PASS -> 4h PASS -> 12h PASS -> 24h PAUSED/NOT_STARTED
```

The 12-hour evidence root is
`/root/NEW-HOST-12H-NONFORMAL-20260830T055931Z` with SHA-256
`da424835ad2e697ece123f6979c18454856978683a816b3e039978d3ef5c691b`.
The 12-hour window ran for `43200` canonical seconds from
`2026-08-30T06:09:09.842465860Z`, with zero systemd restarts, 634 successful
archive timer runs, zero archive-worker failures, final eligible backlog zero,
and one recoverable USD-M sequence-gap event represented by an explicit
unreliable marker. Optional `taker_buy_sell_volume_5m` side data recorded 72
accepted and 72 recoverable RuntimeError failures without core impact.

The current GreenCloud ordinary KVM policy is an operational constraint, not a
Recorder benchmark. The official [Terms of Service](https://greencloudvps.com/terms-of-service.php),
checked 2026-08-31, states that KVM CPU cores are shared except VDS, average
usage should not exceed 30%, and bursts to 100% are permitted for 10 minutes
every 24 hours. The operator observed approximately 20% in the provider panel
during recent Recorder operation; classify this only as an **OPERATOR /
PROVIDER-PANEL OBSERVATION**. Longer non-formal burn-ins are paused because
the deployment has limited CPU headroom under that policy.

The next sequence is exact-head profiling, selection of one measured hot path,
one narrow correctness-preserving Python optimization, deterministic
same-workload A/B, live production-equivalent A/B if justified, review, and
only then another optimization. Provider-panel CPU% alone is not an
optimization benchmark; collect normalized CPU seconds, workload volume/event
rate, queue/high-watermark, backpressure, latency, RSS, FD/thread,
reconnect/resync, and Raw/manifest/Catalog correctness evidence.

CPU work must preserve Raw v1, exact payload bytes, receive timestamps,
canonical CBOR, CRC32C, SHA-256, bounded ingress, durability/fsync,
seal/manifest/Catalog ordering, reconnect/discontinuity evidence, gap/resync,
crash/recovery, deterministic replay, and Spot/USD-M sequence semantics. Do
not use longer fsync intervals, remove CRC/SHA, substitute floats for exact
numeric semantics, silently delete metrics/gaps, or merge streams without
review.

The RSS trend remains **WATCH / NOT YET PROVEN LEAK**: approximately
`244400128` bytes at 2h, `260460544` at 4h, and `286740480` at 12h. The 12-hour
report found stepwise growth without swap, OOM, restart, or clear exhaustion.
Continue observing it during later profiling and new-artifact validation.

These are adjustable engineering checkpoints, not formal M22.9 stages.

## 6. Formal M22.9 status

Formal M22.9 has **NOT STARTED** for the current candidate. A precondition-only
inspection found service continuity and healthy Catalog/Raw/manifest authority,
but only about `32.523064 h` of free-space runway. Formal authority requires an
environment able to support the complete independent approximately 278-hour
chain (`2h + 12h + 24h + 72h + 168h`). The gate stopped before T0 with
`INSUFFICIENT_MEASURED_CAPACITY_RUNWAY`; no root or acceptance ID was created.

This is not a formal-run failure. The 278-hour chain is a later gate, not the
next test. Independent non-formal runs can reclaim disposable data safely after
evidence is frozen.

## 7. Open work / not yet done

- The 24h checkpoint is paused/not started; 72h and 168h are not started.
- No next engineering action is automatically authorized; the next phase
  requires separate authorization and an explicit current-main path.
- M23.1/M23.2 remain fresh-profiling decisions, and no group of optimizations
  is authorized together.
- Formal M22.9 has not started; its capacity-complete environment is not ready.
- Production Ready is not authorized.
- M23.5 native C++ remains unauthorized unless Python remains a measured
  bottleneck after cheaper algorithmic work. Full C++ and Go rewrites remain
  unauthorized.
- Other open/monitoring items remain in [`risk_register.md`](risk_register.md)
  and [`known_limitations.md`](known_limitations.md); M23.4 does not implicitly
  close them.

## 8. Current known risks / operational lessons

- Formal M22.9 needs capacity runway for the whole chain before T0.
- An optional unbounded full-history audit consumed excessive RAM after the 2h
  window and was terminated. Do not repeat it on this small VPS; use bounded,
  project-owned mechanisms.
- If CPU optimization changes production code, build a new immutable Wheel and
  deployment identity. The completed 30m/2h/4h/12h duration does not transfer;
  a new candidate must begin a separately selected short qualification and
  progressive validation sequence.
- Non-formal data may be disposable, but cleanup must be separately authorized
  and keep Catalog, Raw, manifests, and archive authority consistent.
- Traffic variance makes raw CPU percentages misleading; include record/byte
  workload context.
- Closed reconnect intervals are not unresolved gaps. Preserve STARTED,
  COMPLETED, and OPEN context.
- During the M23.4 work `/dev/null` was briefly replaced and immediately
  restored. Neither operator incident changed the Recorder process or durable
  state, but both remain disclosed lessons rather than hidden events.

## 9. Next operator/developer action

`NEXT_PHASE=REQUIRES_SEPARATE_AUTHORIZATION`. Verify live main, then choose
either fresh profiling/development against exact current GitHub main or a new
immutable artifact from current main followed by a separately authorized short
deployment qualification. Build a new Wheel and identity if deploying; do not
inherit the older 30m/2h/4h/12h duration credit. Do not automatically start
24h burn-in, formal M22.9, M23.5, or a C++/Go rewrite.

## 10. Recent closed engineering hardening

PR #45 removed the BBO duplicate best-price scan and corrected USD-M taker
retry/cursor and initial side-data rate-limit behavior. PR #46 corrected
remote-delete mixed-generation authority by using one SQLite read snapshot. PR
#47 introduced one collector-owned USD-M public REST cooldown/request lock for
core depth snapshots and side-data REST, including cancellation preservation of
completed 418/429 observations before lock release. These are current GitHub
engineering facts, not VPS deployment or long-run validation evidence.

## 11. Remaining nonblocking backlog

The following do not reopen the closed shared-gate P2 or remote-delete P1:

- repeated-cancellation supplemental regression test-strength P3;
- process-memory-only cooldown state across restart;
- conservative typed/no-header 418 fallback over-delay risk;
- documentation/source-provenance item if still genuinely unresolved;
- RSS WATCH / not a proven leak;
- R-034 Spot bootstrap official-wording conflict;
- unresolved formal capacity runway.

## 12. Rules for the next researcher/developer

- Verify live GitHub, artifact, host/boot/process incarnation, service health,
  and durable-state authority first.
- Prefer repository contracts and frozen evidence over chat summaries.
- Do not modify source, configuration, or capacity during a validation window.
- Never label non-formal runs as formal M22.9 evidence.
- Freeze and hash evidence before data retirement.
- Never delete Raw, Catalog, or manifests inconsistently; use a separately
  authorized safe procedure.
- Do not overwrite or reinterpret historical evidence to match current state.
- Do not start speculative M23.5 or a redesign without a measured gate.
- Do not repeat optional unbounded full-history audits on the small VPS.
