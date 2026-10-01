# MVP qualification plan through accepted 24h

Owner-requested plan, October 1, 2026. This is a proposed development/execution
sequence, not a claim of qualification or an instruction to resume the paused
VPS workflow. Review basis: main `d6f37576f5b044bf578501cba291fe889e45a919`.
See the [architecture review](reviews/2026-10-01-architecture-review.md) for
reproduced defects and optimization evidence.

## Objective and fixed scope

Reach an independently reviewed Formal **24h PASS** for one frozen artifact,
configuration, ProductKey workload and explicitly chosen data corpus. Keep
Python 3.12, one process, Spot/USD-M modules, Raw v1, SQLite and native systemd.
No new trading/UI/service framework or native-language rewrite.

The chain is identity → passing stopped baseline → fresh readiness → 2h → 12h
→ 24h. Each duration is a separate stage with its own T0, online target,
operator-controlled stop/quiescence, complete terminal audit and independent
verification. Minimum online time is **38 hours**, excluding baseline, readiness,
audit, maintenance and retries. Old V4 and interrupted V5 evidence grants no
new-artifact credit. 72h/168h and Production Ready are outside this plan;
24h completion retains `PRODUCTION_READY=NO`.

Default workload is the already reviewed four ProductKeys and twelve core
contexts, with the same auxiliary settings. Freeze their actual config values
from the installed authority during preflight; do not guess the symbols from
old examples. Any intentional reduction is a distinct declared qualification
profile, not certification of the wider previous workload.

## Milestones

| ID | Deliverable | Exit gate | Next |
|---|---|---|---|
| Q0 | Architecture review and executable offline probes | Six confirmed findings, baseline checks, scoped plan; no live changes | Q1 |
| Q1 | Small correctness fixes | Heartbeat failure is supervised, inaccessible cwd does not crash CLI, normalized product identities cannot collide; regressions and offline suite PASS | Q2 |
| Q2 | Practical observation/audit path and selected corpus | Sustained delta consumption keeps up; complete exact audit is feasible; corpus/policy decision is documented and reviewed | Q3 |
| Q3 | Frozen release, stopped deployment, baseline and readiness | Exact Wheel/lock/config/unit identity; package/maintenance/capacity gates; complete verified baseline; every configured product READY | Q4 |
| Q4 | Formal 2h | Valid target, full terminal PASS, independent verifier PASS, reviewed eligible final | Q5 |
| Q5 | Formal 12h | Same artifact/profile and passing 2h predecessor; full terminal PASS and independently reviewed eligible final | Q6 |
| Q6 | Formal 24h and closeout | Same artifact/profile and passing 12h predecessor; planned connection rotation/recovery evidence, full terminal PASS and independently reviewed final | Stop at 24h |

Q0 is the only work delivered in this review. Subsequent milestones are pending.

### Q1 — Repair confirmed correctness defects

Implement F2/F6 with the current runtime/discovery structures; no new lifecycle
framework. Heartbeat unexpected return/exception must stop collectors coherently,
retain the causal failure and report FAILED; ordinary requested shutdown remains
STOPPED. Run real state-write failure, early task exit and stop/cancellation tests.

Fix F3 at a consistent identity boundary, testing depth snapshots from distinct
symbols, server shutdown across markets/streams, and valid same-product duplicates.
Changing dedup semantics must change its declared version/content-addressed build
identity, preserve existing immutable outputs, and maintain their readers.
This is an offline consumer defect; avoid expanding into a Parquet redesign.

Keep F4 outside the critical path because normalize/replay are offline duties.
For F5, use measured duplicate/quality rates to decide whether the small bounded
production-history change belongs in this release; preserve durable gap facts
and checkpoint intervals. No speculative queue or thread-pool tuning.

Gate: focused regression tests, full offline pytest, Ruff, mypy, M0 and Raw
golden. Exact Linux/macOS CI is required before release. No VPS action in Q1.

### Q2 — Make qualification affordable and able to keep up

**Fix observation service rate first.** Reproduce the selected real stream count,
60s rotation, real archive transitions and enabled auxiliary work in an offline
running-path test with several consecutive observation windows, plus one missed
window/catch-up and archive retirement race. At the current core-only row rate,
480 chunk and 300 archive rows arrive per 300 seconds, above 256/256 capacity.
The baseline must not hide this by seeding all generated rows as historical.

Try the least invasive supported choices first:

1. Measure longer existing rotation intervals while preserving <=1s fsync and
   the existing size cap. Use this only if real row/byte rates and recovery
   behavior show adequate headroom for the explicitly selected workload.
2. If parameter tuning cannot sustain the workload, make a narrow acceptance
   policy revision: budgeted bounded batches with continuous cursor replay.
   Cover SQL batches, serialized bytes, causal references and the frozen
   snapshot together. Reuse existing immutable shards if necessary. Keep
   old V5 evidence semantics/readers; version evidence only when semantics
   change. Do not prescribe another large acceptance subsystem in advance.

Any change to ADR-0034's one-page authority requires a short reviewed ADR before
implementation. Retain 300s target cadence, 600s gap failure, 900s recoverable
readiness and the meaning of unresolved target work unless the owner explicitly
selects a documented policy change. Never count terminal audit as online credit.

**Reduce work before changing assurance.** Add compact archive status for normal
operator checks. Profile stored reads, decompression, CBOR decode/re-encode, SQL
companions and duplicate identity verification separately. Merge stored hashing
and decompressed qualification through one descriptor where exact equivalence
is proven. Keep CRC, both SHA identities, size/statistics and mutation detection.
Start with one worker; bounded concurrency is optional only after measurements
show a bottleneck and memory/I/O headroom on the shared host.

**Select the starting corpus explicitly.** Recommended MVP scope is a fresh,
isolated qualification corpus with the same selected runtime workload, retaining
the 143,362 historical manifests/Raw/Catalog and interrupted evidence as a
separate historical custody/audit obligation. This demonstrates a new artifact's
capture, growth, archive, recovery and rotation; it does not certify historical
archive integrity or large-history startup. Use a small targeted existing-history
fixture alongside it; do not add another long duration gate.

This recommendation is not an authorized reset. Current V5 automatically audits
the entire data root, and production identity enforces canonical paths. Therefore
an empty subdirectory cannot simply be substituted. Before this option is used,
document a minimal qualification-root/deployment-profile exception or a separately
reviewed consistent corpus separation, including separate Catalog/registration
and rollback authority. Do not move/delete the old tree as routine cleanup,
rewrite old Catalog rows, change archive markers in place or claim that a clean
root solves R-078. Exact deployed identities must reflect the actual paths.

If the owner chooses the existing corpus instead, preserve ADR-0034 full-corpus
qualification, apply safe scan optimizations and complete its baseline before
T0. Do not carry on an unchanged >14h partial operation merely because progress
exists. Decide using a measured complete representative audit and an explicit
wall-time estimate for both producer and independent LIVE passes. Removing the
second full LIVE pass is a policy/assurance change requiring a separately reviewed
ADR, not a transparent optimization.

Q2 gate:

- Selected workload's delta backlog is stable and drains after a one-window
  disturbance; terminal processed cursors equal its frozen target high-waters.
- Representative production-format Raw/archive passes a complete audit and
  independent verification, with corruption/loss/replacement/retirement negatives.
- Record unique stored/uncompressed bytes, rows/sec, phase wall/CPU time,
  sampled RSS, cgroup memory separately, and estimated 24h corpus cost.
- Suggested engineering target for the bounded MVP corpus: complete baseline
  and terminal qualification including the required independent pass within
  two hours each. This is a planning target, not an existing contract or a
  reason to omit data; if infeasible, resolve scope/cost before Q3.
- Report R-078 separately: fresh-corpus success does not close old-corpus P2.
- Explicitly documented corpus/paths, operator procedure and compatibility;
  offline regressions and independent review PASS. No live restart in Q2.

### Q3 — Freeze once and verify the operational procedure

Build one exact immutable Wheel from reviewed source, pin dependency lock,
record source/tree/Wheel/config/unit/deployment hashes, run dual-platform CI,
locked build, fresh-wheel install and dependency/CLI smoke checks. Build the
canonical environment in place; retain coherent rollback materials. Keep
Recorder disabled until the selected authorized pre-start.

Production preflight refreshes package/boot/reboot/lock state, capacity and archive
readiness. Install pending maintenance before T0, then use the existing bounded
runtime masks during each selected timed window. Check active AND enabled state.
Restore normal OS update authority in closeout; do not disable updates indefinitely.

Complete the selected stopped baseline and independent verification; any partial
root is not a passing predecessor. Start Recorder through systemd, prove all
configured products READY and inspect a short ordinary warm-up's measured rates,
queue/backpressure, RSS and archive progress. This warm-up is readiness work,
not an extra mandatory 30m/4h duration chain. Fixes found here return to code
review/freeze; do not silently swap an artifact inside an accepted chain.

Before starting the first stage, prepare a concrete operator procedure for target
publication, stop/disable, archive drain, paused mutators, terminal audit, verifier,
timer/maintenance restoration and failure retention. Monitoring reads compact
evidence and never treats expected recoverable NOT_READY as an independent
immediate failure or relaunches a new stage when systemd/host maintenance restarts.

### Q4/Q5/Q6 — Run the separate 2h → 12h → 24h gates

For each stage, repeat exact identity, fresh readiness, maintenance and capacity
gates; bind its exact passing predecessor; create exactly one authorized T0.
Keep the code, dependency set, ProductKeys, streams and profile fixed throughout
the chain. Recorder should use ordinary automatic reconnection and archive
behavior; do not suppress these paths to obtain an artificially easy run.

Observe canonical cadence, process/service/boot identity, disk reserve, cursor
pending trend, queue/backpressure, RSS/CPU and archive backlog. Use the existing
readiness recovery policy; only its authoritative acceptance path produces the
stage verdict. Capacity planning includes active/seal overhead and the selected
archive runway, not all 278 hours of future qualification.

At target, online candidate duration stops accruing. Operator stop/disable,
verified archive drain and quiescence precede terminal qualification. Accept only
after a complete terminal root, independent verification and reviewed eligible
final. An online PASS_CANDIDATE alone cannot advance. Do not automatically chain
stages from an unchanged/paused monitor.

For 24h, inspect the normal ~23h50m connection rotation and full per-product gap,
resnapshot/recovery evidence. Existing offline fault injection covers deliberate
kill/disk/network faults; do not add destructive live fault trials to the same
formal window. A defect or incomplete stage retains its evidence and yields no
credit; fix the cause before a separately selected retry. An observer interruption
may resume only under the existing same-process/boot/service constraints; no new
T0 or credited cross-maintenance process identity is inferred.

Final handoff records exact artifact/profile/corpus, 2h/12h/24h finals and review
results, rotation/recovery, integrity/resource/coverage limitations, stopped and
disabled Recorder state if handing off stopped, and restored archive/OS authority.
This plan ends there. Normal continuous operation or 72h/168h remains a subsequent
owner-selected task.

## Work intentionally left after 24h

Bounded normalization merge/group improvements; portable archive-client/platform
certification; older historical corpus audit/scalability; V1–V4 code isolation;
broader products/auxiliary coverage; long-duration qualification. Raw integrity
and deletion checks remain mandatory throughout, even where a follow-up is deferred.
