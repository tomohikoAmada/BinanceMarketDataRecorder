# MVP milestone plan: 38 accepted hours on the cloud server

Updated October 2, 2026 following the owner’s performance instructions and choice
of safe old-data cleanup followed by a fresh independent qualification corpus.
The stable filename preserves links from the original 24h plan. See the
[cloud analysis and implemented optimizations](reviews/2026-10-02-cloud-performance-and-38h.md)
and [original review](reviews/2026-10-01-architecture-review.md).

## Definition of done

**Formal 2h PASS + Formal 12h PASS + Formal 24h PASS**, at least **38 accepted
hours (136,800 target seconds)**, on one frozen artifact/configuration/workload
and corpus lineage on the cloud server. Each stage must have a valid T0/target,
complete terminal audit, independent verification and reviewed eligible final.
A single 38h process uptime, code completion or microbenchmark PASS is insufficient.
Baseline, readiness, audit, maintenance and retries add wall time, not credit.

Sequence: exact identity → passing stopped baseline → fresh readiness → 2h → 12h
→ 24h. Old failed/partial and V4 records give no new-artifact credit. End after
these three accepted stages; 72h/168h are separate scope. The existing broader
`PRODUCTION_READY` status remains NO after this plan’s narrower endpoint.

## Fixed MVP scope

Cloud target: greencloud-tokyo-01, Ubuntu 24.04 x86_64, currently four logical CPUs
and about 6 GB RAM. Keep Python 3.12, one Recorder process, SQLite, Raw v1, verified
archive and systemd. The installed public configuration was read-only checked:
Spot BTCUSDT/ETHUSDT and USD-M BTCUSDT/ETHUSDT; twelve core contexts; rotation
60s / 128 MiB and durability 1s. Retain auxiliary and network settings and freeze
their exact values before deployment. Mac timings prove no cloud performance gate.

## Milestones

| ID | Work | Completion gate | Status |
|---|---|---|---|
| Q0 | Review and cloud performance optimization | Cloud Raw proofs/totals equal; sustainable bounded-delta regressions; offline gates | Engineering candidate delivered; independent review/CI pending |
| Q1 | Small correctness fixes | Heartbeat failure/early exit supervised; unreadable-cwd CLI works; normalized product identity isolated with versioned outputs | Next development task |
| Q2 | Release review and cloud performance decision | Reviewed exact wheel/lock; clean install; dual-platform CI; practical full-observation/audit forecast | Pending |
| Q3 | Safe old-data cleanup, fresh corpus and stopped deployment/readiness | Coherent custody/rollback; new canonical Catalog/archive registration; exact identity; passing baseline; all products READY | Pending; fresh-corpus choice authorized |
| Q4 | Formal 2h | Target, complete terminal audit and independently reviewed eligible final PASS | Not started |
| Q5 | Formal 12h | Same artifact/profile and eligible 2h predecessor; complete final PASS | Not started |
| Q6 | Formal 24h and 38h closeout | Same artifact/profile and eligible 12h predecessor; rotation/recovery; complete final PASS; all three finals bind ≥38h | Not started |

No cleanup, deployment, Recorder start, Formal stage or monitor resume occurs in
Q0. Installed operational authority remains in [production state](CURRENT_PRODUCTION_STATE.md).
Do not mark the 38h plan complete when engineering alone passes.

## Q0/Q1 — Engineering work

Q0 has implemented lazy boundary summaries with complete per-frame validation,
compact archive status with explicit detail pages, and [ADR-0035](adr/0035-v5-bounded-delta-batches.md)
finite batches: four SQL pages of 256 rows/family, 1024 causal references, 7 MiB
delta entries, existing 8 MiB document and 240s budget. Old starts retain original
one-page policy on replay/resume. Cloud samples show about 21% lower scanner wall
time and archive status/serialization reduced from 7.76s/163 MB to 0.18s/416 bytes.
Normal four-product seal/archive windows consume 480/300 rows; a missed cadence
can defer byte-bounded work and subsequent observations drain it. These are not
complete live observations or full audit qualification.

Q1 uses current structures for heartbeat failure/early exit → coherent stop and
FAILED cause; normal stop remains STOPPED. Skip inaccessible optional Git discovery
without weakening data-root permissions. Namespace normalized dedup identity by
venue/market/symbol/stream, test cross-product equal inputs and valid duplicates,
version changed build semantics, and preserve old immutable outputs/readers.
Normalization fan-in can wait until after 38h. Measure quality-audit memory growth;
only add bounded production history if needed, preserving persistent gap facts.

## Q2 — Cloud gate; escalate only on a measured failure

Run focused and full offline tests, Ruff/mypy/M0/Raw golden, Linux/macOS CI, a
locked wheel build and fresh-wheel smoke. Independently review batching, including
historical replay/resume, causal tampering, snapshot races, cancellation and byte
bounds. Measure SQL, Raw decode/statistics, identity and publication separately.

The Q3 preflight warm-up must measure complete observations on the frozen cloud
artifact with core/auxiliary work, size-triggered seals and archive retirement.
Require stable normal backlog, recovery after one missed cadence, bounded memory
and working headroom inside 240s. Keep 300s target, 600s maximum evidence gap and
900s recoverable readiness. Unfinished target work remains INCOMPLETE.

Forecast complete audit cost for the growing new corpus, including BOTH producer
and independent LIVE passes, from actual event/byte growth and cloud scan rates
plus SQL/I/O overhead. Six chunks alone cannot predict it. Remove the previous
suggested two-hour audit target as an artificial blocker; agree on measured
practical wall time before T0. Never omit records or count audit time as credit.

If still insufficient: first test two bounded Python Raw workers with deterministic
ordered results and existing cancellation; no concurrent Catalog/evidence writes.
If residual parsing/object CPU cost dominates, compare an isolated Go or C/C++/Rust
scanner on identical cloud Raw, returning the existing small proof. Require full
format/strict-schema/CRC/hash/statistics/reconnect/mutation equivalence. Keep Python
transactions and exchange/service orchestration; no whole-repository rewrite.

Upgrade hardware only for demonstrated limits: usable parallelism or single-core
saturation for CPU, RSS/swap/PSI for RAM, iowait/device latency for storage. Include
steal and co-resident contention in cloud measurements. Freeze any changed host
profile before qualification. No hardware/native dependency is mandatory by default.

## Q3 — Safe cleanup and fresh qualification scope

The owner chose safe cleanup. Inventory exact project-owned active paths, Catalog,
manifests, archives and interrupted evidence. Keep Recorder inactive/disabled;
drain verified archive and check no active partials/unarchived Raw. Briefly pause
the project archive mutator for a coherent Catalog/manifest/registration rollback
snapshot. Preserve unrelated services and files.

Retire the old dataset from active qualification into verified recoverable custody.
Keep existing archived Raw/external manifests and historical evidence; a fresh test
does not require destroying their unique copies. Unarchived Raw is never deleted.
Physical cleanup is limited to explicitly identified redundant/test files with
canonical child-path/protected-path/mount-root checks from AGENTS.md. Never build
a deletion target across local/remote shell interpolation. No cleanup occurred in Q0.

Initialize a new Catalog/layout at the SAME canonical writer root
`/var/lib/binance-market-data-recorder` after retiring its coherent old scope.
Use a distinct registered archive directory on the already mounted archive volume,
outside the old registered data scope; freeze its exact path/UUID/storage ID/marker
and timer binding. No mount/format, symlink exception or general multi-root framework.
Do not import old rows/manifests into the new corpus or clear it between stages.
Fresh-corpus success does not close historical full-audit scalability R-078.

Deploy the reviewed wheel/lock in canonical paths, bind exact source/tree/wheel/
config/unit/deployment/archive-timer authority and complete the new stopped baseline
and independent verification. Refresh package/reboot/boot/lock, capacity and archive
checks. Use existing bounded maintenance masks during timed windows and restore
ordinary OS authority afterward. Enable/load Recorder at the selected pre-start,
prove four products READY and run the ordinary Q2 performance warm-up. No extra
mandatory 30m/4h duration chain. A required fix returns to review/freeze before T0.

## Q4/Q5/Q6 — Pass each stage and close out

Repeat identity, fresh readiness, maintenance and capacity gates; bind the eligible
predecessor and create exactly one T0. Keep code, dependencies, config/workload,
host profile and growing corpus lineage fixed. Use ordinary reconnection/archive
paths. Monitor compact evidence, process/service/boot identity, reserve, backpressure,
delta progress, RSS/CPU and archive backlog. Do not load giant all-history JSON
or let a paused monitor automatically restart work.

At target, stop accruing time; operator stop/disable, verified archive drain and
quiescence precede complete terminal audit and independent verification. Online
PASS_CANDIDATE alone cannot advance. Failed/incomplete windows retain evidence and
earn no credit; fix their cause before retry. Resume only under existing same-process/
boot/service constraints; host maintenance does not create an authorized new T0.

24h must include inspection of ordinary ~23h50m connection rotation, per-product
gaps and depth resnapshot/recovery. Existing offline kill/disk/network fault tests
remain; no destructive injection is added to the same Formal window.

Final handoff binds all three reviewed eligible finals and ≥136,800 accepted target
seconds, exact artifact/profile/corpus, integrity, rotation/recovery and measured
resource limits. If handing off stopped, Recorder is inactive AND disabled;
archive and normal OS maintenance authority are restored. Only then record
`QUALIFICATION_38H=PASS`. This plan ends there.
