# Current Production State

## M4 third run (2026-11-03)

Recorder source `f3804c3` began warm-up at 13:08:22 UTC on GreenCloud Tokyo with PID 45800 and invocation `c3ac2c4578724ee8a4f6366d31c35929`. The external observer writes to `/srv/recorder-data/recorder-archive/m4-f3804c3-20261103/`. M4.1 passed at 13:17:22.316038728 UTC with 50/50 depth bridges, 4,038 active sources available, and zero pending. The 2h checkpoint falls at 15:17:22 UTC on November 3. The 12h and 24h checkpoints fall at 01:17:22 and 13:17:22 UTC on November 4. Production readiness is pending.

## M4.2 checkpoint (second run, historical, 2026-11-03)

The second run failed the 2h per-source availability gate. 344 of 4,038 frozen sources exceeded 36 seconds of unavailability or uncertainty. Recorder stopped cleanly, is inactive and disabled, and left zero partial chunks. The observer stopped and OS updates resumed. The 12h and 24h checkpoints for this run are cancelled. The next qualification starts at M4.1 with a new T0. See [the milestone record](production_milestone_plan.md).

## M4 rerun baseline (historical, 2026-11-03)

Recorder source `41d1839` started at 09:39:18 UTC on GreenCloud Tokyo. **M4.1 passed at 09:50:41.299712099 UTC** with PID 33345, invocation `b9cf14589aca449996a4aa32c5b81576`, and zero restarts. The baseline freezes all 4,038 active sources, 50 synchronized depth books, and a pending queue of zero. The external observer writes to `/srv/recorder-data/recorder-archive/m4-41d1839-20261103/`. Checkpoints fall at 11:50:41 UTC on November 3 and 21:50:41 UTC on November 3 and 09:50:41 UTC on November 4. Production readiness is pending.

## M4.2 checkpoint (first run, historical, 2026-11-03)

The run that began qualification at 05:18:17 UTC failed the 2h per-source availability gate. 411 of 4,038 sources exceeded 36 seconds of unavailability or uncertainty. The Recorder stopped cleanly and is inactive and disabled. Normal OS updates were restored. The 12h and 24h targets for that run are cancelled. The earlier M4.1 baseline below is retained as historical evidence. See the [M4.2 result and repair notes](production_milestone_plan.md#任务-m42第一阶段检查点t0--2h-稳定性与零积压门禁).

## M4.1 baseline (historical, 2026-11-03)

- **Production Readiness**: **NO** (currently progressing through the finalized milestone plan `M1` → `M2` → `M3` → `M4` in [`production_milestone_plan.md`](production_milestone_plan.md); M1–M3 are complete; M4.1 is complete after independent correction; timed M4.2–M4.4 qualification is pending).
- **Previous P0–P6 Plan**: Abolished and superseded by `M1`–`M4`.
- **Milestone Progress**:
  - `M1.0` (Pre-M1 Baseline Pipeline Benchmark): **COMPLETED after independent correction** (2026-11-02; [review](benchmarks/m1.0-review-20261102.md)). Corrected v2 tool and reports verify two identical inputs and 13,966 decoded Raw frames per run (content digest `cb65e4c7...`). At the M1.0 freeze, production `src/` matched `f109ffc`; the 12s synthetic baseline includes startup/shutdown and makes no steady-state rotation/backlog claim. Original v1 reports are retained under `docs/benchmarks/archive/`.
  - `M1.1` (15m / 128 MiB rotation): **COMPLETED after independent correction** ([review](benchmarks/m1.1-review-20261102.md)).
  - `M1.2` (Catalog WAL+NORMAL and shared availability watermark): **COMPLETED after independent correction** ([review](benchmarks/m1.2-review-20261102.md)); 4,660-source offline SQL validation and actual WAL tail truncation recovery. No new VPS deployment or host-state observation in this review.
  - `M1.3` (closed-bar K-line persistence, REST gap backfill, S12 deduplication and seed retirement): **COMPLETED after independent correction** ([review](benchmarks/m1.3-review-20261102.md)).
  - `M1.4` (direct envelope framing and batched REST persistence): **COMPLETED after independent correction** ([review](benchmarks/m1.4-review-20261102.md)); removed residual Writer dictionary copies and fixed unobserved Future exceptions on REST drain failures.
  - `M2.1` (remove live post-seal full-file readback and background re-verification): **COMPLETED after independent correction** ([review](benchmarks/m2.1-review-20261102.md)); removed live verification toggles and seal compatibility fallbacks, fixed compressed short writes, and exercised actual rotation/stop and manual corruption detection.
  - `M2.2` (remove startup empty chunks): **COMPLETED after independent correction** ([review](benchmarks/m2.2-review-20261102.md)); startup emits no empty Raw/boundary files, first real stream chunks carry the manifest marker. Fixed startup evidence being masked by an ambiguous old-frame gap; verified legacy/new Raw normalization and replay together.
  - `M2.3` / `M2.4`: **COMPLETED after independent correction**; unified collection and deferred peripheral imports, with the Catalog split by domain.
  - `M3.1` / `M3.2`: **COMPLETED after independent correction**; actual three-way 45-minute runs, full offline/stress/recovery checks.
  - `M3.3`: **COMPLETED after independent correction** ([review](benchmarks/m3.3-review-20261103.md)); source-matched Wheel retained, fresh-venv CLI checks pass, 32 MiB production frame limit restored, observer verified against actual Raw/Catalog.
  - `M4.1`: **COMPLETED after independent correction** ([review](benchmarks/m4.1-review-20261103.md)). Withdrew the original premature PASS, fixed empty worker jobs, monitor starvation and repeated future-snapshot replacement; all prior Raw and failure history retained.
  - Next task: **M4.2**, same-process 2h checkpoint at **2026-11-03 07:18:17 UTC (15:18:17 Asia/Shanghai)**. No 2h/12h/24h qualification has passed yet.
- **GreenCloud VPS state observed 2026-11-03**:
  - Unit `binance-market-data-recorder.service`: **active / enabled** for the authorized qualification run; PID **17133**, InvocationID **9384c31ecf5944ffb6da398e9bde673c**, NRestarts **0**.
  - Installed source **3ff043f8d0ed5a790369197a47d3efeb219c94d9**, Wheel SHA-256 **e8cb5caee2f7e4a81fa9beabd476da3844ddae5b75adc79a1fa126cdbc33a05a**. Deployment identity VERIFIED; exact release under `/opt/binance-market-data-recorder/m41-3ff043f-20261103/`.
  - Service started **05:04:09 UTC**. Formal **T0 = 05:18:17.209919 UTC**, frozen after AVAILABLE initialization; warm-up is retained and excluded from timed qualification. [Baseline](benchmarks/m4.1-baseline-20261103.json) binds all 4,038 active source IDs, products, policy omissions, process/boot identity and checkpoint targets.
  - At T0: **50/50 snapshots and depth sequence bridges**, 1,844 connected WS sources, 2,194 valid REST sources, **pending=0**, no open active source unavailability. Legacy ready=45/50: five quiet products lack only their first agg_trade; no missing depth is excused.
  - External observer `bmdr-m4-observer-3ff043f-20261103.service` runs as bmdr with a 26h limit. Evidence: `/srv/recorder-data/recorder-archive/m4-3ff043f-20261103/`; sampling began before service start and continues after this review.
  - Five apt/unattended-update units are **runtime-masked / inactive** for the qualification quiet window. Restore normal updates on controlled exit; at final controlled stop record **inactive / disabled** as the plan requires. Do not interpret the present enabled state as Production Ready.
  - Rejected runs are retained under `run1-overflow-preserved-20261103/`, `m41-rejected-20261103T033044Z/`, `m41-rejected-6830f70-20261103T043743Z/`, and `m41-rejected-d33c4e3-20261103T044705Z/` below `/var/lib/binance-market-data-recorder/`. No unarchived Raw was deleted.

## Summary of Retained Baseline & Diagnostic Findings (P0–P3 & P4-prep)

1. **P4 Diagnostic Runs (2026-11-01, [`milestones/P4/README.md`](milestones/P4/README.md))**:
   - **Run A (`f622a4c`)**: Exposed a 17.8 MiB Spot directory payload exceeding the default 16 MiB frame limit, and USD-M dated public ADL (`BTCUSDT_261225`, `BTCUSDT_270327`) returning HTTP `400 / -1121`. Owner approved initial-release omission of dated public ADL with `UNKNOWN` official applicability ([`ADR-0057`](adr/0057-initial-production-dated-adl-exception.md)).
   - **Run B (`f622a4c`)**: Exposed `Catalog` single-lock starvation when draining `AvailabilityJournal` in unbounded transactions.
   - **Run C (`8bdae20`)**: Ran ~55 minutes and was stopped cleanly when `AvailabilityJournal` pending rows reached `52,997` (because 2,233 healthy sources emitted checkpoints every 10s while `8bdae20` drained one `128`-row batch per pass; `2a4fefd` later raised the drain cap to `32` batches or `100ms`) and sealed `874` chunks in 55 minutes (`~15.9` chunks/min).
2. **P3 Independent Closure (`30e8a4a` / `30ebbd6`, [`milestones/P3/review-20261101.md`](milestones/P3/review-20261101.md#r3-independent-closure))**:
   - Accepted durable per-source availability intervals, clock-uncertainty boundary guards, and incremental new-file verification (`raw-independent.v2`).
3. **P2 Independent Closure (`1c1785c` / `464ed56`, [`milestones/P2/review-20261029.md`](milestones/P2/review-20261029.md#r5-independent-closure))**:
   - Accepted Spot/USD-M/COIN-M product discovery, single REST representations ([`ADR-0053`](adr/0053-single-rest-representation-and-required-information.md)), measured D06 live polling + daily trade supplement ([`ADR-0054`](adr/0054-d06-fixed-cadence-and-daily-trade-supplement.md)), synchronous `Retry-After` gate, and owner-approved omission of dated premium-index klines ([`ADR-0055`](adr/0055-initial-production-dated-premium-exception.md)).
4. **P0 & P1 Baseline ([`milestones/P0/README.md`](milestones/P0/README.md), [`milestones/P1/README.md`](milestones/P1/README.md))**:
   - P0 aligned development base `113cdb3` (merging `9e8ec37`/`d81e7d2` and `01d7421`) and recorded isolated VPS baseline against installed `20e57f3`.
   - P1 delivered bounded vectored raw writes, long-lived receive loops, and sequence-only capture health ([`ADR-0052`](adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md)) at `b292d0d`.

Historical checkpoints prior to P0 are condensed in [`CURRENT_PRODUCTION_STATE_HISTORY.md`](CURRENT_PRODUCTION_STATE_HISTORY.md) and [`history/pre-P0-CURRENT_PRODUCTION_STATE.md`](history/pre-P0-CURRENT_PRODUCTION_STATE.md).
