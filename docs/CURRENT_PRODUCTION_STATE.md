# Current Production State

## Active Status (2026-11-02)

- **Production Readiness**: **NO** (currently progressing through the finalized milestone plan `M1` → `M2` → `M3` → `M4` in [`production_milestone_plan.md`](production_milestone_plan.md); `M1.0` completed, baseline established in [`docs/benchmarks/baseline-main.json`](benchmarks/baseline-main.json)).
- **Previous P0–P6 Plan**: Abolished and superseded by `M1`–`M4`.
- **Milestone Progress**:
  - `M1.0` (Pre-M1 Baseline Pipeline Benchmark): **COMPLETED after independent correction** (2026-11-02; [review](benchmarks/m1.0-review-20261102.md)). Corrected v2 tool and reports verify two identical inputs and 13,966 decoded Raw frames per run (content digest `cb65e4c7...`). Production `src/` remains unchanged from `f109ffc`; the 12s synthetic baseline includes startup/shutdown and makes no steady-state rotation/backlog claim. Original v1 reports are retained under `docs/benchmarks/archive/`.
  - `M1.1` (15m / 128 MiB rotation): **COMPLETED after independent correction** ([review](benchmarks/m1.1-review-20261102.md)).
  - `M1.2` (Catalog WAL+NORMAL and shared availability watermark): **COMPLETED after independent correction** ([review](benchmarks/m1.2-review-20261102.md)); 4,660-source offline SQL validation and actual WAL tail truncation recovery. No new VPS deployment or host-state observation in this review.
  - Next task: `M1.3` (closed-bar K-line persistence, REST gap backfill, S12 deduplication and seed retirement).
- **GreenCloud VPS (`greencloud-tokyo-01`) State**:
  - Primary service `binance-market-data-recorder.service` remains `inactive` and `disabled` (`PID 0`, `NRestarts 0`).
  - Temporary P4 diagnostic units (`p4-btc-recorder.service` and timers) were stopped cleanly after diagnostic runs; all 45, 337, and 874 sealed chunks across Runs A, B, and C passed independent `raw-independent.v2` verification with zero corruption.

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
