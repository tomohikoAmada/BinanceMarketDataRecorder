# Risk Register

## Current Active Risks (November 2026)

New engineering and qualification work follows the streamlined milestone plan (`M1` → `M2` → `M3` → `M4`) in [`production_milestone_plan.md`](production_milestone_plan.md).

| Risk / Finding | Current Control & Evidence | Remaining Work (M1–M4) |
|---|---|---|
| **Spool 60s rotation & small-file IOPS storm** | P4 Run C produced 874 sealed `.zst` files in 55 minutes (projected 115,200 `.zst` + 115,200 `.json` manifests/day for BTC alone), amplifying SQLite transactions, Zstd readbacks, and archive transfers. | **M1**: Increase default spool rotation interval to `600s`–`900s` (or `128 MiB` size threshold) to cut file count and seal IOPS by >90%. |
| **`AvailabilityJournal` & SQLite write amplification** | P4 Run C stopped cleanly after `AvailabilityJournal` queued 52,997 pending rows (2,233 healthy sources × 1 row/10s) against `Catalog` running `PRAGMA synchronous=FULL` under a single `RLock`. | **M1**: Switch WAL `Catalog` to `PRAGMA synchronous=NORMAL`; record availability rows only on state transitions (`unavailable` / `recovery` / `failure` / `lifecycle`) + single process heartbeat. |
| **Source explosion from duplicate K-lines & zombie seed sources** | 1,845 of 2,233 BTC sources are K-lines; `S12` (`@+08:00`) duplicates 10 sub-8h intervals identical to UTC (`S11`), and 615 one-shot `kline_seed_*` REST sources remain in active per-second loops after completion. | **M1**: Remove the 10 UTC-identical intervals from `S12` (saving 300 sources) and retire completed one-shot seed sources from active scheduling loops. |
| **Hot-path CPU & per-frame CBOR overhead** | Strict Pydantic re-validation, `_ImmutableEnvelopeSnapshot` deep copies, and repeating 17 string keys in every frame's CBOR map add ~582 B/frame and ~12–16 μs/frame CPU overhead. | **M1 / M2**: Replace hot-path Pydantic copies/validation with lightweight structures and eliminate double JSON parsing in REST persistence. |
| **Redundant multi-pass verification & hashing (R-078)** | Same-host seal path compresses, reads back, decompresses, and hashes, then spawns `verification_worker` subprocess to decompress and CBOR-re-encode every frame again (`raw-independent.v2`). | **M2**: Consolidate local verification into a single seal-time verification pass + archive transfer readback verification; eliminate redundant same-host re-decompression and startup 0-frame `.zst` marker storms. |
| **USD-M / COIN-M dated public endpoint applicability (`ADR-0055`, `ADR-0057`)** | Listed USD-M/COIN-M dated symbols return HTTP `400 / -1121` on `premiumIndexKlines` and USD-M dated public `adlQuantile`. Perpetual controls succeed. Public inquiry submitted as [binance/binance-connector-python#585](https://github.com/binance/binance-connector-python/issues/585). | **Mitigated / Disclosed**: Owner approved prospective initial-release omission of dated premium-index klines ([`ADR-0055`](adr/0055-initial-production-dated-premium-exception.md)) and dated public ADL ([`ADR-0057`](adr/0057-initial-production-dated-adl-exception.md)) with `UNKNOWN` official applicability. |
| **D06 `historicalTrades` REST weight budget (`ADR-0054`)** | 1s polling across 19 derivative products would require ~5,700 weight/min (above the 2,400/min exchange pool). Measured cadences (`2s` USDT / `3s` USDC / `30s` COIN-M, ~765 weight/min) + daily trade file supplement implemented and accepted in P2. | **Closed in P2** ([`milestones/P2/README.md`](milestones/P2/README.md)); verify long-run stability in **M4**. |
| **Shared REST `Retry-After` gate ordering (`P2-R4`)** | Synchronous cooldown gate before persistence await verified across 33 cooldown regressions and malformation fallbacks (`f9aaeee`). | **Closed in P2** ([`milestones/P2/review-20261028.md`](milestones/P2/review-20261028.md#review-f9aaeee)). |
| **D11 liquidation stream semantics & Spot bootstrap wording (`R-034`)** | D11 discloses U/C reference vs changelog conflict without fabricating totals; Spot bootstrap retains `lastUpdateId + 1` under `ADR-0011`. | **Monitoring**: Preserve exact Raw payloads and disclosed boundaries. |
| **Shared-VPS long-run capacity & 2h/12h/24h qualification (`R-060`, `R-078`)** | P0–P3 local suites and short P4 runs do not establish 24h production qualification on `greencloud-tokyo-01`. | **M4**: Execute continuous `2h` → `12h` → `24h` qualification on the VPS after completing `M1`–`M3`. |

---

## Retained Historical Findings Summary (October 2026)

- **P3 Closure (`30e8a4a` / `30ebbd6`)**: Closed P3-R1 (durable manifest-only gap flags in verification), P3-R2 (sparse-stream idle time vs writer stall), and P3-R3 (RPI fast-recovery and clock-discontinuity boundary preservation). See [`milestones/P3/review-20261101.md`](milestones/P3/review-20261101.md).
- **P1 Closure (`b292d0d`)**: Closed P1-R1 (receive-loop retirement check after scheduling yield) and validated sequence-only capture health ([`milestones/P1/review-20261027.md`](milestones/P1/review-20261027.md)).
- **P0 Closure (`43aab9f`)**: Closed P0-R1/R3/R4 and R2 peak-retaining steady-state classifier ([`milestones/P0/review-20261026.md`](milestones/P0/review-20261026.md)).
- **Pre-P0 Architecture & Q1–Q4 Findings (`F1`–`F7`, `R-076`–`R-079`)**:
  - `F1` (`ADR-0037` shared v2 representation), `F2` (failed heartbeat collector drain), `F3` (`ADR-0036` normalized key product/stream namespacing), `F4` (normalization merge fan-in), `F5` (bounded reconstructor audit memory), `F6` (resilient cwd discovery), `F7` (auxiliary `FAILED` reflected in `DEGRADED` aggregate health), and `R-079` (Spot snapshot idle worker cleanup at `23ea5c4`) were implemented and verified offline.
  - Note on `Alpha101Crypto`: Recorder remains strictly independent; `Alpha101Crypto` is only an optional external consumer example with no influence on Recorder contracts.

Full risk ID definitions (`R-001` through `R-078`) and earlier incident history are preserved in [`risk_register_history.md`](risk_register_history.md).
