# Project Handoff

## Active Engineering Direction (November 2026)

The previous P0–P6 milestone plan has been **abolished** and replaced by the new streamlined Python milestone plan in [`production_milestone_plan.md`](production_milestone_plan.md):

1. **M1 — Primary Performance & Bottleneck Remediation**:
   - Change spool rotation policy from `60s` to `600s`–`900s` (or `128 MiB`) to eliminate the small-file seal/verify/archive storm.
   - Switch SQLite `Catalog` in WAL mode to `PRAGMA synchronous=NORMAL` and refactor `AvailabilityJournal` to write only on state transitions (`unavailable` / `recovery` / `failure` / `lifecycle`) plus a single process-level heartbeat row instead of per-source rows every 10s.
   - Deduplicate `S12` (`@kline_<interval>@+08:00`) by removing the 10 sub-8h intervals that are identical to UTC (`S11`), persist only closed K-line bars (`k.x == True`, while updating in-memory liveness on all pushes), and retire completed one-shot `kline_seed_*` REST sources from active per-second polling loops.
   - Streamline the per-frame hot path (remove redundant Pydantic validation, `_ImmutableEnvelopeSnapshot` deep copies, and double JSON parsing on REST persistence) while preserving **100% Raw v1 on-disk compatibility**.
2. **M2 — De-Overengineering, Verification Simplification & Codebase Consolidation**:
   - Simplify the multi-pass verification chain (retain write-time CRC32C, seal-time Zstd readback & SHA-256, and archive transfer verification; remove same-host redundant subprocess decompression/re-encoding passes and startup zero-frame `.zst` boundary file storms).
   - Remove legacy single-symbol collectors (`SpotCollector`, `UsdMCollector`, `UsdMSideDataManager`) and retired acceptance/audit bloat, unifying around a clean collector architecture and modularizing `Catalog`.
3. **M3 — Local Benchmark & Pre-VPS Convergence**:
   - Validate core regression tests, contract checks, and fixed-input A/B benchmarks (confirming reductions in CPU/frame, SQLite transactions, and file/IOPS rates while preserving Raw v1 data integrity).
4. **M4 — GreenCloud VPS Staged Qualification (`BTC + ETH` Full Scope: `2h` → `12h` → `24h`)**:
   - Run the streamlined Python build directly with `BTC + ETH` full scope on `greencloud-tokyo-01` through continuous **2h**, **12h**, and **24h** checkpoints with normal archive and verification enabled.

---

## Retained Baseline Context (P0–P3 & P4 Diagnostic Summary)

- **P0 (`113cdb3` / `43aab9f`)**: Merged local development branch and remote main; established isolated VPS benchmark baseline ([`milestones/P0/README.md`](milestones/P0/README.md)).
- **P1 (`b292d0d`)**: Implemented bounded vectored append, single long-lived receive task per WebSocket connection, and sequence-only capture health under [`ADR-0052`](adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md) ([`milestones/P1/README.md`](milestones/P1/README.md)).
- **P2 (`1c1785c` / `464ed56`)**: Implemented multi-asset Spot/USD-M/COIN-M discovery, single REST representations ([`ADR-0053`](adr/0053-single-rest-representation-and-required-information.md)), measured D06 cadences (`2s` USDT / `3s` USDC / `30s` COIN-M) + daily trade supplement ([`ADR-0054`](adr/0054-d06-fixed-cadence-and-daily-trade-supplement.md)), synchronous `Retry-After` gate, and initial-release omission of dated premium-index klines ([`ADR-0055`](adr/0055-initial-production-dated-premium-exception.md), [issue #585](https://github.com/binance/binance-connector-python/issues/585)) ([`milestones/P2/README.md`](milestones/P2/README.md)).
- **P3 (`30e8a4a` / `30ebbd6`)**: Implemented per-source availability tracking, clock-discontinuity guards, and incremental new-file verification under [`ADR-0051`](adr/0051-production-continuity-and-incremental-verification.md) and [`ADR-0056`](adr/0056-source-recovery-and-bounded-background-verification.md) ([`milestones/P3/README.md`](milestones/P3/README.md)).
- **P4 Diagnostics (`8bdae20`)**: Identified the three concrete VPS bottlenecks addressed in `M1`: `AvailabilityJournal` 52,997-row backlog, 60s spool rotation file storm, and USD-M dated ADL `400/-1121` omission ([`ADR-0057`](adr/0057-initial-production-dated-adl-exception.md), [`milestones/P4/README.md`](milestones/P4/README.md)).

 Use [`developer_guide.md`](developer_guide.md) for codebase orientation and [`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md) for current host status. Earlier handoffs are condensed in [`PROJECT_HANDOFF_HISTORY.md`](PROJECT_HANDOFF_HISTORY.md) and [`history/pre-P0-PROJECT_HANDOFF.md`](history/pre-P0-PROJECT_HANDOFF.md).
