# Project Handoff

## Active Engineering Direction (2026-11-02)

The previous P0–P6 milestone plan has been **abolished** and replaced by the finalized Python milestone plan (`M1` → `M2` → `M3` → `M4`) in [`production_milestone_plan.md`](production_milestone_plan.md):

1. **M1 — Primary Performance, I/O & Source Amplification Remediation**:
   - **M1.0**: Freeze a deterministic multi-stream replay baseline fixture on current `main` (`f109ffc`) before changing runtime code (**COMPLETED**; report saved in `docs/benchmarks/baseline-main.json` and `baseline-main.json`, verified 100% repeatability with `95f2fc2c...` digest).
   - **M1.1**: Change default spool rotation policy from `60s` to `600s`–`900s` (with `128 MiB` size cap, per-stream phase staggering, and periodic `fsync`). (NEXT TASK)
   - **M1.2**: Coalesce unsaved healthy `AvailabilityJournal` checkpoints per source and replace 10s per-source SQLite row churn with per-source transitions + a shared monitor watermark. Switch routine `Catalog` WAL commits to `PRAGMA synchronous = NORMAL` while enforcing a `FULL` / `wal_checkpoint(TRUNCATE)` barrier before irreversible local source deletion (`LOCAL_DELETED`), plus startup WAL rollback reconciliation (`ADR-0059`).
   - **M1.3**: Retire completed one-shot REST seeds (`kline_seed_*`, `continuous_seed_*`, `reference_price_seed`) from active polling/monitoring loops; verify `S11` (UTC) vs `S12` (`@+08:00`) closed-bar equivalence across the 10 sub-8h intervals (`1s`–`4h`, `8h`) and disable duplicates; persist closed WS K-line bars (`k.x == True`) with in-memory liveness updates, `forming_filtered_count` accounting, and automatic REST backfill (`closed_bar_backfill`) on missed close boundaries (`ADR-0058`).
   - **M1.4**: Streamline hot-path envelope construction (single immutable internal frame record shared by CBOR encoding and chunk stats, preserving `source_sequence` immutability and Raw v1 byte format) and eliminate REST `D16` / `418` / `429` payload re-parsing and double thread hops.
2. **M2 — De-Overengineering, Verification Consolidation & Codebase Decoupling**:
   - **M2.1**: Consolidate seal-time readback and semantic/frame verification into a single post-`fsync` `read_artifact(..., scan_frames=True)` pass bound to content SHA-256 and durable control state.
   - **M2.2**: Replace 80+ startup 0-record `.zst` chunks (`seal_capture_startup`) with `process_boundary_before_first_frame` manifest flags + one archivable session-boundary manifest per process incarnation.
   - **M2.3**: Extract a market-agnostic `PublicWebSocketStream` transport, map legacy `spot_symbols` / `usdm_symbols` configs onto the unified capture supervisor, and retire the duplicate legacy collector path after equivalence verification.
   - **M2.4**: Make `cli.py` imports command-scoped/lazy (isolating `pyarrow`, retired V1–V5 acceptance tools, `storage/macos/`, and `archive/remote_*.py` from the capture daemon) and split `storage/catalog.py` into domain modules.
3. **M3 — Controlled Three-Way Benchmark & Pre-VPS Convergence**:
   - Compare (1) Baseline `main`, (2) Optimized Same-Scope, and (3) Optimized Production-Scope over multi-rotation (`>= 45–60m`) runs, plus the 1M-event stress test and WAL rollback / K-line backfill regressions.
4. **M4 — GreenCloud VPS Continuous Qualification (`BTC + ETH`: `2h` → `12h` → `24h`)**:
   - Run `[recorder.coverage].underlying_assets = ["BTC", "ETH"]` on `greencloud-tokyo-01` in one continuous service incarnation across `2h`, `12h`, and `24h` checkpoints (any checkpoint failure requires fixing and restarting from `0h`).

---

## Retained Baseline Context (P0–P3 & P4 Diagnostic Summary)

- **P0 (`113cdb3` / `43aab9f`)**: Merged local workspace and remote `main`; established isolated VPS baseline ([`milestones/P0/README.md`](milestones/P0/README.md)).
- **P1 (`b292d0d`)**: Implemented bounded vectored append, single long-lived receive task per WebSocket connection, and sequence-only capture health under [`ADR-0052`](adr/0052-bounded-raw-batches-and-sequence-only-capture-health.md) ([`milestones/P1/README.md`](milestones/P1/README.md)).
- **P2 (`1c1785c` / `464ed56`)**: Implemented multi-asset Spot/USD-M/COIN-M discovery, single REST representations ([`ADR-0053`](adr/0053-single-rest-representation-and-required-information.md)), measured D06 cadences (`2s` USDT / `3s` USDC / `30s` COIN-M) + daily trade supplement ([`ADR-0054`](adr/0054-d06-fixed-cadence-and-daily-trade-supplement.md)), synchronous `Retry-After` gate, and initial-release omission of dated premium-index klines ([`ADR-0055`](adr/0055-initial-production-dated-premium-exception.md), [issue #585](https://github.com/binance/binance-connector-python/issues/585)) ([`milestones/P2/README.md`](milestones/P2/README.md)).
- **P3 (`30e8a4a` / `30ebbd6`)**: Implemented per-source availability tracking, clock-discontinuity guards, and incremental new-file verification under [`ADR-0051`](adr/0051-production-continuity-and-incremental-verification.md) and [`ADR-0056`](adr/0056-source-recovery-and-bounded-background-verification.md) ([`milestones/P3/README.md`](milestones/P3/README.md)).
- **P4 Diagnostics (`8bdae20` / `2a4fefd`)**: Exposed the `AvailabilityJournal` 52,997-row backlog (at `128` rows/pass in `8bdae20`, raised to `32` batches / `100ms` in `2a4fefd`), `874` sealed files in 55 minutes (`~15.9` files/min), and USD-M dated ADL `400/-1121` omission ([`ADR-0057`](adr/0057-initial-production-dated-adl-exception.md), [`milestones/P4/README.md`](milestones/P4/README.md)).

Use [`developer_guide.md`](developer_guide.md) for codebase orientation and [`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md) for current host status. Earlier handoffs are condensed in [`PROJECT_HANDOFF_HISTORY.md`](PROJECT_HANDOFF_HISTORY.md) and [`history/pre-P0-PROJECT_HANDOFF.md`](history/pre-P0-PROJECT_HANDOFF.md).
