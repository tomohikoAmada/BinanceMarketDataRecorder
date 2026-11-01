# Retained Pre-P0 Agent Operational History (Condensed)

> Historical operational instructions prior to October 26, 2026 (`P0`). These entries are retained for historical audit context only and do not govern active engineering. See [`../../AGENTS.md`](../../AGENTS.md) and [`../production_milestone_plan.md`](../production_milestone_plan.md) for current instructions.

## Key Pre-P0 Milestones & Operational Checkpoints (October 2026)

1. **Q1 – Q5 (`c87d580`, `605bc16`, `23ea5c4`)**:
   - **Q1 / Q2**: Addressed October 1 architecture review findings `F1`–`F7` (`ADR-0036` normalized key product/stream namespacing, `ADR-0037` shared v2 lifecycle representation, bounded reconstructor audit memory, failed-heartbeat collector stop, resilient cwd discovery, auxiliary `FAILED` health propagation).
   - **Q3**: Verified typed-empty observation recovery (`605bc16`) and initial sparse WebSocket startup boundary (`ADR-0038`).
   - **Q4 (`23ea5c4`)**: Fixed Spot snapshot idle worker cleanup (`R-079`) and completed a reviewed 2h run (`6,718` chunks archived, full LIVE + terminal verification `50m05.516s`).
   - **Q5 / Q6 – Q8 (`01d7421`)**: Executed optimization and 2h/12h recovery runs under `ADR-0039`–`ADR-0041`.

2. **R1 – R2 Multi-Symbol Capacity & Warm-Reader Runs (`20e57f3` / `9e8ec37`)**:
   - **R1 (`ADR-0042`–`ADR-0044`)**: Verified stop-aware auxiliary drain and sparse open-interest endpoint retry handling across 2h and 12h runs.
   - **R2 (`ADR-0045`–`ADR-0050`)**: Evaluated `N=1` (`BTC`) and `N=2` (`BTC` + `ETH`) 24h runs on `greencloud-tokyo-01`, identifying monitor budget contention, stream-close diagnostics, and warm-start reader/successor ordering boundaries.
   - Installed VPS source at the end of pre-P0 work was `20e57f3` (with local uncommitted/branch state `9e8ec37` / `d81e7d2` merged with remote `01d7421` into `113cdb3` during `P0`).
