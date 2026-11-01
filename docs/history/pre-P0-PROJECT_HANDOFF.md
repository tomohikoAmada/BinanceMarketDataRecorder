# Retained Pre-P0 Project Handoff History (Condensed)

> Historical engineering handoffs from October 1–26, 2026 (prior to `P0`). For the current engineering handoff, see [`../PROJECT_HANDOFF.md`](../PROJECT_HANDOFF.md).

## Summary of Pre-P0 Handoff Lineage

1. **Q1 – Q8 Qualification & Optimization Series (`ADR-0036` – `ADR-0041`)**:
   - Resolved October 1 architecture review items (`F1`–`F7`) and Spot idle snapshot worker cleanup (`R-079`, `23ea5c4`).
   - Demonstrated that whole-corpus LIVE + terminal Raw audits (`R-078`, `50m05s` for `6,718` chunks in Q4; >14h for `143,362` chunks in V5) do not scale to multi-day multi-asset capture.

2. **R1 – R2 Multi-Asset & Continuity Series (`ADR-0042` – `ADR-0050`)**:
   - Investigated `99.5%` per-source continuity policies, shared REST rate-limit gates, stream-close diagnostics, and warm-start readiness classification across `N=1` (`BTC`) and `N=2` (`BTC` + `ETH`) configurations.
   - Concluded that sustainable multi-asset production requires:
     1. Incremental per-file verification rather than whole-history re-audits (`ADR-0051`);
     2. Bounded vectored raw writes and sequence-only capture health rather than per-frame full L2 orderbook reconstruction on the capture path (`ADR-0052`);
     3. Single primary REST representations (`ADR-0053`).
