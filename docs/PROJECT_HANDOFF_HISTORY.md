# Project Handoff History (Condensed)

> Historical handoff snapshots through October 1, 2026. Statements below are historical records of prior development phases. For the active engineering handoff, see [`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md).

## Summary of Historical Development Phases

1. **M0 – M20: Core Recorder & Dual-Platform Foundation (July – August 2026)**
   - Established independent repository identity (`ADR-0001`, `ADR-0007`), framed Raw v1 format (`ADR-0010`: 32B magic header + length-prefixed canonical CBOR + CRC32C, sealed to `.zst` + JSON manifest + SQLite `Catalog`), caller-owned `websockets` capture (`ADR-0009`), Spot (`lastUpdateId + 1`, `ADR-0011`) and USD-M (`U/u/pu`) sequence continuity, external storage UUID/marker discovery (`ADR-0014`), transactional archive copy/verify/delete (`ADR-0015`), disk emergency reserve stop (`ADR-0016`), Parquet normalization (`ADR-0020`), deterministic replay (`ADR-0021`), and M20 proxy/Linux systemd deployment (`ADR-0025`, `ADR-0026`).

2. **M21 – M23 & MS1 – MS4: Multi-Symbol Generalization & Remote Archive (August – September 2026)**
   - **M21.4 / M21.4.11**: Added bounded WebSocket queue backpressure (`R-004`) and durable `STARTED` → forced `reconnect_gap` seal → `COMPLETED` reconnect-boundary tracking (`ADR-0027`, `R-047`).
   - **M22.0 – M22.8**: Implemented media-local Archive Set layout (`ADR-0030`), receipt-bound remote source deletion (`ADR-0029`, `R-048`), and online WAL `sqlite3.Connection.backup()` (`R-050`).
   - **M23.4**: Added incremental write-time seal evidence (`_ImmutableEnvelopeSnapshot`, `R-058`) so clean seals avoid re-scanning uncompressed `.partial` files before Zstd compression.
   - **MS1 – MS4 (`ADR-0032`)**: Generalized runtime from hardcoded `BTCUSDT` to configurable `ProductKey(market, symbol)` instances sharing one Spot REST limiter and one USD-M `UsdMRestCooldown` / `run_owned_blocking_call` gate (`R-065`, `R-066`).

3. **M22.9 V1 – V5: Online Qualification & Full-History Audit Bottleneck (September – October 2026)**
   - **V1 – V4 (`ADR-0033`)**: Exposed three operational issues on the shared GreenCloud VPS (`greencloud-tokyo-01`):
     1. Concurrent archive `LOCAL_DELETED` vs observer scan race (`R-070`, fixed via frozen lifecycle boundary in `31cabe44`);
     2. Unattended `apt-daily-upgrade` restarting systemd units mid-run (`R-071`, mitigated via runtime-only quiet-window unit masks);
     3. Unbounded `O(N)` manifest history scans causing observer memory/cadence failure (`R-074`, ~4.14 GB RSS at 112k manifests).
   - **V5 (`ADR-0034`, `89bc6b41c0cb7d270ca6e4d2dec9a375137c028e`)**: Bounded online sample memory (`R-076`), but still required a whole-history terminal Raw/archive audit (`R-077`/`R-078`). On 2026-10-01, the owner stopped the V5 baseline after >14 hours (`87,423` of `143,362` manifests audited), motivating the later transition in `ADR-0051` from whole-history re-audits to incremental per-file verification.
