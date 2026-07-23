# Requirements Traceability

This matrix maps every requirement family from the M0 charter to an
authoritative contract and delivery/verification milestone. Detailed acceptance
steps are in `milestone_plan.md`; a row never weakens those steps.

| ID | Requirement family | Contract / decision | Delivery and proof |
| --- | --- | --- | --- |
| WF-01 | Exactly one milestone/run/commit; read prerequisites/status; report and stop | `AGENTS.md`, milestone universal gate | Every M0-M18; acceptance record per milestone |
| WF-02 | Stop with evidence on official API/platform/permission semantic blocker; no fake substitute | `AGENTS.md`, risks R-002/R-020/R-022 | M2/M9/M12 and any affected milestone |
| WF-03 | No real trading, account API, API key or credential reads | project/security contract, ADR-0005 | Static/config tests M1; transport tests M2+ |
| WF-04 | No format/repair of external disk; no GUI/web/trading/factor/backtest in Recorder | project/storage/macOS contracts | Boundary tests/reviews every milestone |
| IDN-01 | Binance-scoped display/repository/distribution/import/CLI/application-data identity and final workspace | ADR-0007, `AGENTS.md` | M0.2 search/path/history tests; M1 packaging |
| IDN-02 | M0/M0.1 identities and paths only in classified history; both commits preserved | ADR-0006/0007, M0.1/M0.2 acceptance | M0.2 Git object and `rg` checks |
| IDN-03 | Independent unofficial project; no affiliation/sponsorship/endorsement/logo or Binance-owned-looking service namespace | ADR-0007, project contract | M0.2 disclaimer/forbidden-string tests; M14/M18 release checks |
| BND-01 | Recorder independent of all consumers; one-way generic contracts | ADR-0001/0007, architecture | M0.2; generic consumer proof M16 |
| BND-02 | Alpha101Crypto remains an untouched historical audit object and optional ordinary consumer only | project contract, repository audit | M0-M0.2 and optional M16 read-only review |
| BND-03 | Recorder is Binance-specific with separate Spot/USD-M modules; no speculative other-exchange framework | ADR-0007, architecture/data contract | M0.2 contract check; M2/M4/M5 collectors; M6 market-specific reconstruction tests |
| SRC-01 | Only listed official Binance sources establish behavior and changelogs | `AGENTS.md`, `binance_sources.md` | M0 inventory; M2 updater/ADRs; M6 source refresh; M7 refresh |
| SRC-02 | Select pages from llms.txt; no default llms-full; allowlisted hosts; hashes/time; no remote execution | source contract, ADR-0005 | M2 unit/security tests |
| SRC-03 | Evaluate modular Spot/USD-M SDK; ban deprecated/unofficial core SDK/MCP | ADR-0005, source inventory | M2 dependency/probe tests |
| SRC-04 | SDK WebSocket use requires raw/timing/lifecycle/IDs/fault/backpressure proof | ADR-0005, R-001 | M2 transport ADR/probes |
| DAT-01 | Spot BTCUSDT depth 100 ms, aggTrade, bookTicker, REST snapshot | project/data contracts | M4 fixtures/mock/15-min smoke |
| DAT-02 | USD-M BTCUSDT same core streams/snapshot with independent failure | project/data contracts | M5 fixtures/mock/30-min combined smoke |
| DAT-03 | Side data: mark/index/premium/funding/OI/liquidation/exchange filters; cannot block L2 | project/data contracts, ADR-0012 | M7 official fixtures, rate provenance, no-fill and failure-isolation tests |
| DAT-04 | Preserve exact payload and exchange/receive clocks, identity/version/sequence provenance | data contract, ADR-0004 | M3 envelope tests; M4/M5 live evidence |
| DAT-05 | Raw append-only/immutable; duplicates allowed; derived dedup/repartition/rebuild | data contract, ADR-0002 | M3/M15 repeatability and hash tests |
| DAT-06 | Per-file time/count/bytes/schema/version/hash/sequence/gap/resync metadata | data contract | M3 manifest tests; M6 checkpoint gap/resync lineage tests |
| DAT-07 | `.partial`, safe seal, recover/truncate/quarantine without false completeness | ADR-0002, storage contract | M3 kill/corruption/property tests |
| DAT-08 | Format compares NDJSON, NDJSON+Zstd, MessagePack/CBOR/other and chooses language-neutral framed checksummed Zstd | ADR-0002 | M0 decision; M3 golden vectors |
| DAT-09 | Deterministic replay, clock choices, checkpoints, explicit gaps | ADR-0004, data contract | M6 fixed-hash, checkpoint-restore and random-deletion tests; M15/M16 order tests |
| STO-01 | Internal disk always active target; no direct external active writes; external absence normal | storage contract, ADR-0003 | M4/M5/M9 fault tests |
| STO-02 | Default application-support layout; no persistent data in repo/Desktop/Documents/iCloud/tmp | project/architecture/storage contracts | M1 path tests; M14 runtime |
| STO-03 | External folder optional/shared, registration only; no volume ownership/format change | storage/macOS contracts, ADR-0003/0014 | M9 scope-probe tests pass; physical matrix M17 |
| STO-04 | Identity uses UUID/name/fs type/relative folder/marker/storage_id and re-resolves mountpoint | storage/macOS contracts, ADR-0014 | M9 rename/reinsert and mismatch tests pass |
| STO-05 | READY requires writable access and write/fsync/rename/readback; read-only reported | storage contract, ADR-0014 | M9 capability/read-only tests pass; physical media matrix unrun |
| STO-06 | Full required storage state set | storage contract, ADR-0014/0017 | M9/M12 enum contract test passes |
| STO-07 | Archive temp/copy/fsync/readback/size+SHA/rename/manifest/Catalog then delete | storage contract, ADR-0003, ADR-0015 | M10 crash/fault/idempotence matrix passes |
| STO-08 | Never delete active/unverified/unarchived; delete retry; unique-copy warning | storage contract, R-011 | M10 tests/CLI/docs; M18 handbook |
| STO-09 | Only registered directory touched; residual temp cleanup bounded | storage contract, ADR-0015 | M9/M10 filesystem audit tests pass |
| SPC-01 | 40% warning, 15% critical, emergency max(10 GiB,5%) | project/storage contracts, ADR-0016 | M11 exact-boundary and alert-transition tests pass |
| SPC-02 | 1h/6h/24h/7d robust growth and ETAs; insufficient/nonpositive sentinels | storage contract, ADR-0016 | M11 synthetic multi-window/archive-on tests pass |
| SPC-03 | Emergency suspends non-core, prioritizes verified archive, never deletes unarchived; seal/stop/gap | project/storage contracts, ADR-0016 | M11 real-spool emergency integration test passes |
| MET-01 | UTC/market/stream daily input, quality, output and performance metrics | project/data contracts, ADR-0013 | M8 deterministic fixture, Collector reconciliation and midnight tests |
| MET-02 | JSON+CSV reports and Catalog summary; SQLite excludes event corpus | project/data/architecture contracts, ADR-0013 | M8 aggregate-schema, atomic output and deterministic rebuild tests |
| MET-03 | Structured status, CLI status/daily reports, restart continuity/no double count | data contract, ADR-0013 | M8 stable-batch retry, restart and honest-status tests |
| MAC-01 | Disk Arbitration startup/events/mount/eject; PyObjC or proven minimal helper | macOS contract, ADR-0014/0017, R-022 | M9 startup callbacks and M12 non-forced unmount/eject callback bridge pass; physical media matrix M17 |
| MAC-02 | Required storage/archive CLI surface | macOS contract | M9/M11/M12 CLI tests pass |
| MAC-03 | Safe eject blocks work, syncs/closes, handles busy/refusal/forced removal | macOS/storage contracts, ADR-0017 | M12 idle/COPY/VERIFY/refusal/forced-removal/reinsert matrix passes |
| OPS-01 | User LaunchAgent, logged-in default, no root; logging/restart/SIGTERM/lock | macOS contract | M14 reboot/crash/install tests |
| OPS-02 | Sleep risk/marked gaps; scoped optional prevent-sleep; no closed-lid promise | macOS contract, R-017 | M14 sleep/wake; M18 limitations |
| OPS-03 | Blue/green candidate readiness/overlap/dedup/rollback/no planned unmarked gap | architecture contract | M13 scenario tests; M15 dedup |
| OPS-04 | 24-hour proactive rotation reuses blue/green mechanism | architecture/macOS contracts | M4/M5 lifecycle then M13 proof |
| NRM-01 | Raw compression without mutation; versioned Parquet date/hour, lineage, gaps, rerunnable | data contract, ADR-0002 | M15 repeatability/DuckDB/hash tests |
| CON-01 | Receive/exchange replay, range/seek/gap/manifest/dataset version | data contract, ADR-0004 | M16 deterministic tests |
| CON-02 | Generic consumers hide archive location and use no Recorder internals/reverse coupling | ADR-0001/0006, architecture | M16 independent example; optional named-consumer validation |
| FAI-01 | Required network/sequence/process/Catalog/disk/volume/checksum/sleep/deploy fault matrix | risk register and M17 plan | M17 fault report |
| FAI-02 | 72-hour then post-fix 7-day soak; daily volume/resources/reconnect/gap/archive/forecast evidence | M17 plan | M17 complete reports |
| REL-01 | V1 docs/package/test reports/limitations/Ubuntu preparation | M18 plan | M18 release packet |
| REL-02 | V1 exit: continuous Spot/UM, rebuild/gaps/recovery/archive/space/blue-green/launchd/no GUI/key/trading/generic consumer | project contract and M18 plan | M18 acceptance; human decision |
| FUT-01 | Architecture permits Ubuntu adapter/API gateway/UI/more markets/consumer strategies without adding them to V1 | architecture/project contracts | M16 contract; M18 checklist |

## Milestone coverage

The M0, M0.1, M0.2, and M1-M18 sections in `milestone_plan.md` each contain
scope, non-scope, dependencies, acceptance, and rollback. This traceability
matrix should be updated whenever a requirement, ADR, or milestone acceptance
changes.
