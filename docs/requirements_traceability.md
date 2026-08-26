# Requirements Traceability

This matrix maps every requirement family from the M0 charter to an
authoritative contract and delivery/verification milestone. Detailed acceptance
steps are in `milestone_plan.md`; a row never weakens those steps.

| ID | Requirement family | Contract / decision | Delivery and proof |
| --- | --- | --- | --- |
| WF-01 | Exactly one milestone/run; read prerequisites/status; report and stop; use multiple logical commits only when a milestone explicitly requires them | `AGENTS.md`, milestone universal gate | Every M0-M20; acceptance record per milestone |
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
| DAT-09 | Deterministic replay, clock choices, checkpoints, explicit gaps | ADR-0004/0021, data contract | M6 fixed-hash/checkpoint tests; M16 equal-time order, clock, gap and checkpoint-seek tests pass |
| STO-01 | Internal disk always active target; no direct external active writes; external absence normal | storage contract, ADR-0003 | M4/M5/M9 fault tests |
| STO-02 | Platform default application-data layout; no persistent data in repo/Desktop/Documents/iCloud/tmp | project/architecture/storage contracts | M1 macOS and M20 Linux path tests; M14/M20 runtime |
| STO-03 | External folder optional/shared, registration only; no volume ownership/format change | storage/macOS contracts, ADR-0003/0014 | M9 scope-probe tests pass; physical matrix M17 |
| STO-04 | Identity uses UUID/name/fs type/relative folder/marker/storage_id and re-resolves mountpoint | storage/macOS contracts, ADR-0014 | M9 rename/reinsert and mismatch tests pass |
| STO-05 | READY requires writable access and write/fsync/rename/readback; read-only reported | storage contract, ADR-0014 | M9 capability/read-only tests pass; physical media matrix unrun |
| STO-06 | Full required storage state set | storage contract, ADR-0014/0017 | M9/M12 enum contract test passes |
| STO-07 | Archive temp/copy/fsync/readback/size+SHA/rename/manifest/Catalog then delete | storage contract, ADR-0003, ADR-0015 | M10 crash/fault/idempotence matrix passes |
| STO-08 | Never delete active/unverified/unarchived; delete retry; unique-copy warning | storage contract, R-011 | M10 tests/CLI/docs; M18 handbook |
| STO-09 | Only registered directory touched; residual temp cleanup bounded | storage contract, ADR-0015 | M9/M10 filesystem audit tests pass |
| SPC-01 | Historical local-profile 40%/15%/emergency thresholds; future VPS 18/14/12/10 GiB plus ETA triggers and protected reserve | ADR-0016, ADR-0028, storage/vps contracts | M11 local implementation tests; future exact-VPS measured validation |
| SPC-02 | 1h/6h/24h/7d robust growth and ETAs; insufficient/nonpositive sentinels | storage contract, ADR-0016 | M11 synthetic multi-window/archive-on tests pass |
| SPC-03 | Emergency suspends non-core, prioritizes verified archive, never deletes unarchived; seal/stop/gap | project/storage contracts, ADR-0016 | M11 real-spool emergency integration test passes |
| MET-01 | UTC/market/stream daily input, quality, output and performance metrics | project/data contracts, ADR-0013 | M8 deterministic fixture, Collector reconciliation and midnight tests |
| MET-02 | JSON+CSV reports and Catalog summary; SQLite excludes event corpus | project/data/architecture contracts, ADR-0013 | M8 aggregate-schema, atomic output and deterministic rebuild tests |
| MET-03 | Structured status, CLI status/daily reports, restart continuity/no double count | data contract, ADR-0013 | M8 stable-batch retry, restart and honest-status tests |
| MAC-01 | Disk Arbitration startup/events/mount/eject; PyObjC or proven minimal helper | macOS contract, ADR-0014/0017, R-022 | M9 startup callbacks and M12 non-forced unmount/eject callback bridge pass; physical media matrix M17 |
| MAC-02 | Required storage/archive CLI surface | macOS contract | M9/M11/M12 CLI tests pass |
| MAC-03 | Safe eject blocks work, syncs/closes, handles busy/refusal/forced removal | macOS/storage contracts, ADR-0017 | M12 idle/COPY/VERIFY/refusal/forced-removal/reinsert matrix passes |
| OPS-01 | User LaunchAgent, logged-in default, no root; logging/restart/SIGTERM/lock | macOS contract, ADR-0019 | M14 plist/CLI/current-session launchctl, SIGTERM/SIGKILL restart and lock tests pass; physical reboot/login window remains manual evidence |
| OPS-02 | Sleep risk/marked gaps; scoped optional prevent-sleep; no closed-lid promise | macOS contract, ADR-0019, R-017 | M14 NSWorkspace registration, deterministic sleep-gap and real scoped-caffeinate cleanup tests pass; M18 retains lid limit |
| OPS-03 | Blue/green candidate readiness/overlap/dedup/rollback/no planned unmarked gap | architecture contract, ADR-0018 | M13 synchronized/failure/rollback/Raw-tag/Catalog scenario tests pass; M15 owns dedup |
| OPS-04 | 24-hour proactive rotation reuses blue/green mechanism | architecture/macOS contracts, ADR-0018 | M13 scheduled 23 h 40 min path uses the same gate; M4/M5 23 h 50 min fallback remains marked |
| NRM-01 | Raw compression without mutation; versioned Parquet date/hour, lineage, gaps, rerunnable | data contract, ADR-0002/0020 | M15 all-stream parser, repeatability, Raw-hash, lineage, gap/dedup/conflict and DuckDB tests pass |
| CON-01 | Receive/exchange replay, range/seek/gap/manifest/dataset version | consumer/data contracts, ADR-0004/0021 | M16 half-open range, equal-time order, missing-clock/gap, manifest corruption and checkpoint-seek tests pass |
| CON-02 | Generic consumers hide archive location and use no Recorder internals/reverse coupling | ADR-0001/0007/0021, architecture | M16 public descriptors omit paths; independent example imports only replay API; named-consumer validation remains optional |
| FAI-01 | Required network/sequence/process/Catalog/disk/volume/checksum/sleep/deploy fault matrix | risk register and M17 plan | M17 fault report |
| FAI-02 | 72-hour and 168-hour continuous-operation proof with resource/rotation/archive evidence | Risk R-035 and Future Work | Not satisfied: M22.9 24h result INCOMPLETE; corrected artifact must restart the full chain |
| FAI-03 | Depth lifecycle/gap resync, fail-fast core terminal recovery and restartable side data | ADR-0023 | M19 deterministic lifecycle/overflow/terminal/retry tests |
| DAT-10 | Critical Spot rules/USD-M 5m statistics plus revisioned official archive import without clock fabrication | ADR-0024, data coverage | M19 schema/checksum/404/revision/timestamp/idempotency tests |
| DAT-11 | Market-specific normalized schemas for M19 Live data; one row per 5m period; exchange-timestamp identity; explicit empty response; deterministic duplicate/conflict treatment | data contract, ADR-0020, data coverage | M19.2 parser/schema and Raw-to-Parquet regressions |
| REL-01 | V1 docs/package/test reports/limitations/Ubuntu preparation | M18 plan | M18 release packet |
| REL-02 | V1 exit: continuous Spot/UM, rebuild/gaps/recovery/archive/space/blue-green/launchd/no GUI/key/trading/generic consumer | project contract and M18 plan | M18 acceptance; human decision |
| FUT-01 | Architecture permits Ubuntu adapter/API gateway/UI/more markets/consumer strategies without adding them to V1 | architecture/project contracts | M16 contract; M18 checklist |
| NET-01 | One direct/environment/explicit proxy decision for Spot/USD-M WS, REST SDK/urllib and Historical; no raw URL disclosure | ADR-0025, data contract, R-038 | M20 proxy/config/Mock CONNECT and redaction tests plus direct/explicit online smoke |
| LNX-01 | Ubuntu ARM64 Python 3.12 paths and dependencies; PyObjC Darwin-only | ADR-0026, operations guide | M20 Linux paths, fresh-Wheel aarch64 import smoke and doctor |
| LNX-02 | Non-root idempotent systemd lifecycle, journald, SIGTERM seal, TOML proxy, no data deletion | ADR-0026, operations guide | M20 static unit tests and RK3588 install/start/stop/restart/uninstall-retention evidence |
| LNX-03 | Already-mounted Linux external directory identity/capacity/marker; no auto mount/eject/format/repair | storage contract, ADR-0026, R-041 | M20 mountinfo/findmnt/lsblk fixtures; physical media remains M21 |
| FAI-04 | Proxy restart produces visible reconnect/resync/gap evidence without silent loss | ADR-0025/0026, R-039 | M20 Mock CONNECT plus RK3588 Mihomo restart; repeated long-run proof M21 |
| FAI-05 | A fatal bounded post-close handoff failure cannot seal the old tail complete or restart without same-gap/first-new Raw evidence | ADR-0027, Risk R-054 | M22.9 Spot/USD-M backpressure/session-restart origin and stale-generation regressions, global-stop negative cases, plus existing R1/R2/R2.1/R2.2 crash matrix; local fixes not deployed |
| VPS-01 | Ubuntu 24.04 LTS x86_64, Python 3.12, non-root systemd, shared 2 vCPU/4 GiB/40 GB-class VPS | ADR-0028 | Future exact-VPS artifact/readiness/2h/12h/24h/72h/168h acceptance; not deployed by D0/D1 |
| VPS-02 | VPS owns live acquisition, Raw/seal, Catalog/recovery, gap/provenance, metrics/status; heavy offline work remains local | ADR-0028, offline workspace | Future live/offline execution profiles; one Recorder distribution; not implemented by D0/D1 |
| ARC-01 | Local client pulls from VPS over SSH through replaceable transport seam | ADR-0029, archive transfer contract | Future archive-client transport and fault matrix; current ArchiveManager remains local |
| ARC-02 | Durable local verification -> durable exact VPS pre-delete authorization (bound to receipt + source identity) -> source revalidation -> unlink/durability -> terminal state with restart reconciliation; one copy is not backup | ADR-0029, R-048 | Future remote transaction implementation and crash/restart deletion authorization tests |
| ARC-03 | Archive Set logical identity, physical storage_id, whole-chunk placement, self-describing media, rebuildable index | ADR-0030, offline workspace | Future multi-media archive-client implementation and rebuild test |
| ARC-04 | Post-session consistent Catalog snapshots with latest+previous retention; snapshots never replace Raw | ADR-0029, offline workspace, R-050 | Future SQLite-supported backup/snapshot implementation and transfer verification |
| CAP-04 | VPS free-byte/ETA policy: 18/14/12/10 GiB and 7d/72h/24h, protected 10 GiB reserve | ADR-0028, vps_operations, R-049 | Future measured VPS validation; current local M11 behavior is historical profile behavior |
| ENV-01 | MacBook, remote Linux, and exact VPS have separate test and acceptance responsibilities | test environment matrix, ADR-0028/0029 | M22.8 isolated remote-source/archive failure matrix; M22.9 exact VPS stages; LAN/RK3588 evidence never substitutes for the exact VPS chain |

## Milestone coverage

The M0, M0.1, M0.2, and M1-M20 sections in `milestone_plan.md` each contain
scope, non-scope, dependencies, acceptance, and rollback. M19 review-fix
submilestones add scoped acceptance records without extending the top-level
M0-M20 plan. M21.4 (USD-M ingress backpressure repair) was merged through
PR #7, deployed to production, and passed 2h and 12h formal validation
windows with independent evidence reviews. The formal 24-hour window passed
and was confirmed by a corrective integrity review and a Backpressure
contract forensic review; the gen5 natural backpressure recovery contract
passed in-window. The original M21.4 formal 72h window was executed: core
process stability stood, but the data-integrity contract FAILED
(FORMAL_72H_RESULT=FAIL, eligible_for_next_stage=false). A later deployed
M21.4.11 artifact (`f659895…`) passed its independent formal 72h
observational gate (PASS: 27/27 explicit WebSocket transitions, +0 unmarked,
0 false-complete, 27/27 first-new Raw `sequence_gap`) but was then found to
contain a restart-only orphan-intent defect, making ELIGIBLE_FOR_168H=false;
the 168h window was not run. The further PR #11 corrected artifact is merged
to `main` but NOT deployed; a newly deployed corrected artifact must restart
the complete staged acceptance chain (2h→12h→24h→72h→168h).

### M21.4 traceability

| Element | Reference | Status |
|---------|-----------|--------|
| Incident | M21.3 USD-M book_ticker ingress overflow → CoreMarketTerminalFailure | Root cause identified |
| Code repair | PR #7, commit cf1e749c..., bounded awaitable backpressure, stream-local recovery, owned-worker protection | Merged |
| Deterministic tests | 603 pytest, stress, 1000-cycle, M0 contracts, Go golden | PASS |
| Deployment identity | Wheel SHA 926615b09..., direct_url, Canonical Gate | CONFIRMED |
| Stage D | stop/seal/install/readiness | PASS |
| 2h preflight | PID continuity, both markets READY 100% | PASS (short-window process stability; independent review) |
| 12h observation | 141 soak samples, 0 core errors, planned rotation verified | PROCESS-STABILITY PASS; data-integrity SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING |
| 24h window | Formal 24h PASS, eligible_for_72h=true | PROCESS-STABILITY PASS; data-integrity SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING |
| 24h corrective review | Journal re-export with epoch-derived UTC bounds (journald microsecond resolution); gen2/3/4 reclassified PRE_WINDOW; gen5 found in-window | EVIDENCE_INTEGRITY_PASS_WITH_CORRECTED_REPORT |
| 24h contract forensic review | gen5 RECOVERY_CONTRACT_PASS; gen6 timing CONSISTENT; contamination classification; evidence-source matrix | FORENSIC_CONTRACT_REVIEW |
| gen5 contract proof | Catalog STARTED/COMPLETED paired; Raw first-new sequence_gap; manifests gap=true/complete=false; historical_continuity_restored=false | PASS |
| 72h window | Formal 72h core process stability PASS (PID/NRestarts/boot_id unchanged); data-integrity contract FAIL (14:08 book_ticker disconnect) | **FAIL** (FORMAL_72H_RESULT=FAIL, eligible_for_next_stage=false) |
| 72h forensics | M21.4.10 unexpected-disconnect forensics: same-generation reconnect, no STARTED/COMPLETED, no sequence_gap, gap=false/complete=true | UNEXPECTED_DISCONNECT_CONTRACT_RESULT=FAIL |
| Reconnect boundary repair | M21.4.11 PR `fix/m21-4-reconnect-boundary-integrity`: unified state machine, manifest-level `reconnect_gap`, seal defense, read-only audit tool; R1..R5/R2/R2.1/R2.2/R3.x corrections: crash-durable intent ordering, exact gap lifecycle, Catalog-first marker durability, exact operational-event idempotency, boundary-local/frame-less audit classification, side-data fail-closed restart, deterministic canonical output | Merged to main through PR #11; NOT DEPLOYED |
| Historical audit (corrected, authoritative) | R2.2 read-only rerun: 161,817 chunks scanned (cutoff `1786349202047196027`, inventory SHA `ffaf34bd...`); 4,691 transitions; 11 explicit (all Catalog-identity-proven); 4,680 unmarked; 0 unknown; 0 overlap; catalog matched_pairs=11/unmatched=0/0 | `tools/audit_reconnect_boundaries.py` + `/var/tmp/m21-4-11-r2-2-historical-audit.json`; canonical SHA-256 `1122431c56ebd8367bbbed1a8fc0e30f1f020d7edfd9c34602c9988d89d4b35f`; earlier manifest-level scanner retained as SUPERSEDED |
| Later 72h observational gate | Deployed M21.4.11 artifact `f659895…` passed its independent formal 72h observational window (27/27 explicit WS transitions, +0 unmarked, 0 false-complete, 27/27 first-new Raw `sequence_gap`) | FORMAL_72H_RESULT=PASS |
| Orphan extension-intent P1 | Restart-only orphan-intent defect discovered in `f659895…`; the 168h gate requires a controlled service restart | ELIGIBLE_FOR_168H=false; 168h NOT RUN |
| PR #11 corrected artifact | M21.4.11-R3.x orphan-intent corrections merged through PR #11 | MERGED; NOT DEPLOYED |
| 168h window | — | PENDING (not started) |
| Documentation | M21.4 acceptance, deployment evidence, 24h forensics, 72h failure | `docs/milestone_acceptance/M21.4.md`, `docs/milestone_evidence/M21.4-deployment-and-validation.md`, `docs/milestone_evidence/M21.4-24h-validation-forensics.md`, `docs/milestone_evidence/M21.4-ingress-overflow-analysis.md`, `docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md` |
| Risk updates | R-004 (Monitoring, gen5 PASS; 72h FAIL), R-035 (Open, 72h FAIL), R-044 (Monitoring, epoch-bound mitigations), R-045 (Monitoring, not recovered), R-047 (Open, reconnect boundary integrity) | `docs/risk_register.md` |
| Production Ready | Not claimed | — |

### M21.4 24h evidence paths (host-local)

| Purpose | Path |
|---------|------|
| 24h run root | `/var/tmp/bmdr-m21-4-deploy-postmerge-20260804T023200Z/preflight/m21-4-24h-20260805T150930Z` |
| Original run | `RUN_ROOT/run/` (run-start.json, analysis.json, report.md, artifacts/formal-window.samples.jsonl, journals/) |
| Corrective review | `RUN_ROOT/corrective-integrity-review-20260806T152159Z/` |
| Contract forensic review | `RUN_ROOT/backpressure-contract-review-20260806T153010Z/` |
| Anchors | `RUN_ROOT/anchors/` |

These paths are local to the RK3588 host. Only the documentation in this
repository is published to GitHub; the run, review, and archive evidence
themselves are not uploaded.

The later exact-VPS M22.9 artifact at incident authority `ddb7309…` entered
staged acceptance, but its 24h result is INCOMPLETE after the fatal
post-close-handoff continuity defect. The local correction and deterministic
regressions are recorded in `docs/milestone_acceptance/M22.9.md`; the corrected
artifact is not deployed, receives no old duration credit, and must restart the
full staged chain. This matrix should be updated whenever a requirement, ADR,
or milestone acceptance changes.
