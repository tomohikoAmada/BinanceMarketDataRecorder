# Risk Register

Severity: Critical / High / Medium / Low. Status is Open, Monitoring, Mitigated,
or Accepted. Each implementing milestone must update its risks and evidence.

| ID | Risk | Sev. | Mitigation / evidence gate | Owner milestone | Status |
| --- | --- | --- | --- | --- | --- |
| R-001 | Official SDK WebSocket callbacks may hide/re-encode payloads, own receive timing, or drop under blocking callbacks | Critical | Probe confirmed all three failures; ADR-0009 rejects SDK WebSocket streams and selects caller-owned exact-byte transport | M2 | Mitigated |
| R-002 | Binance documentation paths/semantics change; specified `/docs/llms.txt` redirects to portal HTML | High | Updater uses the working official index and validates host, every redirect, HTTP 200, body/content and selected-page hashes | M2 | Mitigated |
| R-003 | USD-M stream routing changed to `/public` and `/market`; stale endpoints could silently omit streams | Critical | M5 pins the current notice: depth/bookTicker use `/public`, aggTrade uses `/market`; live acceptance verifies every route; M7 must revalidate side-data routes | M2/M5/M7 | Mitigated |
| R-004 | Callback or writer backpressure causes silent loss/unbounded memory | Critical | M3 bounded spool plus M4/M5 finite WebSocket buffer/receipt queues; overflow raises a critical market-local Collector fault, never drops silently | M3-M5 | Mitigated |
| R-005 | kill -9 leaves a corrupt tail that appears sealed | Critical | ADR-0010 framing/CRC, actual SIGKILL mid-frame recovery, quarantine matrix, verified compression and atomic manifest/Catalog tests | M3 | Mitigated |
| R-006 | Spot or USD-M sequence semantics are applied to the other market | Critical | Separate Spot `U/u` and USD-M `U/u/pu` schema modules and fixtures retain semantics without M6 continuity inference | M4-M6 | Mitigated |
| R-007 | Wall clock adjustment, reboot, or sleep makes receive ordering/lag misleading | High | Dual UTC/monotonic clocks, boot domain, sleep gaps, versioned replay tie-break | M3/M6/M14/M16 | Open |
| R-008 | External disk path aliases a different disk after rename/remount | Critical | UUID + marker + storage_id + relative path; re-resolve mountpoint; mismatch blocks writes | M9 | Open |
| R-009 | Filesystem reports writable but lacks needed durable/atomic behavior | High | In-directory write/fsync/rename/readback probe; report DEGRADED/ERROR, never emulate success | M9 | Open |
| R-010 | Disk disappears or process dies during copy/verify/Catalog boundary | Critical | Retain internal source; idempotent transaction/reconciliation at every crash point | M10 | Open |
| R-011 | Verified archive becomes only copy after internal deletion and later fails | High | Explicit warning/manifest verification; user-owned independent backup; periodic verify CLI | M10/M18 | Accepted |
| R-012 | Internal disk fills while archive unavailable or seal requires temp overhead | Critical | Thresholds/forecast, reserve includes seal overhead, emergency graceful stop, no unarchived delete | M3/M11 | Open |
| R-013 | Daily counters duplicate or lose increments across UTC boundary/restart | High | Idempotent persisted aggregation, deterministic fixtures and midnight/restart tests | M8 | Open |
| R-014 | Side-data polling/rate limits impair L2 collectors or funding cadence is assumed | High | Separate workers/budgets, official rate evidence, no forward fill/fixed 8-hour assumption | M7 | Open |
| R-015 | Blue/green cutover stops old instance before candidate is synchronized | Critical | Independent candidate readiness, overlap evidence, rollback, normalized dedup | M13 | Open |
| R-016 | launchd restart/multiple instances cause conflicting active writers | Critical | Single-writer locks with explicit supervised overlap identities; crash/seal recovery | M13/M14 | Open |
| R-017 | Mac sleep/closed lid creates unavoidable data gaps | High | Detect/mark sleep, scoped optional power assertion, document no closed-lid guarantee | M14/M18 | Accepted |
| R-018 | Compression or normalization mutates/deletes canonical Raw | Critical | Separate temp/output, source hashes, immutable manifests, repeatability tests | M3/M15 | Open |
| R-019 | Consumer depends on external mountpoint or Recorder internals | High | Catalog/manifest resolution and versioned M16 adapter contract | M16 | Open |
| R-020 | Official public API access is geographically/system restricted or transiently times out | High | M4 observed a one-second SDK TLS/proxy timeout; explicit 10-second timeout plus bounded snapshot retry keeps core streams active, while no successful snapshot remains a visible failure; no unofficial proxy | M2/M4/M5/M7 | Monitoring |
| R-021 | CRC32C/CBOR/Zstd implementations disagree across languages | Medium | ADR-0010 exact profile plus byte-identical Python vector and independent standard-library Go framing/CRC verifier | M3 | Mitigated |
| R-022 | PyObjC/Disk Arbitration cannot deliver required event/eject semantics in user context | High | M9 capability spike with evidence; minimal native helper only if verified; otherwise stop/update risk | M9/M12 | Open |
| R-023 | Data volumes exceed initial forecast or Catalog becomes a bottleneck | Medium | M3 million-frame bounded-memory gate passes and Catalog schema excludes event bodies; live growth/report validation remains M8/M11/M17 | M3/M8/M11/M17 | Monitoring |
| R-024 | User pre-existing changes in Alpha101Crypto are overwritten | High | Research repo remains read-only; audit baseline records dirty frontend files | All | Mitigated |
| R-025 | Project identifiers regress to an earlier M0/M0.1 identity | High | ADR-0007 constants and allowlisted legacy-name/path scans | M0.2/M1/M14/M18 | Mitigated |
| R-026 | Name, visual identity, wording, service label, or publisher metadata falsely implies an official Binance relationship | Critical | Prominent disclaimer; no Binance logo; author-controlled namespace; forbidden wording/namespace tests | M0.2/M14/M18 | Mitigated |
| R-027 | V1 over-engineers an unrequested multi-exchange framework | Medium | Binance Spot/USD-M scope in ADR-0007; another exchange requires separate review | M0.2 and all design milestones | Mitigated |
| R-028 | CloudFront/WAF challenges block scripted retrieval of some interactive developer-portal catalog pages | Medium | Treat every non-200/empty response as failure; select downloadable official Markdown and official SDK source; record challenge and never use an unofficial mirror | M2 and ongoing source refresh | Monitoring |
| R-029 | Local-book logic mixes Spot and USD-M continuity, hides a missing event, or overstates bookTicker as a checksum | Critical | ADR-0011 market-bound rules, random deletion fault tests, immutable incomplete intervals, same-ID-only ticker comparison and explicit non-checksum wording | M6 | Mitigated |

## M0 open questions assigned to milestones

- Exact live Spot/USD-M stream endpoints, `@100ms` payload fixtures, ping/pong
  and rate limits: M4/M5 revalidation using the M2 source pipeline.
- Official SDK REST/WebSocket fitness and exact locked versions: resolved by
  ADR-0008/ADR-0009; rerun probes on upgrades.
- Byte-exact combined wrapper versus inner payload capture: M4/M5 endpoint
  selection must preserve whichever wire form is selected.
- CBOR canonical profile, CRC32C coverage, Zstd parameters and file naming:
  resolved by ADR-0010; format changes require new vectors/version review.
- Hard minimum reserve beyond the required emergency threshold: M11, using M3
  measured seal overhead and live growth evidence.
- Disk Arbitration implementation technology and filesystem behavior matrix:
  M9/M12.
- Normalized primary keys/dedup tie-break and Parquet schemas: M15.
- Replay total-order details and consumer dataset-version policy: M16.
- Whether a future official Binance MCP exists with verifiable installation:
  not required; reconsider only from official documentation.
- Future distribution/import/CLI/application-data identifiers must match
  ADR-0007 exactly. M14 must select an author-controlled service namespace and
  cannot derive one from Binance ownership or historical names.
