# Risk Register

Severity: Critical / High / Medium / Low. Status is Open, Monitoring, Mitigated,
or Accepted. Each implementing milestone must update its risks and evidence.

| ID | Risk | Sev. | Mitigation / evidence gate | Owner milestone | Status |
| --- | --- | --- | --- | --- | --- |
| R-001 | Official SDK WebSocket callbacks may hide/re-encode payloads, own receive timing, or drop under blocking callbacks | Critical | M2 raw-byte/backpressure/lifecycle/fault probes; generic client only via transport ADR if SDK fails | M2 | Open |
| R-002 | Binance documentation paths/semantics change; specified `/docs/llms.txt` currently redirects to portal HTML | High | M2 updater validates final host, status, content type/body and selected-page hashes; stop on unresolved semantics | M2 | Open |
| R-003 | USD-M stream routing changed to `/public` and `/market`; stale endpoints could silently omit streams | Critical | Verify each V1 stream against current official docs and public smoke; never infer routing | M2/M5/M7 | Open |
| R-004 | Callback or writer backpressure causes silent loss/unbounded memory | Critical | Bounded queue, explicit failure policy, synthetic million-event and overload/fault tests | M3-M5 | Open |
| R-005 | kill -9 leaves a corrupt tail that appears sealed | Critical | Framing/CRC scan, atomic seal, manifest/hash, recovery/quarantine fault matrix | M3 | Open |
| R-006 | Spot or USD-M sequence semantics are applied to the other market | Critical | Separate official fixtures/implementations; `U/u` and `U/u/pu` assertions and resync tests | M4-M6 | Open |
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
| R-020 | Official public API access is geographically/system restricted | High | Public opt-in smoke captures exact error; stop affected milestone rather than use unofficial proxy | M2/M4/M5/M7 | Open |
| R-021 | CRC32C/CBOR/Zstd implementations disagree across languages | Medium | M3 canonical byte rules and Python/Go-or-Rust golden vectors before acceptance | M3 | Open |
| R-022 | PyObjC/Disk Arbitration cannot deliver required event/eject semantics in user context | High | M9 capability spike with evidence; minimal native helper only if verified; otherwise stop/update risk | M9/M12 | Open |
| R-023 | Data volumes exceed initial forecast or Catalog becomes a bottleneck | Medium | Million-event stress, event corpus stays out of SQLite, rolling growth reports | M3/M8/M11/M17 | Open |
| R-024 | User pre-existing changes in Alpha101Crypto are overwritten | High | Research repo remains read-only; audit baseline records dirty frontend files | All | Mitigated |
| R-025 | Project identifiers regress to an earlier M0/M0.1 identity | High | ADR-0007 constants and allowlisted legacy-name/path scans | M0.2/M1/M14/M18 | Mitigated |
| R-026 | Name, visual identity, wording, service label, or publisher metadata falsely implies an official Binance relationship | Critical | Prominent disclaimer; no Binance logo; author-controlled namespace; forbidden wording/namespace tests | M0.2/M14/M18 | Mitigated |
| R-027 | V1 over-engineers an unrequested multi-exchange framework | Medium | Binance Spot/USD-M scope in ADR-0007; another exchange requires separate review | M0.2 and all design milestones | Mitigated |

## M0 open questions assigned to milestones

- Exact current Spot/USD-M stream endpoints, payload schemas, ping/pong and rate
  limits: M2.
- Official SDK REST/WebSocket fitness and exact locked versions: M2.
- Byte-exact combined wrapper versus inner payload capture: M2 transport ADR.
- CBOR canonical profile, CRC32C coverage, Zstd parameters and file naming: M3.
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
