# Milestone Plan

Status: frozen by M0, historically corrected by M0.1, and finally scoped/named
by M0.2/ADR-0007 on 2026-07-22. Execute exactly one milestone per run and one
local commit per milestone. Never start the next milestone automatically.

## Universal gate

Before every milestone, read `AGENTS.md`, this plan, and the milestone's ADRs
and contracts; inspect Git status; verify the previous milestone acceptance;
and preserve unrelated changes. At completion, run the stated gates, record
unrun tests without lowering standards, update risks/compatibility, make one
local commit containing only that milestone, require a clean worktree, report,
and stop.

If official API semantics, macOS permission, or platform behavior cannot meet a
gate, stop implementation, capture evidence, and update the risk register. No
plausible but semantically different substitute is acceptable. Every milestone
inherits the bans on trading/accounts/keys, GUI, filesystem format/repair, and
modifying external consumer repositories. A future named-consumer validation is
read-only and optional unless separately authorized.

Rollback always preserves immutable data. “Revert commit” below means revert
code/config/schema additions; it never means delete or rewrite captured Raw.

## Dependency graph

```text
M0 -> M0.1 -> M0.2 -> M1 -> M2 -> M3 -> M4 -> M5 -> M6 -> M7 -> M8 -> M9 -> M10
                                                   M8 -> M11
                                             M9 -> M12
                                   M4/M5/M6 -> M13 -> M14
                         M3/M6/M8/M10/M13 -> M15 -> M16
                                  all implemented/fault gates -> M17 -> M18
```

Sequential execution remains mandatory even where the graph permits a weaker
technical dependency.

## M0 — Repository audit, boundary freeze, and complete plan

Status: **ACCEPTED** by commit recorded in the final M0 report.

- Scope: read-only audit of Alpha101Crypto's required contracts/config and
  Binance `data`/`exchange`/`store` modules; independent Git repository;
  `AGENTS.md`, README, project/architecture/data/storage/macOS contracts, full
  plan, traceability, source inventory, risks and foundational ADRs; minimal
  offline test.
- Non-scope: Python production package, CLI/service/config model, SDK install,
  updater tool, WebSocket/REST implementation, Collector, GUI, or any change to
  Alpha101Crypto.
- Dependencies: none. Bootstrap exception: the target repository, AGENTS and
  plan did not exist before M0; the research repo had no applicable AGENTS.
- Acceptance: boundaries do not conflict with audited Alpha101Crypto; every
  milestone is independently committable; all unknowns are explicit; all input
  requirements map to contracts/milestones; `pytest` and standalone verifier
  pass; only M0 is committed and worktree is clean.
- Rollback: revert/archive the M0 commit and repository before implementation;
  do not touch the audited research repository.

## M0.1 — Project identity, workspace, and generic consumer boundary correction

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M0.1.md` and reported at handoff.

- Scope: preserve the M0 Git history while moving the same repository to the
  intermediate identity/workspace recorded verbatim in ADR-0006 and the M0.1
  acceptance record; freeze that milestone's identifiers; remove the original
  Recorder identity from then-current surfaces; make Raw, Catalog, normalize,
  replay, archive, and consumer contracts generic; add ADR-0006, traceability
  and M0.1 acceptance; update contract tests.
- Non-scope: M1 packaging/config/CLI implementation, `src` production code,
  Binance connection, Collector/WebSocket/REST implementation, GUI, production
  data migration, or modification of Alpha101Crypto.
- Dependencies: accepted M0 root commit
  `1634b09e57d287eba82ef34f117b4657979cc38b`, clean worktree, and preservation
  of the existing `.git` object database during the directory move.
- Acceptance: Git top-level and frozen identities equal ADR-0006; original M0
  commit remains a commit object; exact legacy identity/path searches contain
  only explicitly classified migration history; every Alpha101 reference is
  classified as historical audit or optional ordinary external consumer; all
  current CLI/data/service paths use the new identity; M0 verifier and pytest
  pass on Python 3.12; no `src`, M1/network/GUI/Collector code, external-repo
  change, or dirty post-commit worktree.
- Rollback: revert the single M0.1 commit and move the same repository directory
  back only before later milestones depend on the new public identifiers; do
  not rewrite M0, create a second history, or move/delete production data.

## M0.2 — Binance-specific project identity correction

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M0.2.md` and reported at handoff.

- Scope: preserve M0/M0.1 history while moving the same repository to
  `/Users/amada/Documents/Development/Crypto/BinanceMarketDataRecorder`; freeze
  Binance-specific display/distribution/import/CLI/application-data identities;
  establish the independent unofficial-project disclaimer and brand/service
  namespace rules; make Spot/USD-M the product modules; remove exchange-neutral
  V1 positioning; add ADR-0007, traceability, M0.2 acceptance, and contract
  verification while preserving M16's generic consumer boundary.
- Non-scope: M1 packaging/config/CLI implementation, `src`, `pyproject.toml`,
  production configuration, Binance connection, Collector/WebSocket/REST
  implementation, GUI, service identifiers, branding assets, other exchanges,
  production data migration, or modification of Alpha101Crypto.
- Dependencies: accepted M0 commit
  `1634b09e57d287eba82ef34f117b4657979cc38b`, accepted M0.1 commit
  `b186c08191392e7454d259d8cfd16d0263e0901f`, clean worktree, and preservation
  of the existing `.git` object database during the directory move.
- Acceptance: Git top-level and all current identities match ADR-0007; README,
  project contract and release plan contain the unofficial/no-affiliation/
  no-sponsorship/no-endorsement rule; active documents do not imply official
  status or use Binance-owned-looking reverse-DNS namespaces; intermediate
  names occur only in allowlisted history/migration/test evidence; active V1 is
  Binance Spot/USD-M rather than multi-exchange; M0/M0.1 objects remain
  reachable; Python 3.12 pytest/verifier/diff checks pass; no M1/production/
  network/GUI code, external-repository change, or dirty post-commit worktree.
- Rollback: revert the single M0.2 commit and move the same repository directory
  back only before M1 publishes identifiers; do not rewrite earlier commits,
  create a second history, or move/delete production data.

## M1 — Python engineering skeleton, configuration, and CLI

- Scope: Python 3.12 `src` layout; `pyproject.toml`; installable package; lint,
  type checking and pytest; structured logging; strict Pydantic configuration;
  CLI/version/Git commit injection; default application-support paths;
  `binance-market-recorder doctor`, `config show`, and `status`;
  `binance-market-data-recorder` distribution and
  `binance_market_data_recorder` import package; path/permission guards; empty
  keys/credential surface.
- Non-scope: any Binance connection, collector, external-volume write, raw
  writer, launchd install, or production data.
- Dependencies: M0/M0.1/M0.2 contracts, ADR-0001/ADR-0007, and clean M0.2
  acceptance.
- Acceptance: clean environment install; unit/lint/type tests pass; all CLI
  commands run offline; default/override path and permissions are tested;
  repository/Desktop/Documents/iCloud/persistent `/tmp` are rejected for
  production roots; config schema contains no secret/key fields; no Binance or
  real external disk access.
- Rollback: revert M1; M0 documents remain usable and no production state needs
  migration.

## M2 — Agent Native docs pipeline and SDK capability validation

- Scope: `tools/update_binance_docs.py`; working `llms.txt`; configured selected
  Spot/USD-M pages and URL/time/hash records; allowed-host/redirect/content
  validation; pinned official modular SDK candidates; offline capability probes;
  unsigned public REST depth smoke; SDK versus generic WebSocket evidence; REST
  transport ADR and WebSocket transport ADR.
- Non-scope: long-running capture, spool writer, production Collector, account
  endpoints/keys, deprecated connectors, third-party MCP, or default download of
  all `llms-full.txt`.
- Dependencies: M1 tooling; ADR-0005; source/risk records.
- Acceptance: every semantic conclusion cites official evidence; only
  `developers.binance.com` and `github.com/binance` are downloadable; remote
  content is never executed; deprecated Futures connector absent; public
  no-key smoke works or milestone stops with official/platform evidence;
  default tests offline, online tests explicitly marked; probes establish raw
  payload/timestamp/lifecycle/backpressure/fault capabilities; no long-running
  Collector.
- Rollback: revert tool/dependency/ADR selection, keep the source evidence; no
  captured Raw exists.

## M3 — Event contract, raw chunks, Catalog, and crash recovery

- Scope: executable EventEnvelope v1; ADR-0002 byte-level specification and
  cross-language golden vectors; framed append-only writer; bounded queue;
  60-second/128-MiB configurable rotation; <=1-second configurable durability
  window; flush/fsync; `.partial`, seal, separate Zstd artifact, manifest and
  SHA-256; CRC32C frames; startup scan/tail truncation/quarantine; SQLite
  Catalog and idempotent transitions.
- Non-scope: real network, Binance parsing beyond synthetic envelopes, order
  book, normalization/Parquet, external archive, or service deployment.
- Dependencies: M2 transport/schema evidence and M1 config/logging.
- Acceptance: kill -9 matrix recovers last complete frame and never creates a
  false sealed file; repeat startup is idempotent; corruption/truncation and
  property/fault tests pass; 1,000,000 synthetic events pass within documented
  bounded memory; queue overload is explicit, never silent; format golden
  vectors pass; Catalog never stores full events.
- Rollback: stop writers, preserve/version any test chunks, revert M3. A
  superseding format retains readers/transcoder for any non-test chunks.

## M4 — Binance Spot BTCUSDT real-time Collector

- Scope: Spot diff depth 100 ms, aggTrade, bookTicker and public REST depth
  snapshots; selected transport; ping/pong, serverShutdown, backoff reconnect,
  planned 24-hour rotation; exchange/receive clocks, connection/session/version,
  raw payload and snapshot provenance; graceful stop/network evidence.
- Non-scope: USD-M, book reconstruction, Parquet, archive, strategies, callback
  compression or complex calculation.
- Dependencies: M2 Spot schemas/transport ADR; M3 durable writer.
- Acceptance: official-schema fixtures; local mock WebSocket; disconnect,
  duplicate and out-of-order tests; explicitly marked public live smoke >=15
  minutes; all accepted/malformed evidence reaches Raw; callback only envelopes
  and bounded-enqueues; shutdown seals safely; no key/account call.
- Rollback: disable Spot worker, seal/mark end and gap as applicable, revert M4;
  retain readable raw chunks and version provenance.

## M5 — Binance USD-M BTCUSDT real-time Collector

- Scope: independent USD-M perpetual diff depth 100 ms, aggTrade, bookTicker
  and public REST snapshot; USD-M sequence fields including `U/u/pu`; separate
  connection/failure/metrics state; side-data extension point.
- Non-scope: order-book reconstruction, side-data implementation, archive,
  research or coupling Spot/UM failure.
- Dependencies: accepted M4 pattern, M2 current routed-endpoint/schema evidence,
  M3 writer.
- Acceptance: M4-equivalent fixtures/mock/disconnect/duplicate/out-of-order and
  public live gates; official `U/u/pu` semantics asserted; Spot and USD-M run
  together >=30 minutes; market-specific statistics; injected crash/failure of
  either leaves the other running; exact payloads written Raw.
- Rollback: disable only USD-M, seal/mark its state, revert M5; Spot continues.

## M6 — Spot and USD-M order-book reconstruction and quality audit

- Scope: diff buffer before snapshot; official snapshot bridging; absolute
  price-level apply and zero delete; market-specific gap/resync; checkpoints;
  best bid/ask, crossed/empty checks; bookTicker comparison; explicit unreliable
  intervals and deterministic hashes.
- Non-scope: execution/queue model, strategy, treating checks as exchange
  checksum where none is documented, or hiding gaps.
- Dependencies: M4/M5 Raw and M2 official algorithms; M3 checkpoints/storage.
- Acceptance: official examples; randomized sequence properties; deleted update
  always creates a gap; gap never complete; repeated replay hashes match;
  checkpoint restore equals origin replay; Spot and UM rules cannot be mixed.
- Rollback: remove derived books/checkpoints and revert; Raw remains unchanged
  and can rebuild with a new algorithm version.

## M7 — USD-M auxiliary market data

- Scope: independently configurable current-official mark price,
  index/premium, funding, open interest, liquidation events, exchange info and
  filter snapshots; explicit sparse/polling semantics; source/fetch time and
  REST rate-limit provenance; isolated failure/metrics.
- Non-scope: blocking/pausing core L2, forward-filling missing values, assuming
  an eight-hour funding interval, private endpoints, strategies.
- Dependencies: M5 isolation and M2 refreshed official source pipeline.
- Acceptance: each kind has official schema fixture and failure statistics;
  public/no-key behavior verified; missing values not silently filled;
  observed/official funding cadence preserved; rate limits tested; side-data
  failure leaves both core collectors healthy.
- Rollback: disable individual data kind and remove/rebuild its derived outputs;
  preserve Raw/provenance and core streams.

## M8 — Metrics, daily traffic reports, and status

- Scope: all project-contract input, quality, output and performance counters;
  structured runtime JSON; `binance-market-recorder status`;
  `binance-market-recorder report daily`; UTC
  rollover; persisted idempotent aggregates; per-market/stream audit.
- Non-scope: GUI/metrics web server, full events in SQLite, storage forecast
  algorithm (M11), or external archive mechanics.
- Dependencies: M3 Catalog, M4-M7 event/quality signals.
- Acceptance: fixed fixture yields deterministic JSON/CSV; UTC midnight is
  correct; restart/replay does not double count; each stream reconciles input
  and output; required lag percentiles/queue/write/fsync/CPU/RSS/free-space/age
  fields exist with explicit unavailable semantics.
- Rollback: regenerate versioned reports/aggregates from Catalog/manifests and
  revert; never change Raw.

## M9 — macOS external-volume detection and archive registration

- Scope: Disk Arbitration adapter; startup inventory and appearance/disappearance;
  mountpoint/UUID/name/filesystem/read-only/capacity; folder registration and
  marker; in-folder write/fsync/rename/readback probe; full public storage state
  machine; display-only unregistered volumes.
- Non-scope: copying/deleting spool, formatting/repairing/remounting, volume-root
  marker, fixed-name identity, or making Collector depend on external storage.
- Dependencies: M1 CLI/config, ADR-0003, M8 status.
- Acceptance: available APFS/exFAT environments tested and unavailable physical
  tests explicitly unrun; unplug/replug and UUID recognition after name/path
  change; read-only cannot be READY; zero writes outside registered folder; no
  external disk leaves Collector normal; PyObjC/helper semantics proven or
  milestone stops per R-022.
- Rollback: unregister/disable adapter without deleting markers or archives;
  revert M9; internal capture continues.

## M10 — Archive transaction, verification, and safe local deletion

- Scope: oldest-sealed selection; `.copying`; streaming copy; fsync/reopen/full
  readback; size/SHA-256; atomic final rename; external manifest; Catalog
  transaction; separate local deletion; retries/reconciliation; disappearance,
  collision, residual-temp and crash idempotence.
- Non-scope: any file outside the registered directory, active/unverified
  deletion, multiple backup guarantees, safe eject UI/flow (M12).
- Dependencies: M3 chunks/Catalog and M9 READY identity/state.
- Acceptance: kill -9 during copy, verify and both sides of Catalog commit;
  simulated unplug; checksum mismatch; existing same/mismatched target;
  deletion failure; every failure retains internal source; verified success can
  release local space; operations remain inside registered directory.
- Rollback: stop new archive work, reconcile in-flight transactions, retain all
  internal sources not fully committed, revert; never mass-delete target data.

## M11 — Space alerts and exhaustion forecast

- Scope: internal/per-target space states; 1 h/6 h/24 h/7 d robust growth rates;
  40%/15%/`max(10 GiB,5%)` thresholds and UTC ETAs; backlog/oldest age; alerts;
  emergency policy; `binance-market-recorder storage forecast`.
- Non-scope: silent data deletion, changing filesystems, claiming forecast when
  history is insufficient.
- Dependencies: M8 persisted metrics and M10 archive/delete rates.
- Acceptance: synthetic positive/negative/insufficient/multi-window rates and
  archive-on scenarios; no NaN/infinity; exact `INSUFFICIENT_DATA` and
  `NOT_APPROACHING`; emergency suspends non-core work, prioritizes verified
  archive, seals/stops before hard reserve and opens explicit gap without
  filling disk.
- Rollback: disable forecasts/alerts only after preserving conservative hard
  stop; revert algorithm and rebuild history-derived output.

## M12 — macOS safe eject

- Scope: `binance-market-recorder storage eject <id>`; block new allocation;
  wait/cancel work;
  fsync/transaction completion or rollback; close handles; Disk Arbitration
  unmount/eject; safe-to-remove result; busy/refusal/forced-removal recovery.
- Non-scope: forced filesystem manipulation, format/repair, claiming success
  before system confirmation.
- Dependencies: M9 Disk Arbitration and M10 transactions.
- Acceptance: idle, COPY, VERIFY, system refusal, forced unplug, and reinsertion
  tests; internal source never lost; busy immediate request is refused or
  explicitly waits; current transaction reconciles.
- Rollback: disable eject command while preserving passive disappearance
  recovery; users may use macOS eject; revert M12.

## M13 — Blue/green upgrade and gap-free planned deploy

- Scope: versioned instances; independent candidate connection; snapshot/book
  sync and health readiness; old/new overlap; duplicate tagging/dedup support;
  guarded old shutdown; rollback/audit; reuse for 24-hour rotation.
- Non-scope: hiding overlap, stopping old on unready candidate, GUI deploys, or
  unplanned-fault guarantees beyond explicit gap marking.
- Dependencies: M4-M6 health/reconstruction, M8 metrics, M3 raw provenance.
- Acceptance: failed/unready candidate leaves old running; synchronized
  candidate permits cutover; overlap duplicates are identifiable; no unmarked
  planned gap; rollback works; no GUI.
- Rollback: stop candidate and retain old version; preserve overlap artifacts
  and deployment log; revert supervisor changes after safe state.

## M14 — launchd, logs, and MacBook power risk

- Scope: user LaunchAgent plist; install/uninstall/start/stop/status scripts;
  stdout/stderr; auto-restart; SIGTERM; permissions; multi-instance prevention
  compatible with supervised overlap; sleep-risk detection; optional scoped
  prevent-sleep assertion; documentation of lid-close limit.
- Non-scope: root LaunchDaemon, permanent power-setting changes, or promise of
  closed-lid capture.
- Dependencies: M13 lifecycle, M8 status, M3 safe sealing.
- Acceptance: login auto-start; crash and machine reboot recovery; CLI status;
  no root; sleep/wake gap explicitly recorded; single instance except managed
  blue/green; power assertion cleans up.
- Rollback: unload LaunchAgent, stop/seal service, restore prior manual launch
  path, revert M14; no user power setting remains changed.

## M15 — Compression, normalized Parquet, and rebuildable data

- Scope: operationalize/version sealed Raw compression from ADR-0002 without
  in-place mutation; normalize every Spot/UM stream; Parquet UTC date/hour
  partitions; schema/version/source hashes; deterministic dedup and gap
  propagation; checkpoints; rerunnable output; DuckDB smoke query.
- Non-scope: strategy/factors/backtests, hiding gaps, mutation of Raw, or
  filesystem-location coupling in consumer output.
- Dependencies: M3 format, M4-M7 schemas/quality, M13 overlap semantics.
- Acceptance: repeated same Raw produces logically identical versioned data;
  each partition traces to source hashes; blue/green overlap resolves
  deterministically; gaps remain visible; Raw hashes unchanged; DuckDB query
  succeeds.
- Rollback: delete only versioned derived/compressed copies proven rebuildable,
  retain Raw, revert M15 and regenerate with old version.

## M16 — Replay interface and generic consumer data contract

- Scope: receive/exchange-time replay and event clock; market/stream/time reads;
  checkpoint seek; explicit gap policy; generic manifest query/dataset version;
  generic `docs/consumer_contract.md`; independent example consumer; optional
  read-only validation that Alpha101Crypto can connect as an ordinary consumer.
- Non-scope: Recorder strategy, factor, BacktestRunner change, live trading, or
  modifying any consumer repository. The optional named-consumer validation is
  not a V1 gate and cannot add reverse dependencies or specialized core fields.
- Dependencies: M15 datasets, M6 checkpoints, ADR-0004.
- Acceptance: identical input produces identical total order; any consumer does
  not know archive mountpoint; generic Catalog/manifest APIs resolve locations;
  the independent example uses only published contracts; no named consumer is
  required for V1 completion.
- Rollback: retain prior reader/version, revert new API/example, leave Raw and
  datasets intact.

## M17 — Fault injection and long-running proof

- Scope: network/DNS/Binance close/serverShutdown; missing/duplicate/out-of-order
  depth; Collector/Archive kill -9; Catalog lock/transaction fault; local space;
  missing/read-only external disk; checksum mismatch; sleep/wake; failed
  blue/green; 72-hour PoC then seven-day run after fixes; daily operational
  evidence and forecast error.
- Non-scope: lowering durations, explaining gaps without evidence, or release
  packaging.
- Dependencies: all M1-M16 acceptance gates and representative macOS hardware.
- Acceptance: full required fault matrix; daily messages/bytes/CPU/RSS/disk/
  reconnect/gap/resync/archive throughput/forecast error; no unexplained gap,
  unbounded memory, infinite partial buildup, or wrong deletion; complete
  seven-day report. Any restart after a fix restarts the applicable soak clock.
- Rollback: return to last accepted version, preserve all test/run evidence and
  Raw, repeat affected soak after correction.

## M18 — macOS V1 release

- Scope: install/launchd/config/CLI/data/archive/eject/recovery/backup docs;
  72-hour/seven-day reports; limitations; Ubuntu preparation; versioned package
  and complete test report; prominent independent/unofficial/no-affiliation/
  no-sponsorship/no-endorsement disclaimer; manual release decision packet.
- Non-scope: automatic remote merge/release, GUI/trading, Ubuntu certification,
  or features excluded from V1.
- Dependencies: accepted M17 and every prior compatibility/operations contract.
- Acceptance: Spot/UM continuous Raw, reconstructable L2 and detectable gaps;
  crash recovery; optional unplug-safe archive; verified-delete; 40%/ETA;
  blue/green; LaunchAgent; no GUI/trading/key; generic independent consumer;
  Binance Spot/USD-M scope and non-official identity are unambiguous;
  reproducible package/docs/test evidence.
- Rollback: retain prior signed/versioned package and data readers, uninstall the
  candidate LaunchAgent cleanly, restore previous service version, and await a
  new human release decision.

Completion of M18 still stops and waits for explicit human publication/merge.
