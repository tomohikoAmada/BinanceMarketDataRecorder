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

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M1.md`.

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

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M2.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

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

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M3.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

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

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M8.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

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

M11's 40%/15%/percentage-based thresholds remain the accepted historical
local-profile implementation contract. ADR-0028 prospectively selects the
explicit 18/14/12/10 GiB plus ETA policy for the future shared VPS; this does
not rewrite M11 acceptance evidence.

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
  explicitly waits; current transaction reconciles. M12 chooses immediate
  structured `BUSY`; the existing idempotent archive retry completes work
  before a repeated eject request.
- Rollback: disable eject command while preserving passive disappearance
  recovery; users may use macOS eject; revert M12.

## M13 — Blue/green upgrade and gap-free planned deploy

- Scope: versioned instances; independent candidate connection; snapshot/book
  sync and health readiness; old/new overlap; duplicate tagging/dedup support;
  guarded old shutdown; rollback/audit; reuse for 24-hour rotation.
- Non-scope: hiding overlap, stopping old on unready candidate, GUI deploys, or
  unplanned-fault guarantees beyond explicit gap marking.
- Dependencies: M4-M6 health/reconstruction, M8 metrics, M3 raw provenance.
- Acceptance: failed/unready candidate leaves old running; readiness proves
  current connections and persisted events for all core streams, a persisted
  public snapshot, and market-specific book sync; synchronized candidate
  permits cutover only after fresh post-readiness old/new events; overlap
  duplicates are identifiable; deployment transitions are durable; reverse
  rollback and pre-24-hour rotation use the same gate; no unmarked planned gap;
  no GUI.
- Rollback: stop candidate and retain old version; preserve overlap artifacts
  and deployment log; revert supervisor changes after safe state.

## M14 — launchd, logs, and MacBook power risk

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M14.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

- Scope: user LaunchAgent plist; install/uninstall/start/stop/status scripts;
  stdout/stderr; auto-restart; SIGTERM; permissions; multi-instance prevention
  compatible with supervised overlap; sleep-risk detection; optional scoped
  prevent-sleep assertion; documentation of lid-close limit.
- Non-scope: root LaunchDaemon, permanent power-setting changes, or promise of
  closed-lid capture.
- Dependencies: M13 lifecycle, M8 status, M3 safe sealing.
- Acceptance: login auto-start and machine-reboot-then-login recovery; current
  session launchctl bootstrap/crash-restart/bootout proof; CLI status validates
  PID/heartbeat; no root; SIGTERM seals/stops; sleep/wake gap is explicit; a
  kernel service lock permits only in-process managed blue/green overlap;
  scoped power assertion cleans up and no persistent power setting changes.
  If a disruptive reboot window is unavailable, record that gate as unrun with
  no claim of physical reboot evidence; this does not waive the V1 criterion.
- Rollback: unload LaunchAgent, stop/seal service, restore prior manual launch
  path, revert M14; no user power setting remains changed.

## M15 — Compression, normalized Parquet, and rebuildable data

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M15.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

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

Status: **ACCEPTED** by the commit containing
`docs/milestone_acceptance/M16.md`; exact SHA is reported at handoff to avoid a
self-referential commit hash.

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

## M17 — Short-term reliability and fault injection

- Scope: Spot bootstrap boundary correction; Spot snapshot rate-limit
  containment; network/DNS/Binance close/serverShutdown;
  missing/duplicate/out-of-order depth; Collector/Archive kill -9; Catalog
  lock/transaction fault; local space; missing/read-only external disk;
  checksum mismatch; sleep/wake; failed blue/green; physical normal archive,
  safe eject, pull-during-copy and idempotent reinsertion recovery; complete
  offline engineering gates.
- Non-scope: 72-hour/168-hour continuous-operation proof, release packaging, or
  claiming production/trading readiness.
- Dependencies: all M1-M16 acceptance gates and representative macOS hardware.
- Acceptance: complete required short-term fault matrix; Spot bootstrap and
  rate-limit regressions; physical external normal/error paths; no wrong source
  deletion or false archive commit; complete offline pytest, Ruff, strict mypy,
  contract verification, independent Go Raw golden verification and clean
  diff. Record 72-hour/168-hour proof as explicitly unexecuted and deferred.
- Status statement: short-term functionality and fault injection passed;
  continuous 72-hour and 168-hour operation was not executed. This does not
  make the version suitable for real-money trading.
- Rollback: return to last accepted version, preserve all test/run evidence and
  Raw, and repeat affected short-term tests after correction.

## M18 — Mac Developer Preview

- Scope: `0.1.0a1` wheel/sdist and SHA-256 manifest; CLI/version/commit
  provenance; logged-in-user LaunchAgent install/start/stop/status/uninstall;
  uninstall-with-data-retention proof; concise macOS quickstart, architecture,
  data/storage, operations and limitations documentation; clean-environment
  install verification; short public-data Spot/USD-M smoke; focused safety
  review and complete test report.
- Non-scope: Production/Stable/Trading Ready claims, remote publication, PyPI,
  GitHub Release, 72-hour/168-hour run, GUI, strategy, backtest, trading,
  accounts/keys, other exchanges, Ubuntu or Windows implementation.
- Dependencies: accepted M17 short-term gates and every prior
  compatibility/operations contract.
- Acceptance: reproducible wheel/sdist; verified CLI; rootless LaunchAgent
  lifecycle whose uninstall preserves data and whose reinstall reads Catalog;
  5–15-minute independent-data-root Spot/USD-M smoke; focused boundary review;
  complete offline pytest/Ruff/strict-mypy/contracts/Go-golden/build/install
  evidence; all skips listed; Developer Preview and long-run warning in every
  required release surface.
- Rollback: uninstall only the candidate LaunchAgent/code environment, preserve
  the application data root and Catalog, and reinstall the prior package.

Completion of M18 still stops and waits for explicit human publication/merge.

## M19 — Reliability repair and critical market-data completeness

- Scope: market-local depth resync; fail-fast core terminal recovery;
  restartable/visible side data; truthful RSS gauges; Spot exchangeInfo; six
  official USD-M latest-closed 5m statistics; revisioned official historical
  backfill; offline macOS CI and coverage contracts.
- Non-scope: strategies, models, backtests, accounts, orders, trading, API
  credentials, live raw-trade/kline streams, L3 fabrication, other exchanges,
  Ubuntu/RK3588, or long-running acceptance.
- Dependencies: accepted M18 Developer Preview; ADR-0011/0012/0022; current
  official Binance documentation, modular SDK, and public-data README.
- Acceptance: A–G audit recorded; every resync and terminal path tested;
  side-task recovery/status tested; public schemas and rate-limit provenance
  tested; archive checksum/revision/404/timestamp/clock/idempotency tested;
  offline pytest plus explicit stress, Ruff, strict mypy, contracts, Go golden,
  build/install and clean diff pass; branch pushed and PR opened without merge.
- Rollback: preserve immutable Live Raw, historical source revisions, Catalog
  evidence and gaps; disable affected side dataset/readiness, revert only M19
  commits, and never substitute third-party or fabricated data.

### M19.1 — Second-review blockers

**Scope.** Explicit official archive filenames and online URL smoke; immediate
  fail-fast for normally returning core collectors; ordered USD-M side-task
  shutdown; durable 5-minute Cursors and bounded catch-up; streaming Parquet
  normalization; safe HTTP Range recovery; truthful recovered book update ID;
  full backfill lineage verification; and repair of the existing macOS CI
  workflow validation failure.
**Non-scope.** Other symbols, credentials, accounts, orders, trading, strategy,
  models, R-034 production-rule changes, new PRs, or merge.
**Dependencies.** The M19 branch and immutable contracts in ADR-0023/0024.

**Acceptance.** Focused regressions, complete offline/stress/toolchain gates,
  explicit unsigned online archive and six-endpoint smoke, same-branch push,
  and a successful existing-PR CI run.
**Rollback.** Revert only M19.1 commits; preserve Raw, historical revisions,
  Catalog Cursors/events, and the open R-034 evidence.

### M19.2 — Normalized consumer contract for M19 Live data

**Scope.** Market-specific Spot/USD-M exchange-info schemas; six USD-M
5-minute schemas; multi-record REST expansion; timestamp-centered semantic
identity; explicit empty observations; and deterministic duplicate/conflict
handling through Raw-to-Parquet.
**Non-scope.** Collector lifecycle, Historical Importer architecture, R-034
production rules, other symbols, credentials, accounts, trading, strategy,
models, platform ports, new PRs, or merge.
**Dependencies.** The M19/M19.1 Live Raw provenance and the immutable
`normalized-dataset.v1` contract in ADR-0020.

**Acceptance.** Offline real-envelope-shaped parser fixtures and full
Raw-to-Parquet tests cover every M19 Live stream, multi-record expansion,
overlap deduplication, conflicts, empty and malformed observations, plus old
stream regression; complete offline/stress/toolchain/install gates pass; the
same PR branch is pushed and its CI succeeds.
**Rollback.** Revert only the M19.2 commit; preserve immutable Live Raw,
Catalog and existing normalized artifacts, and rebuild compatible datasets
from Raw after a corrected parser is available.

## M20 — Ubuntu ARM64/RK3588 transport and native deployment

- Status: **ACCEPTED and merged through PR #3.**
  Final PR Head: `2ebb981ea956929467b6dc4b0990875cc43e53bf`;
  Merge Commit: `80b8a5745fc64ee4e0ed0db7691c3acf7d2567bc`.
  macOS and Ubuntu CI both passed. M20 is closed.
- Scope: one redaction-safe `direct`/`environment`/`explicit` proxy policy for
  every Spot/USD-M WebSocket and REST exit plus Historical Backfill; Linux XDG
  paths; Ubuntu ARM64 dependency proof; non-root systemd service lifecycle;
  discovery and registration of already-mounted Linux external archive
  directories; RK3588 short public-data deployment and Mihomo-restart fault
  evidence.
- Non-scope: new symbols/markets, account/key/order/trading features, Raw or
  normalized schema changes, automatic mount/unmount/format/repair, firewall
  or routing changes, Docker/Kafka/Kubernetes, Linux blue/green certification,
  72-hour/168-hour soak, Production Ready claims, merge, release, or tag.
- Dependencies: M19.2 merged into `main`; Python `>=3.12,<3.13`; existing Raw,
  Catalog, reconnect/resync/gap and archive safety contracts; user-selected
  Mihomo node left unchanged.
- Acceptance: complete offline/stress/lint/type/contracts/Go/build gates;
  clean-venv final-Wheel ARM64 imports; static and real systemd lifecycle;
  direct and explicit public Spot/USD-M REST/WebSocket plus small Historical
  smoke; 30-minute concurrent service run; graceful stop/recovery/restart;
  Mihomo restart causes visible disconnect/reconnect/depth resync or explicit
  unreliable evidence; no proxy URL/credential/production data committed;
  branch pushed, PR opened, Code Reviewed, and merged through PR #3.
  Post-merge handoff completed. macOS Python 3.12 CI pass; Ubuntu Python 3.12
  CI pass.
- Rollback: stop/seal the service, retain `/var/lib` data and Catalog, install
  the prior Wheel/unit/config, and restart. Revert M20 code only; never delete
  or rewrite Raw. Linux blue/green and long soak remain M21 work.

## M21.2 / M21.3 / M21.4 — long-run evidence and recovery stability

- M21.2 completed its formal 72-hour window but failed because the Recorder
  restarted 639 times. Archive and disk evidence passed; the run isn't eligible
  for 168-hour continuation. See
  `docs/milestone_evidence/M21.2-72h-failure-analysis.md`.
- M21.3 was merged through PR #6 at
  `a9db145718338faa49a4e4c57bea9a821e74d828`. Its 2-hour preflight and closed
  12-hour observation passed, but a later USD-M `book_ticker` ingress overflow
  ended PID continuity before the 24-hour gate could start.
- M21.4 was merged through PR #7 at
  `cf1e749c7a533e916dbfb685212e5549a38c70dd`. The production Wheel
  (SHA-256 `926615b09ef46130f49a87fe8ab20acb7cfa6313daa67af5b718931bd95ff329`)
  was deployed and passed Stage D plus the Canonical Installed Identity Gate.
  Formal 2-hour and 12-hour process-stability windows passed with independent
  evidence reviews. The formal 24-hour window passed on process stability
  (corrective integrity review and Backpressure contract forensic review
  confirmed; the natural gen5 backpressure recovery cycle passed its recovery
  contract, RECOVERY_CONTRACT_PASS). M21.4 does not change public data
  schemas or begin a new soak. See
  `docs/milestone_acceptance/M21.4.md`,
  `docs/milestone_evidence/M21.4-ingress-overflow-analysis.md`,
  `docs/milestone_evidence/M21.4-deployment-and-validation.md`, and
  `docs/milestone_evidence/M21.4-24h-validation-forensics.md`.

  **M21.4.9/10/11 — formal 72h FAIL and reconnect boundary repair.** The
  formal 72-hour window's core process stability PASSED (PID 317289,
  NRestarts=0), but its data-integrity contract FAILED: the
  2026-08-07T14:08:24Z USD-M `book_ticker` unexpected disconnect reconnected
  within the same generation with no gap evidence, and every planned rotation
  seals `gap=false/complete=true`. FORMAL_72H_RESULT=FAIL,
  eligible_for_next_stage=false. The 12h/24h data-integrity acceptance is
  SUPERSEDED_BY_RECONNECT_INTEGRITY_FINDING (process stability stands). A
  corrected boundary-local read-only audit found 4,680 unmarked reconnect
  boundaries (11 explicit, 0 ambiguous). The M21.4.11
  repair (`fix/m21-4-reconnect-boundary-integrity`) implements the unified
  Reconnect Boundary state machine, manifest-level `reconnect_gap`, seal
  defense, and the read-only audit tool; the M21.4.11-R1..R5 correction adds
  crash-durable STARTED-before-seal ordering, boundary-local audit
  classification, strictly read-only audit semantics, side-data fail-closed
  terminal restart, and deterministic canonical audit output.   R2/R2.1/R2.2
  further add SEALING seal-intent recovery keyed by exact gap lifecycle,
  Catalog-first zero-record marker durability, exact operational-event
  idempotency, and logical audit transitions across frame-less chunks. It is
  merged to `main` through PR #11, but NOT DEPLOYED. The merged code state is
   distinct from deployed/production-validated state.
  Full record:
  `docs/milestone_evidence/M21.4-72h-failure-and-reconnect-integrity.md`.

  **M21.4.11 formal 72h PASS and M21.4.11-R3 orphan extension-intent P1.**
  The deployed artifact (`f659895…`) passed its independent formal 72h
  observational window: process continuity, 27/27 explicit WS transitions,
  +0 unmarked, 0 false-complete, 27/27 first-new Raw `sequence_gap`.
  FORMAL_72H_RESULT=PASS. The same review found a latent P1: a reconnect
  boundary that merely EXTENDS an open pending gap persisted a SEALING seal
  intent with a freshly minted gap_id and no lifecycle; startup recovery
  scans every historical SEALING intent and would materialize a phantom
  `STREAM_DISCONTINUITY_STARTED` on the next service restart (production
  example: um_perpetual `book_ticker` 2026-08-13T08:20:35Z, orphan gap_id
  `33e6420b`, marker `7223d5ba`, parent `70ace625`). The 168h gate
  requires a controlled service restart, so
  ELIGIBLE_FOR_168H=false for `f659895…`. The M21.4.11-R3 correction makes
  extension intents reuse the canonical pending-gap identity (attempt
  metadata under a separate `extension` key) and teaches startup recovery
  to recognize legacy orphan shapes from durable evidence without phantom
  materialization; REQ-103 intent-only crash recovery is preserved. An
  independent exact-head review (PR #11 R2) rejected the R3 closed-parent
  legacy discriminator as P1-unsound because it used UTC wall-clock
  containment plus generation equality as causal proof, which wall-clock
  rollback can defeat for a genuine post-completion boundary.
  M21.4.11-R3.1 corrects it with clock-independent durable identity rules
  (frame-bearing SEALING evidence, boundary connection equal to the
  parent's completing connection, generation identity), fail-closed
  ambiguity, and an explicit operator-reviewed additive classification
  authority; UTC was removed as silent-suppression authority. A further
  independent exact-head review (PR #11 R3.2) found three P1s in R3.1:
  UTC containment still gated whether CLOSED-parent ambiguity engaged
  (an orphan outside the parent's numeric interval could still become a
  phantom STARTED, and inverted-wall pairs were dropped from the
  interval universe); the classification authority was consulted before
  stronger durable proofs and could therefore override them; and no
  deterministic read-only production pre-start inventory existed.
  M21.4.11-R3.2 corrects all three with the exhaustive three-way
  partition (PROVEN_LEGITIMATE / PROVEN_EXTENSION / AMBIGUOUS, no
  fourth default), the strongly-bound authority
  (`legacy-reconnect-classification.v2`: chunk_id + canonical
  seal-intent SHA-256, consulted only for AMBIGUOUS candidates, and
  contradictions fail closed), the shared decision engine, the read-only
  `recovery legacy-reconnect-preflight` command, the two-phase startup
  (global pre-decision before any legacy lifecycle mutation), and the
  mandatory pre-start classification sequence documented in
  `docs/ubuntu_rk3588_operations.md`. UTC never gates classification.
  A third independent exact-head review (PR #11 R3.2) rejected it:
  REV-001 (P1) the "no possible parent → proven_legitimate" proof is
  unsound because malformed/unkeyable historical lifecycle authority can
  disappear from the searched universe; REV-002 (P1) the authority digest
  bound only chunk_id + seal intent, not `verified_frames`, although
  verified_frames drives classification; REV-003 (P1) the documented
  `root:root 0600` authority mode is unreadable by the production
  service (User=orangepi Group=orangepi); REV-004 (P2) the "read-only"
  preflight called `ensure_storage_layout()` and could mkdir/fsync
  missing directories. M21.4.11-R3.3 corrects all four: the legacy
  no-parent absence proof is REMOVED (absence of a parent only widens
  uncertainty; automatic legitimacy for legacy intents requires positive
  proof — trustworthy `verified_frames > 0` or the exact
  completing-connection proof — everything else is AMBIGUOUS); new
  intents emitted by the corrected runtime carry the durable
  `intent_schema: reconnect-seal-intent.v2` provenance (persisted inside
  the immutable SEALING evidence; pure extensions reuse the pending gap
  identity, decision-point-2 uses a fresh genuine gap, so a versioned
  fresh ABSENT intent safely materializes REQ-103 without operator
  classification; unknown future schemas fail closed); malformed
  lifecycle authority is surfaced as explicit degraded-authority
  predecision blockers instead of being silently skipped; the authority
  is bumped to `legacy-reconnect-classification.v3` with
  `classification_evidence_sha256 = sha256(canonical_json({chunk_id,
  seal_intent, verified_frames}))` binding the COMPLETE immutable
  decision evidence; the documented authority installation contract is
  owner=root group=orangepi mode=0640 (service-readable, not
  service-writable) enforced by a deterministic permission-contract
  test; and the preflight is intrinsically read-only (layout derived
  without mutation, exit 0 eligible / exit 2 ineligible with the full
  JSON report). SCHEMA_MIGRATION_REQUIRED=false and
  CATALOG_MUTATION_REQUIRED=false (the intent version field is a forward
  persistent evidence-contract revision, not a SQLite schema migration);
  ADDITIVE_COMPATIBILITY_AUTHORITY_REQUIRED=true and
  PRESTART_LEGACY_CLASSIFICATION_REQUIRED=true for the first corrected
  production start. The R3.3 focused exact-head review accepted the core
  algorithm but returned one P1 and two P2s. M21.4.11-R3.4 closes them
  narrowly without touching the accepted algorithm:
  REV-003-R3.3-001 (P1) — the authority file moved OUT of the
  service-writable data root into the root-controlled configuration
  namespace (`config_file.parent /
  legacy_reconnect_classifications.json`; production
  `/etc/binance-market-data-recorder/…`, parent root:orangepi 0750,
  file root:orangepi 0640), so the service can read but can never
  unlink/rename/replace the authority pathname (file mode 0640 alone
  was insufficient because the service owns the data-root directory);
  the permission-contract test now models both file and parent
  directory; R3.3-SCHEMA-001 (P2) — an explicit `intent_schema: null`
  (or any present-but-noncanonical value) now fails closed instead of
  being treated like a missing legacy key; R3.3-DOC-001 (P2) — the
  Ubuntu operations status now records that the deployed `f659895…`
  PASSED the formal 72h observational gate and became NOT ELIGIBLE FOR
  168H, while the corrected artifact is NOT DEPLOYED with validation
  PENDING. It is
  merged to `main` through PR #11 and remains NOT DEPLOYED. Production validation for the corrected
  artifact is PENDING: after review and separately authorized
  deployment, the NEW artifact must re-execute the full staged chain
  (exact artifact identity → readiness → 2h → 12h → 24h → 72h → 168h).
  See ADR-0027 "Pending-gap extensions and orphan seal-intent prevention /
  Legacy extension-orphan recovery (M21.4.11-R3, corrected R3.1,
  corrected R3.2, corrected R3.3, corrected R3.4)".

  The validation sequence continues: after the repair is reviewed, merged,
  and separately authorized for deployment, the NEW artifact must re-execute
  deployment canonical identity → readiness → 2h → 12h → 24h → 72h → 168h.
  The old artifact's windows are historical reference only. The 168-hour
  window is never started automatically.

## D0/D1 — VPS and remote archive architecture freeze

Status: **DOCUMENTATION-ONLY ARCHITECTURE FREEZE**. Approved by ADR-0028,
ADR-0029, and ADR-0030; implementation and deployment are not included.

- Primary production target: Ubuntu 24.04 LTS x86_64, Python 3.12, systemd,
  non-root Recorder service, 2 vCPU/4 GiB/40 GB-class shared VPS.
- Live VPS role: public acquisition, Raw spool/seal, Catalog, recovery,
  integrity/gap/provenance state, metrics/status, and archive export support.
- Offline role: local Normalize, heavy Replay/analysis, and Historical Backfill
  using the same Recorder distribution and the Offline Workspace.
- Archive direction: local client pulls from VPS over SSH through a replaceable
  `RemoteTransport` seam; durable local verification and receipt authorization
  precede immediate VPS source deletion.
- Archive Set: `archive_set_id` is logical collection identity;
  `storage_id` is one physical medium; chunks remain whole and media metadata
  is self-describing/rebuildable.
- VPS capacity policy: WARNING/CRITICAL/EMERGENCY/HARD RESERVE at 18/14/12/10
  GiB, with ETA triggers at 7 days/72 hours/24 hours.
- Acceptance roles: MacBook development, LAN Linux pre-production, and exact
  VPS staged acceptance `2h -> 12h -> 24h -> 72h -> 168h` after identity and
  readiness. LAN evidence does not substitute for VPS evidence.

See `docs/vps_operations.md`, `docs/archive_transfer_contract.md`,
`docs/offline_workspace.md`, and `docs/test_environment_matrix.md`.

## Future Work

M21 historical work contains the Ubuntu ARM64/RK3588 soak evidence and recovery
hardening. Future work includes the separately authorized VPS deployment and
staged acceptance, the portable macOS/Linux/Windows archive client, repeated
connection rotations, notifications, and a separately reviewed Web UI. Future
work has no production implementation in this documentation milestone and
does not include strategy, backtest, or trading implementation without a new
human-approved project scope.
