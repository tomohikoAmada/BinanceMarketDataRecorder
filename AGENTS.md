# Binance Market Data Recorder Agent Contract

Current owner direction October29, 2026: follow the new
[production milestone plan](docs/production_milestone_plan.md), P0 through P6.
Design for at least ten underlying assets; the initial production exit is BTC
and ETH across their Spot quote pairs and USD-M/COIN-M perpetual and dated
futures. Options are deferred. Capture selected public data for offline research
and backtesting. P2 follows [ADR-0053](docs/adr/0053-single-rest-representation-and-required-information.md):
WS keeps exact payload bytes; each REST response stores one primary body or SDK-model
representation preserving required precision, exchange/receive times, identity,
sequence and gap information. Exact HTTP bytes/unknown fields are not universal gates;
do not persist both a complete model and Base64 body or label SDK output as original
HTTP. Existing data/readers remain compatible. P2 implementation and local validation are
delivered under ADR-0054. Independent review of 195762c accepts the measured D06
fixed-cadence/daily supplement; independent review of f9aaeee closes R4. The owner now
accepts an initial release without U/C dated premium-index klines under
[ADR-0055](docs/adr/0055-initial-production-dated-premium-exception.md). Perpetual premium
and all other selected scope remain. Prioritize reliable basic data and as much additional
distinct public information as resources allow. Independent review of 13602c8/58a3ca2
accepts the exception's source selection, coverage/budget and historical-read boundary;
71 focused checks and exact 13602c8 dual-platform CI are verified. P2 remains NOT ACCEPTED
because the subsequent docs-only 58a3ca2 macOS CI exposed P2-R5: a shutdown/reconnect test
assumes exactly three manifests despite legal automatic rotation creating a separate
boundary marker. Fix deterministic test timing/layout assertions and verify the final CI;
do not change the production protocol to satisfy the count or repeat accepted R1/R4 audits.
See [current independent review](docs/milestones/P2/review-20261029.md).
Research use is unknown and must not determine capture scope. Preserve distinct public
information first; remove equivalent representations and repeated work before reducing
observations. P2 records source priorities and explicit resource-limited coverage in the
existing inventory; preserve core facts across the selected markets. No silent dynamic
shedding or retrospective removal of failed sources. P3 removes hashes without a real
identity/integrity consumer and repeated same-operation work, while keeping meaningful
file/archive checks. See ADR-0053 and the plan for the exact boundary.
Owner selected 99.5% continuity, with honest per-source gaps and
automatic recovery. Saved-data correctness is a separate requirement. Sudden
power-loss engineering/tests are out of scope; retain existing periodic sync and
normal stop/drain, without claiming a new owner-approved loss window.

Research and the first core-path VPS baseline are preserved. R1/R3/R4 passed
PR #87 review; the remaining R2 classifier fix retains peak history through partial
drains and is verified by negative and bounded-oscillation regressions. See the
[P0 review](docs/milestones/P0/review-20261026.md) and appended v4 classification
of retained boundaries. Original measurements remain unchanged; no VPS rerun was
needed. P0 corrections are complete. [P1 review](docs/milestones/P1/review-20261027.md)
of 5218d9d found R1: retirement could miss the transition from a scheduling yield
to idle recv. Independent re-review accepts fix b292d0d: retirement is checked
before receive admission; the original three reproductions and 196 focused tests
(including nine new regressions) pass, and exact-commit dual-platform CI is verified.
P1 is complete. P2 overall remains NOT ACCEPTED; P3 has not started.
See [f9aaeee independent review](docs/milestones/P2/review-20261028.md#review-f9aaeee).
R1's measured per-product polling and independent daily supplement remain accepted.
R4 is closed: the existing cooldown is enabled synchronously before the persistence
await. Both six-case reproductions, 33 cooldown regressions and 42 related tests pass
independently, with one online smoke skipped. Unparseable Retry-After uses existing
fallback/ban semantics while retaining the original failure header; storage errors
and normal stop/drain ownership remain intact. No new limiter or in-flight cancellation.
R2 official U/C dated-premium applicability remains unresolved; preserve the complete
target and historical failures without substitution. The initial-production selection
is authorized to omit only these dated premium sources before scheduling, including
new dated products. Keep perpetual premium and all other source families. Record the
omission as owner-approved uncaptured scope, with official applicability still UNKNOWN;
do not label it exchange-not-offered or rewrite old Raw, gaps or denominators. The
source/budget reporting and implementation boundary have passed independent review.
Keep Draft PR #88 and its branch until P2-R5 is corrected and independently verified. Exact
f9aaeee dual-platform CI/logs remain verified; no new blocking code issue was found in that review.
Do not repeat R1 history audits or already closed R4 work.
Independent review of 066c2c3 accepts the bounded R2 investigation evidence and unsent
clarification draft; the runtime is unchanged and official applicability remains unknown.
See [scope and impact](docs/milestones/P2/review-20261028.md#review-066c2c3). On October29
the owner approved that narrow initial-production exception and separately authorized
the prepared public clarification. It is submitted as
[binance/binance-connector-python#585](https://github.com/binance/binance-connector-python/issues/585).
Waiting for an official reply is no longer a first-release P2 acceptance gate. The
official-source stop rule still prohibits inventing dated-premium semantics; it does
not prevent implementation of this explicitly declared omission. Do not ask again for
these approved actions. P2/PR #88 remain pending R5 correction/re-review; no
production work is authorized by documentation status alone.
The owner selected option one on October28: explicit per-product live polling
plus official daily trade files as a separate delayed supplement. This direction
is approved; do not ask for it again. First verify product/date/field coverage,
then implement a small independent download/import path and measured cadences.
Keep actual acquisition times and live gaps; historical facts cannot replace
receive times or live coverage. The 6s proposal is not automatically selected.
The existing 1s default stays until a measured, disclosed configuration replaces
it; routine work within this decision does not need another scope approval.
P1 preserves Raw v1, uses bounded vectored append and long-lived receive tasks,
and evaluates sequence-only capture health under ADR-0052. Local writer gains
do not establish all-feature CPU gains or VPS capacity. Compare matched per-source
timed work and tail; the NON_STEADY profile remains diagnostic only.
Use 2h checkpoints within 1/2-asset 24h runs, measured expansion/backoff, then
48h and 72h checkpoints within the final continuous 72h run. Avoid a new generic
fault/capacity framework, repeated whole-history audits, and private approval
chains. Apply substantive new data/verification contracts explicitly in P0 and
the affected implementation phase; preserve historical evidence and existing
data. Routine authorized development does not need repeated approval.

P0 preserved local base 9e8ec37 and user edits in d81e7d2, then merged
accepted remote main 01d7421 into development base 113cdb3. The first isolated
VPS benchmark measures installed source 20e57f3, not either HEAD and not full
production capacity. Preserve user changes while aligning the development base.
See [research](docs/research/2026-10-26/README.md) for evidence and limits.
All checkpoints and superseded workflow directions below are historical; they
do not override the current owner scope or authorize old production operations.

Historical operational instructions have moved to [retained history](docs/history/pre-P0-AGENTS.md); they are not active run/deployment instructions. Future acceptance follows [ADR-0051](docs/adr/0051-production-continuity-and-incremental-verification.md).

## Frozen project identity

- Display name: **Binance Market Data Recorder**
- Repository directory: `BinanceMarketDataRecorder`
- Repository path:
  macOS `/Users/amada/Documents/Development/Crypto/BinanceMarketDataRecorder`;
  Ubuntu ARM64 `/home/orangepi/BinanceMarketDataRecorder`
- Python distribution: `binance-market-data-recorder`
- Python import package: `binance_market_data_recorder`
- CLI: `binance-market-recorder`
- macOS application data:
  `~/Library/Application Support/BinanceMarketDataRecorder/`
- Linux interactive data:
  `~/.local/share/BinanceMarketDataRecorder/`
- Linux system service data/config:
  `/var/lib/binance-market-data-recorder/` and
  `/etc/binance-market-data-recorder/recorder.toml`

Use the established package, CLI, service, and data identifiers in ADR-0007.
The project uses its own branding and author-controlled service namespaces.

## Non-goals

- No current GUI, web frontend, FastAPI product API, or trading interface. A
  future Web UI is separately authorized and must not be forced into Recorder
  core.
- No factors, Alpha DSL, strategies, positions, backtests, account ledger, or
  live/simulated execution.
- No account endpoints, orders, API keys, secrets, or credential discovery.
- No other exchanges or unbounded all-exchange subscriptions. P2 discovers products
  only for configured underlying assets under explicit resource budgets.
- No Docker as the certified V1 deployment, Kafka, Kubernetes, or cloud-first
  stateless collection.
- No automatic formatting, repair, repartitioning, or exclusive ownership of an
  external volume.
- No exclusive ownership of the production VPS host or filesystem; unrelated
  co-resident services remain outside Recorder control.
- No production data under the repository, Desktop, Documents, iCloud Drive, or
  `/tmp`.

## Architecture boundary

Dependency direction is one-way:

```text
Binance public APIs
  -> Binance Spot / USD-M modules (COIN-M and dated identities in P2)
  -> Binance Market Data Recorder
  -> immutable raw chunks
  -> normalized datasets and manifests
  -> generic replay and consumer contracts
  -> arbitrary research, backtest, and monitoring consumers
```

Recorder is an independent repository and service. Consumers may depend only on
its generic published contracts, Catalog read interfaces, manifests, and replay
APIs, never Recorder internals. Recorder must not import from or write to any
consumer project. Alpha101Crypto is one optional external consumer example; it
has no privileged influence on Raw, Catalog, replay, normalization, or archive
protocols. See ADR-0001 and ADR-0007.

Spot and USD-M transport/schema modules are required boundaries. Do not build a
framework for hypothetical exchanges as a V1 acceptance condition. Supporting
another exchange requires a separate future architecture review. The
operator-configured product-set authority is ADR-0032; its ProductKey is
`(market, symbol)` and its runtime topology is one process with one Collector
per configured ProductKey.

## Data-integrity rules

1. Collector writes only to the internal active area. An external volume is
   never an active write target and its absence is not a Collector failure.
2. Preserve the exact raw WebSocket payload bytes plus exchange times, receive
   UTC wall-clock time, monotonic time, market, symbol, stream, connection ID,
   collector instance/version, and source sequence IDs. REST uses the declared
   single representation and required-field contract in ADR-0053; SDK models
   are not byte-exact HTTP bodies.
3. Raw files are append-only while active and immutable after sealing. Active
   files end in `.partial`. Duplicate raw events are allowed; derived layers
   own explicit deterministic deduplication.
   Future P2 D06 may use the explicitly versioned compact polling representation
   in ADR-0053 after measuring its benefit; this does not authorize blanket WS
   deduplication or rewriting historical Raw.
4. Every sealed file has count, time and sequence ranges, byte counts, schema
   and collector versions, SHA-256, and gap/resync metadata.
5. Never label a truncated, checksum-failed, or sequence-incomplete interval as
   complete. Recover to the last verified frame or quarantine it.
6. Archive through a target temporary file, fsync, readback, size and SHA-256
   verification, atomic rename, external manifest, and Catalog transaction.
   Delete the source only after all local/remote authorization steps succeed.
7. Never silently delete unarchived raw data, even under disk pressure. At the
   hard reserve, seal gracefully, stop collection, emit
   `DISK_EMERGENCY_STOP`, and mark the gap start.
8. All replay ordering and deduplication tie-breakers must be specified and
   deterministic. Raw payload bytes remain recoverable.

## Performance and meaningful SHA-256 use

Avoid excessive, meaningless SHA-256 work. A new or repeated digest must serve a
specific content identity, corruption check, trust transition or published
contract. Explain that purpose in the change/review; do not add per-event hash
chains, whole-corpus scans or dependency-tree hashing merely for routine metrics,
status polling or extra reassurance.

- Prefer one canonical immutable snapshot and one digest reused within the same
  operation over rebuilding/sorting/hashing the same bytes repeatedly. Bind the
  file, Catalog row and proof to that exact snapshot; do not cache across updates.
- Where equivalent validation can share an unavoidable read, compute required
  hashes/statistics together. Stored and uncompressed SHA-256 cover different
  byte representations and remain required. CRC, canonical encoding, counts,
  sequence/gap checks and writer poison/recovery rules also remain intact.
- A copy-time or write-time digest cannot replace independent full target
  readback after fsync. Preserve archive identity/receipt/manifest checks,
  restart/retry revalidation and deletion authorization, including the required
  source revalidation immediately before retirement.
- Reusing validation across steps requires an explicit ownership/observation
  boundary and mutation/replacement/retirement guards. A sealed pathname or
  matching inode, size and timestamps alone does not prove unchanged content.
  Historical audit artifacts retain their original verification rules. New P0–P6
  qualification follows ADR-0051: independently read each new sealed file,
  without requiring repeated whole-history LIVE passes.
- Benchmark reduced reads, canonicalization, allocations and wall/CPU cost with
  identical inputs; verify exact bytes/hashes/results and fault behavior. Report
  local prototypes separately from representative VPS evidence. Do not claim
  production speed or safety from fewer SHA calls alone. Substantial verification
  or archive-protocol changes require the corresponding contract/ADR review.

## Official-source priority

For Binance behavior, use only these sources, in this order:

1. The current Binance Agent Native index (`llms.txt`) and selected pages from
   the Binance developer portal.
2. Binance developer portal product pages and changelogs.
3. Official `binance/binance-connector-python` source and releases for SDK
   behavior.
4. Official `binance/binance-spot-api-docs` repository for Spot behavior and
   changelog corroboration.
5. Official `binance/binance-toolbox-python` examples only as corroborating
   implementation evidence, never as a silent replacement for conflicting
   normative product documentation.

Record URL, UTC retrieval time, content SHA-256, and relevant conclusion in
`docs/binance_sources.md`. Do not load all of `llms-full.txt` by default. The
documentation updater may download text only from `developers.binance.com` and
`github.com/binance`; it must never execute downloaded content.

If official sources conflict, have moved, or cannot establish the required
semantics, stop that implementation, preserve evidence, and update
`docs/risk_register.md`. Do not substitute a plausible but semantically
different implementation.

## SDK and transport policy

Evaluate `binance-sdk-spot` and
`binance-sdk-derivatives-trading-usds-futures` first for public REST snapshots
and metadata. Do not add deprecated `binance-futures-connector-python`, the
third-party `python-binance` package as a production core dependency, or an
unverified “Binance MCP”.

Every production network exit uses the M20 proxy policy. `direct` explicitly
ignores proxy environment variables, `environment` uses standard environment
discovery plus `no_proxy`, and `explicit` accepts only unauthenticated HTTP(S)
proxy URLs. Never emit a raw proxy URL into logs, status, Raw, manifests,
Catalog event bodies, or snapshots. Public state is limited to proxy mode,
scheme, loopback, and port.

Use an official SDK for WebSocket capture only if M2 proves raw-payload
fidelity, receive-time control, lifecycle/reconnect/24-hour rotation control,
unhidden depth update IDs, fault-injection support, and backpressure without
silent loss. Otherwise use a mature generic WebSocket client against an
officially documented stream and record the decision in an ADR.

## Security prohibition

Never place trades, call account or order endpoints, read API keys, inspect
credential stores, add secret fields to configuration, or request trading
permissions. Public, unsigned market-data endpoints only.

## Development workflow

Follow the owner's latest task and the current code. Historical milestone
instructions describe their original runs; they do not automatically constrain
new development. Update obsolete documentation and checks when requirements
change. Keep documents practical and avoid adding speculative infrastructure.

1. Read the current handoff and the contracts relevant to the task.
2. Check Git status and preserve unrelated changes.
3. Implement a coherent scope, using as many logical commits as the work needs.
4. Run checks that cover the change; record results and relevant unrun checks.
5. Update current docs where behavior or status changed. Record substantial
   architecture or data-contract changes in an ADR.
6. Report the result, remaining limitations, and the next useful step.

Production operations follow the current owner-approved task. A documentation
status or a historical live-run procedure is not itself a command to start work.

## Test layers

- Unit: deterministic, offline, no filesystem outside a temporary test root.
- Integration: local components and SQLite/filesystem behavior; offline by
  default.
- E2E fixture: local mock servers and official-schema fixtures.
- Fault injection/property: truncation, kill points, disk/volume/network and
  sequencing faults.
- Online smoke: explicit pytest marker, public endpoints only, opt-in, never
  part of the default test command.
- Soak/manual platform tests: explicitly recorded duration, machine, and
  unrun reasons.

Tests must not write to a real external volume unless the user explicitly
registered a test folder and the milestone calls for it.

## Common commands

Current baseline engineering checks:

```bash
git status --short --branch
python3.12 -m pip install -e '.[dev]'
python3.12 -m pytest -q
python3.12 -m ruff check .
python3.12 -m mypy
python3.12 tests/verify_m0_contracts.py
git diff --check
binance-market-recorder --version
binance-market-recorder config show
binance-market-recorder doctor
binance-market-recorder status
binance-market-recorder report daily --date <YYYY-MM-DD>
binance-market-recorder normalize status
binance-market-recorder normalize run
python3.12 examples/replay_consumer.py --data-root <root> \
  --build-id <sha256> --market spot --stream agg_trade
binance-market-recorder storage list
binance-market-recorder storage inspect <path>
binance-market-recorder storage register <folder-path>
binance-market-recorder storage unregister <storage-id>
binance-market-recorder storage status
binance-market-recorder storage eject <storage-id>
binance-market-recorder storage forecast
binance-market-recorder archive status
binance-market-recorder archive retry [--storage-id <storage-id>]
binance-market-recorder archive verify <storage-id>
binance-market-recorder launchd install --label <author-owned-label> \
  --author-controls-namespace
binance-market-recorder launchd start
binance-market-recorder launchd stop
binance-market-recorder launchd status
binance-market-recorder launchd uninstall
sudo binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd install --user <user> --group <group>
sudo binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd start
binance-market-recorder --config /etc/binance-market-data-recorder/recorder.toml \
  systemd status
python3.12 tools/update_binance_docs.py --output-dir <temporary-directory>
python3.12 tools/probe_binance_transports.py
BINANCE_MARKET_RECORDER_ONLINE=1 python3.12 -m pytest -m online -q
python3.12 -m pytest -m stress -q tests/stress/test_million_events.py
go run tools/verify_raw_chunk_golden.go
```

The documentation update performs network access only to its official
allowlist. The transport probe is offline by default; `--online-rest` and the
`online` pytest marker opt in only to unsigned public depth snapshots.
The `stress` marker is excluded from the default suite and must be run
explicitly for M3 acceptance because it writes and scans one million synthetic
Raw frames in a temporary test directory.

## Storage safety

### Remote administrative command safety

The 2026-09-12 root-home incident makes these rules permanent for every human,
agent, prompt, script, and SSH operation:

- Never recursively remove `/`, `/root`, another home directory, `/etc`,
  `/opt`, `/var`, `/srv`, a mount root, or an unresolved/empty target.
- Never assume a shell variable crosses the SSH boundary, and never construct
  a remote deletion target from local/remote interpolation. Cleanup variables
  must be created, validated, and consumed in the same remote shell with unset
  variables fatal.
- Prefer retaining evidence and creating a new `mktemp -d` child under an
  approved disposable parent. Do not clean merely to obtain a fresh workspace.
- If cleanup is necessary, require a nonempty canonical path, exact approved
  parent/child relationship, explicit protected-path and mount-root refusal,
  and printed resolved target before mutation. Creation/inspection and cleanup
  are separate reviewed steps; cleanup failure is normally nonfatal.
- A stopped live service is not necessarily safe across reboot. Every stopped
  handoff and maintenance preflight must record both `systemctl is-active` and
  `systemctl is-enabled`; keep Recorder disabled until an explicitly authorized
  pre-start step.

The interactive defaults are:

```text
macOS: ~/Library/Application Support/BinanceMarketDataRecorder/
Linux: ~/.local/share/BinanceMarketDataRecorder/
```

The Ubuntu system-service root is `/var/lib/binance-market-data-recorder`.
Linux discovers only already-mounted external block filesystems; it never
mounts, unmounts, formats, repairs, or creates udev rules. Without a reliable
OS eject capability it must report manual action and must not claim
`SAFE_TO_REMOVE`.

Never treat an entire external volume as project-owned. Operate only inside the
registered relative directory identified by volume UUID plus marker and
`storage_id`. Do not change filesystem format, repair it, write outside that
directory, or rely only on `/Volumes/<name>`. Never use the repository or its
parent `/Users/amada/Documents/Development/Crypto` as a production data root.

The future local archive client targets macOS, Linux, and Windows. Platform
volume/eject adapters may differ, but archive verification, Archive Set
identity, receipt binding, and deletion authorization remain portable. The
archive protocol/library is implemented; the operator-selected archive-machine
receive/readback/receipt command freeze and cross-platform production
certification remain pending.
