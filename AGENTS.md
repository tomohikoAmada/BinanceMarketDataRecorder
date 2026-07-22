# Binance Market Data Recorder Agent Contract

## Frozen project identity

- Display name: **Binance Market Data Recorder**
- Repository directory: `BinanceMarketDataRecorder`
- Repository path:
  `/Users/amada/Documents/Development/Crypto/BinanceMarketDataRecorder`
- Python distribution: `binance-market-data-recorder`
- Python import package: `binance_market_data_recorder`
- CLI: `binance-market-recorder`
- macOS application data:
  `~/Library/Application Support/BinanceMarketDataRecorder/`

Project, package, CLI, launchd, log, configuration, and service identifiers use
the frozen project identity in ADR-0007. This is an independent, unofficial
project; the name identifies the public API/data source and does not imply
affiliation, maintenance, sponsorship, certification, partnership, or
endorsement by Binance. Never use Binance logos or official visual identity.
Future reverse-DNS/service/publisher identifiers must use a namespace owned or
controlled by the project author; never use a Binance-owned-looking reverse-DNS
root under `.com`, `.org`, or `.io`, and do not guess the author's final
namespace in advance.

## Project goal

Build a long-running, stateful Python 3.12 recorder specifically for Binance
public market data on macOS Apple Silicon. V1 captures BTCUSDT Spot and USD-M
perpetual depth at 100 ms, aggregate trades, book ticker events, and public REST
depth snapshots, followed by defined USD-M auxiliary data. It keeps recoverable
immutable raw payloads, deterministic replay metadata, explicit gap evidence,
and optional verified archival to a user-registered directory.

The authoritative scope is `docs/project_contract.md`. The delivery sequence and
acceptance gates are in `docs/milestone_plan.md`.

## Non-goals

- No GUI, web frontend, FastAPI product API, or trading interface.
- No factors, Alpha DSL, strategies, positions, backtests, account ledger, or
  live/simulated execution.
- No account endpoints, orders, API keys, secrets, or credential discovery.
- No other exchanges or additional symbols in V1.
- No Docker as the certified V1 deployment, Kafka, Kubernetes, or cloud-first
  stateless collection.
- No automatic formatting, repair, repartitioning, or exclusive ownership of an
  external volume.
- No production data under the repository, Desktop, Documents, iCloud Drive, or
  `/tmp`.

## Architecture boundary

Dependency direction is one-way:

```text
Binance public APIs
  -> Binance Spot and USD-M modules
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
another exchange requires a separate future architecture review.

## Data-integrity rules

1. Collector writes only to the internal active area. An external volume is
   never an active write target and its absence is not a Collector failure.
2. Preserve the exact raw WebSocket payload bytes plus exchange times, receive
   UTC wall-clock time, monotonic time, market, symbol, stream, connection ID,
   collector instance/version, and source sequence IDs.
3. Raw files are append-only while active and immutable after sealing. Active
   files end in `.partial`. Duplicate raw events are allowed; derived layers
   own explicit deterministic deduplication.
4. Every sealed file has count, time and sequence ranges, byte counts, schema
   and collector versions, SHA-256, and gap/resync metadata.
5. Never label a truncated, checksum-failed, or sequence-incomplete interval as
   complete. Recover to the last verified frame or quarantine it.
6. Archive through a target temporary file, fsync, readback, size and SHA-256
   verification, atomic rename, external manifest, and Catalog transaction.
   Delete the internal source only after all those steps succeed.
7. Never silently delete unarchived raw data, even under disk pressure. At the
   hard reserve, seal gracefully, stop collection, emit
   `DISK_EMERGENCY_STOP`, and mark the gap start.
8. All replay ordering and deduplication tie-breakers must be specified and
   deterministic. Raw payload bytes remain recoverable.

## Official-source priority

For Binance behavior, use only these sources, in this order:

1. The current Binance Agent Native index (`llms.txt`) and selected pages from
   the Binance developer portal.
2. Binance developer portal product pages and changelogs.
3. Official `binance/binance-connector-python` source and releases for SDK
   behavior.
4. Official `binance/binance-spot-api-docs` repository for Spot behavior and
   changelog corroboration.

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

Use an official SDK for WebSocket capture only if M2 proves raw-payload
fidelity, receive-time control, lifecycle/reconnect/24-hour rotation control,
unhidden depth update IDs, fault-injection support, and backpressure without
silent loss. Otherwise use a mature generic WebSocket client against an
officially documented stream and record the decision in an ADR.

## Security prohibition

Never place trades, call account or order endpoints, read API keys, inspect
credential stores, add secret fields to configuration, or request trading
permissions. Public, unsigned market-data endpoints only.

## Milestone workflow

Exactly one milestone is allowed per run and per local commit. Before work:

1. Read this file, `docs/milestone_plan.md`, and the milestone's ADRs/contracts.
2. Check Git status and preserve unrelated user changes.
3. Verify the previous milestone's acceptance record. M0 has no predecessor;
   its bootstrap exception is recorded in the M0 acceptance section.
4. State the current milestone and do not implement later scope.

Before completing a milestone:

1. Run all acceptance commands possible in the current environment.
2. List tests not run and why; never silently lower a gate.
3. Update the plan, risk/source records, contracts, and acceptance evidence.
4. Ensure the commit contains only the current milestone, create one local
   commit, verify a clean worktree, and stop.
5. Report milestone, modified files, architecture decisions, commands, test
   results, unrun tests, known limitations, compatibility impact, and the next
   milestone name.

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

Current M9 engineering checks:

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
binance-market-recorder storage list
binance-market-recorder storage inspect <path>
binance-market-recorder storage register <folder-path>
binance-market-recorder storage unregister <storage-id>
binance-market-recorder storage status
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

The production default is:

```text
~/Library/Application Support/BinanceMarketDataRecorder/
```

Never treat an entire external volume as project-owned. Operate only inside the
registered relative directory identified by volume UUID plus marker and
`storage_id`. Do not change filesystem format, repair it, write outside that
directory, or rely only on `/Volumes/<name>`. Never use the repository or its
parent `/Users/amada/Documents/Development/Crypto` as a production data root.
