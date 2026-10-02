# Binance Market Data Recorder Agent Contract

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

## Project goal and current checkpoint

Build a stateful Python 3.12 recorder for Binance public market data. The
production target is Ubuntu 24.04 x86_64 with a non-root systemd service;
macOS Apple Silicon is the development/local profile, and Ubuntu ARM64/RK3588
has separate validation evidence. Configurable Spot/USD-M products, immutable
Raw, recovery/gaps, verified archive, normalization, replay, and V5 acceptance
are implemented.

Only local/remote main remains; unrelated stash/untracked bundles and historical
worktrees stay retained. Installed frozen4e1cf32/treeafd041f6 implements reviewed
ADR-0038 startup boundaries/durable forced flags after605bc16's sparse-startup
nonformal failure. Independent aggregate/source/helper and test-only Profile D
supplement ACCEPT; exact CI37031972497 bothPASS, cloud266/clean locked wheel PASS,
actual canonical deployment VERIFIED. Wheel2905d30e…; identity1be38a7d….
Later documentation commits are not replacement wheels. Pre-start/target auxiliary
gate is strict. During observations only typed EmptySideDataResponse retry may
recover within unchanged actual-success/correct-cursor freshness bounds; original
error/status stays visible. Generic/Catalog/terminal/WS failures stay blocked.
No cursor advance or complete-period claim for missing data.

The owner resumes through reviewed eligible Formal2h, then stop before12h.
All14 flags/four Spot/USD-M BTCUSDT/ETHUSDT products and growing corpus remain.
Boot59bb1735-ac48-408d-b3ec-bda79fc49b41; Recorder inactive AND disabled,
MainPID0/Resultsuccess/NRestarts0,1182 chunks/transactions LOCAL_DELETED,
Catalogok/0partials/backlog. Archive actual finite recurrence passed after deploy,
now timer/worker paused solely for stopped engineering baseline; normal OS
updates restored, no live monitor. Prior605bc16 engineering baseline/both LIVE/
verify passed697; failed catch-up preserved. New exact4e engineering attempt2
FAILED before publication on real discontinuity-array serialization. Retain failed
controls and do not resume their prefix under a changed identity. Minimal local
private-index correction is independently ACCEPTed; replacement release/fresh baseline remain;
see docs/milestone_acceptance/Q3-discontinuity-array-correction.md. Next actual warm-up/convergence,
resources/cumulative forecasts, authoritative post-warm-up baseline under a finite
quiet window, strict readiness, Formal2h plus terminal/completed/independent
verification. No Formal T0/credit;12H_STARTED=NO;PRODUCTION_READY=NO.

Old metadata/archive, failed attempts and unpublished143362-chunk baseline
remain retained; R-078 stays OPEN. Do not reset the new corpus, physically delete
unique evidence, or repeat an unchanged failed live run. CurrentQ0–Q6 plan is
2h+12h+24h=38 accepted hours, with stage audits outside credited duration.
Q1/Q2 gates complete; F4/portable archive follow-ups remain separate. Economical
read-only monitoring is authorized; it cannot invent a T0 or restart Recorder.
Historical automation remains paused.
New owner-authorized current-chat continuation vps-2h wakes at actual five-hour
quota reset+60s, then updates its next dated schedule. If work is already advancing,
only reschedule and exit; do not duplicate active work/T0. Pause after reviewed2h.

Before current-state or milestone work, read `docs/PROJECT_HANDOFF.md` and
`docs/CURRENT_PRODUCTION_STATE.md`. The implementation map is
`docs/developer_guide.md`; project scope is `docs/project_contract.md`; delivery
and acceptance gates are in `docs/milestone_plan.md`. Historical evidence retains
its time-local status and is not current operational authority.

## Non-goals

- No current GUI, web frontend, FastAPI product API, or trading interface. A
  future Web UI is separately authorized and must not be forced into Recorder
  core.
- No factors, Alpha DSL, strategies, positions, backtests, account ledger, or
  live/simulated execution.
- No account endpoints, orders, API keys, secrets, or credential discovery.
- No other exchanges, automatic all-symbol discovery, or exchange/plugin
  framework. Product selection follows ADR-0032 and becomes effective only on
  restart.
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
another exchange requires a separate future architecture review. The
operator-configured product-set authority is ADR-0032; its ProductKey is
`(market, symbol)` and its runtime topology is one process with one Collector
per configured ProductKey.

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
   Delete the source only after all local/remote authorization steps succeed.
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
