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
public market data. The primary future production profile is Ubuntu 24.04 LTS
x86_64 with systemd and a non-root service. macOS Apple Silicon remains a
development/local profile, and Ubuntu ARM64/RK3588 remains a distinct Linux
validation and historical evidence profile. The current implementation captures
configured Spot and USD-M perpetual depth at 100 ms, aggregate trades, book ticker
events, and public REST depth snapshots, followed by defined USD-M auxiliary
data. MS2 implements an operator-configured finite product set across those
two markets, with BTCUSDT/BTCUSDT compatibility only when both selection
fields are absent. MS3-B offline evidence is merged through PR #56 at
`303e073e25d5ed53d7cf6e26a9c6c6e879013b50`; MS3 is closed. MS4-B target
preflight and the exact four-ProductKey stopped deployment are reviewed
complete. The owner-authorized MS4-C attempt on 2026-09-10 reached its bounded
steady interval but remains `EXECUTED_PARTIAL_NOT_ACCEPTED` as an
original-window record because controlled recovery was not executed in that
approved window. The separate 2026-09-11 R3 run is a passing
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT`: it closes the MS4-C recovery gate for
review, with `formal_m22_9_credit_seconds=0`. The MS4-D review records the
bounded four-ProductKey qualification as `REVIEWED_COMPLETE` while preserving
the original partial-window disposition, zero Formal M22.9 credit, and
`PRODUCTION_READY=NO`; it does not authorize a new live run. The system keeps
recoverable immutable raw payloads,
deterministic replay metadata, explicit gap evidence, and verified archival
across the approved VPS/local Offline Workspace boundary.

The historical failed-at-T0 milestone status is
`M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE`; the
owner-authorized Formal 2-hour attempt on greencloud-tokyo-01 remains
`FORMAL_M22_9_2H=EXECUTED_FAILED_AT_T0`. It failed before
the first observer sample at T0
`2026-09-11T13:41:32.831837Z` / `1789134092831837314` ns, with BOOTTIME
`789457702764089` and boot ID
`f2720022-bc39-4e22-bc68-af6bfce92274`. The failed observer unit was
`binance-market-data-acceptance-2h-20260911T132845Z-5b4fd719.service`,
InvocationID `ee05ad7a73f04dec867629767439bc82`; it exited 1 before the
first sample. Stage-start failed with 24 blockers: 23
`catalog_manifest_disagreement:<chunk_id>` findings and one
`unexplained_raw_absence` finding. It receives zero duration credit.

The authoritative failed root is
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T132845Z-5b4fd719-646792f2`;
the stage root is its child
`2h-a8b45b7e1da640c497213b172c235815`. Its immutable stage-start SHA-256 is
`1c35f62e0673488c441374971e8e8552212e9693aacfba35820ea613ae4a08f6`.
The separate pre-T0 aborted setup root
`/srv/recorder-data/recorder-archive/acceptance/m22.9/formal-2h-20260911T130258Z-646792f2`
is preserved as an earlier zero-credit setup record and is not the formal
failed T0 root.

The focused quiescent forensic review found all 26 unique chunk IDs named by
the 24 stage-start blockers currently reconciled as
`AUTHORIZED_ARCHIVE_STATE_CURRENTLY_COMPLETE`: Catalog rows are
`LOCAL_DELETED`, internal manifests remain, internal sealed Raw is absent,
and verified archive Raw/manifests are present. All 26 local archive
transactions and source retirements occurred after T0. This supports, but does
not prove, a concurrent AcceptanceObserver filesystem/Catalog snapshot race;
there is no per-read interleaving trace. The full installed read-only post-stop
audit completed over 112,817 manifests in 115.95 seconds with zero Catalog
findings, zero integrity findings, and zero chunks with scan issues. The
compact review evidence and additive correction authority are under
`.../operator-evidence/closeout-review/`; their SHA-256 values are recorded in
`docs/milestone_acceptance/M22.9-2h.md`. The correction supersedes only the
original mistaken no-result statement and does not alter the failed-stage
disposition.

The final VPS state recorded by that historical closeout was
`RECORDER=STOPPED`, with systemd inactive/dead,
`MainPID=0`, `Result=success`, `NRestarts=0`, and no active `.partial` files.
`ARCHIVE_TIMER=ENABLED_ACTIVE` remains enabled and active/waiting; the verified
archive target is READY, backlog/pending/failed counts are zero, and Catalog
integrity is ok. The observer remained loaded failed for evidence. That
historical attempt's status is `FORMAL_M22_9=EXECUTED_FAILED_AT_T0`,
`FORMAL_M22_9_CREDIT_SECONDS=0`, `12H=NOT_STARTED`, and
`PRODUCTION_READY=NO`. At that checkpoint, the exact P2 source/review base
`646792f2e5fc5b7195ea58541d3f1dfda6555b7f` and deployment identity
`11029b9434f72fe48912659c050167e6a058e9827cd40401c9a852dff3c019cd` were
the installed artifact basis; the then-current engineering source containing
the reviewed fix was not deployed. The installed P2 artifact was not
retry-eligible; this was later superseded by the exact-artifact redeploy below.

The exact current main artifact is now installed and READY on
greencloud-tokyo-01. Source
`e267ae38bdbb206c8f54dcb5fa338b8f1c54c61d`, tree
`e968ede54d9f110ef7371a4847a54940177a1a19`, wheel SHA-256
`9bb924ad7cc38466d2b79c291f1b864890d5d071413dce1047eaf06d76532fdb`,
and deployment identity
`582bf645dea0c6ad2c409880d44a68b97a55ddbe0c9bfb68d930e12daa0d75a6`
verify. Recorder is `RUNNING_READY` for the four configured ProductKeys and 12
core stream contexts; the archive timer is enabled/active. No new Formal T0
has been created, Formal credit remains zero, and 12h is not started. Current
milestone status is
`EXACT_ARTIFACT_STOPPED_REDEPLOY_AND_READINESS=COMPLETE`;
`NEXT=FORMAL_M22_9_2H_RETRY_START`.
The exact code-review commit `31cabe4445ee699ad284aa707d24333c78cf8d21`
was independently reviewed against base
`e214120a25a5aff28fad4903c9510920a25738d3` with P0=0, P1=0, and P2=0.
That earlier review milestone status was
`ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_FIX=REVIEWED_COMPLETE`;
its time-local `NEXT` was `EXACT_ARTIFACT_REDEPLOY_PREFLIGHT`. That docs-only closeout was not a
deployment authorization; any redeploy or retry requires separate
authorization.

The preceding operational milestone was
`EXACT_ARTIFACT_REDEPLOY_PREFLIGHT`. The exact main candidate
`e267ae38bdbb206c8f54dcb5fa338b8f1c54c61d` (tree
`e968ede54d9f110ef7371a4847a54940177a1a19`) was staged only in a private,
root-controlled VPS evidence root. At that checkpoint the old P2 artifact was
still installed and Recorder was stopped. The prior Formal 2-hour failure
remained immutable with zero credit, no new Formal run had started, and
`PRODUCTION_READY=NO`. That preflight was not deployment or live-run
authorization; its time-local `NEXT` was
`EXACT_ARTIFACT_STOPPED_REDEPLOY_AND_READINESS`.

The P2 evidence bundle is `/srv/recorder-data/recorder-archive/evidence/M22.9-P2-20260911T090741Z`.
It verified the P2 deployment source/review base
`646792f2e5fc5b7195ea58541d3f1dfda6555b7f` (tree
`c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c`) in the installed deployment,
drained the registered archive target through the existing verified transaction
path, and completed the bounded non-formal live interaction before stopping
Recorder. `P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES` records exact identity
verification. At that historical checkpoint `CURRENT_MAIN_DEPLOYED=NO`; the
then-current engineering source was not installed. At P2 completion the
artifact was the Formal candidate; after the failed T0 it remained only the
installed evidence basis, until the later exact-artifact redeploy superseded
it.
The archive timer is
enabled and active/waiting; Recorder was stopped. The P2 review remains
`M22_9_P2=REVIEWED_COMPLETE`; the later Formal attempt is a separate
milestone record.
The historical MS4-D review base used main
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` (tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`) and includes the merged MS4-C
evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d`; base CI run `34551834444`
completed successfully. P1 was based on main at its historical checkpoint
`83a063f5bb9f91508238c9fd86d21aa45d1bd501`. Neither is a deployment
authorization.

P1 remains reviewed complete and keeps `AcceptanceObserver` as the sole
measurement/evidence authority for a stage. Its external systemd 255 transient
`Type=exec` procedure is separate from the P2 deployment. P2's archive timer
uses the fixed verified deployment and the registered target
`ef852751-721c-4145-9083-f6fd48718480` at
`/srv/recorder-data/recorder-archive`; source retirement occurred only after
the existing ArchiveManager/Catalog readback, hash, manifest, and transaction
checks. The active writer root remains `/var/lib/binance-market-data-recorder`;
the approximately 2 TB `/dev/vdb1` is an archive target, not an active writer
root. See `docs/milestone_acceptance/M22.9-P2.md` for the bounded P2 record.

The authoritative scope is `docs/project_contract.md`. Before current-state or
milestone work, read `docs/PROJECT_HANDOFF.md` and
`docs/CURRENT_PRODUCTION_STATE.md`; the delivery sequence and acceptance gates
are in `docs/milestone_plan.md`. Historical evidence files preserve their own
time-local status and must not be treated as current operational authority.

## Non-goals

- No current GUI, web frontend, FastAPI product API, or trading interface. A
  future Web UI is separately authorized and must not be forced into Recorder
  core.
- No factors, Alpha DSL, strategies, positions, backtests, account ledger, or
  live/simulated execution.
- No account endpoints, orders, API keys, secrets, or credential discovery.
- No other exchanges, automatic all-symbol discovery, or exchange/plugin
  framework. Product selection follows ADR-0032 and becomes effective only on
  restart; MS2 offline acceptance does not authorize deployment.
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
4. Ensure every commit contains only the current milestone. Normally create one
   local commit; when the human milestone instruction explicitly requires
   multiple logical commits (as in M19), keep those commits milestone-pure,
   verify a clean worktree, and stop.
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
