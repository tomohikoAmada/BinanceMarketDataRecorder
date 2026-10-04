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

Latest owner authorization October4: complete Q5 closeout, implement the updated
Q6 optimization/release plan, then complete optimized-artifact VPS Q7/Formal2h;
stop before Q8/12h,24h,48h. Monitoring gate is **at least99.9% recorded PASS checks**,
per ADR-0039; this supersedes the previous ordinary-network-error single-check veto.
It is not a data-completeness claim or a waiver of strict endpoints/core/Raw/archive
integrity/both complete LIVE audits/completed verify/independent restored closeout.
Q5's sole43200.002463724s target and1435/1436 monitors=99.9303621% satisfy duration
and new monitoring gates; Q5 CLOSEOUT_PENDING, not COMPLETE/accepted12h.
Original BLOCK and old-policy independent rejection remain historical evidence.
Recorder inactiveANDdisabled; normal archive timer paused for actual audits,
worker naturally idle. No newT0 or capture restart. Audit-only leaf
closeout-monitor999-n51_b54j actually ARMED15:04:39.252568666UTC with the SAME
20:35:45UTC cutoff (Oct5 04:35:45Shanghai), no extension. Fresh19451-chunk census,
12.404629488GiB reserve and19865.747431334s remaining admission independently ACCEPT.
Native terminal service started15:05:15UTC, PID117119/inva8342d75…628dd,
Typeoneshot/TimeoutStartinfinity; source900s no-progress watchdog unchanged.
At15:20:31UTC its first full LIVE pass was advancing (3 audit progress records,
546299121 bytes processed), no terminal/final/completed verify yet. Do not duplicate it.
Q6/Q7 NOT_STARTED until full Q5 closeout. Existing vps-2h ACTIVE, next actual quota
reset+1minute17:06:38UTC /Oct5 01:06:38Shanghai; checkpoint-based single wakes.
The following checkpoints retain their historical rules/results, not current authority.

Latest owner authorizes Q5 Formal12h through full terminal/verification/independent
restored closeout; stop before24h. Q5 RUNNING, soleT0Oct4 09:53:49Shanghai,
target21:53:49Shanghai/stage12h-1c68e5eaf9724cfbb26e4f5c904786f6. Fresh6718-chunk
forecast/helpers/actual67500s arm/pinned publication-aware prestart/coreREADY/strict26
PASS/ACCEPT. Native observer+sampler healthy; do not restart/createT0. Finite expiry
Oct5 04:35:45Shanghai, no extension. Accepted2h only until actual12h target/full
terminal/verification/independent restored closeout. Two no-T0 prep failures retained. Exact
Q5 scope/checkpoint: docs/milestone_acceptance/Q5-formal12h-20261004.md.
Latest owner planning update inserts Q6 optimization/release preparation AFTER
complete Q5 closeout, then new optimized-artifact VPS Q7/2h, Q8/12h, Q9/24h,
Q10/48h. This documentation request does not start those jobs or alter activeQ5.
The new artifact earns its own86h/309600s; no current/old duration transfers.
Q6 must implement/review explicit48h/new-chain support (current CLI lacks48h),
preserving historical72h/168h verification/resume semantics. Exact scope and
performance/safety/release gates: docs/milestone_plan.md, Q6–Q10.
Completed corrected-source Q4, October4,
2026 (Shanghai): source23ea5c4/wheelbb7e357e…52a0/identity0ea93b2c…fbee own
VPS Formal2h7200.001928737s, both full LIVE terminal audits, completed verification,
independent GPT-6.1 Sol xhigh LOCAL/OFFLINE eligibility and actual restored-handoff
ACCEPT. Final64889189…b00c2/terminald7b460fb…eb5574/stage2h-e77218f9d1fa4c78a8a48ce8bafdaf27.
Exact CI37130186751 both platforms/local1998/cloud233/clean locked wheel/actual
canonical deployment VERIFY and corrected engineering/warm-up/authoritative
baseline gates independently PASS/ACCEPT. All14 flags/four products/growing corpus
and previous cf3909e9 genuine PASS/custody remain retained; no old credit transfer.
Evidence root `/srv/recorder-data/recorder-archive/evidence/Q4-spot-idle-20261003-Xo1tH7WN`.
Full terminal114303 records/241 shards/6718 archived chunks/10151154 frames,
actual50m05.516s/CPU2899.089s/cgroup1182.3MiB/swap0. Independent guarded
reconstruction36.722126s/Raw+network+productionopens0. Strict endpoints/239 in-period
auxiliary samples PASS. Retain current three typed-empty retries/two missed REST
snapshots (~5s recovery), historical1269 gap-incomplete/64 zero-frame manifests,
and30s sampling limits; no every-event/continuous completeness claim.

Actual controlled OS RESTORED October3,22:00:16.698841510UTC BEFORE expiry
cancellation22:00:53.894653775UTC/readbackinactive, no23:31:19 window extension.
Five approved runtime masks removed/original three units enabled-active; archive
actual recurrence/four natural successful workers/finite future deadline PASS.
Handoff22:04:51.880189757UTC RecorderinactiveANDdisabled/MainPID0/NRestarts0,
same verified config/identity/boot; Catalogok/0partials/backlog/all6718LOCAL_DELETED.
Q0–Q4 COMPLETE/current corrected accepted2h; Q5 RUNNING/Q6–Q10 NOT_STARTED/PRODUCTION_READY=NO.
Existing hourly `vps-2h` reactivated for12h; completion requires pausing it after
full Q5 closeout push. Never duplicate an already advancing native stage.
Historical tasks stay paused. Latest acceptance/review are in
`docs/milestone_acceptance/Q4-formal2h-spot-idle-20261004.md` and current handoff.

Build a stateful Python3.12 recorder for Binance public market data. Production
is Ubuntu24.04 x86_64/non-root systemd; macOS Apple Silicon is development/local,
Ubuntu ARM64/RK3588 has separate evidence. Configurable Spot/USD-M products,
immutable Raw, recovery/gaps, verified archive, normalization, replay and V5
acceptance are implemented. Keep the existing one-process/per-ProductKey
Collector architecture, SQLite, and published consumer contracts.

Later documentation commits are not replacement wheels. Pre-start/target
auxiliary gate is strict. During observations only typed EmptySideDataResponse
retry may recover within unchanged actual-success/correct-cursor freshness bounds;
original error/status stays visible. Generic/Catalog/terminal/WS failures at
observed checks stay blocked. No cursor advance or complete-period claim for
missing data. Historical ADR-0038 startup and private-array/CI corrections remain
accepted and documented; old evidence is not current operational authority.

Only local/remote main remains; preserve unrelated stash/untracked bundles and
historical worktrees. Old metadata/archive, failed attempts and unpublished143362
scope remain retained; no corpus reset/unique physical deletion/unchanged failed
live rerun. Current plan finishes Q5, then Q6 optimization and NEW optimized-artifact
2h+12h+24h+48h=86 accepted target hours, with audits outside credit. This supersedes
the old unstarted24h/38h continuation. R-078 and prior38h peak reserve failure
remain OPEN. Before later stages
reforecast actual reserve/temp/staging/backlog/controls and practical full-audit
windows from the larger current measured scope. No speculative hardware/stack
change. F4 bounded merge is evaluated in Q6; portable archive rollout stays
separate. Economical read-only
monitoring cannot invent a T0 or restart Recorder; healthy native tests survive
model quota interruptions.

Before current-state or milestone work, read `docs/PROJECT_HANDOFF.md` and
`docs/CURRENT_PRODUCTION_STATE.md`. Implementation map: `docs/developer_guide.md`;
project scope: `docs/project_contract.md`; gates: `docs/milestone_plan.md`.

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
  Preserve both independent full LIVE acceptance audit passes; completed proof
  replay and ordinary bounded delta observations retain their existing scope.
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
