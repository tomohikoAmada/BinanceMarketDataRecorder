# MS4 qualification acceptance ledger

## Current MS4-D final closure — reviewed complete (2026-09-11)

MS4-D reviewed the exact implementation and the bounded MS4-C/R3 evidence
below. The result is a closed, non-formal, four-ProductKey qualification
record. It does not certify every possible symbol, long-duration operation,
Formal M22.9, external-media production archiving, source retirement, or
Production Ready.

```text
REVIEW_BASE_MAIN_SHA=e11d5cbdf861ab82bb110ead8e98a1f9498f3c55
REVIEW_BASE_MAIN_TREE=9fcf3e4128706938ffd02ef5a6af80c558cb234b
IMPLEMENTER=LUNA_MAX
FINAL_REVIEWER=PRIMARY_AGENT
PRODUCT_CODE_AND_GATES_DIFF_FROM_DEPLOYED_SOURCE=NONE
MS4_D_REVIEW_BASE_PR=61
MS4_D_REVIEW_BASE_PR_COMMIT_SHA=013e20d6b911fde2f443aa6c855039599483ef7d
MS4_D_REVIEW_BASE_PR_COMMIT_TREE=9fcf3e4128706938ffd02ef5a6af80c558cb234b
MS4_D_REVIEW_BASE_CI_RUN=34551834444
MS4_D_REVIEW_BASE_CI_STATUS=COMPLETED_SUCCESS
DEPLOYED_SOURCE_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
DEPLOYED_SOURCE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
WHEEL_SHA256=cfce08f747bf53372e4619d37bdfdbab9a6b3bd39c7f09337ddf86c9286b5602
LOCK_SHA256=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335
CONFIG_SHA256=4dbbb6bf415b209635857df6cf537478b03f262b2c3a88553ac77bb9c1c0d781
SYSTEMD_UNIT_SHA256=d5afc4c2228a78f02ffd7be07775e7c53acda90b8c2b1b3581d64020537188b6
DEPLOYMENT_IDENTITY_SHA256=e925733f9e0388b705ffcad40664bdb049ba786f4b170462275891f838cd41e5
EVIDENCE_ROOT=/Users/amada/Downloads/BinanceMarketDataRecorder-MS4-C-20260910
STOPPED_AUDIT_PATH=vps-evidence/stopped-audit.json
RECOVERY_R3_AUDIT_PATH=recovery-r3-evidence/recovery-audit.json
ARCHIVE_RECEIVE_RESULT_PATH=evidence/receive-result.json
STOPPED_AUDIT_SHA256=0e14518202cfa4039f4275fe3ef230752b26ac9a9a146a177b6dab60aafba0d9
RECOVERY_R3_AUDIT_SHA256=be69a8bc3e88f78ded017021b85d2e7978ea3d2d42d9bdcf8b9974e38e204f7e
ARCHIVE_RECEIVE_RESULT_SHA256=41f47ca9f3d672f6bd085d6036f7f5e8a3e0fa0256b24f92c6f1f98029e27de7
PRODUCT_KEYS=(spot,BTCUSDT),(spot,ETHUSDT),(um_perpetual,BTCUSDT),(um_perpetual,ETHUSDT)
AUXILIARY_KINDS=none
PROXY_MODE=direct
MS4_A=REVIEWED_COMPLETE
MS4_B=REVIEWED_COMPLETE
MS4_C=REVIEWED_COMPLETE
RECOVERY_GATE=REVIEWED_COMPLETE
MS4_D=REVIEWED_COMPLETE
MS4=REVIEWED_COMPLETE
MS4_D_REVIEW_CONCLUSION=ALL_11_MAIN_GATES_PASS
BOUNDED_MULTI_SYMBOL_QUALIFICATION=PASS
QUALIFICATION_SCOPE=NONFORMAL_BOUNDED_FOUR_PRODUCT_CORE
FORMAL_M22_9=NOT_STARTED
FORMAL_M22_9_CREDIT_SECONDS=0
PRODUCTION_READY=NO
CURRENT_MAIN_DEPLOYED=NO
RECORDER=STOPPED
ACTUAL_ROLLBACK_EXECUTION=NOT RUN
QUEUE_DEPTH=UNAVAILABLE
ARCHIVE_SCOPE=MAC_INTERNAL_APFS_RECEIVER_ONLY_TEST_PATH
FINAL_READ_ONLY_VPS_CHECK_AT=2026-09-11T06:39:23Z
FINAL_READ_ONLY_VPS_CHECK=PASS
FINAL_RECORDER_STATE=inactive/dead
FINAL_RECORDER_MAINPID=0
FINAL_RECORDER_RESULT=success
FINAL_RECORDER_NRESTARTS=0
FINAL_ARCHIVE_TIMER_STATE=disabled/inactive
ACTIVE_WRITER_ROOT=/var/lib/binance-market-data-recorder
ARCHIVE_DISK_DEVICE=/dev/vdb1
ARCHIVE_DISK_FSTYPE=ext4
ARCHIVE_DISK_MOUNT=/srv/recorder-data
ARCHIVE_DISK_TOTAL_BYTES=2163348520960
ARCHIVE_DISK_USED_BYTES=19120271360
ARCHIVE_DISK_AVAILABLE_BYTES=2122221260800
ARCHIVE_DISK_USE_PERCENT=1%
ARCHIVE_DISK_ROLE=MOUNTED_ARCHIVE_TARGET_NOT_ACTIVE_WRITER_ROOT
NEXT=FORMAL_M22_9_PREPARATION_REQUIRES_SEPARATE_AUTHORIZATION
```

### MS4-D gate ledger

Each main gate uses exactly one of the acceptance states `PASS`, `FAIL`,
`NOT RUN` or `BLOCKED`. Scope limits are recorded in the reason; they do not
create a fifth gate state.

| Gate | State | Direct evidence and bounded limitation |
| --- | --- | --- |
| IDENTITY | `PASS` | The deployed source/tree and Wheel, lock, config, unit and deployment-identity hashes above matched at MS4-B, the original C window and R3 checks. |
| TOPOLOGY | `PASS` | One process owned exactly four configured ProductKeys, 12 core stream contexts and four REST snapshot contexts; no extra product or automatic discovery was used. Auxiliary data was disabled. |
| READINESS | `PASS` | All four ProductKeys were ready in the original T0 and 121 observations; R3 retained all four ready and receiving for 15 post-fault observations. This is not long-duration evidence. |
| RAW_MANIFEST_CATALOG | `PASS` | The stopped audit recorded 1,637 SEALED transitions across all core and snapshot contexts with valid manifests/Catalog state; R3 added 23 validated chunks across 12 core contexts. Active, partial, malformed, degraded and unclosed counts were zero. |
| CONTINUITY_RECOVERY | `PASS` | An exact MainPID-owned socket disconnect produced one explicit `sequence_gap` for `um_perpetual:BTCUSDT/book_ticker`, a new connection and recovery in `0.436024210` seconds. The original C window remains recovery `NOT RUN`; historical continuity is not claimed restored. |
| SHARED_REST | `PASS` | Accepted exact-source offline traces and the bounded live run showed no cooldown bypass or duplicate global owner. Auxiliary live side-data and deliberate live 418/429 behavior were not covered. |
| ARCHIVE | `PASS` | One authorized receiver-only receive/readback/hash/manifest/receipt cycle passed for the internal APFS MacBook Downloads test path. It is not external-media or production archive certification; no formal Catalog snapshot or source retirement was performed. |
| CPU_RSS_QUEUE_BACKPRESSURE | `PASS` | Bounded RSS, cgroup memory and CPU samples showed no OOM, restart or backpressure symptoms. Queue depth was unavailable, so no long-run queue trend is claimed. |
| CAPACITY_RUNWAY | `PASS` | Minimum free bytes were `44328247296`, hard-reserve violations were zero, and the last ETA was approximately 25h versus the 3h bounded envelope. The final read-only check at `2026-09-11T06:39:23Z` also observed `/dev/vdb1` (`ext4`) mounted at `/srv/recorder-data`, with `2163348520960` total bytes, `19120271360` used, `2122221260800` available and `1%` used. This is a mounted archive target, not the active writer root; shared-host attribution and Formal runway remain unproven. |
| SHUTDOWN | `PASS` | Final read-only VPS check at `2026-09-11T06:39:23Z` confirmed systemd `inactive/dead`, `MainPID=0`, `Result=success`, `NRestarts=0`, and archive timer `disabled/inactive`; no start, mutation or live traffic occurred. The active writer root remains `/var/lib/binance-market-data-recorder`. |
| ROLLBACK_COMPATIBILITY | `PASS` | MS4-B compatibility preflight was verified for the preserved identity. `ACTUAL_ROLLBACK_EXECUTION=NOT RUN`; no claim is made that an artifact rollback was exercised. |

The original 2026-09-10 MS4-C window remains
`EXECUTED_PARTIAL_NOT_ACCEPTED`; the independent 2026-09-11 R3 supplement
closes the recovery gate only and does not transfer duration credit. The
receiver-only archive result is a selected test-path result, not production
archive certification. These limitations are non-formal scope boundaries for
this closure, and remain open risks for later work.

### MS4-D documentation checks

This milestone changes documentation only. The permitted checks are
`git diff --check`, a scoped status/claim `rg` review, and `git status`; pytest,
Ruff, MyPy, build, online smoke, and Formal M22.9 observation were not run
because they are outside this documentation-only closure. A final read-only VPS
status/identity/disk check at `2026-09-11T06:39:23Z` was `PASS`; it observed
the Recorder stopped and the archive target mounted, with no start, mutation or
live traffic.

## Historical MS4-C disposition — reviewed complete; original window partial

The owner-authorized MS4-C attempt on 2026-09-10 reached the bounded steady
interval after all four configured ProductKeys became ready. Its original
window disposition remains `EXECUTED_PARTIAL_NOT_ACCEPTED` because controlled
recovery was not executed in that approved window; it is not retroactively
reclassified. The detailed run record is
[`MS4-C-20260910`](../milestone_evidence/MS4-C-20260910.md).

The separate 2026-09-11 R3 evidence is a passing
`NONFORMAL_MS4_RECOVERY_SUPPLEMENT`. Fresh official eligibility was recorded
at `2026-09-11T01:01:49Z`; MainPID `685913` was `READY` before the exact owned
socket was disconnected. The only durable recovery was
`um_perpetual:BTCUSDT/book_ticker`, completed in `0.436024210` seconds with a
new connection. Fifteen post-fault observations kept all four ProductKeys
ready with receive progress; 12 core contexts passed sealed validation
(`validated_chunks=23`); Catalog was clean with no malformed, degraded,
unclosed or active chunks; no `.partial` file remained; and final systemd was
`inactive/dead`, MainPID `0`, Result `success`. The supplement is
`PASS`, closes the MS4-C recovery gate for review, and grants zero Formal
M22.9 duration credit. The gap remains explicitly recorded and historical
continuity is not claimed restored.

The Tokyo Recorder was then stopped cleanly and remains stopped. One
receiver-only SSH archive cycle to the owner-authorized internal APFS MacBook
Downloads test target was verified after the stop. The archive timer is
explicitly disabled. It created no remote pending authority, did not retire
the VPS source, and has no formal receipt-bound Catalog snapshot. The status
block below is the time-local pre-MS4-D snapshot; the current closure is
recorded above. It does not authorize Formal M22.9.

```text
MS4_A=REVIEWED_COMPLETE
MS4_B=REVIEWED_COMPLETE
MS4_C=REVIEWED_COMPLETE
RECOVERY_GATE=REVIEWED_COMPLETE
MS4=INCOMPLETE
MS4_D=NOT_STARTED
NEXT=MS4_D_REVIEW_STAGE_CLOSURE
```

## Historical MS4-B disposition — target preflight and stopped deployment

The Tokyo target preflight, old-deployment rollback evidence, exact frozen
Linux artifact, complete four-ProductKey configuration, stopped systemd
installation, and deployment identity verification are reviewed complete for
the stopped boundary.
The Recorder was gracefully stopped before mutation and remains stopped. This
MS4-B boundary record does not include the later MS4-C attempt, controlled
recovery, source retirement or Formal M22.9. Its status block is time-local;
the current MS4-D closure is recorded at the top of this file.

```text
MS4_A=REVIEWED_COMPLETE
MS4_B=REVIEWED_COMPLETE
MS4_C=REVIEWED_COMPLETE
RECOVERY_GATE=REVIEWED_COMPLETE
MS4_D=NOT_STARTED
NEXT=MS4_D_REVIEW_STAGE_CLOSURE
```

## Reviewed MS4-B evidence ledger — 2026-09-10

MS4-B is **REVIEWED_COMPLETE** for its stopped boundary: target preflight,
rollback preservation, exact artifact installation, exact four-ProductKey
configuration, and deployment identity verification. This exit does not include
systemd start, readiness, live capture, controlled recovery, remote archive
receive, or any Formal M22.9 window. The review uses the owner-supplied Tokyo
VPS Luna-max execution summary plus Git authority and implementation/runbook
cross-checks. The private evidence bundle was not directly inspected in this
local worktree:
`PRIVATE_EVIDENCE_BUNDLE_DIRECT_INSPECTION=NOT_RUN_LOCAL`.

```text
EVIDENCE_RUN_ID=MS4-B-STOPPED-20260910T093753Z
HOST_ID_SHA256=0568e0cf8458b7686f7094b470fd8d361a4981f8781eb0a63ee63b17ce2d55e9
OS=Ubuntu 24.04.4 LTS
ARCH=x86_64
KERNEL=6.8.0-138-generic
PYTHON=3.12.3
SYSTEMD=255
SERVICE_USER=bmdr
SERVICE_GROUP=bmdr
DATA_ROOT=/var/lib/binance-market-data-recorder
ARTIFACT_ROOT=/opt/binance-market-data-recorder
WHEEL_PATH=/opt/binance-market-data-recorder/releases/303e073e25d5ed53d7cf6e26a9c6c6e879013b50/binance_market_data_recorder-0.1.0a1-py3-none-any.whl
CONFIG_PATH=/etc/binance-market-data-recorder/recorder.toml
UNIT_PATH=/etc/systemd/system/binance-market-data-recorder.service
DEPLOYMENT_IDENTITY_PATH=/etc/binance-market-data-recorder/deployment-identity.json
REVIEWED_SOURCE_GIT_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
REVIEWED_SOURCE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
WHEEL_SHA256=cfce08f747bf53372e4619d37bdfdbab9a6b3bd39c7f09337ddf86c9286b5602
LOCK_SHA256=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335
CONFIG_SHA256=4dbbb6bf415b209635857df6cf537478b03f262b2c3a88553ac77bb9c1c0d781
SYSTEMD_UNIT_SHA256=d5afc4c2228a78f02ffd7be07775e7c53acda90b8c2b1b3581d64020537188b6
DEPLOYMENT_IDENTITY_SHA256=e925733f9e0388b705ffcad40664bdb049ba786f4b170462275891f838cd41e5
PRODUCT_KEYS=(spot,BTCUSDT),(spot,ETHUSDT),(um_perpetual,BTCUSDT),(um_perpetual,ETHUSDT)
CORE_STREAM_CONTEXTS=12
AUXILIARY_KINDS=all_disabled
PROXY_MODE=direct
DEPLOYMENT_VERIFY=VERIFIED
ROLLBACK_VERIFY=VERIFIED
LEGACY_RECONNECT_PREFLIGHT=398_candidates; ambiguous/conflict/contradiction/degraded=0
SYSTEMD_FINAL=enabled=true; ActiveState=inactive; SubState=dead; MainPID=0
RAW_CATALOG_DELETION=NONE
BINANCE_LIVE_CAPTURE_STARTED=NO
RECORDER_STARTED=NO
CONTROLLED_RECOVERY_RUN=NO
FORMAL_M22_9_STARTED=NO
F0_BYTES=44766736384
F1_BYTES=44766720000
MEASURED_DT_SECONDS=60.000321759
SHORT_SHARED_HOST_G_NET_BYTES_PER_SECOND=273.065202313559
SHORT_SHARED_HOST_T_RUNWAY_SECONDS=124619693.288
CONSERVATIVE_24H_RATE_BYTES_PER_SECOND=107636.550405
CONSERVATIVE_RUNWAY_HOURS=87.819461
MS4_3H_PREFLIGHT=PASS_PREFLIGHT_ONLY
ARCHIVE_TARGET=ARCHIVE_TARGET_BLOCKED_OPERATOR_INPUT
ETHUSDT_OFFICIAL_ELIGIBILITY=NOT_CAPTURED
OLD_SOURCE_SHA=c421605e302d2ad46acdb2466627f64644181c9a
OLD_WHEEL_SHA256=278ee0b0df1e7766e205684ad1e401b12fb98341296164edc1c0de9b6d58c9c6
OLD_CONFIG_SHA256=5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb
OLD_UNIT_SHA256=d5afc4c2228a78f02ffd7be07775e7c53acda90b8c2b1b3581d64020537188b6
OLD_IDENTITY_SHA256=bdda546432bcaf6d29f281cd2a281b4d684bc447b2fafa382ffd1948f39a107f
CATALOG_COMPATIBILITY=REMOTE_DELETE_PENDING,REMOTE_DELETED
```

The short capacity sample is shared-host evidence and is not attributed to the
Recorder. The conservative 24-hour rate is used for planning; neither result
authorizes live T0 or Formal M22.9. The registered same-host external target is
not a confirmed cross-machine archive authority. Before MS4-C, the owner must
provide the archive machine/SSH/destination/workflow authority, capture
ETHUSDT eligibility from an allowed official source immediately before start,
and issue explicit start authorization. The Recorder remains stopped and
`PRODUCTION_READY=NO`.

## Historical MS4-A — local preparation acceptance ledger (time-local snapshot; superseded)

### 1. Frozen representative profile

The reviewable selection fragment is
[`docs/runbooks/MS4_candidate_profile.toml.example`](../runbooks/MS4_candidate_profile.toml.example).
It uses explicit product-selection mode:

| Market | Symbols | Exact ProductKeys |
| --- | --- | --- |
| Spot | `BTCUSDT`, `ETHUSDT` | `(spot, BTCUSDT)`, `(spot, ETHUSDT)` |
| USD-M perpetual | `BTCUSDT`, `ETHUSDT` | `(um_perpetual, BTCUSDT)`, `(um_perpetual, ETHUSDT)` |

The exact expected set is therefore:

```text
{(spot, BTCUSDT), (spot, ETHUSDT),
 (um_perpetual, BTCUSDT), (um_perpetual, ETHUSDT)}
```

Every ProductKey owns the existing `diff_depth`, `agg_trade` and
`book_ticker` streams: 4 products × 3 streams = 12 core stream contexts.
The fragment explicitly disables all current auxiliary kinds, including the
USD-M global `funding_info` and `exchange_info` owner and Spot exchange-info
side data. This keeps the initial qualification sample's auxiliary workload
at zero; enabling any side kind later requires a new complete config hash and
review. `ETHUSDT` is a proposed sample symbol, not a supported-symbol
allowlist. Official eligibility and provenance must be checked immediately
before any live execution; no official source was consulted in this local run.

### 2. Artifact and identity ledger

The following is the time-local MS4-A snapshot, retained for provenance. It was
written before the target work and is superseded by the reviewed MS4-B ledger
above. The artifact was staged outside the repository and is not committed.

| Item | Local evidence |
| --- | --- |
| Wheel | `/var/tmp/bmdr-ms4a-artifacts.XyEK3q/binance_market_data_recorder-0.1.0a1-py3-none-any.whl` |
| Wheel SHA-256 | `0a75f59fd9f050b37542d4673db00593b998c581154a3ec06d21d0900c0516d7` |
| sdist | `/var/tmp/bmdr-ms4a-artifacts.XyEK3q/binance_market_data_recorder-0.1.0a1.tar.gz` |
| sdist SHA-256 | `7757f5fbac049602e865e5e0ef4bf088265ec794ef41fea0513b2e219fa5ed64` |
| Package version | `0.1.0a1` |
| Build interpreter | `/Users/amada/miniforge3/bin/python`, CPython `3.12.9`, Darwin arm64 |
| Build tooling | `build 1.3.0`; wheel metadata generated by setuptools `81.0.0` |
| Wheel metadata | `Root-Is-Purelib: true`, `Tag: py3-none-any` |
| Runtime Linux lock | `requirements/linux-x86_64-python312.lock`, SHA-256 `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| CI tool lock | `requirements/ci-linux-python312.lock`, SHA-256 `656fdae154982ab2095376059d23ed7ca45367a65fd5567ce396358b5efae9f2` |
| Profile fragment | `docs/runbooks/MS4_candidate_profile.toml.example`, SHA-256 `ec945b2dc34c8fb1bfed8406f030ba8e68dd1bbd4847fc093303bd22ee3b9149` |
| Complete deployment config | **HISTORICAL MS4-A SNAPSHOT: NOT FROZEN** — superseded by reviewed MS4-B config SHA above |
| systemd unit | **HISTORICAL MS4-A SNAPSHOT: NOT FROZEN** — superseded by reviewed MS4-B unit SHA above |
| deployment-identity.v1 | **HISTORICAL MS4-A SNAPSHOT: NOT GENERATED** — superseded by reviewed MS4-B identity SHA above |

The profile fragment hash is not a deployment `config_sha256`. A final
identity must hash the complete root-controlled config, the installed unit,
the retained Wheel and lock, and the observed systemd properties through the
existing `deployment identity-create` path.

The implementation-enforced `vps-production-v1` paths are documented by the
existing service code and operations guide:

```text
artifact root: /opt/binance-market-data-recorder/
venv:         /opt/binance-market-data-recorder/venv/bin/python
config:       /etc/binance-market-data-recorder/recorder.toml
data root:    /var/lib/binance-market-data-recorder/
unit:         /etc/systemd/system/binance-market-data-recorder.service
```

These are code-enforced canonical paths, not evidence that a particular host
has them or that the owner has selected them. The operator must confirm the
actual target and service principal before MS4-B. No service user, group,
archive destination, hostname or VPS identity was guessed here.

### 3. Existing coverage map

MS4-A reuses the accepted MS2/MS3 behavior evidence and current commands. It
does not add a scheduler, benchmark framework, Contract/Projection change or
new integration model.

| MS4 concern | Existing implementation/checks | Use in MS4-C |
| --- | --- | --- |
| Config and exact ProductKeys | `tests/unit/test_ms2_configurable_products.py`, `tests/unit/test_config.py`, `binance-market-recorder config show` | Parse the final config independently and compare exact four-key set |
| Product readiness | `tests/unit/test_collector_readiness.py`, `tests/unit/test_ms2_configurable_products.py`, `tests/unit/test_vps_service_readiness.py`, `deployment readiness` | Require all configured products ready; no runtime boolean-only claim |
| Shared USD-M authority | `tests/unit/test_usdm_shared_rest_gate.py`, `tests/integration/test_ms3b_production_paths.py` | Reuse offline proof; inspect live identity/topology only |
| Concurrent products/backpressure/rotation | `tests/integration/test_ms3b_multi_product_load.py`, `tests/integration/test_usdm_ingress_backpressure.py`, `tests/unit/test_ms3a_product_rotation_attribution.py` | Observe configured profile; do not inflate 2-hour result into capacity certification |
| Raw, manifest and Catalog integrity | `tests/integration/test_catalog_and_seal.py`, `tests/fault_injection/test_recovery.py`, `tests/unit/test_raw_chunk_format.py`, `go run tools/verify_raw_chunk_golden.go` | Verify exact hashes, completeness/no-gap and Catalog closure |
| Archive lifecycle | `tests/integration/test_archive_transaction.py`, `tests/integration/test_recovery_archive_states.py`, `tests/fault_injection/test_archive_kill9.py`, `archive status/verify` | Run only with an operator-selected archive target |
| Capacity and reserve | `tests/unit/test_storage_forecast.py`, `tests/unit/test_vps_capacity_profile.py`, `tests/integration/test_disk_emergency.py`, `storage forecast` | Use measured free bytes and conservative net growth |
| Deployment identity and service | `tests/unit/test_deployment_identity.py`, `tests/unit/test_systemd_service.py`, `tests/unit/test_vps_service_readiness.py`, `systemd status`, `deployment verify` | Target-only after exact files and non-root principal exist |
| Offline contract/tool gates | `tests/verify_m0_contracts.py`, `python -m ruff check .`, `python -m mypy`, `python -m build --no-isolation` | Reuse exact-source CI; run only local preparation checks listed below |

Available operational interfaces are `config show`, `doctor`, `status`,
`report daily`, `recovery legacy-reconnect-preflight`, `normalize status/run`,
`storage list/inspect/register/status/forecast`, `archive
status/retry/drain/verify`, `systemd install/start/stop/restart/status`, and
`deployment identity-create/verify/readiness/rollback-check/acceptance`.
The qualification runbook uses only these existing interfaces plus `journalctl`.

### 4. Local preparation validation

Executed on Darwin arm64, with no Binance network access and no production
filesystem:

| Check | Result |
| --- | --- |
| `git diff --check` | PASS |
| `pip check` in existing local Python 3.12.9 environment | PASS — no broken requirements |
| `python -m build --no-isolation` to external staging directory | PASS — wheel and sdist built |
| Wheel metadata inspection | PASS — pure-Python wheel, Python `>=3.12,<3.13` |
| Clean temporary wheel install (`--no-index --no-deps`, system-site dependency smoke) | PASS |
| Installed wheel `--version` | PASS — `0.1.0a1`, git `303e073e25d5` |
| Installed wheel `config show` using the profile fragment | PASS — four configured products |
| Installed wheel `doctor` | PASS — `network_accessed=false`, `filesystem_mutated=false` |
| Installed wheel `status` | PASS — structured status returned |
| CLI parser/help inventory | PASS — current commands enumerated from `cli.py` |

The exact merged-code CI run `34436773366` is reused: macOS and Ubuntu passed
all required steps, including source/wheel build, Linux x86_64 lock install,
clean production dependency checks and clean-wheel smoke. No full suite or
static gate was rerun because MS4-A changes are documentation/config-fragment
only and the production code/tests are unchanged from that green exact-source
run.

Ubuntu x86_64 compatibility is therefore proven for the merged source and CI
clean-wheel path, but the locally built macOS artifact has not itself been
installed with the Linux x86_64 lock. `py3-none-any` alone is not sufficient
proof for its native/transitive dependencies. In this historical MS4-A
snapshot, the final artifact-specific Linux lock install and target service
identity remained **PENDING MS4-B in the MS4-A snapshot**; the reviewed MS4-B ledger above records
their subsequent completion.
The generic `doctor` x86_64 warning described in M22.7B remains informational;
the exact deployment identity/readiness gate is authoritative.

Not run: full pytest, focused MS3 suites, Ruff, MyPy, compileall, M0/Raw gates,
online smoke, stress/soak, Linux target installation, systemd mutation,
deployment identity generation, live readiness, archive transfer, VPS access,
and any acceptance duration. These are either already covered by unchanged
exact-source evidence or require the separately authorized target.

## 5. MS4-C qualification window and capacity method

The proposed MS4-C envelope is frozen for review, not yet approved for
execution:

```text
outer startup envelope: 15 minutes after explicit start request (absolute)
readiness observer bound: 300 seconds (bounded implementation interval)
steady state:           2 hours after all four ProductKeys are READY
recovery observation:   15 minutes, separately bounded and attributed
shutdown/verification:  15 minutes
planning margin:        15 minutes
minimum conservative runway envelope: 3 hours
```

The 300-second readiness observer interval is not an automatic extension of
the 15-minute outer stage deadline. Use existing systemd/status/service-state
read-only observation as appropriate, and require the final authoritative
deployment readiness result to return `READY` within the outer envelope.

There is no automatic extension, repetition, or `72h`/`168h` campaign. The
two-hour window cannot certify long-term RSS, rotation or formal M22.9.

For a measured interval, let `F0` and `F1` be free bytes at monotonic times
`t0` and `t1`, and let `R = 10 GiB` be the unchanged hard reserve. Use the
conservative non-negative net growth rate:

```text
g_net = max(0, (F0 - F1) / (t1 - t0))       bytes/second
T_runway = (F1 - R) / g_net                  seconds, when g_net > 0
T_runway = INSUFFICIENT_DATA                when g_net == 0 without history
```

Use the existing `storage forecast` history and multiple 1h/6h/24h/7d
windows when available; do not attribute free-space changes to Recorder when
co-resident processes may contribute. Treat archive throughput as zero until
the selected archive path has completed its own verified receive/readback/
manifest/receipt cycle. Before T0 require `F > R` and `T_runway` to exceed the
3-hour envelope plus any explicitly recorded operational margin. Preserve all
Raw and Catalog state if the reserve gate fails.

## 6. Evidence template

The following fields are required for the later MS4-C record. Blank,
`NOT RUN`, `BLOCKED` and `PENDING` values remain visible; they are never
converted to PASS by inference.

```text
RUN_ID=
HOST_ID / HOSTNAME=
HOST_OS_VERSION=
HOST_ARCH=
PYTHON_EXACT_VERSION=
SYSTEMD_VERSION=
SOURCE_GIT_SHA=303e073e25d5ed53d7cf6e26a9c6c6e879013b50
SOURCE_TREE=2b30a4dd2b8c694ac2e3abad88d6cb56a75badee
WHEEL_PATH=
WHEEL_SHA256=
LOCK_PATH=requirements/linux-x86_64-python312.lock
LOCK_SHA256=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335
COMPLETE_CONFIG_PATH=
CONFIG_SHA256=
SYSTEMD_UNIT_PATH=/etc/systemd/system/binance-market-data-recorder.service
SYSTEMD_UNIT_SHA256=
DEPLOYMENT_IDENTITY_PATH=
DEPLOYMENT_IDENTITY_SHA256=
SERVICE_USER=
SERVICE_GROUP=
DATA_ROOT=
ARCHIVE_STORAGE_ID=
ARCHIVE_DESTINATION=
PROXY_MODE=direct
PRODUCT_KEYS=(spot,BTCUSDT),(spot,ETHUSDT),(um_perpetual,BTCUSDT),(um_perpetual,ETHUSDT)
AUXILIARY_KINDS=none

IDENTITY=PASS|FAIL|NOT RUN|BLOCKED
TOPOLOGY=PASS|FAIL|NOT RUN|BLOCKED
READINESS=PASS|FAIL|NOT RUN|BLOCKED
RAW_MANIFEST_CATALOG=PASS|FAIL|NOT RUN|BLOCKED
CONTINUITY_RECOVERY=PASS|FAIL|NOT RUN|BLOCKED
SHARED_REST=PASS|FAIL|NOT RUN|BLOCKED
ARCHIVE=PASS|FAIL|NOT RUN|BLOCKED
CPU_RSS_QUEUE_BACKPRESSURE=PASS|FAIL|NOT RUN|BLOCKED
CAPACITY_RUNWAY=PASS|FAIL|NOT RUN|BLOCKED
SHUTDOWN=PASS|FAIL|NOT RUN|BLOCKED
ROLLBACK_COMPATIBILITY=PASS|FAIL|NOT RUN|BLOCKED
STAGE_RESULT=PASS_CANDIDATE|FAIL|INCOMPLETE|REVIEW_REQUIRED
BLOCKING_FINDINGS=
```

## Historical MS4-A operator inputs and exit disposition (superseded)

The following list is the time-local MS4-A input gate, retained for provenance;
it is superseded by the reviewed MS4-B evidence ledger above:

- exact target host, Ubuntu release, x86_64 architecture, Python executable,
  systemd version and co-resident-service boundary;
- pre-existing non-root `SERVICE_USER` and `SERVICE_GROUP`;
- confirmation of the implementation-enforced artifact/config/data/unit paths
  and ownership/mode policy;
- complete configuration file and SHA-256, including final product eligibility,
  auxiliary-kind decision, rotation, and direct proxy policy;
- selected archive machine, registered project directory, `storage_id`,
  destination path and receive/verify/receipt authorization;
- retained old deployment identity, old Wheel/lock/config/unit and a proven
  Catalog-compatible rollback root; a pre-MS1 binary is not an automatic target
  for the MS1+ Catalog;
- approved startup/steady/recovery/shutdown durations and the explicit
  MS4-B access/deployment authorization.

MS4-A local preparation is reviewed complete: the existing interfaces are mapped,
the exact four-key sample is recorded, a local artifact and hashes exist, the
runbook and evidence schema are reviewable, and no implementation blocker was
found. Target artifact compatibility, final identity, host/runway evidence,
archive verification, and live qualification remain pending because their
inputs and authorization are absent.

```text
MS4-A=REVIEWED_COMPLETE
HISTORICAL_MS4_B=NOT_AUTHORIZED
HISTORICAL_MS4_C=NOT_STARTED
MS4-D=NOT_STARTED
CURRENT_MAIN_DEPLOYED=NO
FORMAL_M22_9=NOT_STARTED
PRODUCTION_READY=NO
HISTORICAL_NEXT=OWNER_RESUME_REQUIRED
```

## Historical final preparation review and stage closeout

GPT-6 Astra reviewed the local preparation and corrected the retained lock
path, the absolute-path venv publication guidance, retained Wheel reference,
non-formal evidence boundary and remote/local archive command distinction.
The runbook is an approved local preparation reference; target-specific commands
remain explicitly gated before deployment. MS4-A completion does not mean the
whole MS4 milestone or deployment qualification is complete.

The historical owner-requested rest predates the current MS4-B target
preflight/stopped-deployment record above. Its statement that MS4-B/C/D were
unstarted and that no VPS access was authorized is retained as history, not as
current authority. The current record remains stopped and has not started
MS4-C.
