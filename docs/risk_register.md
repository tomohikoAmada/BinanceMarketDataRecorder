# Risk Register

## Current milestone risk checkpoint — M22_9 Formal 2-hour retry closeout (2026-09-12)

The exact-artifact Formal retry passed T0 and ten samples, then Ubuntu
`apt-daily-upgrade` upgraded glibc and Python 3.12, requested a systemd manager
reexecution, and externally restarted Recorder and the transient observer.
This broke the required process/service incarnation at approximately 85
minutes. The stage is incomplete, earns zero credit, and does not unlock 12
hours. Recorder is stopped; deployment, archive, Catalog, and partial-file
closeout checks pass.

R-071 is open. A later retry is ineligible until a bounded maintenance
quiet-window preflight handles pending package work and systemd service
reexecution while preserving operating-system security update authority. This
closeout does not implement that policy or create another T0.

```text
MILESTONE=M22_9_FORMAL_2H_RETRY_CLOSEOUT
MILESTONE_STATUS=REVIEWED_COMPLETE
FORMAL_M22_9_2H_RETRY=EXECUTED_INCOMPLETE_HOST_MAINTENANCE_INTERRUPTED
FORMAL_M22_9_CREDIT_SECONDS=0
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
12H=NOT_STARTED
PRODUCTION_READY=NO
NEXT=FORMAL_M22_9_HOST_MAINTENANCE_QUIET_WINDOW_PREFLIGHT
```

## Previous milestone risk checkpoint — M22_9 post-merge macOS CI reconnect-layout repair (2026-09-12)

The test-only repair addresses a schedule-sensitive false failure in the
session-restart reconnect-boundary coverage. GitHub main push offline-ci run
`34663566549` failed only macOS Python 3.12 job `103470852952` with
`1 failed, 1651 passed, 24 skipped, 4 deselected`; Ubuntu job `103470853142`
passed all gates. PR #68 exact-head run `34663191565` passed both platforms,
and twenty local exact Spot-node repetitions on macOS arm64 Python 3.12.14
passed. The legal ordinary-plus-`reconnect_gap` layout is already enforced by
the existing helper, which preserves exact original connection identity and
payload prefix. The equivalent USD-M assertion is repaired at the same time.

The repair changes no production behavior, Binance source semantics, public
contracts, dependencies, workflow, VPS state, or archive data. Independent lead
review found P0=0, P1=0, and P2=0; the production/test delta is independently
reviewed complete, while GitHub merge remains a separate operation. The helper
continues to fail closed on extra manifests, wrong lifecycle flags, wrong
connection identity, sequence-gap fabrication, and payload ordering. The
current status and unchanged deployment authority are:

```text
MILESTONE=M22_9_POST_MERGE_MACOS_CI_RECONNECT_LAYOUT_REPAIR
MILESTONE_STATUS=REVIEWED_COMPLETE
BASE_MAIN_SHA=bed826909e1a53089289981e9ea435c7ddbf07f0
BASE_MAIN_TREE=a85a39f5096f834a95bae11f9d047fac18602bc6
BRANCH=codex/m22-9-main-macos-ci-reconnect-layout
GITHUB_MAIN_PUSH_RUN=34663566549
MACOS_PYTHON312_JOB=103470852952
UBUNTU_PYTHON312_JOB=103470853142
PR_68_EXACT_HEAD_RUN=34663191565
VPS_TOUCHED=NO
CURRENT_MAIN_DEPLOYED=NO
RECORDER=STOPPED
ARCHIVE_TIMER=ENABLED_ACTIVE
FORMAL_M22_9=EXECUTED_FAILED_AT_T0
FORMAL_M22_9_CREDIT_SECONDS=0
12H=NOT_STARTED
PRODUCTION_READY=NO
NEXT=EXACT_ARTIFACT_REDEPLOY_PREFLIGHT
REDEPLOY_RETRY_AUTHORIZATION=SEPARATE_AUTHORIZATION
```

These stopped/active values are carried forward from the last authoritative
VPS record; no VPS or production archive action was part of this milestone.
After GitHub merge, exact-artifact redeploy preflight remains the next
gate, and any redeploy or Formal retry requires separate authorization.

## Previous local milestone risk checkpoint — AcceptanceObserver/archive concurrency fix reviewed complete (2026-09-12)

`ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_FIX=REVIEWED_COMPLETE`. The fix
freezes a short-lived Catalog lifecycle boundary before the long filesystem
scan, uses one authoritative membership comparison, defers rows committed
after that boundary, and re-reads fresh exact-chunk lifecycle state when
sealed Raw disappears. Existing ArchiveManager verification and fail-closed
archive semantics are unchanged. Deterministic offline tests cover the
post-boundary race, `LOCAL_DELETE_PENDING` unlink/validation windows,
unauthorized absence, external corruption, and observer read-only behavior.

The source is not deployed. The historical Formal 2-hour attempt remains
`EXECUTED_FAILED_AT_T0` with zero duration credit; `12H=NOT_STARTED` and
`PRODUCTION_READY=NO`. Independent Luna Max review of exact code-review
commit `31cabe4445ee699ad284aa707d24333c78cf8d21` against base
`e214120a25a5aff28fad4903c9510920a25738d3` found P0=0, P1=0, and P2=0.
The 112,817-manifest audit is historical performance evidence; terminal
`LOCAL_DELETED` uses durable transaction evidence, while full external
verification remains a separate stage safeguard. Any redeploy or Formal retry
requires separate authorization.

NEXT=EXACT_ARTIFACT_REDEPLOY_PREFLIGHT

## Historical Formal 2h failed-closeout risk checkpoint — 2026-09-11

At that historical checkpoint, `M22_9_2H_CLOSEOUT=REVIEWED_COMPLETE` was the
current milestone. The Formal
2-hour attempt remains `EXECUTED_FAILED_AT_T0`, with zero duration credit;
12 hours and all later stages are not started. Exact P2 deployment source/
review base `646792f2e5fc5b7195ea58541d3f1dfda6555b7f` (tree
`c7bcd5efbd9601e1dcef8c5e000435f2e0f82a6c`) is verified in the installed
deployment; Recorder is stopped and the managed archive timer is
`ENABLED_ACTIVE`. The registered archive target
`ef852751-721c-4145-9083-f6fd48718480` drained and passed full verification
with 112,570 verified files and zero failures/pending files.

The archive capacity gate passed for the bounded 278-hour projection: the
conservative existing 24-hour generation rate is `349910.017730 B/s`, leaving
approximately 1.772 TB of archive free-space margin after the projection. The
active `/dev/vda1` writer root remains the limiting risk: only about 26.46
hours of forecast runway remain above the 10 GiB hard reserve at that rate.
Archive margin does not substitute for active-root runway. Every Formal stage
start and end must recheck timer health, backlog, target space, and active-root
runway. The short live interaction is not a long-run generation proof.

`P2_EXACT_DEPLOYMENT_SOURCE_INSTALLED=YES` records exact identity verification;
after this docs-only merge, `CURRENT_MAIN_DEPLOYED=NO` because the descendant
is not installed. The failed stage-start reported 24 blockers across 26 unique
chunks. A post-stop exact reconciliation and full audit found zero persistent
Catalog/integrity findings and all 112,817 archived lifecycles authorized.
This supports, but does not prove, a concurrent observer/archive snapshot
race. The installed artifact is not retry-eligible until the scoped fix is
reviewed and a later exact artifact is separately authorized and deployed.
See [`M22.9-2h acceptance`](milestone_acceptance/M22.9-2h.md).

NEXT=ACCEPTANCE_OBSERVER_ARCHIVE_CONCURRENCY_DIAGNOSIS_FIX_OFFLINE_TEST

### Historical pre-P2 authority (preserved)

GitHub `main` at MS4-C review start was
`efae0135ed5272d18d800af0ac247b70ece07422` with tree
`53342ac880cc36d65eba6f5e9b49fa722cc9d56b`; its MS3-A merge parents were
`52bf086dd240556b054821f33bf1e2840fdcf912` and
`ad1e941af3cd2bd3922eac239ba92a47058e9875`. Historical MS4-C
behavior/deployment-source authority was `303e073e25d5ed53d7cf6e26a9c6c6e879013b50` with tree
`2b30a4dd2b8c694ac2e3abad88d6cb56a75badee`; the MS1 merge
`d38180074b5f76ab6b7778eea7fc505160c671ae` with tree
`95f16f05b30b7db23e43ebb6439ed0d055081902` is historical foundation lineage
only. MS1 is merged through PR #51, MS2 through PR #54, MS3-A through PR #55
and MS3-B through PR #56. The GitHub `main` snapshot at MS4-C review start was
not deployed as current production; later merges may change live `main`. The
last independently qualified deployed artifact is
pre-MS1 source `c421605e302d2ad46acdb2466627f64644181c9a`; its clean 24-hour
non-formal stage is complete and remains artifact-specific. Formal M22.9 has
not started and Production Ready remains NO. The MS2 configurable-product
runtime and MS3-B offline evidence are merged and accepted. MS4-B
stopped-deployment review is complete. The original MS4-C two-hour window
remains `EXECUTED_PARTIAL_NOT_ACCEPTED`; the separate passing R3 non-formal
supplement closes the recovery gate for review with zero Formal M22.9 duration
credit. MS4-D reviewed the bounded four-ProductKey qualification and records
overall MS4 as `REVIEWED_COMPLETE`; the Recorder is stopped and the archive
timer is disabled. Source retirement/external-media certification remain
unauthorized. Current status and authority boundaries are in
[`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md) and
[`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md).
The historical MS4-D review base was main
`e11d5cbdf861ab82bb110ead8e98a1f9498f3c55` (tree
`9fcf3e4128706938ffd02ef5a6af80c558cb234b`) and includes the merged MS4-C
evidence closeout PR #61 at commit
`013e20d6b911fde2f443aa6c855039599483ef7d` with the same tree; base CI run
`34551834444` completed successfully. The earlier `efae0135…` / PR #60
snapshot above is historical MS4-C review-start authority, not current.
The current milestone is `M22_9_P1=REVIEWED_COMPLETE`, based
on main `83a063f5bb9f91508238c9fd86d21aa45d1bd501`. The Recorder remains
stopped, `FORMAL_M22_9=NOT_STARTED`, formal credit is zero, and
`PRODUCTION_READY=NO`; the next milestone is
`M22_9_P2_EXACT_DEPLOYMENT_ARCHIVE_CAPACITY_PREFLIGHT`. P1's systemd 255
lifecycle probes were harmless and
did not touch Recorder or Raw. The active `/dev/vda1` runway is not sufficient
for the approximately 278-hour chain; the mounted `/dev/vdb1` is an archive
target only and does not remove that active-root precondition.

## Frozen historical long-run notice

The following notice is preserved verbatim because the Developer Preview
documentation contract requires the same historical cut on all living
surfaces. Its words such as “当前”, “尚未部署”, and its old staged-validation
requirements describe that historical incident cut only. They are not current
project/runtime authority and do not override the current risk states below.

```text
原始M21.4正式72小时窗口的进程稳定性PASS，但reconnect-boundary数据完整性合同FAIL；随后部署的M21.4.11工件`f659895…`已通过独立正式72小时观测门。
该工件随后因restart-only orphan-intent缺陷被判定`ELIGIBLE_FOR_168H=false`，因此168小时验收未运行。
PR #11的进一步修复后来进入M22.9 incident artifact；当前本地continuity修复
尚未部署，新的修复工件必须从2h→12h→24h→72h→168h重新开始验收。
M22.9 exact-VPS 24小时阶段结果为INCOMPLETE；已确认 fatal post-close
handoff 路径会遗漏持久 gap 证据。修复仅在本地完成、尚未部署；72小时不具备资格。
静态审查、单元测试、故障注入和短期在线测试不能替代长期运行证明。
当前版本为Mac Developer Preview;Ubuntu ARM64/RK3588为Developer Preview / Soak Candidate;不得用于真实资金交易。
```

Current project and runtime authority remains
[`CURRENT_PRODUCTION_STATE.md`](CURRENT_PRODUCTION_STATE.md) and
[`PROJECT_HANDOFF.md`](PROJECT_HANDOFF.md).

Severity: Critical / High / Medium / Low. Status is Open, Monitoring, Mitigated,
or Accepted. Each implementing milestone must update its risks and evidence.

| ID | Risk | Sev. | Mitigation / evidence gate | Owner milestone | Status |
| --- | --- | --- | --- | --- | --- |
| R-001 | Official SDK WebSocket callbacks may hide/re-encode payloads, own receive timing, or drop under blocking callbacks | Critical | Probe confirmed all three failures; ADR-0009 rejects SDK WebSocket streams and selects caller-owned exact-byte transport | M2 | Mitigated |
| R-002 | Binance documentation paths/semantics change; specified `/docs/llms.txt` redirects to portal HTML | High | Updater uses the working official index and validates host, every redirect, HTTP 200, body/content and selected-page hashes | M2 | Mitigated |
| R-003 | USD-M stream routing changed to `/public` and `/market`; stale endpoints could silently omit streams | Critical | M5 pins the current notice: depth/bookTicker use `/public`, aggTrade uses `/market`; live acceptance verifies every route; M7 must revalidate side-data routes | M2/M5/M7 | Mitigated |
| R-004 | Callback or writer backpressure causes silent loss/unbounded memory | Critical | M3 bounded spool plus M4/M5 finite WebSocket buffer/receipt queues; M20 wires one configured bound to both layers and phase-staggers per-stream Raw seals. M21.4 replaces USD-M `put_nowait` with bounded, Writer-aware, stop-aware awaitable backpressure. Sustained saturation triggers bounded connection closure and recovery; Writer/fsync/seal/Catalog failures remain fatal. Historical M21.4 gen5 passed the ordinary recovery contract, and the Spot parity correction applies the same bounded handoff. M22.9 then confirmed R-054: if the separate post-close handoff also timed out, USD-M could reach fatal supervision without durable gap evidence. The correction preserves both bounds and the fatal exception while forcing a durable same-gap lifecycle before terminal exit; it is included in the recorded older deployed artifact `e074d41a…`. The M23.4 B run had zero backpressure episodes/timeouts under its workload, while longer validation remains pending. | M3-M5/M20/M21.4/M22.9/M23 | Monitoring |
| R-005 | kill -9 leaves a corrupt tail that appears sealed | Critical | ADR-0010 framing/CRC, actual SIGKILL mid-frame recovery, quarantine matrix, verified compression and atomic manifest/Catalog tests | M3 | Mitigated |
| R-006 | Spot or USD-M sequence semantics are applied to the other market | Critical | Separate Spot `U/u` and USD-M `U/u/pu` schema modules and fixtures retain semantics without M6 continuity inference | M4-M6 | Mitigated |
| R-007 | Wall clock adjustment, reboot, or sleep makes receive ordering/lag misleading | High | Dual event clocks plus ADR-0019 NSWorkspace sleep intervals and wall/monotonic discontinuity events prevent silent continuity; M16 still owns replay boot-domain/tie-break policy | M3/M6/M14/M16 | Monitoring |
| R-008 | External disk path aliases a different disk after rename/remount | Critical | ADR-0014 UUID + marker + storage_id + relative path resolution; rename/remount and mismatch-block tests pass | M9 | Mitigated |
| R-009 | Filesystem reports writable but lacks needed durable/atomic behavior | High | In-directory write/fsync/rename/readback/cleanup probe implemented; read-only and scope tests pass; physical filesystem matrix remains M17 evidence | M9/M17 | Monitoring |
| R-010 | Disk disappears or process dies during copy/verify/Catalog boundary | Critical | ADR-0015 transaction; actual SIGKILL at copy, verify and both Catalog-commit sides; retry reconciliation retains the source until verified commit | M10/M17 | Mitigated |
| R-011 | Verified archive becomes only copy after internal deletion and later fails | High | Explicit warning/manifest verification; user-owned independent backup; periodic verify CLI | M10/M18 | Accepted |
| R-012 | Internal disk fills while archive unavailable or seal requires temp overhead | Critical | ADR-0016 robust thresholds/forecast; hard reserve includes two rotation buffers; tested archive-first then seal/stop/event/gap with no unarchived delete | M3/M11/M17 | Mitigated |
| R-013 | Daily counters duplicate or lose increments across UTC boundary/restart | High | Transactional stable-ID aggregate batches, deterministic JSON/CSV, midnight/restart/retry tests; kill-before-flush reconciliation remains M17 | M8/M17 | Monitoring |
| R-014 | Side-data polling/rate limits impair L2 collectors or funding cadence is assumed | High | One process-local USD-M public REST `UsdMRestCooldown` and request lock are collector-owned and shared by core depth snapshots and all enabled side-data REST routes. Core and side use pre-lock wait, shared serialization, and post-lock recheck; HTTP and typed 418/429 outcomes coordinate through the same gate, including completed observations during cancellation. No forward fill or fixed 8-hour assumption; cursor/failure-isolation tests remain required. | M7/PR45-PR47 | Mitigated |
| R-015 | Blue/green cutover stops old instance before candidate is synchronized | Critical | ADR-0018 requires durable three-stream/snapshot/book readiness plus fresh post-readiness old/new events; failure leaves old running; Raw tags and Catalog transitions pass Spot/USD-M and rollback tests; normalized dedup remains M15 | M13/M15 | Mitigated |
| R-016 | launchd restart/multiple instances cause conflicting active writers | Critical | ADR-0019 kernel-held per-data-root service lock, atomic state, SIGKILL restart test and one-process ADR-0018 overlap boundary; all-core failure exits for launchd restart | M13/M14 | Mitigated |
| R-017 | Mac sleep/closed lid creates unavoidable data gaps | High | NSWorkspace begin/wake plus clock-discontinuity gap evidence; optional service-PID-scoped caffeinate cleanup; explicit no closed-lid/explicit-sleep guarantee | M14/M18 | Accepted |
| R-018 | Compression or normalization mutates/deletes canonical Raw | Critical | ADR-0020 reads verified Raw only; before/after Raw hashes, content-addressed immutable output, repeated-build and missing-source fail-closed tests pass | M3/M15 | Mitigated |
| R-019 | Consumer depends on external mountpoint or Recorder internals | High | `consumer-contract.v1` exposes explicit build/hash/count descriptors without physical paths; ManifestCatalog resolves contained relative paths; independent example imports only public replay API | M16 | Mitigated |
| R-020 | Official public API access is geographically/system restricted or transiently times out | High | M4 observed a one-second SDK TLS/proxy timeout; explicit 10-second timeout plus bounded snapshot retry keeps core streams active, while no successful snapshot remains a visible failure; no unofficial proxy | M2/M4/M5/M7 | Monitoring |
| R-021 | CRC32C/CBOR/Zstd implementations disagree across languages | Medium | ADR-0010 exact profile plus byte-identical Python vector and independent standard-library Go framing/CRC verifier | M3 | Mitigated |
| R-022 | PyObjC/Disk Arbitration cannot deliver required event/eject semantics in user context | High | M9 unprivileged callbacks pass; M12 verifies official SDK/PyObjC callback signatures and default non-forced unmount/eject bridge, dissenter evidence and timeout behavior; physical target exercise remains M17 | M9/M12/M17 | Monitoring |
| R-023 | Data volumes exceed initial forecast or Catalog becomes a bottleneck | Medium | M3 bounded-memory gate; M11 minute-scale aggregate-only samples and robust multi-window rates; live forecast accuracy remains M17 | M3/M8/M11/M17 | Monitoring |
| R-024 | User pre-existing changes in Alpha101Crypto are overwritten | High | Research repo remains read-only; audit baseline records dirty frontend files | All | Mitigated |
| R-025 | Project identifiers regress to an earlier M0/M0.1 identity | High | ADR-0007 constants and allowlisted legacy-name/path scans | M0.2/M1/M14/M18 | Mitigated |
| R-026 | Name, visual identity, wording, service label, or publisher metadata falsely implies an official Binance relationship | Critical | Prominent disclaimer; no Binance logo; author-controlled namespace; forbidden wording/namespace tests | M0.2/M14/M18 | Mitigated |
| R-027 | V1 over-engineers an unrequested multi-exchange framework | Medium | Binance Spot/USD-M scope in ADR-0007; another exchange requires separate review | M0.2 and all design milestones | Mitigated |
| R-028 | CloudFront/WAF challenges block scripted retrieval of some interactive developer-portal catalog pages | Medium | Treat every non-200/empty response as failure; select downloadable official Markdown and official SDK source; record challenge and never use an unofficial mirror | M2 and ongoing source refresh | Monitoring |
| R-029 | Local-book logic mixes Spot and USD-M continuity, hides a missing event, or overstates bookTicker as a checksum | Critical | ADR-0011 market-bound rules, random deletion fault tests, immutable incomplete intervals, same-ID-only ticker comparison and explicit non-checksum wording | M6 | Mitigated |
| R-030 | Sparse liquidation snapshots are mistaken for a complete liquidation feed, or silence is treated as zero activity | High | ADR-0012 preserves exact frames, marks snapshot/sparse semantics, records connection failures and never synthesizes missing/zero events | M7 | Mitigated |
| R-031 | Eject races archive allocation, force-unmounts busy media, targets the wrong Disk Arbitration object, or treats disappearance/unmount-only as safe | Critical | ADR-0017 atomic Catalog latch; all nonterminal archive work is BUSY; default non-force callbacks require both unmount and eject success; M17 physical ExFAT exercise found partition-object eject returned Apple `kDAReturnUnsupported` after a successful unmount; adapter now resolves associated whole media with `DADiskCopyWholeDisk`, and an already-unmounted retry requires exact prior Catalog refusal evidence; physical retry confirmed orderly eject, absence and UUID-based reinsertion; a separate physical pull during COPY returned I/O error, retained the internal source, remained uncommitted, and recovered idempotently after reinsertion | M12/M17 | Mitigated |
| R-032 | Normalization exhausts memory/disk or publishes a partial/incompatible Parquet build | High | ADR-0020 uses fixed 10,000-row external-sort/write batches, bounded open partition spools, internal `.work`, fsync/readback/atomic commit, exact PyArrow profile and DuckDB smoke; an abnormally large semantic collision group and live-volume sizing remain M17 evidence | M15/M17 | Monitoring |
| R-033 | Replay silently mixes builds, clocks, gaps or physical storage order, or exhausts descriptors/memory | High | ADR-0021 requires explicit build/clock/policies; verifies manifest paths/hashes; fixed 10,000-row batches and bounded 32-way merge; equal-time, orphan, corruption, missing-clock, gap and checkpoint tests pass; live-volume sort-space sizing remains M17 | M16/M17 | Monitoring |
| R-034 | Current official Global Spot bootstrap wording conflicts with the official toolbox example and observed adjacent snapshot/diff boundary | Critical | 2026-07-26 Global page SHA-256 `8a127810e46793aa47b42e33ce0df963c9a169c3f27f89b97cbc6a603f0c823e` remains conflicting. Preserve Raw; ADR-0011 uses `lastUpdateId + 1` without claiming official correction. `tools/evaluate_spot_bootstrap.py` reports both outcomes. Revert only on corrected normative source/maintainer answer or repeatable quality divergence. | M17/M19 | Open |
| R-035 | Long-running reliability evidence is artifact-specific and does not transfer to a behavior-changing multi-symbol artifact | Critical | The pre-MS1 deployed artifact completed the clean 24h non-formal stage, with accepted cadence and RSS watches. The historical MS4-C behavior/deployment source `303e073e25d5ed53d7cf6e26a9c6c6e879013b50` was installed on the Tokyo VPS and is now historical. P2 separately installed and verified its exact deployment source/review base `646792f2e5fc5b7195ea58541d3f1dfda6555b7f`, then stopped it after the bounded non-formal interaction. MS4-D records the bounded four-ProductKey qualification as reviewed complete, while the original MS4-C two-hour window remains `EXECUTED_PARTIAL_NOT_ACCEPTED`; the passing R3 non-formal supplement closes only the recovery gate and grants zero Formal M22.9 duration credit. Long-duration/formal stages, external-media certification, and Production Ready remain uncompleted. | M21.4/M22.9/MS4/M22.9-P2 | Monitoring |
| R-047 | Ordinary/planned reconnect boundaries seal as complete without gap evidence | Critical | The historical 72h window and fixed-cutoff audit remain evidence of the original silent-gap class. M21.4.11 introduced the durable STARTED→forced reconnect-gap seal→first-new `sequence_gap`→COMPLETED lifecycle, crash intent, seal defense, and deterministic audit; the correction is included in the recorded older deployed artifact `e074d41a…`. The M23.4 B window recorded 17 STARTED, 17 COMPLETED, 0 OPEN intervals, later classified `CLOSED_HISTORICAL_BASELINE`. This does not claim zero disconnects or substitute for longer/formal validation. Historical additive remediation remains designed but unexecuted. | M21.4/M23 | Monitoring |
| R-048 | Remote archive receipt or transport success authorizes deletion of the wrong VPS Raw source, or a crash/unexplained absence is misclassified as authorized deletion | Critical | ADR-0029 binds chunk, Raw stored hash, manifest identity/hash, archive_set_id, storage_id, target identity, session identity, and verification outcome. M22.3 implements the durable local chain and M22.4A persists the exact canonical receipt/source-bound pending authority. M22.4B reloads that authority by receipt ID only, retains the exact manifest, freshly hashes stored and decompressed bytes through a held no-follow Raw fd, final-compares the held file to the parent-relative leaf, uses one exact parent-relative unlink, fsyncs the same anchored parent fd, and atomically commits one exact terminal row/event afterward. PR #46 corrected the split-generation remote-delete authority load by reading one SQLite snapshot, so mixed-generation authority cannot be assembled from separate reads. CASE B alone auto-reconciles startup absence; CASE C/D, terminal-present, unsafe objects, and ownership overlap fail closed. M22.5 adds only fixed byte/message verbs: exact receipt stdin, process-aware Raw EOF/exit/reaping, strict identifiers, normal OpenSSH host-key policy, and same-receipt response-loss reconciliation. SSH exit status is never domain authority. The MS4-C receiver-only Mac Downloads path passed selected receive/readback/hash/manifest checks, but it is not external-media or production archive certification; no remote deletion/source-retirement authority was issued. | D0/D1/M22.3/M22.4A/M22.4B/M22.5/PR46 | Mitigated |
| R-049 | A shared VPS reaches reserve while offline work competes with live capture | Critical | ADR-0028 prioritizes live acquisition/seal/Catalog/recovery and forbids default heavy Normalize/Replay/Backfill on the VPS. The current M22.9 precondition measured about `32.523064 h` of runway, insufficient for the complete approximately 278h formal chain, so T0 correctly did not start. Progressive non-formal runs may reclaim disposable data only through a separately authorized consistency-safe retirement after evidence is frozen; this does not satisfy formal capacity. Provision and remeasure a capacity-complete environment before formal T0. | D0/D1/M22.7A/M22.9/M23 | Open |
| R-050 | Live Catalog backup is copied inconsistently or treated as Raw replacement | High | M22.6 opens the live WAL Catalog with `mode=ro`, `query_only=ON`, normal locking and no `immutable=1`, and uses only `sqlite3.Connection.backup()` without checkpoint or DB/WAL/SHM filesystem copy. The remote backup and transferred standalone file each run exact integrity plus existing authoritative receipt/event/state validation. Local reopened size/SHA and durable immutable manifest/generation gates precede mirrored crash-safe latest/previous retention. Real WAL/concurrent-writer, full-bytes-plus-nonzero, corruption, fsync, one/both-slot, local SIGKILL and remote SIGKILL-residue tests fail closed. Snapshot-only retry never replays archival/deletion; Raw remains authority. Real sshd/LAN/VPS deployment and restore remain future/non-scope. | D0/D1/M22.6 | Mitigated |
| R-051 | Archive Set index loss makes removable media uninterpretable or falsely implies redundancy | High | ADR-0030 requires media-local archive_set_id/storage_id metadata and manifests, whole-chunk placement, rebuildable global index, and explicit ARCHIVE != BACKUP wording. M22.2 implements media identity/inventory and index rebuild; M22.3 receipts independently revalidate from media-local Raw, external manifest with embedded exact source manifest, Archive Set entry, receipt, and registered marker after the workspace index is removed. Archive remains one copy, not backup. | D0/D1/M22.2/M22.3 | Mitigated |
| R-052 | A pre-M22 binary is used after remote lifecycle state has been persisted and ignores remote ownership during recovery/rollback | Critical | M22.4A keeps remote values out of `ChunkState` and uses two additive same-Catalog tables. A new binary accepts the exact historical schema in read-only mode as an empty remote projection and adds both tables transactionally on writable open; partial or malformed remote schema fails closed. Reverse downgrade after any remote row persists remains NOT CLAIMED / NOT GENERALLY SUPPORTED because an old binary cannot see remote ownership. M22.7B records the operational deployment/rollback policy and passed the rollback compatibility preflight on the real host; actual artifact rollback remains not exercised. Production remote-state enablement remains outside this host gate. | M22.0/M22.4A/M22.7B | Open |
| R-053 | Historical generic M11 alert authority could be confused with named VPS policy state | High | M22.7A keeps `observe()`/`observe_internal()` and Catalog schemas generic, makes profile selection explicit and derived-only, retains `storage-forecast.v1` M11 fields, and tests generic→VPS→generic evaluation without persisted VPS identity. | M22.7A | Mitigated |
| R-054 | A fatal post-close handoff timeout can escape before durable reconnect-gap evidence, allowing the old Raw tail to be sealed complete and the first post-restart frame to remain unmarked | Critical | Historical M22.9 forensics confirmed the exact USD-M branch. The reviewed correction freezes call-site authority, resets ephemeral boundary state per generation without erasing pending-gap authority, fails session restart closed with the exact unpersisted boundary and same-gap recovery, and creates no fake gap for global stop. Deterministic origin/stale-state, crash, stress, and full-suite regressions pass; the correction is included in the recorded older deployed artifact `e074d41a…`. The M23.4 window's 17 disconnect intervals were all completed with 0 open and later classified `CLOSED_HISTORICAL_BASELINE`, but longer non-formal and formal validation remain required. | M22.9 continuity corrections/M23 | Monitoring |
| R-055 | Acceptance evidence is collected from a different artifact, is overwritten, or treats wall-clock time as elapsed duration | Critical | The installed M22.9 observer binds deployment identity, prior-stage hashes, exact manifest path/byte inventory, process incarnation, service instance, boot ID, UTC, and Linux CLOCK_BOOTTIME; strict Catalog/Raw/manifest failures block publication, evidence is canonical/no-clobber, and resume validates the immutable chain. No stage auto-advances or emits Production Ready. A precondition-only inspection used these authorities but did not create a root or start T0 because capacity was insufficient. An unfinished-stage directory copy fork remains an accepted nonblocking limitation under the operator-owned evidence/hash-chain threat model. | M22.9 | Mitigated |
| R-056 | Long startup recovery creates a stale heartbeat and cannot stop cooperatively | Critical | Historical forensics confirmed approximately 17m50s healthy recovery versus a 30s stale-heartbeat threshold. The startup-liveness corrections preserve heartbeat ownership, stable-sealed fast path, unstable validation, cancel/resume, readiness, and post-capacity stop behavior. They are included in the recorded older deployed artifact `e074d41a…`; the service reached READY and remained on the same healthy incarnation through the M23.4 window and later precondition inspection. Formal M22.9 still has no duration credit. | M22.9/M23 | Mitigated |
| R-057 | Missing or size-mismatched stable SEALED Raw is reconciled as a nonfatal action rather than failing startup | Medium | Targeted review recorded this as P2/nonblocking for the startup-liveness architecture. It is pre-existing post-commit external-loss/filesystem-corruption behavior, separate from crash-recovery authority establishment, and is not promoted to P1 or changed in this documentation update. | M22.9 | Monitoring |
| R-058 | Incremental clean-seal evidence diverges from exact Raw bytes because caller-owned envelope state mutates, a partial write is counted, the source changes after close, or retained-duplicate cleanup deletes the last Raw copy | Critical | M23.4 snapshots Raw semantics into private immutable values; byte and semantic evidence commit only after complete writes; ambiguous failures poison one-shot evidence. Header parity, compression/decompression validation, sealed-artifact validation before ordinary-`SEALED` retained-Raw deletion, and archive-successor convergence fail closed. Final independent review recorded P0/P1/P2/P3 all zero. PR #41 merged, post-merge CI passed, and the two-hour VPS A/B observed no normal clean-seal `scan_chunk`, an 82.2811% lower maximum seal latency, and no integrity regression. Formal M22.9 remains unrun. | M23.4 | Mitigated |
| R-059 | Validation evidence collection or disposable test-data retirement destabilizes the small VPS or corrupts state | High | An optional post-profile unbounded full-history audit consumed excessive RAM and was terminated after the completed M23.4 window; do not repeat it on this VPS. Use only bounded project-owned checks. Freeze/hash evidence before cleanup, and retire/reset non-formal data only through a separately authorized consistency-safe procedure that preserves Catalog/Raw/manifest/archive authority; never use arbitrary recursive deletion or manual Catalog surgery. | M23/M22.9 operations | Open |
| R-060 | GreenCloud ordinary KVM shared-CPU policy constrains sustained Recorder headroom | High | Historical provider-policy evidence remains an operational constraint, not a Recorder benchmark. The clean-24h campaign recorded `PASS_WITH_JITTER_WATCH`; the bounded MS4-C evidence recorded RSS `286846976 -> 390660096` bytes, cgroup `MemoryCurrent` peak `1285488640` bytes, CPU increment `6631.033603` seconds, and no OOM. These observations do not establish long-run/formal headroom; queue depth was unavailable, although no backpressure, heartbeat, readiness or terminal symptoms were observed. | M23/MS4 | Monitoring |
| R-061 | Provider-panel CPU readings are mistaken for a normalized multi-product performance benchmark | High | The approximately 20% panel observation is explicitly non-scientific. MS3/MS4 must use process CPU seconds, event volume, queue/high-watermark, backpressure, latency, RSS, and integrity evidence for the configured profile. | M23/MS3/MS4 | Monitoring |
| R-062 | Stepwise RSS growth is mistaken for a confirmed memory leak or safe configurable-product scaling | Medium | Clean-24h evidence classified RSS `EARLY_GROWTH_THEN_PLATEAU`; the bounded MS4-C evidence recorded RSS `286846976 -> 390660096` bytes and cgroup `MemoryCurrent` peak `1285488640` bytes without OOM. This finite observation does not prove a memory leak or safe long-term configurable-product scaling; queue depth was unavailable and longer/formal evidence remains open. | M23/MS4 | Monitoring |
| R-063 | Validation duration credit transfers from the pre-MS1 artifact to MS1 or a later multi-symbol artifact | High | The original MS4-C two-hour window remains `EXECUTED_PARTIAL_NOT_ACCEPTED`; the separate R3 non-formal recovery supplement grants zero Formal M22.9 duration credit. No 12h, 24h, 72h or 168h duration transfers, and Formal M22.9 remains separate and unstarted. | MS4/M22.9 | Open |
| R-064 | Configurable-product propagation or incomplete runtime identity could collapse discontinuity evidence or side-data cursors across products | High | MS1 uses explicit `(market, symbol, stream)` lifecycle identity and `(kind, symbol)` symbol-specific cursors, preserves non-unique historical records, migrates legacy rows atomically/idempotently to `BTCUSDT`, and rejects malformed/partial migrations. ADR-0032 requires MS2 to preserve this foundation for every configured ProductKey across Spot/USD-M. | MS1/MS2 | Mitigated |
| R-065 | Fan-out multiplies shared REST gates or global side data, or reports global readiness while a configured product is unhealthy | High | ADR-0032 requires the shared Spot limiter, one process-owned USD-M request lock/cooldown, one process-global USD-M side-data owner, independent product readiness, and exact configured-versus-runtime ProductKey equality. MS2 offline acceptance proves topology, exact shared object identity, observed 418/429 sibling blocking, global ownership, zero-market construction, and independently verified readiness. Updated MS3-B evidence adds real Collector/Poller core/product/global competition, finite terminal-request accounting, page release/reacquisition, real Catalog cursor isolation, and mixed running-path sibling progress. The bounded MS4-C/R3 evidence kept all four expected ProductKeys ready and showed no observed shared-REST fault; queue depth was unavailable, and long-run CPU/RSS/capacity proof and formal stages remain open. | MS2/MS3/MS4 | Monitoring |
| R-066 | Cancellation of an in-flight USD-M side REST call releases the shared gate while its SDK worker thread still runs | High | Deterministic production-path reproduction found bare `asyncio.to_thread` could release the real `asyncio.Lock` before its worker completed. `run_owned_blocking_call` now retains ownership through worker completion; waiting cancellation, in-flight cancellation, post-stop request, and core exclusion are covered by `test_ms3b_production_paths.py`. | MS3-B | Mitigated |
| R-067 | A detached acceptance unit can be garbage-collected or mistaken for the evidence authority, or an unsafe retry can create a second T0/root | High | M22.9-P1 keeps `AcceptanceObserver` and immutable `stage-final.json`/SHA chain authoritative; external systemd uses `Type=exec`, `Restart=no`, fixed operator-selected registered relative root, and no automatic stage advancement. Inspect InvocationID/journal separately, and resume only the exact unfinished root when boot/process/service/identity and the 600-second gap bound still match; otherwise preserve and fail closed. | M22.9-P1 | Monitoring |
| R-068 | Active writer root can reach its hard reserve before a long Formal chain even when the registered archive target has ample space | Critical | P2 observed the exact current deployment with the archive timer enabled and backlog returning to zero. The conservative existing 24-hour generation rate is `349910.017730 B/s`; active-root runway above the 10 GiB reserve is only about 26.46 hours, so every Formal stage start/end must recheck runway, timer health, backlog, and target margin. | M22.9-P2 | Monitoring |
| R-069 | A monotonic archive timer can be enabled without an immediately established future periodic trigger, or can later stall while the Recorder is stopped | High | P2 explicitly bootstrapped the first bounded archive service cycle, then observed autonomous `OnUnitActiveSec` triggers with future monotonic next times, zero failed transactions, and backlog zero. Keep the timer enabled/active and inspect service result, journal, backlog, and future trigger before every Formal stage. | M22.9-P2 | Monitoring |
| R-070 | AcceptanceObserver can compare filesystem inventory with a Catalog snapshot while the archive timer is concurrently committing `LOCAL_DELETED` retirement, producing a false stage blocker or hiding a real lifecycle defect | High | The Formal 2-hour attempt failed closed at T0. Post-stop review reconciled all 26 implicated chunks and the full 112,817-manifest audit had zero persistent Catalog/integrity findings. The reviewed fix freezes a lifecycle-coherent boundary, makes one membership comparison, defers later rows, and fresh-validates authorized `LOCAL_DELETE_PENDING`/`LOCAL_DELETED` absence with existing external verification. Deterministic race, unauthorized-loss, corruption, and read-only tests pass; independent review found P0/P1/P2=0. Source is not deployed, exact-artifact redeploy preflight remains separate, and no retry or retroactive credit is authorized. | M22.9 observer fix | Monitoring |
| R-071 | Unattended host package maintenance can reexecute systemd and restart Recorder or a transient Formal observer inside a duration window, invalidating process identity and relaunching a new-stage command despite `Restart=no` | Critical | The 2026-09-12 retry preserves one interrupted original root and one ineligible relaunch root, awards zero credit, and stops Recorder. Before another retry, complete a bounded quiet-window preflight that handles pending package work and prevents maintenance-driven service reexecution during measurement, then restores normal security-update authority. Never resume across changed Recorder PID/InvocationID/service instance or treat an automatic relaunch as authorized. | M22.9 host-maintenance preflight | Open |
| R-036 | USD-M 5m limited-retention polls are missed while the recorder is offline | High | Independent durable Cursor per kind, bounded paginated catch-up from Cursor + 5m, Raw fsync before advance, EMPTY_RESPONSE/no-advance, and explicit gap after retention; complete long-run operation before relying on continuity | M19/M19.1 | Open |
| R-037 | Binance historical archive checksum is revised or a file is missing | High | Immutable URL+checksum revisions with `supersedes`; 404 GAP; verified ZIP/Parquet lineage; never silently overwrite | M19 | Mitigated |
| R-038 | Split proxy decisions bypass the operator's intended route or leak a URL/credential | Critical | ADR-0025 single policy is injected into all WS/urllib/SDK/Historical exits; direct empty handler, environment/no_proxy, explicit validation, SDK mapping, redacted state and Mock CONNECT tests | M20 | Mitigated |
| R-039 | Mihomo restart or CONNECT timeout silently loses depth continuity | Critical | Transport failure enters existing bounded reconnect; depth unexpected-disconnect requests a fresh snapshot/resync; service/Catalog evidence and M20 real restart injection must not claim the interrupted interval complete | M20/M21 | Monitoring |
| R-040 | systemd runs Collector as root, inherits an SSH proxy, or uninstall removes data | Critical | Explicit non-root User/Group validation; unit has only PYTHONUNBUFFERED, TOML proxy, SIGTERM/90s, managed-unit marker and data-retaining idempotent uninstall tests | M20 | Mitigated |
| R-041 | Linux mount alias/UUID or disappearance targets the wrong archive directory | Critical | mountinfo + findmnt + lsblk corroboration, external/hotplug evidence, reliable UUID + marker + relative path + storage_id, existing capability probe; fixture disappearance retains internal source; physical external media not exercised in M20 remains open | M20/M21 | Monitoring |
| R-042 | ARM64 dependency lacks compatible native Wheel or Linux imports PyObjC | High | Fresh CPython 3.12 venv installs final Wheel; aarch64 native import smoke covers PyArrow/Zstd/CRC32C/CBOR/websockets/Binance SDK; Darwin markers and Linux no-PyObjC import test | M20 | Mitigated |
| R-043 | Artifact identity/version is ambiguous due to CWD Git contamination | Medium | CLI `--version` Git suffix may change when invoked from repository directory. Production identity must be established from immutable Wheel SHA, direct_url.json, non-editable install state, and Canonical Gate static verification. Run production CLI from `/tmp`. | M21.4 | Monitoring |
| R-044 | Observation/evidence collector serialization failures lose monitoring data | Medium | M21.4 12h collector lost 144/144 JSON observations due to timer-field parser bug. The 24h collector fixed raw+JSON collection (273/273 pairs, 0 parse errors), but the original 24h journal export used local-time bounds (8 h offset) and the first corrective review miscounted Catalog-only events via journal search; both were corrected by epoch-derived UTC-bounded re-export and a contract forensic review. Mitigations now mandatory: journal boundaries derived from the nanosecond T0/Target but enforced at journald's microsecond resolution (RFC3339 UTC microseconds or read-only-verified `@` epoch syntax; structured `__REALTIME_TIMESTAMP` exports; `BOUNDARY_PRECISION_AMBIGUOUS` for undecidable boundary records), Catalog/Raw/Manifest evidence-source matrix, raw text + parsed JSON per round, independent corrective directories, never overwrite originals. | M21.4 | Monitoring |
| R-045 | USD-M side-data REST rate-limit/error accumulates lifetime failures | Medium | One `UsdMRestCooldown` is shared by every manager-owned REST poller. Typed 429/418 SDK failures and classified HTTP responses install an IP-wide, stop-aware monotonic gate; Retry-After is honored; missing-header fallbacks are 60 seconds for 429 and 24 hours initially for 418, escalating to 3 days. Core markets remain isolated and side-data gaps remain explicit. Historical traceability for the cumulative 232 remains in `RUN_ROOT/corrective-integrity-review-20260806T152159Z/journals/recorder.log`; the correction is included in the `303e073e25d5ed53d7cf6e26a9c6c6e879013b50` deployed artifact, which passed the bounded MS4-C core qualification. Auxiliary data was disabled in this profile, so side-data live qualification remains not covered. | This correction | Mitigated for stopped install; side-data live qualification pending |
| R-046 | Permanently hung kernel I/O cannot be cancelled even with owned-worker protection | High | M21.4.2 shields asyncio ownership through blocking calls, but an OS write/fsync/compression/sqlite operation already executing cannot be deadline-interrupted. The owner waits rather than closing descriptors out from under the worker. | M21.4 | Accepted |

## M22.8 closure note

The accepted fixed M22.8 run validated the real Mac/OpenSSH/VPS boundary for
receipt binding, wrong-identity rejection, crash and retry recovery, delete
response-loss reconciliation, and snapshot-only retry without production
mutation. R-048 and R-050 remain bounded by their documented contracts; exact
production VPS staged acceptance remains M22.9. Two evidence-packaging P2
findings are recorded in `docs/milestone_acceptance/M22.8.md` and do not block
M22.8 or become M22.9 prerequisites.

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
- Disk Arbitration discovery/eject design resolved by ADR-0014/0017; physical
  filesystem, busy-process and removal matrix remains M17.
- Normalized primary keys/dedup tie-break and Parquet schemas: resolved by
  ADR-0020; live-volume sizing remains M17 evidence.
- Replay total-order details and consumer dataset-version policy: M16.
- Whether a future official Binance MCP exists with verifiable installation:
  not required; reconsider only from official documentation.
- Future distribution/import/CLI/application-data identifiers must match
  ADR-0007 exactly. ADR-0019 resolves service identity without guessing: install
  requires an explicit author-controlled label/attestation and forbids
  Binance-owned-looking or placeholder namespaces.
