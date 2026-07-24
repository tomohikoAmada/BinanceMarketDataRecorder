# M17 Initial Block and Short-Term Continuation Evidence

- Milestone: M17 — Short-term reliability and fault injection
- Status: **SHORT-TERM ACCEPTED — LONG-RUN DEFERRED**
- Date: 2026-07-24
- Starting commit: `93091281fbe8d9c866ddf0ee83c3e823c96314cc`
- Certified host: macOS 26.5.2, Apple Silicon arm64, Python 3.12.9

## Stop reason

The required 72-hour and seven-day clocks were not started. Two short,
credential-free public preflights wrote only below:

- `~/Library/Application Support/BinanceMarketDataRecorder/m17-validation/`

Both markets preserved Raw events, manifests and daily reports. The first run
showed that a snapshot which does not bridge leaves readiness false. A proposed
retry was tested locally, then a second public probe showed that the current
official Spot bootstrap sentence and the observed adjacent snapshot/diff
boundary cannot both be satisfied.

The official developer page and official Spot documentation repository say:

1. discard buffered events with `u <= lastUpdateId`;
2. the first remaining event contains `lastUpdateId` in `[U,u]`;
3. normally a subsequent event starts at previous `u + 1`;
4. only `U > localUpdateId + 1` is a live gap.

The second probe recorded, among other exact immutable records:

| Evidence | Value |
| --- | ---: |
| first Spot snapshot `lastUpdateId` | 97,799,318,536 |
| first Spot diff `[U,u]` | [97,799,318,545, 97,799,318,546] |
| later Spot snapshot `lastUpdateId` | 97,799,318,619 |
| first remaining Spot diff `[U,u]` | [97,799,318,620, 97,799,318,630] |
| Spot snapshots retained | 24 |
| Spot diff events retained | 355 |

The later boundary is adjacent and satisfies the documented live continuity
rule, but `97,799,318,619` is not inside
`[97,799,318,620,97,799,318,630]`. Treating
`lastUpdateId + 1` as the bridge would look operationally plausible, but it
would contradict the explicit current official bootstrap sentence. Project
rules require stopping rather than silently substituting different semantics.

## Rate-limit evidence and containment

The diagnostic retry used Spot depth `limit=5000`, whose captured response
headers showed weight 250. It was stopped immediately after the official SDK
reported `TooManyRequestsError` followed by `RateLimitBanError`. No key,
account, order or trading endpoint was used. No further Binance API call or
long-running process remains active.

The unsafe fast semantic-retry experiment was removed from the worktree. The
public Raw evidence remains outside Git and is not represented as an accepted
soak.

## Other preflight findings

- macOS and arm64 satisfy the representative-host dependency.
- Internal available capacity was about 469 GiB.
- No external volume or registered archive target was present. Physical
  unplug/read-only/eject and non-zero archive-throughput gates therefore also
  remain unrun.
- The external consumer repository was inspected read-only and not modified.
- Existing deterministic coverage includes Raw/Archive SIGKILL, deleted and
  out-of-order depth, checksum corruption, emergency space handling,
  sleep/wake, and failed blue/green handoff.
- Targeted regression tests passed before the official-semantics conflict was
  classified. These are not a substitute for the required fault matrix or
  soaks.

## Required unblock

Obtain verifiable clarification or a corrected official Binance source for the
Spot initial bridge. Then:

1. update ADR-0011 and version the reconstruction algorithm if semantics
   change;
2. add a rate-aware snapshot/resync state machine and deterministic tests;
3. rerun M6/M13 compatibility and the full M17 fault matrix;
4. provide a user-registered physical external test folder for the required
   media scenarios;
5. start a fresh uninterrupted 72-hour PoC;
6. after fixes, start a fresh seven-day run.

Any fix or restart resets the applicable soak clock. M17 has no acceptance
record and no milestone commit.

## 2026-07-24 short-term continuation

The user authorized the official toolbox example as additional evidence and
provided a dedicated physical external test directory. ADR-0011 now separates
Global documentation, official example behavior, Raw evidence, and engineering
inference. Spot reconstruction is versioned as v2 with target
`lastUpdateId + 1`; R-034 remains open because no maintainer confirmation or
corrected normative page exists.

ADR-0022 contains Spot REST request weight, single-flight, shared IP pacing,
429/418 blocking, capped full-jitter transient retry, cancellation and bounded
bootstrap-buffer behavior. The historical ban end was not retained, so this
continuation made no real Spot REST request.

The short-term external-media and fault matrix may continue, but neither the
72-hour gate nor the 168-hour total acceptance run is started by this work.
There is still no M17 acceptance record or completion commit.

## Physical external target and normal archive evidence

- Allowed directory:
  `/Volumes/SamsungDisk/CryptoRecorder-M17-Test`
- Directory and marker: both non-symlinks; realpath stayed exactly inside the
  allowed directory.
- Volume UUID: `FA8CAFBF-FA93-335D-9C4A-D34E0A8C7CCC`
- Observed volume/filesystem/device: `SamsungDisk`, ExFAT, `disk4s1`, USB,
  external/ejectable, writable.
- Capacity at inspection: 1,000,186,576,896 total bytes and approximately
  785.8 GB free.
- Registered storage ID:
  `4a10fc8f-9197-4b5b-9467-d1d05b945870`
- The explicit `.m17-probe-*` and Recorder registration probes both passed
  write, file fsync, directory fsync, atomic rename, readback, SHA-256 and
  cleanup with zero probe residuals.

The normal archive source was generated internally from seed `20260724` as 32
eight-MiB deterministic binary payloads (268,435,456 payload bytes; payload
SHA-256
`0b83df01cb0366240c1b919460a28b543be594964b48a4978e10e1f5e1ef2a0c`).
It was explicitly tagged `m17_disposable_archive_fixture`; no existing market
file was used. Sealing produced chunk
`782daef0-7044-4001-9361-fa4c46e5bac4`, 268,460,657 stored bytes, SHA-256
`384663ae6f9ae5540146577e9bdb5b2948577f7c86fe6ec326e167c62264c915`.
The fixture was below one percent of both observed internal and external free
space.

Transaction `58954748-7538-51b6-95f6-8d70d1ab7241` visibly used its owned
`.copying` path from 1 MiB through 268,460,657 bytes. Copy plus fsync took
1.168 seconds, file fsync approximately 1.122 seconds, full readback/hash
verification 0.104 seconds, and the whole transaction 2.375 seconds
(approximately 113 MB/s). The final artifact and external manifest were
durable before Catalog commit. Final and expected SHA-256 matched. Catalog
chunk/transaction reached `LOCAL_DELETED`; only then was the internal test
source removed, releasing approximately 268 MB. Daily Catalog metrics recorded
one archived file, 268,460,657 archived bytes and the same deleted-local byte
count. Formal `archive verify` returned one verified file, zero failures and
zero pending files. No `.copying`, `.partial`, probe, symlink, or open-handle
residual was found in the dedicated directory.

This normal-path result does not substitute for safe eject/reinsert or physical
removal during copy. Those remain interactive gates.

## Offline regression after the short-term fixes

The complete offline suite passed with 316 tests, five explicit online skips
and one stress deselection. The only warnings were Python's known
multi-threaded `fork()` deprecation warnings in five existing SIGKILL archive
tests. Strict mypy passed 75 source files, historical M0-M16 contract
verification passed, and the independent Go Raw v1 golden verifier passed.
Ruff was rerun after one test-regex formatting correction.

## Safe-eject physical finding

The first formal safe-eject request had no active archive transaction, no open
Recorder handle, and used only default, non-forced Disk Arbitration options.
The registered ExFAT volume unmounted successfully, but ejecting the partition
object `disk4s1` failed with signed status `-119930868`
(`0xF8DA000C`). The macOS 26.5 SDK `DADissenter.h` identifies this exact value
as `kDAReturnUnsupported`. The system therefore did **not** report the device
safe to remove, the interactive checkpoint was not issued, and no physical
removal was assumed.

Apple's installed `DADisk.h` documents `DADiskCopyWholeDisk` as obtaining the
associated whole-disk object. The adapter now preserves the non-forced,
volume-specific unmount but submits the subsequent media-eject request on that
associated whole-disk object. A retry from `PRESENT_UNMOUNTED` is accepted only
when Catalog retains exact evidence that the prior operation successfully
unmounted the same registered target and then failed at the eject stage. It
does not remount the volume, access external files, use `diskutil`, or use a
force option. Deterministic adapter/coordinator tests cover both the object
selection and the refusal-evidence gate.

After the complete offline gates passed, the explicit evidence-gated retry used
the same registered UUID and BSD partition identity. Disk Arbitration confirmed
`unmounted=true`, `ejected=true`, no failed stage, and no dissenter; Catalog
reached `SAFE_TO_REMOVE`. This proves the normal system eject request, but not
yet the user's physical removal, absence observation, or reinsertion recovery.
Those remain at interactive checkpoint one.

After the user confirmed physical removal, `/Volumes/SamsungDisk` was absent
and Disk Arbitration no longer returned the registered UUID. Structured status
now separates the physical target state `ABSENT` from the retained Catalog
control evidence `SAFE_TO_REMOVE`; the latter proves the earlier orderly eject
without masking actual device absence. No Recorder process was running, the
internal data root remained `READY`, and the targeted no-external-volume
regressions passed. This is not evidence of a live Collector continuing across
removal because no live Collector was started for this checkpoint.

Reinsertion resolved the same UUID at `/Volumes/SamsungDisk`, validated the
same marker nonce and storage ID, returned the target to `READY`, and changed
Catalog control from `SAFE_TO_REMOVE` to `ACTIVE` without registration.

For interactive disconnect checkpoint two, a new deterministic internal Raw
chunk was generated from seed `20260725`: chunk
`54949df2-9ee8-411a-baa8-8724c48cd4f4`, 536,918,028 stored bytes, SHA-256
`813d0c98b0e6f54a745717ba888518d53e565f679336c62eaf1b04a40a43049c`.
It is below one percent of both internal and external free space. The
ArchiveManager transaction
`9e21e27d-7ca3-5c89-b101-9c6ae00a54ff` is intentionally paused after writing
33,554,432 bytes to its owned `.copying` file. The internal source exists and
matches its hash; the final external name does not exist; Catalog remains
`COPYING`; no external manifest or archived commit exists; and the worker has
the `.copying` descriptor open. Its internal release gate is absent, so the
worker cannot continue until a later user-confirmed step.

After the user confirmed direct physical removal, the mountpoint and volume
UUID were absent before the internal release gate was opened. The worker's next
write returned `[Errno 5] Input/output error` and was classified
`DISAPPEARED_DURING_COPY`; it exited without retry. Catalog remained
`COPYING`, `catalog_archived=false`, no verified/final commit was made, and the
536,918,028-byte internal source still matched SHA-256
`813d0c98b0e6f54a745717ba888518d53e565f679336c62eaf1b04a40a43049c`.

The exercise also exposed that `last_error` alone was not a separately
queryable operational failure event. ArchiveManager now records one idempotent
`ARCHIVE_ATTEMPT_FAILED` event per transaction attempt, including failure kind,
Catalog state and internal-source preservation. The deterministic unplug test
passes. The physical attempt was registered under event ID
`archive-attempt-failed:9e21e27d-7ca3-5c89-b101-9c6ae00a54ff:2` using its
actual Catalog error and occurrence timestamp.

The same UUID and marker returned after reinsertion. The owned 33,554,432-byte
`.copying` residual was present, while the final name and external manifest
were absent and Catalog was still `COPYING`. Formal `archive retry` removed
only that owned residual, recopied from the verified internal source, performed
full external readback and SHA-256 verification, committed the external
manifest and Catalog, then deleted the internal fixture. Transaction
`9e21e27d-7ca3-5c89-b101-9c6ae00a54ff` reached `LOCAL_DELETED` on attempt
three with no current error. A second retry returned `NO_ELIGIBLE_CHUNKS`
without changing the attempt count or timestamps. Formal verification reported
both physical-test artifacts verified and backlog zero.

## Short-term fault matrix

The consolidated offline/local-only matrix passed 128 tests:

| Scenario | Evidence |
| --- | --- |
| Spot network disconnect | reconnects with a new connection ID; Raw retained |
| USD-M network disconnect | reconnect path preserves duplicate/out-of-order Raw |
| DNS failure | injected `socket.gaierror`, backoff, reconnect and Raw persistence |
| `serverShutdown` | exact frame persisted before reconnect |
| missing/duplicate/out-of-order Depth | gap, duplicate and ordering tests |
| snapshot 429 / 418 / 5xx | Retry-After/ban block and classification tests |
| snapshot cancellation/concurrency/buffer cap | single-flight and restart tests |
| Collector/Raw writer SIGKILL | partial recovery matrix |
| Archive Manager SIGKILL | copy/verify/Catalog boundary recovery matrix |
| Catalog commit failure | source retention and idempotent retry |
| checksum failure | quarantine/corruption and archive verification tests |
| read-only external target | registration/status refusal simulation |
| external disappearance | deterministic simulation plus the physical ExFAT test |
| blue/green candidate failure/unready | old instance remains running |
| sleep/wake | notification, clock-gap and runtime continuity tests |

The matrix used fixtures, local loopback servers and the dedicated external
directory only. It made no Binance request and started no long-duration clock.

## Final short-term regression

- Complete offline pytest: 316 passed, five explicit online skips, one stress
  deselection. The five warnings are the known Python multi-threaded `fork()`
  deprecation warning in Archive SIGKILL tests.
- Ruff: passed.
- Strict mypy: passed for 76 source/tool files.
- M0-M16 contract verifier: passed.
- Independent Go Raw v1 golden verifier: passed.
- `git diff --check`: passed.
- Recorder/worker process leak: none.
- Dedicated-directory open handles: none.
- Dedicated-directory transient residuals: no `.copying`, `.partial`,
  `.m17-probe-*`, or symbolic links.
- External consumer repository: inspected read-only; its pre-existing frontend
  changes were not modified.

The internal worker status/release/log controls were deleted. The marker, two
verified Raw test artifacts and their two external manifests remain in the
dedicated M17 directory. They are not transient residuals: Catalog marks them
`LOCAL_DELETED`, so deleting the external artifacts would make the Catalog and
verification contract false and would remove the only remaining copies.
They remain explicitly disposable M17 fixtures pending an audited fixture
retirement decision; no file outside the dedicated directory was touched.

M17 short-term functionality and fault injection are accepted. Continuous
72-hour and 168-hour operation was not executed and is deferred under R-035.
Static review, deterministic tests, fault injection and short online runs do
not replace that evidence. The current version is not suitable for real-money
trading.
