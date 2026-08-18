# Storage Contract

## Internal storage is authoritative for ingestion

The Collector writes only to the internal root. The internal spool is the set
of data durably written internally but not both externally verified and locally
deleted. There is no fixed minimum retention duration. If no external archive
is ready, the spool grows until space policy requires an explicit emergency
stop; absence of an external target is not a Collector error.

Allowed production root:

```text
macOS interactive: ~/Library/Application Support/BinanceMarketDataRecorder/
Linux interactive: ~/.local/share/BinanceMarketDataRecorder/
Linux systemd:     /var/lib/binance-market-data-recorder/
```

The primary future production host is Ubuntu 24.04 LTS x86_64 on a shared
2 vCPU/4 GiB/40 GB-class VPS. The interactive macOS and Linux roots above
remain valid local profiles; the Ubuntu ARM64/RK3588 root is a distinct
validation/historical profile, not the production authority.

Forbidden defaults include the repository, its parent
`/Users/amada/Documents/Development/Crypto`, Desktop, Documents, iCloud Drive,
and `/tmp` for persistent data.

When a Linux checkout is directly below `$HOME`, the repository itself remains
forbidden but `$HOME` cannot be treated wholesale as a workspace because that
would reject the XDG default by construction.

## Chunk states

```text
ACTIVE.partial
  -> RECOVERED.partial (optional, tail truncated with evidence)
  -> SEALING
  -> SEALED
  -> ARCHIVE_COPYING
  -> ARCHIVE_VERIFYING
  -> ARCHIVED_VERIFIED
  -> LOCAL_DELETE_PENDING
  -> LOCAL_DELETED
```

Quarantine is terminal for automatic processing but preserves evidence. All
transitions are idempotent and reconciled against both filesystem and Catalog.
ACTIVE, SEALING, unverified, checksum-failed, or unarchived data cannot be
deleted.

M3 implements `ACTIVE`, `RECOVERED`, `SEALING`, `SEALED`, and `QUARANTINED`
for the internal Raw lifecycle. M10 implements all archive states through
`LOCAL_DELETED` under ADR-0015.
Catalog paths are relative to the selected internal root, and SQLite stores
chunk lifecycle metadata/transitions only—not market-event bodies.

M6 checkpoints are derived files below `data/checkpoints/`. They are written to
an in-directory `.partial`, flushed and fsynced, atomically renamed, and then
registered as metadata in Catalog. A checkpoint includes source Raw chunk
hashes and is refused for an unreliable book. It may be deleted and rebuilt;
Raw chunks remain immutable and authoritative.

M15 normalized outputs are derived files below
`data/normalized/normalized-dataset.v1/`. Work files stay under its `.work`
directory and are disposable after interruption. Final Parquet partitions and
their manifests are content-addressed, written through in-directory unique
partials, fsynced, reopened for logical verification, atomically renamed, and
never modified in place. A build may read an internally retained Raw artifact
or the same content from a verified currently READY M10 archive; manifests
record content hashes and relative Recorder paths, never a mountpoint.
Unavailable or unverified Raw aborts the build. Deleting normalized artifacts
does not authorize deletion or mutation of Raw.

M16 consumers configure only the internal Recorder application-data root and
an explicit normalized build ID. `ManifestCatalog` resolves build-relative
partition/checkpoint paths after containment and hash checks. Public
descriptors never expose an external Raw archive mountpoint, archive
transaction or `storage_id`; consumers do not decide where Raw resides.
Replay uses only ephemeral non-persistent sort work and never modifies the
internal root.

## External target identity and access boundary

An archive registration contains:

- application-generated `storage_id`;
- reliable filesystem/volume UUID;
- source device on Linux;
- observed volume name and filesystem type;
- registered directory relative to the volume mount root;
- a marker file inside the registered directory;
- marker schema/version and random identity nonce.

Volume name/mountpoint are observations, not identity. UUID and marker must
agree before use. Recorder only scans, creates, copies, verifies, renames, and
deletes its own known files within the registered directory. It never writes
at the volume root or accesses unrelated siblings.

Usable filesystems are capability-based, not allowlisted: the OS must mount the
volume writable, the directory must be accessible, and an in-directory probe
must pass write, fsync, rename, reopen, and readback. A read-only mount reports
`READ_ONLY`; Recorder never remounts, repairs, or reformats it.

On Linux, `/proc/self/mountinfo` is the current mount-namespace authority and
`findmnt --json` plus `lsblk --json` corroborate source, filesystem, UUID, and
external/hotplug identity. Recorder considers only user/OS already-mounted
external block filesystems and requires a reliable filesystem UUID before
registration. It never mounts/unmounts, creates udev rules, or changes
partition/filesystem state. Missing media leaves Active Collection running;
archive status becomes absent/failed and the internal sealed source remains.

## Storage states

At least these public states are required:

`ABSENT`, `PRESENT_UNMOUNTED`, `MOUNTED`, `UNREGISTERED`, `PROBING`, `READY`,
`READ_ONLY`, `LOW_SPACE`, `COPYING`, `VERIFYING`, `EJECT_PENDING`,
`SAFE_TO_REMOVE`, `DISAPPEARED_DURING_COPY`, `DEGRADED`, and `ERROR`.

State transitions include evidence and timestamps. `READY` means identity and
capability probes passed at the current mountpoint; it is not inferred from a
path merely existing.

M21.0 adds a strictly observational Soak path without weakening that readiness
contract. Soak opens the existing Catalog through SQLite `mode=ro` with
`query_only=ON`, reads registration/control state and validates the existing
marker, mount, writability and capacity, but does not run the in-directory
capability probe, call storage activation, or modify `storage_control`. Normal
Archive and `storage status` paths retain the full probe and activation safety
behavior. An unregistered configured `storage_id` is `NOT_REGISTERED` evidence,
not evidence that a registered disk is physically `ABSENT`.

M9 implements the public state vocabulary, UUID-based resolution, Catalog
`storage_targets`, and the marker
`.binance-market-data-recorder-storage.json`. `storage list` displays
unregistered external volumes without writing. Registration requires an
existing folder below the volume root; its probe creates, fsyncs, renames,
reads, and removes only a unique temporary file inside that folder. Unregister
removes Catalog eligibility but preserves the marker and user/archive data.
M10 implements copy/verify and disappearance-during-copy behavior.
M11 implements `LOW_SPACE`. M12 implements `EJECT_PENDING` and
`SAFE_TO_REMOVE` under ADR-0017.

Capacity severity and archive eligibility are deliberately distinct:

- `OK` and `WARNING` targets remain `READY`; WARNING emits capacity evidence
  but does not stop a new verified archive transaction.
- `CRITICAL` and `EMERGENCY` targets report `LOW_SPACE`; Archive Drain starts
  no new transaction and exits successfully with `TARGET_LOW_SPACE`.

The current local storage implementation retains its M11 threshold behavior
for existing local targets. That behavior is not the approved universal VPS
policy: ADR-0028/0029 select the explicit free-byte and ETA thresholds below
for the future production VPS. Neither policy changes archive verification or
the Catalog state machine.

M20 does not map Linux to the macOS eject transaction. Without a proven udisks
capability, `storage eject` returns `MANUAL_ACTION_REQUIRED`,
`safe_to_remove=false`, and performs no unmount/eject mutation. Only macOS
Disk Arbitration's successful unmount plus eject callbacks may produce the
existing `SAFE_TO_REMOVE` claim.

The future Offline Workspace may contain several physical archive media. An
`archive_set_id` identifies the logical collection and `storage_id` continues
to identify one physical medium. A chunk remains whole on one medium. Existing
UUID/marker/relative-directory identity remains the physical access boundary;
Archive Set metadata and a rebuildable global index are additive archive-client
metadata responsibilities, not a change to this storage state machine. M22.2
stores the durable media-local identity and whole-chunk inventory inside the
registered directory and rebuilds the separate workspace index from attached
media; it does not change the registered-storage marker or Catalog schema.

## Archive transaction

For the oldest eligible sealed source:

1. Reserve an idempotent transaction and target relative path in Catalog.
2. Stream to a target `.copying` file inside the registered folder.
3. Flush and fsync the target file and required directory metadata.
4. Close, reopen, and read the complete target.
5. Compare stored size and SHA-256 with the source/manifest.
6. Atomically rename within the target directory to the final immutable name
   and fsync the containing directory.
7. Commit an external manifest that points to verified content.
8. Commit the verified external location and transaction in Catalog.
9. Attempt internal source deletion and record success separately.

No earlier step authorizes source deletion. Copy interruption, disk removal,
hash mismatch, name collision, Catalog failure, or delete failure must be
retryable without corrupting a good copy. Existing final names are accepted
only after full identity/hash verification; mismatches are never overwritten.
Residual temp files are cleaned only when Catalog/age/ownership prove they
belong to an abandoned Recorder transaction.

After local deletion, status must warn that the external target may be the only
remaining copy. Recorder archival is not itself a multi-copy backup policy.

For the approved future VPS topology, the local client pulls from the VPS over
SSH through a transport-neutral `RemoteTransport` seam. Local durability,
readback, size/SHA-256, Raw manifest identity, Archive Set/storage identity,
durable receipt, VPS receipt validation, and source revalidation precede VPS
source deletion. SSH success alone is never sufficient. The complete future
transaction is defined in `docs/archive_transfer_contract.md`; it is not
implemented by the current local ArchiveManager.

## Safe eject transaction

An immediate eject request is mutually exclusive with archive reservation in
Catalog. Any nonterminal archive transaction (`COPYING`, `VERIFYING`,
`VERIFIED`, or `LOCAL_DELETE_PENDING`) returns `BUSY`; the user completes it
with the existing idempotent archive retry before requesting eject again. With
no such work, `EJECT_PENDING` blocks every new reservation.

Recorder revalidates target identity, fsyncs its external archive directories,
checkpoints/fsyncs internal Catalog state, closes its handles, and requests
default non-forced Disk Arbitration unmount followed by eject. Both callbacks
must succeed before `SAFE_TO_REMOVE`/“可以拔出”. A dissenter, timeout, unmount
without eject, or physical disappearance never claims success and never
deletes internal Raw. No force/whole-disk option, format, repair, or remount is
allowed. A timeout retains `EJECT_PENDING` because a late asynchronous
completion remains possible; an explicit retry resolves it. The same UUID plus
valid marker and readiness probe after confirmed removal/reinsertion returns
the target to `ACTIVE` and resumes allocation.

M10 uses `raw/<sealed-name>` and
`manifests/<chunk-id>.archive-manifest.json` below the registered root.
`external-archive-manifest.v1` includes the exact Raw manifest bytes, its
SHA-256, the final artifact identity, full-readback evidence, and verification
time. The internal Raw manifest is retained after local artifact deletion as
provenance, not as another copy of the event data. `archive retry` advances one
oldest item, `archive status` is local/read-only, and
`archive verify <storage-id>` re-reads every committed artifact and manifest.

## Space measurements and forecasts

Measure internal and every registered external target independently. For the
primary 40 GB-class VPS profile, the initial internal policy is:

- `NORMAL`: free > 18 GiB;
- `WARNING`: free <= 18 GiB or ETA to hard reserve <= 7 days;
- `CRITICAL`: free <= 14 GiB or ETA <= 72 hours;
- `EMERGENCY`: free <= 12 GiB or ETA <= 24 hours;
- `HARD RESERVE`: free <= 10 GiB.

The most severe applicable state wins. Growth history includes at least 1 h,
6 h, 24 h, and 7 d windows with a documented robust median/EWMA method. Output
net local growth, archive backlog and oldest age, UTC threshold ETAs,
`INSUFFICIENT_DATA`, or `NOT_APPROACHING`; it never emits NaN/infinity as JSON.
For an external archive target, the existing target-readiness rules remain
separate from the VPS internal policy. The VPS policy is based on actual free
bytes, not Recorder ownership of the filesystem.

Emergency order is: suspend compaction/non-core derivation; prioritize archive
and deletion already authorized by verification; never delete unarchived raw;
at hard reserve, gracefully seal, stop collectors, emit `DISK_EMERGENCY_STOP`,
and open an explicit gap interval.

M11 currently implements ADR-0016 with persisted per-scope capacity samples.
The future VPS profile preserves the observed-growth method while selecting the
ADR-0028 thresholds. Each window
uses the median of consecutive net-consumption slopes after at least 80% time
coverage; the maximum available window median is the conservative operational
rate. Internal and every `external:<storage_id>` scope remain independent.
Severity changes are append-only alerts. Existing local M11 hard-reserve
calculation remains historical implementation behavior; the future VPS hard
reserve is the protected 10 GiB threshold. In both profiles, actions are
ordered seal, Collector stop, `DISK_EMERGENCY_STOP`, and gap open. No emergency
action deletes unarchived Raw.
