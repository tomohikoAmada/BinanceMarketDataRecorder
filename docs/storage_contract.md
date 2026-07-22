# Storage Contract

## Internal storage is authoritative for ingestion

The Collector writes only to the internal root. The internal spool is the set
of data durably written internally but not both externally verified and locally
deleted. There is no fixed minimum retention duration. If no external archive
is ready, the spool grows until space policy requires an explicit emergency
stop; absence of an external target is not a Collector error.

Allowed production root:

```text
~/Library/Application Support/Alpha101CryptoRecorder/
```

Forbidden defaults include repository paths, Desktop, Documents, iCloud Drive,
and `/tmp` for persistent data.

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

## External target identity and access boundary

An archive registration contains:

- application-generated `storage_id`;
- macOS volume UUID;
- observed volume name and filesystem type;
- registered directory relative to the volume mount root;
- a marker file inside the registered directory;
- marker schema/version and random identity nonce.

Volume name/mountpoint are observations, not identity. UUID and marker must
agree before use. Recorder only scans, creates, copies, verifies, renames, and
deletes its own known files within the registered directory. It never writes
at the volume root or accesses unrelated siblings.

Usable filesystems are capability-based, not allowlisted: macOS must mount the
volume writable, the directory must be accessible, and an in-directory probe
must pass write, fsync, rename, reopen, and readback. A read-only mount reports
`READ_ONLY`; Recorder never remounts, repairs, or reformats it.

## Storage states

At least these public states are required:

`ABSENT`, `PRESENT_UNMOUNTED`, `MOUNTED`, `UNREGISTERED`, `PROBING`, `READY`,
`READ_ONLY`, `LOW_SPACE`, `COPYING`, `VERIFYING`, `EJECT_PENDING`,
`DISAPPEARED_DURING_COPY`, `DEGRADED`, and `ERROR`.

State transitions include evidence and timestamps. `READY` means identity and
capability probes passed at the current mountpoint; it is not inferred from a
path merely existing.

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

## Space measurements and forecasts

Measure internal and every registered external target independently. Severity:

- `WARNING`: free percentage <= 40%;
- `CRITICAL`: free percentage <= 15%;
- `EMERGENCY`: free bytes <= `max(10 GiB, 5% of capacity)`.

The most severe applicable state wins. Growth history includes at least 1 h,
6 h, 24 h, and 7 d windows with a documented robust median/EWMA method. Output
net local growth, archive backlog and oldest age, UTC threshold ETAs,
`INSUFFICIENT_DATA`, or `NOT_APPROACHING`; it never emits NaN/infinity as JSON.

Emergency order is: suspend compaction/non-core derivation; prioritize archive
and deletion already authorized by verification; never delete unarchived raw;
at hard reserve, gracefully seal, emit `DISK_EMERGENCY_STOP`, stop collectors,
and open an explicit gap interval.
