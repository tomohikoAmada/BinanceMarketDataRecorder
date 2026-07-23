# ADR-0017: Non-forced macOS safe eject

- Status: Accepted
- Date: 2026-07-23
- Milestone: M12

## Context

An archive target is optional and may become the only Raw copy after verified
local deletion. Eject must therefore serialize against archive allocation,
durably close the Recorder's work, and distinguish an operating-system
confirmation from a refusal or physical disappearance. A fixed mountpoint,
successful `unmount(2)` assumption, or disappearance callback is not sufficient
evidence that media is safe to remove.

The macOS 26.5 SDK `DiskArbitration.h` defines `DADiskUnmount` and `DADiskEject`
as asynchronous operations. Their callbacks receive a non-null `DADissenter`
on failure and null on success. The same header defines
`kDADiskUnmountOptionForce` as unmounting even while files are active. PyObjC
12.2.1 exposes these exact callback signatures plus dissenter status and status
text. M12 verified the installed SDK headers with `xcrun --show-sdk-path` and
the binding metadata with Python introspection.

## Decision

Catalog stores one `storage_control` row per target when needed. The default
state is `ACTIVE`. `begin_storage_eject` runs under `BEGIN IMMEDIATE`: it
rejects a target with any nonterminal archive transaction as `BUSY`, otherwise
sets `EJECT_PENDING`. `reserve_archive_transaction` checks the same row inside
its reservation transaction. Thus either archive reservation wins and eject
is refused, or eject wins and no new archive work can be allocated.

Immediate eject does not wait on or kill an archive worker. The user completes
its crash-reconcilable M10 transaction with `archive retry`, then retries
eject. This avoids cancellation semantics that could abandon an open copy.
`COPYING`, `VERIFYING`, `VERIFIED`, and `LOCAL_DELETE_PENDING` are all busy;
only `LOCAL_DELETED` is terminal.

Once latched, Recorder:

1. revalidates UUID, relative path, marker, `storage_id`, and nonce;
2. fsyncs its existing `raw`, `manifests`, and registered-root directories;
3. checkpoints the internal Catalog WAL and fsyncs its state directory;
4. calls `DADiskUnmount` with `kDADiskUnmountOptionDefault`;
5. only after a null unmount dissenter calls `DADiskEject` with
   `kDADiskEjectOptionDefault`;
6. reports `SAFE_TO_REMOVE` and “可以拔出” only after both callbacks succeed.

No force or whole-disk option is used. Default single-volume unmount avoids
silently unmounting sibling volumes; media eject may consequently be refused
when another volume remains active. A refusal clears the allocation latch and
records dissenter evidence. If unmount succeeds but eject is refused, the
result is still not safe-to-remove. Physical disappearance without both success
callbacks is `FORCED_REMOVAL`, not success, and preserves the internal source.

A successful eject retains `SAFE_TO_REMOVE` while absent or unmounted. After
the same UUID returns and marker/capability readiness succeeds at its current
mountpoint, the registry atomically returns the target to `ACTIVE`; archive
allocation may resume. A crash while `EJECT_PENDING` remains conservatively
blocked until a verified ready status or a repeated explicit eject resolves it.
Likewise, a callback timeout or exception after the system request retains
`EJECT_PENDING`: the asynchronous operation might still complete, so reopening
archive allocation would race a late unmount. A confirmed physical
disappearance is recorded separately and remains non-success.

## Consequences

- The CLI returns zero only for confirmed `SAFE_TO_REMOVE`; busy, refused, and
  forced-removal outcomes are structured nonzero results.
- Recorder never formats, repairs, remounts, or force-unmounts media.
- Internal collection is independent of eject and continues on the internal
  spool.
- Other processes may still hold a volume; Disk Arbitration remains the final
  authority and may refuse.
- M12 proves the adapter callback bridge with deterministic fakes and the live
  machine's framework metadata. Physical APFS/HFS+/exFAT/read-only and busy-app
  device testing remains part of M17.

## Alternatives rejected

- Shelling out to `diskutil`: weaker structured callback/dissenter evidence and
  unnecessary process parsing.
- `kDADiskUnmountOptionForce`: violates the busy/refusal safety contract.
- Whole-disk unmount: could affect unregistered sibling volumes.
- Treating disappearance or unmount alone as success: media may have been
  forcibly removed or eject may have failed.
- Cancelling or deleting an in-flight `.copying` transaction: M10 recovery and
  internal-source retention are safer and already deterministic.

## Rollback

Disable the eject CLI and leave passive disappearance/reinsertion recovery
enabled. Users may eject through macOS. Preserve `storage_control`, archive
transactions, Raw artifacts, manifests, and registrations; never delete a
source as part of rollback.
