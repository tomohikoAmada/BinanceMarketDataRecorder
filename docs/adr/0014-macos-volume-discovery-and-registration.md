# ADR-0014: macOS volume discovery and registered-directory readiness

- Status: Accepted
- Date: 2026-07-22
- Milestone: M9

## Context

The Recorder must recognize an optional external archive folder after volume
removal, rename, or mountpoint change without treating `/Volumes/<name>` as an
identity. It must observe lifecycle events, distinguish read-only mounts, and
prove filesystem capabilities without writing outside the selected folder.
Polling alone cannot establish the required Disk Arbitration event semantics.

Primary platform references retrieved 2026-07-22 are Apple's documentation for
[`DARegisterDiskAppearedCallback`](https://developer.apple.com/documentation/diskarbitration/daregisterdiskappearedcallback),
[`DARegisterDiskDisappearedCallback`](https://developer.apple.com/documentation/diskarbitration/daregisterdiskdisappearedcallback%28_%3A_%3A_%3A_%3A%29),
[`DARegisterDiskDescriptionChangedCallback`](https://developer.apple.com/documentation/diskarbitration/daregisterdiskdescriptionchangedcallback),
[`DADiskCopyDescription`](https://developer.apple.com/documentation/diskarbitration/dadiskcopydescription),
and
[`DASessionScheduleWithRunLoop`](https://developer.apple.com/documentation/diskarbitration/dasessionschedulewithrunloop).
The selected bridge is
[`pyobjc-framework-DiskArbitration` 12.2.1](https://pypi.org/project/pyobjc-framework-DiskArbitration/12.2.1/),
whose matching core and Cocoa bridge versions are locked with wheel hashes.

## Capability evidence

On the certified macOS 26.5.2 Apple Silicon/Python 3.12.9 host, an unprivileged
Python process created a `DASession`, registered PyObjC closures for appeared
and disappeared callbacks, scheduled the session on a Core Foundation run
loop, and received 21 startup disk objects. Description values included volume
UUID, name, filesystem kind, volume path, media writability/size, internal and
removable flags. The automated platform test repeats session creation,
registration and non-mutating startup delivery.

No physical external volume was attached during M9. Consequently actual cable
removal/reinsertion and physical APFS/exFAT/read-only media tests are explicitly
unrun, not replaced by a claim of physical validation. Offline model tests cover
the state transitions, UUID remount resolution and scope rules. M12 must
separately prove unmount/eject callbacks before exposing safe eject.

## Decision

- Use exact-pinned PyObjC 12.2.1 to bridge Apple's Disk Arbitration framework.
- Startup discovery schedules appeared callbacks; the long-running adapter also
  registers description-changed and disappeared callbacks. No polling fallback
  is represented as equivalent.
- Expose only volume objects explicitly described as non-internal and having a
  volume UUID. Unknown/internal objects are never registration candidates.
- Use `DADiskCopyDescription` for UUID, observed name/filesystem, mountpoint,
  media properties and size. For a mounted volume, combine media writability
  with the actual `statvfs` read-only flag and filesystem capacity.
- Persist registration metadata in the additive Catalog `storage_targets`
  table. Identity is `storage_id` + UUID + relative path + marker nonce; name,
  filesystem and mountpoint remain observations.
- The marker `.binance-market-data-recorder-storage.json` is created inside an
  already-existing selected folder. Registering the volume root is forbidden.
- `READY` requires marker agreement and a current in-folder write, file fsync,
  atomic rename, directory fsync, reopen/readback and cleanup probe. A read-only
  observation returns `READ_ONLY` without attempting the probe.
- `storage list` and `storage inspect` are read-only. `storage register` is the
  explicit authorization to probe and create a marker. `storage status` may
  repeat the probe only inside registered folders. `unregister` removes Catalog
  eligibility but deliberately retains marker and archived files.
- M9 defines the complete public state vocabulary, while M10-M12 own archive,
  low-space forecasting, and eject transitions. M9 does not copy or delete Raw.

## Consequences

The same UUID and registered relative path resolve after volume-name or
mountpoint changes. A UUID/marker/storage-id mismatch blocks `READY`. An absent
or unmounted volume is normal storage state and does not affect Collector
status. The additional PyObjC runtime is macOS-conditional and exact-pinned as
one tested bridge set.

Disk Arbitration callbacks execute on the adapter run-loop thread; callers must
keep handlers short and hand work off. M9 does not install a background monitor
because launchd/service assembly is M14.

## Alternatives rejected

- Fixed `/Volumes/<name>` paths: aliases the wrong disk after rename/remount.
- Polling mount directories: does not meet lifecycle event semantics.
- `diskutil` subprocess polling: unnecessary and weaker than direct callbacks.
- Volume-root marker or dedicated volume ownership: violates the shared-volume
  contract.
- Filesystem allowlist: capability behavior matters more than APFS/exFAT labels.

## Rollback

Disable registration commands and revert M9. Existing marker files and the
additive `storage_targets` table may remain inert; rollback does not delete them
or any archive data. Internal capture is independent and continues.
