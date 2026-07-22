# ADR-0003: Registered-directory external archive

- Status: Accepted
- Date: 2026-07-22

M0.2 scope note: archive identity is storage-functional and independent of
downstream consumers. Marker/service names follow ADR-0007 and must not imply
ownership or endorsement by Binance.

## Context

Users may share an existing APFS, HFS+, exFAT, or other macOS-writable volume.
The disk can disappear at any time, can remount under a different name/path, or
can be read-only. Collector continuity must not depend on it.

## Decision

External storage is always an optional, explicitly registered directory. Its
identity combines volume UUID, observed volume name/filesystem, directory path
relative to the volume root, an in-directory marker, and `storage_id`.
Mountpoint is dynamically resolved by UUID using a macOS adapter. Capability
probes occur only in the registered directory.

Archive uses copy, fsync, full readback size/SHA-256 verification, atomic
rename, external manifest, Catalog commit, then separately recorded local
delete. The internal Collector never writes directly to external storage.

## Consequences

- Removal pauses archival but not capture.
- Volume renames/mountpoint changes remain discoverable.
- Recorder does not require or alter a particular filesystem format.
- After verified local deletion, the external file can be the only copy; users
  need an independent backup policy.

## Alternatives rejected

- Dedicated/formatted volume: destructive and outside scope.
- Fixed `/Volumes/<name>`: ambiguous and unstable.
- Direct active recording to external disk: makes unplugging a capture fault.
- Size-only verification: cannot establish byte identity.

## Rollback

Unregistering prevents new work but does not delete archive data or marker
without a separate future, explicit policy. Existing verified locations remain
in Catalog/manifests.
