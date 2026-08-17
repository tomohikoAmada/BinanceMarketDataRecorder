# Archive Transfer Contract

Status: approved future architecture; not implemented.

This document defines the integrity transaction for a local archive client
pulling immutable Raw from the production VPS. It deliberately does not define
an SSH script, custom wire protocol, API server, or credential format.

## Roles

- **VPS Recorder:** owns live acquisition, active/spooled Raw, sealing,
  manifests, Catalog, recovery, and source deletion after valid authorization.
- **Local archive client:** pulls selected sealed Raw, verifies and durably
  commits the local artifact, records the receipt, and requests deletion.
- **RemoteTransport:** moves source bytes and receipt/authorization messages.
  SSH is the V1 implementation choice, but archive integrity does not depend
  on SSH command construction.
- **Offline Workspace:** owns the local Cold Archive, derived datasets,
  historical imports, Catalog snapshots, and Archive Set discovery metadata.

## Source selection

Only an immutable, sealed Raw artifact with its manifest is eligible. The
selection records the source identity before transfer. Active `.partial`,
unsealed, quarantined, checksum-failed, or incomplete source material is not
eligible for deletion authorization.

## Integrity transaction

The conceptual order is:

```text
VPS sealed Raw + manifest selected
  -> local transfer complete
  -> local file durability/fsync as supported by this contract
  -> local reopen/readback
  -> stored size verification
  -> Raw stored SHA-256 verification
  -> Raw manifest identity/hash verification
  -> Archive Set + physical storage identity committed
  -> durable local archive receipt committed
  -> VPS validates exact receipt/authorization
  -> VPS revalidates unchanged source identity
  -> source deletion authorized
  -> VPS Catalog records resulting state
```

The target is written through a transaction-owned temporary artifact and is
published atomically. A final artifact with the same name is accepted only
after full identity and hash verification; it is never overwritten on a
mismatch. A local verification failure retains the VPS source and leaves
retryable evidence.

## Receipt binding

The durable receipt and the deletion authorization refer to one immutable
transaction. They must bind, at minimum, the following conceptual identity:

| Identity | Purpose |
| --- | --- |
| `chunk_id` | Selects the exact Raw chunk |
| Raw stored hash | Binds the physical bytes |
| Raw manifest identity/hash | Binds completeness/provenance metadata |
| `archive_set_id` | Binds logical archive membership |
| `storage_id` | Binds one physical medium |
| target artifact identity/path | Binds the local destination |
| archive session identity | Prevents cross-session replay |
| verification outcome/version | Binds the verification algorithm/result |

The future serialized receipt may add fields, but it must not omit the
identity needed to prevent wrong-source deletion. A receipt is not transferable
to another chunk, medium, set, target, or session.

## Failure boundaries

- Transfer interruption: retain VPS source; local temporary state is
  retryable or quarantined according to ownership evidence.
- Local fsync/readback/size/hash failure: do not publish or authorize delete.
- Manifest or identity mismatch: fail closed; never trust the target name.
- Local receipt durability failure: no deletion authorization is sent.
- VPS receipt mismatch, stale session, or changed source: retain source and
  surface the failure.
- VPS deletion failure after authorization: retain Catalog evidence and make
  the operation retryable without copying or deleting a different source.
- Post-session Catalog snapshot failure: do not undo verified Raw archival or
  deletion; surface the snapshot failure and retry it from the post-session
  state.

Unknown continuity or incomplete evidence remains an explicit gap/failure. No
failure path may label an interval complete merely because a transport exited
successfully.

## Catalog snapshots

After a successful archive session, the VPS creates a consistent SQLite backup
representing the resulting post-session state, including source deletion and
Catalog transitions. A raw filesystem copy of a live WAL database is not a
valid snapshot method. The local client verifies the snapshot and keeps at
least `latest` and `previous` snapshots. Snapshots are operational recovery
evidence; Raw and manifests remain the market-event authority.

## Portability and transport seam

The future client targets macOS, Linux, and Windows. Platform-specific volume,
fsync, path, and eject adapters may differ, but Archive Set identity,
verification, receipt binding, and deletion authorization remain portable
transaction semantics. SSH is replaceable by a future transport/authentication
implementation without rewriting those semantics.

## Current implementation boundary

The current Recorder implements a local registered-directory archive
transaction and Linux/macOS storage adapters. It does not implement this
VPS-pull workflow, Archive Set support, remote receipts, or Catalog snapshot
transfer. Existing Raw and public data contracts remain unchanged.

## Non-goals

No restricted SSH account, custom protocol, HTTP/Firebase/Web UI, automatic
replication, RAID, erasure coding, or backup guarantee is introduced here.
