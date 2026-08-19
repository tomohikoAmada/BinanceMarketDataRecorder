# Archive Transfer Contract

Status: approved architecture; M22.3 local receive/receipt, M22.4A durable
remote pending authority, and M22.4B exact Raw-only deletion/durability/recovery
are implemented. Remote transport and snapshots remain future work.

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
  -> VPS persists durable deletion-authorized/pending fact
       bound to exact receipt + exact source identity
  -> VPS revalidates unchanged source identity
  -> unlink exact source
  -> filesystem deletion durability (including parent directory)
  -> VPS Catalog records terminal deletion state
```

The durable VPS deletion-authorized/pending fact must exist before any source
unlink. It binds the exact validated local archive receipt and the exact
immutable VPS Raw/manifest source identity. Source revalidation remains
immediately before the destructive mutation; the pending fact does not replace
it. If revalidation fails after the pending fact is persisted, the source is
not unlinked and the pending transaction is retained for fail-closed recovery.
The implemented projection and its exact state names are defined below;
M22.4B now supplies the bounded destructive retry and terminal APIs.

The target is written through a transaction-owned temporary artifact and is
published atomically. A final artifact with the same name is accepted only
after full identity and hash verification; it is never overwritten on a
mismatch. A local verification failure retains the VPS source and leaves
retryable evidence.

The local publication gate is stronger than a pre-rename file fsync. The
transaction must establish, in order, durability of the temporary file,
close/reopen full readback, stored-size and SHA-256 verification, Raw
manifest/source identity verification, atomic publication, and durability of
the final published artifact namespace, including required parent-directory
metadata durability where the selected platform contract supports it. It must
then durably commit Archive Set/physical-media metadata, durably commit the
receipt, and durably commit the receipt's own namespace/containing metadata
required for the receipt to survive a crash. Only after those facts are
durable may the receipt participate in VPS deletion authorization. If the
selected platform cannot establish the required final-namespace or receipt
commit semantics, the workflow fails closed and does not create an
authorization-capable receipt.

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
- Crash after unlink and required filesystem/directory deletion durability but
  before the terminal Catalog deletion-state commit: restart reconciles
  idempotently to the terminal deleted state only when a matching durable
  pre-delete authorization/pending fact bound to the exact receipt and exact
  source identity exists and all available durable identity/evidence
  validates.
- Source absent without a matching durable pre-delete authorization/pending
  fact: unexplained source loss. Fail closed; never retroactively normalize it
  into an authorized deletion merely because a local receipt exists.
- Authorization identity, receipt identity, source identity, manifest
  identity, or durable state disagreement: fail closed; never delete a
  different source.
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

## Same-host and remote source-lifecycle boundary

The current same-host `ArchiveState` and `archive_transactions` semantics
remain unchanged and continue to represent only the ADR-0015 local registered-
directory transaction. Remote transfer itself does not mutate the VPS source
lifecycle: a selected VPS Raw chunk remains `SEALED` while zero, one, or
multiple remote copies may have occurred.

The M22 remote source lifecycle is frozen separately as:

```text
SEALED -> REMOTE_DELETE_PENDING -> REMOTE_DELETED
```

`REMOTE_DELETE_PENDING` means that exact pre-delete authorization is durable;
the source may still exist, or may be absent only in the authorized
crash-after-unlink/pre-terminal-commit recovery window. It does not claim that
deletion completed. `REMOTE_DELETED` means exact unlink and required deletion
filesystem/parent-directory durability completed before the terminal Catalog
fact was committed. Remote persistence is a separate projection and must not
be added to `ARCHIVE_CHUNK_STATES`, `ArchiveState`, or
`archive_transactions`; remote recovery therefore has its own semantic branch.
Same-host and remote archival mutually exclude one another through the
Catalog source-lifecycle transition from `SEALED`: if same-host archival first
acquires `SEALED` as `ARCHIVE_COPYING`, remote authorization cannot acquire it;
if remote authorization first acquires `SEALED` as
`REMOTE_DELETE_PENDING`, same-host archival cannot acquire it. No second
distributed locking system is introduced by this freeze.

## Current implementation boundary

The current Recorder implements a local registered-directory archive
transaction, Linux/macOS storage adapters, and a transport-neutral read-only
sealed Raw source identity/export kernel. The M22.1 kernel selects and fully
validates immutable `SEALED` sources and emits deterministic descriptor
identity plus exact manifest bytes. M22.2 adds media-local Archive Set
identity/inventory and a rebuildable explicit-path workspace index without
changing the live Catalog or existing external manifest schema.

M22.3 implements local fake/in-process stored-byte receive on Linux and macOS.
It streams to an exclusive same-directory temporary, requires exact length,
file fsync, close/reopen full readback, size and stored-SHA-256 verification,
exclusive no-clobber publication, parent-directory fsync, and final Raw
revalidation. It then commits a distinct `external-archive-manifest.v1`, the
unchanged M22.2 Archive Set entry, and an exact-field
`remote-archive-receipt.v1`. The archive manifest embeds the exact source
`raw-chunk-manifest.v1` bytes; `archive_manifest_sha256` and
`source_manifest_sha256` are separate authorities. Its deterministic receive
transaction ID is source/set/storage/path bound and independent of the UUID4
receipt session. Receipt revalidation starts from the exact M22.1 descriptor
material and re-establishes the physical marker, Archive Set medium and entry,
archive manifest and embedded source manifest, and full Raw size/hash chain.
The workspace index is not authority. Windows end-to-end receipt durability
fails closed as unsupported.

M22.4A stores the VPS pre-delete authority in the existing Catalog SQLite
database using separate additive `remote_archive_transactions` and
`remote_archive_events` tables. The exact canonical M22.3 receipt bytes are
retained; `receipt_id` is the remote transaction identity; authoritative reads
reparse the receipt and rederive the M22.1 descriptor digest from current
Catalog identity plus the exact retained source manifest. Physical
`chunks.state` remains `SEALED`. Same-host reservation and remote authorization
serialize through one `BEGIN IMMEDIATE` authority boundary, so only one can
own a chunk.

M22.4A creates only `REMOTE_DELETE_PENDING` and implements read-only
NORMAL/CASE A/B/C/D and terminal interpretation with explicit
`PRESENT_MATCHING`, `PRESENT_MISMATCH`, `ABSENT`, and `UNKNOWN` observations.
Pending remote sources are excluded from unarchived archive backlog while
their retained VPS bytes remain visible. It performs no source unlink,
delete-rename, deletion-parent durability operation, SSH, `RemoteTransport`,
or production transition to `REMOTE_DELETED`.

M22.4B adds a receipt-ID-only `RemoteDeleter`. It reloads the exact persisted
receipt, initial authorization event, current `SEALED` chunk/no-same-host
ownership, retained manifest, reconstructed M22.1 descriptor, and remote row.
For CASE A it anchors `layout.sealed` with a no-follow directory descriptor,
opens the exact direct Raw leaf relative to that descriptor with no-follow
flags, keeps the Raw descriptor open, and freshly verifies stored size,
stored SHA-256, decompressed byte count, and decompressed SHA-256. After a
final lifecycle recheck and held-fd/parent-relative leaf identity match it
performs the one exact Raw `unlink`, proves absence, fsyncs the same anchored
parent descriptor, and proves post-fsync absence before the terminal Catalog
transaction. The `raw-chunk-manifest.v1` file is never deleted or rewritten.

CASE B requires the same exact retained pending authority and manifest but can
only observe an already-absent leaf; it repeats parent fsync/absence proof and
terminalizes without unlink. Startup calls only this recovery-only CASE-B
entry. CASE A remains explicit operator/workflow retry and ordinary startup
never initiates a new deletion. Terminal state requires one canonical
`REMOTE_DELETE_PENDING -> REMOTE_DELETED` event atomically committed with the
row. K1-K5 real process-death recovery, the real post-unlink parent-fsync
failure, ambiguous SQLite commit readback, and concurrent same-receipt callers
are exercised using test-owned temporary Recorder roots. Linux and macOS are
supported only when all required POSIX primitives succeed; Windows fails
closed before unlink.

New binaries accept a legitimate pre-M22
Catalog with an empty remote projection and writable opens add both remote
tables atomically. A pre-M22 binary after remote state persists is not claimed
or generally supported; there is no downgrade relabeling.

## Non-goals

No restricted SSH account, custom protocol, HTTP/Firebase/Web UI, automatic
replication, RAID, erasure coding, or backup guarantee is introduced here.
