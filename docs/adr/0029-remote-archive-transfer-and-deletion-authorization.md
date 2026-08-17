# ADR-0029: Remote archive transfer and VPS source-deletion authorization

- Status: Accepted
- Date: 2026-08-17
- Relates to: ADR-0003, ADR-0015, ADR-0016, ADR-0028, ADR-0030

## Context

The implemented archive transaction is local to a Recorder process and a
registered directory. The approved future architecture moves the live Recorder
to a VPS while the durable archive is maintained by a local client. Transport
success alone cannot authorize deletion of the VPS Raw source: the local copy
must become durable, independently verified, identity-bound archive authority
first.

The design must preserve the existing Raw, manifest, Catalog, and crash
recovery guarantees without coupling integrity semantics to an SSH script or a
future authentication system.

## Decision

Direction is **LOCAL CLIENT -> pulls -> VPS**. V1 transport is SSH through an
ordinary CLI-capable shell. A restricted SSH user, custom authentication server,
HTTP API, Firebase integration, and Web UI are explicitly not part of this
freeze.

The archive client is structured around this seam:

```text
Archive Set / verification / deletion authorization / receipt
                           |
                    RemoteTransport
                           |
                       SSH (V1)
```

Archive integrity logic selects an immutable sealed Raw chunk and its manifest,
requests or pulls the source through `RemoteTransport`, writes a target
temporary artifact on a local registered medium, fsyncs as supported by the
contract, reopens and reads it, verifies stored size and SHA-256, verifies the
Raw manifest identity, commits the Artifact and Archive Set identity, and
durably commits a local receipt. Only then may the client send a deletion
authorization to the VPS.

The VPS validates the exact receipt and revalidates that the source has not
changed since selection. Only after successful validation does the VPS
authorize source deletion and record the resulting Catalog state. A failed,
ambiguous, stale, or mismatched receipt retains the VPS source.

The receipt/authorization binding must contain sufficient immutable identity to
prevent deleting a different source. At minimum it reasons about:

- `chunk_id`;
- Raw stored hash and Raw manifest identity/hash;
- `archive_set_id` and physical `storage_id`;
- target artifact identity/path;
- archive session identity; and
- verification outcome/version.

The receipt is evidence of a specific archive transaction, not a general
permission to delete a path. The exact serialized receipt format remains an
implementation concern of the future contract implementation; no new public
market-data schema is introduced here.

After verified local archive and successful deletion authorization, the VPS Raw
may be deleted immediately. No mandatory VPS grace period is required. One
verified durable local copy is sufficient to authorize deletion, but
**ARCHIVE != BACKUP** and documentation must warn that the local archive may
become the only copy.

After each successful archive session, the VPS must produce a consistent
post-session Catalog SQLite snapshot using a SQLite-supported backup/snapshot
mechanism. Copying a live WAL database with raw `cp` is not valid. The snapshot
is transferred and verified locally; V1 retains at least `latest` and
`previous` post-session snapshots. Snapshot failure does not undo successful
Raw archival or deletion; it is surfaced and retryable. Catalog snapshots are
operational recovery evidence, never Raw authority.

## Invariants

- Exact Raw bytes and immutable manifests remain recoverable authority.
- No source deletion follows from SSH success, file existence, name equality,
  or size equality alone.
- Local durability, readback, size/hash verification, manifest identity,
  Archive Set identity, receipt durability, VPS receipt validation, and source
  revalidation precede deletion authorization.
- An unknown or incomplete transfer remains a gap/failure and never becomes
  `COMPLETE` by implication.
- Receipt identity binds the source, destination, session, and verification
  outcome tightly enough to prevent wrong-source deletion.
- Transport/authentication can be replaced without rewriting archive integrity
  or deletion semantics.
- A Catalog snapshot never replaces Raw or its manifests.

## Responsibilities

### Local archive client

Selects eligible VPS sealed chunks, performs local durable verification,
commits Archive Set metadata and physical artifact state, stores receipts,
requests deletion authorization, verifies Catalog snapshots, and reports
failures without deleting unverified data.

### RemoteTransport

Moves bytes and receipt/authorization messages while exposing only transport
success/failure and the data needed by the archive contract. V1 is SSH. It does
not decide verification, Archive Set identity, or deletion eligibility.

### VPS Recorder

Exposes selected immutable sealed Raw and manifests, validates deletion
authorizations against current source identity, performs authorized deletion,
records resulting Catalog state, and creates post-session Catalog snapshots.

### Operator

Controls SSH access, local archive media, storage registration, and retry or
eject decisions. The operator does not edit Raw or manufacture receipts.

## Explicitly excluded scope

- production code or deployment;
- a custom SSH protocol, restricted account, HTTP API, or web UI;
- automatic replication, RAID, erasure coding, or a mandatory second copy;
- deleting unverified VPS Raw or recovering space by deleting unarchived Raw;
- changing EventEnvelope, Raw, manifest, Catalog market-data, normalized, or
  replay schemas;
- automatic notification delivery.

## Consequences and tradeoffs

Pulling from the local client keeps removable-media availability and operator
control out of the latency-critical VPS path. It requires the client to be
online for archive progress and requires a carefully bound receipt exchange.
SSH is simple for V1 but is a replaceable transport, not the archive contract.
Immediate VPS deletion limits disk pressure but makes the local archive a
possible single copy, so independent backup policy remains outside Recorder's
authority.

## Relationship to existing ADRs

ADR-0003 and ADR-0015 remain authoritative for registered-directory identity,
copy/readback/hash/manifest semantics, and crash-reconcilable local deletion.
This ADR extends those semantics across a remote VPS/local-client boundary and
prospectively supersedes their same-host assumption. ADR-0028 assigns the VPS
live responsibilities. ADR-0030 defines the Archive Set and physical-media
identities. ADR-0021 remains the consumer boundary and hides physical archive
location.

## Migration and implementation implications

Future implementation must introduce a transport-neutral archive seam, a
receipt and authorization lifecycle, remote source revalidation, and a
SQLite-supported post-session Catalog snapshot workflow. Existing local
archive behavior must remain readable and recoverable during migration. No
implementation may make SSH command strings part of Raw or manifest identity.

## Validation requirements

- Fault-inject transfer interruption, local fsync/readback/hash failure,
  manifest mismatch, target collision, receipt loss/replay, source mutation,
  VPS restart, and Catalog snapshot failure.
- Prove every failure retains the VPS source unless the complete authorization
  chain is durable and identity-exact.
- Prove receipt replay cannot delete a second chunk or a different storage
  target.
- Verify `latest` and `previous` snapshots represent post-session state and
  are independently readable.
- Run the same archive transaction tests with SSH and a local/fake
  `RemoteTransport` implementation.
