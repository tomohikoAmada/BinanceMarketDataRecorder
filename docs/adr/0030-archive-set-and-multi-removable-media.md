# ADR-0030: Archive Set and multi-removable-media model

- Status: Accepted
- Date: 2026-08-17
- Relates to: ADR-0003, ADR-0014, ADR-0015, ADR-0017, ADR-0029

## Context

Long-term Recorder Raw may exceed one removable HDD, SSD, or USB device. The
archive therefore needs a logical collection identity that can span media while
keeping each immutable Raw chunk physically whole and independently
interpretable. A workspace index is useful but cannot be the only durable
authority because it may be lost.

## Decision

An **Archive Set** is a logical continuous archive collection identified by
`archive_set_id`. Each physical archive medium has its own registered identity,
`storage_id`. A medium-local identity/manifest records at least its
`archive_set_id`, its own `storage_id`, and the artifacts/manifests it contains.

One immutable Raw chunk remains whole on one physical medium. The system does
not stripe a chunk across media. Different media may contain different
portions of continuous history in one Archive Set.

A central/global index may accelerate discovery, but it is convenience state.
The global index must be rebuildable from durable media-local metadata and
manifests. Losing a local workspace index must not make a medium or its
artifacts uninterpretable. Archive Set membership and physical storage
identity are part of archive receipt and verification reasoning, not consumer
data semantics.

Archive Set support is a future archive-client capability. Existing
single-registered-directory/macOS and Linux storage identities remain valid
physical-medium primitives; they are not silently reinterpreted as a complete
Archive Set implementation.

## Invariants

- `archive_set_id` identifies the logical collection; `storage_id` identifies
  one physical medium.
- Every archived Raw artifact has one unambiguous physical `storage_id`.
- A Raw chunk is never split across media.
- Media-local metadata and manifests are durable evidence and sufficient to
  identify the medium and interpret its contents.
- A global index is rebuildable and never the sole authority.
- Archive Set metadata cannot weaken Raw hash, manifest, or receipt checks.
- Archive Set support does not imply redundancy, RAID, replication, or backup.

## Responsibilities

### Archive client

Maintains logical set membership, registers each physical medium, writes only
inside the selected medium's Recorder directory, and commits media-local
metadata before authorizing deletion.

### Physical medium

Carries its own identity marker, Archive Set metadata, immutable Raw artifacts,
and archive manifests. It remains interpretable when detached from the local
workspace index.

### Offline Workspace index

Provides convenience discovery and set-level queries. It can be rebuilt by
scanning media-local metadata and manifests and therefore cannot be the only
source of truth.

### VPS Recorder

Knows the source Raw and Catalog lifecycle. It does not own the entire Archive
Set or assume all media are simultaneously online.

## Explicitly excluded scope

- RAID, striping, distributed storage, erasure coding, automatic replication;
- automatic media mounting, formatting, repair, or exclusive volume ownership;
- requiring all Archive Set media to be present for a valid individual chunk;
- treating a single Archive Set copy as redundant backup;
- changing public Raw/EventEnvelope or consumer contracts.

## Consequences and tradeoffs

Media rotation is operationally simple and preserves whole-file recovery, but
queries spanning history may require locating and attaching several media.
Self-describing media costs metadata space and write coordination, while it
prevents an index loss from becoming a data-interpretation loss. The model
supports continuity without pretending to provide fault tolerance.

## Relationship to existing ADRs

ADR-0003 and ADR-0014 define the registered-directory, UUID/marker, and
`storage_id` identity of an individual physical target. ADR-0015 defines the
verified artifact transaction. ADR-0017 defines the implemented macOS safe
eject behavior. This ADR adds the logical `archive_set_id` above those physical
identities. ADR-0029 uses both identities in receipt binding and deletion
authorization.

## Migration and implementation implications

Future implementation must define media-local metadata and a rebuildable
workspace index without changing existing Raw artifacts. It must make
Archive Set membership explicit when a medium is registered and preserve
single-medium operation. Existing archive manifests remain valid and can be
associated with a set through additive archive metadata rather than a Raw
format change.

## Validation requirements

- Register multiple media with one `archive_set_id` and distinct `storage_id`s.
- Prove a chunk is never split and a target collision cannot bind to another
  medium or set.
- Detach one medium and confirm unrelated media and already verified chunks
  remain interpretable.
- Delete the workspace index, rebuild it from media-local metadata/manifests,
  and compare identities and artifact inventory.
- Exercise media insertion, removal, rename/remount, read-only, corruption,
  and safe-eject failure paths without claiming redundancy.
