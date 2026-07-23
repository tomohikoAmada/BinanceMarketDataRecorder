# ADR-0015: Crash-reconcilable archive transaction

- Status: Accepted
- Date: 2026-07-23

## Context

A sealed internal Raw chunk may be copied to a removable, user-registered
directory. The process, SQLite transaction, or volume can disappear at any
instruction boundary. A file name, successful write call, or size match alone
does not authorize deletion of the internal source.

## Decision

Each eligible `SEALED` chunk receives one deterministic transaction identity
for one `storage_id`. Catalog advances the transaction and chunk together:

```text
COPYING -> VERIFYING -> VERIFIED -> LOCAL_DELETE_PENDING -> LOCAL_DELETED
```

The target layout is limited to `raw/` and `manifests/` below the registered
root. Copy uses an owned `.copying` name, bounded streaming I/O, file and
directory fsync, close/reopen full readback, stored-size and SHA-256 comparison,
then an in-directory atomic rename. A versioned external manifest commits the
archive identity, verification facts, source-manifest SHA-256, and the exact
source manifest bytes.

The verified Catalog transition is committed only after the final artifact and
external manifest are durable. Local deletion is a later state transition. It
first revalidates target marker, final artifact, external manifest, internal
source size, and internal source hash. Source unlink and its directory fsync
precede the separately recorded `LOCAL_DELETED` transition.

On restart, filesystem facts are reconciled from the saved state. A matching
final artifact or manifest is reused only after full validation. A conflicting
file is never overwritten. Only the exact transaction-owned temporary name may
be removed; unrelated partial-looking files are untouched. Stable metric batch
IDs prevent retry from double-counting archive or deletion bytes.

The internal Raw chunk manifest remains after local artifact deletion as
immutable provenance and is embedded byte-for-byte in the external manifest.
It is not treated as a second Raw data copy.

## Consequences

- A failed copy, readback, identity check, external-manifest commit, or verified
  Catalog commit leaves the internal Raw artifact intact.
- A crash after verified commit can safely resume local deletion; a crash after
  unlink but before its Catalog transition reconciles the already absent source.
- External disappearance pauses archive work and is reported as
  `DISAPPEARED_DURING_COPY`; it does not enter Collector failure handling.
- Once `LOCAL_DELETED` is committed, the external artifact may be the only Raw
  copy. CLI/status and documentation must state this explicitly.
- `archive retry` advances at most one oldest transaction/chunk per invocation.

## Alternatives rejected

- Move/rename directly from internal to external: not portable across
  filesystems and provides no independently verified copy.
- Delete immediately after target rename: omits manifest and Catalog durability
  evidence.
- Trust an existing same-name target by size: hash and provenance can differ.
- Sweep all `.copying` files: may delete unrelated user data.
- Store Raw event bodies in SQLite: violates the Catalog boundary and scales
  poorly.

## Rollback

Stop allocating new transactions. Reconcile any existing transaction using its
Catalog row and exact owned paths; retain every internal source that has not
reached verified deletion authorization. Reverting code must not remove
external artifacts, manifests, markers, or unrelated files.
