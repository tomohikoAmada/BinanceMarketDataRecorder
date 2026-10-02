# ADR-0036: Product-scoped normalized deduplication and build identity

- Status: Accepted engineering decision; Q2 independent implementation review pending
- Date: 2026-10-02
- Milestone: Q1
- Supersedes: ADR-0020 deduplication/build-identity rules for newly written builds

## Problem

Some v1 semantic keys, including REST depth snapshots and server shutdown
events, lack symbol or stream identity. Equal payloads from distinct products
can collapse or acquire a false identity conflict. Raw remains intact, but
the derived dataset may omit an otherwise valid product record. The old build
identity does not bind deduplication version and therefore cannot distinguish
a corrected build over identical Raw/checkpoints.

## Decision

New writers use `normalized-dedup.v2`. Before hashing any parser-provided
semantic identity, the single candidate boundary wraps it as:

```text
{venue, market, symbol, stream, identity: <existing per-kind semantic identity>}
```

This applies to valid, malformed and shutdown records alike. Within that
namespace, v1 duplicate-winner, source-provenance and logical-conflict rules
remain unchanged. Cross-product records remain separate. No consumer-specific
key or Binance transport/schema change is introduced.

New build IDs add `dedup_version` to the existing canonical identity containing
dataset version, sorted Raw identities and verified checkpoint identities.
Rows, Arrow metadata and build/partition manifests consistently declare v2.
Dataset, stream schema, writer profile, Raw and Catalog versions do not change.

The public replay Catalog branches only on the two implemented dedup versions:
v1 verifies the original build hash without a dedup field; v2 verifies the
new hash including that field. Unknown versions are refused. Old immutable
builds and artifacts are retained and remain explicitly readable; they are
not rewritten or retroactively claimed to have correct multi-product dedup.
Selecting v2 requires an explicit new build ID. Tests produce a v1-format
fixture, rebuild the same Raw as v2, verify different build IDs and unchanged
old files, and replay both through the public API.

## Limits and rollback

Normalization remains an offline role. The separate F4 fan-in resource risk
is not addressed by this correction. Consumers using v1 code must retain their
v1 build or upgrade the reader before selecting a v2 build; older reader code
cannot verify the new build identity. Reverting writer code leaves both
versions' immutable output intact. Never modify Raw or a published build as
rollback. Q2 release review and Q3 cloud qualification remain separate gates.
