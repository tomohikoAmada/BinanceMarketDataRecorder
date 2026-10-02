# ADR-0035: Bounded V5 delta batches for the four-product workload

- Status: Implemented candidate; independent review and deployment pending
- Date: 2026-10-02 (Asia/Shanghai)
- Scope: Narrow refinement of ADR-0034's online consumption limits

## Problem

Four products and twelve core stream contexts sealing every 60 seconds generate
60 chunks per 300-second observation. Normal sealing plus verified local archive
retirement produces 480 chunk transitions and 300 archive events. One 256-row
page per family cannot keep up, irrespective of CPU speed. Larger SQL pages
alone also overlook causal-reference and serialized-byte bounds.

## Decision

Keep each SQL page at 256 rows. A new stage declares this exact `delta_policy`
in its hash-bound stage-start and repeats it in every sample:

```json
{"version":"bounded-batches.v1","sql_page_cap":256,"max_pages_per_family":4,"causal_reference_cap":1024,"max_canonical_delta_bytes":7340032}
```

Capture at most four consecutive pages per family within the same SQLite read
transaction and frozen high-waters, including all indexed companions. Stop
paging when fewer than 256 rows remain. End that transaction before Raw work.
Publish the resulting ordered bounded entries in the existing `delta_pages`
lists; these are observation batches, not claims that a single SQL query read
1024 rows. No unbounded catch-up loop or new persistence service is introduced.

Pure replay accepts at most 1024 entries per family and retains at most 1024
causal references for this policy. Neither a lookup nor a later SQL page advances
an unconsumed companion cursor. New samples reserve 1 MiB of the existing 8 MiB
document bound for continuation/live metadata by limiting delta entries to 7 MiB.
The exact total-document check still rejects any overflow. Exhausted byte/time
budgets leave explicit pending work; target pending remains INCOMPLETE.

The existing 240-second work cutoff, 300-second cadence, 600-second maximum
gap and 900-second readiness recovery remain unchanged. Raw v1, Catalog schema,
full baseline/terminal audits and independent LIVE verification are unchanged.

## Compatibility

Absence of `delta_policy` means the original one-page V5: 256 entries/references
and its original 4 MiB delta-entry budget. Reading or resuming historical stages
does not widen them. Unknown policies and changes after T0 are rejected. The V5
envelope/shard schema stays the same; this is a declared policy extension, not
permission to reinterpret an old chain. The new verifier reads both policies.
Older verifiers cannot qualify the new policy and must not be used for it.
Deployment identity and exact verifier source remain bound to qualification.

## Validation and limits

Integration tests use real writers, sealing, Catalog and ArchiveManager for all
four ProductKeys and twelve streams, with five-minute windows and one missed
cadence. Normal windows consume 480/300 rows without pending work; the byte
budget can defer catch-up work and it drains during subsequent windows. An
old-format start still consumes at most 256 rows. Existing cancellation, archive
retirement, causal tampering and full-finalization checks remain required.

The test workload uses small synthetic Raw frames. Production auxiliary traffic,
size-triggered seals, decode cost and SQL capture still require cloud measurement
on the frozen artifact before Formal T0. This policy is deliberately finite;
it does not certify arbitrary symbols or event rates.
