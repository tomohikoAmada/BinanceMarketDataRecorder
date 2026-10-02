# ADR-0037: Share exact archive companions within bounded V5 observations

- Status: Implemented candidate; independent review and cloud qualification pending
- Date: 2026-10-02
- Scope: Q3 correction to ADR-0035; no Raw/Catalog/full-audit format change

## Evidence

The retained Q3 nonformal cloud run exhausted most of its 7 MiB delta budget
repeating the same archive transaction bundle across related chunk/archive rows.
Archive cursor consumption lagged, and replay exceeded 1024 causal references
before the later archive family could discharge them. The run failed before
publication; no Formal credit exists. Its actual failed document is unavailable,
so its final reference count is not asserted.

## Decision

New starts freeze `bounded-batches.v2`, retaining four 256-row SQL pages per
family, 7 MiB canonical delta bytes, 8 MiB total document bytes, 240s work,
300s cadence, 600s maximum evidence gap and 900s readiness recovery.

Each observation has an `archive_companions` table keyed by transaction ID.
A row's `companions.archive` is that ID instead of another complete copy of
its transaction/chunk/event/transition bundle. The table exists only within
that document. Admission counts canonical entries AND the complete table,
including braces, keys and commas; additions are committed only with an admitted
entry. Identical IDs with differing bundles fail. Replay rejects missing,
misidentified, inline or unused table entries, expands exact original companions,
and performs the ordinary row/digest/causal proof checks. No row or Raw proof is
omitted, no companion lookup advances a cursor, and no cross-document cache is
introduced. Publication still enforces the total 8 MiB cap.

For v2, the 1024 causal-reference cap applies to the completed observation's
persisted continuation. Family replay can temporarily introduce references that
later families corroborate and discharge in that same finite observation. Memory
remains bounded by three bounded batches and the existing fixed companion
fan-out, plus the prior bounded continuation. Unmatched references are retained,
conflicting digests fail, final continuation above 1024 fails, and target pending
work still prevents eligibility. This fixes family-order overflow without
increasing persisted caps or deleting causal evidence.

## Compatibility and tests

Missing-policy original starts and exact v1 starts retain their previous bytes,
per-row reference cap, replay and resume. Their producers do not switch to v2.
Unknown/changed policy fails. Older verifiers cannot qualify v2; the exact updated
verifier/deployment source is frozen with the new stage. The evidence envelope
remains V5 with an explicitly declared policy extension.

Regression tests preserve exact expanded rows, compare repeated vs shared byte
cost, reject table/digest tampering, discharge a transient 1080-reference burst
into a 56-reference continuation, reject genuinely excessive final references,
and exercise original/v1/v2 real-writer/archive throughput and resume. Cloud
all-enabled observations and full terminal verification remain required before
Formal T0. This ADR grants no throughput, duration or production-ready credit.
