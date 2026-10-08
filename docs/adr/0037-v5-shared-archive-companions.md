# ADR-0037: Share exact archive companions within bounded V5 observations

- Status: Implemented; exact-input independent review ACCEPT; cloud qualification pending
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
including braces, keys, commas and the canonical trailing newline; additions are committed only with an admitted
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

October8 Q7 correction: equal 1024-row chunk/archive pages advanced the archive
family faster through a large pre-start backlog (chunk lifecycles have additional
non-archive transitions). Retained observations had481 then897 unresolved
references; the next bounded snapshot reproduces the cap error using metadata
only. Compact producers now schedule continuous prefixes by the smallest next-row
unresolved-reference count, keeping the last within-cap prefix checkpoint.
The bounded speculative walk may temporarily exceed1024 to close a mutually
referencing lifecycle, but only a final within-cap checkpoint is published.
If that walk yields no advancing checkpoint, the two existing independent
causal groups (operational pairs and chunk/archive lifecycles) are tried
separately so an unclosed pair cannot starve a closed archive lifecycle.
This scheduling uses numeric reference identities, introduces no new digests,
does not acknowledge lookup companions, and does not replace exact independent
replay. Existing byte/time/Raw admission happens first; pruning can only remove
admitted descriptors, then the exact used shared table is rebuilt. A blocked
prefix stays pending and cannot qualify a target. The envelope, v2 policy,
persisted cap and original/v1 replay/resume remain unchanged. Real600-chunk
writer/archive backlog and full-cap mutual-reference/group-isolation tests cover progress and
finite-input bounds; representative cloud qualification remains required.

## Compatibility and tests

Missing-policy original starts and exact v1 starts retain their previous bytes,
per-row reference cap, replay and resume. Their producers do not switch to v2.
Unknown/changed policy fails. Older verifiers cannot qualify v2; the exact updated
verifier/deployment source is frozen with the new stage. The evidence envelope
remains V5 with an explicitly declared policy extension.

Regression tests preserve exact expanded rows, compare repeated vs shared byte
cost, reject table/digest tampering, discharge a transient 1080-reference burst
into a 56-reference continuation, reject genuinely excessive final references,
and exercise original/v1/v2 real-writer/archive throughput and resume. Real SQL
rows at exactly 7 MiB and one byte above, plus 60 actual archive bundles with
SQL rows filling the exact cap, cover producer/reader byte-accounting equality.
Independent review found the initial candidate undercounted an empty table by
one byte and overcounted multiple bundles by N-1; the correction consistently
uses the canonical empty table's three bytes. Cloud
all-enabled observations and full terminal verification remain required before
Formal T0. This ADR grants no throughput, duration or production-ready credit.
