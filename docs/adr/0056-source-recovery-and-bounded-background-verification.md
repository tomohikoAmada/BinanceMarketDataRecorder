# ADR-0056: Source recovery evidence and bounded independent Raw verification

Status: implementation candidate; independent review NOT ACCEPTED, 2026-10-30.
[P3-R1–R3](../milestones/P3/review-20261030.md) identify missing durable-gap checks,
incorrect idle-to-pending writer timing, and missed short RPI intervals. The intended
behavior below remains required; corrections are tracked in the plan's P3 section
per owner direction. Implements ADR-0051/0053; no production
qualification, transport scope change or archive deletion authorization is granted.

## Recovery and observation boundary

SelectedAssetCapture retains the existing WS reconnect/backoff, per-product depth
snapshot/sequence bridge and bounded shared REST scheduler. HTTP/schema failures
remain saved observations of that source. Common Raw/Catalog failures propagate to
the service owner, which stops and drains affected capture rather than retrying them
as network failures. The existing platform service owns terminal process restart.

The source identity is the existing `(venue_market, symbol-or-shared-scope, stream,
transport)` key, joined with `/`; it is not a digest. The additive Catalog tables
`source_availability`, `source_availability_gaps`, and `source_availability_lifecycle`
store current observations, gap identities/bounds and eligible intervals. Callbacks
only enqueue bounded in-memory evidence; at most128 records are committed per flush
outside the receive loop. The65536-record queue fails explicitly on exhaustion;
shutdown still releases/drains capture. No evidence is silently dropped.

A one-second monitoring pass observes connections, persisted payload errors, depth
bridge state, response deadlines and writer progress. Sparse connected WS sources
need no invented payload. A group with received but unwritten frames and no completed
write for10 seconds is unavailable conservatively from its progress boundary; only
that connection's source group is affected. Sequence depth stays unavailable until a
snapshot-backed bridge is reliable. Existing reconnect Raw/manifests/Catalog evidence
remains authoritative and is not replaced by the new availability ledger.

REST request freshness is distinct from publication freshness. Regular deadlines use
the actual receive time plus configured cadence and the existing10-second success
budget. Native closed-bar scheduling uses its existing calendar/next-close schedule.
A closed-bar response older than the requested native period, or an empty response
without that bar, is saved but not credited as a current publication. Five-minute
statistical responses must belong to the requested evaluated window; legitimate empty
windows remain explicit observations, consistent with ADR-0045. Delivery publication
remains pending until the requested dated delivery is present. Metadata and recent-trade
observations do not become stale merely because their underlying value did not change.

Availability recovery closes a gap after the valid response has completed persistence,
not at the earlier receive boundary. Failures/counters and completed gaps survive
recovery. Healthy observations checkpoint at most every10 seconds per source. After
restart the interval since the last durable observation is conservatively unknown;
an open gap keeps its identity and initial cause. A monitoring pause over5 seconds
and a backward wall clock are unknown, rather than invented healthy time. Lifecycle
suspension ends eligibility without erasing preceding failures. This is monitoring
evidence, not a proof of every exchange event or a new power-loss guarantee.

`report continuity` unions gaps and unknown tails per source, clips to observed
eligibility, and never averages products. Qualification must freeze the complete
expected source IDs/configuration and pass them to the report; undiscovered expected
IDs remain UNKNOWN_SOURCE. A report of only discovered sources is diagnostic and
explicitly carries `production_qualified=false`. Declared uncaptured sources and
non-admitted products remain in P2's existing inventory/status, not silently removed.

## New-file queue, proof and workers

A new SEALED transition atomically enrolls its chunk in `raw_verification_queue`.
Schema creation does not enroll or scan historical files. The standalone
`verify new --chunk-id` command explicitly enrolls/rechecks one historical chunk when
needed. No startup full-history inventory or migration of Raw is required.

One independent verifier process has its own Catalog connection and a nonblocking
per-root process lock. It processes at most32 candidates,512MiB of declared
stored+expanded bytes, and30 seconds between files per batch. Actual reads are paced
at16MiB/s of stored+expanded input; declared size bounds stop expansion beyond the
manifest. A current file is indivisible; the time budget is a between-file limit.
The service sizes its byte budget to at least three times rotation+maximum-frame
size and adjusts the child timeout; the CLI exposes budgets. Oversized CLI candidates
remain pending with explicit IDs, never silently skipped as verified.

The independent scanner shares one compressed descriptor/pass for stored SHA-256,
expanded SHA-256/size, header/frame/canonical-CBOR/CRC checks and statistics. It checks
Catalog/manifest identity, product/source, versions, counts, receive/exchange time
ranges, sequence ranges, provenance, capture flags and connection-boundary semantics.
A decoded payload with no continuous exchange sequence still has only its declared
quality guarantees. Full content digests plus descriptor/path mutation guards bind
the observation; stat metadata alone never replaces reading bytes.

Immutable `raw_verification_results` bind chunk ID, exact manifest digest, both byte
identities, verifier version, result/time and checked metadata. The queue publishes a
success only after that result commits against unchanged Catalog identity. Results
are proofs of their observation boundary, not permanent assurances against later
external mutation. Explicit rechecks append new proofs. A first failure is retained
in RAW_VERIFICATION_FAILED, current error/attempt/backoff stays in the queue, and
repair retries the original data. No corrupted artifact is deleted or marked verified.

Ordinary worker runs read only pending new files; caught-up runs do not reopen Raw.
`verify status` exposes counts, bytes, oldest age, failures and enrollment high-water.
A checkpoint freezes that high-water and uses `verify new --through-utc-ns` while
capture continues. Active partials remain outside the sealed boundary. A verified
registered local archive can supply a retired file; absent/unavailable copies remain
failed/pending and cannot earn saved-data qualification.

The existing owned subprocess lifecycle is reused for reports and verification:
one child per family, bounded timeout, stop/kill/reap, retry without stopping capture.
Archive drain retains its own lock/file/time budgets and now paces copy/readback at
16MiB/s. No normal archive or host task is disabled. Normalization/replay remain
operator-invoked consumers, with no periodic normalization or capture dependency.

## Compatible processing and formats

Sealing's independent post-fsync readback now obtains stored and expanded identities
in one pass, rather than reading the compressed file again for its manifest digest.
Recovery/retry still reads the relevant existing artifact again; archive target
post-fsync readback, receipt/manifest identity and pre-retirement source checks remain
separate trust boundaries. No validation cache crosses updates or retirement.

Normalized rows retain every published digest field and value. One immutable
SourceRecord lazily computes its Raw payload digest once for all derived children,
including malformed identity. Normal poll identity no longer computes a discarded
model digest; actual blue/green identity consumers still compute it. Temporary
partition files use operation-local ordinal names. Installed SDK versions are cached
in process modules rather than queried for every response. No Raw v2/block metadata
change is justified by these measurements; Raw v1 and historical readers remain.

Restricted normalization introduces the optional, explicit
`normalization-input-selection.v1` object in a v2 build manifest: `products`, half-open
UTC receive bounds, and `checkpoint_scope=none`. The build identity wraps the original
source-build digest with that object; the updated public replay reader verifies it.
Restricted builds do not scan unrelated checkpoint history. Whole selected files are
verified even when only some rows fall in the range. Unrestricted old builds keep
exactly the old identity/manifest and remain readable. Older readers can reject new
restricted builds, not misread them as unrestricted results. No published row field
is removed, emptied or repurposed.

## Storage safety and validation

All service configurations now run existing internal-capacity observations and a
real hard-reserve stop. Explicit VPS profiles keep their existing policy; ordinary
configurations use the established max(5GiB,2%capacity,2×rotation) reserve. The stop
records DISK_EMERGENCY_STOP, drains/seals and retains all unarchived data. No new
power-loss window, source deletion right, volume ownership or host operation is added.

See [P3 result](../milestones/P3/README.md) for deterministic recovery/fault/background
regressions, same-input local CPU/I/O/allocation comparison and validation commands.
Actual VPS throughput, daily growth, sustained backlog and99.5% continuity belong to
P4–P6; local fixtures do not certify them.
