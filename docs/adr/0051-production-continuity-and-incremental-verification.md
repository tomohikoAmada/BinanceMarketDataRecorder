# ADR-0051: Production per-source continuity and new-file verification

Status: accepted by the owner's 2026-10-26 production milestone plan; P0 contract adopted. Runtime implementation belongs to P1–P3. P4–P6 cannot use the new qualification until those implementations exist.

## Decision

The [production plan](../production_milestone_plan.md), [P0 coverage inventory](../milestones/P0/coverage.md) and this ADR govern future production qualification. They supersede the future-stage sequencing, sampled-slot pass criteria and repeated whole-corpus qualification procedures of ADR-0033–0035, 0039–0043, 0046–0048 and 0050 **for P0–P6 only**. Historical results and their readers remain valid under their original contracts; no old result receives new production credit. Existing implementation details and data preservation rules remain unless explicitly changed below.

Each run freezes candidate identity, configuration, selected assets/products, required source identities, source-specific publishing/polling cadence, deadlines, lifecycle and recovery rules. Shared data has one source identity and links to its products. A missing applicable source is NOT_IMPLEMENTED/UNKNOWN, not silently optional. Exchange-not-offered and authentication restrictions are declared with official evidence before a run; a failed source cannot be removed retrospectively.

For source s in a half-open window [a,b), eligible duration E is its predeclared service lifecycle intersected with the window. Let G be the union of unavailable and unknown intervals, clipped to E. `coverage(s) = 1 - duration(G) / duration(E)`. No denominator yields NOT_APPLICABLE with lifecycle evidence, never 100%. Overlapping causes are counted once per source. Missing monitoring evidence is unknown. For wall-clock reporting retain UTC boundaries, and use monotonic elapsed time within a boot; boot/clock uncertainty must not create invented healthy time.

Each required source must meet **99.5%**, in the full run and each complete 24h window counted from that run's start. No averaging across streams, products, markets or days. The corresponding unavailable budgets are 36s/2h, 432s/24h, 864s/48h, 1296s/72h. The budgets are durations, not permission to discard messages.

WS evidence includes connection/heartbeat, receive and persistence progress and applicable sequence/recovery observations. Connection-open alone is insufficient when writing stalls. Quiet markets and event-sparse liquidation do not fail simply for no event. Depth becomes valid again only when snapshot/sequence recovery has completed. For a known failure at t and valid recovery at r record [t,r); when exact t is unknown, conservatively start at the last boundary that was proved valid. Sampled 30s pass counts are diagnostic only. Network/exchange attribution does not remove failed intervals.

REST success requires valid status/body/source identity and an observable newly completed request. Source-specific F deadlines and expected data periods are fixed in the inventory; expired success does not refresh itself. Empty responses allowed by that endpoint remain explicit empty observations/query ranges, never fabricated zero rows or a claim that a period record exists. Distinguish request freshness from data publication freshness and record both. Funding adjustment metadata can be sparse; OI evaluated windows retain ADR-0045's durable sparse semantics. Poll retries must respect the actual shared IP/request budgets.

## Saved-data correctness and verification

Correctness is separate from continuity. No observed corruption, silent discard or false persistence/completion claim is acceptable. Fixed fixtures compare saved original bytes, ordering and counts to the input. Live runs reconcile observable received/queued/persisted counters and explain tails/gaps; absent independent exchange truth, they cannot claim every exchange event was captured.

Every new sealed file must receive at least one complete read by a path independent of the writer before it is marked verified: stored size/digest, decoded size/digest, header/frame/CRC/count, product/source/time and applicable sequence/gap metadata. Merge compatible checks into one sequential pass where practical; no metadata-only writer assertion substitutes for reading bytes. Preserve an immutable result bound to chunk ID, stored/content/manifest digests, verifier version and result/time. Changed bytes or verifier-relevant metadata invalidate reuse; errors retain the affected file and its failure evidence. P3 implements the durable queue/results; P0 does not pretend the existing V5 command implements this workflow.

Capture continues while independent workers verify sealed files and archive normally. At a checkpoint freeze the current-run sealed-file boundary and finish any unverified files through that boundary; active partials remain pending with an explicit boundary, never marked verified. The run records received/writer progress and outstanding work at the checkpoint. Final acceptance waits for the final candidate window's pending new files to be checked. Verification backlog/count/oldest age and archive backlog must stay bounded and catch up, including after a transient worker failure. A failed verifier is repaired and resumes that same saved corpus; no recapture is needed solely to repair offline tooling.

Verified archive-copy rules, custody and deletion authorization are unchanged: an ingestion verification result is not authorization to delete a sole copy. Destination bytes must still be read and verified and the existing transaction completed before eligible source retirement. Historical V5/full audit tools remain available for targeted diagnosis and old artifacts. P0–P6 do not require a historical baseline scan, two released LIVE passes, duplicated full evidence exports, private protective windows, OS-owner masking or multiple model signatures.

## Formats, durability and implementation boundary

EventEnvelope v1, Raw chunk v1, existing manifests/Catalog, normalized datasets and replay formats are unchanged by P0. REST SDK model re-encoding is still labelled as such. P2 must define any added market/source identities and raw HTTP body representation explicitly, including compatibility, before implementing a format change. Unknown fields or malformed-but-received bytes must be preservable with honest quality evidence in P1; this is not yet implemented by this ADR.

Retain current periodic fsync and normal stop/drain/sync. Received/in-memory progress is distinct from persisted/synchronized progress. Sudden-power-loss work/tests are deferred; there is no new owner-approved loss window and no zero-loss-at-power-cut claim. No historical data migration or deletion is needed for this decision.

## Stage procedure and validation

P4: one-asset 2h checkpoint within its uninterrupted 24h. P5: two-asset 2h checkpoint within a separate 24h, measured asset expansion/backoff. P6: final configuration's single continuous 72h with a 48h checkpoint, all three full 24h subwindows, and completed new-file verification; keep healthy capture running when handing over production. Source/config changes require affected tests and a fresh final-candidate duration, not concatenated old runs. Documentation-only edits do not invalidate capture.

P0 delivers source/package identity, inventory and separate full-current-feature baseline. P1–P3 supply meaningful regressions for byte preservation, persistence accounting, source-specific gaps, recovery, background verification/archival and normal drain. These are exit requirements, not a new generic fault/capacity framework or an approval chain.
