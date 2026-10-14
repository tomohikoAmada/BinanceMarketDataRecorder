# R1 independent source review

Verdict: **ACCEPT_SOURCE_CORRECTNESS_LOCAL_OFFLINE**. No unresolved actionable finding remains. This accepts the current candidate source, not a release, deployment, or live duration.

Candidate: `/Users/amada/.codex/worktrees/recorder-r1/BinanceMarketDataRecorder`. Base: `9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b`. Review head: `9e8ec373a5e9e8bede36cd6f5ff9a21cd355fb3b` with the candidate modifications still present. Exact source manifest: `final-source-manifest.json`, SHA-256 `559729184f97a45e52c9f86220fbcacba5f89d43f132d92454e1eb6bbbef33a2` (122 source members). Exact source/tests/docs inputs: `final-input-manifest.json`, SHA-256 `326dab05b779beebd41cc4e38999ed18b8f8a6ec40e55f9738522709385a155d`. Final readback found no byte drift. The ten changed source files and their full hashes are in the machine result. No acceptance from the prior production identity was transferred.

## Timestamp and immutable version boundaries

USD-M depth and bookTicker T populate transaction time; aggTrade T populates trade time. E remains event time, and zero remains a present value. The v2 extractor preserves exact payload bytes, declares module v2, leaves the other T slot null, and rejects missing, boolean, negative, noninteger, or out-of-int64 required times as malformed. Raw/EventEnvelope/chunk protocols remain unchanged.

Normalize v2 derives valid core E/T from payloads even for legacy v1 Raw. Effective times enter the logical hash, so two records with identical sequence identity but different effective timestamps survive as conflict variants rather than being silently deduplicated. My extra persisted fixture confirms this behavior. Dataset identity and directories include v2, preventing reuse of a v1 build under the new semantic meaning. Explicit `ManifestCatalog(..., dataset_version="normalized-dataset.v1")` retains the legacy dataset's recorded clocks and hashes, and catalog validation rejects cross-version mixing. The regression preserves old Raw, Parquet and manifests while a separate v2 build uses the corrected clock. No production/historical rederivation was performed.

One concrete finding was reproduced and corrected before acceptance. Exact base extractor code accepts E=2**63 or T=2**63 into a synthetically persisted v1 Raw envelope. New parsing marked such an input invalid, but the initial candidate's missing explicit invalid exchange-times mapping made `_candidate` fall back to the unrepresentable legacy fields, causing Arrow OverflowError and aborting the build. The final `_invalid` supplies explicit all-null exchange times for USD-M core rows. Both exact-base E/T cases now pass through real Raw, Normalizer and Replay; the invalid/error/Raw provenance remain intact. The initial two failures and corrected recheck remain in this review directory. This does not assert that any production data contains these values.

## Backoff

Reconnect and FullJitter share a small capped doubling helper. It doubles only until the finite configured maximum, so runtime and magnitude remain bounded for 1025 failures, 100,000 failures and 10**100 failures. Prefix behavior and jitter remain the same for ordinary finite values and extreme finite bounds. Stop-aware wait and failure reset policy are unchanged. The helper is appropriate in scope; no retry framework or broader policy abstraction is necessary.

## Count and payload-byte ownership

The existing queue count bound remains. Weighted admission accounts payload bytes once, atomically on the event loop, and rejects oversize items explicitly. Dequeue decreases queued bytes but leaves owned bytes charged. Exact-item completion releases the captured admission size; identity/deque accounting also handles repeated admissions of one object and out-of-order completion. Space notifications recheck both count and bytes, and retained saturation deadlines keep existing stop, timeout and failed-writer behavior.

Both writers release receipt ownership after the full Raw batch has drained, durable gap completion has succeeded, and observers have returned. Release contains no suspension point, then clears payload-bearing batch/envelope references before producers resume. A failure or cancellation in these phases does not mark unsuccessful receipts complete. The owned blocking-call semantics continue waiting for in-flight writer mutations before abort/close. Existing post-close boundary handoff and durable STARTED/COMPLETED, sync, seal intent, recovery and explicit Gap semantics remain intact.

Independent near-16MiB collector tests use six legal 16775168-byte messages per market and real Raw spool/writers with slow drains at the default 64MiB ownership budget. They force waiting below the count limit, verify all payloads and order, verify charge during callbacks, and finish with zero queued/owned bytes. Existing shorter-frame sustained overload tests retain the last already-received boundary payload, mark gaps, and do not claim unread WebSocket/TCP messages were captured. Existing 56 reconnect/crash boundaries and the writer failure/cancellation tests pass against the candidate.

A 64MiB budget is per collector payload ownership, not total RSS. The currently receiving frame, decoded structures, envelope/encoding overhead and transport buffers are separate, as ADR-0044 discloses. Transport assembler high/low values 4/1 remain flow-control watermarks; fragmentation is inspected but not executed by this independent reviewer. The 16MiB transport limit does not enlarge Raw's encoded-body max_frame_bytes; exact-body overhead remains part of that pre-existing fail-closed contract. The two-stage framing limits must not be described as proof that every possible 16MiB wire payload fits a default 16MiB Raw body.

## Independent verification and access

All tests used Python 3.12 from the existing engineering venv, candidate PYTHONPATH, disabled bytecode/cache writes to the candidate, and private synthetic paths. Guarded phases:

- 140 selected queue/backoff/schema/Normalize/Replay/ingress cases passed; 3 selected cases were deselected (the two real transport instances and one existing stress case).
- 56 reconnect/crash/durable-gap boundary cases passed.
- 3 additional independent near-16MiB collector/logical-hash cases passed.
- 18 final timestamp cases passed, including the two exact-base-extractor regressions and 16 current candidate timestamp cases.

These phase counts overlap; they are not summed as distinct tests. `git diff --check` passed independently. The parent reports full initial pytest 2108 PASS, 24 online SKIP and 5 stress deselected, with one M0 supported-root-override issue subsequently passing its supported override; Ruff/mypy/M0/Go and six actual fragmented transport cases pass. These parent results were not independently rerun or converted into independent network evidence.

The guard denied every socket connect/bind/DNS event before operation. Two separate Python-process imports attempted the known urllib3 IPv6 loopback capability bind and were denied; no connection or bind completed. No VPS/network connection, production or historical Raw/manifests, or repository mutation occurred. Test Raw/manifests were created and read only under this exclusive private review directory. Documentation source URLs and the copied Contracts fixture were inspected offline; external source retrieval belongs to the parent's retained evidence, not this reviewer.

## Qualification limits

Freeze/CI/new locked wheel/cloud/canonical stopped deployment and fresh same-release own-duration 2h/12h remain separate future gates. This source result grants none of their credit, no old duration, no every-exchange-event completeness, canonical V5/24h/48h eligibility, or production-ready status. The implementation stays small and local: one timestamp interpretation/version boundary, one capped helper, and a per-queue ownership extension. No additional engineering correction is required by this review.
