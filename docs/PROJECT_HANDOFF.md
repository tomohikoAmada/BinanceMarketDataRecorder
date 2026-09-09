# BinanceMarketDataRecorder — Project Handoff

This handoff is self-contained for a new development team. It separates
current GitHub engineering authority, the older deployed/qualified artifact,
and the future multi-symbol plan. Documentation is not deployment or live
traffic authorization.

## Start here: the boundary

MS2 implementation/offline acceptance, independent review, and merge are
**CLOSED**. **MS3-A is merged and MS3-B candidate offline evidence is updated**
on branch `feat/ms3b-shared-resource-acceptance`; its independent re-review/
merge is pending. MS1 remains merged and closed. See [MS2 acceptance](milestone_acceptance/MS2.md)
and [MS3 acceptance](milestone_acceptance/MS3.md). Do not restart or
reopen the historical M23 optimization, BBO, Storage Forecast, remote-delete,
single-symbol shared USD-M gate, prior burn-in campaigns, clean 24h campaign,
MS1 migration design, Raw v1, Contracts, or the closed MS1 review findings
unless new MS2+ evidence creates a concrete contradiction.

The following pre-MS2 authority checks were completed before implementation:

1. Fetch live GitHub `main`.
2. Verify that `d38180074b5f76ab6b7778eea7fc505160c671ae` remains an ancestor.
3. Inspect every commit after `d38180074b5f76ab6b7778eea7fc505160c671ae`.
4. If those commits are only the post-MS1 documentation synchronization, use
   `d38180074b5f76ab6b7778eea7fc505160c671ae` and tree
   `95f16f05b30b7db23e43ebb6439ed0d055081902` as implementation authority.
5. If any later commit changes source or behavior, **STOP** and establish the
   new implementation authority before MS2.
6. Read `AGENTS.md`, `docs/CURRENT_PRODUCTION_STATE.md`, this handoff,
   `docs/milestone_plan.md`, and `docs/architecture.md`.
7. Read ADR-0032, retain ADR-0031 as superseded history, and read
   `docs/milestone_acceptance/MS1.md`.
8. Inspect the current single-symbol runtime assembly.
9. Obtain explicit MS2 implementation authorization.

## Current authority split

### A. Live GitHub main and post-MS1 implementation/behavior authority

```text
LIVE_GITHUB_MAIN=01527037254595267003f886689bb270e08b5e5d
LIVE_GITHUB_MAIN_TREE=eb63b645660a64ac606341ce3fb7f447a6e89457
POST_MS1_IMPLEMENTATION_AUTHORITY_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
POST_MS1_IMPLEMENTATION_AUTHORITY_TREE=95f16f05b30b7db23e43ebb6439ed0d055081902
MS1_MERGE_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
MS1_IMPLEMENTATION_MERGED=YES
MS1_PR=51
MS1_POST_MERGE_CI_RUN=33955915046
MS1_POST_MERGE_CI_PASS=YES
CURRENT_MAIN_DEPLOYED=NO
CURRENT_MAIN_SHA=01527037254595267003f886689bb270e08b5e5d
CURRENT_MAIN_TREE=eb63b645660a64ac606341ce3fb7f447a6e89457
MS2_PR=54
MS2_MERGED=YES
MS3=OFFLINE_CANDIDATE_EVIDENCE_UPDATED
MS3_A_MERGED=YES
MS3_A_PR=55
MS3_B=OFFLINE_CANDIDATE_EVIDENCE_UPDATED
MS3_B_CANDIDATE=feat/ms3b-shared-resource-acceptance
MS3_B_MERGED=NO
MS3_INDEPENDENT_RE_REVIEW=PENDING
MS4=NEXT_AFTER_INDEPENDENT_MS3_RE_REVIEW_AND_MERGE
NEXT=INDEPENDENT_MS3_RE_REVIEW
```

The MS3-A merge parents are `52bf086dd240556b054821f33bf1e2840fdcf912` and
`ad1e941af3cd2bd3922eac239ba92a47058e9875`. PR #51 is merged, PR #54 is the
merged MS2 behavior authority, and PR #55 is the merged MS3-A authority on
current `main`; none is deployed as current production. The MS3-B candidate
adds only the shared-resource acceptance work recorded in
`docs/milestone_acceptance/MS3.md`, including the real production-path
Collector/Poller supplement and its minimal in-flight REST cancellation
lifecycle correction. It remains an unmerged, offline-only candidate.

MS1 merged the durable identity foundation. It did not implement runtime
fan-out, multi-symbol startup, or a new readiness policy.

### B. Deployed and clean-24h authority

The last independently qualified deployed artifact before MS1 is:

```text
SOURCE_SHA=c421605e302d2ad46acdb2466627f64644181c9a
SOURCE_TREE=a521dd61f8a090b4930cce5254985383f8893a3f
WHEEL_SHA256=278ee0b0df1e7766e205684ad1e401b12fb98341296164edc1c0de9b6d58c9c6
LOCK_SHA256=44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335
CONFIG_SHA256=5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb
SYSTEMD_UNIT_SHA256=d5afc4c2228a78f02ffd7be07775e7c53acda90b8c2b1b3581d64020537188b6
DEPLOYMENT_ID=bdda546432bcaf6d29f281cd2a281b4d684bc447b2fafa382ffd1948f39a107f
```

That pre-MS1 artifact completed the single-symbol current-main non-formal
clean-24h stage with verdict
`A — CLEAN_24H_PASS_CURRENT_NONFORMAL_STAGE_COMPLETE`. Its evidence is
artifact-specific. It is not Formal M22.9 evidence, MS1 live qualification,
or a 14-product result. No 72h or 168h burn-in is required before beginning
multi-symbol development, and no old duration credit transfers to a new
multi-symbol artifact.

Accepted watches for later qualification are capacity cadence tail jitter
(p99 about 16.0819 s; max 27.3311 s; no events above 30 s) and RSS
early-growth-then-plateau (maximum 377339904 bytes, with the final segment
approximately flat). They are not MS2 blockers.

### C. MS2 implemented target

ADR-0032 supersedes ADR-0031. The future target is Binance-specific Spot and
USD-M perpetual capture with an operator-configured finite symbol list for
each market. There is no fixed symbol allowlist, automatic all-symbol
discovery, exchange/plugin framework, or hot runtime topology reload; changed
configuration takes effect on normal process restart. The project is not
becoming a multi-exchange framework.

The implemented `[recorder]` surface is `spot_symbols = [...]` and
`usdm_symbols = [...]`. Only when both fields are absent does legacy
compatibility mode resolve to the historical BTCUSDT/BTCUSDT profile. If either
field is present, explicit product-selection mode uses each supplied list
exactly and resolves an omitted sibling to an empty list; both resolved lists
empty is invalid. `ProductKey = (market, symbol)`; one process owns one durable
Catalog and one existing market Collector per configured ProductKey. The
Current `main` contains that runtime; it is not deployed.

An empty resolved USD-M set means zero USD-M Collectors, zero product-specific
USD-M side-data managers, no process-global USD-M side-data owner, and no
USD-M REST or WebSocket traffic. The global owner exists only when a USD-M
ProductKey exists and at least one global kind is enabled. An empty Spot set
similarly creates no Spot Collector or Spot side-data traffic. The legacy
`GLOBAL_SIDE_DATA_SYMBOL="BTCUSDT"` sentinel never creates a ProductKey.

## MS1 accepted architecture

Implemented now:

- durable discontinuity identity `(market, symbol, stream)`, with `gap_id`
  included for lifecycle matching;
- symbol-specific side-data cursor identity `(kind, symbol)`;
- legacy Catalog migration to `BTCUSDT`, atomic, idempotent, restart-safe, and
  fail-closed;
- in-memory `BTCUSDT` normalization for historical symbol-less seal intents,
  without rewriting persisted historical intents;
- explicit-symbol new normal APIs;
- Raw v1 and external Contracts unchanged.

Cross-symbol gap collision is not possible under the accepted matching logic.
Global side-data remains global; the six persisted 5-minute statistic cursor
families are symbol-specific. Runtime fan-out is not part of MS1.

## Roadmap: MS2 → MS3 → MS4

### MS2 — Configurable product runtime

Implemented, independently reviewed, merged through PR #54, and offline-accepted. Includes explicit
finite Spot/USD-M product lists,
ProductKey propagation through existing WS/REST/schema/envelope/spool paths,
dynamic one-process Collector assembly, product-aware service state and
configuration-bound readiness, product-aware hard-reserve discontinuity
evidence, the shared USD-M REST authority, and one process-global USD-M
side-data owner. Preserve product ownership, per-product reconnect/resync and
backpressure isolation, and the MS1 durable identities.

Acceptance is primarily deterministic/offline: actual runtime ProductKeys equal
the configured expected set; identities cannot collide; product failure does
not alter another; product readiness is observable; global readiness is
configuration-bound and fail-closed; shared REST gating is not multiplied;
global side-data is not duplicated; cursors remain independent; and one-process
restart/shutdown remains coherent.

MS2 does not change Raw v1 or Contracts, add another exchange, implement
automatic all-symbol discovery, redesign archive format, optimize unrelated
hot paths, run a long burn-in, or
declare Production Ready.

### MS3 — Shared resources / rotation / observability

**MS3-B OFFLINE CANDIDATE EVIDENCE UPDATED.** MS3-A is merged through PR #55.
The candidate retains the deterministic F1–F10 model evidence and now adds
real `UsdMCollector`/`RestSideDataPoller` gate, pagination/cursor, rate-limit,
cancel/stop, and mixed 14-product/42-core-stream running-path evidence. The
existing sequential storage Profile D test is labeled as storage-layer proof;
the new running-path test proves simultaneous activity and sibling progress
under target backpressure. It adds no generic scheduler, persisted metrics
migration, or speculative optimization. Independent re-review/merge and live
qualification remain pending; see `docs/milestone_acceptance/MS3.md`.

### MS4 — Configurable-product integration / deployment qualification

Planned after MS2 and MS3. Freeze exact main, run offline CI, build a new
immutable artifact, record all identities, and deploy only with separate
authorization. Run a bounded qualification using a representative mixed
Spot/USD-M configured profile with non-BTC products, proving all configured
products ready, isolation under reconnect/resync, no unresolved
discontinuities, Catalog/Raw/manifest/archive integrity, shared REST behavior,
and resource behavior. A fixed qualification workload is evidence only, not a
supported-symbol allowlist. Do not automatically schedule 72h or 168h; formal
M22.9 remains separate.

## Non-negotiable boundaries

The Recorder captures Binance public market data only. It has no account
endpoints, API keys, credentials, orders, trading, strategies, or external
consumer repository dependency. Raw payload bytes and Raw v1 framing remain
recoverable and unchanged. Do not write production data under the repository
or use an external volume as an active Collector target.

`FORMAL_M22_9_STARTED=NO`, `PRODUCTION_READY=NO`,
`DEPLOYMENT_AUTHORIZED=NO`, `CURRENT_MAIN_DEPLOYED=NO`,
`MS2_IMPLEMENTATION_STARTED=YES`, `MS2=CLOSED`,
`MS3_A_MERGED=YES`, `MS3_B=OFFLINE_CANDIDATE_EVIDENCE_UPDATED`,
`MS3_B_CANDIDATE=feat/ms3b-shared-resource-acceptance`,
`MS3_B_MERGED=NO`, `MS3_INDEPENDENT_RE_REVIEW=PENDING`,
`MS4=NEXT_AFTER_INDEPENDENT_MS3_RE_REVIEW_AND_MERGE`,
`NEXT=INDEPENDENT_MS3_RE_REVIEW`.
