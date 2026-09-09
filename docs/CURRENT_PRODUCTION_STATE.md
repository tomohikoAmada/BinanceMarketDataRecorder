# Current Production State

This is the concise current authority. It separates GitHub engineering source,
the independently qualified deployed artifact, and the future multi-symbol
program. Verify live GitHub before acting; this document does not authorize
deployment, live traffic, formal acceptance, or data retirement.

## Status at a glance

```text
LIVE_GITHUB_MAIN=VERIFY_AT_TAKEOVER
POST_MS1_IMPLEMENTATION_AUTHORITY_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
POST_MS1_IMPLEMENTATION_AUTHORITY_TREE=95f16f05b30b7db23e43ebb6439ed0d055081902
MS1_MERGE_SHA=d38180074b5f76ab6b7778eea7fc505160c671ae
MS1_STATUS=MERGED
MS1_PR=51
MS1_POST_MERGE_CI_RUN=33955915046
MS1_POST_MERGE_CI_PASS=YES
CURRENT_MAIN_DEPLOYED=NO
CURRENT_NONFORMAL_VALIDATION_STAGE_COMPLETE=YES
MULTI_SYMBOL_DEVELOPMENT_MAY_BEGIN=YES
MS2_IMPLEMENTATION_STARTED=YES
MS2=CLOSED
MS2_INDEPENDENT_PR_REVIEW=COMPLETE
MS2_MERGED_PR=54
MS3=IN_PROGRESS
MS3_A_CANDIDATE=feat/ms3a-product-rotation-attribution
MS3_B=NOT_STARTED
MS4=PLANNED
FORMAL_M22_9=NOT_STARTED
PRODUCTION_READY=NO
```

## Current MS3-A candidate authority

MS2 implementation, offline acceptance, independent review, and merge are
closed on current `main` via PR #54 at
`52bf086dd240556b054821f33bf1e2840fdcf912`, tree
`762729846fae766ae1c2edde0af95cd63648e7df`. The MS3-A implementation candidate
is the local branch `feat/ms3a-product-rotation-attribution`; it is not merged
or deployed. MS3-B remains outstanding. See [MS2 acceptance](milestone_acceptance/MS2.md)
for the closed MS2 implementation record and compatibility.

## A. Verified pre-MS2 main and post-MS1 implementation/behavior authority

| Item | Authority |
| --- | --- |
| Live GitHub `main` | verify at takeover |
| Post-MS1 implementation/behavior authority | `d38180074b5f76ab6b7778eea7fc505160c671ae` |
| Post-MS1 implementation tree | `95f16f05b30b7db23e43ebb6439ed0d055081902` |
| MS1 merge | `d38180074b5f76ab6b7778eea7fc505160c671ae` |
| Merge parents | `c421605e302d2ad46acdb2466627f64644181c9a`, `11e100fbcb974e7d54f0515c99e08ac6042b9204` |
| MS1 | merged via PR #51; reviewed head `11e100fbcb974e7d54f0515c99e08ac6042b9204` |
| Post-merge CI | `offline-ci` run `33955915046`, push event, macOS and Ubuntu Python 3.12 jobs successful |
| Deployment | live GitHub `main` is not deployed |

MS1 is the merged multi-symbol durable identity foundation. Its three
implementation commits remain provenance, not separate current authorities:
`026e357eb9af9b5b9fd111872dc6dcc30e9c599d`,
`39fbd04172a6b5b27b41d43c57d0e5ff575b95d4`, and
`11e100fbcb974e7d54f0515c99e08ac6042b9204`.

The last behavior-changing authority before MS2 was the MS1
merge `d38180074b5f76ab6b7778eea7fc505160c671ae`, with tree
`95f16f05b30b7db23e43ebb6439ed0d055081902`. A documentation-only descendant
may make live GitHub `main` newer without changing that implementation or
behavior authority. At takeover, verify live `main`, confirm this SHA remains
an ancestor, and inspect every later commit. If any later commit changes
source or behavior, stop and establish the new implementation authority before
MS2.

## B. Deployed and clean-24h authority

The last independently qualified deployed artifact is the pre-MS1,
single-symbol artifact. It is not current `main` and is not MS1 live
qualification.

| Item | Frozen value |
| --- | --- |
| Source SHA | `c421605e302d2ad46acdb2466627f64644181c9a` |
| Source tree | `a521dd61f8a090b4930cce5254985383f8893a3f` |
| Wheel SHA-256 | `278ee0b0df1e7766e205684ad1e401b12fb98341296164edc1c0de9b6d58c9c6` |
| Lock SHA-256 | `44cd373324f2af5f2682851996bc59a16199c65f8de9e98089131e1c67d6f335` |
| Config SHA-256 | `5aee65a7de55cf06645c70296870346004c712fc6f9cd43390e1ea8b3ffabfbb` |
| systemd unit SHA-256 | `d5afc4c2228a78f02ffd7be07775e7c53acda90b8c2b1b3581d64020537188b6` |
| Deployment identity | `bdda546432bcaf6d29f281cd2a281b4d684bc447b2fafa382ffd1948f39a107f` |

### Clean 24-hour non-formal closure

Final independent verdict:
`A — CLEAN_24H_PASS_CURRENT_NONFORMAL_STAGE_COMPLETE`.

```text
BOOT_ID=f2720022-bc39-4e22-bc68-af6bfce92274
PID=157507
PROC_STARTTIME=7023878
T0=2026-09-03T06:10:44.357173Z
T1=2026-09-04T06:10:51.870564Z
DURATION_SECONDS=86408.67870779098
EVIDENCE_ARCHIVE=CURRENT-MAIN-24H-CLEAN-NONFORMAL-EVIDENCE-20260904T062029Z.tar.gz
EVIDENCE_ARCHIVE_SHA256=1ae0001eec78b6aebff2b45183a4191ac7bfcb99b2b9e10588dc3be635a50946
RESULT_SHA256=c08ba6299390b92f0f2687c14177682c05c34781bc4ae020710e51f7056da9d9
```

Core result: Spot gaps `0`; USD-M gaps `0`; order-book resync `2`; recovered
discontinuities `22`; unresolved discontinuities `0`; stream-attempt terminal
`2`, both subsequently recovered; 22 discontinuity START identities matched
22 COMPLETE identities; Catalog/SQLite errors `0`; backpressure timeouts `0`;
swap `0`; OOM `0`; final readiness `YES`; final books synchronized `YES`.
This is non-formal evidence and must not be labeled Formal M22.9 evidence.

Accepted non-blocking watches for future multi-symbol qualification are
capacity cadence `PASS_WITH_JITTER_WATCH` (full-window p99 about `16.0819 s`,
max `27.3311 s`, zero events over 30 s) and RSS
`EARLY_GROWTH_THEN_PLATEAU` (T0 `270000128`, 12h `335282176`, T1 `368766976`,
maximum `377339904` bytes; final 18–24h approximately flat).

```text
SEVENTY_TWO_HOUR_BURNIN_REQUIRED_BEFORE_MULTI_SYMBOL=NO
ONE_SIXTY_EIGHT_HOUR_BURNIN_REQUIRED_BEFORE_MULTI_SYMBOL=NO
PERFORMANCE_ENGINEERING_REOPENED=NO
```

## C. Implemented configurable-product runtime and next qualification

ADR-0032 supersedes ADR-0031. The future target remains Binance-specific with
Spot and USD-M perpetual markets, but the operator explicitly configures a
finite symbol list independently for each market. There is no fixed symbol
allowlist, automatic all-symbol discovery, exchange/plugin framework, or hot
runtime topology reload. Configuration changes take effect on normal process
restart. See
[`docs/adr/0032-configurable-product-set.md`](adr/0032-configurable-product-set.md).

The implemented surface is `[recorder]` with `spot_symbols = [...]` and
`usdm_symbols = [...]`. Legacy compatibility mode applies only when both fields
are absent, resolving to BTCUSDT in both markets. If either field is present,
explicit product-selection mode applies: supplied lists are exact and an
omitted sibling resolves to an empty list; both resolved lists empty is invalid.
MS2 assembles one Collector per configured ProductKey in one process.
It passed offline acceptance; MS3/MS4 and live qualification remain pending.

In explicit Spot-only mode, the resolved USD-M set is empty: no USD-M
Collectors, product-specific side-data managers, process-global USD-M
side-data owner, REST polling, or WebSocket collection are instantiated. An
empty Spot set similarly creates no Spot Collector or Spot side-data traffic.
The process-global USD-M side-data owner exists only when a USD-M ProductKey is
configured and at least one global kind is enabled. The legacy
`GLOBAL_SIDE_DATA_SYMBOL="BTCUSDT"` sentinel never creates a core ProductKey.
The accepted sequence is MS2 configurable product runtime, MS3 shared-resource
scaling/rotation/observability, then MS4 configurable-product integration and
bounded live qualification.

## Boundaries that remain frozen

MS1 uses durable discontinuity identity `(market, symbol, stream)` with
`gap_id` for lifecycle matching and `(kind, symbol)` for symbol-specific side
data cursors. Legacy single-symbol Catalog identity is migrated to `BTCUSDT`
atomically, idempotently, restart-safely, and fail-closed; historical
symbol-less seal intents are not rewritten. Global side data remains global.

Raw v1 framing and payload bytes are unchanged. External Contracts are
unchanged. No account endpoints, credentials, orders, trading, other
exchanges, or arbitrary-symbol framework are authorized.

## Formal and deployment status

```text
DEPLOYMENT_AUTHORIZED=NO
FORMAL_M22_9_STARTED=NO
PRODUCTION_READY=NO
```

Any MS4 deployment requires a newly frozen source, immutable Wheel, lock,
config, unit, and deployment identities, followed by separate authorization
and a fresh bounded qualification. Historical single-symbol duration credit
does not transfer to a behavior-changing multi-symbol artifact.

## Next action

Complete the independent MS3-A review. Do not merge this candidate, deploy,
start MS3-B, or run qualification as part of this work package.
