# BBO Cache Candidate Review Bundle

## Scope and authority

This is one bounded, correctness-preserving BBO lookup candidate. It does not
modify GitHub `main`, open a PR, merge, deploy, run a VPS campaign, modify
Contracts, modify Raw, start Storage work, or modify M23.4.

```text
LIVE_AUTHORITY_MATCH=YES
REMOTE_MAIN=8c83d4fd79f3af061c8b293a7bbfa537285f3300
REMOTE_MAIN_TREE=03b3dc32fb96c785dd5285989e785ff7f2704419
REMOTE_BRANCHES=main-only
OPEN_PR_COUNT=0
```

The branch was created from the verified remote main commit. The two unrelated
pre-existing untracked storage-review files were preserved.

## Commit identity

```text
BRANCH=perf/cache-orderbook-bbo
BASE_SHA=8c83d4fd79f3af061c8b293a7bbfa537285f3300
BASE_TREE=03b3dc32fb96c785dd5285989e785ff7f2704419
CANDIDATE_SHA=0fecc068e369f60f3d937480f905e224c7bbf7bf
CANDIDATE_TREE=a521dd61f8a090b4930cce5254985383f8893a3f
CANDIDATE_PARENT=8c83d4fd79f3af061c8b293a7bbfa537285f3300
COMMIT_SUBJECT=perf: cache order book best levels
```

This bundle is generated after the implementation commit so it can contain the
exact candidate SHA/tree without requiring a second implementation commit. It
is an evidence artifact in the worktree; the implementation commit contains
only the three changed candidate files listed below.

## Changed files and diff summary

```text
CHANGED_FILE_COUNT=3
CHANGED_FILES=
  src/binance_market_data_recorder/orderbook/model.py
  tests/unit/test_orderbook_model.py
  tools/benchmark_orderbook_bbo_cache.py
PRODUCTION_FILES_CHANGED=src/binance_market_data_recorder/orderbook/model.py
```

Exact implementation diff:

- `OrderBook` initializes one cached best price per side from the materialized
  snapshot.
- Side updates retain the authoritative `dict[Decimal, Decimal]` mappings.
- Non-zero updates promote only a newly better price in O(1); quantity remains
  in the side dictionary and is read at lookup time.
- A batch that deletes the cached best performs one final `max`/`min` fallback
  after all levels in that side have been applied. A delete/reinsert batch also
  uses that final recomputation, preserving the actual final Decimal key.
- `best_bid`, `best_ask`, `is_crossed`, serialization, and hashing call the
  same public surfaces and retain their existing results.
- Tests add focused update cases, a fixed-seed dictionary-only differential
  oracle, and a fallback-recomputation spy.
- `tools/benchmark_orderbook_bbo_cache.py` is evidence-only and is not used by
  the production runtime.

## Root cause and mutation audit

```text
BBO_SOURCE_ROOT_CAUSE_CONFIRMED=YES
DIRECT_BIDS_ASKS_MUTATION_OUTSIDE_ORDERBOOK=NO
CACHE_ARCHITECTURE_SAFE=YES
```

The base `OrderBook` stored bids and asks as dictionaries and computed
`max(self.bids)` / `min(self.asks)` on every best-level property access. The
live reconstruction audit calls `is_crossed`, which reads both properties for
each accepted update.

The production search covered all Python sources for `OrderBook`, `.bids`,
`.asks`, `best_bid`, `best_ask`, `is_crossed`, `canonical_mapping`, and
`logical_hash`, plus direct mapping operations (`[]`, `pop`, `update`,
`clear`, `setdefault`, and `del`). The only production writes to the OrderBook
side dictionaries are in `orderbook/model.py`. The differential test's plain
dictionary oracle intentionally mutates its own independent test state; it is
not a production mutation site.

The public `bids` and `asks` attributes remain public and unchanged. The cache
invariant is maintained by the synchronous `OrderBook` constructor and
`apply()` writer. No custom mapping, ordered-map dependency, heap, or
encapsulation refactor was introduced.

## Architecture and complexity

```text
_best_bid_price: Decimal | None
_best_ask_price: Decimal | None
```

The cache stores price only. `best_bid` returns
`(_best_bid_price, self.bids[_best_bid_price])`, and `best_ask` analogously
reads quantity from the authoritative dictionary.

```text
BEST_BID_COMMON_LOOKUP_COMPLEXITY_BEFORE=O(number of live bid levels)
BEST_BID_COMMON_LOOKUP_COMPLEXITY_AFTER=O(1)
BEST_ASK_COMMON_LOOKUP_COMPLEXITY_BEFORE=O(number of live ask levels)
BEST_ASK_COMMON_LOOKUP_COMPLEXITY_AFTER=O(1)
BEST_DELETION_FALLBACK_COMPLEXITY=O(number of live levels on that side), once per affected side/update batch
```

Cache invariants:

1. An empty side has a `None` cached best.
2. A non-empty side's cached price is the exact current best price key after
   construction or `apply()` completes.
3. In-place quantity updates do not replace the cached price and the property
   reads the current dictionary quantity.
4. A strictly better insertion promotes the cache without a scan.
5. A current-best deletion causes one post-batch fallback; non-best deletion
   causes no fallback.
6. Snapshot validation and all update validation remain unchanged and complete
   before each side's mutations begin.

## Semantic preservation

```text
INITIALIZATION_SEMANTICS_PRESERVED=YES
NONBEST_UPDATE_SEMANTICS_PRESERVED=YES
BEST_QUANTITY_UPDATE_SEMANTICS_PRESERVED=YES
BEST_DELETION_SEMANTICS_PRESERVED=YES
EMPTY_SIDE_SEMANTICS_PRESERVED=YES
CROSSED_BOOK_SEMANTICS_PRESERVED=YES
BOOK_TICKER_SEMANTICS_PRESERVED=YES
CANONICAL_MAPPING_PRESERVED=YES
LOGICAL_HASH_PRESERVED=YES
```

The focused cases cover initial snapshot best levels, better bid/ask
insertions, best and non-best quantity updates, non-best and best deletions,
last-level empty transitions, same-batch replacement, Decimal-key
delete/reintroduction, crossed-book behavior, ticker comparison, and
equivalent final-book canonical/hash equality. Existing parser, reconstructor,
checkpoint, fault-injection, continuity, gap, and resync tests remain green.

## Randomized differential test

```text
RANDOMIZED_DIFFERENTIAL_SEED=2026090201
RANDOMIZED_DIFFERENTIAL_CASES=4 initial sizes × 256 updates = 1024 applied updates
RANDOMIZED_DIFFERENTIAL_RESULT=PASS
```

The test uses sizes 1, 4, 16, and 64, Spot-shaped valid `DepthUpdate` objects,
and a deterministic `random.Random(2026090201)`. Its small independent oracle
uses only authoritative dictionaries and derives best bid with `max` and best
ask with `min`. The operation cycle includes better insertions, non-best
quantity changes, best quantity changes, non-best deletion, best deletion,
worse insertion, delete/reintroduction with duplicate same-price levels,
multi-side updates, and forced empty bid/ask batches.

After every applied update it compares complete bids and asks mappings,
`update_id`, `best_bid`, `best_ask`, `is_empty`, `is_crossed`,
`canonical_mapping()`, and `logical_hash()`. All 1024 updates and all operation
categories passed.

## Mechanical complexity proof

```text
MECHANICAL_COMMON_PATH_SCAN_REMOVED=YES
BEST_DELETION_FALLBACK_BOUNDED=YES
```

`test_common_updates_do_not_scan_and_best_deletion_scans_once_per_side`
monkeypatches the narrow `_recompute_best_price` helper after construction.
It applies 100 non-best bid/ask updates and observes zero helper calls. A
current-best bid deletion then records exactly one bid fallback, and a
current-best ask deletion records exactly one ask fallback. The production
test exposes no debug counter.

## Test results

```text
FOCUSED_TEST_RESULT=86 passed (orderbook model/reconstructor/parser/checkpoint/fault tests)
FULL_PYTEST_RESULT=1466 passed, 24 skipped, 4 deselected, 13 warnings in 121.24s
RUFF_RESULT=PASS (python3.12 -m ruff check .)
MYPY_RESULT=PASS (Success: no issues found in 246 source files)
DIFF_CHECK_RESULT=PASS (git diff --check and staged/commit diff checks)
```

The skips are the repository's opt-in public Binance, preview, and historical
backfill smoke tests. No online smoke, stress, VPS, deployment, or production
data test was run.

## Microbenchmark method

The evidence-only script is
`tools/benchmark_orderbook_bbo_cache.py`. It was run as two fresh processes
using the same `/Users/amada/miniforge3/bin/python3.12` interpreter
(Python 3.12.9), with the exact base source selected through a detached base
worktree and `PYTHONPATH`, and the exact candidate source selected from the
candidate `src` tree. Each fixture has 20 warm-up iterations, disables GC only
during its timed loop, excludes fixture construction from timed work, and uses
the same pre-built updates for both source identities.

Measured scenarios:

- `common_nonbest_apply_audit`: apply non-best bid/ask quantity updates, then
  read `is_crossed`.
- `best_quantity_apply_audit`: apply current-best bid/ask quantity updates,
  then read `is_crossed`.
- `occasional_best_deletion_apply_audit`: current-best delete/reinsert every
  20th update, non-best updates otherwise, then read `is_crossed`.
- `direct_best_lookup`: repeated `best_bid` and `best_ask` reads.
- `best_deletion_fallback_apply_only`: delete/reinsert the current best every
  iteration and measure `apply()` without audit, isolating the intentional
  fallback cost.

All values below are microseconds per iteration. Speedup is baseline divided by
candidate; values above 1.0 favor the candidate.

| Levels/side | Iterations | Scenario | Base µs/iter | Candidate µs/iter | Speedup |
| ---: | ---: | --- | ---: | ---: | ---: |
| 1,000 | 2,000 | common apply+audit | 30.066938 | 1.670438 | 17.999× |
| 1,000 | 2,000 | best quantity apply+audit | 30.753563 | 1.677271 | 18.335× |
| 1,000 | 2,000 | occasional best deletion apply+audit | 32.466771 | 1.963583 | 16.534× |
| 1,000 | 2,000 | direct best lookup | 29.240854 | 0.243396 | 120.137× |
| 1,000 | 2,000 | best-deletion fallback apply-only | 1.615792 | 17.119729 | 0.094× |
| 5,000 | 800 | common apply+audit | 149.289688 | 1.670000 | 89.395× |
| 5,000 | 800 | best quantity apply+audit | 148.905781 | 1.697344 | 87.729× |
| 5,000 | 800 | occasional best deletion apply+audit | 149.121094 | 4.980053 | 29.944× |
| 5,000 | 800 | direct best lookup | 149.734428 | 0.251823 | 594.603× |
| 5,000 | 800 | best-deletion fallback apply-only | 1.652760 | 77.108959 | 0.021× |
| 10,000 | 400 | common apply+audit | 287.251980 | 1.694895 | 169.481× |
| 10,000 | 400 | best quantity apply+audit | 283.641770 | 1.840103 | 154.145× |
| 10,000 | 400 | occasional best deletion apply+audit | 303.913125 | 8.609480 | 35.300× |
| 10,000 | 400 | direct best lookup | 282.401458 | 0.241458 | 1,169.570× |
| 10,000 | 400 | best-deletion fallback apply-only | 1.601353 | 151.903750 | 0.011× |

Exact benchmark identities:

```text
BENCHMARK_BASE_SHA=8c83d4fd79f3af061c8b293a7bbfa537285f3300
BENCHMARK_CANDIDATE_SHA=0fecc068e369f60f3d937480f905e224c7bbf7bf
BENCHMARK_FIXTURES=1000/2000, 5000/800, 10000/400
```

```text
BENCHMARK_COMMON_PATH_RESULT=1.670438/1.670000/1.694895 µs candidate at 1000/5000/10000 levels; 18.0×/89.4×/169.5× speedup
BENCHMARK_BEST_LOOKUP_RESULT=0.243396/0.251823/0.241458 µs candidate; 120.1×/594.6×/1169.6× speedup
BENCHMARK_BEST_DELETION_RESULT=0.094×/0.021×/0.011× isolated fallback speedup; candidate intentionally performs one O(N) scan
MATERIAL_COMMON_PATH_IMPROVEMENT=YES
```

The microbenchmark is evidence for this code path only. It is not a
whole-process profile, production throughput proof, or VPS result.

## Known limitations and explicit boundaries

- Direct external mutation of the public `bids`/`asks` dictionaries would
  bypass any `OrderBook` cache invariant. No supported production code does
  this; the attributes were intentionally not privatized or replaced.
- Best-price deletion remains O(N) once per affected side/update batch by
  design. No heap, tree, sorted parallel array, dependency, or native code was
  added.
- The microbenchmark does not establish whole-process CPU savings or account
  for network, Raw, Catalog, filesystem, or host scheduling behavior.
- Existing Decimal, validation, update ID, continuity, gap/resync, audit,
  serialization, checkpoint, and comparison behavior is covered by parity and
  repository gates but not changed in contract/schema files.

```text
INCLUSIVE_PROFILE_SHARE_IS_NOT_EXACT_CPU_PERCENTAGE=YES
WHOLE_PROCESS_CPU_SAVING_CLAIMED=NO
VPS_AB_RUN=NO
PR_OPENED=NO
MERGED=NO
DEPLOYED=NO
STORAGE_CHANGED=NO
M23_4_CHANGED=NO
```

## Executor verdict

```text
FINAL_EXECUTOR_VERDICT=ACCEPTED AS A BOUNDED BBO CACHE CANDIDATE; FUNCTIONAL PARITY, MECHANICAL SCAN REMOVAL, FULL OFFLINE VALIDATION, AND MATERIAL SAME-WORKLOAD COMMON-PATH IMPROVEMENT ALL PASS. REVIEW/DEPLOYMENT/VPS VALIDATION REMAIN SEPARATE.
REVIEW_BUNDLE=BBO_CACHE_CANDIDATE_REVIEW_BUNDLE.md
```
