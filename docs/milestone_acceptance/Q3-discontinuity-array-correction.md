# Q3 — Private discontinuity index encoding

October2 UTC / October3 Shanghai,2026. Q3 remains PARTIAL_NOT_ACCEPTED;
Formal2h has not started, credit0,12H_STARTED=NO,PRODUCTION_READY=NO.

## Actual failure and cause

Installed4e1cf32 engineering attempt2 on1182 retained chunks failed16:40:53 UTC,
after its16:31:34 start, before publishing audit-root or completed verification.
Unit `binance-recorder-q3-startup-engineering-baseline-2.service`, invocation
ec9cbd60f5d04a3480090d1ed1415a99, exit2. CPU562.412s, memory peak323919872B
including cache. Original result/log/control files remain retained under
`/srv/recorder-data/recorder-archive/evidence/Q3-startup-boundary-20261002-mfJDVr8h`.
Failure log SHA5b74fbe4a458f2eee38e1102ec62c5e8386263dc1b8f6ac1aa08d4902aa1a3de.
No failure is converted into a PASS and no failed prefix is reused under a new identity.

The last durable checkpoint has18920 records/39 shards and operational event8.
Event10/11 are a real network STARTED/COMPLETED pair, distinct from the earlier
sparse initial-connection failure. `_operational_events` passed this array of
9-key mappings to `canonical_json`, whose public document contract calls
`dict(document)`. That raises `ValueError: dictionary update sequence element
#0 has length 9; 2 is required`. Earlier697-chunk PASS contained no such pair.
This is an audit implementation error, not evidence of corrupt Raw or hardware
insufficiency; no data loss is inferred.

## Minimal correction and verification

Only serialize the private SQLite `gaps.pair` value as deterministic UTF-8 JSON
array, matching its existing `json.loads` consumer. Keep the authoritative
mapping encoder, Raw/Catalog/proof formats, gap classification, readiness,
batch/time/cadence limits and both LIVE passes unchanged. No new framework,
fallback, schema or ADR is needed for this private scratch representation.

Real sealed Raw + Catalog full-baseline integration cases exercise an open gap,
a completed gap and malformed generation authority. Both valid cases must
publish without findings and reconstruct through LIVE and frozen-control
verification; malformed authority must remain FAIL. All cases retain zero
Formal duration credit. Before the fix the open/completed cases both reproduced
the exact ValueError. Retained local log:
`/Users/amada/Library/Caches/BinanceMarketDataRecorder/q3-startup-boundary-20261002/discontinuity-array-before-fix.log`.
After correction56 affected tests PASS7.49s, then all6 finalize-module cases
including malformed PASS1.28s and the full affected57-test selection PASS7.32s.
Full offline1994 PASS164.75s,24 online SKIP,5 stress deselected;13 existing
fork warnings. Ruff, strict mypy287, M0, Go golden and diff check PASS.
Independent GPT-6.1 Sol xhigh LOCAL/OFFLINE source-only ACCEPT, no open findings;
60 independent tests PASS5.71s, zero network/SSH/production operations/repository
edits. Twelve real scratch-index traversals separately confirm exact array1/2,
malformed FAIL, LIVE/historical reconstruction and original-method ValueError.
Review identity `/private/var/tmp/bmdr-startup-ordinary-review-kbJkqHnV/discontinuity-array-review-identity.json`,
SHA92e0233ae052b0d5a1b60645f344ca874f30bea1281adf3e971549d45609aa6b.
Exact runtime/test diff SHA55d82ae9575fe7d1a0a2bf161331a14fb1655df261fd2d6ff625487d2514d04b.
The five startup runtime files remain unchanged; prior source ACCEPT is reused.
Exact replacement CI/wheel/cloud/deployment/baseline gates remain pending.

Recorder remains inactive AND disabled,1182 chunks/transactions LOCAL_DELETED,
Catalogok/0partials/backlog. Archive authority is paused for the failed audit;
normal OS authority restored. No live monitor or Formal T0 exists. All14 flags,
four products and the growing corpus stay unchanged. Preserve unique archive,
old metadata and both failed attempts. Future archive pauses stop the timer
first and wait for the current worker to finish, preserving strict quiescence.

Next: independent review, freeze/push one exact replacement, dual-platform CI,
clean locked wheel/affected cloud tests, stopped canonical deployment and fresh
engineering baseline/both LIVE/completed verification. Then actual warm-up/
convergence/resource forecasts, finite-window authoritative post-warm-up baseline,
strict readiness and reviewed eligible Formal2h. Stop before12h.
